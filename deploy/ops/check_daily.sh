#!/bin/bash
# §10 SLA 日次チェックスクリプト
# 毎日 1 回実行し、SLA メトリクスを .airflow/logs/daily_check.log に記録する。
# WSL2 での実行: Windows タスクスケジューラから
#   wsl -d Ubuntu-24.04 -- /home/nori/Airflow/deploy/ops/check_daily.sh
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$PROJECT_DIR"

LOG_DIR="${PROJECT_DIR}/.airflow/logs"
REPORT_LOG="${LOG_DIR}/daily_check.log"
VENV="${PROJECT_DIR}/.venv/bin/python"

mkdir -p "$LOG_DIR"

DATE=$(date -u +%Y-%m-%d)
TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)

echo "=== ${TS} ===" | tee -a "$REPORT_LOG"

# ── 1. タスク状態取得 ────────────────────────────────────────────
STATUS_JSON=$("$VENV" - <<'EOF'
import json, sys
sys.path.insert(0, 'src')
from airflow_mcp.taskboard.store import TaskStore
store = TaskStore(".airflow/tasks")
all_tasks = store.list_all()
counts = {}
for t in all_tasks:
    s = t.get("status", "unknown")
    counts[s] = counts.get(s, 0) + 1
done_dir = store.base / "done"
done_total = len(list(done_dir.rglob("task_*.json")))
counts["done_total"] = done_total
print(json.dumps(counts))
EOF
)

DONE=$(echo "$STATUS_JSON" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('done_total',0))")
NEEDS_HUMAN=$(echo "$STATUS_JSON" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('needs_human',0))")
IN_PROGRESS=$(echo "$STATUS_JSON" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('in_progress',0))")
TOTAL=$(echo "$STATUS_JSON" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(sum(v for k,v in d.items() if k!='done_total'))
")

echo "tasks: done=${DONE} needs_human=${NEEDS_HUMAN} in_progress=${IN_PROGRESS} total=${TOTAL}" | tee -a "$REPORT_LOG"

# ── 2. critical ログ件数（pipefail 対策: wc -l のみ使用）────────
CRITICAL=0
if [[ -f "${LOG_DIR}/${DATE}.jsonl" ]]; then
    count=$(grep '"level":"critical"' "${LOG_DIR}/${DATE}.jsonl" 2>/dev/null | wc -l) || true
    CRITICAL="${count//[[:space:]]/}"
fi
echo "critical_logs: ${CRITICAL}" | tee -a "$REPORT_LOG"

# ── 3. 監査ハッシュ生成・検証 ────────────────────────────────────
"$VENV" -m airflow_mcp audit hash 2>&1 | tee -a "$REPORT_LOG"
AUDIT_RESULT=$("$VENV" -m airflow_mcp audit verify 2>&1)
echo "$AUDIT_RESULT" | tee -a "$REPORT_LOG"
AUDIT_OK=$(echo "$AUDIT_RESULT" | grep -c "chain ok") || true

# ── 4. SLA 判定 ──────────────────────────────────────────────────
SLA_PASS=true
if [[ "${CRITICAL}" -ne 0 ]]; then
    echo "SLA FAIL: critical_logs=${CRITICAL}" | tee -a "$REPORT_LOG"
    SLA_PASS=false
fi
if [[ "${NEEDS_HUMAN}" -ge 10 ]]; then
    echo "SLA FAIL: needs_human=${NEEDS_HUMAN} (>=10)" | tee -a "$REPORT_LOG"
    SLA_PASS=false
fi
if [[ "${AUDIT_OK}" -lt 1 ]]; then
    echo "SLA FAIL: audit chain broken" | tee -a "$REPORT_LOG"
    SLA_PASS=false
fi

if [[ "$SLA_PASS" == "true" ]]; then
    echo "SLA: PASS [critical=0 needs_human=${NEEDS_HUMAN} audit=ok]" | tee -a "$REPORT_LOG"
else
    echo "SLA: FAIL" | tee -a "$REPORT_LOG"
fi

echo "---" | tee -a "$REPORT_LOG"
