#!/bin/bash
# §10 SLA 7日間サマリーレポート
# 使い方: ./deploy/ops/sla_report.sh [日数(デフォルト7)]
PROJECT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
LOG="${PROJECT_DIR}/.airflow/logs/daily_check.log"
DAYS="${1:-7}"

if [[ ! -f "$LOG" ]]; then
    echo "ログファイルが見つかりません: $LOG"
    echo "check_daily.sh を先に実行してください。"
    exit 1
fi

echo "========================================"
echo " §10 SLA レポート（直近 ${DAYS} 日間）"
echo "========================================"
echo ""

PASS=0
FAIL=0
TOTAL_CRITICAL=0
MAX_NEEDS_HUMAN=0
AUDIT_BREAKS=0

while IFS= read -r line; do
    if [[ "$line" == "SLA: PASS"* ]]; then
        ((PASS++)) || true
    elif [[ "$line" == "SLA: FAIL"* ]]; then
        ((FAIL++)) || true
    fi
    if [[ "$line" == "critical_logs:"* ]]; then
        n="${line#critical_logs: }"
        TOTAL_CRITICAL=$((TOTAL_CRITICAL + n))
    fi
    if [[ "$line" == "tasks:"* ]]; then
        nh=$(echo "$line" | grep -oP 'needs_human=\K\d+' || echo 0)
        [[ "$nh" -gt "$MAX_NEEDS_HUMAN" ]] && MAX_NEEDS_HUMAN="$nh"
    fi
    if [[ "$line" == *"CHAIN BROKEN"* ]]; then
        ((AUDIT_BREAKS++)) || true
    fi
done < <(tail -n $((DAYS * 20)) "$LOG")

TOTAL=$((PASS + FAIL))
[[ "$TOTAL" -eq 0 ]] && { echo "データなし（check_daily.sh がまだ実行されていません）"; exit 0; }

echo "集計期間のチェック回数: ${TOTAL} 回"
echo ""
echo "SLA 合格: ${PASS} / ${TOTAL} 日"
echo "SLA 不合格: ${FAIL} / ${TOTAL} 日"
echo ""
echo "指標詳細:"
printf "  %-30s %s\n" "critical ログ累計:" "${TOTAL_CRITICAL} 件（目標: 0）"
printf "  %-30s %s\n" "needs_human 最大値:" "${MAX_NEEDS_HUMAN} 件/日（目標: <10）"
printf "  %-30s %s\n" "監査チェーン破断:" "${AUDIT_BREAKS} 回（目標: 0）"
echo ""

if [[ "$FAIL" -eq 0 && "$TOTAL" -ge "$DAYS" ]]; then
    echo "✅ 本番運用基準（§10）達成"
elif [[ "$TOTAL" -lt "$DAYS" ]]; then
    echo "⏳ 継続監視中 ... あと $((DAYS - TOTAL)) 日"
else
    echo "❌ SLA 未達（要調査）"
fi

echo ""
echo "--- 直近のログ ---"
tail -20 "$LOG"
