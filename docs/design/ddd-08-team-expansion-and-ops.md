# DDD-08：チーム拡張・本番化設計

> Phase 5（最終）。MVP・並列実行基盤の上に **§01 全エージェント + §02 Content + §03 Business** を展開し、セキュリティ・運用監視・夜間バッチを整備して **本番運用可能** な状態にする。
>
> 参照要件：`requirements-ai-team-composition.md` 全般、`requirements-overview.md` §4「非機能要件」（NF-04〜07）
> 前提：DDD-01〜07 全て

---

## 1. スコープ

### 1.1 含む

- §01〜§03 全エージェントの `skills.md` 雛形
- エージェント間の handoff 連携パターン（4 段レビュー等）
- セキュリティ・認証（JWT 化、レート制限、権限境界）
- 監査ログの改ざん耐性
- 運用監視ダッシュボード（CLI + Web）
- 夜間バッチスケジューラ
- 通知連携（Slack / メール）
- 24 時間連続運用の検証戦略

### 1.2 含まない

- マルチホスト分散運用（将来課題）
- 人間 UI（Web フロントエンド）の完全実装

---

## 2. エージェント展開（§01〜§03）

### 2.1 ディレクトリ構成

```
.airflow/agents/
├── orchestration/
│   ├── dispatcher/skills.md          # agent_dispatcher
│   ├── cleaner/skills.md             # agent_cleaner（要件 NF-11 旧称 "Hermes Agent"。DDD-01 §1.2.1 用語統一参照）
│   └── feature-extractor/skills.md   # agent_feature_extractor
├── engineering/                 # §01
│   ├── tech-lead/skills.md
│   ├── eng-director/skills.md
│   ├── qa/skills.md
│   ├── frontend/skills.md
│   ├── backend/skills.md
│   ├── security/skills.md
│   ├── devops/skills.md
│   ├── architect/skills.md
│   ├── mobile/skills.md
│   ├── data/skills.md
│   ├── reviewer/skills.md
│   ├── tester/skills.md
│   ├── debugger/skills.md
│   └── docs/skills.md
├── content/                     # §02
│   ├── content-director/skills.md
│   ├── brand-voice/skills.md
│   ├── root-cause/skills.md
│   ├── anti-ai-slop/skills.md
│   └── morning-brainstorm/skills.md
└── business/                    # §03
    ├── marketing-director/skills.md
    ├── business-strategy/skills.md
    ├── partnership-manage/skills.md
    └── legal-review/skills.md
```

### 2.1.1 ID とディレクトリの対応表（全 26 エージェント）

DDD-01 §1.2 の命名規約に従い、エージェント ID（underscore）とディレクトリ名（hyphen）を明確に対応させる：

| # | エージェント ID | ディレクトリ | セクション |
|---|--------------|----------|----------|
| 1 | `agent_dispatcher` | `orchestration/dispatcher/` | orchestration |
| 2 | `agent_cleaner` | `orchestration/cleaner/` | orchestration |
| 3 | `agent_feature_extractor` | `orchestration/feature-extractor/` | orchestration |
| 4 | `agent_tech_lead` | `engineering/tech-lead/` | engineering |
| 5 | `agent_eng_director` | `engineering/eng-director/` | engineering |
| 6 | `agent_qa` | `engineering/qa/` | engineering |
| 7 | `agent_frontend` | `engineering/frontend/` | engineering |
| 8 | `agent_backend` | `engineering/backend/` | engineering |
| 9 | `agent_security` | `engineering/security/` | engineering |
| 10 | `agent_devops` | `engineering/devops/` | engineering |
| 11 | `agent_architect` | `engineering/architect/` | engineering |
| 12 | `agent_mobile` | `engineering/mobile/` | engineering |
| 13 | `agent_data` | `engineering/data/` | engineering |
| 14 | `agent_reviewer` | `engineering/reviewer/` | engineering |
| 15 | `agent_tester` | `engineering/tester/` | engineering |
| 16 | `agent_debugger` | `engineering/debugger/` | engineering |
| 17 | `agent_docs` | `engineering/docs/` | engineering |
| 18 | `agent_content_director` | `content/content-director/` | content |
| 19 | `agent_brand_voice` | `content/brand-voice/` | content |
| 20 | `agent_root_cause` | `content/root-cause/` | content |
| 21 | `agent_anti_ai_slop` | `content/anti-ai-slop/` | content |
| 22 | `agent_morning_brainstorm` | `content/morning-brainstorm/` | content |
| 23 | `agent_marketing_director` | `business/marketing-director/` | business |
| 24 | `agent_business_strategy` | `business/business-strategy/` | business |
| 25 | `agent_partnership_manage` | `business/partnership-manage/` | business |
| 26 | `agent_legal_review` | `business/legal-review/` | business |

**内訳**：調整層 3 + Engineering 14 + Content 5 + Business 4 = **26 エージェント**

### 2.1.2 リポジトリ位置とデプロイ位置の関係

| 用途 | パス |
|------|------|
| **リポジトリ管理（雛形・正本）** | `agents/<section>/<name>/skills.md` |
| **ランタイム（実行時の参照先）** | `.airflow/agents/<section>/<name>/skills.md` |

デプロイ時に `agents/` → `.airflow/agents/` へコピー（または symlink）する。
ランタイム側で個別エージェントごとの override が必要な場合は、`.airflow/agents/<section>/<name>/overrides.md` を追加で配置（雛形には反映しない）。

### 2.2 `skills.md` 雛形

```yaml
---
schema_version: "1.0.0"
agent_id: agent_<name>
section: engineering | content | business | orchestration
platform: claude-code | claude-cowork | hermes-agent | custom
role: "<1 行で役割>"

responsibilities:
  - "<具体的責務>"

inputs:
  - "<受け取るタスク種別>"

outputs:
  - "<生成する成果物>"

triggers:
  - "<起動タイミング>"

escalation_criteria:
  - "<needs_human に切り替える条件>"

preferred_models:
  - id: model_qwen25_32b_q3
    weight: 1.0
  - id: model_qwen25_14b_q4
    weight: 0.5

context_isolation: subagent

required_accuracy: high | medium | low
language: ja | en | code | mixed

prompt_templates:
  system: "system/<name>.md"

dependencies:
  upstream: ["<前段エージェント>"]
  downstream: ["<後段エージェント>"]

mcp_scopes:
  - "tasks:read"
  - "tasks:write"
  - "notes:read"
  - "models:execute"
  - "gpu:reserve"
---

# Role
<役割の詳細>

# Process
<処理手順>

# Examples
<Few-shot 例 3-5 件>
```

### 2.3 §01 Engineering（14 エージェント）

要件定義 §2.2 の責任範囲を `skills.md` に落とし込む。各エージェントの `required_accuracy` と `preferred_models` の方針：

| エージェント | required_accuracy | preferred top model | 自律実行 |
|------------|-----------------|------------------|--------|
| `tech-lead` | high | qwen25_32b_q3 | PR 作成時 |
| `eng-director` | medium | qwen25_14b_q4 | 毎週月曜 09:00 |
| `qa` | high | qwen25_coder_32b_q3 | PR 作成時 + 毎晩 02:00 |
| `frontend` | medium | qwen25_coder_32b_q3 | タスク投入時 |
| `backend` | medium | qwen25_coder_32b_q3 | タスク投入時 |
| `security` | high | qwen25_32b_q3 | PR 作成時 + 毎晩 02:30 |
| `devops` | high | qwen25_32b_q3 | 設定変更タスク（人間承認必須） |
| `architect` | high | qwen25_32b_q3 | 設計依頼タスク |
| `mobile` | medium | qwen25_coder_32b_q3 | タスク投入時 |
| `data` | medium | qwen25_coder_32b_q3 | タスク投入時 |
| `reviewer` | medium | qwen25_coder_32b_q3 | PR 作成時 |
| `tester` | medium | qwen25_coder_32b_q3 | 新機能 PR 時 |
| `debugger` | high | qwen25_coder_32b_q3 | バグレポート / CI 失敗時 |
| `docs` | medium | qwen25_14b_q4 | PR マージ後 |

### 2.4 §02 Content（5 エージェント）

```
content-director ─→ brand-voice ─→ root-cause ─→ anti-ai-slop ─→ done
                         │              │              │
                         ↓ NG          ↓ NG          ↓ NG
                    director に差戻し（handoff_note: kind=question）
```

各エージェントの `escalation_criteria` 例：

```yaml
# brand-voice/skills.md（抜粋）
escalation_criteria:
  - "ブランドガイドラインに該当する基準が見当たらない"
  - "ガイドライン違反の度合いが grey zone（信頼度 0.4-0.6）"
  - "感情的・政治的トピックを含む"
```

### 2.5 §03 Business（4 エージェント）

法務・契約系は **すべて人間（弁護士）確認必須** を `escalation_criteria` に明記：

```yaml
# legal-review/skills.md（抜粋）
escalation_criteria:
  - "すべての契約書 — AI は一次フィルタのみ、最終確認は人間（弁護士）"
required_accuracy: high
context_isolation: subagent
mcp_scopes:
  - "tasks:read"
  - "tasks:write"
  - "notes:read"
  - "models:execute"
```

partnership-manage は **「ハイプ製品をお断り」** ロジックを Few-shot で明示：

```markdown
# Examples（partnership-manage/skills.md 内）

## 採用例
- 入力: 既存ワークフローに具体的な時短効果（30% 以上）が実証されている AI 製品
  出力: {recommend: true, reason: "実証データあり、空気デザインの読者層に有用"}

## 不採用例
- 入力: "Revolutionary AI" "Game-changing" など抽象語のみで具体的事例なし
  出力: {recommend: false, reason: "ハイプの特徴、具体的価値提案なし"}
```

---

## 3. エージェント連携パターン

### 3.1 4 段レビュー（§02）

`taskboard.tasks.transition` の連鎖で実現：

```
タスク投入（content task）
   owner = content-director
   ↓ director が膨らませる
   handoff_note(kind=result, body="膨らませた原稿…")
   tasks.transition(in_progress, owner=brand-voice)
   ↓
brand-voice チェック
   if OK: handoff_note(kind=decision, body="ブランド合致"), transition(owner=root-cause)
   if NG: handoff_note(kind=question), transition(owner=content-director, needs_human=false)
   ↓ ... 同様に root-cause → anti-ai-slop
   ↓
anti-ai-slop 完了
   handoff_note(kind=result, body="最終原稿"), transition(done)
```

タスクスキーマの `chain` フィールド（DDD-01 §2.1 で定義済み）で連鎖を追跡：

```yaml
chain:
  - agent_id: agent_content_director
    completed_at: "..."
    result_ref: "handoff_note_id"
  - agent_id: agent_brand_voice
    completed_at: "..."
    result_ref: "..."
```

### 3.2 PR レビュー（§01）

```
PR 作成 → タスク生成（auto_dispatch）
   ↓ 並列に投入
   ┌─ qa（テスト実行）
   ├─ security（脆弱性スキャン）
   └─ reviewer（コードレビュー）
   ↓ 全完了
tech-lead が集約
   ↓ 承認 or needs_human
```

並列タスクの集約には MCP ツール `tasks.join`（DDD-02 §3.2 で定義済み）を使用。

`all_done` で全子タスクが done になったら親タスクが起動。状態は Task の `join` フィールド（DDD-01 §2.1）に保存される。

---

## 4. セキュリティ・認証（NF-04〜07）

### 4.1 認証方式の昇格

Phase 1〜4 は API Key（静的トークン）。Phase 5 で **JWT** を追加：

```yaml
auth:
  modes:
    - api_key       # 既存
    - jwt           # 追加（多人数運用時）
  jwt:
    issuer: "airflow-local"
    signing_alg: "EdDSA"
    private_key_path: ".airflow/auth/ed25519.pem"
    public_key_path: ".airflow/auth/ed25519.pub"
    default_ttl_sec: 3600
```

JWT クレーム：

```json
{
  "iss": "airflow-local",
  "sub": "agent_qa",
  "exp": 1234567890,
  "iat": 1234564290,
  "scopes": ["tasks:read", "tasks:write", "models:execute"],
  "session_id": "..."
}
```

### 4.2 レート制限（NF-06 対応）

MCP サーバーに per-token のレート制限を導入：

```yaml
rate_limit:
  enabled: true
  default_limits:
    tasks_per_minute: 60
    models_execute_per_minute: 20
    notes_search_per_minute: 100
  per_agent_overrides:
    agent_qa: { models_execute_per_minute: 60 }   # 夜間バッチで多用
  burst_multiplier: 2
```

実装：Token Bucket アルゴリズム。超過時 `AUTH_RATE_LIMITED` recoverable エラー + `retry_after_ms`。

### 4.3 権限境界（NF-04, NF-07 対応）

| エージェント種別 | 許可スコープ | 禁止操作 |
|--------------|----------|--------|
| Engineering 実装系 | tasks:rw, notes:r, models:exec, gpu:reserve | インフラ変更（devops のみ） |
| QA / Reviewer | tasks:rw, models:exec | 本番デプロイ |
| Content 系 | tasks:rw, notes:r, models:exec | コード変更 |
| Business 系 | tasks:rw, notes:r, models:exec | 全システム設定変更 |
| Cleaner Agent | tasks:rw + tasks:delete | モデル / GPU 操作 |
| Dispatcher | tasks:write, agents:read, notes:read | model:exec |

### 4.4 機密ノートのアクセス制御

DDD-04 で定義した `confidential: true` ノートに加え、エージェント単位の許可リスト：

```yaml
# ingester.yaml
confidential_access:
  agent_legal_review: ["category=legal"]
  agent_business_strategy: ["category=business", "category=strategy"]
  # 上記以外は confidential ノートにアクセス不可
```

`notes.search` / `notes.get` はトークンスコープをチェックし、許可外なら `NOTE_CONFIDENTIAL_ACCESS_DENIED` fatal。

### 4.5 監査ログの改ざん耐性

DDD-03 §7.3 の懸案を実装：

```
1. 日次ログを .airflow/logs/<date>.jsonl に追記書き込み（append-only）
2. 日次バッチ（毎日 03:00）で:
   a. sha256(<date>.jsonl) を計算
   b. ハッシュチェーン: hash_<N> = sha256(<date>.jsonl + hash_<N-1>)
   c. .airflow/logs/integrity/<date>.hash に記録
3. 検証 CLI: airflow audit verify --since 2026-01-01
```

ハッシュチェーン破壊検出：critical アラート、Slack 通知。

### 4.6 トークンローテーション

```bash
airflow auth rotate --agent agent_qa
# 新トークン発行、24 時間後に旧トークン無効化
# その間に各クライアントが切替
```

---

## 5. 監視・ダッシュボード

### 5.1 CLI ダッシュボード（統合版）

```bash
$ airflow status

╔════════════════════════════════════════════════════════════════╗
║  Airflow Status  -  2026-05-22 15:30:00 JST                     ║
╠════════════════════════════════════════════════════════════════╣
║ Mode: NORMAL (degradation off)                                  ║
║ GPU: 14.2/16.0 GB | Profile: 14b-plus-8b | Loaded: 14b, 8b, emb ║
╠════════════════════════════════════════════════════════════════╣
║ Tasks                                                            ║
║   inbox:      12   in_progress:  4   done(today):  47           ║
║   needs_human: 1   blocked: 0                                   ║
╠════════════════════════════════════════════════════════════════╣
║ Sections                                                         ║
║   §01 Engineering: 28 (5 active, qa busy)                       ║
║   §02 Content:      8 (1 active, brand-voice)                   ║
║   §03 Business:     4 (0 active)                                ║
╠════════════════════════════════════════════════════════════════╣
║ Recent Events                                                    ║
║   14:55  task.done       task_a... (qa, 4.2s, score 0.89)       ║
║   14:54  escalation      task_b... (legal-review)               ║
║   14:50  scheduler.profile_switched single-32b → 14b-plus-8b    ║
╠════════════════════════════════════════════════════════════════╣
║ Alerts: 1 needs_human pending                                    ║
╚════════════════════════════════════════════════════════════════╝
```

### 5.2 Web ダッシュボード

最小限の閲覧用 Web UI（read-only）：

| ページ | 内容 |
|-------|------|
| `/` | 上記 CLI と同等のサマリ |
| `/tasks` | タスク一覧（フィルタ・検索） |
| `/tasks/<id>` | タスク詳細、handoff_notes 時系列、関連ログ |
| `/agents` | エージェント一覧、稼働状況 |
| `/models` | モデル統計、KNN coverage |
| `/scheduler` | キュー深度、プロファイル履歴 |
| `/audit` | ログ検索（audit_id トレース） |

実装：FastAPI（Python）+ シンプルな Server-Side Rendering。認証は内蔵 JWT。

### 5.3 メトリクスエクスポート

Phase 4 で SQLite クエリ集計だった指標を Prometheus 形式に：

```
# HELP airflow_tasks_total Tasks by status
# TYPE airflow_tasks_total counter
airflow_tasks_total{status="done",section="engineering"} 1247

# HELP airflow_model_execution_duration_seconds
# TYPE airflow_model_execution_duration_seconds histogram
airflow_model_execution_duration_seconds_bucket{model="qwen25_14b_q4",le="2"} 89
...
```

オプトインで Grafana 連携可能。

---

## 6. 通知連携

### 6.1 通知チャンネル

```yaml
notifications:
  channels:
    slack:
      enabled: true
      webhook_url_env: "AIRFLOW_SLACK_WEBHOOK"
      default_channel: "#airflow-alerts"
    email:
      enabled: false
      smtp_host: "..."
    cli:
      enabled: true
      bell_on_critical: true
```

### 6.2 通知ルール

| イベント | チャンネル | 内容 |
|---------|---------|------|
| `needs_human` 遷移 | Slack | タスクリンク + handoff_note |
| `critical` ログ | Slack + email | フルコンテキスト |
| `scheduler.starvation_force` | Slack | タスク ID + 待機時間 |
| `gpu.oom` | Slack | 関連モデル + 直近のタスク |
| 監査ログ改ざん検出 | Slack + email | ハッシュチェーン破断点 |
| 夜間バッチ完了 | Slack | サマリ（成功率、所要時間） |

### 6.3 メッセージテンプレート

```
:warning: *Escalation*: task_abc123 needs human review
*Title*: 契約書 §3.2 損害賠償条項のレビュー
*Section*: business
*Agent*: legal-review
*Reason*: 判例適合性の判断が必要（信頼度 0.42）
*Link*: http://localhost:7400/tasks/task_abc123
```

---

## 7. 夜間バッチスケジューラ

### 7.1 スケジュール定義

```yaml
# .airflow/config/schedules.yaml
schedules:
  - name: nightly_qa
    cron: "0 2 * * *"
    description: "毎晩 02:00 から QA エージェントが起動"
    actions:
      - dispatch:
          tag: "nightly_qa"
          targets:
            - agent_qa
          tasks:
            - type: "run_test_suite"
              repos: ["repo_main"]

  - name: security_scan
    cron: "30 2 * * *"
    actions:
      - dispatch:
          targets: [agent_security]
          tasks: [type: "scan_dependencies"]

  - name: eng_director_briefing
    cron: "0 9 * * 1"   # 毎週月曜 09:00
    actions:
      - dispatch:
          targets: [agent_eng_director]
          tasks: [type: "weekly_engineering_review"]

  - name: morning_brainstorm
    cron: "0 7 * * *"
    actions:
      - dispatch:
          targets: [agent_morning_brainstorm]
          tasks: [type: "generate_ideas"]

  - name: business_briefing
    cron: "0 9 * * 2,5"   # 火・金 09:00
    actions:
      - dispatch:
          targets: [agent_marketing_director, agent_business_strategy]
          tasks: [type: "weekly_brief"]

  - name: cleaner
    cron: "0 3 * * *"
    actions:
      - cleaner: { mode: "scan_and_dedupe" }

  - name: audit_hash
    cron: "0 3 * * *"
    actions:
      - audit: { mode: "chain_hash" }
```

### 7.2 実行ランナー

`airflow-scheduler.service`（systemd）：

- cron 形式を内蔵パーサで処理
- 各実行で audit_id 採番、ログ記録
- 失敗時のリトライ（DDD-03 ポリシー準拠）

---

## 8. 24 時間連続運用の検証

### 8.1 検証シナリオ

```
[0:00] 全システム起動
[2:00] 夜間 QA 起動、§01 大量タスク投入（50 件）
[2:30] security scan 起動（並行 30 件）
       → スケジューラがプロファイル切替（14b-plus-8b → coder-plus-emb）
[3:00] cleaner 起動 + audit hash 計算
[7:00] morning brainstorm → §02 タスク 3 件生成
       → プロファイル 14b-plus-8b へ復帰
[9:00] 火曜なら business briefing
[終日] 通常ワークロード（人間からの随時タスク投入）
[18:00] 日中タスク収束
[翌 0:00] 24 時間サマリレポート
```

### 8.2 合格基準

- [ ] 24 時間中の `critical` ログが 0 件
- [ ] needs_human 遷移が想定範囲内（< 10 件/日）
- [ ] スターベーション `force_run` 発生が 0 件
- [ ] 全 100+ 件のタスクが done または needs_human に到達
- [ ] GPU OOM 発生 0 件
- [ ] 監査ハッシュチェーンが連続

### 8.3 メモリリーク監視

```bash
# 24 時間後の差分
airflow ops memcheck --baseline-file baseline.json
# RSS, VRAM, SQLite サイズ, ログサイズの伸び率を確認
```

---

## 9. 障害対応プレイブック

### 9.1 needs_human 対応

```
1. Slack 通知受信
2. /tasks/<id> を確認
3. handoff_notes 末尾の escalation 理由を読む
4. 関連ノート（references）を参照
5. 決定:
   a. 承認 → handoff_note 追加 + transition(done)
   b. 差戻し → 元エージェントに transition + reason
   c. 仕様変更 → 別タスク生成 + cancel
```

### 9.2 スターベーション発生時

```
1. critical アラート受信
2. airflow scheduler dashboard で原因確認
3. 緊急対応:
   a. 短期: airflow scheduler degrade --on でスループット優先
   b. 中期: プロファイル手動切替
4. 根本対応:
   - キュー深度トレンド分析
   - エージェントの preferred_models 見直し
```

### 9.3 GPU OOM 発生時

```
1. critical アラート受信
2. airflow gpu status で現状確認
3. 影響タスク（blocked）一覧確認
4. 復旧:
   a. ollama restart
   b. airflow gpu reset
   c. blocked タスクを inbox に戻す
5. 原因究明:
   - そのモデル + コンテキスト長の組合せをレジストリで調整
   - vram_gb 設定値の見直し
```

### 9.4 監査ハッシュチェーン破断

```
1. critical アラート受信（最重要）
2. システム凍結（新タスク受付停止）: airflow lockdown
3. 破断点前後のログを保全（読み取り専用 mount）
4. インシデント調査:
   - 改ざんなのか、システム障害なのか
   - 改ざんなら影響範囲特定
5. 復旧:
   - 改ざんなし: 破断点以降を再計算
   - 改ざんあり: 監査要件に従い対応
```

---

## 10. パフォーマンス目標と本番運用基準

### 10.1 本番運用 SLA

| 指標 | 目標 | 測定窓 |
|------|-----|------|
| タスク成功率（success=true） | ≥ 95% | 24 時間 |
| 平均 auto_score（high accuracy） | ≥ 0.85 | 24 時間 |
| p95 タスク完了時間（interactive） | ≤ 3 秒 | 24 時間 |
| 夜間バッチ完了率 | 100% | 各実行 |
| needs_human 件数 | < 10 件/日 | 24 時間 |
| critical ログ件数 | 0 件 | 24 時間 |
| 24 時間連続稼働 | 99.5% | 1 か月 |

### 10.2 キャパシティ計画

```
RTX 4060 Ti 16GB 1 枚で:
  - 夜間バッチ: 14b で 100 タスク/夜（3 時間で完了）
  - 日中: 50 タスク/日
  - 月間処理可能タスク: 約 4500

これを超える場合:
  - GPU 増設（24GB 機）
  - クラウド API 併用（OpenRouter / Claude API のハイブリッド）
```

---

## 11. 検証チェックリスト

Phase 5 完了時 = **本番運用可能**：

- [ ] §01〜§03 全エージェントの `skills.md` 作成
- [ ] 4 段レビュー（§02）が end-to-end で動作
- [ ] PR レビュー並列集約（§01）が `tasks.join` で動作
- [ ] JWT 認証で全 MCP サーバーが動く
- [ ] レート制限が超過時に正しくエラー返却
- [ ] 機密ノートが許可エージェントのみアクセス可
- [ ] 監査ハッシュチェーン日次バッチが動作
- [ ] Slack 通知が needs_human/critical で発火
- [ ] systemd で全サービスが管理されている
- [ ] 夜間バッチが 4 種類すべて正常実行
- [ ] §8 の 24 時間連続運用シナリオで合格基準達成
- [ ] §9 のプレイブックがリハーサル済み
- [ ] §10 SLA を 7 日間連続で達成

---

## 12. オープン課題（本プロジェクト範囲外）

> **用語**：本プロジェクトの **Phase 5 ＝ 10 週で実装完了する範囲**。
> 「v2.0」は **Phase 5 完了後の次期リリース**（マルチホスト・大規模展開等）を指す。
> 両者は **異なる時期** を意味し、混同しない。

| 課題 | 検討時期 |
|------|--------|
| マルチホスト分散運用 | v2.0 |
| Web ダッシュボードのフル機能化 | v2.0 |
| クラウド API ハイブリッド運用 | v2.0 |
| ノートからの要件定義生成（要件 F-L29〜L31） | v2.0（`airflow generate requirements --from <vault>` を予定） |
| F-M15 ハルシネーション簡易検出 | v2.0（DDD-06 §5 のフィードバック源として組込） |
| F-P26/P27 タスク中断・チェックポイント | v2.0（DDD-07 §16 で先送り済み） |
| 多言語対応（en/zh のエージェント） | 必要時 |
| エージェント間メモリ共有（ベクトル化） | v2.0 |
| 自動 A/B テスト（モデル比較） | v2.0 |
| プロアクティブ提案エージェント | v2.0 |

---

## 13. 全 DDD 完了サマリ

| Phase | DDD | 内容 | 行数 |
|-------|-----|------|-----|
| 1 | DDD-01 | 共通データスキーマ | 538 |
| 1 | DDD-02 | MCP インターフェース | 584 |
| 1 | DDD-03 | エラー処理・ログ設計 | 444 |
| 2 | DDD-04 | タスクボード+Vault Ingestion | 605 |
| 2 | DDD-05 | ローカル LLM 実行基盤 | 586 |
| 3 | DDD-06 | モデル動的選択+精度補完 | 645 |
| 4 | DDD-07 | 並列スケジューラ | 693 |
| 5 | **DDD-08** | **チーム拡張+本番化** | **本文書** |

**累計 4,000+ 行（概算）の詳細設計**で、概要要件 → 動くシステムへの設計を完了。
（注：各 DDD の行数はレビュー修正により変動する。最新は `wc -l docs/design/ddd-*.md` で確認）

---

## 14. 実装着手準備

### 14.1 推奨スタック

| 層 | 技術 |
|---|------|
| 言語 | Python 3.11+ |
| MCP サーバー | `modelcontextprotocol` SDK |
| HTTP / Web | FastAPI |
| データバリデート | `pydantic` v2 + `jsonschema` |
| SQLite ラッパー | `aiosqlite` |
| ベクトル DB | Qdrant Docker |
| Ollama クライアント | `ollama` Python SDK |
| ファイル監視 | `watchdog` |
| systemd 連携 | unit ファイルテンプレート |
| 通知 | `slack-sdk` |
| テスト | `pytest` + `pytest-asyncio` |

### 14.2 リポジトリ構成案

```
airflow/
├── docs/                 # 既存（要件 + DDD）
├── schemas/              # DDD-01 の JSON Schema 群
├── src/
│   ├── airflow/
│   │   ├── mcp/          # 5 つの MCP サーバー
│   │   │   ├── taskboard.py
│   │   │   ├── notes.py
│   │   │   ├── models.py
│   │   │   ├── gpu.py
│   │   │   └── agents.py
│   │   ├── core/         # スキーマ・エラー・ログ
│   │   ├── runtime/      # Ollama ラッパー、ExecutionLog
│   │   ├── scheduler/    # 並列スケジューラ
│   │   ├── ingester/     # Vault 取り込み
│   │   ├── cli/          # airflow コマンド
│   │   └── web/          # Web ダッシュボード
│   └── agents/           # 26 エージェントの skills.md
├── tests/
├── deploy/
│   └── systemd/          # unit ファイル
└── pyproject.toml
```

### 14.3 実装フェーズ（再掲）

| フェーズ | 期間 | DDD 範囲 |
|--------|-----|---------|
| Phase 1 実装 | 2 週 | DDD-01〜03 |
| Phase 2 実装（MVP） | 2 週 | DDD-04〜05 |
| Phase 3 実装 | 2 週 | DDD-06 |
| Phase 4 実装 | 2 週 | DDD-07 |
| Phase 5 実装 | 2 週 | DDD-08 |

→ **10 週後に本番運用可能**な状態。

---

*詳細設計フェーズ完了。次は Phase 1 実装に着手可能。*
