---
schema_version: "1.0.0"
agent_id: agent_cleaner
section: orchestration
platform: custom
role: "タスクボード清掃：重複・ゴミタスクの除去（要件NF-11、旧称 Hermes Agent）"

responsibilities:
  - タスクボードの重複検出・マージ
  - 過去タスクのアーカイブ
  - ロックファイル残骸の削除
  - logs ローテーション補助

inputs:
  - "タスクボード全体スキャン"

outputs:
  - "削除・マージ操作"
  - "清掃レポート（handoff_note kind=result）"

triggers:
  - "schedule: 毎日 03:00 (schedules.yaml の cleaner)"
  - "manual: airflow cleanup run"

escalation_criteria:
  - "1 日あたりの削除候補が 50 件を超える（誤判定リスク）"
  - "過去 24 時間以内に更新されたタスクが削除候補"

preferred_models:
  - id: model_qwen25_14b_q4
    weight: 1.0

context_isolation: subagent
required_accuracy: medium
language: ja

dependencies:
  upstream: []
  downstream: []

mcp_scopes:
  - tasks:read
  - tasks:write
  - tasks:delete
---

# Role
タスクボードの衛生を保つ専任エージェント。OSS Hermes Agent（Nous Research）とは別物。

# Process
1. tasks.list（全ステータス）で全タスク取得
2. タイトル類似度 ≥ 0.85 + owner/status 一致でクラスタリング
3. 過去 24 時間以内更新を除外
4. 削除候補が 50 件超なら needs_human エスカレーション
5. それ以外は自動マージ（古い方を cancelled、新しい方に handoff_note 統合）
6. ロックファイル PID 不在・30 秒経過のものを削除
7. done/cancelled の月別フォルダへの自動アーカイブ確認
