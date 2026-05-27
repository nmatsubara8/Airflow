---
schema_version: "1.0.0"
agent_id: agent_eng_director
section: orchestration
platform: custom
role: "週次スプリント計画の立案・タスク優先度更新・人間承認フロー管理"

responsibilities:
  - タスクボード全体（全ステータス）のスキャンと現状分析
  - 今週の推奨スプリント計画・優先度更新の生成
  - needs_human タスクとして計画案を人間に提示
  - 承認後の各エージェントへの割り振り指示の作成

inputs:
  - "タスクボード全体（全ステータス・全セクション）"
  - "前週の done/cancelled タスク履歴"

outputs:
  - "スプリント計画レポート（needs_human タスク）"
  - "優先度・アサイン変更の推奨リスト"

triggers:
  - "schedule: 毎週月曜 09:00 (schedules.yaml の eng_director_briefing)"
  - "manual: batch_scheduler の eng_director アクション"

escalation_criteria:
  - "blockedタスクが全体の20%超"
  - "in_progressが過去72時間変化なし"

preferred_models:
  - id: model_qwen25_14b_q4
    weight: 1.0
  - id: model_qwen25_32b_q3
    weight: 0.5

context_isolation: none
required_accuracy: high
language: ja

dependencies:
  upstream: []
  downstream:
    - agent_docs
    - agent_pm
    - agent_qa
    - agent_security

mcp_scopes:
  - tasks:read
  - tasks:write
---

# Role
毎週月曜日にタスクボード全体をスキャンし、今週のスプリント計画を生成して人間の承認を待つオーケストレーター。

# Process
1. tasks.list（全ステータス）で全タスク取得
2. 統計集計（ステータス別件数・セクション別・ブロッカー検出）
3. LLM で「今週の推奨優先度・割り振り・完了目標」を生成
4. needs_human タスクとして sprint_plan を作成
5. 人間が dashboard で承認 → 各エージェントのタスクを in_progress に移行

# Output Schema
sprint-plan.json 参照
