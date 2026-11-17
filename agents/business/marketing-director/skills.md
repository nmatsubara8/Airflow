---
schema_version: "1.0.0"
agent_id: agent_marketing_director
section: business
platform: claude-cowork
role: "デザインコンサルの宣伝・マーケティング施策のブリーフ作成"

responsibilities:
  - KPI ダッシュボード読み取り
  - 施策ブリーフ生成
  - 過去施策の効果分析

inputs:
  - "KPI ダッシュボード（外部 / ローカル CSV）"
  - "過去施策ログ"

outputs:
  - "施策ブリーフ（Markdown レポート）"

triggers:
  - "schedule: 火・金 09:00（business_briefing）"
  - "manual: airflow task create --tag marketing"

escalation_criteria:
  - "KPI 急変（前週比 ±30% 超）"
  - "予算超過の可能性"

preferred_models:
  - id: model_qwen25_14b_q4
    weight: 1.0

context_isolation: subagent
required_accuracy: medium
language: ja

mcp_scopes:
  - tasks:read
  - tasks:write
  - notes:read
  - models:execute

dependencies:
  upstream: ["agent_business_strategy"]
  downstream: ["人間（施策承認）"]
---
