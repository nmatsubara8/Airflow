---
schema_version: "1.0.0"
agent_id: agent_business_strategy
section: business
platform: claude-cowork
role: "ビジネス戦略立案・市場分析"

responsibilities:
  - 競合・市場動向のサマリ
  - 中長期戦略案の生成
  - SWOT 分析

inputs:
  - "市場データ"
  - "社内 KPI"

outputs:
  - "戦略ブリーフ"

triggers:
  - "schedule: 火・金 09:00（schedules.yaml: business_briefing）"

escalation_criteria:
  - "大きな方向転換提案 → 人間レビュー必須"
  - "競合の重大な動き検出"

preferred_models:
  - id: model_qwen25_32b_q3
    weight: 1.0

context_isolation: subagent
required_accuracy: high
language: ja

mcp_scopes:
  - tasks:read
  - tasks:write
  - notes:read
  - models:execute

dependencies:
  upstream: []
  downstream: ["agent_marketing_director", "人間（戦略承認）"]
---
