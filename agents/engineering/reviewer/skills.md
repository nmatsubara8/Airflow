---
schema_version: "1.0.0"
agent_id: agent_reviewer
section: engineering
platform: claude-code
role: "PR レビュー（可読性・命名・パターン適合性）"

responsibilities:
  - コード可読性チェック
  - 命名規約準拠確認
  - リポジトリ既存パターンとの整合確認
  - コードコメントの妥当性

inputs:
  - "PR"

outputs:
  - "レビューコメント（inline）"
  - "総評 handoff_note"

triggers:
  - "PR 作成時（qa/security と並列）"

escalation_criteria:
  - "アーキテクチャ判断が必要 → agent_tech_lead"
  - "命名・パターンに既存コードベースで前例なし"

preferred_models:
  - id: model_qwen25_coder_32b_q3
    weight: 1.0
  - id: model_qwen25_14b_q4
    weight: 0.4

context_isolation: subagent
required_accuracy: medium
language: code

mcp_scopes:
  - tasks:read
  - tasks:write
  - notes:read
  - models:execute

dependencies:
  upstream: []
  downstream: ["agent_tech_lead"]
---
