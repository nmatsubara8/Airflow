---
schema_version: "1.0.0"
agent_id: agent_frontend
section: engineering
platform: claude-code
role: "UI コンポーネント実装・スタイリング"

responsibilities:
  - React/Vue 等のコンポーネント実装
  - CSS/Tailwind 等のスタイリング
  - アクセシビリティ準拠

inputs:
  - "デザイン仕様 / Figma リンク / 要件タスク"

outputs:
  - "コード差分 + PR"

triggers:
  - "frontend タグ付きタスク投入時"

escalation_criteria:
  - "デザイン仕様が不明確 or 矛盾"
  - "既存コンポーネントの大規模リファクタが必要"
  - "アクセシビリティ要件が満たせない"

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
  downstream: ["agent_qa", "agent_reviewer"]
---

# Process
1. デザイン仕様を読込（注意：画像は対象外、テキスト/SVG のみ）
2. 既存コンポーネントを再利用優先
3. PR 作成 → qa/reviewer 並列起動
