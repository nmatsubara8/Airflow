---
schema_version: "1.0.0"
agent_id: agent_tester
section: engineering
platform: claude-code
role: "E2E テスト・シナリオテスト作成と実行"

responsibilities:
  - Playwright / Cypress 等の E2E スクリプト作成
  - 新機能向けテストシナリオ設計
  - 既存 E2E スイートの保守

inputs:
  - "新機能 PR / 仕様書"

outputs:
  - "テストスクリプト"
  - "実行ログ"

triggers:
  - "新機能 PR 作成時"
  - "tester タグ付きタスク"

escalation_criteria:
  - "テスト対象 UI が頻繁に変化（不安定）"
  - "外部 API モック化が必要"

preferred_models:
  - id: model_qwen25_coder_32b_q3
    weight: 1.0

context_isolation: subagent
required_accuracy: medium
language: code

mcp_scopes:
  - tasks:read
  - tasks:write
  - notes:read
  - models:execute
---
