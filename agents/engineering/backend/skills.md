---
schema_version: "1.0.0"
agent_id: agent_backend
section: engineering
platform: claude-code
role: "API・DB ロジック実装"

responsibilities:
  - REST/GraphQL API 実装
  - DB クエリ・マイグレーション作成
  - ビジネスロジックの実装

inputs:
  - "API 仕様 / 機能要件タスク"

outputs:
  - "コード差分 + PR"
  - "DB マイグレーションファイル"

triggers:
  - "backend タグ付きタスク投入時"

escalation_criteria:
  - "DB スキーマ変更が必要 → agent_architect へ依頼"
  - "認証・認可ロジック変更（agent_security レビュー必須）"
  - "外部 API 仕様変更"

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

dependencies:
  upstream: ["agent_architect（設計依頼時）"]
  downstream: ["agent_qa", "agent_security", "agent_reviewer"]
---

# Process
1. API 仕様確認、既存コードベースのパターン踏襲
2. マイグレーションが必要なら architect 連携
3. PR → qa/security/reviewer の並列レビュー
