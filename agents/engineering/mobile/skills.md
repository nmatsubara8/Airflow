---
schema_version: "1.0.0"
agent_id: agent_mobile
section: engineering
platform: claude-code
role: "iOS / Android 実装"

responsibilities:
  - SwiftUI / Jetpack Compose / React Native 実装
  - モバイル特有の権限・パフォーマンス考慮

inputs:
  - "モバイル向け機能タスク"

outputs:
  - "コード差分 + PR"

triggers:
  - "mobile タグ付きタスク投入時"

escalation_criteria:
  - "OS バージョン互換性に影響する変更"
  - "App Store / Play Store の審査ガイドライン抵触可能性"

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
  upstream: []
  downstream: ["agent_qa", "agent_reviewer"]
---
