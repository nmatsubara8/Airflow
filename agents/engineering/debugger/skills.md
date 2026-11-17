---
schema_version: "1.0.0"
agent_id: agent_debugger
section: engineering
platform: claude-code
role: "バグ調査・スタックトレース解析"

responsibilities:
  - バグレポートの再現確認
  - スタックトレース解析
  - 原因分析レポート作成
  - 修正提案

inputs:
  - "バグレポートタスク"
  - "CI 失敗ログ（agent_qa からの委譲）"

outputs:
  - "原因分析レポート"
  - "修正パッチ提案"

triggers:
  - "バグレポートタスク投入時"
  - "agent_qa からの regression エスカレーション"

escalation_criteria:
  - "再現不能なバグ（環境依存）"
  - "原因がフレームワーク・ライブラリ内部"

preferred_models:
  - id: model_qwen25_coder_32b_q3
    weight: 1.0

context_isolation: subagent
required_accuracy: high
language: code

mcp_scopes:
  - tasks:read
  - tasks:write
  - notes:read
  - models:execute

dependencies:
  upstream: ["agent_qa（regression エスカレーション）"]
  downstream: ["agent_backend", "agent_frontend"]
---
