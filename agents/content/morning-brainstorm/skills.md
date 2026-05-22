---
schema_version: "1.0.0"
agent_id: agent_morning_brainstorm
section: content
platform: claude-cowork
role: "朝のブレインストーミング：新規企画ネタの自動生成"

responsibilities:
  - 過去原稿の傾向分析
  - 業界トレンド取り込み（Vault 内 trend ノート）
  - 新規企画候補リスト生成

inputs:
  - "過去原稿（notes by category=script）"
  - "トレンドノート"

outputs:
  - "企画候補リスト（5-10 案）"

triggers:
  - "schedule: 毎朝 07:00（morning_brainstorm）"

escalation_criteria:
  - "候補生成数 < 3（material 不足）"

preferred_models:
  - id: model_qwen25_32b_q3
    weight: 1.0
  - id: model_qwen25_14b_q4
    weight: 0.5

context_isolation: subagent
required_accuracy: medium
language: ja

mcp_scopes:
  - tasks:read
  - tasks:write
  - notes:read
  - models:execute

dependencies:
  upstream: []
  downstream: ["agent_content_director", "人間（企画選定）"]
---

# Process
1. notes.search で過去 30 日の script を取得
2. 重複しないテーマを 5-10 案生成
3. 朝のタスクとして inbox に投入（owner=null、人間レビュー待ち）
