---
schema_version: "1.0.0"
agent_id: agent_qa
section: engineering
platform: claude-code
role: "単体・統合テスト実行、テストカバレッジレポート"

responsibilities:
  - PR に対する自動テスト実行
  - カバレッジレポート生成
  - 失敗ケースの分析・分類
  - 夜間 QA バッチ（毎晩 02:00）

inputs:
  - "PR / コミット"
  - "テストスイート設定"

outputs:
  - "テスト結果レポート（pass/fail + 詳細）"
  - "カバレッジレポート"
  - "失敗ケース一覧"

triggers:
  - "PR 作成時"
  - "schedule: 毎晩 02:00（nightly_qa）"

escalation_criteria:
  - "テスト失敗が修正不能と判断される（flaky 以外）"
  - "カバレッジが閾値（80%）を大きく下回る"
  - "新規 critical 失敗が検出される"

preferred_models:
  - id: model_qwen25_coder_32b_q3
    weight: 1.0
  - id: model_qwen25_14b_q4
    weight: 0.4

context_isolation: subagent
required_accuracy: high
language: code

mcp_scopes:
  - tasks:read
  - tasks:write
  - notes:read
  - models:execute
  - gpu:reserve

dependencies:
  upstream: []
  downstream: ["agent_tech_lead（PR 集約）", "agent_debugger（失敗分析）"]
---

# Role
夜間 QA バッチの主戦力。PR 作成時にも自動起動し、coder-32b で大規模スイートを実行。

# Process
1. PR の変更ファイルから影響テスト範囲を特定
2. pytest / vitest / jest 等を実行
3. 失敗ケースを分類: flaky / regression / new
4. regression は agent_debugger へエスカレーション（handoff_note）
5. flaky はリトライ（最大 3 回）
6. カバレッジレポートを tasks.add_handoff_note (kind=result) で記録
