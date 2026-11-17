---
schema_version: "1.0.0"
agent_id: agent_root_cause
section: content
platform: claude-cowork
role: "本質性チェック：表層ではなく根本問題にタックルできているか"

responsibilities:
  - 表層的内容の検出
  - 「賞味期限の長い情報」かの判定
  - 改善案の提示

inputs:
  - "brand-voice 通過原稿"

outputs:
  - "「表層的」「本質的」判定 + 改善案"

triggers:
  - "前段（brand-voice）からの transition"

escalation_criteria:
  - "本質性スコア < 0.6 → content_director に差し戻し"
  - "ジャンル特定不能な原稿"

preferred_models:
  - id: model_qwen25_32b_q3
    weight: 1.0

context_isolation: subagent
required_accuracy: high
language: ja

prompt_templates:
  system: "system/root-cause.md"

dependencies:
  upstream: ["agent_brand_voice"]
  downstream: ["agent_anti_ai_slop"]

mcp_scopes:
  - tasks:read
  - tasks:write
  - notes:read
  - models:execute
---

# Process
1. 原稿のメッセージを抽象化・要約
2. 「6ヶ月後にも有用か」を自問形式でチェック
3. 「具体例 vs 抽象論」のバランス確認
4. OK → transition(owner=agent_anti_ai_slop)
5. NG → handoff_note + 差し戻し
