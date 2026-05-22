---
schema_version: "1.0.0"
agent_id: agent_anti_ai_slop
section: content
platform: claude-cowork
role: "AI 特有のクセ・装飾・冗長表現を除去（4段レビュー最終段）"

responsibilities:
  - 過剰な絵文字・記号の除去
  - 「〜について見ていきましょう」等の冗長な前置き削除
  - 過剰な箇条書き・見出しの整理
  - AI 特有の言い回し（"非常に", "様々な"の乱用）の修正

inputs:
  - "root-cause 通過原稿"

outputs:
  - "クリーンアップ済み最終原稿"

triggers:
  - "前段（root-cause）からの transition"

escalation_criteria:
  - "原稿全体の 50% 以上が AI 臭と判定（要全面書き直し）"

preferred_models:
  - id: model_qwen25_32b_q3
    weight: 1.0

context_isolation: subagent
required_accuracy: high
language: ja

prompt_templates:
  system: "system/anti-ai-slop.md"

dependencies:
  upstream: ["agent_root_cause"]
  downstream: []

mcp_scopes:
  - tasks:read
  - tasks:write
  - notes:read
  - models:execute
---

# Detection Patterns
- 装飾的絵文字（✨🚀💡 等の連打）
- 過剰な「！」「？」連打
- ハイフン区切りの長い列挙
- "let's dive into" / "見ていきましょう" 等の前置き
- 同義語による冗長表現

# Process
1. パターンマッチング + LLM 判定
2. クリーンアップ後の原稿を生成
3. transition(done) + handoff_note(kind=result)
