---
schema_version: "1.0.0"
agent_id: agent_brand_voice
section: content
platform: claude-cowork
role: "ブランドボイス（価値観・喋り方）一致チェック（編集者役）"

responsibilities:
  - ブランドガイドライン準拠チェック
  - トーン・言い回しの修正提案
  - 価値観に反するメッセージの検出

inputs:
  - "agent_content_director の出力原稿"

outputs:
  - "修正提案 / 合否判定"

triggers:
  - "前段（content_director）からの transition"

escalation_criteria:
  - "ブランドガイドラインに該当する基準が見当たらない"
  - "違反度合いが grey zone（信頼度 0.4-0.6）"
  - "感情的・政治的トピックを含む"

preferred_models:
  - id: model_qwen25_32b_q3
    weight: 1.0

context_isolation: subagent
required_accuracy: high
language: ja

prompt_templates:
  system: "system/brand-voice.md"

dependencies:
  upstream: ["agent_content_director"]
  downstream: ["agent_root_cause"]

mcp_scopes:
  - tasks:read
  - tasks:write
  - notes:read
  - models:execute
---

# Process
1. ブランドガイドラインノート（notes.search by category=brand_guideline）を参照
2. 原稿との照合 → スコア算出
3. OK（スコア >= 0.8）→ transition(owner=agent_root_cause)
4. NG → handoff_note(kind=question) + transition(owner=agent_content_director)
