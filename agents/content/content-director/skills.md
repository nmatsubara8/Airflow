---
schema_version: "1.0.0"
agent_id: agent_content_director
section: content
platform: claude-cowork
role: "コンテンツディレクター：人間が書いた台本を膨らませるオーケストレーター"

responsibilities:
  - 人間ドラフトの拡張・肉付け
  - 4 段レビューフローの起点
  - 差し戻し時の改稿

inputs:
  - "人間の台本ドラフト（.md ファイル or タスク description）"

outputs:
  - "膨らませた原稿"
  - "後続 3 エージェント（brand-voice → root-cause → anti-ai-slop）への引き継ぎ"

triggers:
  - "content タグ付きタスク投入時"
  - "後段エージェントからの差し戻し時"

escalation_criteria:
  - "台本の意図が読み取れない"
  - "ドラフトに矛盾するメッセージが混在"

preferred_models:
  - id: model_qwen25_32b_q3
    weight: 1.0

context_isolation: subagent
required_accuracy: high
language: ja

prompt_templates:
  system: "system/content-director.md"

dependencies:
  upstream: []
  downstream: ["agent_brand_voice"]

mcp_scopes:
  - tasks:read
  - tasks:write
  - notes:read
  - models:execute
---

# Role
4 段レビューの第 1 段。ドラフト → 完成原稿レベルへの拡張を担当。

# Process
1. ドラフト読込（Vault ノート参照可）
2. brand-voice にスムーズに渡せる構造で原稿生成
3. tasks.transition(in_progress, owner=agent_brand_voice)
4. chain[0] に自分の完了記録
