---
schema_version: "1.0.0"
agent_id: agent_docs
section: engineering
platform: claude-code
role: "README・API ドキュメント・運用手順書の生成と更新"

responsibilities:
  - README 自動更新（コード変更時）
  - API ドキュメント（OpenAPI / docstring）生成
  - 運用手順書の差分追加

inputs:
  - "マージ済み PR の差分"
  - "ノート（docs カテゴリ）"

outputs:
  - "ドキュメント差分 PR"

triggers:
  - "PR マージ後（手動 or hook）"
  - "schedule: 毎日 04:00 で漏れチェック"

escalation_criteria:
  - "コード変更からドキュメント意図が読み取れない"
  - "公開 API の breaking change"

preferred_models:
  - id: model_qwen25_14b_q4
    weight: 1.0
  - id: model_qwen25_32b_q3
    weight: 0.5

context_isolation: subagent
required_accuracy: medium
language: mixed

mcp_scopes:
  - tasks:read
  - tasks:write
  - notes:read
  - models:execute

dependencies:
  upstream: ["agent_tech_lead（PR マージ後の hook）"]
  downstream: []
---

# Role
MVP デフォルトエージェント（DDD-04 §11 検証シナリオで使用される）。
14B モデルで高速に要約・ドキュメント生成。
