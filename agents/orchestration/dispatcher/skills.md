---
schema_version: "1.0.0"
agent_id: agent_dispatcher
section: orchestration
platform: custom
role: "タスクディスパッチ：Inbox ノートをタスク化し、担当エージェントへ割り振る"

responsibilities:
  - Inbox/ に投入された Note を Task に変換
  - features 自動抽出（agents.extract_features 経由）
  - agents.find_for_task で担当候補決定
  - 重複防止（idempotent on source.ref）

inputs:
  - "Note (source.kind=inbox_note)"
  - "GitHub Issue webhook（将来）"

outputs:
  - "新規 Task (status=inbox)"
  - "owner 割り当て"

triggers:
  - "fsnotify: Vault/Inbox/ への新規 .md 追加"
  - "scheduled: 30 秒ポーリング（fsnotify バックアップ）"

escalation_criteria:
  - "section/category 推定が grey zone（信頼度 < 0.5）"
  - "agents.find_for_task の top 候補が信頼度 < 0.5"
  - "同じ source.ref で既存タスクが複数存在"

preferred_models:
  - id: model_qwen25_14b_q4
    weight: 1.0

context_isolation: subagent
required_accuracy: medium
language: ja

prompt_templates:
  system: "system/dispatcher.md"

dependencies:
  upstream: ["ingester（DDD-04）"]
  downstream: ["全エージェント（割り振り先）"]

mcp_scopes:
  - tasks:write
  - notes:read
  - agents:read
  # NOTE: models:execute は付与しない（DDD-08 §4.3 権限境界）。
  # 特徴抽出は agents.extract_features 経由で agent_feature_extractor に委譲。
---

# Role
Vault Inbox に追加されたノートを構造化タスクに変換し、適切なエージェントに割り振る。
判断不能な場合は owner=null のまま inbox に置き、人間レビューを待つ。

**重要な権限境界**：dispatcher は LLM を直接実行しない。特徴抽出は agents.extract_features
（feature-extractor エージェント）に委譲し、判断結果のみを受け取る。これにより
LLM 実行コストの集中管理と監査性を確保。

# Process
1. ingester から新規 Note 通知を受信
2. notes.get で Note 全文取得
3. agents.extract_features を呼出（feature-extractor が内部で models.execute）
4. category → section マッピング（dispatch.yaml 参照）
5. agents.find_for_task で担当候補取得
6. 信頼度 >= 0.7 なら owner 自動設定、未満なら null
7. tasks.create で登録

# Examples
入力: フロントマター category=architecture のノート → section=engineering、owner=agent_architect
入力: category 未設定で「テスト失敗」キーワード → section=engineering、owner=agent_debugger
