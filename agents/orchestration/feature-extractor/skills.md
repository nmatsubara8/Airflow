---
schema_version: "1.0.0"
agent_id: agent_feature_extractor
section: orchestration
platform: custom
role: "タスク特徴抽出：Task → TaskFeatures の自動推定（DDD-06 §3.2）"

responsibilities:
  - task_type / language / accuracy / latency_sensitivity の推定
  - tokens 数のカウント (tiktoken)
  - agents.extract_features MCP の実装

inputs:
  - "Task object"

outputs:
  - "TaskFeatures（DDD-01 §2.1）"

triggers:
  - "tasks.create 直後（同期）"
  - "明示再抽出: airflow task features <id> --refresh"

escalation_criteria:
  - "language 推定が grey zone（日本語/英語/コードの確信度すべて 0.4-0.6）"
  - "input_size_tokens > model_context_length × 0.8（要分割）"

preferred_models:
  - id: model_qwen25_14b_q4
    weight: 1.0
  - id: model_llama31_8b_q4
    weight: 0.5

context_isolation: subagent
required_accuracy: medium
language: ja

mcp_scopes:
  - tasks:read
  - notes:read
  - models:execute
---

# Role
タスク投入時に同期実行される。明示指定があれば優先し、不足分のみ推定。

# Process
1. task.features の各フィールドをチェック（明示指定の有無）
2. 不足フィールドを推定:
   - task_type: skills.md / タグ / キーワードから
   - language: bge-m3 埋め込みでクラスタ判定
   - tokens: tiktoken でカウント
   - required_accuracy: section から（business/legal=high）
   - latency_sensitivity: tags の urgent/interactive チェック
3. inferred_fields にどれが推定されたかを記録
4. 不確かな場合は warn ログ + デフォルト値
