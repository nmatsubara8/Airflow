# テスト戦略・品質保証計画

> 各 DDD に分散しているテスト記述を集約。単体／統合／負荷／カオステスト、CI 設定、カバレッジ目標、フェーズ別検証シナリオを一元管理。
>
> **正本ポリシー**：個別の検証項目・パフォーマンス目標値は **各 DDD §「検証チェックリスト」「パフォーマンス目標」が正本**。本書は実行容易性のために横断集約したコピー。両者で値が異なる場合は DDD 側を信頼する。

---

## 1. テストピラミッド

```
              ╱╲
             ╱  ╲      E2E (5%)
            ╱----╲     - 24時間連続運用シナリオ
           ╱      ╲    - MVP end-to-end（DDD-04/05）
          ╱        ╲
         ╱----------╲   統合テスト (20%)
        ╱            ╲  - MCP サーバー間連携
       ╱              ╲ - スケジューラ + GPU + LLM
      ╱                ╲
     ╱------------------╲ 単体テスト (75%)
    ╱  Schema validation ╲ - 各モジュール
   ╱  Lock concurrency    ╲- スキーマバリデート
  ╱  KNN calculation       ╲ - 個別ロジック
 ╱──────────────────────────╲
```

カバレッジ目標：**全体 80% 以上、コアモジュール（スケジューラ・LLM ラッパー）90% 以上**。

---

## 2. テスト分類

### 2.1 単体テスト

| 対象 | 主な検証内容 | DDD 参照 |
|------|----------|--------|
| JSON Schema バリデート | 全 7 スキーマ × サンプル JSON 各 3 件以上 | DDD-01 §10 |
| 楽観的ロック | 同時更新衝突の検出、リトライ動作 | DDD-04 §3 |
| Audit ID 採番 | ULID 形式・parent 伝播 | DDD-03 §5 |
| エラー分類 | recoverable/escalate/fatal の昇格ロジック | DDD-03 §3.3 |
| フロントマター解析 | YAML 不正・未知フィールド・confidential フラグ | DDD-04 §5.3 |
| KNN 距離計算 | 重み付きユークリッド、近傍 k=5 抽出 | DDD-06 §4.2 |
| Affinity Score | swap_cost 3 状態、starvation_boost 加算 | DDD-07 §3 |
| LRU 退避順 | pending_tasks > 0 除外、pinned 除外 | DDD-07 §9.2 |
| Aging スコア | 600 秒経過後の priority 加算 | DDD-07 §6.1 |
| 構造化出力検証 | JSON Schema 適合チェック、リトライ修正プロンプト | DDD-05 §6 |
| 二重実行類似度 | 0.95/0.85 閾値判定 | DDD-05 §8 |

### 2.2 統合テスト

| シナリオ | 検証項目 | DDD 参照 |
|---------|--------|--------|
| MVP end-to-end | Vault→ingester→dispatcher→docs→done、30 秒以内 | DDD-04 §11 |
| LLM execute（構造化出力） | model.execute → schema_valid=true、5 秒以内 | DDD-05 §12 |
| 4 段レビュー | content-director → brand-voice → root-cause → anti-ai-slop の連鎖 | DDD-08 §3.1 |
| PR レビュー並列集約 | qa/security/reviewer 並列 → tech-lead 集約（tasks.join） | DDD-08 §3.2 |
| モデル recommend → execute | KNN 推奨 → 実行 → ExecutionLog 記録 | DDD-06 §11 |
| Reroute 発動 | auto_score < 0.7 → fallback_chain で再実行 | DDD-06 §11.3 |
| スターベーション detection | 1 時間放置 → force_run フラグ | DDD-07 §12.3 |
| デグラデーション ヒステリシス | 30秒継続でトリガー、回復で30秒継続で OFF | DDD-07 §12.4 |
| プリロード hit/miss | 投機ロード → 実利用記録 | DDD-07 §7.3 |
| Cleaner Agent | 重複検出 + 24時間以内除外 | DDD-08 §7（schedules）+ agents/orchestration/cleaner/skills.md |

### 2.3 負荷試験

```bash
# DDD-07 §13.3 のシナリオ
airflow test load --tasks 100 --models qwen25_14b_q4,qwen25_32b_q3
```

| 指標 | 目標 |
|------|-----|
| 完了時間 | シリアル想定値の **1/2-1/3** |
| モデルスワップ回数 | 単純実装比 **1/5 以下** |
| p95 レイテンシ（interactive） | **3 秒以内** |
| GPU アイドル率（タスクあり時） | **10% 以下** |
| Starvation force_run 発生率 | **< 0.1%** |
| Preload hit ratio | **60% 以上** |

### 2.4 カオステスト（DDD-07 §13.4 拡張）

| イベント | 注入方法 | 期待動作 |
|---------|--------|--------|
| Ollama プロセス kill | `pkill -9 ollama` | fatal ログ、新タスク受付停止、運用者通知 |
| GPU OOM 強制 | 32B モデル 3 並列要求 | GPU_OOM critical、関連タスク blocked |
| ディスクフル（logs） | `dd if=/dev/zero of=/tmp/fill` | warn ログ、ローテーション加速、停止せず |
| Vault フォルダ削除 | `mv /mnt/d/vaults /mnt/d/vaults.bak` | INGEST_PATH_MISSING fatal、ingester 停止、taskboard 継続 |
| SQLite ロック残留 | プロセス強制終了 + ロックファイル残置 | 起動時クリーンアップで自動削除 |
| ネットワーク遮断（Qdrant） | `iptables -A OUTPUT -p tcp --dport 6333 -j DROP` | recoverable リトライ 5 回、failed gracefully |
| 監査ハッシュチェーン改ざん | `<date>.jsonl` を直接編集 | critical アラート、ロックダウンモード |

### 2.5 セキュリティテスト

| 検証 | 方法 |
|------|------|
| AUTH_TOKEN_MISSING | ヘッダなしリクエスト |
| AUTH_TOKEN_INVALID | 偽造トークン |
| AUTH_SCOPE_INSUFFICIENT | tasks:read のみのトークンで tasks.update |
| AUTH_RATE_LIMITED | 1 分間に 1000 リクエスト |
| Confidential ノートアクセス | 非許可エージェントから notes.get |
| パストラバーサル | `notes.get` に `../../etc/passwd` |
| Vault 外パス監視 | 設定外パスを ingester に登録 |
| JSON 巨大ペイロード | 10MB の handoff_note |
| Path injection | task.title に shell special chars |

---

## 3. フェーズ別検証チェックリスト

### Phase 1（横断基盤、2 週）

- [ ] 7 スキーマファイルが schemas/ に配置
- [ ] schemas/examples/*.json が各 3 件以上、全て jsonschema バリデート pass
- [ ] 5 MCP サーバーがスタブで stdio 起動
- [ ] エラーレスポンスが DDD-03 §2.2 形式に準拠
- [ ] `audit.trace` で階層追跡が動作
- [ ] `airflow logs trace <audit_id>` が動作

### Phase 2（MVP コア、2 週）

- [ ] §2.2 統合テスト「MVP end-to-end」が 30 秒以内
- [ ] §2.2 「LLM execute」が 5 秒以内
- [ ] 楽観的ロック衝突がテストで再現できる
- [ ] フロントマター不正で NOTE_PARSE_ERROR が発生
- [ ] Confidential ノートが notes.search の結果から除外
- [ ] §2.4 カオステスト「Vault 削除」「SQLite ロック残留」が pass

### Phase 3（モデル選択 + 精度補完、2 週）

- [ ] KNN レイテンシ < 100ms
- [ ] Cold Start fallback で preferred_models が使われる
- [ ] ε-greedy が指定確率で別モデル選択
- [ ] required_accuracy=high で二重実行が自動 ON
- [ ] Reroute 発動 → fallback_chain で再実行成功
- [ ] フィードバック登録が KNN 推奨にリアルタイム反映

### Phase 4（並列スケジューラ、2 週）

- [ ] スワップ回数が単純実装比 1/5
- [ ] interactive p95 < 3 秒
- [ ] バッチスループット 2-3 倍
- [ ] aging 10 分超過で priority 加算
- [ ] deadline 5 分前で force_run
- [ ] 1 時間超過で必ず force_run
- [ ] プロファイル切替が空きタイミングで発動
- [ ] デグラデーションヒステリシス動作

### Phase 5（チーム拡張 + 本番化、2 週）

- [ ] 26 エージェントの skills.md 全配置
- [ ] 4 段レビュー end-to-end
- [ ] PR レビュー並列集約（tasks.join）
- [ ] JWT 認証で全 MCP 動作
- [ ] レート制限超過時の `AUTH_RATE_LIMITED`
- [ ] Slack 通知が needs_human / critical で発火
- [ ] 監査ハッシュチェーン日次バッチ動作
- [ ] §3 24 時間連続運用シナリオで合格基準達成

---

## 4. テストハーネス

### 4.1 ディレクトリ構成

```
tests/
├── unit/
│   ├── schemas/
│   ├── taskboard/
│   ├── llm_runtime/
│   ├── scheduler/
│   └── ingester/
├── integration/
│   ├── mvp_e2e/
│   ├── content_4stage/
│   ├── pr_review_join/
│   └── parallel_load/
├── chaos/
│   ├── ollama_kill.sh
│   ├── gpu_oom.py
│   └── vault_delete.sh
├── security/
│   └── auth_negative.py
├── fixtures/
│   ├── vaults/test-vault/
│   ├── tasks/sample-tasks/
│   └── notes/sample-notes/
└── conftest.py
```

### 4.2 推奨ツール

| 用途 | ツール |
|------|------|
| 単体テスト | `pytest`, `pytest-asyncio` |
| カバレッジ | `pytest-cov`, 目標 80%+ |
| Property-based | `hypothesis`（特にスキーマ・距離計算） |
| 統合テスト | docker-compose で Ollama + Qdrant をテスト用に起動 |
| 負荷試験 | `locust` or 自作 `airflow test load` |
| カオス注入 | bash スクリプト + pytest fixture |
| MCP テスト | `mcp` SDK のテストユーティリティ |

### 4.3 fixture 戦略

- **Vault fixture**：13 個の YouTube ノート（セッション前半で確認済み）を `tests/fixtures/vaults/test-vault/` に配置
- **Task fixture**：各ステータス・各セクションのサンプル
- **モデル mock**：実 Ollama 不要な単体テスト用に `MockOllamaClient`
- **GPU mock**：VRAM 状態を擬似的に制御

---

## 5. CI/CD 設定

### 5.1 GitHub Actions

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]
jobs:
  unit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -e ".[dev]"
      - run: pytest tests/unit -v --cov=airflow --cov-report=xml
      - uses: codecov/codecov-action@v4

  schema-validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install jsonschema
      - run: python -m airflow.test.schema_validate schemas/ schemas/examples/

  integration:
    runs-on: ubuntu-latest
    services:
      qdrant:
        image: qdrant/qdrant:latest
        ports: [6333:6333]
    steps:
      - uses: actions/checkout@v4
      - run: ./tests/integration/setup_ollama_mock.sh
      - run: pytest tests/integration -v

  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install ruff mypy
      - run: ruff check .
      - run: mypy src/
```

### 5.2 ローカル開発フロー

```bash
make test         # unit + integration
make test-unit    # 高速、unit のみ
make test-chaos   # 別ターゲット（本番影響あり）
make lint
make coverage     # HTML レポート生成
```

---

## 6. テストデータ管理

### 6.1 fixture バージョニング

- `tests/fixtures/` は Git 管理
- スキーマ変更時は fixture も更新（migration スクリプトと同期）
- バイナリ・大容量データは Git LFS

### 6.2 confidential テストデータ

- 機密ノートのテスト用には `confidential: true` フラグ付きの fake データを使用
- 実在の機密情報は絶対にコミットしない（pre-commit hook で検出）

---

## 7. メトリクスと SLO 検証

### 7.1 継続的測定

| メトリクス | 取得元 | 検証頻度 |
|---------|------|--------|
| タスク成功率 | ExecutionLog | 日次 |
| 平均 auto_score | ExecutionLog | 日次 |
| p95 レイテンシ | ExecutionLog | 日次 |
| GPU アイドル率 | gpu_state スナップショット | 5 分ごと |
| Starvation 発生率 | scheduler logs | 日次 |
| 監査ハッシュ整合 | integrity/*.hash | 日次 |

### 7.2 SLO 違反時のアクション

```yaml
slo_violations:
  task_success_rate < 95%:
    - alert: critical
    - action: 直近 24 時間の失敗を分析、原因モデルを特定
  p95_latency_interactive > 3s:
    - alert: warn
    - action: スケジューラデグラデーションを手動 ON 検討
  needs_human_per_day > 10:
    - alert: warn
    - action: エージェントの escalation_criteria 見直し
```

---

## 8. 24 時間連続運用検証（最終受入テスト）

DDD-08 §8.1 シナリオを **2 週間に 1 回** 自動実行：

```bash
# .github/workflows/24h-soak.yml
on:
  schedule:
    - cron: "0 0 1,15 * *"   # 月 2 回
jobs:
  soak:
    runs-on: [self-hosted, gpu]
    steps:
      - run: airflow test soak --duration 24h --report-out reports/$(date).md
```

合格基準（DDD-08 §8.2）：
- critical ログ 0 件
- needs_human < 10 件
- starvation force_run 0 件
- 全タスク done または needs_human
- GPU OOM 0 件
- 監査ハッシュ連続

---

## 9. 品質ゲート

PR マージの条件：

| ゲート | 基準 |
|-------|------|
| 単体テスト | 全 pass |
| カバレッジ | 80%+（コアモジュール 90%+） |
| Lint | エラー 0 |
| 型チェック（mypy strict） | エラー 0 |
| スキーマバリデート | 全 fixture pass |
| 統合テスト（変更影響範囲） | 全 pass |
| セキュリティスキャン | High/Critical 0 |

---

## 10. オープン課題

| 課題 | 検討時期 |
|------|--------|
| 性能回帰テスト（ベンチマーク継続記録） | Phase 4 |
| AI 出力品質の自動評価（LLM-as-judge） | v2.0 |
| マルチホスト分散テスト | v2.0 |
| 24h soak の負荷パターン多様化 | Phase 5 |

---

*関連：[[DDD-01]]〜[[DDD-08]] 各 §「検証チェックリスト」セクション。本書は横断集約版。*
