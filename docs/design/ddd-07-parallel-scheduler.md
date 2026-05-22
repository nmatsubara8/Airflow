# DDD-07：並列スケジューラ詳細設計

> Phase 4。DDD-06 までは「シリアル実行 + 動的モデル選択」だったが、ここで **並列化** と **GPU 資源管理** を実装可能な粒度に落とす。
>
> 参照要件：`requirements-parallel-execution.md` 全般（スターベーション防止含む）、`requirements-overview.md` NF-01〜03
> 前提：[[DDD-01：共通データスキーマ仕様書]]、[[DDD-02：MCP インターフェース仕様書]]、[[DDD-05：ローカル LLM 実行基盤]]、[[DDD-06：モデル動的選択・学習]]

---

## 1. スコープ

### 1.1 含む

- GPU リソース管理（VRAM 予約・解放・LRU 退避）
- マルチキュースケジューラ
- 親和性スコア計算（精度 × swap_cost）
- プロファイル管理（同時起動可能なモデルセット）
- スターベーション防止（aging / deadline / fairshare）
- 投機的プリロード
- 高負荷時のデグラデーション
- 並列実行テスト戦略

### 1.2 含まない（後続）

- §02/§03 への展開 → DDD-08
- セキュリティ・監視 → DDD-08

---

## 2. アーキテクチャ概観

```
                ┌─────────────────────────────────────┐
                │ タスク投入（エージェント / Dispatcher）  │
                └────────────────────┬────────────────┘
                                     │
                  ┌──────────────────▼──────────────────┐
                  │       Affinity Scorer                │
                  │   model.recommend (DDD-06)           │
                  │   + GPU state (現在のロード済み)       │
                  │   + swap_cost 計算                    │
                  └──────────────────┬──────────────────┘
                                     │
                  ┌──────────────────▼──────────────────┐
                  │     Multi-Queue Scheduler            │
                  │  ┌───┬───┬───┬───┬───┐               │
                  │  │14B│32B│ 8B│emb│...│ ← model別キュー│
                  │  └───┴───┴───┴───┴───┘               │
                  │   + aging / deadline / fairshare      │
                  └──────────────────┬──────────────────┘
                                     │
                  ┌──────────────────▼──────────────────┐
                  │       Profile Manager                │
                  │   - 同時起動プロファイル選択           │
                  │   - 切替コスト評価                    │
                  └──────────────────┬──────────────────┘
                                     │
                  ┌──────────────────▼──────────────────┐
                  │     GPU State Manager                │
                  │   - VRAM 予約 / 退避                  │
                  │   - LRU + pinned 制御                 │
                  └──────────────────┬──────────────────┘
                                     │
                  ┌──────────────────▼──────────────────┐
                  │       Inference Runtime              │
                  │   Ollama（OLLAMA_NUM_PARALLEL=N）     │
                  └──────────────────────────────────────┘
```

すべて gpu-mcp サーバー内の単一プロセスで動作（マルチスレッド）。

---

## 3. 親和性スコア

### 3.1 計算式

```
total_score(model) = expected_accuracy × accuracy_weight
                   - swap_cost × latency_weight
                   + affinity_bonus
                   + starvation_boost
```

| 項 | 内容 | デフォルト重み |
|----|------|------------|
| `expected_accuracy` | DDD-06 KNN の score | 1.0 |
| `accuracy_weight` | 設定 | 0.7（通常時）/ 0.3（高負荷時） |
| `swap_cost` | §3.2 で算出 | — |
| `latency_weight` | 設定 | 0.3（通常時）/ 0.7（高負荷時） |
| `affinity_bonus` | ロード済みなら +0.0、未ロードなら -0.0（実質は swap_cost に集約） | — |
| `starvation_boost` | 待機時間ベース、§5 で算出 | — |

`latency_sensitivity = interactive` のタスクは `latency_weight × 2.0` を適用。

### 3.2 swap_cost の算出

```
if model is loaded:
  swap_cost_ms = 0

elif VRAM 余裕で追加ロード可能:
  swap_cost_ms = model.estimated_load_time_ms  (実測ベース)

elif 既存モデルを退避すれば追加可能:
  swap_cost_ms = unload_time_ms (1-2秒)
               + new_model.load_time_ms
               + (退避モデルの再ロード期待値 × 退避確率)

else:
  swap_cost_ms = INFINITY  (現プロファイルでは実行不可)
```

`load_time_ms` は GpuState の各モデルメタに過去計測値を保持（DDD-01 Model スキーマに `avg_latency_ms_per_100tok` と並列で `avg_load_time_ms` を追加）。

### 3.3 再ルート時の親和性無視

DDD-06 §7.4 で予告した通り、`reroute` 発動時は親和性スコアを **無視して fallback model を強制** する。
理由：再ルートは精度補完が目的であり、速度を犠牲にしてでも上位モデルを使う。

---

## 4. プロファイル管理

### 4.1 プロファイルの定義

```yaml
# .airflow/config/parallel.yaml
profiles:
  - name: single-32b
    models: [model_qwen25_32b_q3]
    vram_required_gb: 14

  - name: 14b-plus-8b
    models: [model_qwen25_14b_q4, model_llama31_8b_q4]
    vram_required_gb: 14

  - name: dual-8b-plus-emb
    models: [model_llama31_8b_q4, model_qwen25_7b_q4, model_bge_m3]
    vram_required_gb: 11

  - name: coder-plus-emb
    models: [model_qwen25_coder_32b_q3, model_bge_m3]
    vram_required_gb: 15
```

### 4.2 プロファイル選択アルゴリズム

```
1. キュー状態をスキャン:
   queue_depth = { model_id: count }

2. 各プロファイルについて評価:
   for profile in profiles:
     - 含まれるモデルでカバーできるキュー件数 = sum(queue_depth[m] for m in profile.models)
     - 評価スコア = カバー件数 × 重み

3. 切替コスト評価:
   if current_profile != best_profile:
     - 共通モデルは保持
     - 差分モデルのアンロード/ロード時間を計算
     - 切替コスト > 閾値（30秒）なら現プロファイル継続

4. 選択
```

### 4.3 プロファイル切替のタイミング

- **キューが空になった時**（自然な切替タイミング）
- **30 秒ごとの定期チェック**（負荷変動への追従）
- **手動切替**：`airflow profile switch <name>`

---

## 5. マルチキュースケジューラ

### 5.1 キュー構造

```
queues = {
  "model_qwen25_14b_q4": Queue[Task],
  "model_qwen25_32b_q3": Queue[Task],
  "model_llama31_8b_q4": Queue[Task],
  ...
}
```

タスクはモデル選択完了後に対応するサブキューに enqueue。

### 5.2 enqueue ロジック

```
1. Affinity Scorer がタスクの top model を決定
2. queues[top_model.id].push(task)
3. task.scheduling.enqueued_at = now()
4. task.scheduling.selected_model_id = top_model.id
5. ログ: info "scheduler.enqueued"
```

### 5.3 dequeue ロジック（次タスク選択）

```
1. starvation チェック（§5 詳細）→ 優先タスクがあれば即返す
2. ロード済みモデルのキューから優先順に取得:
   for model_id in loaded_models_lru_order:
     if queues[model_id].not_empty():
       return queues[model_id].pop_highest_priority()
3. ロード済みキューが全空 → プロファイル内モデルキューを探索
4. それも空 → プロファイル切替検討
5. 結果なし → idle wait
```

### 5.4 同モデル連続実行上限

`max_consecutive_same_model = 10`：
- 同じモデルで 10 件連続処理したら、強制的に別モデルキューにスイッチ
- 別モデルキューが空 → 元のモデルで継続（実質スターベーションなし）

実装：スケジューラ内に `consecutive_count[model_id]` カウンタ。

### 5.5 同モデル並列実行

Ollama v0.5+ は単一モデルで複数リクエストを並列処理可能：

```yaml
# Ollama 環境変数
OLLAMA_NUM_PARALLEL=2    # 同モデル同時 2 リクエスト
OLLAMA_MAX_LOADED_MODELS=3
```

スケジューラは VRAM 余裕がある限り同モデル内で並列実行を許可。
ただし VRAM 消費が増えるため、KV キャッシュサイズを考慮（モデルメタに記録）。

---

## 6. スターベーション防止（DDD 化）

要件定義 §3.2.1 を実装可能な粒度に落とす。

### 6.1 待機時間ブースト（Aging）

```
effective_priority(task) = base_priority + aging_score(task)

aging_score(task):
  wait_sec = now - task.scheduling.enqueued_at.epoch_sec
  if wait_sec < max_wait_before_boost_seconds:  # 600 sec (10 min)
    return 0
  else:
    return (wait_sec - 600) / 60 × aging_factor  # 0.1 per min
```

`effective_priority` が高いタスクは Affinity スコアを上書きして優先される。

### 6.2 デッドライン強制実行

```
if task.deadline and (task.deadline - now) < deadline_threshold_seconds:  # 300
  task.scheduling.starvation_flags.append("deadline_near")
  → 親和性スコア・swap_cost 無視
  → スワップ発生してでも即実行
```

### 6.3 絶対上限（max_wait_seconds）

```
if wait_sec > max_wait_seconds:  # 3600 sec (1 hour)
  task.scheduling.starvation_flags.append("force_run")
  → swap_cost = 0 として扱い、最優先実行
  → critical ログ「scheduler.starvation_force」
```

### 6.4 モデル別フェアシェア（高負荷時）

```yaml
fair_share:
  enabled_on_high_load: true
  minimum_gpu_time_pct:
    "32b": 30
    "14b": 50
    "8b":  20
```

高負荷時：直近 5 分の GPU 時間配分を測定し、最低保証割合を下回るキューを優先。

### 6.5 公平性チェックループ

スケジューラ内に 30 秒ごとに走るループ：

```
every 30 sec:
  for task in all_queues:
    wait_sec = now - task.enqueued_at
    if wait_sec > max_wait_seconds:
      promote_to_force_run(task)
    elif wait_sec > max_wait_before_boost:
      update_aging_score(task)
    elif task.deadline_close():
      flag_deadline_near(task)

  fair_share_audit()  # 各モデル時間配分の集計
```

---

## 7. 投機的プリロード

### 7.1 プリロード判定

```
1. 現在実行中タスクの残り時間予測:
   remaining_ms = max(0, expected_total_ms - elapsed_ms)

2. キュー先頭タスクのモデルチェック:
   if queue.head.model != currently_loaded:
     swap_target = queue.head.model

3. VRAM 余裕確認:
   if free_vram_gb >= swap_target.vram_gb:
     → 即座にロード開始（並行実行中もバックグラウンドで）

4. VRAM 余裕なし:
   if remaining_ms < swap_target.load_time_ms:
     → 待っても間に合わない、プリロードしない
   else:
     → 退避候補がいるか確認、可能ならプリロード
```

### 7.2 プリロード中断

```
高優先度タスクがプリロード対象モデル以外を要求:
  → プリロード中断、新しい優先タスクを優先処理

deadline_near または force_run タスクが来た:
  → プリロード中断
```

### 7.3 プリロード精度の学習

プリロード後、そのモデルが本当に使われたかを記録：
- 使われた → `preload_hit` カウント増加
- 使われずアンロードされた → `preload_miss` カウント増加

ヒット率が 50% 未満のパターンは投機を停止（パターン = タスク種別 × 時間帯）。

---

## 8. デグラデーション（高負荷時の振る舞い変更）

### 8.1 検出条件

```yaml
degradation:
  high_load_queue_threshold: 20
  high_load_gpu_threshold_pct: 90
  hysteresis_seconds: 30
```

- 全キュー合計 ≥ 20 件 → high load 候補
- GPU 使用率 ≥ 90% → high load 候補
- 両方が 30 秒継続 → デグラデーション ON

回復時：両方が 30 秒間閾値を下回ったら OFF（ヒステリシス）。

### 8.2 デグラデーション ON 時の動作変更

| 設定項目 | 通常時 | デグラデーション時 |
|--------|------|----------------|
| `accuracy_weight` | 0.7 | 0.3 |
| `latency_weight` | 0.3 | 0.7 |
| `exploration_rate` (ε-greedy) | 0.1 | 0.0（探索停止） |
| 二重実行 | required_accuracy=high で実行 | required_accuracy=high のみ、interactive 除外 |
| 再ルート | 通常実行 | 無効化（fallback chain スキップ） |

### 8.3 ログ・通知

```
info "scheduler.degraded_on"  with reason
info "scheduler.degraded_off" with duration_sec
```

---

## 9. GPU State Manager

### 9.1 役割

- VRAM 使用量のリアルタイム監視（nvidia-smi or Ollama `/api/ps`）
- モデルロード/アンロード API（DDD-02 §6.3 の `gpu.reserve` 実装）
- LRU 退避アルゴリズム
- pinned モデル制御（明示的に常駐させたいモデル）

### 9.2 LRU 退避

```
gpu.reserve(model_X):
  if model_X already loaded:
    return  # no-op
  if free_vram_gb >= model_X.vram_gb:
    load(model_X)
    return

  # 退避必要
  candidates = loaded_models sorted by last_used_at asc
                excluding pinned
                excluding currently in use (pending_tasks > 0)

  to_evict = []
  freed_gb = 0
  for c in candidates:
    to_evict.append(c)
    freed_gb += c.vram_gb
    if freed_gb >= model_X.vram_gb:
      break

  if freed_gb < model_X.vram_gb:
    raise GPU_INSUFFICIENT_VRAM
    return

  for m in to_evict:
    unload(m)

  load(model_X)
```

### 9.3 pinned モデル

設定で常駐させたいモデルを指定：

```yaml
gpu:
  pinned_models:
    - model_bge_m3   # 埋め込みは常に必要なため
```

pinned モデルは LRU 退避対象外。

### 9.4 状態スナップショット

`.airflow/runtime/gpu_state.json` を 5 秒ごと（または状態変化時）に更新。
DDD-01 §2.4 の GpuState スキーマに準拠。

---

## 10. 並列度の階層

### 10.1 階層別並列度

| 階層 | 並列度 | 実装 |
|------|------|------|
| **同一モデル並列推論** | `OLLAMA_NUM_PARALLEL` (2) | Ollama 内蔵 |
| **複数モデル並列ロード** | プロファイル定義（最大 3） | GPU State Manager |
| **CPU/GPU ハイブリッド** | Phase 5 以降 | llama.cpp CPU mode |

### 10.2 並列度の動的調整

VRAM 残量と KV キャッシュ消費から動的に：

```
max_parallel(model) = min(
  OLLAMA_NUM_PARALLEL,
  free_vram_gb / kv_cache_per_request_gb
)
```

通常は OLLAMA_NUM_PARALLEL=2 で固定、VRAM 不足時のみ 1 に降格。

---

## 11. メトリクスとモニタリング

### 11.1 重要メトリクス

```
- scheduler_queue_depth{model_id}
- scheduler_wait_time_ms_histogram{model_id, percentile}
- scheduler_starvation_events_total{reason: aged|deadline|force_run}
- scheduler_profile_switches_total{from, to, reason}
- gpu_vram_used_gb
- gpu_model_loaded{model_id}
- gpu_preload_hit_ratio{model_id, task_type}
- gpu_eviction_total{model_id}
- model_swap_total
- scheduler_degraded{is_on}
```

### 11.2 ダッシュボード

```bash
$ airflow scheduler dashboard

=== GPU State ===
VRAM: 14.2 / 16.0 GB
Loaded: qwen25_14b_q4 (9GB, 23 tasks/h), bge-m3 (1GB, pinned)
Active Profile: 14b-plus-8b

=== Queue Status ===
Model              Depth  AvgWait  Oldest  Mode
qwen25_14b_q4         5      4.2s    18s   normal
qwen25_32b_q3         2     45.0s    2m    AGING
bge_m3                0       -      -     idle

=== Starvation Events (last hour) ===
aged:        3 tasks (max wait 13min)
deadline:    1 task
force_run:   0 tasks

=== Profile History (last hour) ===
14:00-14:23 single-32b
14:23-15:00 14b-plus-8b  (switch reason: 14B queue depth 12)

=== Preload Stats ===
qwen25_14b → qwen25_32b: 8 hits / 12 attempts (67%)
```

### 11.3 アラート（Phase 5 で通知連携）

| 条件 | レベル | 通知 |
|------|-----|------|
| starvation_force 発生 | critical | 即時 |
| degradation 5 分以上継続 | warn | 即時 |
| preload hit ratio < 30% | info | 日次サマリ |
| GPU OOM | critical | 即時 |

---

## 12. シナリオ

### 12.1 シナリオ A：典型的なバッチ負荷

```
キュー初期状態:
  14B: 50 件 (§01 QA), 32B: 30 件 (§02 Content), 8B: 20 件 (分類)

スケジューラ動作:
  t=0 プロファイル選択 → 14b-plus-8b（最も件数カバー）
  t=0 14B + 8B 並列ロード
  t=0-180s 14B 10 件 + 8B 10 件を消化（並列）
  t=180s consecutive=10 達成 → 14B → 32B キュー切替を試行
                                  VRAM 不足 → 8B 退避 → 32B ロード
  t=200s 32B 10 件処理
  t=400s 32B キュー残 20 件、14B キュー残 40 件
        → 14B → 32B 周期的に切替（aging 監視）
  
fairshare 監視:
  32B 割合 < 30% なら 32B を優先
```

### 12.2 シナリオ B：interactive タスク割り込み

```
状態: 14B + 8B 並列稼働中、両方処理中
新規タスク到着: features.latency_sensitivity = interactive, model = 14B 想定

スケジューラ:
  t=0 affinity_scorer: latency_weight×2.0 適用
  t=0 14B キューの head に挿入（priority=10 同等扱い）
  t=0 14B 並列スロット空き待ち（OLLAMA_NUM_PARALLEL=2）
  t=2s 既存推論完了 → interactive タスク実行開始
  t=4s 完了 → handoff_note 追加
```

### 12.3 シナリオ C：スターベーション + force_run

```
14B が長時間連続実行され、32B キューに 1 件のタスクが 1 時間以上滞留

スケジューラ 30 秒ループ:
  t=3600s 該当タスクの wait_sec >= max_wait_seconds
  t=3600s task.starvation_flags = ["force_run"]
  t=3600s critical "scheduler.starvation_force" ログ
  t=3601s 14B の現実行完了待ち → 14B アンロード → 32B ロード
  t=3622s 32B タスク実行
```

### 12.4 シナリオ D：デグラデーション

```
夜間バッチ大量投入で queue_depth = 50, GPU = 95%

t=0     高負荷検出
t=30s   30 秒継続 → デグラデーション ON
        accuracy_weight: 0.7 → 0.3
        latency_weight: 0.3 → 0.7
        二重実行: high のみ、interactive は除外
        再ルート: 無効化

t=30s-180s スループット 2-3 倍、精度若干低下
t=180s  queue_depth < 20 + GPU < 90% で 30 秒継続 → デグラデーション OFF
        通常モードに復帰
```

---

## 13. テスト戦略

### 13.1 単体テスト

| 対象 | テスト内容 |
|------|---------|
| Affinity Scorer | 各種特徴入力に対するスコア計算の妥当性 |
| Multi-Queue | enqueue/dequeue 順序、starvation flag 反映 |
| Profile Manager | プロファイル選択ロジック、切替コスト評価 |
| GPU State | LRU 退避順、pinned 除外 |
| Preload | hit/miss カウント、中断挙動 |

### 13.2 統合テスト

| シナリオ | 検証項目 |
|---------|---------|
| 単一モデル長時間バッチ | スループット、メモリリーク |
| 2 モデル交互スワップ | swap 回数、ログ整合 |
| interactive 割り込み | p95 レイテンシ < 3秒 |
| スターベーション意図的発生 | aging/deadline/force_run の各分岐 |
| デグラデーション境界 | ヒステリシス動作（30秒ルール） |

### 13.3 負荷試験

```bash
# 100 タスク同時投入
airflow test load --tasks 100 --models qwen25_14b_q4,qwen25_32b_q3

# 観測指標:
- 完了時間（vs シリアル想定値）
- モデルスワップ回数
- p95 レイテンシ
- GPU アイドル率
```

### 13.4 カオステスト

| イベント | 期待動作 |
|---------|---------|
| Ollama プロセス kill | DDD-03 fatal、新タスク受付停止、運用者通知 |
| OOM 強制発生 | GPU_OOM critical ログ、関連タスク blocked、回復 |
| ディスクフル（ログ） | warn、ログローテーション加速、停止せず |
| Vault フォルダ消失 | ingester 停止、taskboard は継続稼働 |

---

## 14. パフォーマンス目標と実測

### 14.1 目標（要件定義の再掲 + 詳細化）

| 指標 | 目標 |
|------|-----|
| モデルスワップ回数（単純実装比） | **1/5 以下** |
| GPU アイドル率（タスクあり時） | **10% 以下** |
| interactive p95 レイテンシ | **3 秒以内** |
| バッチスループット（単純実装比） | **2-3 倍** |
| キュー待ち時間 p95（キュー深度 20） | **10 分以内** |
| スターベーション force_run 発生率 | **0.1% 未満** |
| プリロード hit 率 | **60% 以上** |

### 14.2 ベースライン比較

Phase 3 完了時の `docs/baseline/phase3-orchestration.md` の指標と比較し、`docs/baseline/phase4-parallel.md` に記録。

---

## 15. 検証チェックリスト

Phase 4 完了時：

- [ ] Affinity Scorer がロード済みモデルを優先する
- [ ] プロファイル切替が空きタイミング or 30 秒ループで動作
- [ ] 同モデル連続 10 件で切替が発生
- [ ] aging が 10 分超過で priority に反映
- [ ] deadline 5 分前で force_run フラグが立つ
- [ ] 1 時間超過で必ず force_run になる
- [ ] fair_share が高負荷時に発動
- [ ] preload が VRAM 余裕時に動作
- [ ] preload hit ratio が継続記録される
- [ ] デグラデーションのヒステリシス動作
- [ ] GPU OOM で blocked 遷移
- [ ] §14 のパフォーマンス目標を実測クリア

---

## 16. オープン課題

| 課題 | 検討するフェーズ |
|------|--------------|
| CPU オフロードのスケジューリング | Phase 5 |
| マルチ GPU 環境への拡張 | Phase 5 以降 |
| ML ベースの swap_cost 予測 | Phase 5 |
| プリロードパターンの自動学習 | Phase 5 |
| キャンセル可能タスクの中断・再開 | Phase 5 |

---

*次：DDD-08（チーム拡張・本番化）で §02/§03 への展開とセキュリティ・監視を設計し、全体を完了する。*
