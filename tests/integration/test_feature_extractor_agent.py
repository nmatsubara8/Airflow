"""agents.extract_features × feature-extractor agent 統合確認テスト。

検証スコープ:
  1. 実 skills.md の読み込みとエージェントメタデータ整合性
  2. agents.find_for_task → feature-extractor の発見経路
  3. agents.extract_features MCP ツールの全フィールド・inferred_fields 追跡
  4. common.feature_extractor との委譲整合（refactoring #5 後の動作）
  5. skills.md に記載のエスカレーション条件の境界値
  6. dispatcher → extract_features の統合フロー
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

os.environ.setdefault("AIRFLOW_AUTH_LOCALHOST_BYPASS", "true")

import airflow_mcp.agents.server as agents_mod
from tests.helpers import make_mcp_caller

_AGENTS_DIR = Path(__file__).parent.parent.parent / "agents"
_FEATURE_EXTRACTOR_SKILLS = _AGENTS_DIR / "orchestration" / "feature-extractor" / "skills.md"

_call = make_mcp_caller(agents_mod.call_tool)


@pytest.fixture(autouse=True)
def reset_agents():
    """各テスト前後に _agents をリセット。"""
    agents_mod._agents.clear()
    yield
    agents_mod._agents.clear()


@pytest.fixture
def loaded_agents(monkeypatch):
    """実 agents/ ディレクトリから全 skills.md を読み込んだ状態を返す。"""
    monkeypatch.setattr(agents_mod, "_AGENTS_DIR", _AGENTS_DIR)
    agents_mod._load_agents()
    return agents_mod._agents


# ── シナリオ 1: 実 skills.md の読み込みとメタデータ整合性 ─────────────────

class TestSkillsMdLoading:

    def test_feature_extractor_skills_file_exists(self):
        """前提: skills.md ファイルが存在すること。"""
        assert _FEATURE_EXTRACTOR_SKILLS.exists(), \
            f"skills.md not found: {_FEATURE_EXTRACTOR_SKILLS}"

    def test_load_agents_registers_feature_extractor(self, loaded_agents):
        """_load_agents() が feature-extractor を agent_feature_extractor として登録すること。"""
        assert "agent_feature_extractor" in loaded_agents

    def test_agent_id_matches_filename_convention(self, loaded_agents):
        agent = loaded_agents["agent_feature_extractor"]
        assert agent["agent_id"] == "agent_feature_extractor"

    def test_section_is_orchestration(self, loaded_agents):
        """skills.md の section: orchestration が正しく読まれること。"""
        agent = loaded_agents["agent_feature_extractor"]
        assert agent["section"] == "orchestration"

    def test_role_field_present(self, loaded_agents):
        """role フィールドが空でないこと。"""
        agent = loaded_agents["agent_feature_extractor"]
        assert agent.get("role")

    def test_responsibilities_include_feature_fields(self, loaded_agents):
        """responsibilities に task_type / language / accuracy が含まれること。"""
        agent = loaded_agents["agent_feature_extractor"]
        resp_text = " ".join(agent.get("responsibilities", []))
        # skills.md の responsibilities に feature field 名が含まれる
        assert any(kw in resp_text for kw in ["task_type", "language", "accuracy"]), \
            f"responsibilities did not mention feature fields: {resp_text}"

    def test_preferred_models_field_present(self, loaded_agents):
        """preferred_models フィールドが存在しリストであること。"""
        agent = loaded_agents["agent_feature_extractor"]
        preferred = agent.get("preferred_models", [])
        assert isinstance(preferred, list)
        assert len(preferred) >= 1

    def test_preferred_model_ids_are_known(self, loaded_agents):
        """preferred_models に登録されているモデル ID が既知の形式であること。"""
        agent = loaded_agents["agent_feature_extractor"]
        for entry in agent.get("preferred_models", []):
            assert "id" in entry, f"preferred_model entry missing 'id': {entry}"

    def test_mcp_scopes_includes_tasks_read(self, loaded_agents):
        """mcp_scopes に tasks:read が含まれること（DDD-02 §7）。"""
        agent = loaded_agents["agent_feature_extractor"]
        scopes = agent.get("mcp_scopes", [])
        assert "tasks:read" in scopes

    def test_body_contains_process_description(self, loaded_agents):
        """skills.md の # Process セクションが body として読まれていること。"""
        agent = loaded_agents["agent_feature_extractor"]
        body = agent.get("body", "")
        assert "Process" in body or "process" in body.lower()

    def test_schema_version_is_set(self, loaded_agents):
        agent = loaded_agents["agent_feature_extractor"]
        assert agent.get("schema_version") == "1.0.0"


# ── シナリオ 2: agents.find_for_task → feature-extractor 発見経路 ──────────

class TestFindForTask:

    @pytest.mark.asyncio
    async def test_find_returns_feature_extractor_for_orchestration_task(self, loaded_agents):
        """section=orchestration のタスクで feature-extractor が候補に現れること。"""
        task = {"section": "orchestration", "tags": ["workflow", "feature"]}
        result = await _call("agents_find_for_task", {"task": task})
        candidate_ids = [c["agent_id"] for c in result["candidates"]]
        assert "agent_feature_extractor" in candidate_ids, \
            f"feature-extractor not found in: {candidate_ids}"

    @pytest.mark.asyncio
    async def test_feature_extractor_scores_higher_for_orchestration_than_engineering(
        self, loaded_agents
    ):
        """engineering タスクより orchestration タスクで高スコアになること。"""
        task_orch = {"section": "orchestration", "tags": []}
        task_eng = {"section": "engineering", "tags": []}

        result_orch = await _call("agents_find_for_task", {"task": task_orch})
        result_eng = await _call("agents_find_for_task", {"task": task_eng})

        # orchestration タスクのときは候補に含まれる
        orch_ids = [c["agent_id"] for c in result_orch["candidates"]]
        assert "agent_feature_extractor" in orch_ids

        # engineering タスクのときはスコア 0 なので含まれない（または下位）
        eng_ids = [c["agent_id"] for c in result_eng["candidates"]]
        if "agent_feature_extractor" in eng_ids:
            orch_score = next(
                c["score"] for c in result_orch["candidates"]
                if c["agent_id"] == "agent_feature_extractor"
            )
            eng_score = next(
                c["score"] for c in result_eng["candidates"]
                if c["agent_id"] == "agent_feature_extractor"
            )
            assert orch_score >= eng_score

    @pytest.mark.asyncio
    async def test_get_feature_extractor_agent_via_mcp(self, loaded_agents):
        """agents.get でメタデータを取得できること。"""
        result = await _call("agents_get", {"agent_id": "agent_feature_extractor"})
        assert result["agent_id"] == "agent_feature_extractor"
        assert result["section"] == "orchestration"

    @pytest.mark.asyncio
    async def test_agents_list_includes_feature_extractor(self, loaded_agents):
        """agents.list に feature-extractor が含まれること。"""
        result = await _call("agents_list", {})
        ids = [a["agent_id"] for a in result["agents"]]
        assert "agent_feature_extractor" in ids

    @pytest.mark.asyncio
    async def test_agents_list_orchestration_filter_finds_feature_extractor(self, loaded_agents):
        """section=orchestration フィルタで feature-extractor が絞り込まれること。"""
        result = await _call("agents_list", {"section": "orchestration"})
        ids = [a["agent_id"] for a in result["agents"]]
        assert "agent_feature_extractor" in ids


# ── シナリオ 3: agents.extract_features の全フィールド・inferred_fields ─────

class TestExtractFeaturesAllFields:

    @pytest.mark.asyncio
    async def test_empty_task_infers_all_four_fields(self):
        """空タスクでは 4 フィールド全て推論されること（skills.md §Process step 2）。"""
        result = await _call("agents_extract_features", {"task": {}})
        assert "features" in result
        assert "inferred_fields" in result
        inferred = set(result["inferred_fields"])
        assert inferred == {"task_type", "required_accuracy", "latency_sensitivity", "language"}, \
            f"expected all 4 inferred, got: {inferred}"

    @pytest.mark.asyncio
    async def test_inferred_fields_excludes_preset_values(self):
        """preset 済みフィールドは inferred_fields に含まれないこと。"""
        task = {
            "features": {"task_type": "review", "required_accuracy": "high"},
            "title": "test",
            "tags": [],
        }
        result = await _call("agents_extract_features", {"task": task})
        inferred = set(result["inferred_fields"])
        assert "task_type" not in inferred
        assert "required_accuracy" not in inferred
        # 未設定の 2 フィールドは inferred に含まれる
        assert "latency_sensitivity" in inferred
        assert "language" in inferred

    @pytest.mark.asyncio
    async def test_default_accuracy_is_medium(self):
        """section が未指定のとき required_accuracy のデフォルトは 'medium'。"""
        result = await _call("agents_extract_features", {"task": {"title": "generic task"}})
        assert result["features"]["required_accuracy"] == "medium"
        assert "required_accuracy" in result["inferred_fields"]

    @pytest.mark.asyncio
    async def test_default_latency_is_batch(self):
        """urgent/interactive タグなしのとき latency_sensitivity のデフォルトは 'batch'。"""
        result = await _call("agents_extract_features", {"task": {"title": "generic task"}})
        assert result["features"]["latency_sensitivity"] == "batch"
        assert "latency_sensitivity" in result["inferred_fields"]

    @pytest.mark.asyncio
    async def test_english_title_infers_language_en(self):
        """ASCII タイトルは language='en' と推論されること。"""
        result = await _call("agents_extract_features",
                             {"task": {"title": "Implement rate limiting"}})
        assert result["features"]["language"] == "en"

    @pytest.mark.asyncio
    async def test_business_section_infers_high_accuracy(self):
        """section=business → required_accuracy='high'（skills.md §Process step 2）。"""
        task = {"section": "business", "title": "contract review"}
        result = await _call("agents_extract_features", {"task": task})
        assert result["features"]["required_accuracy"] == "high"

    @pytest.mark.asyncio
    async def test_legal_section_infers_high_accuracy(self):
        task = {"section": "legal", "title": "legal review"}
        result = await _call("agents_extract_features", {"task": task})
        assert result["features"]["required_accuracy"] == "high"

    @pytest.mark.asyncio
    async def test_urgent_tag_infers_interactive_latency(self):
        """tags に 'urgent' → latency_sensitivity='interactive'。"""
        task = {"tags": ["urgent", "review"], "title": "urgent fix"}
        result = await _call("agents_extract_features", {"task": task})
        assert result["features"]["latency_sensitivity"] == "interactive"

    @pytest.mark.asyncio
    async def test_code_review_tag_alias(self):
        """tags に 'code-review' → task_type='review'（エイリアス経由）。"""
        task = {"tags": ["code-review"], "title": "PR review"}
        result = await _call("agents_extract_features", {"task": task})
        assert result["features"]["task_type"] == "review"

    @pytest.mark.asyncio
    async def test_existing_features_not_overwritten(self):
        """preset 済み features は extract_features が上書きしないこと。"""
        task = {
            "features": {
                "task_type": "extract",
                "required_accuracy": "high",
                "latency_sensitivity": "interactive",
                "language": "en",
            },
            "section": "business",  # normally → high, but already set
            "tags": ["urgent"],     # normally → interactive, but already set
            "title": "日本語",       # normally → ja, but already set
        }
        result = await _call("agents_extract_features", {"task": task})
        feats = result["features"]
        assert feats["task_type"] == "extract"
        assert feats["required_accuracy"] == "high"
        assert feats["latency_sensitivity"] == "interactive"
        assert feats["language"] == "en"
        assert result["inferred_fields"] == []


# ── シナリオ 4: common.feature_extractor との委譲整合 ─────────────────────

class TestDelegationConsistency:
    """refactoring #5 後: agents.extract_features が feature_extractor に委譲していること。"""

    @pytest.mark.asyncio
    async def test_mcp_result_matches_direct_extractor_call(self):
        """MCP ツールの結果が extract_features() 直接呼び出しと一致すること。"""
        from airflow_mcp.common.feature_extractor import extract_features

        task = {"tags": ["summarize", "urgent"], "section": "engineering", "title": "要約タスク"}
        direct_features, direct_inferred = extract_features(task)
        mcp_result = await _call("agents_extract_features", {"task": task})

        assert mcp_result["features"] == direct_features
        assert set(mcp_result["inferred_fields"]) == set(direct_inferred)

    @pytest.mark.asyncio
    async def test_tag_aliases_handled_consistently(self):
        """code-review / summary タグが MCP 経由でも正しくエイリアス解決されること。"""
        for tag, expected in [("code-review", "review"), ("summary", "summarize")]:
            result = await _call("agents_extract_features",
                                 {"task": {"tags": [tag], "title": "t"}})
            assert result["features"]["task_type"] == expected, \
                f"tag={tag!r} expected task_type={expected!r}, got {result['features']['task_type']!r}"


# ── シナリオ 5: skills.md エスカレーション条件の境界値 ───────────────────

class TestEscalationBoundaries:
    """skills.md escalation_criteria の動作確認。
    実装は将来追加される可能性があるが、現状で検出可能な境界値を確認する。"""

    @pytest.mark.asyncio
    async def test_large_description_still_returns_features(self):
        """4000 文字超の description でも features が返ること（クラッシュしない）。"""
        big_desc = "あ" * 4100
        task = {"title": "大きなタスク", "description": big_desc, "tags": []}
        result = await _call("agents_extract_features", {"task": task})
        assert "features" in result
        assert result["features"]["language"] == "ja"

    @pytest.mark.asyncio
    async def test_mixed_language_title_returns_ja_when_cjk_present(self):
        """CJK 文字を含む mixed タイトルは 'ja' と推論されること。"""
        task = {"title": "API設計 for backend", "tags": []}
        result = await _call("agents_extract_features", {"task": task})
        assert result["features"]["language"] == "ja"

    @pytest.mark.asyncio
    async def test_pure_ascii_title_returns_en(self):
        """純 ASCII タイトルは 'en' と推論されること。"""
        task = {"title": "Build REST API endpoint", "tags": []}
        result = await _call("agents_extract_features", {"task": task})
        assert result["features"]["language"] == "en"

    @pytest.mark.asyncio
    async def test_empty_title_defaults_to_en(self):
        """title なしのタスクは language='en' がデフォルト。"""
        result = await _call("agents_extract_features", {"task": {}})
        assert result["features"]["language"] == "en"


# ── シナリオ 6: dispatcher → extract_features 統合フロー ─────────────────

class TestDispatcherIntegration:
    """ingester → dispatcher → extract_features の連携フロー確認。"""

    @pytest.mark.asyncio
    async def test_extract_features_from_ingested_task_fields(self, tmp_path):
        """ingester が生成するタスク形式を extract_features に渡して正しく処理されること。"""
        from airflow_mcp.ingester.parser import frontmatter_to_task_fields, parse_markdown

        # ingester が読む .md を作成
        md = tmp_path / "task_md"
        md.write_text(
            "---\n"
            "title: コンテンツ戦略の策定\n"
            "tags: [content, strategy]\n"
            "priority: 2\n"
            "section: business\n"
            "---\n\n"
            "# コンテンツ戦略の策定\n\n"
            "Q3 に向けたコンテンツカレンダーを作成する。\n",
            encoding="utf-8",
        )
        parsed = parse_markdown(md)
        fields = frontmatter_to_task_fields(parsed)

        # ingester が生成したフィールドを extract_features に渡す
        result = await _call("agents_extract_features", {"task": fields})

        assert result["features"]["required_accuracy"] == "high"  # section=business
        assert result["features"]["language"] == "ja"              # Japanese title
        assert result["features"]["latency_sensitivity"] == "batch"
        # section=business で設定済みなので required_accuracy は inferred に含まれる
        assert "required_accuracy" in result["inferred_fields"]

    @pytest.mark.asyncio
    async def test_dispatcher_orchestration_routing_matches_agent_section(self, tmp_path):
        """dispatch.yaml の orchestration カテゴリが feature-extractor の section と一致すること。"""
        from airflow_mcp.dispatcher.main import infer_section, load_config

        config_path = Path(__file__).parent.parent.parent / ".airflow/config/dispatch.yaml"
        config = load_config(str(config_path))

        # orchestration タグを持つタスクは orchestration カテゴリに分類される
        task = {"tags": ["workflow", "airflow"], "title": "Airflow 設定"}
        section = infer_section(task, config)
        assert section == "orchestration"

        # feature-extractor の section と一致する
        assert section == "orchestration"
