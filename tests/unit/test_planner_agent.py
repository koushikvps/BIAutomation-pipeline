"""Tests for Planner Agent."""

import pytest
from unittest.mock import patch, MagicMock, PropertyMock

from planner.agent import PlannerAgent
from shared.models import BuildPlan, ExecutionMode


class TestPlannerAgent:
    @pytest.fixture
    def agent(self, mock_config):
        with patch("planner.agent.LLMClient") as mock_llm_cls, \
             patch("planner.agent.SynapseClient") as mock_syn_cls, \
             patch("planner.agent.PROMPTS_DIR") as mock_dir:
            mock_llm = MagicMock()
            mock_llm.chat_json.return_value = {
                "story_id": "TEST-001",
                "title": "Daily Sales",
                "description": "Sales analysis",
                "business_objective": "Analyze daily sales",
                "source_tables": ["dbo.Sales"],
                "acceptance_criteria": [],
            }
            mock_llm_cls.return_value = mock_llm

            mock_syn = MagicMock()
            mock_syn.execute_query.return_value = []
            mock_syn_cls.return_value = mock_syn

            mock_path = MagicMock()
            mock_path.read_text.return_value = "You are a planner."
            mock_dir.__truediv__ = lambda self, x: mock_path

            agent = PlannerAgent(mock_config)
            agent._llm = mock_llm
            agent._synapse = mock_syn
            agent._story_parser_prompt = "Parse the story."
            agent._plan_generator_prompt = "Generate a plan."
            return agent

    def test_parse_story_dict_input(self, agent):
        story_dict = {
            "story_id": "TEST-001",
            "title": "Daily Sales",
            "description": "Sales analysis",
            "business_objective": "Analyze daily sales",
            "source_system": "SalesDB",
            "source_tables": ["dbo.Sales"],
            "acceptance_criteria": [],
        }
        story = agent._parse_story(story_dict)
        assert story.story_id == "TEST-001"
        assert story.source_tables == ["dbo.Sales"]

    def test_parse_story_string_input(self, agent):
        agent._llm.chat_json.return_value = {
            "story_id": "AI-001",
            "title": "From AI",
            "description": "Parsed by AI",
            "business_objective": "",
            "source_system": "SalesDB",
            "source_tables": ["dbo.Orders"],
            "acceptance_criteria": [],
        }
        story = agent._parse_story("Build a sales report from Orders table")
        assert story.story_id == "AI-001"
        agent._llm.chat_json.assert_called_once()

    def test_detect_mode_greenfield_when_synapse_unreachable(self, agent):
        story = MagicMock()
        story.source_tables = ["dbo.Sales"]
        with patch("pyodbc.connect", side_effect=Exception("Synapse paused")):
            mode, ctx = agent._detect_mode(story)
        assert mode == ExecutionMode.GREENFIELD

    def test_table_name_matches_exact(self):
        assert PlannerAgent._table_name_matches("dbo.Sales", ["dbo_Sales", "other"]) is True

    def test_table_name_matches_partial(self):
        assert PlannerAgent._table_name_matches("dbo.Sales", ["ext_dbo_Sales"]) is True

    def test_table_name_no_match(self):
        assert PlannerAgent._table_name_matches("dbo.Sales", ["Customers", "Products"]) is False

    def test_template_fallback_plan_greenfield(self, agent):
        story = MagicMock()
        story.story_id = "TEST-001"
        story.source_tables = ["dbo.Sales"]
        story.target_view_name = "vw_sales_analysis"
        story.business_objective = "Sales report"
        plan = agent._template_fallback_plan(story, ExecutionMode.GREENFIELD)
        assert isinstance(plan, BuildPlan)
        assert plan.story_id == "TEST-001"
        assert plan.mode == ExecutionMode.GREENFIELD
        assert len(plan.execution_order) >= 2  # bronze + silver + gold

    def test_template_fallback_plan_brownfield(self, agent):
        story = MagicMock()
        story.story_id = "TEST-002"
        story.source_tables = ["dbo.Sales"]
        story.target_view_name = None
        story.business_objective = ""
        plan = agent._template_fallback_plan(story, ExecutionMode.BROWNFIELD)
        assert plan.mode == ExecutionMode.BROWNFIELD
        assert plan.risk_level == "low"

    def test_run_full_pipeline(self, agent):
        agent._llm.chat_json.side_effect = [
            # parse_story response
            {
                "story_id": "TEST-001", "title": "Sales", "description": "desc",
                "business_objective": "report", "source_system": "SalesDB",
                "source_tables": ["dbo.Sales"], "acceptance_criteria": [],
            },
            # generate_build_plan response
            {
                "story_id": "TEST-001", "mode": "greenfield", "risk_level": "low",
                "execution_order": [
                    {"step": 1, "layer": "bronze", "action": "create",
                     "artifact_type": "external_table", "object_name": "[bronze].[ext_Sales]",
                     "source": {"system": "dbo", "schema_name": "dbo", "table": "Sales"},
                     "columns": [], "logic_summary": "External table", "depends_on": []},
                    {"step": 2, "layer": "gold", "action": "create",
                     "artifact_type": "view", "object_name": "[gold].[vw_sales]",
                     "columns": [], "logic_summary": "Gold view", "depends_on": [1]},
                ],
                "validation_requirements": [],
            },
        ]
        with patch("pyodbc.connect", side_effect=Exception("paused")):
            plan = agent.run("Build sales report")
        assert isinstance(plan, BuildPlan)
        assert plan.story_id == "TEST-001"
