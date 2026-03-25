from __future__ import annotations

from identity_validator.utils import read_json
import unittest

from tests.helpers import load_case, run_pipeline


class RecordedPipelineTests(unittest.TestCase):
    def _assert_case(self, case_id: str) -> None:
        case = load_case(case_id)
        context = run_pipeline(case_id, enable_sonar=True)
        project_type = context.results["project_type"]
        self.assertEqual(project_type.status, "success")
        expected_types = case.expected.get("project_types_any_of") or []
        self.assertIn(project_type.data["project_type"], expected_types)

        rule_engine = context.results["rule_engine"]
        self.assertEqual(rule_engine.status, "success")
        self.assertGreaterEqual(rule_engine.metrics["overall_score"], 0)
        self.assertLessEqual(rule_engine.metrics["overall_score"], 100)
        self.assertGreaterEqual(rule_engine.metrics["activity_score"], 0)
        self.assertLessEqual(rule_engine.metrics["activity_score"], 100)
        self.assertGreaterEqual(rule_engine.metrics["originality_score"], 0)
        self.assertLessEqual(rule_engine.metrics["originality_score"], 100)
        self.assertGreaterEqual(rule_engine.metrics["community_activity_score"], 0)
        self.assertLessEqual(rule_engine.metrics["community_activity_score"], 100)
        self.assertGreaterEqual(rule_engine.metrics["community_quality_score"], 0)
        self.assertLessEqual(rule_engine.metrics["community_quality_score"], 100)

        github_activity = context.results["github_activity"]
        self.assertEqual(github_activity.status, "success")

        telegram_semantics = context.results["telegram_semantics"]
        self.assertEqual(telegram_semantics.status, "success")

        project_similarity = context.results["project_similarity"]
        self.assertEqual(project_similarity.status, "success")
        self.assertGreaterEqual(project_similarity.metrics["closest_projects_count"], 1)

        explainer = context.results["llm_explainer"]
        self.assertEqual(explainer.status, "success")
        self.assertEqual(explainer.text, "")
        self.assertIn("llm_grounding_incomplete", explainer.flags)

        self.assertTrue((context.trace_store.root_dir / "run_summary.json").is_file())
        self.assertTrue((context.trace_store.root_dir / "blocks" / "rule_engine.json").is_file())
        self.assertTrue((context.trace_store.root_dir / "llm" / "llm_explainer.json").is_file())
        run_summary = read_json(context.trace_store.root_dir / "run_summary.json")
        self.assertTrue(run_summary["workflow"]["stages"])
        self.assertTrue(run_summary["workflow"]["units"])

    def test_ton_blockchain_recorded(self) -> None:
        self._assert_case("ton_blockchain_ton")

    def test_getgems_recorded(self) -> None:
        self._assert_case("getgems_nft_contracts")

    def test_stonfi_recorded(self) -> None:
        self._assert_case("ston_fi_sdk")

    def test_tonkeeper_recorded(self) -> None:
        self._assert_case("tonkeeper_web")

    def test_blueprint_recorded(self) -> None:
        self._assert_case("ton_org_blueprint")

    def test_ton_punks_recorded(self) -> None:
        self._assert_case("ton_punks")


if __name__ == "__main__":
    unittest.main()
