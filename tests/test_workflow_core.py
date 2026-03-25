from __future__ import annotations

import unittest

from identity_validator.workflow import AtomicUnit, CompositeUnit, prune_plan
from identity_validator.workflow_builder import build_workflow_plan


class WorkflowCoreTests(unittest.TestCase):
    def test_root_plan_contains_expected_stages_and_groups(self) -> None:
        plan = build_workflow_plan()
        stage_units = [stage.unit_ids for stage in plan.stages]
        self.assertEqual(stage_units[0], ["source_collection"])
        self.assertEqual(stage_units[1], ["address_signal", "repo_analysis", "community_analysis"])
        self.assertEqual(stage_units[-1], ["llm_explainer"])

        unit_map = plan.unit_map()
        self.assertIsInstance(unit_map["source_collection"], CompositeUnit)
        self.assertIsInstance(unit_map["project_type"], AtomicUnit)
        self.assertEqual(unit_map["deep_validation"].execution_mode, "parallel")

        deep_validation = unit_map["deep_validation"]
        self.assertEqual(
            [unit.unit_id for unit in deep_validation.plan.units],
            ["contract_validator", "project_similarity", "sonar_research"],
        )

    def test_pruned_plan_keeps_only_needed_units(self) -> None:
        plan = build_workflow_plan()
        pruned = prune_plan(plan, ["github_repo", "github_activity"])
        root_unit_ids = [unit.unit_id for unit in pruned.units]
        self.assertEqual(root_unit_ids, ["source_collection", "repo_analysis"])
        source_collection = pruned.unit_map()["source_collection"]
        self.assertEqual([unit.unit_id for unit in source_collection.plan.units], ["github_repo"])
        repo_analysis = pruned.unit_map()["repo_analysis"]
        self.assertEqual([unit.unit_id for unit in repo_analysis.plan.units], ["github_activity"])


if __name__ == "__main__":
    unittest.main()
