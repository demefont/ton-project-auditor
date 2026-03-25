from __future__ import annotations

import unittest

from identity_validator.registry import BlockRegistry


class RegistryTest(unittest.TestCase):
    def test_registry_loads_expected_blocks(self) -> None:
        blocks = BlockRegistry().load_blocks()
        expected = {
            "github_repo",
            "github_tree",
            "github_activity",
            "telegram_channel",
            "telegram_semantics",
            "address_signal",
            "project_type",
            "project_registry",
            "project_similarity",
            "contract_validator",
            "claim_consistency",
            "risk_validator",
            "sonar_research",
            "rule_engine",
            "llm_explainer",
        }
        self.assertTrue(expected.issubset(set(blocks.keys())))


if __name__ == "__main__":
    unittest.main()
