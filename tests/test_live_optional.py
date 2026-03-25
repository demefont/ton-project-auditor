from __future__ import annotations

import os
import unittest

from tests.helpers import run_pipeline


@unittest.skipUnless(os.getenv("RUN_LIVE_CASES") == "1", "Set RUN_LIVE_CASES=1 to run live public-data tests")
class LivePipelineTests(unittest.TestCase):
    def test_live_case_works(self) -> None:
        context = run_pipeline("ton_blockchain_ton", mode="live", llm_mode="template")
        self.assertEqual(context.results["github_repo"].status, "success")
        self.assertEqual(context.results["project_type"].status, "success")


if __name__ == "__main__":
    unittest.main()
