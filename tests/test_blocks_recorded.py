from __future__ import annotations

import asyncio
import json
import tempfile
import urllib.error
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from identity_validator.base import BlockManifest, ExecutionContext
from identity_validator.llm import TemplateLLMClient
from identity_validator.models import BlockResult, ProjectCase, RunOptions
from identity_validator.tracing import TraceStore
from identity_validator.utils import keyword_hits, scam_keyword_hits
from identity_validator.sources import (
    _extract_tgchannels_entries,
    _extract_tgstat_posts,
    _ensure_github_git_bundle_sync,
    _git_run_repo_mutation,
    _select_canonical_project_url,
    fetch_github_repo_meta,
    get_telegram_snapshot,
)
from identity_validator.validators.github_activity.validator import GitHubActivityBlock
from identity_validator.validators.github_tree.validator import GitHubTreeBlock
from identity_validator.validators.address_signal.validator import AddressSignalBlock
from identity_validator.validators.identity_confirmation.validator import IdentityConfirmationBlock
from identity_validator.validators.project_type.validator import ProjectTypeBlock
from identity_validator.validators.rule_engine.validator import RuleEngineBlock
from identity_validator.validators.sonar_research.validator import SonarResearchBlock
from identity_validator.validators.telegram_channel.validator import TelegramChannelBlock
from identity_validator.validators.telegram_semantics.validator import TelegramSemanticsBlock
from identity_validator.validators.llm_explainer.validator import LLMExplainerBlock
from tests.helpers import run_pipeline


class RecordedBlockTests(unittest.TestCase):
    def _make_context(self, *, telegram_handle: str = "") -> ExecutionContext:
        temp_dir = tempfile.mkdtemp(prefix="block_ctx_")
        case = ProjectCase(
            case_id="test_case",
            name="Test Project",
            telegram_handle=telegram_handle,
            root_dir=temp_dir,
        )
        return ExecutionContext(
            case=case,
            options=RunOptions(mode="live", llm_mode="template", enable_sonar=True, sonar_model="sonar"),
            trace_store=TraceStore(temp_dir),
            llm_client=TemplateLLMClient(),
        )

    def test_project_type_for_protocol_case(self) -> None:
        context = run_pipeline("ton_blockchain_ton", target_blocks=["project_type"])
        result = context.results["project_type"]
        self.assertEqual(result.status, "success")
        self.assertEqual(result.data["project_type"], "protocol_infra")

    def test_contract_validator_for_contract_case(self) -> None:
        context = run_pipeline("getgems_nft_contracts", target_blocks=["contract_validator"])
        result = context.results["contract_validator"]
        self.assertEqual(result.status, "success")
        self.assertGreater(result.metrics["contract_file_count"], 0)

    def test_wallet_case_project_type(self) -> None:
        context = run_pipeline("tonkeeper_web", target_blocks=["project_type"])
        result = context.results["project_type"]
        self.assertEqual(result.status, "success")
        self.assertEqual(result.data["project_type"], "wallet_app")

    def test_project_type_uses_discovery_type_hint_alias(self) -> None:
        context = self._make_context()
        context.case.type_hint = "explorer"
        context.results["github_repo"] = BlockResult(
            block_id="github_repo",
            status="success",
            summary="Loaded repo tonviewer/tonviewer",
            data={
                "repo": {
                    "full_name": "tonviewer/tonviewer",
                    "description": "",
                    "topics": [],
                },
                "readme_excerpt": "",
            },
        )
        block = ProjectTypeBlock(
            BlockManifest(
                block_id="project_type",
                name="Project Type",
                kind="analyzer",
                description="Classify the project type.",
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.data["project_type"], "explorer")
        self.assertIn("type_hint:explorer", result.data["reasons"])

    def test_project_type_detects_staking_product_from_case_description(self) -> None:
        context = self._make_context()
        context.case.description = "bemo is a TON liquid staking platform."
        context.results["github_repo"] = BlockResult(
            block_id="github_repo",
            status="success",
            summary="No repository details",
            data={
                "repo": {
                    "full_name": "",
                    "description": "",
                    "topics": [],
                },
                "readme_excerpt": "",
            },
        )
        block = ProjectTypeBlock(
            BlockManifest(
                block_id="project_type",
                name="Project Type",
                kind="analyzer",
                description="Classify the project type.",
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.data["project_type"], "staking_protocol")
        self.assertIn("text:liquid staking", result.data["reasons"])

    def test_project_type_keeps_protocol_service_type_hint(self) -> None:
        context = self._make_context()
        context.case.type_hint = "protocol_service"
        context.case.description = "TON DNS and .ton domain service."
        context.results["github_repo"] = BlockResult(
            block_id="github_repo",
            status="success",
            summary="Loaded repo ton-blockchain/dns-contract",
            data={
                "repo": {
                    "full_name": "ton-blockchain/dns-contract",
                    "description": "TON DNS Smart Contracts",
                    "topics": [],
                },
                "readme_excerpt": "",
            },
        )
        block = ProjectTypeBlock(
            BlockManifest(
                block_id="project_type",
                name="Project Type",
                kind="analyzer",
                description="Classify the project type.",
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.data["project_type"], "protocol_service")
        self.assertIn("type_hint:protocol_service", result.data["reasons"])

    def test_project_type_keeps_tooling_api_type_hint(self) -> None:
        context = self._make_context()
        context.case.type_hint = "tooling_api"
        context.case.description = "Public API platform for TON."
        context.results["github_repo"] = BlockResult(
            block_id="github_repo",
            status="success",
            summary="Loaded repo tonkeeper/tonapi-go",
            data={
                "repo": {
                    "full_name": "tonkeeper/tonapi-go",
                    "description": "Public API platform for TON.",
                    "topics": [],
                },
                "readme_excerpt": "",
            },
        )
        block = ProjectTypeBlock(
            BlockManifest(
                block_id="project_type",
                name="Project Type",
                kind="analyzer",
                description="Classify the project type.",
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.data["project_type"], "tooling_api")
        self.assertIn("type_hint:tooling_api", result.data["reasons"])

    def test_telegram_block_extracts_posts(self) -> None:
        context = run_pipeline("ston_fi_sdk", target_blocks=["telegram_channel"])
        result = context.results["telegram_channel"]
        self.assertEqual(result.status, "success")
        self.assertGreater(result.metrics["post_count"], 0)
        self.assertIn("community_activity_score", result.metrics)
        self.assertTrue(result.data["entries"])

    def test_contract_analysis_optional_for_protocol_case(self) -> None:
        context = run_pipeline("ton_blockchain_ton", target_blocks=["contract_validator"])
        result = context.results["contract_validator"]
        self.assertEqual(result.status, "success")
        self.assertEqual(result.data["contract_analysis_mode"], "optional")
        self.assertGreaterEqual(result.metrics["contract_file_count"], 1)

    def test_github_activity_detects_stale_repo(self) -> None:
        context = run_pipeline("ton_punks", target_blocks=["github_activity"])
        result = context.results["github_activity"]
        self.assertEqual(result.status, "success")
        self.assertGreaterEqual(result.metrics["last_commit_age_days"], 365)
        self.assertIn("repo_is_stale", result.flags)

    def test_github_tree_skips_when_source_is_incomplete(self) -> None:
        context = self._make_context()
        context.case.github_repo = "ston-fi/dex-core"
        context.results["github_repo"] = BlockResult(
            block_id="github_repo",
            status="success",
            summary="Loaded repo ston-fi/dex-core",
            data={"repo": {"default_branch": "main"}},
        )
        block = GitHubTreeBlock(
            BlockManifest(
                block_id="github_tree",
                name="GitHub Tree",
                kind="collector",
                description="Load repository tree.",
            )
        )
        with patch(
            "identity_validator.validators.github_tree.validator.get_github_tree",
            new=AsyncMock(
                return_value={
                    "tree": [],
                    "_source_status": "incomplete",
                    "_source_summary": "GitHub tree API is rate limited in interactive mode.",
                }
            ),
        ):
            result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "skipped")
        self.assertEqual(result.summary, "GitHub tree API is rate limited in interactive mode.")
        self.assertIn("github_tree_source_incomplete", result.flags)

    def test_github_activity_skips_when_source_is_incomplete(self) -> None:
        context = self._make_context()
        context.case.github_repo = "ston-fi/dex-core"
        context.results["github_repo"] = BlockResult(
            block_id="github_repo",
            status="success",
            summary="Loaded repo ston-fi/dex-core",
            data={"repo": {"default_branch": "main"}},
        )
        block = GitHubActivityBlock(
            BlockManifest(
                block_id="github_activity",
                name="GitHub Activity",
                kind="collector",
                description="Load repository activity.",
            )
        )
        with patch(
            "identity_validator.validators.github_activity.validator.get_github_activity_bundle",
            new=AsyncMock(
                return_value={
                    "commits": [],
                    "releases": [],
                    "commit_pages_loaded": 0,
                    "commit_page_limit_hit": False,
                    "_source_status": "incomplete",
                    "_source_summary": "GitHub activity API is rate limited in interactive mode.",
                }
            ),
        ):
            result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "skipped")
        self.assertEqual(result.summary, "GitHub activity API is rate limited in interactive mode.")
        self.assertIn("github_activity_source_incomplete", result.flags)

    def test_project_similarity_returns_registry_matches(self) -> None:
        context = run_pipeline("ton_punks", target_blocks=["project_similarity"])
        result = context.results["project_similarity"]
        self.assertEqual(result.status, "success")
        self.assertGreaterEqual(result.metrics["closest_projects_count"], 1)
        self.assertEqual(result.data["clone_risk"], "low")
        self.assertTrue(result.data["closest_projects"])
        self.assertEqual(result.data["clone_source_project"], {})
        self.assertIn("self_declared_repository_copy", result.flags)
        self.assertTrue(result.data["self_declared_copy_excerpt"])

    def test_telegram_semantics_block(self) -> None:
        context = run_pipeline("ton_punks", target_blocks=["telegram_semantics"])
        result = context.results["telegram_semantics"]
        self.assertEqual(result.status, "success")
        self.assertGreaterEqual(result.metrics["community_health_score"], 0)
        self.assertLessEqual(result.metrics["community_health_score"], 100)
        self.assertEqual(result.text, "")
        self.assertTrue(result.data["content_labels"])

    def test_keyword_hits_require_term_boundaries(self) -> None:
        self.assertEqual(keyword_hits("Trade on x1000.finance today", ("x100",)), [])
        self.assertEqual(keyword_hits("This token can do x100 this cycle", ("x100",)), ["x100"])

    def test_scam_keyword_hits_ignore_anti_scam_seed_phrase_warning(self) -> None:
        safe_text = "The team will never DM you first. Never share your seed phrase with anyone."
        self.assertEqual(scam_keyword_hits(safe_text), [])
        risky_text = "Connect your wallet and share your seed phrase to claim the reward."
        self.assertEqual(scam_keyword_hits(risky_text), ["seed phrase"])

    def test_telegram_channel_ignores_anti_scam_warning_and_x1000_brand(self) -> None:
        context = self._make_context(telegram_handle="dedust")
        block = TelegramChannelBlock(
            BlockManifest(
                block_id="telegram_channel",
                name="Telegram Channel",
                kind="collector",
                description="Collect Telegram channel activity.",
            )
        )
        snapshot = {
            "handle": "dedust",
            "source": "tgstat",
            "fetched_at": "2026-03-22T18:55:00Z",
            "entries": [
                {
                    "published_at": "2026-03-22T18:55:00Z",
                    "text": "The team will never DM you first. Never share your seed phrase.",
                },
                {
                    "published_at": "2026-03-21T18:55:00Z",
                    "text": "We launched x1000.finance trading terminal for TON users.",
                },
            ],
            "posts": [
                "The team will never DM you first. Never share your seed phrase.",
                "We launched x1000.finance trading terminal for TON users.",
            ],
        }
        with patch(
            "identity_validator.validators.telegram_channel.validator.get_telegram_snapshot",
            new=AsyncMock(return_value=snapshot),
        ):
            result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.metrics["scam_keyword_hits"], 0)
        self.assertNotIn("telegram_scam_terms_detected", result.flags)

    def test_telegram_semantics_ignore_anti_scam_warning_and_x1000_brand(self) -> None:
        context = self._make_context(telegram_handle="dedust")
        context.results["telegram_channel"] = BlockResult(
            block_id="telegram_channel",
            status="success",
            summary="Loaded Telegram snapshot with 3 posts from tgstat",
            metrics={
                "posts_30d": 3,
                "active_days_30d": 3,
                "last_post_age_days": 1,
            },
            data={
                "entries": [
                    {
                        "published_at": "2026-03-22T18:55:00Z",
                        "text": "The team will never DM you first. Never share your seed phrase.",
                    },
                    {
                        "published_at": "2026-03-21T18:55:00Z",
                        "text": "We launched x1000.finance trading terminal for TON users.",
                    },
                    {
                        "published_at": "2026-03-20T18:55:00Z",
                        "text": "Routing improvements are now live on the swap page.",
                    },
                ]
            },
        )
        block = TelegramSemanticsBlock(
            BlockManifest(
                block_id="telegram_semantics",
                name="Telegram Semantics",
                kind="analyzer",
                description="Analyze Telegram channel semantics.",
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.metrics["scam_post_count"], 0)
        self.assertNotIn("telegram_semantic_scam_signals", result.flags)
        self.assertFalse(result.needs_human_review)

    def test_address_signal_marks_known_jetton_match_from_ton_mcp(self) -> None:
        context = self._make_context()
        context.case.wallet_address = "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT"
        context.case.project_url = "https://notcoin.ton"
        block = AddressSignalBlock(
            BlockManifest(
                block_id="address_signal",
                name="Address Signal",
                kind="collector",
                description="Extract TON-style addresses and enrich them with TON MCP.",
            )
        )
        with patch(
            "identity_validator.validators.address_signal.validator.get_ton_mcp_known_jettons",
            new=AsyncMock(
                return_value={
                    "status": "success",
                    "summary": "TON MCP returned 1 known jetton.",
                    "count": 1,
                    "jettons": [
                        {
                            "symbol": "NOT",
                            "name": "Notcoin",
                            "address": "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT",
                            "decimals": 9,
                        }
                    ],
                }
            ),
        ), patch(
            "identity_validator.validators.address_signal.validator.get_ton_account_activity",
            new=AsyncMock(
                return_value={
                    "status": "success",
                    "summary": "Observed 5 address transactions in the last 30 days.",
                    "address": "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT",
                    "last_tx_at": "2026-03-20T10:00:00Z",
                    "last_tx_age_days": 4,
                    "tx_count_7d": 2,
                    "tx_count_30d": 5,
                    "tx_count_30d_limit_hit": False,
                    "sample_transactions": [],
                }
            ),
        ), patch(
            "identity_validator.validators.address_signal.validator.get_ton_mcp_back_resolve_dns",
            new=AsyncMock(
                return_value={
                    "status": "success",
                    "summary": "TON MCP reverse-resolved the project address.",
                    "address": "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT",
                    "domain": "notcoin.ton",
                }
            ),
        ), patch(
            "identity_validator.validators.address_signal.validator.get_ton_mcp_balance_by_address",
            new=AsyncMock(
                return_value={
                    "status": "success",
                    "summary": "TON MCP returned the project balance.",
                    "address": "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT",
                    "balance": "123.456 TON",
                    "balance_nano": "123456000000",
                }
            ),
        ), patch(
            "identity_validator.validators.address_signal.validator.get_ton_mcp_resolve_dns",
            new=AsyncMock(
                return_value={
                    "status": "success",
                    "summary": "TON MCP resolved notcoin.ton.",
                    "domain": "notcoin.ton",
                    "address": "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT",
                }
            ),
        ):
            result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.metrics["ton_mcp_known_jetton_match"], 1)
        self.assertEqual(result.metrics["ton_address_activity_checked"], 1)
        self.assertEqual(result.metrics["ton_address_last_tx_age_days"], 4)
        self.assertEqual(result.metrics["ton_address_tx_count_30d"], 5)
        self.assertEqual(result.metrics["ton_mcp_reverse_dns_found"], 1)
        self.assertEqual(result.metrics["ton_mcp_balance_checked"], 1)
        self.assertEqual(result.metrics["ton_mcp_dns_match"], 1)
        self.assertEqual(result.data["ton_mcp"]["matched_jetton"]["symbol"], "NOT")
        self.assertEqual(result.data["ton_activity"]["tx_count_7d"], 2)
        self.assertEqual(result.data["ton_mcp"]["reverse_dns"]["domain"], "notcoin.ton")
        self.assertEqual(result.data["ton_mcp"]["balance"]["balance"], "123.456 TON")
        self.assertEqual(result.data["ton_mcp"]["dns_match"]["domain"], "notcoin.ton")

    def test_address_signal_keeps_working_when_ton_mcp_lookup_fails(self) -> None:
        context = self._make_context()
        context.case.wallet_address = "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT"
        block = AddressSignalBlock(
            BlockManifest(
                block_id="address_signal",
                name="Address Signal",
                kind="collector",
                description="Extract TON-style addresses and enrich them with TON MCP.",
            )
        )
        with patch(
            "identity_validator.validators.address_signal.validator.get_ton_mcp_known_jettons",
            new=AsyncMock(
                return_value={
                    "status": "error",
                    "summary": "TON MCP known jetton lookup failed: test error",
                    "count": 0,
                    "jettons": [],
                }
            ),
        ), patch(
            "identity_validator.validators.address_signal.validator.get_ton_account_activity",
            new=AsyncMock(
                return_value={
                    "status": "error",
                    "summary": "activity failed",
                    "address": context.case.wallet_address,
                    "last_tx_at": "",
                    "last_tx_age_days": -1,
                    "tx_count_7d": 0,
                    "tx_count_30d": 0,
                    "tx_count_30d_limit_hit": False,
                    "sample_transactions": [],
                }
            ),
        ), patch(
            "identity_validator.validators.address_signal.validator.get_ton_mcp_back_resolve_dns",
            new=AsyncMock(return_value={"status": "error", "summary": "reverse dns failed", "address": context.case.wallet_address, "domain": ""}),
        ), patch(
            "identity_validator.validators.address_signal.validator.get_ton_mcp_balance_by_address",
            new=AsyncMock(return_value={"status": "error", "summary": "balance failed", "address": context.case.wallet_address, "balance": "", "balance_nano": ""}),
        ):
            result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.metrics["ton_mcp_known_jetton_match"], 0)
        self.assertEqual(result.data["ton_mcp"]["status"], "error")
        self.assertEqual(result.metrics["ton_address_activity_checked"], 0)
        self.assertEqual(result.metrics["ton_mcp_reverse_dns_found"], 0)
        self.assertEqual(result.metrics["ton_mcp_balance_checked"], 0)

    def test_identity_confirmation_detects_brand_mismatch(self) -> None:
        context = self._make_context(telegram_handle="toncoin")
        context.case.name = "TON Blockchain"
        context.case.requested_input = "EVAA TON lending protocol"
        context.case.description = "EVAA TON lending protocol"
        context.case.project_url = "https://github.com/ton-blockchain/ton"
        context.case.github_repo = "ton-blockchain/ton"
        context.results["github_repo"] = BlockResult(
            block_id="github_repo",
            status="success",
            summary="Loaded repo ton-blockchain/ton",
            data={
                "repo": {
                    "full_name": "ton-blockchain/ton",
                    "description": "TON blockchain monorepo",
                    "homepage": "",
                }
            },
        )
        context.results["telegram_channel"] = BlockResult(
            block_id="telegram_channel",
            status="success",
            summary="Loaded Telegram snapshot",
            data={"handle": "toncoin"},
        )
        context.results["address_signal"] = BlockResult(
            block_id="address_signal",
            status="success",
            summary="No wallet signals",
            data={"unique_addresses": []},
        )
        context.results["project_registry"] = BlockResult(
            block_id="project_registry",
            status="success",
            summary="Loaded curated registry",
            data={"profiles": []},
        )
        block = IdentityConfirmationBlock(
            BlockManifest(
                block_id="identity_confirmation",
                name="Identity Confirmation",
                kind="validator",
                dependencies=["github_repo", "telegram_channel", "address_signal", "project_registry"],
                description="Confirms that the selected candidate matches the requested project.",
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.data["identity_status"], "mismatch")
        self.assertIn("identity_brand_mismatch", result.flags)
        self.assertIn("identity_based_on_noncanonical_reference", result.flags)

    def test_identity_confirmation_accepts_exact_repo_match_with_telegram_signal(self) -> None:
        context = self._make_context(telegram_handle="toncoin")
        context.case.name = "TON Blockchain"
        context.case.description = "Reference TON blockchain implementation and core infrastructure."
        context.case.project_url = "https://github.com/ton-blockchain/ton"
        context.case.github_repo = "ton-blockchain/ton"
        context.results["github_repo"] = BlockResult(
            block_id="github_repo",
            status="success",
            summary="Loaded repo ton-blockchain/ton",
            data={
                "repo": {
                    "full_name": "ton-blockchain/ton",
                    "description": "Main TON monorepo",
                    "homepage": "",
                }
            },
        )
        context.results["telegram_channel"] = BlockResult(
            block_id="telegram_channel",
            status="success",
            summary="Loaded Telegram snapshot",
            data={"handle": "toncoin"},
        )
        context.results["address_signal"] = BlockResult(
            block_id="address_signal",
            status="success",
            summary="No wallet signals",
            data={"unique_addresses": []},
        )
        context.results["project_registry"] = BlockResult(
            block_id="project_registry",
            status="success",
            summary="Loaded curated registry",
            data={"profiles": []},
        )
        block = IdentityConfirmationBlock(
            BlockManifest(
                block_id="identity_confirmation",
                name="Identity Confirmation",
                kind="validator",
                dependencies=["github_repo", "telegram_channel", "address_signal", "project_registry"],
                description="Confirms that the selected candidate matches the requested project.",
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.data["identity_status"], "confirmed")
        self.assertEqual(result.metrics["exact_repo_match"], 1)
        self.assertTrue(result.data["exact_repo_match"])
        self.assertIn("github_repo_exact", result.data["corroborating_signals"])
        self.assertNotIn("identity_brand_mismatch", result.flags)

    def test_identity_confirmation_accepts_canonical_domain_when_repo_is_unavailable(self) -> None:
        context = self._make_context(telegram_handle="tonkeeper_news")
        context.case.name = "Tonkeeper"
        context.case.description = "Tonkeeper TON wallet"
        context.case.project_url = "https://tonkeeper.com"
        context.case.github_repo = "tonkeeper/wallet"
        context.results["github_repo"] = BlockResult(
            block_id="github_repo",
            status="error",
            summary="GitHub source rate-limited",
        )
        context.results["telegram_channel"] = BlockResult(
            block_id="telegram_channel",
            status="success",
            summary="Loaded Telegram snapshot",
            data={"handle": "tonkeeper_news"},
        )
        context.results["address_signal"] = BlockResult(
            block_id="address_signal",
            status="success",
            summary="No wallet signals",
            data={"unique_addresses": []},
        )
        context.results["project_registry"] = BlockResult(
            block_id="project_registry",
            status="success",
            summary="Loaded curated registry",
            data={"profiles": []},
        )
        block = IdentityConfirmationBlock(
            BlockManifest(
                block_id="identity_confirmation",
                name="Identity Confirmation",
                kind="validator",
                dependencies=["github_repo", "telegram_channel", "address_signal", "project_registry"],
                description="Confirms that the selected candidate matches the requested project.",
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.data["identity_status"], "confirmed")
        self.assertEqual(result.data["evidence_status"], "sufficient")
        self.assertIn("github_source_unavailable", result.flags)
        self.assertNotIn("identity_brand_mismatch", result.flags)

    def test_identity_confirmation_accepts_telegram_entrypoint_when_snapshot_is_unavailable(self) -> None:
        context = self._make_context(telegram_handle="tonswap_org")
        context.case.name = "Tonswap"
        context.case.requested_input = "Tonswap TON DEX"
        context.case.description = "Decentralized exchange on TON."
        context.case.project_url = "https://tonswap.org"
        context.results["github_repo"] = BlockResult(
            block_id="github_repo",
            status="skipped",
            summary="GitHub repository is not provided",
        )
        context.results["telegram_channel"] = BlockResult(
            block_id="telegram_channel",
            status="skipped",
            summary="Telegram snapshot is unavailable",
        )
        context.results["address_signal"] = BlockResult(
            block_id="address_signal",
            status="success",
            summary="No wallet signals",
            data={"unique_addresses": []},
        )
        context.results["project_registry"] = BlockResult(
            block_id="project_registry",
            status="success",
            summary="Loaded curated registry",
            data={"profiles": []},
        )
        block = IdentityConfirmationBlock(
            BlockManifest(
                block_id="identity_confirmation",
                name="Identity Confirmation",
                kind="validator",
                dependencies=["github_repo", "telegram_channel", "address_signal", "project_registry"],
                description="Confirms that the selected candidate matches the requested project.",
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.data["identity_status"], "confirmed")
        self.assertEqual(result.data["evidence_status"], "sufficient")
        self.assertIn("telegram_source_unavailable", result.flags)
        self.assertIn("telegram_reference", result.data["corroborating_signals"])

    def test_identity_confirmation_uses_case_name_for_brand_overlap(self) -> None:
        context = self._make_context(telegram_handle="bemofinance")
        context.case.name = "bemo"
        context.case.requested_input = "bemo TON liquid staking"
        context.case.description = "Liquid staking protocol on TON."
        context.case.project_url = "https://bemo.fi"
        context.results["github_repo"] = BlockResult(
            block_id="github_repo",
            status="error",
            summary="GitHub source unavailable",
        )
        context.results["telegram_channel"] = BlockResult(
            block_id="telegram_channel",
            status="success",
            summary="Loaded Telegram snapshot",
            data={"handle": "bemofinance"},
        )
        context.results["address_signal"] = BlockResult(
            block_id="address_signal",
            status="success",
            summary="No wallet signals",
            data={"unique_addresses": []},
        )
        context.results["project_registry"] = BlockResult(
            block_id="project_registry",
            status="success",
            summary="Loaded curated registry",
            data={"profiles": []},
        )
        block = IdentityConfirmationBlock(
            BlockManifest(
                block_id="identity_confirmation",
                name="Identity Confirmation",
                kind="validator",
                dependencies=["github_repo", "telegram_channel", "address_signal", "project_registry"],
                description="Confirms that the selected candidate matches the requested project.",
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.data["identity_status"], "confirmed")
        self.assertNotIn("identity_brand_mismatch", result.flags)

    def test_select_canonical_project_url_rejects_file_like_domains(self) -> None:
        url = _select_canonical_project_url(
            ["https://u003eblueprint.config.ts", "https://blueprint.ton.org"],
            "ton-org/blueprint",
            "Official all-in-one TON development tool for writing smart contracts.",
        )
        self.assertEqual(url, "https://blueprint.ton.org")

    def test_select_canonical_project_url_strips_trailing_backslash(self) -> None:
        url = _select_canonical_project_url(
            ["https://ston.fi\\"],
            "ston-fi/sdk",
            "Official SDK for building on STON.fi protocol.",
        )
        self.assertEqual(url, "https://ston.fi")

    def test_fetch_github_repo_meta_replaces_invalid_api_homepage_from_html(self) -> None:
        api_meta = {
            "full_name": "getgems-io/nft-contracts",
            "description": "Set of TON NFT related contracts & code",
            "homepage": "https://code-0eb15370f045c7e0.css",
        }
        page_html = """
        <html>
          <head>
            <meta property="og:description" content="Set of TON NFT related contracts & code - getgems-io/nft-contracts" />
          </head>
          <body>
            <span aria-label="Homepage"></span>
            <a href="https://getgems.io">Website</a>
          </body>
        </html>
        """
        with patch("identity_validator.sources.http_fetch_json", new=AsyncMock(return_value=api_meta)):
            with patch("identity_validator.sources.http_fetch_text", new=AsyncMock(return_value=page_html)):
                result = asyncio.run(fetch_github_repo_meta("getgems-io/nft-contracts"))
        self.assertEqual(result.get("homepage"), "https://getgems.io")

    def test_git_run_repo_mutation_recovers_from_stale_lock(self) -> None:
        with tempfile.TemporaryDirectory(prefix="git_lock_repo_") as temp_dir:
            repo_dir = Path(temp_dir)
            git_dir = repo_dir / ".git"
            git_dir.mkdir(parents=True, exist_ok=True)
            lock_path = git_dir / "shallow.lock"
            lock_path.write_text("stale", encoding="utf-8")
            calls = []

            def fake_git_run(args, cwd=None, timeout=180):
                calls.append((list(args), cwd, timeout))
                if len(calls) == 1:
                    raise RuntimeError(
                        f"fatal: Unable to create '{lock_path}': File exists. "
                        "Another git process seems to be running in this repository."
                    )
                return "ok"

            with patch("identity_validator.sources._git_run", side_effect=fake_git_run):
                result = _git_run_repo_mutation(["git", "fetch", "--deepen", "200", "origin", "main"], repo_dir, timeout=300)

        self.assertEqual(result, "ok")
        self.assertEqual(len(calls), 2)
        self.assertFalse(lock_path.exists())

    def test_ensure_github_git_bundle_stops_when_repository_is_not_shallow(self) -> None:
        with tempfile.TemporaryDirectory(prefix="git_depth_repo_") as temp_dir:
            repo_dir = Path(temp_dir)
            (repo_dir / ".git").mkdir(parents=True, exist_ok=True)
            fetch_calls = []

            def fake_git_run(args, cwd=None, timeout=180):
                if args[:4] == ["git", "log", "--reverse", "--format=%cI"]:
                    return "2026-03-08T20:06:40+01:00"
                if args[:3] == ["git", "rev-list", "--count"]:
                    return "92"
                if args[:3] == ["git", "rev-parse", "--is-shallow-repository"]:
                    return "false"
                if args[:2] == ["git", "fetch"]:
                    fetch_calls.append(list(args))
                    return ""
                raise AssertionError(f"Unexpected git command: {args}")

            with patch.dict("identity_validator.sources._GITHUB_GIT_BUNDLE_CACHE", {}, clear=True):
                with patch("identity_validator.sources._git_cache_path", return_value=repo_dir):
                    with patch("identity_validator.sources._git_cache_lock_path", return_value=repo_dir.parent / "ston-fi__sdk.test.lock"):
                        with patch("identity_validator.sources._git_run", side_effect=fake_git_run):
                            result = _ensure_github_git_bundle_sync("ston-fi/sdk", "main", 400)

        self.assertEqual(result["path"], str(repo_dir))
        self.assertEqual(result["default_branch"], "main")
        self.assertEqual(fetch_calls, [])

    def test_ensure_github_git_bundle_stops_after_fetch_without_progress(self) -> None:
        with tempfile.TemporaryDirectory(prefix="git_depth_stall_") as temp_dir:
            repo_dir = Path(temp_dir)
            (repo_dir / ".git").mkdir(parents=True, exist_ok=True)
            fetch_calls = []

            def fake_git_run(args, cwd=None, timeout=180):
                if args[:4] == ["git", "log", "--reverse", "--format=%cI"]:
                    return "2026-03-23T15:53:21+03:00"
                if args[:3] == ["git", "rev-list", "--count"]:
                    return "140"
                if args[:3] == ["git", "rev-parse", "--is-shallow-repository"]:
                    return "true"
                if args[:2] == ["git", "fetch"]:
                    fetch_calls.append(list(args))
                    return ""
                raise AssertionError(f"Unexpected git command: {args}")

            with patch.dict("identity_validator.sources._GITHUB_GIT_BUNDLE_CACHE", {}, clear=True):
                with patch("identity_validator.sources._git_cache_path", return_value=repo_dir):
                    with patch("identity_validator.sources._git_cache_lock_path", return_value=repo_dir.parent / "getgems-io__nft-contracts.test.lock"):
                        with patch("identity_validator.sources._git_run", side_effect=fake_git_run):
                            result = _ensure_github_git_bundle_sync("getgems-io/nft-contracts", "main", 400)

        self.assertEqual(result["path"], str(repo_dir))
        self.assertEqual(fetch_calls, [["git", "fetch", "--deepen", "200", "origin", "main"]])

    def test_rule_engine_ignores_skipped_source_flags_in_final_risks(self) -> None:
        context = self._make_context()
        context.results["github_repo"] = BlockResult(block_id="github_repo", status="skipped", summary="repo unavailable")
        context.results["github_tree"] = BlockResult(block_id="github_tree", status="skipped", summary="tree unavailable")
        context.results["telegram_channel"] = BlockResult(
            block_id="telegram_channel",
            status="success",
            summary="Loaded Telegram snapshot",
            metrics={"community_activity_score": 44, "ton_keyword_hits": 2, "scam_keyword_hits": 0},
        )
        context.results["telegram_semantics"] = BlockResult(block_id="telegram_semantics", status="skipped", summary="semantics unavailable")
        context.results["github_activity"] = BlockResult(block_id="github_activity", status="skipped", summary="activity unavailable")
        context.results["project_type"] = BlockResult(
            block_id="project_type",
            status="skipped",
            summary="Repository signals are unavailable",
            flags=["missing_repo_signals"],
        )
        context.results["address_signal"] = BlockResult(block_id="address_signal", status="success", summary="No wallet signals", data={})
        context.results["contract_validator"] = BlockResult(
            block_id="contract_validator",
            status="skipped",
            summary="Project type is unavailable",
            flags=["missing_project_type"],
        )
        context.results["claim_consistency"] = BlockResult(
            block_id="claim_consistency",
            status="success",
            summary="Consistency check found 1 mismatch",
            metrics={"alignment_score": 85},
            data={"mismatches": ["identity_is_weakly_confirmed"]},
            flags=["identity_is_weakly_confirmed"],
        )
        context.results["risk_validator"] = BlockResult(
            block_id="risk_validator",
            status="success",
            summary="Risk level is low with score 8",
            metrics={"risk_score": 8},
            data={"risk_level": "low"},
            flags=["identity_unconfirmed"],
        )
        context.results["sonar_research"] = BlockResult(block_id="sonar_research", status="skipped", summary="disabled")
        context.results["project_similarity"] = BlockResult(
            block_id="project_similarity",
            status="skipped",
            summary="Repository signals are unavailable",
            data={"clone_risk": "unknown"},
            flags=["missing_repo_signals"],
        )
        context.results["identity_confirmation"] = BlockResult(
            block_id="identity_confirmation",
            status="success",
            summary="identity=weak evidence=partial signals=1",
            metrics={"identity_score": 58},
            data={"identity_status": "weak", "evidence_status": "partial", "source_failures": []},
            flags=["identity_unconfirmed"],
        )
        block = RuleEngineBlock(
            BlockManifest(
                block_id="rule_engine",
                name="Rule Engine",
                kind="synthesizer",
                dependencies=[
                    "github_repo",
                    "github_tree",
                    "telegram_channel",
                    "telegram_semantics",
                    "project_type",
                    "address_signal",
                    "contract_validator",
                    "claim_consistency",
                    "risk_validator",
                    "sonar_research",
                    "github_activity",
                    "project_registry",
                    "project_similarity",
                    "identity_confirmation",
                ],
                description="Combines validator outputs into transparent scores.",
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertIn("identity_unconfirmed", result.data["risks"])
        self.assertNotIn("missing_project_type", result.data["risks"])
        self.assertNotIn("missing_repo_signals", result.data["risks"])

    def test_sonar_research_handles_missing_repo_payload(self) -> None:
        context = self._make_context()
        context.results["github_repo"] = BlockResult(
            block_id="github_repo",
            status="skipped",
            summary="GitHub repository is not provided",
            data={"repo": None},
        )
        context.results["project_type"] = BlockResult(
            block_id="project_type",
            status="success",
            summary="Project type resolved",
            data={"project_type": "token"},
        )
        block = SonarResearchBlock(
            BlockManifest(
                block_id="sonar_research",
                name="Sonar Research Block",
                kind="analyzer",
                dependencies=["github_repo", "project_type"],
                description="Optional public-web research block powered by Sonar-compatible model.",
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertIn("research_text", result.data)

    def test_llm_explainer_returns_grounded_structured_payload(self) -> None:
        context = self._make_context(telegram_handle="tonkeeper")
        context.case.name = "Tonkeeper | @tonkeeper"
        context.case.description = "Self-custodial wallet for TON and Telegram users."
        context.case.github_repo = "tonkeeper/tonkeeper-web"
        context.case.project_url = "https://tonkeeper.com"
        context.results["rule_engine"] = BlockResult(
            block_id="rule_engine",
            status="success",
            summary="type=wallet_app overall=84 risk=low identity=confirmed clone=low",
            metrics={
                "overall_score": 84,
                "activity_score": 61,
                "originality_score": 80,
                "community_activity_score": 73,
                "community_quality_score": 76,
                "identity_score": 94,
            },
            data={
                "project_type": "wallet_app",
                "risk_level": "low",
                "identity_status": "confirmed",
                "evidence_status": "sufficient",
                "clone_risk": "low",
                "strengths": ["identity_confirmed", "recent_onchain_activity"],
                "risks": [],
                "next_checks": ["optional_manual_review"],
                "community_findings": ["wallet_updates"],
            },
        )
        context.results["github_repo"] = BlockResult(
            block_id="github_repo",
            status="success",
            summary="Loaded repo tonkeeper/tonkeeper-web",
            data={
                "repo": {
                    "full_name": "tonkeeper/tonkeeper-web",
                    "description": "Self-custodial wallet for TON and Telegram users.",
                    "topics": ["wallet", "tonconnect"],
                },
                "readme_excerpt": "Tonkeeper is a self-custodial wallet for TON.",
            },
        )
        context.results["github_activity"] = BlockResult(
            block_id="github_activity",
            status="success",
            summary="Repository freshness metrics",
            metrics={"last_commit_age_days": 12, "commits_90d": 34},
        )
        context.results["project_similarity"] = BlockResult(
            block_id="project_similarity",
            status="success",
            summary="Similarity against registry computed",
            data={"clone_risk": "low", "closest_projects": [], "distinctive_features": ["tonconnect"]},
        )
        context.results["identity_confirmation"] = BlockResult(
            block_id="identity_confirmation",
            status="success",
            summary="identity=confirmed evidence=sufficient signals=3",
            data={
                "identity_status": "confirmed",
                "evidence_status": "sufficient",
                "canonical_domain": "tonkeeper.com",
                "corroborating_signals": ["canonical_domain", "telegram_channel", "wallet_signal"],
            },
        )
        context.results["telegram_channel"] = BlockResult(
            block_id="telegram_channel",
            status="success",
            summary="Loaded Telegram snapshot",
            data={"handle": "tonkeeper"},
            metrics={"last_post_age_days": 1, "posts_30d": 28, "active_days_30d": 20, "community_activity_score": 73},
        )
        context.results["telegram_semantics"] = BlockResult(
            block_id="telegram_semantics",
            status="success",
            summary="Telegram semantic risk=low topics=wallet_updates",
            metrics={"semantic_risk_score": 8, "community_health_score": 92, "promo_post_ratio": 0.1},
            data={"dominant_topics": ["wallet_updates"], "content_labels": ["wallet_updates"], "semantic_risk_level": "low"},
        )
        context.results["address_signal"] = BlockResult(
            block_id="address_signal",
            status="success",
            summary="Detected address activity",
            data={
                "ton_activity": {
                    "status": "success",
                    "address": "EQTESTTONKEEPERADDRESS",
                    "tx_count_7d": 7,
                    "tx_count_30d": 31,
                    "last_tx_age_days": 0,
                },
                "ton_mcp": {
                    "matched_jetton": {},
                    "dns_match": {},
                    "reverse_dns": {},
                    "balance": {},
                },
            },
        )
        context.results["contract_validator"] = BlockResult(
            block_id="contract_validator",
            status="success",
            summary="Contract optional for wallet case",
            metrics={"contract_score": 100, "contract_file_count": 0, "address_signal_count": 1},
            data={"addresses": ["EQTESTTONKEEPERADDRESS"]},
        )
        block = LLMExplainerBlock(
            BlockManifest(
                block_id="llm_explainer",
                name="LLM Explainer",
                kind="synthesizer",
                dependencies=[
                    "rule_engine",
                    "github_repo",
                    "github_activity",
                    "telegram_channel",
                    "telegram_semantics",
                    "address_signal",
                    "identity_confirmation",
                    "contract_validator",
                    "project_similarity",
                ],
                description="Produces a grounded bilingual explanation and project overview from collected evidence.",
            )
        )
        context.llm_client.complete = AsyncMock(
            return_value=json.dumps(
                {
                    "project_name": "Tonkeeper",
                    "project_type": "wallet_app",
                    "project_overview_en": "Tonkeeper is a self-custodial wallet for TON and Telegram users. It is designed for people who need direct control over TON assets. The repository description and public identity signals support this wallet use case.",
                    "project_overview_ru": "Tonkeeper — некастодиальный кошелек для TON и пользователей Telegram. Публичное описание репозитория и сигналы идентичности подтверждают этот сценарий использования. Он ориентирован на прямое управление TON-активами.",
                    "explanation_text_en": "Overall score is 84 and the risk level is low. Supporting signals include the official website and Telegram channel, while on-chain activity remains visible.",
                    "explanation_text_ru": "Итоговый балл 84, уровень риска низкий. Подтверждающие сигналы включают официальный сайт и Telegram-канал, а активность в блокчейне остается видимой.",
                    "evidence_used": [
                        "repository.description",
                        "identity_confirmation.corroborating_signals",
                        "ton_onchain.tx_count_30d",
                    ],
                },
                ensure_ascii=False,
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertTrue(result.text)
        self.assertIn("project_overview_en", result.data)
        self.assertIn("project_overview_ru", result.data)
        self.assertIn("explanation_text_en", result.data)
        self.assertIn("explanation_text_ru", result.data)
        self.assertIn("wallet for TON", result.data["project_overview_en"])
        self.assertTrue(any("А" <= ch <= "я" for ch in result.data["project_overview_ru"]))
        self.assertEqual(result.data["evidence_pack"]["telegram"]["semantic_risk_level"], "low")
        self.assertEqual(result.data["evidence_pack"]["telegram"]["community_health_score"], 92)
        self.assertFalse(result.flags)

    def test_llm_explainer_rejects_unknown_evidence_keys(self) -> None:
        context = self._make_context()
        context.case.name = "bemo"
        context.case.requested_input = "bemo TON liquid staking"
        context.case.description = "Liquid staking protocol on TON."
        context.case.project_url = "https://bemo.fi"
        context.case.telegram_handle = "bemofinance"
        context.results["rule_engine"] = BlockResult(
            block_id="rule_engine",
            status="success",
            summary="type=dapp_product overall=75 risk=low identity=confirmed clone=low",
            metrics={
                "overall_score": 75,
                "activity_score": 50,
                "originality_score": 50,
                "community_activity_score": 50,
                "community_quality_score": 79,
                "identity_score": 80,
            },
            data={
                "project_type": "dapp_product",
                "risk_level": "low",
                "identity_status": "confirmed",
                "evidence_status": "sufficient",
                "clone_risk": "low",
            },
        )
        context.results["identity_confirmation"] = BlockResult(
            block_id="identity_confirmation",
            status="success",
            summary="identity=confirmed",
            data={
                "identity_status": "confirmed",
                "evidence_status": "sufficient",
                "canonical_domain": "bemo.fi",
                "corroborating_signals": ["canonical_domain", "telegram_channel"],
                "source_failures": [],
                "brand_overlap_tokens": ["bemo"],
            },
        )
        context.results["telegram_channel"] = BlockResult(
            block_id="telegram_channel",
            status="success",
            summary="Loaded Telegram snapshot",
            metrics={"last_post_age_days": 182, "posts_30d": 0, "active_days_30d": 0, "community_activity_score": 0},
            data={"handle": "bemofinance"},
        )
        context.results["telegram_semantics"] = BlockResult(
            block_id="telegram_semantics",
            status="success",
            summary="Telegram semantic risk=low",
            metrics={"semantic_risk_score": 21, "promo_post_ratio": 0.4, "duplicate_post_ratio": 0.0, "near_duplicate_pair_ratio": 0.0},
            data={"dominant_topics": ["product_updates"], "content_labels": ["product_updates"]},
        )
        context.results["address_signal"] = BlockResult(
            block_id="address_signal",
            status="success",
            summary="No on-chain address",
            data={"ton_activity": {"tx_count_30d": 0, "last_tx_age_days": -1}, "ton_mcp": {}},
        )
        context.results["contract_validator"] = BlockResult(
            block_id="contract_validator",
            status="skipped",
            summary="Contract not applicable",
        )
        context.results["project_similarity"] = BlockResult(
            block_id="project_similarity",
            status="skipped",
            summary="Repository signals are unavailable",
        )
        block = LLMExplainerBlock(
            BlockManifest(
                block_id="llm_explainer",
                name="LLM Explainer",
                kind="synthesizer",
                dependencies=[
                    "rule_engine",
                    "github_repo",
                    "github_activity",
                    "telegram_channel",
                    "telegram_semantics",
                    "address_signal",
                    "identity_confirmation",
                    "contract_validator",
                    "project_similarity",
                ],
                description="Produces a grounded bilingual explanation and project overview from collected evidence.",
            )
        )
        context.llm_client.complete = AsyncMock(
            return_value=json.dumps(
                {
                    "project_name": "bemo",
                    "project_type": "dapp_product",
                    "project_overview_en": "bemo is a liquid staking protocol on TON.",
                    "project_overview_ru": "bemo — это протокол ликвидного стекинга на TON.",
                    "explanation_text_en": "Overall score: 75. Risk level: low.",
                    "explanation_text_ru": "Общий балл: 75. Уровень риска: низкий.",
                    "evidence_used": ["identity_confirmation.corresponding_signals"],
                },
                ensure_ascii=False,
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.text, "")
        self.assertIn("llm_invalid_evidence_keys", result.flags)
        self.assertIn("llm_grounding_incomplete", result.flags)

    def test_llm_explainer_rejects_invalid_identifier_mentions(self) -> None:
        context = self._make_context()
        context.case.name = "Tonswap"
        context.case.requested_input = "Tonswap TON DEX"
        context.case.description = "Decentralized exchange on TON."
        context.case.project_url = "https://tonswap.org"
        context.case.telegram_handle = "tonswap_org"
        context.results["rule_engine"] = BlockResult(
            block_id="rule_engine",
            status="success",
            summary="type=dapp_product overall=77 risk=low identity=confirmed clone=low",
            metrics={
                "overall_score": 77,
                "identity_score": 80,
            },
            data={
                "project_type": "dapp_product",
                "risk_level": "low",
                "identity_status": "confirmed",
                "evidence_status": "sufficient",
                "clone_risk": "low",
            },
        )
        context.results["identity_confirmation"] = BlockResult(
            block_id="identity_confirmation",
            status="success",
            summary="identity=confirmed",
            data={
                "identity_status": "confirmed",
                "evidence_status": "sufficient",
                "canonical_domain": "tonswap.org",
                "corroborating_signals": ["canonical_domain", "telegram_reference"],
                "source_failures": [],
                "brand_overlap_tokens": ["tonswap"],
            },
        )
        context.results["telegram_channel"] = BlockResult(
            block_id="telegram_channel",
            status="skipped",
            summary="Telegram snapshot unavailable",
            data={"handle": "tonswap_org"},
        )
        context.results["telegram_semantics"] = BlockResult(
            block_id="telegram_semantics",
            status="skipped",
            summary="Telegram semantics unavailable",
        )
        context.results["address_signal"] = BlockResult(
            block_id="address_signal",
            status="success",
            summary="No on-chain address",
            data={"ton_activity": {"tx_count_30d": 0, "last_tx_age_days": -1}, "ton_mcp": {}},
        )
        context.results["contract_validator"] = BlockResult(
            block_id="contract_validator",
            status="skipped",
            summary="Contract not applicable",
        )
        context.results["project_similarity"] = BlockResult(
            block_id="project_similarity",
            status="skipped",
            summary="Repository signals are unavailable",
        )
        block = LLMExplainerBlock(
            BlockManifest(
                block_id="llm_explainer",
                name="LLM Explainer",
                kind="synthesizer",
                dependencies=[
                    "rule_engine",
                    "github_repo",
                    "github_activity",
                    "telegram_channel",
                    "telegram_semantics",
                    "address_signal",
                    "identity_confirmation",
                    "contract_validator",
                    "project_similarity",
                ],
                description="Produces a grounded bilingual explanation and project overview from collected evidence.",
            )
        )
        context.llm_client.complete = AsyncMock(
            return_value=json.dumps(
                {
                    "project_name": "Tonswap",
                    "project_type": "dapp_product",
                    "project_overview_en": "Tonswap uses the website tonwsap.org and Telegram handle @tonswap_org as its public entry points.",
                    "project_overview_ru": "Tonswap uses the website tonwsap.org and Telegram handle @tonswap_org as its public entry points.",
                    "explanation_text_en": "Overall score: 77. Risk level: low. Identity confirmation is supported by tonwsap.org.",
                    "explanation_text_ru": "Overall score: 77. Risk level: low. Identity confirmation is supported by tonwsap.org.",
                    "evidence_used": [
                        "identity_confirmation.canonical_domain",
                        "requested_entity.case_description",
                        "overall_score",
                    ],
                },
                ensure_ascii=False,
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertEqual(result.text, "")
        self.assertIn("llm_invalid_identifier_mentions", result.flags)
        self.assertIn("llm_grounding_incomplete", result.flags)

    def test_llm_explainer_marks_onchain_metrics_unavailable_without_address(self) -> None:
        context = self._make_context()
        context.case.name = "Tonswap"
        context.case.requested_input = "Tonswap TON DEX"
        context.case.description = "Decentralized exchange on TON."
        context.case.project_url = "https://tonswap.org"
        context.case.telegram_handle = "tonswap_org"
        context.results["rule_engine"] = BlockResult(
            block_id="rule_engine",
            status="success",
            summary="type=dapp_product overall=77 risk=low identity=confirmed clone=low",
            metrics={
                "overall_score": 77,
                "identity_score": 80,
            },
            data={
                "project_type": "dapp_product",
                "risk_level": "low",
                "identity_status": "confirmed",
                "evidence_status": "sufficient",
                "clone_risk": "low",
            },
        )
        context.results["identity_confirmation"] = BlockResult(
            block_id="identity_confirmation",
            status="success",
            summary="identity=confirmed",
            data={
                "identity_status": "confirmed",
                "evidence_status": "sufficient",
                "canonical_domain": "tonswap.org",
                "corroborating_signals": ["canonical_domain", "telegram_reference"],
                "source_failures": [],
                "brand_overlap_tokens": ["tonswap"],
            },
        )
        context.results["address_signal"] = BlockResult(
            block_id="address_signal",
            status="success",
            summary="No on-chain address",
            data={"ton_activity": {"status": "skipped", "tx_count_30d": 0, "last_tx_age_days": -1}, "ton_mcp": {}},
        )
        context.results["project_similarity"] = BlockResult(
            block_id="project_similarity",
            status="skipped",
            summary="Repository signals are unavailable",
        )
        block = LLMExplainerBlock(
            BlockManifest(
                block_id="llm_explainer",
                name="LLM Explainer",
                kind="synthesizer",
                dependencies=[
                    "rule_engine",
                    "github_repo",
                    "github_activity",
                    "telegram_channel",
                    "telegram_semantics",
                    "address_signal",
                    "identity_confirmation",
                    "contract_validator",
                    "project_similarity",
                ],
                description="Produces a grounded bilingual explanation and project overview from collected evidence.",
            )
        )
        context.llm_client.complete = AsyncMock(
            return_value=json.dumps(
                {
                    "project_name": "Tonswap",
                    "project_type": "dapp_product",
                    "project_overview_en": "Tonswap is a decentralized exchange on TON.",
                    "project_overview_ru": "Tonswap is a decentralized exchange on TON.",
                    "explanation_text_en": "Overall score: 77. Risk level: low. Identity confirmation uses tonswap.org and @tonswap_org.",
                    "explanation_text_ru": "Overall score: 77. Risk level: low. Identity confirmation uses tonswap.org and @tonswap_org.",
                    "evidence_used": [
                        "identity_confirmation.canonical_domain",
                        "requested_entity.case_description",
                        "overall_score",
                    ],
                },
                ensure_ascii=False,
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertIsNone(result.data["onchain_tx_count_30d"])
        self.assertIsNone(result.data["last_onchain_tx_age_days"])
        self.assertEqual(result.data["evidence_pack"]["ton_onchain"]["address"], "")
        self.assertIsNone(result.data["evidence_pack"]["ton_onchain"]["tx_count_30d"])
        self.assertIsNone(result.data["evidence_pack"]["ton_onchain"]["last_tx_age_days"])

    def test_telegram_channel_returns_skipped_when_source_is_unavailable(self) -> None:
        context = self._make_context(telegram_handle="MMM2049")
        block = TelegramChannelBlock(
            BlockManifest(
                block_id="telegram_channel",
                name="Telegram Channel Collector",
                kind="collector",
                description="Collect Telegram channel snapshot for social signal analysis.",
            )
        )
        with patch(
            "identity_validator.validators.telegram_channel.validator.get_telegram_snapshot",
            new=AsyncMock(side_effect=TimeoutError("telegram source timeout")),
        ):
            result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "skipped")
        self.assertIn("Telegram snapshot is unavailable", result.summary)
        self.assertIn("telegram_unavailable", result.flags)

    def test_extract_tgstat_posts(self) -> None:
        html = """
        <div id="post-1" class="card card-body border p-2 px-1 px-sm-3 post-container">
            <div class="post-body">
                <div class="post-text">First <b>message</b><br />Line 2</div>
                <div class="post-text"></div>
            </div>
        </div></div></div><hr class="m-0 mb-2">
        <div id="post-2" class="card card-body border p-2 px-1 px-sm-3 post-container">
            <div class="post-body">
                <div class="post-text">Second message with <a href="https://example.com">link</a></div>
            </div>
        </div></div></div><hr class="m-0 mb-2">
        """
        posts = _extract_tgstat_posts(html)
        self.assertEqual(posts, ["First message Line 2", "Second message with link"])

    def test_extract_tgchannels_entries(self) -> None:
        html = """
        <small class="channel-post__post-date">19 March 2026 17:00</small>
        <p class="channel-post__post-text">First <b>update</b><br/>Line 2 TON Community | X | YouTube | Lin kedIn | T ON.org</p>
        <a href="https://t.me/test/10"><span class="channel-post__post-more-label">Read more</span></a>
        <small class="channel-post__post-date">17 March 2026 10:15</small>
        <p class="channel-post__post-text">Second post</p>
        <a href="https://t.me/test/9"><span class="channel-post__post-more-label">Read more</span></a>
        """
        entries = _extract_tgchannels_entries(html)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["text"], "First update Line 2")
        self.assertEqual(entries[0]["published_at"], "2026-03-19T17:00:00Z")
        self.assertEqual(entries[1]["url"], "https://t.me/test/9")

    def test_extract_tgchannels_entries_from_live_like_blocks(self) -> None:
        html = """
        <div class="channel-post">
            <div class="channel-post__channel-info">
                <div class="channel-post__channel-caption">
                    <h3 class="channel-post__channel-title">Storm Trade Fam</h3>
                    <small class="channel-post__post-date">04 September 2025 07:56</small>
                </div>
            </div>
            <p class="channel-post__post-text">First support answer from the public chat.</p>
            <a class="channel-post__post-more" data-role="show-detail-post-modal"
               target="_blank" rel="nofollow"
               href="https://t.me/storm_trade_fam/896255"><span class="channel-post__post-more-label">Read more&#8230;</span>
            </a>
        </div>
        <div class="channel-post">
            <div class="channel-post__channel-info">
                <div class="channel-post__channel-caption">
                    <h3 class="channel-post__channel-title">Storm Trade Fam</h3>
                    <small class="channel-post__post-date">03 September 2025 13:10</small>
                </div>
            </div>
            <p class="channel-post__post-text">Fee rates depend on the trading pair:<br/>For <b>USDT pairs</b> from <b>0.06%</b> to <b>0.12%</b></p>
            <a class="channel-post__post-more" data-role="show-detail-post-modal"
               target="_blank" rel="nofollow"
               href="https://t.me/storm_trade_fam/896118"><span class="channel-post__post-more-label">Read more&#8230;</span>
            </a>
        </div>
        <footer></footer>
        """
        entries = _extract_tgchannels_entries(html)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["url"], "https://t.me/storm_trade_fam/896255")
        self.assertEqual(entries[0]["published_at"], "2025-09-04T07:56:00Z")
        self.assertIn("USDT pairs", entries[1]["text"])

    def test_get_telegram_snapshot_uses_tgchannels_when_tgstat_is_empty(self) -> None:
        context = self._make_context(telegram_handle="storm_trade_fam")
        html_by_url = {
            "https://tgstat.com/channel/@storm_trade_fam": "<html><title>Channel not found - 404</title></html>",
            "https://tgchannels.org/channel/storm_trade_fam?lang=all&start=0": """
                <div class="channel-post">
                    <div class="channel-post__channel-info">
                        <div class="channel-post__channel-caption">
                            <small class="channel-post__post-date">04 September 2025 07:56</small>
                        </div>
                    </div>
                    <p class="channel-post__post-text">First live-like community answer</p>
                    <a class="channel-post__post-more" href="https://t.me/storm_trade_fam/896255"><span class="channel-post__post-more-label">Read more</span></a>
                </div>
                <div class="channel-post">
                    <div class="channel-post__channel-info">
                        <div class="channel-post__channel-caption">
                            <small class="channel-post__post-date">03 September 2025 13:10</small>
                        </div>
                    </div>
                    <p class="channel-post__post-text">Second live-like community answer</p>
                    <a class="channel-post__post-more" href="https://t.me/storm_trade_fam/896118"><span class="channel-post__post-more-label">Read more</span></a>
                </div>
                <footer></footer>
            """,
        }

        async def fake_fetch(url: str, headers: dict[str, str] | None = None, timeout: int = 30) -> str:
            del headers, timeout
            return html_by_url.get(url, "<html></html>")

        with patch("identity_validator.sources.http_fetch_text", new=AsyncMock(side_effect=fake_fetch)), patch("builtins.print") as print_mock:
            snapshot = asyncio.run(get_telegram_snapshot(context.case, context.options))
        self.assertEqual(snapshot["source"], "tgchannels")
        self.assertEqual(snapshot["post_count"], 2)
        self.assertEqual(snapshot["entries"][0]["url"], "https://t.me/storm_trade_fam/896255")
        print_mock.assert_not_called()

    def test_get_telegram_snapshot_raises_without_stdout_noise_when_all_sources_fail(self) -> None:
        context = self._make_context(telegram_handle="wallet")

        async def fake_fetch(url: str, headers: dict[str, str] | None = None, timeout: int = 30) -> str:
            del headers, timeout
            if url == "https://tgstat.com/channel/@wallet":
                raise urllib.error.HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
            if url == "https://tgchannels.org/channel/wallet?lang=all&start=0":
                return "<html><body><footer></footer></body></html>"
            raise TimeoutError("telegram web timeout")

        with patch("identity_validator.sources.http_fetch_text", new=AsyncMock(side_effect=fake_fetch)), patch("builtins.print") as print_mock:
            with self.assertRaises(RuntimeError) as ctx:
                asyncio.run(get_telegram_snapshot(context.case, context.options))
        self.assertIn("tgstat: HTTP Error 404: Not Found", str(ctx.exception))
        self.assertIn("tgchannels: TGChannels returned no posts", str(ctx.exception))
        self.assertIn("telegram_web: telegram web timeout", str(ctx.exception))
        print_mock.assert_not_called()

    def test_rule_engine_keeps_unavailable_public_metrics_empty(self) -> None:
        context = self._make_context()
        context.results["project_type"] = BlockResult(
            block_id="project_type",
            status="success",
            summary="type=dapp_product",
            data={"project_type": "dapp_product"},
        )
        context.results["risk_validator"] = BlockResult(
            block_id="risk_validator",
            status="success",
            summary="risk=low",
            metrics={"risk_score": 18},
            data={"risk_level": "low"},
        )
        context.results["identity_confirmation"] = BlockResult(
            block_id="identity_confirmation",
            status="success",
            summary="identity=confirmed",
            metrics={"identity_score": 81},
            data={
                "identity_status": "confirmed",
                "evidence_status": "partial",
                "source_failures": ["telegram_channel"],
            },
        )
        block = RuleEngineBlock(
            BlockManifest(
                block_id="rule_engine",
                name="Rule Engine",
                kind="validator",
                description="Combine collected evidence into deterministic project metrics.",
            )
        )
        result = asyncio.run(block.run(context))
        self.assertEqual(result.status, "success")
        self.assertIsNone(result.metrics["community_score"])
        self.assertIsNone(result.metrics["community_activity_score"])
        self.assertIsNone(result.metrics["community_quality_score"])
        self.assertIsNone(result.metrics["activity_score"])
        self.assertIsNone(result.metrics["originality_score"])
        self.assertIsNone(result.metrics["onchain_tx_count_30d"])
        self.assertIsNone(result.metrics["last_onchain_tx_age_days"])


if __name__ == "__main__":
    unittest.main()
