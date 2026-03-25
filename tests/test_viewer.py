from __future__ import annotations

import asyncio
import json
from pathlib import Path
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from unittest.mock import AsyncMock, patch

from identity_validator.models import ProjectCase
from identity_validator.viewer import (
    _candidate_query_rank_score,
    _copy_statement_excerpt,
    _final_result_payload,
    _discovery_match_tokens,
    _enrich_candidates_with_public_identity,
    _enrich_candidate_with_public_wallet,
    _geckoterminal_candidates,
    _is_canonical_project_url,
    _market_search_terms,
    _merge_candidates,
    _parse_llm_json_object,
    _public_identity_result_candidate,
    _public_identity_search_queries,
    _registry_candidate,
    _registry_discovery_candidates,
    _rank_candidates_with_llm,
    _select_market_identity_url,
    _should_skip_external_discovery_from_registry,
    _should_skip_public_web_identity,
    build_block_detail_payload,
    build_run_payload,
    create_http_server,
    discover_runs,
)
from tests.helpers import ROOT, run_pipeline


class ViewerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.context = run_pipeline("ton_punks")
        cls.run_dir = cls.context.trace_store.root_dir
        cls.artifacts_root = ROOT / "artifacts"
        cls.validators_root = ROOT / "identity_validator" / "validators"

    def test_discover_runs_contains_generated_run(self) -> None:
        runs = discover_runs(self.artifacts_root)
        run_ids = {item["run_id"] for item in runs}
        self.assertIn(self.run_dir.name, run_ids)
        project_keys = [item["project_key"] for item in runs if item.get("project_key")]
        self.assertEqual(len(project_keys), len(set(project_keys)))

    def test_build_run_payload_contains_workflow_stages(self) -> None:
        payload = build_run_payload(self.run_dir, self.validators_root)
        self.assertEqual(payload["case"]["case_id"], "ton_punks")
        self.assertTrue(payload["workflow"]["units"])
        self.assertTrue(payload["workflow"]["stages"])
        self.assertIn("presentation", payload)
        self.assertIn("result", payload)
        unit_ids = {item["unit_id"] for item in payload["workflow"]["units"]}
        self.assertIn("source_collection", unit_ids)
        self.assertIn("identity_confirmation", unit_ids)
        self.assertIn("rule_engine", unit_ids)

    def test_build_run_payload_contains_final_result(self) -> None:
        payload = build_run_payload(self.run_dir, self.validators_root)
        self.assertEqual(payload["result"]["status"], "success")
        self.assertTrue(payload["result"]["explanation_text"])
        self.assertIn("overall_score", payload["result"]["metrics"])
        self.assertIn("facts", payload["result"])
        self.assertIn("risk_evidence", payload["result"])
        self.assertIn("clone_analysis", payload["result"])
        self.assertEqual(payload["result"]["clone_risk"], "low")
        self.assertTrue(payload["result"]["facts"])

    def test_build_run_payload_ignores_discovery_errors_in_overall_run_status(self) -> None:
        with tempfile.TemporaryDirectory(prefix="viewer_run_copy_") as temp_dir:
            copied_run_dir = Path(temp_dir) / self.run_dir.name
            shutil.copytree(self.run_dir, copied_run_dir)
            summary_path = copied_run_dir / "run_summary.json"
            summary = json.loads(summary_path.read_text("utf-8"))
            summary["discovery"] = {
                "query": "test query",
                "summary": "discovery finished with partial external coverage",
                "selected_candidate_key": "url:test.example",
                "candidates": [
                    {
                        "candidate_key": "url:test.example",
                        "name": "Test Candidate",
                        "project_url": "https://test.example",
                        "telegram_handle": "",
                        "github_repo": "",
                        "wallet_address": "",
                    }
                ],
                "source_statuses": {
                    "coingecko": {
                        "status": "error",
                        "candidate_count": 0,
                        "summary": "HTTP Error 429: Too Many Requests",
                    }
                },
                "ranking_status": "success",
                "ranking_summary": "Selected candidate: Test Candidate.",
            }
            summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), "utf-8")
            payload = build_run_payload(copied_run_dir, self.validators_root)
        self.assertEqual(payload["run"]["status"], "success")
        self.assertFalse(payload["run"]["error"])

    def test_build_run_payload_localizes_final_result_to_russian(self) -> None:
        payload = build_run_payload(self.run_dir, self.validators_root, locale="ru")
        self.assertTrue(any("А" <= ch <= "я" for ch in payload["result"]["explanation_text"]))
        self.assertTrue(any("А" <= ch <= "я" for ch in payload["result"]["clone_analysis"]["summary"]))
        if payload["result"]["risk_evidence"]:
            self.assertTrue(
                any(any("А" <= ch <= "я" for ch in item["summary"]) for item in payload["result"]["risk_evidence"])
            )

    def test_final_result_payload_includes_ton_mcp_known_jetton_details(self) -> None:
        case = {
            "case_id": "known_jetton_case",
            "name": "Notcoin",
            "wallet_address": "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT",
        }
        results = {
            "rule_engine": {
                "status": "success",
                "metrics": {"overall_score": 80, "onchain_tx_count_30d": 5, "last_onchain_tx_age_days": 4},
                "data": {
                    "project_type": "token",
                    "risk_level": "low",
                    "clone_risk": "low",
                    "strengths": ["ton_mcp_known_jetton_verified"],
                    "risks": [],
                    "next_checks": [],
                },
                "flags": [],
                "needs_human_review": False,
            },
            "llm_explainer": {
                "status": "success",
                "data": {
                    "project_name": "Notcoin",
                    "project_type": "token",
                    "overall_score": 80,
                    "risk_level": "low",
                    "strengths": ["ton_mcp_known_jetton_verified"],
                    "risks": [],
                    "next_checks": [],
                },
                "flags": [],
                "needs_human_review": False,
            },
            "address_signal": {
                "status": "success",
                "metrics": {"ton_mcp_known_jetton_match": 1},
                "data": {
                    "ton_activity": {
                        "status": "success",
                        "summary": "Observed 5 address transactions in the last 30 days.",
                        "address": "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT",
                        "last_tx_at": "2026-03-20T10:00:00Z",
                        "last_tx_age_days": 4,
                        "tx_count_7d": 2,
                        "tx_count_30d": 5,
                        "tx_count_30d_limit_hit": False,
                        "sample_transactions": [],
                    },
                    "ton_mcp": {
                        "status": "success",
                        "summary": "TON MCP returned 1 known jetton.",
                        "matched_jetton": {
                            "symbol": "NOT",
                            "name": "Notcoin",
                            "address": "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT",
                            "decimals": 9,
                        },
                        "reverse_dns": {
                            "status": "success",
                            "summary": "TON MCP reverse-resolved the project address.",
                            "address": "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT",
                            "domain": "notcoin.ton",
                        },
                        "balance": {
                            "status": "success",
                            "summary": "TON MCP returned the project balance.",
                            "address": "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT",
                            "balance": "123.456 TON",
                            "balance_nano": "123456000000",
                        },
                    }
                },
            },
        }
        payload = _final_result_payload(case, results, locale="en")
        self.assertEqual(payload["metrics"]["onchain_tx_count_30d"], 5)
        self.assertEqual(payload["metrics"]["last_onchain_tx_age_days"], 4)
        self.assertTrue(any(item["key"] == "ton_mcp_known_jetton" for item in payload["facts"]))
        self.assertTrue(any(item["key"] == "ton_onchain_activity" for item in payload["facts"]))
        self.assertTrue(any(item["key"] == "ton_mcp_known_jetton" for item in payload["risk_evidence"]))
        self.assertTrue(any(item["key"] == "ton_onchain_activity" for item in payload["risk_evidence"]))
        self.assertTrue(any(item["key"] == "ton_mcp_reverse_dns" for item in payload["facts"]))
        self.assertTrue(any(item["key"] == "ton_mcp_balance" for item in payload["facts"]))
        self.assertIn("TON MCP confirmed", payload["explanation_text"])
        self.assertIn("on-chain movement", payload["explanation_text"])

    def test_copy_statement_excerpt_requires_real_copy_hit(self) -> None:
        excerpt = _copy_statement_excerpt(
            "Tonkeeper Web is a non-custodial crypto wallet and gateway to blockchain dApps.",
            [],
        )
        self.assertEqual(excerpt, "")

    def test_registry_candidate_prefers_recorded_homepage_over_github_url(self) -> None:
        case = ProjectCase.load(ROOT / "cases" / "tonkeeper_web" / "case.json")
        candidate = _registry_candidate(case, "Tonkeeper TON wallet", "", "", "")
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["project_url"], "https://tonkeeper.com")

    def test_registry_candidate_infers_canonical_url_from_readme_snapshot(self) -> None:
        case = ProjectCase.load(ROOT / "cases" / "ston_fi_sdk" / "case.json")
        candidate = _registry_candidate(case, "STON.fi TON DEX", "", "", "")
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["project_url"], "https://ston.fi")

    def test_registry_candidate_does_not_infer_source_file_as_canonical_url(self) -> None:
        case = ProjectCase.load(ROOT / "cases" / "ton_org_blueprint" / "case.json")
        candidate = _registry_candidate(case, "TON Blueprint", "", "", "")
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["project_url"], "https://github.com/ton-org/blueprint")

    def test_final_result_payload_exposes_identity_confirmation_semantics(self) -> None:
        case = {
            "case_id": "evaa_probe",
            "name": "TON Blockchain",
            "project_url": "https://github.com/ton-blockchain/ton",
            "github_repo": "ton-blockchain/ton",
        }
        results = {
            "rule_engine": {
                "status": "success",
                "metrics": {"overall_score": 32, "identity_score": 18},
                "data": {
                    "project_type": "unknown",
                    "risk_level": "low",
                    "clone_risk": "low",
                    "identity_status": "mismatch",
                    "evidence_status": "partial",
                    "strengths": [],
                    "risks": ["identity_brand_mismatch"],
                    "next_checks": ["reselect_project_candidate"],
                },
                "flags": ["identity_brand_mismatch"],
                "needs_human_review": True,
            },
            "llm_explainer": {
                "status": "success",
                "data": {
                    "project_name": "TON Blockchain",
                    "project_type": "unknown",
                    "overall_score": 32,
                    "risk_level": "low",
                    "identity_status": "mismatch",
                    "evidence_status": "partial",
                    "identity_score": 18,
                    "strengths": [],
                    "risks": ["identity_brand_mismatch"],
                    "next_checks": ["reselect_project_candidate"],
                },
                "flags": [],
                "needs_human_review": True,
            },
            "identity_confirmation": {
                "status": "success",
                "data": {
                    "identity_status": "mismatch",
                    "evidence_status": "partial",
                    "canonical_domain": "",
                    "noncanonical_reference_domain": "github.com",
                    "corroborating_signals": ["github_repo"],
                    "source_failures": [],
                    "brand_overlap_tokens": [],
                },
                "flags": ["identity_brand_mismatch", "identity_based_on_noncanonical_reference"],
            },
        }
        payload = _final_result_payload(case, results, locale="en")
        self.assertEqual(payload["identity_status"], "mismatch")
        self.assertEqual(payload["metrics"]["identity_score"], 18)
        self.assertFalse(any(item["key"] == "identity_confirmation" for item in payload["facts"]))
        self.assertTrue(any(item["key"] == "identity_confirmation" for item in payload["risk_evidence"]))
        self.assertIn("Overall score: 32. Overall risk level: low.", payload["explanation_text"])
        self.assertIn("does not align with the requested project brand", payload["explanation_text"])

    def test_final_result_payload_keeps_unknown_type_when_runtime_type_is_unknown(self) -> None:
        case = {
            "case_id": "tonviewer_probe",
            "name": "Tonviewer",
            "type_hint": "explorer",
        }
        results = {
            "rule_engine": {
                "status": "success",
                "metrics": {"overall_score": 83},
                "data": {
                    "project_type": "unknown",
                    "risk_level": "low",
                    "clone_risk": "low",
                    "strengths": [],
                    "risks": [],
                    "next_checks": [],
                },
                "flags": [],
                "needs_human_review": False,
            },
            "llm_explainer": {
                "status": "success",
                "data": {
                    "project_name": "Tonviewer",
                    "project_type": "unknown",
                    "overall_score": 83,
                    "risk_level": "low",
                    "strengths": [],
                    "risks": [],
                    "next_checks": [],
                },
                "flags": [],
                "needs_human_review": False,
            },
        }
        payload = _final_result_payload(case, results, locale="en")
        self.assertEqual(payload["project_type"], "unknown")

    def test_final_result_payload_includes_project_overview_text(self) -> None:
        case = {
            "case_id": "wallet_probe",
            "name": "Tonkeeper",
            "github_repo": "tonkeeper/tonkeeper-web",
        }
        results = {
            "rule_engine": {
                "status": "success",
                "metrics": {
                    "overall_score": 84,
                    "community_activity_score": 72,
                    "community_quality_score": 78,
                },
                "data": {
                    "project_type": "wallet_app",
                    "risk_level": "low",
                    "clone_risk": "low",
                    "identity_status": "confirmed",
                    "strengths": [],
                    "risks": [],
                    "next_checks": [],
                },
                "flags": [],
                "needs_human_review": False,
            },
            "llm_explainer": {
                "status": "success",
                "data": {
                    "project_name": "Tonkeeper",
                    "project_type": "wallet_app",
                    "overall_score": 84,
                    "risk_level": "low",
                    "project_overview_en": "Tonkeeper is a self-custodial wallet for TON and Telegram users. It is aimed at people who need direct control over TON assets and wallet actions. The strongest visible public signal is the repository description, which clearly states the wallet use case.",
                    "explanation_text_en": "Overall score is 84 and risk level is low. The repository description explicitly identifies the product as a self-custodial TON wallet.",
                    "strengths": [],
                    "risks": [],
                    "next_checks": [],
                },
                "flags": [],
                "needs_human_review": False,
            },
            "github_repo": {
                "status": "success",
                "data": {
                    "repo": {
                        "full_name": "tonkeeper/tonkeeper-web",
                        "description": "self-custodial wallet for TON and Telegram users",
                        "topics": ["wallet", "tonconnect", "telegram"],
                    },
                    "readme_excerpt": "Tonkeeper is a self-custodial wallet for TON.",
                },
            },
            "project_similarity": {
                "status": "success",
                "data": {
                    "distinctive_features": ["tonconnect", "self_custody"],
                },
            },
        }
        discovery = {
            "selected_candidate_key": "name:tonkeeper",
            "candidates": [
                {
                    "candidate_key": "name:tonkeeper",
                    "name": "Tonkeeper | @tonkeeper",
                    "description": "Self-custodial wallet for TON and Telegram users.",
                    "project_url": "https://tonkeeper.com",
                    "telegram_handle": "tonkeeper",
                }
            ],
        }
        payload = _final_result_payload(case, results, discovery=discovery, locale="en")
        self.assertIn("Tonkeeper", payload["project_overview_text"])
        self.assertIn("self-custodial wallet", payload["project_overview_text"])
        self.assertIn("direct control over TON assets", payload["project_overview_text"])

    def test_final_result_payload_prefers_deterministic_overview_when_description_is_thin(self) -> None:
        case = {
            "case_id": "ton_diamonds_probe",
            "name": "TON Diamonds",
            "project_url": "https://ton.diamonds",
        }
        results = {
            "rule_engine": {
                "status": "success",
                "metrics": {"overall_score": 72},
                "data": {
                    "project_type": "nft_marketplace",
                    "risk_level": "low",
                    "clone_risk": "low",
                    "identity_status": "weak",
                    "evidence_status": "partial",
                    "strengths": [],
                    "risks": [],
                    "next_checks": [],
                },
                "flags": [],
                "needs_human_review": False,
            },
            "llm_explainer": {
                "status": "success",
                "data": {
                    "project_name": "TON Diamonds",
                    "project_type": "nft_marketplace",
                    "overall_score": 72,
                    "risk_level": "low",
                    "project_overview_en": "TON Diamonds is an NFT marketplace on TON. It helps users seamlessly buy and sell NFTs across the TON ecosystem.",
                    "project_overview_ru": "TON Diamonds — это NFT-маркетплейс на TON. Он упрощает покупку и продажу NFT в экосистеме TON.",
                    "explanation_text_en": "Overall score: 72. Risk level: low.",
                    "explanation_text_ru": "Общий балл: 72. Уровень риска: низкий.",
                    "strengths": [],
                    "risks": [],
                    "next_checks": [],
                },
                "flags": [],
                "needs_human_review": False,
            },
            "github_repo": {"status": "skipped", "data": {}},
        }
        discovery = {
            "selected_candidate_key": "url:ton.diamonds",
            "candidates": [
                {
                    "candidate_key": "url:ton.diamonds",
                    "name": "TON Diamonds",
                    "project_url": "https://ton.diamonds",
                    "telegram_handle": "",
                    "github_repo": "",
                    "wallet_address": "",
                    "description": "NFT marketplace on TON.",
                }
            ],
        }
        payload = _final_result_payload(case, results, discovery=discovery, locale="ru")
        self.assertEqual(payload["project_overview_text"], "TON Diamonds — NFT-маркетплейс на TON.")

    def test_final_result_payload_humanizes_identity_signal_names(self) -> None:
        case = {
            "case_id": "notcoin_probe",
            "name": "Notcoin | @notcoin",
            "project_url": "https://notco.in",
            "telegram_handle": "notcoin",
            "wallet_address": "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT",
            "type_hint": "Gaming (GameFi)",
        }
        results = {
            "rule_engine": {
                "status": "success",
                "metrics": {"overall_score": 83, "identity_score": 95, "onchain_tx_count_30d": 16},
                "data": {
                    "project_type": "gamefi",
                    "risk_level": "low",
                    "clone_risk": "low",
                    "identity_status": "confirmed",
                    "evidence_status": "sufficient",
                    "strengths": [],
                    "risks": [],
                    "next_checks": [],
                },
                "flags": [],
                "needs_human_review": False,
            },
            "llm_explainer": {
                "status": "success",
                "data": {
                    "project_name": "Notcoin",
                    "project_type": "gamefi",
                    "overall_score": 83,
                    "risk_level": "low",
                    "identity_status": "confirmed",
                    "evidence_status": "sufficient",
                    "explanation_text_ru": "Подтверждающие сигналы: canonical_domain, telegram_channel, wallet_signal.",
                    "strengths": [],
                    "risks": [],
                    "next_checks": [],
                },
                "flags": [],
                "needs_human_review": False,
            },
            "identity_confirmation": {
                "status": "success",
                "data": {
                    "identity_status": "confirmed",
                    "evidence_status": "sufficient",
                    "canonical_domain": "notco.in",
                    "corroborating_signals": ["canonical_domain", "telegram_channel", "wallet_signal"],
                    "source_failures": [],
                },
                "flags": [],
            },
            "address_signal": {
                "status": "success",
                "data": {
                    "ton_activity": {
                        "status": "success",
                        "tx_count_30d": 16,
                        "last_tx_age_days": 0,
                    }
                },
            },
        }
        payload = _final_result_payload(case, results, locale="ru")
        self.assertIn("официальный сайт", payload["explanation_text"])
        self.assertIn("Telegram-канал", payload["explanation_text"])
        self.assertIn("TON-адрес и сигналы из блокчейна", payload["explanation_text"])
        self.assertNotIn("canonical_domain", payload["explanation_text"])
        self.assertNotIn("telegram_channel", payload["explanation_text"])
        self.assertNotIn("wallet_signal", payload["explanation_text"])

    def test_project_overview_skips_nonlocalized_candidate_description_in_russian(self) -> None:
        case = {
            "case_id": "notcoin_overview_probe",
            "name": "Notcoin | @notcoin",
            "project_url": "https://notco.in",
            "telegram_handle": "notcoin",
            "type_hint": "Gaming (GameFi)",
        }
        results = {
            "rule_engine": {
                "status": "success",
                "metrics": {
                    "overall_score": 83,
                    "community_activity_score": 86,
                    "community_quality_score": 82,
                },
                "data": {
                    "project_type": "gamefi",
                    "risk_level": "low",
                    "clone_risk": "low",
                    "identity_status": "confirmed",
                    "strengths": [],
                    "risks": [],
                    "next_checks": [],
                },
                "flags": [],
                "needs_human_review": False,
            },
            "llm_explainer": {
                "status": "success",
                "data": {
                    "project_name": "Notcoin",
                    "project_type": "gamefi",
                    "overall_score": 83,
                    "risk_level": "low",
                    "strengths": [],
                    "risks": [],
                    "next_checks": [],
                },
                "flags": [],
                "needs_human_review": False,
            },
            "identity_confirmation": {
                "status": "success",
                "data": {
                    "identity_status": "confirmed",
                    "evidence_status": "sufficient",
                    "canonical_domain": "notco.in",
                    "corroborating_signals": ["canonical_domain", "telegram_channel", "wallet_signal"],
                },
                "flags": [],
            },
            "address_signal": {
                "status": "success",
                "data": {
                    "ton_activity": {
                        "status": "success",
                        "tx_count_30d": 16,
                        "last_tx_age_days": 0,
                    }
                },
            },
        }
        discovery = {
            "selected_candidate_key": "name:notcoin",
            "candidates": [
                {
                    "candidate_key": "name:notcoin",
                    "name": "Notcoin | @notcoin",
                    "description": "Notcoin ($NOT) is a community-driven token aimed at onboarding users into the Web3 ecosystem through a tap-to-earn game.",
                    "project_url": "https://notco.in",
                    "telegram_handle": "notcoin",
                }
            ],
        }
        payload = _final_result_payload(case, results, discovery=discovery, locale="ru")
        self.assertEqual(
            payload["project_overview_text"],
            "Не удалось собрать достаточно подтвержденных публичных данных для обзора проекта.",
        )
        self.assertNotIn("tap-to-earn", payload["project_overview_text"])

    def test_build_run_payload_exposes_first_error_context(self) -> None:
        base_payload = build_run_payload(self.run_dir, self.validators_root)
        workflow = json.loads(json.dumps(base_payload["workflow"]))
        expected_stage_id = ""

        def mark_error(plan: dict) -> bool:
            nonlocal expected_stage_id
            for unit in plan.get("units", []):
                if unit["unit_id"] == "telegram_channel":
                    unit["runtime"]["status"] = "error"
                    unit["result"] = {
                        "status": "error",
                        "summary": "Telegram snapshot is unavailable: timeout",
                        "flags": ["telegram_unavailable"],
                        "needs_human_review": False,
                    }
                    return True
                if unit.get("unit_type") == "composite" and mark_error(unit.get("plan", {})):
                    unit["runtime"]["status"] = "error"
                    return True
            return False

        self.assertTrue(mark_error(workflow))
        for stage in workflow["stages"]:
            if "source_collection" in stage.get("unit_ids", []):
                stage["runtime"]["status"] = "error"
                expected_stage_id = str(stage.get("stage_id") or "")
        summary = {
            "case": base_payload["case"],
            "options": base_payload["options"],
            "results": {},
        }
        with patch("identity_validator.viewer._load_summary", return_value=summary), patch(
            "identity_validator.viewer._load_workflow_snapshot",
            return_value=workflow,
        ):
            payload = build_run_payload(self.run_dir, self.validators_root)
        self.assertEqual(payload["run"]["error"]["unit_id"], "telegram_channel")
        self.assertEqual(payload["run"]["error"]["stage_id"], expected_stage_id)
        self.assertEqual(payload["default_unit_id"], "telegram_channel")

    def test_block_detail_includes_source_and_llm_trace(self) -> None:
        payload = build_block_detail_payload(self.run_dir, "telegram_semantics", self.validators_root)
        self.assertEqual(payload["unit_id"], "telegram_semantics")
        self.assertEqual(payload["unit_type"], "atomic")
        self.assertEqual(payload["presentation"]["kind"], "hybrid")
        self.assertIn("class TelegramSemanticsBlock", payload["validator_source"])
        self.assertTrue(payload["llm_trace"])

    def test_block_detail_localizes_auto_description_to_russian(self) -> None:
        payload = build_block_detail_payload(self.run_dir, "repo_analysis", self.validators_root, locale="ru")
        self.assertEqual(payload["unit_id"], "repo_analysis")
        self.assertIn("Составной блок", payload["auto_description"])
        self.assertIn("Внутренняя топология", payload["auto_description"])

    def test_build_run_payload_prepends_discovery_stage(self) -> None:
        base_payload = build_run_payload(self.run_dir, self.validators_root)
        summary = {
            "case": base_payload["case"],
            "options": base_payload["options"],
            "results": {},
            "discovery": {
                "query": "MMM2049 ton memecoin",
                "summary": "CoinGecko and GeckoTerminal resolved the TON token candidate.",
                "selected_candidate_key": "wallet:EQCH44N73BXEhT8063KAK_27oComvJnmAaebso-dZoyAy6g_",
                "candidates": [
                    {
                        "candidate_key": "wallet:EQCH44N73BXEhT8063KAK_27oComvJnmAaebso-dZoyAy6g_",
                        "name": "MMM",
                        "github_repo": "",
                        "project_url": "https://mmm2049.com/",
                        "telegram_handle": "MMM2049",
                        "wallet_address": "EQCH44N73BXEhT8063KAK_27oComvJnmAaebso-dZoyAy6g_",
                        "description": "Memecoin on TON",
                        "project_type": "Meme",
                        "source_labels": ["coingecko", "geckoterminal"],
                        "match_reason": "Matched TON market listings.",
                        "score": 0.94,
                    }
                ],
                "source_statuses": {
                    "coingecko": {"status": "success", "candidate_count": 1, "summary": "CoinGecko found the TON token."},
                    "geckoterminal": {"status": "success", "candidate_count": 1, "summary": "GeckoTerminal found the TON pool."},
                },
            },
        }
        with patch("identity_validator.viewer._load_summary", return_value=summary), patch(
            "identity_validator.viewer._load_workflow_snapshot",
            return_value=base_payload["workflow"],
        ):
            payload = build_run_payload(self.run_dir, self.validators_root)
        self.assertEqual(payload["workflow"]["stages"][0]["unit_ids"], ["project_discovery"])
        unit_ids = {item["unit_id"] for item in payload["workflow"]["units"]}
        self.assertIn("project_discovery", unit_ids)
        self.assertEqual(payload["workflow"]["units"][0]["unit_type"], "composite")

    def test_http_server_serves_run_and_block_payloads(self) -> None:
        server = create_http_server(
            host="127.0.0.1",
            port=0,
            artifacts_root=self.artifacts_root,
            validators_root=self.validators_root,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base_url = f"http://127.0.0.1:{server.server_address[1]}"
            homepage = self._get_text(base_url)
            self.assertIn("TON Project Auditor", homepage)
            spa_route = self._get_text(f"{base_url}/runs/example")
            self.assertIn("<div id=\"app\"></div>", spa_route)
            runs_payload = self._get_json(f"{base_url}/api/runs")
            self.assertTrue(runs_payload["runs"])
            run_payload = self._get_json(f"{base_url}/api/runs/{self.run_dir.name}")
            self.assertEqual(run_payload["case"]["case_id"], "ton_punks")
            self.assertTrue(run_payload["workflow"]["stages"])
            self.assertIn("presentation", run_payload["workflow"])
            block_payload = self._get_json(
                f"{base_url}/api/runs/{self.run_dir.name}/blocks/telegram_semantics"
            )
            self.assertEqual(block_payload["unit_id"], "telegram_semantics")
            self.assertEqual(block_payload["presentation"]["kind"], "hybrid")
            self.assertIn("trace_output", block_payload)
            head_request = urllib.request.Request(f"{base_url}/", method="HEAD")
            with urllib.request.urlopen(head_request, timeout=10) as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(response.headers.get_content_type(), "text/html")
                self.assertEqual(response.read(), b"")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_http_server_returns_build_instructions_when_frontend_bundle_is_missing(self) -> None:
        with tempfile.TemporaryDirectory(prefix="viewer_static_missing_") as temp_static_root:
            server = create_http_server(
                host="127.0.0.1",
                port=0,
                artifacts_root=self.artifacts_root,
                validators_root=self.validators_root,
                static_root=temp_static_root,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base_url = f"http://127.0.0.1:{server.server_address[1]}"
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    urllib.request.urlopen(base_url, timeout=10)
                self.assertEqual(ctx.exception.code, 503)
                body = ctx.exception.read().decode("utf-8")
                self.assertIn("Frontend bundle is missing", body)
                self.assertIn("npm run build", body)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_http_server_accepts_new_run_post(self) -> None:
        mock_payload = build_run_payload(self.run_dir, self.validators_root)
        mock_payload["run"]["status"] = "running"
        mock_payload["run"]["is_live"] = True
        with patch("identity_validator.viewer.ActiveRunRegistry.start_run", return_value=mock_payload):
            server = create_http_server(
                host="127.0.0.1",
                port=0,
                artifacts_root=self.artifacts_root,
                validators_root=self.validators_root,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base_url = f"http://127.0.0.1:{server.server_address[1]}"
                request = urllib.request.Request(
                    f"{base_url}/api/runs/new",
                    data=json.dumps({"project": "TON-Punks/punk-city-hack-a-tonx"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(request, timeout=10) as response:
                    self.assertEqual(response.status, 201)
                    payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(payload["run"]["status"], "running")
                self.assertTrue(payload["run"]["is_live"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_http_server_accepts_discovery_search(self) -> None:
        mock_discovery = {
            "query": "TON Punks gamefi nft",
            "summary": "Choose the registry-backed TON Punks candidate.",
            "selected_candidate_key": "github:ton-punks/punk-city-hack-a-tonx",
            "candidates": [
                {
                    "candidate_key": "github:ton-punks/punk-city-hack-a-tonx",
                    "name": "TON Punks / Punk City",
                    "github_repo": "TON-Punks/punk-city-hack-a-tonx",
                    "project_url": "https://github.com/TON-Punks/punk-city-hack-a-tonx",
                    "telegram_handle": "tonpunks",
                    "wallet_address": "",
                    "description": "TON GameFi project candidate",
                    "project_type": "gamefi",
                    "source_labels": ["registry", "github_search"],
                    "match_reason": "Exact repo match",
                    "score": 0.98,
                }
            ],
            "source_statuses": {
                "registry": {"status": "success", "candidate_count": 1, "summary": "Registry returned TON Punks."},
                "github_search": {"status": "success", "candidate_count": 1, "summary": "GitHub search returned TON Punks."},
            },
        }
        with patch("identity_validator.viewer.discover_project_candidates", new=AsyncMock(return_value=mock_discovery)):
            server = create_http_server(
                host="127.0.0.1",
                port=0,
                artifacts_root=self.artifacts_root,
                validators_root=self.validators_root,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base_url = f"http://127.0.0.1:{server.server_address[1]}"
                request = urllib.request.Request(
                    f"{base_url}/api/discovery/search",
                    data=json.dumps({"query": "TON Punks gamefi nft"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(request, timeout=10) as response:
                    self.assertEqual(response.status, 200)
                    payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(payload["selected_candidate_key"], "github:ton-punks/punk-city-hack-a-tonx")
                self.assertEqual(payload["candidates"][0]["telegram_handle"], "tonpunks")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_http_server_accepts_live_discovery_session_routes(self) -> None:
        mock_session_payload = {
            "session": {
                "session_id": "discovery_123",
                "status": "running",
                "updated_at": 1234567890,
                "error": {},
            },
            "query": "TON Punks gamefi nft",
            "summary": "",
            "selected_candidate_key": "",
            "candidates": [],
            "source_statuses": {},
            "workflow": {
                "plan_id": "project_discovery.plan",
                "name": "Project discovery",
                "description": "",
                "units": [],
                "edges": [],
                "stages": [],
            },
            "default_unit_id": "",
        }
        with patch("identity_validator.viewer.ActiveDiscoveryRegistry.start_search", return_value=mock_session_payload), patch(
            "identity_validator.viewer.ActiveDiscoveryRegistry.get_payload",
            return_value=mock_session_payload,
        ):
            server = create_http_server(
                host="127.0.0.1",
                port=0,
                artifacts_root=self.artifacts_root,
                validators_root=self.validators_root,
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base_url = f"http://127.0.0.1:{server.server_address[1]}"
                request = urllib.request.Request(
                    f"{base_url}/api/discovery/sessions/new",
                    data=json.dumps({"query": "TON Punks gamefi nft"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(request, timeout=10) as response:
                    self.assertEqual(response.status, 201)
                    created = json.loads(response.read().decode("utf-8"))
                self.assertEqual(created["session"]["session_id"], "discovery_123")

                polled = self._get_json(f"{base_url}/api/discovery/sessions/discovery_123")
                self.assertEqual(polled["session"]["status"], "running")
                self.assertEqual(polled["query"], "TON Punks gamefi nft")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_parse_llm_json_object_accepts_fenced_json(self) -> None:
        raw = """```json
{
  "summary": "Top project candidates.",
  "selected_candidate_key": "wallet:test",
  "ranked_candidates": []
}
```"""
        payload = _parse_llm_json_object(raw)
        self.assertEqual(payload["selected_candidate_key"], "wallet:test")

    def test_rank_candidates_with_llm_leaves_invalid_selected_key_empty(self) -> None:
        candidates = [
            {
                "candidate_key": "wallet:EQVALIDADDRESS",
                "name": "MMM",
                "github_repo": "",
                "project_url": "https://mmm2049.com/",
                "telegram_handle": "MMM2049",
                "wallet_address": "EQVALIDADDRESS",
                "description": "Memecoin on TON",
                "project_type": "token",
                "source_labels": ["coingecko"],
                "match_reason": "test",
                "score": 0.6,
            }
        ]
        raw_response = """```json
{
  "summary": "Ranked candidates.",
  "selected_candidate_key": "wallet:not-present",
  "ranked_candidates": [
    {
      "candidate_key": "wallet:EQVALIDADDRESS",
      "score": 0.9,
      "reason": "Exact match",
      "name": "MMM",
      "project_url": "https://mmm2049.com/",
      "telegram_handle": "MMM2049",
      "project_type": "token"
    }
  ]
}
```"""
        fake_client = AsyncMock()
        fake_client.complete = AsyncMock(return_value=raw_response)
        with patch("identity_validator.viewer.build_llm_client", return_value=fake_client):
            result = asyncio.run(
                _rank_candidates_with_llm(
                    "MMM 2049 TON",
                    candidates,
                    llm_mode="live",
                    model="gpt-4o-mini",
                )
            )
        self.assertEqual(result["selected_candidate_key"], "wallet:EQVALIDADDRESS")
        self.assertEqual(result["candidates"][0]["candidate_key"], "wallet:EQVALIDADDRESS")
        self.assertGreater(result["candidates"][0]["score"], 0.9)

    def test_rank_candidates_with_llm_prefers_canonical_product_over_sdk(self) -> None:
        candidates = [
            {
                "candidate_key": "github:ston-fi/sdk",
                "name": "STON.fi SDK",
                "github_repo": "ston-fi/sdk",
                "project_url": "https://github.com/ston-fi/sdk",
                "telegram_handle": "stonfidex",
                "wallet_address": "",
                "description": "Official SDK for building on STON.fi protocol.",
                "project_type": "tooling_sdk",
                "source_labels": ["registry"],
                "match_reason": "registry",
                "score": 0.78,
            },
            {
                "candidate_key": "github:ston-fi/dex-core",
                "name": "STON.fi",
                "github_repo": "ston-fi/dex-core",
                "project_url": "https://app.ston.fi",
                "telegram_handle": "stonfidex",
                "wallet_address": "",
                "description": "Core DEX contracts and product entrypoint for STON.fi.",
                "project_type": "dex",
                "source_labels": ["github_search"],
                "match_reason": "github",
                "score": 0.72,
            },
        ]
        raw_response = """```json
{
  "summary": "Both candidates look relevant.",
  "selected_candidate_key": "github:ston-fi/sdk",
  "ranked_candidates": [
    {
      "candidate_key": "github:ston-fi/sdk",
      "score": 1.0,
      "reason": "Official STON.fi repository",
      "name": "STON.fi SDK",
      "project_url": "https://github.com/ston-fi/sdk",
      "telegram_handle": "stonfidex",
      "project_type": "tooling_sdk"
    },
    {
      "candidate_key": "github:ston-fi/dex-core",
      "score": 0.6,
      "reason": "DEX implementation",
      "name": "STON.fi",
      "project_url": "https://app.ston.fi",
      "telegram_handle": "stonfidex",
      "project_type": "dex"
    }
  ]
}
```"""
        fake_client = AsyncMock()
        fake_client.complete = AsyncMock(return_value=raw_response)
        with patch("identity_validator.viewer.build_llm_client", return_value=fake_client):
            result = asyncio.run(
                _rank_candidates_with_llm(
                    "STON.fi TON DEX",
                    candidates,
                    llm_mode="live",
                    model="gpt-4o-mini",
                )
            )
        self.assertEqual(result["selected_candidate_key"], "github:ston-fi/dex-core")
        self.assertEqual(result["candidates"][0]["candidate_key"], "github:ston-fi/dex-core")

    def test_enrich_candidate_with_public_wallet_extracts_address_from_search_results(self) -> None:
        candidate = {
            "candidate_key": "github:ton-punks/punk-city-hack-a-tonx",
            "name": "TON Punks / Punk City",
            "github_repo": "TON-Punks/punk-city-hack-a-tonx",
            "project_url": "https://github.com/TON-Punks/punk-city-hack-a-tonx",
            "telegram_handle": "tonpunks",
            "wallet_address": "",
            "description": "TON GameFi project candidate",
            "project_type": "smart_contracts",
            "source_labels": ["registry"],
            "match_reason": "Exact repo match",
            "score": 0.98,
        }
        search_html = """
<html><body>
  <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fgetgems.io%2Fcollection%2FEQAo92DYMokxghKcq-CkCGSk_MgXY5Fo1SPW20gkvZl75iCN">TON Punks</a>
</body></html>
"""
        with patch("identity_validator.viewer.http_fetch_text", new=AsyncMock(return_value=search_html)):
            payload = asyncio.run(_enrich_candidate_with_public_wallet(candidate, "ton punks"))
        self.assertTrue(payload["resolved"])
        self.assertEqual(payload["candidate"]["wallet_address"], "EQAo92DYMokxghKcq-CkCGSk_MgXY5Fo1SPW20gkvZl75iCN")
        self.assertIn("public_web", payload["candidate"]["source_labels"])

    def test_merge_candidates_prefers_canonical_site_for_same_telegram(self) -> None:
        merged = _merge_candidates(
            [
                {
                    "name": "TON Cats",
                    "project_url": "https://t.me/toncats_tg",
                    "telegram_handle": "toncats_tg",
                    "wallet_address": "",
                    "github_repo": "",
                    "description": "Telegram candidate",
                    "project_type": "",
                    "source_labels": ["public_web"],
                    "match_reason": "telegram",
                    "score": 0.9,
                },
                {
                    "name": "TON Cats",
                    "project_url": "https://toncats.pw/en/",
                    "telegram_handle": "toncats_tg",
                    "wallet_address": "",
                    "github_repo": "",
                    "description": "Website candidate",
                    "project_type": "",
                    "source_labels": ["public_web"],
                    "match_reason": "site",
                    "score": 0.8,
                },
            ]
        )
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["telegram_handle"], "toncats_tg")
        self.assertEqual(merged[0]["project_url"], "https://toncats.pw/en")

    def test_compound_query_tokens_keep_brand_identity(self) -> None:
        self.assertIn("toncats", _discovery_match_tokens("ton cats"))
        self.assertEqual(_market_search_terms("ton cats")[:2], ["ton cats", "toncats"])

    def test_market_search_terms_preserve_short_symbol_with_ton_hint(self) -> None:
        terms = _market_search_terms("NOT ton")
        self.assertTrue(any(term.lower() == "not" for term in terms))

    def test_public_identity_search_queries_preserve_short_symbol_with_ton_hint(self) -> None:
        terms = _public_identity_search_queries("NOT ton")
        lowered = {term.lower() for term in terms}
        self.assertIn("not", lowered)
        self.assertIn("not telegram", lowered)

    def test_select_market_identity_url_prefers_canonical_site_over_telegram(self) -> None:
        url = _select_market_identity_url(
            ["https://t.me/notcoin_bot", "https://notco.in", "https://t.me/notcoin"],
            "Notcoin NOT token on TON",
        )
        self.assertEqual(url, "https://notco.in")

    def test_registry_skip_external_for_strong_non_token_candidate(self) -> None:
        candidate = {
            "name": "Tonviewer",
            "github_repo": "",
            "project_url": "https://tonviewer.com",
            "telegram_handle": "",
            "wallet_address": "",
            "description": "Blockchain explorer for TON.",
            "project_type": "explorer",
            "source_labels": ["registry"],
            "match_reason": "project name mentioned in query",
            "score": 1.2,
        }
        self.assertFalse(
            _should_skip_external_discovery_from_registry(
                [candidate],
                "Tonviewer ton",
                "interactive",
                explicit_repo="",
                explicit_url="",
                explicit_telegram="",
            )
        )

    def test_registry_skip_external_for_strong_candidate_in_full_mode(self) -> None:
        candidate = {
            "name": "TON Blockchain",
            "github_repo": "ton-blockchain/ton",
            "project_url": "https://github.com/ton-blockchain/ton",
            "telegram_handle": "toncoin",
            "wallet_address": "",
            "description": "Reference TON blockchain implementation and core infrastructure.",
            "project_type": "protocol_infra",
            "source_labels": ["registry"],
            "match_reason": "project name mentioned in query",
            "score": 0.91,
        }
        self.assertTrue(
            _should_skip_external_discovery_from_registry(
                [candidate],
                "TON Blockchain",
                "full",
                explicit_repo="",
                explicit_url="",
                explicit_telegram="",
            )
        )

    def test_registry_skip_external_for_clear_top_candidate_with_multiple_registry_matches(self) -> None:
        top_candidate = {
            "name": "TON Diamonds",
            "github_repo": "",
            "project_url": "https://ton.diamonds",
            "telegram_handle": "",
            "wallet_address": "",
            "description": "NFT marketplace on TON.",
            "project_type": "nft_marketplace",
            "source_labels": ["registry"],
            "match_reason": "project name mentioned in query, catalog alias match",
            "score": 1.97,
        }
        secondary_candidate = {
            "name": "Getgems NFT Contracts",
            "github_repo": "getgems-io/nft-contracts",
            "project_url": "https://getgems.io",
            "telegram_handle": "getgems",
            "wallet_address": "",
            "description": "TON NFT marketplace contract repository.",
            "project_type": "smart_contracts",
            "source_labels": ["registry"],
            "match_reason": "Matched against known TON project registry.",
            "score": 0.79,
        }
        self.assertTrue(
            _should_skip_external_discovery_from_registry(
                [top_candidate, secondary_candidate],
                "TON Diamonds NFT marketplace",
                "interactive",
                explicit_repo="",
                explicit_url="",
                explicit_telegram="",
            )
        )

    def test_public_identity_candidate_ignores_unrelated_wallet_and_telegram_from_brand_site(self) -> None:
        page_html = """
<html>
  <head><title>Tonviewer | tonviewer.com</title></head>
  <body>
    <a href="https://t.me/wallet">Wallet</a>
    <a href="https://tonviewer.com/EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c">Example address</a>
  </body>
</html>
"""
        with patch("identity_validator.viewer.http_fetch_text", new=AsyncMock(return_value=page_html)):
            candidate = asyncio.run(_public_identity_result_candidate("https://tonviewer.com", "Tonviewer ton", "interactive"))
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["project_url"], "https://tonviewer.com")
        self.assertEqual(candidate["telegram_handle"], "")
        self.assertEqual(candidate["wallet_address"], "")

    def test_candidate_query_rank_score_penalizes_example_repo(self) -> None:
        official_candidate = {
            "name": "STON.fi SDK",
            "github_repo": "ston-fi/sdk",
            "project_url": "https://ston.fi",
            "telegram_handle": "stonfidex",
            "wallet_address": "",
            "description": "Official SDK for building on STON.fi protocol.",
            "project_type": "tooling_sdk",
            "source_labels": ["registry", "github_search"],
            "match_reason": "registry",
            "score": 0.55,
        }
        example_candidate = {
            "name": "DenisSoldugeev/dex-swap-example",
            "github_repo": "DenisSoldugeev/dex-swap-example",
            "project_url": "https://dex-swap-example.git",
            "telegram_handle": "",
            "wallet_address": "",
            "description": "Terminal styled DEX swap example for TON powered by STON.fi.",
            "project_type": "",
            "source_labels": ["github_search"],
            "match_reason": "search",
            "score": 0.72,
        }
        self.assertFalse(_is_canonical_project_url(example_candidate["project_url"]))
        self.assertGreater(
            _candidate_query_rank_score(official_candidate, "STON.fi TON DEX"),
            _candidate_query_rank_score(example_candidate, "STON.fi TON DEX"),
        )

    def test_candidate_query_rank_score_penalizes_learning_repo_against_official_protocol(self) -> None:
        official_candidate = {
            "name": "TON Blockchain",
            "github_repo": "ton-blockchain/ton",
            "project_url": "https://github.com/ton-blockchain/ton",
            "telegram_handle": "toncoin",
            "wallet_address": "",
            "description": "Reference TON blockchain implementation and core infrastructure.",
            "project_type": "protocol_infra",
            "source_labels": ["registry", "github_search"],
            "match_reason": "official",
            "score": 0.91,
        }
        learning_candidate = {
            "name": "lehongvo/TonBlockchain",
            "github_repo": "lehongvo/tonblockchain",
            "project_url": "",
            "telegram_handle": "",
            "wallet_address": "",
            "description": "Learn Ton Blockchain",
            "project_type": "",
            "source_labels": ["github_search"],
            "match_reason": "search",
            "score": 1.065,
        }
        self.assertGreater(
            _candidate_query_rank_score(official_candidate, "TON Blockchain"),
            _candidate_query_rank_score(learning_candidate, "TON Blockchain"),
        )

    def test_registry_discovery_candidates_include_seed_catalog_match(self) -> None:
        candidates = _registry_discovery_candidates("Bemo TON liquid staking", "", "", "")
        self.assertTrue(candidates)
        self.assertEqual(candidates[0]["name"], "bemo")
        self.assertEqual(candidates[0]["project_url"], "https://bemo.fi")
        self.assertEqual(candidates[0]["telegram_handle"], "bemofinance")

    def test_registry_discovery_candidates_include_dedust_seed_match(self) -> None:
        candidates = _registry_discovery_candidates("DeDust TON DEX", "", "", "")
        self.assertTrue(candidates)
        self.assertEqual(candidates[0]["name"], "DeDust")
        self.assertEqual(candidates[0]["project_url"], "https://dedust.io")
        self.assertEqual(candidates[0]["telegram_handle"], "dedust")

    def test_registry_discovery_candidates_include_evaa_seed_match(self) -> None:
        candidates = _registry_discovery_candidates("EVAA TON lending protocol", "", "", "")
        self.assertTrue(candidates)
        self.assertEqual(candidates[0]["name"], "EVAA")
        self.assertEqual(candidates[0]["project_url"], "https://evaa.finance")
        self.assertEqual(candidates[0]["telegram_handle"], "evaatg")

    def test_registry_discovery_candidates_include_ton_dns_seed_match(self) -> None:
        candidates = _registry_discovery_candidates("TON DNS .ton domain service", "", "", "")
        self.assertTrue(candidates)
        self.assertEqual(candidates[0]["name"], "TON DNS")
        self.assertEqual(candidates[0]["project_url"], "https://dns.ton.org")
        self.assertEqual(candidates[0]["github_repo"], "ton-blockchain/dns-contract")

    def test_candidate_query_rank_score_penalizes_unofficial_dashboard_for_product_query(self) -> None:
        official_candidate = {
            "name": "DeDust",
            "github_repo": "",
            "project_url": "https://dedust.io",
            "telegram_handle": "dedust",
            "wallet_address": "",
            "description": "Decentralized exchange on TON.",
            "project_type": "dex",
            "source_labels": ["registry"],
            "match_reason": "registry",
            "score": 0.58,
        }
        dashboard_candidate = {
            "name": "aniruddhsingh7901/dedust-dune-dashboard",
            "github_repo": "aniruddhsingh7901/dedust-dune-dashboard",
            "project_url": "",
            "telegram_handle": "",
            "wallet_address": "",
            "description": "A comprehensive Dune Analytics dashboard for DeDust DEX on the TON blockchain.",
            "project_type": "",
            "source_labels": ["github_search"],
            "match_reason": "search",
            "score": 0.74,
        }
        self.assertGreater(
            _candidate_query_rank_score(official_candidate, "DeDust TON DEX"),
            _candidate_query_rank_score(dashboard_candidate, "DeDust TON DEX"),
        )

    def test_merge_candidates_disambiguates_duplicate_market_names(self) -> None:
        merged = _merge_candidates(
            [
                {
                    "name": "CATS",
                    "project_url": "https://www.geckoterminal.com/ton/pools/pool-a",
                    "telegram_handle": "",
                    "wallet_address": "EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c",
                    "github_repo": "",
                    "description": "Market candidate A",
                    "project_type": "token",
                    "source_labels": ["geckoterminal"],
                    "match_reason": "market a",
                    "score": 1.0,
                },
                {
                    "name": "CATS",
                    "project_url": "https://www.geckoterminal.com/ton/pools/pool-b",
                    "telegram_handle": "",
                    "wallet_address": "EQBadq9p12uC1KfSiPCAaoEvhpXPHj7hBWq-mqGntuwE2C1C",
                    "github_repo": "",
                    "description": "Market candidate B",
                    "project_type": "token",
                    "source_labels": ["geckoterminal"],
                    "match_reason": "market b",
                    "score": 0.95,
                },
            ]
        )
        self.assertEqual(len(merged), 2)
        self.assertNotEqual(merged[0]["name"], merged[1]["name"])
        self.assertIn("|", merged[0]["name"])
        self.assertIn("|", merged[1]["name"])

    def test_merge_candidates_prefers_human_name_over_raw_url_name(self) -> None:
        merged = _merge_candidates(
            [
                {
                    "name": "https://ston.fi",
                    "project_url": "https://ston.fi",
                    "telegram_handle": "",
                    "wallet_address": "",
                    "github_repo": "",
                    "description": "Raw user input",
                    "project_type": "",
                    "source_labels": ["user_input"],
                    "match_reason": "input",
                    "score": 0.55,
                },
                {
                    "name": "STON.fi SDK",
                    "project_url": "https://ston.fi",
                    "telegram_handle": "stonfidex",
                    "wallet_address": "",
                    "github_repo": "ston-fi/sdk",
                    "description": "Official SDK for building on STON.fi protocol.",
                    "project_type": "tooling_sdk",
                    "source_labels": ["registry"],
                    "match_reason": "registry",
                    "score": 0.58,
                },
            ]
        )
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["name"], "STON.fi SDK")

    def test_interactive_discovery_skips_public_web_for_strong_candidate(self) -> None:
        candidates = [
            {
                "candidate_key": "github:tonkeeper/tonkeeper-web",
                "name": "Tonkeeper Web",
                "github_repo": "tonkeeper/tonkeeper-web",
                "project_url": "https://tonkeeper.com",
                "telegram_handle": "tonkeeper_news",
                "wallet_address": "",
                "description": "Wallet",
                "project_type": "wallet_app",
                "source_labels": ["registry", "github_search"],
                "match_reason": "strong",
                "score": 0.9,
            }
        ]
        self.assertTrue(_should_skip_public_web_identity(candidates, "Tonkeeper TON wallet", "interactive"))
        self.assertFalse(_should_skip_public_web_identity(candidates, "Tonkeeper TON wallet", "full"))

    def test_interactive_discovery_keeps_public_web_for_weak_candidate(self) -> None:
        candidates = [
            {
                "candidate_key": "wallet:EQABC",
                "name": "Token",
                "github_repo": "",
                "project_url": "https://www.geckoterminal.com/ton/pools/pool-a",
                "telegram_handle": "",
                "wallet_address": "EQABC",
                "description": "Market token candidate",
                "project_type": "token",
                "source_labels": ["geckoterminal"],
                "match_reason": "market",
                "score": 0.9,
            }
        ]
        self.assertFalse(_should_skip_public_web_identity(candidates, "EVAA TON lending protocol", "interactive"))

    def test_geckoterminal_candidates_filter_non_ton_networks(self) -> None:
        payload = {
            "data": [
                {
                    "id": "ton_EQDHXArD2nU7DZ4coDGKxCLiKRJC-TKmkr_U52IjYetNwksA",
                    "attributes": {
                        "address": "EQDHXArD2nU7DZ4coDGKxCLiKRJC-TKmkr_U52IjYetNwksA",
                        "name": "CATS / TON",
                    },
                    "relationships": {
                        "base_token": {"data": {"id": "ton_EQA3AshPEVly8wQ6mZincrKC_CkJSKXqqjyg0VMsVjF_CATS"}},
                        "quote_token": {"data": {"id": "ton_EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c"}},
                    },
                },
                {
                    "id": "base_0x61477557428a430ca8a6ea6ae8a8e9d80a5087e6",
                    "attributes": {
                        "address": "0x61477557428a430ca8a6ea6ae8a8e9d80a5087e6",
                        "name": "TONNEL Cats Coin / WETH",
                    },
                    "relationships": {
                        "base_token": {"data": {"id": "base_0x90a181612d3d2f77edc3823d80bf53e80007b70a"}},
                        "quote_token": {"data": {"id": "base_0x4200000000000000000000000000000000000006"}},
                    },
                },
            ]
        }
        with patch("identity_validator.viewer.http_fetch_json", new=AsyncMock(return_value=payload)):
            candidates = asyncio.run(_geckoterminal_candidates("ton cats"))
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["wallet_address"], "EQA3AshPEVly8wQ6mZincrKC_CkJSKXqqjyg0VMsVjF_CATS")

    def test_public_identity_enrichment_adds_official_ton_cats_candidate(self) -> None:
        initial_candidates = [
            {
                "candidate_key": "wallet:EQBadq9p12uC1KfSiPCAaoEvhpXPHj7hBWq-mqGntuwE2C1C",
                "name": "CATS",
                "github_repo": "",
                "project_url": "https://www.geckoterminal.com/ton/pools/EQCZCyfGkF-J9JT7ilqy2ihTGJHK7xjZwnLaWj6TUaNBAtrR",
                "telegram_handle": "",
                "wallet_address": "EQBadq9p12uC1KfSiPCAaoEvhpXPHj7hBWq-mqGntuwE2C1C",
                "description": "Market candidate",
                "project_type": "token",
                "source_labels": ["geckoterminal"],
                "match_reason": "market",
                "score": 1.0,
            }
        ]
        official_candidates = [
            {
                "candidate_key": "telegram:toncats_tg",
                "name": "TON Cats",
                "github_repo": "toncats-tg/refCatsjetton",
                "project_url": "https://toncats.pw/en/",
                "telegram_handle": "toncats_tg",
                "wallet_address": "",
                "description": "Official TON Cats website",
                "project_type": "",
                "source_labels": ["public_web"],
                "match_reason": "official web",
                "score": 1.1,
            }
        ]
        merged_candidates = _merge_candidates(initial_candidates + official_candidates)
        with patch("identity_validator.viewer._public_identity_candidates", new=AsyncMock(return_value=official_candidates)), patch(
            "identity_validator.viewer._enrich_candidates_with_public_wallets",
            new=AsyncMock(return_value=(merged_candidates, {"status": "skipped", "candidate_count": 0, "summary": "skipped"})),
        ):
            enriched, status = asyncio.run(_enrich_candidates_with_public_identity(initial_candidates, "ton cats"))
        self.assertEqual(status["status"], "success")
        self.assertEqual(enriched[0]["telegram_handle"], "toncats_tg")
        self.assertEqual(enriched[0]["project_url"], "https://toncats.pw/en")

    def _get_json(self, url: str) -> dict:
        with urllib.request.urlopen(url, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))

    def _get_text(self, url: str) -> str:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.read().decode("utf-8")


if __name__ == "__main__":
    unittest.main()
