from __future__ import annotations

from typing import Dict, List

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult
from identity_validator.utils import lower_text


PROJECT_TYPE_RULES = {
    "protocol_infra": {
        "text_keywords": ["validator", "liteserver", "blockchain", "node", "tonlib", "func compiler"],
        "path_keywords": ["validator", "lite-client", "tonlib", "crypto", "blockchain"],
        "requires_contract": False,
    },
    "smart_contracts": {
        "text_keywords": ["smart contract", "contracts", "jetton", "nft", "sale", "marketplace"],
        "path_keywords": ["contracts/", ".tact", ".fc", ".func", "jetton", "collection", "marketplace"],
        "requires_contract": True,
    },
    "wallet_app": {
        "text_keywords": ["wallet", "extension", "self-custody", "seed phrase", "browser extension"],
        "path_keywords": ["extension", "wallet", "desktop", "mobile"],
        "requires_contract": False,
    },
    "tooling_api": {
        "text_keywords": ["api platform", "public api", "api", "indexer", "rest api", "openapi", "graphql"],
        "path_keywords": ["api", "openapi", "swagger", "server/"],
        "requires_contract": False,
    },
    "tooling_sdk": {
        "text_keywords": ["sdk", "library", "api client", "developer tools", "typescript sdk", "development tool", "development environment", "deploying smart contracts", "blueprint", "template", "cli"],
        "path_keywords": ["sdk", "packages/", "examples/", "api", "templates/", "src/cli/", "src/compile/"],
        "requires_contract": False,
    },
    "protocol_service": {
        "text_keywords": ["dns", "name service", "domain service", ".ton domain", "domain"],
        "path_keywords": ["dns", "domain"],
        "requires_contract": False,
    },
    "explorer": {
        "text_keywords": ["explorer", "block explorer", "analytics", "blockchain explorer", "viewer"],
        "path_keywords": ["explorer", "viewer"],
        "requires_contract": False,
    },
    "dex": {
        "text_keywords": ["dex", "swap", "exchange", "liquidity", "amm"],
        "path_keywords": ["swap", "exchange", "liquidity"],
        "requires_contract": False,
    },
    "derivatives_dex": {
        "text_keywords": ["perpetual", "perpetuals", "futures", "derivatives", "leverage"],
        "path_keywords": ["perp", "futures", "derivatives"],
        "requires_contract": False,
    },
    "staking_protocol": {
        "text_keywords": ["staking", "liquid staking", "validator rewards", "stake ton", "restaking"],
        "path_keywords": ["staking", "stake"],
        "requires_contract": False,
    },
    "nft_marketplace": {
        "text_keywords": ["nft marketplace", "marketplace", "auction", "collection"],
        "path_keywords": ["marketplace", "collection", "nft"],
        "requires_contract": False,
    },
    "dapp_product": {
        "text_keywords": ["app", "platform", "protocol", "service", "product"],
        "path_keywords": ["app/", "frontend", "webapp", "ui/"],
        "requires_contract": False,
    },
}

TYPE_HINT_ALIASES = {
    "wallet": "wallet_app",
    "wallet_app": "wallet_app",
    "api": "tooling_api",
    "tooling_api": "tooling_api",
    "tooling": "tooling_sdk",
    "sdk": "tooling_sdk",
    "tooling_sdk": "tooling_sdk",
    "protocol_infra": "protocol_infra",
    "protocol_service": "protocol_service",
    "dns": "protocol_service",
    "dex": "dex",
    "derivatives_dex": "derivatives_dex",
    "staking_protocol": "staking_protocol",
    "staking": "staking_protocol",
    "liquid_staking": "staking_protocol",
    "nft_marketplace": "nft_marketplace",
    "explorer": "explorer",
    "gamefi": "dapp_product",
    "token": "smart_contracts",
    "jetton": "smart_contracts",
    "meme": "smart_contracts",
    "smart_contracts": "smart_contracts",
}


def _normalized_type_hint(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    compact = raw.replace("-", "_").replace(" ", "_")
    return TYPE_HINT_ALIASES.get(compact, "")


class ProjectTypeBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        repo_result = context.get_result("github_repo")
        tree_result = context.get_result("github_tree")
        raw_type_hint = str(context.case.type_hint or "")
        case_description = str(context.case.description or "")
        if (not repo_result or repo_result.status != "success") and not (raw_type_hint or case_description):
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary="Repository signals and textual type hints are unavailable",
                flags=["missing_repo_signals"],
            )
        repo_data = repo_result.data or {} if repo_result and repo_result.status == "success" else {}
        readme = str(repo_data.get("readme_excerpt") or "")
        repo_meta = repo_data.get("repo") or {}
        repo_full_name = str(repo_meta.get("full_name") or "").lower()
        topics = " ".join(repo_meta.get("topics") or [])
        description = str(repo_meta.get("description") or "")
        paths = []
        if tree_result and tree_result.status == "success":
            paths = [str(item) for item in (tree_result.data or {}).get("paths") or []]
            if not paths:
                paths = [str(item) for item in (tree_result.data or {}).get("paths_sample") or []]
            paths += [str(item) for item in (tree_result.data or {}).get("contract_paths_sample") or []]
        normalized_type_hint = _normalized_type_hint(raw_type_hint)
        joined_text = lower_text(
            [
                description,
                readme,
                topics,
                raw_type_hint,
                raw_type_hint.replace("_", " ").replace("-", " "),
                case_description,
            ]
        )
        low_paths = [str(path or "").lower() for path in paths]
        scores: Dict[str, int] = {}
        reasons: Dict[str, List[str]] = {}
        for project_type, rules in PROJECT_TYPE_RULES.items():
            score = 0
            local_reasons: List[str] = []
            for keyword in rules["text_keywords"]:
                if keyword in joined_text:
                    score += 3
                    local_reasons.append(f"text:{keyword}")
            for keyword in rules["path_keywords"]:
                if any(keyword in path for path in low_paths):
                    score += 2
                    local_reasons.append(f"path:{keyword}")
            if project_type == "tooling_sdk":
                if repo_full_name.endswith("/sdk") or "/sdk" in repo_full_name:
                    score += 5
                    local_reasons.append("repo_name:sdk")
                if "blueprint" in repo_full_name:
                    score += 6
                    local_reasons.append("repo_name:blueprint")
                if "development environment" in joined_text:
                    score += 5
                    local_reasons.append("text:development_environment")
                if "npm install" in joined_text or "package" in joined_text:
                    score += 3
                    local_reasons.append("text:package_signal")
            if project_type == "tooling_api":
                if "api" in repo_full_name or repo_full_name.endswith("/tonapi-go"):
                    score += 5
                    local_reasons.append("repo_name:api")
            if project_type == "protocol_service" and ("dns" in repo_full_name or ".ton" in joined_text):
                score += 5
                local_reasons.append("service:dns")
            if project_type == "explorer" and ("viewer" in repo_full_name or "explorer" in repo_full_name):
                score += 5
                local_reasons.append("repo_name:explorer")
            if project_type == "dex" and ("swap" in repo_full_name or "dex" in repo_full_name):
                score += 4
                local_reasons.append("repo_name:dex")
            if project_type == "derivatives_dex" and ("perp" in repo_full_name or "futures" in repo_full_name):
                score += 4
                local_reasons.append("repo_name:derivatives")
            if project_type == "staking_protocol" and ("stake" in repo_full_name or "staking" in repo_full_name):
                score += 4
                local_reasons.append("repo_name:staking")
            if project_type == "nft_marketplace" and ("market" in repo_full_name or "collection" in repo_full_name):
                score += 4
                local_reasons.append("repo_name:nft_marketplace")
            if project_type == "wallet_app" and "wallet" in repo_full_name:
                score += 4
                local_reasons.append("repo_name:wallet")
            if project_type == "protocol_infra" and repo_full_name.endswith("/ton"):
                score += 4
                local_reasons.append("repo_name:core_ton")
            if project_type == "smart_contracts" and ("contract" in repo_full_name or "contracts" in repo_full_name):
                score += 5
                local_reasons.append("repo_name:contracts")
            if project_type == "smart_contracts" and tree_result and int((tree_result.metrics or {}).get("contract_file_count") or 0) > 0:
                score += 2
                local_reasons.append("path:detected_contract_files")
            if normalized_type_hint == project_type:
                score += 6
                local_reasons.append(f"type_hint:{raw_type_hint}")
            scores[project_type] = score
            reasons[project_type] = local_reasons
        best_type = max(scores, key=scores.get) if scores else "unknown"
        best_score = scores.get(best_type, 0)
        sorted_scores = sorted(scores.values(), reverse=True)
        gap = best_score - (sorted_scores[1] if len(sorted_scores) > 1 else 0)
        confidence = 0.35 + min(0.6, (best_score * 0.08) + max(0, gap) * 0.03)
        if best_score <= 0:
            best_type = "unknown"
            confidence = 0.2
        requires_contract = bool(PROJECT_TYPE_RULES.get(best_type, {}).get("requires_contract", False))
        blockchain_related = best_type != "unknown" or "ton" in joined_text
        contract_analysis_mode = "not_applicable"
        if blockchain_related:
            contract_analysis_mode = "required" if requires_contract else "optional"
        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=f"Classified project as {best_type}",
            metrics={
                "best_score": best_score,
                "requires_contract": requires_contract,
                "blockchain_related": int(blockchain_related),
            },
            data={
                "project_type": best_type,
                "scores": scores,
                "reasons": reasons.get(best_type) or [],
                "requires_contract": requires_contract,
                "blockchain_related": blockchain_related,
                "contract_analysis_mode": contract_analysis_mode,
            },
            evidence=reasons.get(best_type) or [],
            confidence=round(min(0.99, confidence), 2),
            needs_human_review=best_type == "unknown",
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return ProjectTypeBlock(manifest)
