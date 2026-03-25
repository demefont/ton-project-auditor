from __future__ import annotations

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult


class ContractValidatorBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        project_type_result = context.get_result("project_type")
        tree_result = context.get_result("github_tree")
        address_result = context.get_result("address_signal")
        if not project_type_result or project_type_result.status != "success":
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary="Project type is unavailable",
                flags=["missing_project_type"],
            )
        project_type_data = project_type_result.data or {}
        project_type = str(project_type_data.get("project_type") or "unknown")
        requires_contract = bool(project_type_data.get("requires_contract"))
        blockchain_related = bool(project_type_data.get("blockchain_related"))
        contract_analysis_mode = str(project_type_data.get("contract_analysis_mode") or "not_applicable")
        if not blockchain_related:
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary=f"Project type {project_type} is not classified as blockchain-related",
                data={"project_type": project_type, "requires_contract": False, "contract_analysis_mode": "not_applicable"},
            )
        contract_paths = []
        contract_file_count = 0
        contract_ext_counts = {}
        if tree_result and tree_result.status == "success":
            contract_paths = [str(item) for item in (tree_result.data or {}).get("contract_paths_sample") or []]
            contract_file_count = int((tree_result.metrics or {}).get("contract_file_count") or 0)
            contract_ext_counts = dict((tree_result.data or {}).get("contract_ext_counts") or {})
        unique_addresses = []
        if address_result and address_result.status == "success":
            unique_addresses = [str(item) for item in (address_result.data or {}).get("unique_addresses") or []]
        flags = []
        if contract_analysis_mode == "required" and contract_file_count == 0:
            flags.append("missing_contract_files")
        if contract_analysis_mode == "required" and not unique_addresses:
            flags.append("missing_address_signal")
        contract_score = 20 if contract_analysis_mode == "optional" else 30
        if contract_file_count:
            contract_score += min(50, contract_file_count * 5)
        if unique_addresses:
            contract_score += min(20, len(unique_addresses) * 10)
        contract_languages = {
            "tact": 0,
            "func": 0,
            "fif": 0,
            "solidity_like": 0,
        }
        for path in contract_paths:
            low = path.lower()
            if low.endswith(".tact"):
                contract_languages["tact"] += 1
            elif low.endswith(".fc") or low.endswith(".func"):
                contract_languages["func"] += 1
            elif low.endswith(".fif"):
                contract_languages["fif"] += 1
            elif low.endswith(".sol"):
                contract_languages["solidity_like"] += 1
        summary = f"Contract analysis mode={contract_analysis_mode} for {project_type}"
        if contract_file_count:
            summary += f", found {contract_file_count} contract-like files"
        else:
            summary += ", contract-like files not found"
        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=summary,
            metrics={
                "contract_file_count": contract_file_count,
                "address_signal_count": len(unique_addresses),
                "contract_score": min(100, contract_score),
                "contract_analysis_required": 1 if contract_analysis_mode == "required" else 0,
            },
            data={
                "project_type": project_type,
                "requires_contract": requires_contract,
                "blockchain_related": blockchain_related,
                "contract_analysis_mode": contract_analysis_mode,
                "contract_paths": contract_paths[:50],
                "addresses": unique_addresses[:20],
                "contract_languages": contract_languages,
                "contract_ext_counts": contract_ext_counts,
            },
            evidence=contract_paths[:10] + unique_addresses[:5],
            flags=flags,
            confidence=0.85 if contract_file_count else (0.65 if contract_analysis_mode == "optional" else 0.45),
            needs_human_review=bool(flags),
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return ContractValidatorBlock(manifest)
