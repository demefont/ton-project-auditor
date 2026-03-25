from __future__ import annotations

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult


class ClaimConsistencyBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        repo_result = context.get_result("github_repo")
        tg_result = context.get_result("telegram_channel")
        project_type_result = context.get_result("project_type")
        contract_result = context.get_result("contract_validator")
        identity_result = context.get_result("identity_confirmation")
        mismatches = []
        alignment_points = 100

        repo_is_available = bool(repo_result and repo_result.status == "success")
        ton_keyword_hits = int((repo_result.metrics or {}).get("ton_keyword_hits") or 0) if repo_is_available else 0
        if repo_is_available and ton_keyword_hits == 0:
            mismatches.append("repo_has_weak_ton_relevance")
            alignment_points -= 25

        if tg_result and tg_result.status == "success":
            tg_hits = int((tg_result.metrics or {}).get("ton_keyword_hits") or 0)
            if tg_hits == 0:
                mismatches.append("telegram_has_weak_project_relevance")
                alignment_points -= 15

        project_type = "unknown"
        requires_contract = False
        if project_type_result and project_type_result.status == "success":
            project_type = str((project_type_result.data or {}).get("project_type") or "unknown")
            requires_contract = bool((project_type_result.data or {}).get("requires_contract"))

        if requires_contract and contract_result and contract_result.status == "success":
            if "missing_contract_files" in (contract_result.flags or []):
                mismatches.append("claimed_contract_project_without_contract_files")
                alignment_points -= 35

        if project_type_result and project_type_result.status == "success" and project_type == "unknown":
            mismatches.append("project_type_is_unknown")
            alignment_points -= 20

        identity_data = (identity_result.data or {}) if identity_result and identity_result.status == "success" else {}
        identity_status = str(identity_data.get("identity_status") or "unknown")
        evidence_status = str(identity_data.get("evidence_status") or "partial")
        if identity_status == "mismatch":
            mismatches.append("identity_mismatch_detected")
            alignment_points -= 40
        elif identity_status == "weak":
            mismatches.append("identity_is_weakly_confirmed")
            alignment_points -= 15
        if "identity_based_on_noncanonical_reference" in (identity_result.flags or []):
            mismatches.append("identity_uses_noncanonical_reference")
            alignment_points -= 10

        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=f"Consistency check found {len(mismatches)} mismatches",
            metrics={
                "alignment_score": max(0, alignment_points),
                "mismatch_count": len(mismatches),
            },
            data={
                "project_type": project_type,
                "mismatches": mismatches,
                "identity_status": identity_status,
                "evidence_status": evidence_status,
                "source_failures": list(identity_data.get("source_failures") or []),
            },
            flags=mismatches,
            evidence=mismatches,
            confidence=0.75,
            needs_human_review=len(mismatches) > 1 or identity_status != "confirmed",
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return ClaimConsistencyBlock(manifest)
