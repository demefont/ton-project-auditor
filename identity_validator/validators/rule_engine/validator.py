from __future__ import annotations

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult
from identity_validator.utils import stable_score


class RuleEngineBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        repo_result = context.get_result("github_repo")
        tree_result = context.get_result("github_tree")
        tg_result = context.get_result("telegram_channel")
        tg_semantics_result = context.get_result("telegram_semantics")
        activity_result = context.get_result("github_activity")
        project_type_result = context.get_result("project_type")
        address_result = context.get_result("address_signal")
        contract_result = context.get_result("contract_validator")
        consistency_result = context.get_result("claim_consistency")
        risk_result = context.get_result("risk_validator")
        sonar_result = context.get_result("sonar_research")
        similarity_result = context.get_result("project_similarity")
        identity_result = context.get_result("identity_confirmation")

        repo_available = bool(repo_result and repo_result.status == "success")
        tree_available = bool(tree_result and tree_result.status == "success")
        tg_available = bool(tg_result and tg_result.status == "success")
        tg_semantics_available = bool(tg_semantics_result and tg_semantics_result.status == "success")
        activity_available = bool(activity_result and activity_result.status == "success")
        similarity_available = bool(similarity_result and similarity_result.status == "success")
        project_type_available = bool(project_type_result and project_type_result.status == "success")

        repo_metrics = (repo_result.metrics or {}) if repo_available else {}
        tree_metrics = (tree_result.metrics or {}) if tree_available else {}
        tg_metrics = (tg_result.metrics or {}) if tg_available else {}
        tg_semantics_metrics = (tg_semantics_result.metrics or {}) if tg_semantics_available else {}
        tg_semantics_data = (tg_semantics_result.data or {}) if tg_semantics_available else {}
        activity_metrics = (activity_result.metrics or {}) if activity_available else {}
        address_metrics = (address_result.metrics or {}) if address_result else {}
        address_data = (address_result.data or {}) if address_result else {}
        consistency_metrics = (consistency_result.metrics or {}) if consistency_result else {}
        risk_metrics = (risk_result.metrics or {}) if risk_result else {}
        contract_metrics = (contract_result.metrics or {}) if contract_result else {}
        similarity_metrics = (similarity_result.metrics or {}) if similarity_available else {}
        similarity_data = (similarity_result.data or {}) if similarity_available else {}
        similarity_flags = [str(item) for item in (similarity_result.flags or [])] if similarity_result else []
        identity_metrics = (identity_result.metrics or {}) if identity_result and identity_result.status == "success" else {}
        identity_data = (identity_result.data or {}) if identity_result and identity_result.status == "success" else {}
        ton_mcp_data = (address_data.get("ton_mcp") or {}) if isinstance(address_data, dict) else {}
        ton_activity_data = (address_data.get("ton_activity") or {}) if isinstance(address_data, dict) else {}
        ton_mcp_match = ton_mcp_data.get("matched_jetton") or {}
        ton_mcp_match_confirmed = int(address_metrics.get("ton_mcp_known_jetton_match") or 0) > 0
        ton_address_activity_checked = int(address_metrics.get("ton_address_activity_checked") or 0) > 0
        ton_address_last_tx_age_days = (
            -1
            if address_metrics.get("ton_address_last_tx_age_days") in (None, "")
            else int(address_metrics.get("ton_address_last_tx_age_days"))
        )
        ton_address_tx_count_30d = int(address_metrics.get("ton_address_tx_count_30d") or 0)
        reported_onchain_tx_count_30d = ton_address_tx_count_30d if ton_address_activity_checked else None
        reported_last_onchain_tx_age_days = ton_address_last_tx_age_days if ton_address_activity_checked else None
        identity_status = str(identity_data.get("identity_status") or "unknown")
        evidence_status = str(identity_data.get("evidence_status") or "partial")
        identity_score = stable_score(int(identity_metrics.get("identity_score") or 50))
        source_failures = [str(item) for item in identity_data.get("source_failures") or []]

        relevance = (
            stable_score(
                15
                + int(repo_metrics.get("ton_keyword_hits") or 0) * 10
                + int((project_type_result.metrics or {}).get("best_score") or 0) * 4
            )
            if repo_available
            else 50
        )
        maturity = (
            stable_score(
                10
                + min(45, int(tree_metrics.get("total_files") or 0) / 4)
                + min(25, int(repo_metrics.get("readme_chars") or 0) / 120)
                + min(20, int(repo_metrics.get("stargazers_count") or 0) / 50)
            )
            if repo_available or tree_available
            else 50
        )
        reported_community_activity = (
            stable_score(int(tg_metrics.get("community_activity_score") or 0))
            if tg_available
            else None
        )
        reported_community_quality = (
            stable_score(int(tg_semantics_metrics.get("community_health_score") or 60))
            if tg_semantics_available
            else None
        )
        community_activity = reported_community_activity if reported_community_activity is not None else 50
        community_quality = (
            reported_community_quality
            if reported_community_quality is not None
            else (60 if tg_available else 50)
        )
        reported_community = (
            stable_score(
                community_activity * 0.55
                + community_quality * 0.35
                + min(15, int(tg_metrics.get("ton_keyword_hits") or 0) * 3)
                - min(25, int(tg_metrics.get("scam_keyword_hits") or 0) * 10)
            )
            if tg_available or tg_semantics_available
            else None
        )
        community = reported_community if reported_community is not None else 50
        contract_health = 60
        if contract_result:
            if contract_result.status == "skipped":
                contract_health = 60
            else:
                contract_health = stable_score(
                    int(contract_metrics.get("contract_score") or 0)
                )
        consistency = stable_score(int(consistency_metrics.get("alignment_score") or 0))
        risk_score = stable_score(int(risk_metrics.get("risk_score") or 0))
        reported_activity = (
            stable_score(int(activity_metrics.get("activity_score") or 50))
            if activity_available
            else None
        )
        reported_originality = (
            stable_score(int(similarity_metrics.get("originality_score") or 50))
            if similarity_available
            else None
        )
        activity = reported_activity if reported_activity is not None else 50
        originality = reported_originality if reported_originality is not None else 50

        overall = stable_score(
            relevance * 0.20
            + maturity * 0.15
            + consistency * 0.14
            + contract_health * 0.10
            + community * 0.10
            + activity * 0.10
            + originality * 0.09
            + identity_score * 0.12
            - risk_score * 0.24
            + 20
        )
        if identity_status == "mismatch":
            overall = min(overall, 35)
        elif identity_status in {"weak", "incomplete"}:
            overall = min(overall, 72)
        project_type = str((project_type_result.data or {}).get("project_type") or "unknown") if project_type_result else "unknown"
        risk_level = str((risk_result.data or {}).get("risk_level") or "unknown") if risk_result else "unknown"
        clone_risk = str(similarity_data.get("clone_risk") or "unknown") if similarity_result else "unknown"
        closest_projects = [item for item in similarity_data.get("closest_projects") or [] if isinstance(item, dict)]
        distinctive_features = [str(item) for item in similarity_data.get("distinctive_features") or []]
        community_findings = [str(item) for item in tg_semantics_data.get("content_labels") or []]
        strengths = []
        risks = []
        if relevance >= 65:
            strengths.append("strong_ton_relevance")
        if maturity >= 60:
            strengths.append("solid_repository_depth")
        if contract_health >= 70 and project_type == "smart_contracts":
            strengths.append("contracts_are_visible")
        if community_activity >= 55 and community_quality >= 50:
            strengths.append("recent_public_channel_activity")
        if community_quality >= 70:
            strengths.append("community_feed_looks_curated")
        if activity >= 55:
            strengths.append("recent_git_activity")
        if originality >= 70:
            strengths.append("distinct_from_registry")
        if identity_status == "confirmed":
            strengths.append("identity_confirmed")
        if ton_mcp_match_confirmed:
            strengths.append("ton_mcp_known_jetton_verified")
        if ton_address_activity_checked and ton_address_last_tx_age_days >= 0 and ton_address_last_tx_age_days <= 30:
            strengths.append("recent_onchain_activity")
        for flag in (risk_result.flags or []) if risk_result else []:
            risks.append(flag)
        if contract_result and contract_result.status == "success":
            for flag in contract_result.flags or []:
                if flag not in risks:
                    risks.append(flag)
        if similarity_result and similarity_result.status == "success":
            for flag in similarity_result.flags or []:
                if flag not in risks:
                    risks.append(flag)
        if ton_address_activity_checked and ton_address_last_tx_age_days >= 180 and "onchain_activity_is_stale" not in risks:
            risks.append("onchain_activity_is_stale")
        next_checks = ["manual_review"] if overall < 55 or risk_level == "high" else ["optional_manual_review"]
        if identity_status == "mismatch":
            next_checks.append("reselect_project_candidate")
        elif identity_status in {"weak", "incomplete"}:
            next_checks.append("confirm_project_identity")
        if evidence_status == "incomplete" or source_failures:
            next_checks.append("retry_source_collection")
        if project_type_available and project_type == "unknown":
            next_checks.append("refine_project_type")
        if contract_result and "missing_address_signal" in (contract_result.flags or []):
            next_checks.append("verify_contract_addresses")
        if clone_risk in {"moderate", "high"} or "self_declared_repository_copy" in similarity_flags:
            next_checks.append("review_project_originality")
        if activity < 35:
            next_checks.append("review_project_activity")
        if ton_address_activity_checked and ton_address_last_tx_age_days >= 30 and ton_address_tx_count_30d == 0:
            next_checks.append("review_onchain_activity")
        if tg_available and community_activity < 35:
            next_checks.append("review_community_activity")
        if tg_semantics_available and community_quality < 45:
            next_checks.append("review_community_feed_quality")
        if sonar_result and sonar_result.status == "success":
            strengths.append("external_public_signal_checked")
        deduped_strengths = []
        for item in strengths:
            if item not in deduped_strengths:
                deduped_strengths.append(item)
        deduped_risks = []
        for item in risks:
            if item not in deduped_risks:
                deduped_risks.append(item)
        deduped_next_checks = []
        for item in next_checks:
            if item not in deduped_next_checks:
                deduped_next_checks.append(item)
        needs_human_review = (
            overall < 55
            or risk_level == "high"
            or (project_type_available and project_type == "unknown")
            or clone_risk == "high"
            or identity_status != "confirmed"
            or evidence_status == "incomplete"
        )

        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=f"type={project_type} overall={overall} risk={risk_level} identity={identity_status} clone={clone_risk}",
            metrics={
                "overall_score": overall,
                "relevance_score": relevance,
                "maturity_score": maturity,
                "community_score": reported_community,
                "community_activity_score": reported_community_activity,
                "community_quality_score": reported_community_quality,
                "contract_score": contract_health,
                "consistency_score": consistency,
                "activity_score": reported_activity,
                "originality_score": reported_originality,
                "risk_score": risk_score,
                "identity_score": identity_score,
                "onchain_tx_count_30d": reported_onchain_tx_count_30d,
                "last_onchain_tx_age_days": reported_last_onchain_tx_age_days,
            },
            data={
                "project_type": project_type,
                "risk_level": risk_level,
                "clone_risk": clone_risk,
                "identity_status": identity_status,
                "evidence_status": evidence_status,
                "source_failures": source_failures,
                "closest_projects": closest_projects,
                "clone_source_project": similarity_data.get("clone_source_project") or {},
                "self_declared_copy_excerpt": similarity_data.get("self_declared_copy_excerpt") or "",
                "distinctive_features": distinctive_features,
                "community_findings": community_findings,
                "ton_mcp_known_jetton": ton_mcp_match,
                "ton_address_activity": ton_activity_data,
                "strengths": deduped_strengths,
                "risks": deduped_risks,
                "next_checks": deduped_next_checks,
            },
            evidence=deduped_strengths + deduped_risks,
            flags=deduped_risks,
            confidence=0.8,
            needs_human_review=needs_human_review,
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return RuleEngineBlock(manifest)
