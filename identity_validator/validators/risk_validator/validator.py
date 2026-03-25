from __future__ import annotations

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult


class RiskValidatorBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        repo_result = context.get_result("github_repo")
        tg_result = context.get_result("telegram_channel")
        tg_semantics_result = context.get_result("telegram_semantics")
        project_type_result = context.get_result("project_type")
        contract_result = context.get_result("contract_validator")
        consistency_result = context.get_result("claim_consistency")
        activity_result = context.get_result("github_activity")
        identity_result = context.get_result("identity_confirmation")

        risk_score = 0
        flags = []
        repo_available = bool(repo_result and repo_result.status == "success")
        repo_metrics = (repo_result.metrics or {}) if repo_available else {}
        repo_meta = (repo_result.data or {}).get("repo") if repo_available else {}
        if repo_meta and repo_meta.get("archived"):
            risk_score += 25
            flags.append("repo_archived")
        if repo_available and int(repo_metrics.get("readme_chars") or 0) < 120:
            risk_score += 10
            flags.append("readme_is_too_short")
        if repo_available and int(repo_metrics.get("ton_keyword_hits") or 0) == 0:
            risk_score += 20
            flags.append("ton_relevance_is_weak")

        if tg_result and tg_result.status == "success":
            tg_metrics = tg_result.metrics or {}
            scam_hits = int((tg_result.metrics or {}).get("scam_keyword_hits") or 0)
            if scam_hits:
                risk_score += min(40, scam_hits * 15)
                flags.append("telegram_contains_scam_terms")
            last_post_age_days = int(tg_metrics.get("last_post_age_days") or -1)
            posts_30d = int(tg_metrics.get("posts_30d") or 0)
            community_activity_score = int(tg_metrics.get("community_activity_score") or 0)
            if last_post_age_days >= 90:
                risk_score += 12
                flags.append("telegram_recent_activity_is_weak")
            elif last_post_age_days >= 30 and posts_30d == 0:
                risk_score += 6
                flags.append("telegram_recent_activity_is_cooling")
            if community_activity_score < 20 and last_post_age_days >= 30:
                risk_score += 5
                flags.append("telegram_public_activity_is_low")

        if tg_semantics_result and tg_semantics_result.status == "success":
            semantic_metrics = tg_semantics_result.metrics or {}
            semantic_risk_score = int(semantic_metrics.get("semantic_risk_score") or 0)
            if semantic_risk_score >= 60:
                risk_score += 20
                flags.append("telegram_semantic_risk_high")
            elif semantic_risk_score >= 30:
                risk_score += 10
                flags.append("telegram_semantic_risk_moderate")
            for flag in tg_semantics_result.flags or []:
                if flag == "telegram_feed_is_overly_promotional":
                    risk_score += 5
                elif flag == "telegram_feed_is_repetitive":
                    risk_score += 8
                elif flag == "telegram_uses_urgency_marketing":
                    risk_score += 6
                elif flag == "telegram_activity_is_bursty":
                    risk_score += 5
                elif flag == "telegram_semantic_scam_signals":
                    risk_score += 10
                flags.append(flag)

        project_type_available = bool(project_type_result and project_type_result.status == "success")
        project_type = str((project_type_result.data or {}).get("project_type") or "unknown") if project_type_result else "unknown"
        if project_type_available and project_type == "unknown":
            risk_score += 15
            flags.append("project_type_unknown")

        if contract_result and contract_result.status == "success":
            for flag in contract_result.flags or []:
                if flag == "missing_contract_files":
                    risk_score += 30
                    flags.append(flag)
                elif flag == "missing_address_signal":
                    risk_score += 10
                    flags.append(flag)

        if activity_result and activity_result.status == "success":
            activity_metrics = activity_result.metrics or {}
            last_commit_age_days = int(activity_metrics.get("last_commit_age_days") or -1)
            commits_90d = int(activity_metrics.get("commits_90d") or 0)
            unique_authors_365d = int(activity_metrics.get("unique_authors_365d") or 0)
            if last_commit_age_days >= 365:
                risk_score += 20
                flags.append("repo_is_stale")
            elif last_commit_age_days >= 180:
                risk_score += 10
                flags.append("repo_activity_is_old")
            if commits_90d == 0 and last_commit_age_days >= 90:
                risk_score += 10
                flags.append("no_recent_commits")
            if unique_authors_365d <= 1 and int(activity_metrics.get("commits_365d") or 0) >= 6:
                risk_score += 5
                flags.append("single_author_recent_history")

        identity_data = (identity_result.data or {}) if identity_result and identity_result.status == "success" else {}
        identity_status = str(identity_data.get("identity_status") or "unknown")
        evidence_status = str(identity_data.get("evidence_status") or "partial")
        source_failures = [str(item) for item in identity_data.get("source_failures") or []]
        if identity_status == "mismatch":
            risk_score += 30
            flags.append("identity_brand_mismatch")
        elif identity_status == "weak":
            risk_score += 8
            flags.append("identity_unconfirmed")
        if "identity_based_on_noncanonical_reference" in (identity_result.flags or []):
            risk_score += 6
            flags.append("identity_based_on_noncanonical_reference")

        if consistency_result and consistency_result.status == "success":
            mismatch_weights = {
                "repo_has_weak_ton_relevance": 8,
                "telegram_has_weak_project_relevance": 6,
                "claimed_contract_project_without_contract_files": 20,
                "project_type_is_unknown": 8,
            }
            weighted_mismatches = 0
            for flag in consistency_result.flags or []:
                weighted_mismatches += int(mismatch_weights.get(str(flag), 0))
            risk_score += weighted_mismatches
            if weighted_mismatches:
                flags.append("cross_source_mismatches_detected")

        risk_score = max(0, min(100, risk_score))
        risk_level = "low"
        if risk_score >= 60:
            risk_level = "high"
        elif risk_score >= 30:
            risk_level = "moderate"

        deduped_flags = []
        for flag in flags:
            if flag not in deduped_flags:
                deduped_flags.append(flag)

        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=f"Risk level is {risk_level} with score {risk_score}",
            metrics={
                "risk_score": risk_score,
                "flag_count": len(deduped_flags),
            },
            data={
                "risk_level": risk_level,
                "project_type": project_type,
                "identity_status": identity_status,
                "evidence_status": evidence_status,
                "source_failures": source_failures,
            },
            flags=deduped_flags,
            evidence=deduped_flags,
            confidence=0.85,
            needs_human_review=risk_level == "high" or identity_status == "mismatch" or evidence_status == "incomplete",
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return RiskValidatorBlock(manifest)
