from __future__ import annotations

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult
from identity_validator.project_registry import build_project_profile, compare_profiles, distinctive_features
from identity_validator.utils import clip_text, normalize_ws, stable_score


def _copy_statement_excerpt(readme_excerpt: str, copy_hits: list[str]) -> str:
    text = normalize_ws(readme_excerpt)
    if not text:
        return ""
    if not copy_hits:
        return ""
    lowered = text.lower()
    for hit in copy_hits:
        needle = normalize_ws(str(hit or "")).lower()
        if not needle:
            continue
        index = lowered.find(needle)
        if index < 0:
            continue
        start = max(0, index - 72)
        end = min(len(text), index + len(needle) + 148)
        return clip_text(text[start:end], 220)
    return ""


class ProjectSimilarityBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        registry_result = context.get_result("project_registry")
        repo_result = context.get_result("github_repo")
        tree_result = context.get_result("github_tree")
        project_type_result = context.get_result("project_type")
        activity_result = context.get_result("github_activity")
        if not registry_result or registry_result.status != "success":
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary="Project registry is unavailable",
                flags=["missing_project_registry"],
            )
        if not repo_result or repo_result.status != "success":
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary="Repository signals are unavailable",
                flags=["missing_repo_signals"],
            )
        repo_meta = (repo_result.data or {}).get("repo") or {}
        readme_excerpt = str((repo_result.data or {}).get("readme_excerpt") or "")
        paths = [str(item) for item in (tree_result.data or {}).get("paths") or []] if tree_result else []
        project_type = str((project_type_result.data or {}).get("project_type") or "unknown") if project_type_result else "unknown"
        history_fingerprint = ""
        if activity_result and activity_result.status == "success":
            history_fingerprint = str((activity_result.data or {}).get("recent_commit_history_fingerprint") or "")
        current_profile = build_project_profile(
            case_id=context.case.case_id,
            name=context.case.name,
            github_repo=context.case.github_repo,
            description=context.case.description,
            repo_meta=repo_meta,
            readme=readme_excerpt,
            paths=paths,
            project_type=project_type,
            recent_commit_history_fingerprint=history_fingerprint,
        )
        current_repo_key = normalize_ws(str(current_profile.get("github_repo") or "")).lower()
        current_name_key = normalize_ws(str(current_profile.get("name") or "")).lower()
        registry_profiles = []
        for item in (registry_result.data or {}).get("profiles") or []:
            if not isinstance(item, dict):
                continue
            candidate_repo_key = normalize_ws(str(item.get("github_repo") or "")).lower()
            candidate_name_key = normalize_ws(str(item.get("name") or "")).lower()
            if current_repo_key and candidate_repo_key and current_repo_key == candidate_repo_key:
                continue
            if not current_repo_key and current_name_key and current_name_key == candidate_name_key:
                continue
            registry_profiles.append(item)
        comparisons = [compare_profiles(current_profile, candidate) for candidate in registry_profiles]
        comparisons.sort(key=lambda item: float(item.get("overall_similarity") or 0.0), reverse=True)
        top_matches = comparisons[:3]
        top_similarity = float(top_matches[0].get("overall_similarity") or 0.0) if top_matches else 0.0
        top_same_type = bool(top_matches[0].get("same_type")) if top_matches else False
        top_history_match = bool(top_matches[0].get("history_fingerprint_match")) if top_matches else False
        copy_disclosure_hits = list(current_profile.get("copy_disclosure_hits") or [])
        originality_penalty = int(round(top_similarity * 100))
        if top_same_type:
            originality_penalty += 10
        if top_history_match:
            originality_penalty += 10
        if copy_disclosure_hits:
            originality_penalty += 25
        originality_score = stable_score(100 - originality_penalty)
        clone_risk = "low"
        if top_similarity >= 0.82 or (top_history_match and top_similarity >= 0.72):
            clone_risk = "high"
        elif top_similarity >= 0.62 or (top_history_match and top_similarity >= 0.55):
            clone_risk = "moderate"
        distinctive = distinctive_features(current_profile, top_matches, registry_profiles)
        flags = []
        self_declared_copy_excerpt = _copy_statement_excerpt(readme_excerpt, copy_disclosure_hits)
        if clone_risk == "high":
            flags.append("clone_risk_high")
        elif clone_risk == "moderate":
            flags.append("clone_risk_moderate")
        if copy_disclosure_hits:
            flags.append("self_declared_repository_copy")
        best_text_similarity = float(top_matches[0].get("text_similarity") or 0.0) if top_matches else 0.0
        best_structure_similarity = float(top_matches[0].get("structure_similarity") or 0.0) if top_matches else 0.0
        best_contract_similarity = float(top_matches[0].get("contract_similarity") or 0.0) if top_matches else 0.0
        summary = f"Found {len(top_matches)} closest projects, clone_risk={clone_risk}"
        if copy_disclosure_hits and clone_risk == "low":
            summary += ", repository declares a copy of production code"
        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=summary,
            metrics={
                "closest_projects_count": len(top_matches),
                "closest_project_similarity": round(top_similarity, 4),
                "closest_project_distance": round(1.0 - top_similarity, 4),
                "similarity_by_text": round(best_text_similarity, 4),
                "similarity_by_structure": round(best_structure_similarity, 4),
                "similarity_by_contracts": round(best_contract_similarity, 4),
                "originality_score": originality_score,
            },
            data={
                "closest_projects": top_matches,
                "clone_risk": clone_risk,
                "distinctive_features": distinctive,
                "copy_disclosure_hits": copy_disclosure_hits,
                "self_declared_copy_excerpt": self_declared_copy_excerpt,
                "clone_source_project": top_matches[0] if clone_risk in {"moderate", "high"} and top_matches else {},
                "current_profile": {
                    "project_type": current_profile.get("project_type") or "unknown",
                    "language": current_profile.get("language") or "",
                    "text_fingerprint": current_profile.get("text_fingerprint") or "",
                    "tree_fingerprint": current_profile.get("tree_fingerprint") or "",
                    "recent_commit_history_fingerprint": history_fingerprint,
                },
            },
            evidence=[
                f"{item.get('name')}: similarity={item.get('overall_similarity')}"
                for item in top_matches
            ] + ([self_declared_copy_excerpt] if self_declared_copy_excerpt else []),
            flags=flags,
            confidence=0.85,
            needs_human_review=clone_risk == "high",
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return ProjectSimilarityBlock(manifest)
