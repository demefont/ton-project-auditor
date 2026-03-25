from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult
from identity_validator.sources import get_github_activity_bundle
from identity_validator.utils import days_since_iso, normalize_ws, sha256_text, stable_score


def _commit_author(commit: Dict[str, Any]) -> str:
    return str(commit.get("author_login") or commit.get("author_name") or "unknown")


def _commit_subject(commit: Dict[str, Any]) -> str:
    return normalize_ws(str(commit.get("message") or "").splitlines()[0])


class GitHubActivityBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        repo_result = context.get_result("github_repo")
        if not repo_result or repo_result.status != "success":
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary="GitHub repo metadata is unavailable",
                flags=["missing_repo_bundle"],
            )
        repo_meta = (repo_result.data or {}).get("repo") or {}
        default_branch = str(repo_meta.get("default_branch") or "")
        bundle = await get_github_activity_bundle(context.case, default_branch, context.options)
        if str(bundle.get("_source_status") or "") == "incomplete":
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary=str(bundle.get("_source_summary") or "GitHub activity source is incomplete"),
                flags=["github_activity_source_incomplete"],
            )
        commits = [item for item in (bundle.get("commits") or []) if isinstance(item, dict)]
        releases = [item for item in (bundle.get("releases") or []) if isinstance(item, dict)]
        now = datetime.now(timezone.utc)

        def commits_within(days: int) -> List[Dict[str, Any]]:
            selected: List[Dict[str, Any]] = []
            for commit in commits:
                age_days = days_since_iso(commit.get("date") or "", now=now)
                if age_days is not None and age_days <= days:
                    selected.append(commit)
            return selected

        def release_count_within(days: int) -> int:
            count = 0
            for release in releases:
                if release.get("draft"):
                    continue
                age_days = days_since_iso(release.get("published_at") or "", now=now)
                if age_days is not None and age_days <= days:
                    count += 1
            return count

        commits_30d = commits_within(30)
        commits_90d = commits_within(90)
        commits_365d = commits_within(365)
        active_days_30d = len({str(item.get("date") or "")[:10] for item in commits_30d if item.get("date")})
        active_days_90d = len({str(item.get("date") or "")[:10] for item in commits_90d if item.get("date")})
        authors_30d = {_commit_author(item) for item in commits_30d}
        authors_90d = {_commit_author(item) for item in commits_90d}
        authors_365d = {_commit_author(item) for item in commits_365d}
        author_commit_counts_90d = Counter(_commit_author(item) for item in commits_90d)
        top_author_share_90d = 0.0
        if commits_90d:
            top_author_share_90d = round(max(author_commit_counts_90d.values()) / len(commits_90d), 4)

        last_commit_age_days = days_since_iso((commits[0].get("date") if commits else "") or "", now=now)
        if last_commit_age_days is None:
            last_commit_age_days = days_since_iso(str(repo_meta.get("pushed_at") or ""), now=now)
        repo_age_days = days_since_iso(str(repo_meta.get("created_at") or ""), now=now) or 0
        recent_release_age_days = None
        if releases:
            recent_release_age_days = days_since_iso(str(releases[0].get("published_at") or ""), now=now)

        recent_subjects = [_commit_subject(item).lower() for item in commits[:12] if _commit_subject(item)]
        history_fingerprint = sha256_text("\n".join(recent_subjects))[:16] if recent_subjects else ""

        freshness_points = 0
        if last_commit_age_days is not None:
            if last_commit_age_days <= 30:
                freshness_points = 40
            elif last_commit_age_days <= 90:
                freshness_points = 28
            elif last_commit_age_days <= 180:
                freshness_points = 16
            elif last_commit_age_days <= 365:
                freshness_points = 8
        activity_score = stable_score(
            freshness_points
            + min(24, len(commits_90d) * 3)
            + min(14, active_days_90d * 1.5)
            + min(12, len(authors_90d) * 4)
            + min(10, release_count_within(180) * 5)
        )

        flags = []
        if last_commit_age_days is not None and last_commit_age_days >= 365:
            flags.append("repo_is_stale")
        elif last_commit_age_days is not None and last_commit_age_days >= 180:
            flags.append("repo_activity_is_old")
        if not commits_90d:
            flags.append("no_commits_90d")
        if commits_365d and len(authors_365d) <= 1:
            flags.append("single_author_recent_history")
        if top_author_share_90d >= 0.85 and len(commits_90d) >= 6:
            flags.append("commit_concentration_is_high")

        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=f"Loaded GitHub activity with {len(commits)} commits and {len(releases)} releases",
            metrics={
                "repo_age_days": repo_age_days,
                "last_commit_age_days": last_commit_age_days if last_commit_age_days is not None else -1,
                "commits_30d": len(commits_30d),
                "commits_90d": len(commits_90d),
                "commits_365d": len(commits_365d),
                "active_days_30d": active_days_30d,
                "active_days_90d": active_days_90d,
                "unique_authors_30d": len(authors_30d),
                "unique_authors_365d": len(authors_365d),
                "release_count_180d": release_count_within(180),
                "commit_concentration_90d": top_author_share_90d,
                "activity_score": activity_score,
            },
            data={
                "recent_commits": [
                    {
                        "sha": item.get("sha") or "",
                        "date": item.get("date") or "",
                        "author": _commit_author(item),
                        "subject": _commit_subject(item),
                    }
                    for item in commits[:20]
                ],
                "recent_releases": releases[:20],
                "recent_release_age_days": recent_release_age_days,
                "recent_commit_history_fingerprint": history_fingerprint,
                "commit_pages_loaded": int(bundle.get("commit_pages_loaded") or 0),
                "commit_page_limit_hit": bool(bundle.get("commit_page_limit_hit") or False),
            },
            evidence=[
                f"last_commit_age_days={last_commit_age_days}",
                f"commits_90d={len(commits_90d)}",
                f"unique_authors_365d={len(authors_365d)}",
            ],
            flags=flags,
            confidence=0.95,
            needs_human_review="repo_is_stale" in flags,
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return GitHubActivityBlock(manifest)
