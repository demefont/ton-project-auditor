from __future__ import annotations

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult
from identity_validator.sources import get_github_repo_bundle
from identity_validator.utils import TON_KEYWORDS, clip_text, compact_repo_meta, keyword_hits


class GitHubRepoBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        if not context.case.github_repo:
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary="GitHub repository is not provided",
                flags=["missing_github_repo"],
            )
        bundle = await get_github_repo_bundle(context.case, context.options)
        repo = bundle.get("repo") or {}
        readme = str(bundle.get("readme") or "")
        hits = keyword_hits(
            " ".join(
                [
                    str(repo.get("full_name") or ""),
                    str(repo.get("description") or ""),
                    readme,
                    " ".join(repo.get("topics") or []),
                ]
            ),
            TON_KEYWORDS,
        )
        metrics = {
            "stargazers_count": int(repo.get("stargazers_count") or 0),
            "forks_count": int(repo.get("forks_count") or 0),
            "open_issues_count": int(repo.get("open_issues_count") or 0),
            "readme_chars": len(readme),
            "ton_keyword_hits": len(hits),
            "archived": bool(repo.get("archived") or False),
        }
        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=f"Loaded repo {repo.get('full_name') or context.case.github_repo}",
            metrics=metrics,
            data={
                "repo": compact_repo_meta(repo),
                "readme_excerpt": clip_text(readme, 2500),
                "ton_keywords": hits,
            },
            evidence=[
                repo.get("full_name") or context.case.github_repo,
                repo.get("description") or "",
                f"topics={','.join(repo.get('topics') or [])}",
            ],
            confidence=1.0,
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return GitHubRepoBlock(manifest)
