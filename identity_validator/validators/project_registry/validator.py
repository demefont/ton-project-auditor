from __future__ import annotations

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult
from identity_validator.project_registry import load_registry_profiles


class ProjectRegistryBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        profiles = load_registry_profiles(exclude_case_id=context.case.case_id)
        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=f"Loaded curated registry with {len(profiles)} projects",
            metrics={
                "registry_project_count": len(profiles),
            },
            data={
                "profiles": profiles,
                "project_ids": [str(item.get("case_id") or "") for item in profiles],
            },
            evidence=[str(item.get("github_repo") or "") for item in profiles[:5]],
            confidence=1.0,
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return ProjectRegistryBlock(manifest)
