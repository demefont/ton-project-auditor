from __future__ import annotations

import json

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult


class SonarResearchBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        if not context.options.enable_sonar:
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary="Sonar research is disabled",
            )
        repo_result = context.get_result("github_repo")
        project_type_result = context.get_result("project_type")
        repo_meta = ((repo_result.data or {}).get("repo") or {}) if repo_result else {}
        project_type = str((project_type_result.data or {}).get("project_type") or "unknown") if project_type_result else "unknown"
        prompt = (
            "Analyze this public TON-related project using only public information.\n"
            "Focus on whether it looks real, active and relevant to TON.\n"
            "Return 3 short points: external credibility, ecosystem relevance, risks.\n\n"
            f"Project name: {context.case.name}\n"
            f"GitHub repo: {context.case.github_repo}\n"
            f"Telegram handle: {context.case.telegram_handle}\n"
            f"Project type guess: {project_type}\n"
            f"Repository description: {repo_meta.get('description') or ''}\n"
        )
        metadata = {
            "project_name": context.case.name,
            "project_type": project_type,
        }
        text = await context.call_llm(
            block_id=self.block_id,
            model=context.options.sonar_model,
            prompt=prompt,
            metadata=metadata,
        )
        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary="Completed optional Sonar research",
            metrics={"text_chars": len(text)},
            data={"research_text": text},
            text=text,
            confidence=0.6,
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return SonarResearchBlock(manifest)
