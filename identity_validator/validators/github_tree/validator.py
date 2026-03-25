from __future__ import annotations

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult
from identity_validator.sources import get_github_tree
from identity_validator.utils import detect_contract_paths


class GitHubTreeBlock(BaseBlock):
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
        tree = await get_github_tree(context.case, default_branch, context.options)
        if str(tree.get("_source_status") or "") == "incomplete":
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary=str(tree.get("_source_summary") or "GitHub tree source is incomplete"),
                flags=["github_tree_source_incomplete"],
            )
        entries = tree.get("tree") or []
        paths = [str(item.get("path") or "") for item in entries if str(item.get("type") or "") == "blob"]
        contract_paths = detect_contract_paths(paths)
        ext_counts = {}
        root_dirs = []
        root_seen = set()
        path_ext_counts = {}
        for path in contract_paths:
            low = path.lower()
            ext = ""
            if "." in low:
                ext = "." + low.rsplit(".", 1)[-1]
            ext_counts[ext] = ext_counts.get(ext, 0) + 1
        for path in paths:
            low = path.lower()
            if "/" in low:
                root = low.split("/", 1)[0]
                if root and root not in root_seen:
                    root_seen.add(root)
                    root_dirs.append(root)
            ext = ""
            if "." in low:
                ext = "." + low.rsplit(".", 1)[-1]
            path_ext_counts[ext] = path_ext_counts.get(ext, 0) + 1
        metrics = {
            "total_files": len(paths),
            "contract_file_count": len(contract_paths),
            "contract_path_ratio": round(len(contract_paths) / len(paths), 4) if paths else 0.0,
            "root_dir_count": len(root_dirs),
        }
        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=f"Loaded repository tree with {len(paths)} files",
            metrics=metrics,
            data={
                "paths": paths,
                "paths_sample": paths[:150],
                "root_dirs": root_dirs,
                "path_ext_counts": path_ext_counts,
                "contract_paths_sample": contract_paths[:150],
                "contract_ext_counts": ext_counts,
            },
            evidence=contract_paths[:10],
            confidence=1.0,
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return GitHubTreeBlock(manifest)
