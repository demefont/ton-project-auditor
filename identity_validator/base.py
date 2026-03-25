from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from .models import BlockResult, ProjectCase, RunOptions

if TYPE_CHECKING:
    from .llm import BaseLLMClient
    from .tracing import TraceStore


@dataclass
class BlockManifest:
    block_id: str
    name: str
    kind: str
    dependencies: List[str] = field(default_factory=list)
    description: str = ""

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "BlockManifest":
        return cls(
            block_id=str(payload["id"]),
            name=str(payload.get("name") or payload["id"]),
            kind=str(payload.get("kind") or "validator"),
            dependencies=[str(item) for item in payload.get("dependencies") or []],
            description=str(payload.get("description") or ""),
        )


@dataclass
class ExecutionContext:
    case: ProjectCase
    options: RunOptions
    trace_store: "TraceStore"
    llm_client: "BaseLLMClient"
    results: Dict[str, BlockResult] = field(default_factory=dict)
    event_handler: Optional[Callable[[str, str, Optional[BlockResult]], None]] = None

    def get_result(self, block_id: str) -> Optional[BlockResult]:
        return self.results.get(block_id)

    def dependency_payload(self, block_id: str) -> Dict[str, Any]:
        result = self.results.get(block_id)
        return result.to_dict() if result else {}

    async def call_llm(
        self,
        block_id: str,
        model: str,
        prompt: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        return await self.llm_client.complete(
            block_id=block_id,
            model=model,
            prompt=prompt,
            trace_store=self.trace_store,
            metadata=metadata or {},
        )


class BaseBlock:
    def __init__(self, manifest: BlockManifest) -> None:
        self.manifest = manifest

    @property
    def block_id(self) -> str:
        return self.manifest.block_id

    def build_trace_input(self, context: ExecutionContext) -> Dict[str, Any]:
        deps = {
            dep: context.dependency_payload(dep)
            for dep in self.manifest.dependencies
        }
        return {
            "case": context.case.to_dict(),
            "dependencies": deps,
            "options": context.options.to_dict(),
        }

    async def run(self, context: ExecutionContext) -> BlockResult:
        raise NotImplementedError()
