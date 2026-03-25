from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence, Set

from .base import BaseBlock, ExecutionContext
from .llm import build_llm_client
from .models import BlockResult, ProjectCase, RunOptions
from .registry import BlockRegistry
from .tracing import TraceStore
from .utils import ensure_dir
from .workflow import AtomicUnit, CompositeUnit, WorkflowPlan, prune_plan, summarize_status
from .workflow_builder import build_workflow_plan


class Orchestrator:
    def __init__(self, registry: Optional[BlockRegistry] = None) -> None:
        self.registry = registry or BlockRegistry()

    def _expand_targets(self, blocks: Dict[str, BaseBlock], target_blocks: Optional[Iterable[str]]) -> Set[str]:
        if not target_blocks:
            return set(blocks.keys())
        needed: Set[str] = set()

        def visit(block_id: str) -> None:
            if block_id in needed:
                return
            block = blocks[block_id]
            needed.add(block_id)
            for dep in block.manifest.dependencies:
                visit(dep)

        for target in target_blocks:
            visit(str(target))
        return needed

    async def run_case(
        self,
        case: ProjectCase,
        options: RunOptions,
        target_blocks: Optional[Sequence[str]] = None,
        event_handler=None,
    ) -> ExecutionContext:
        blocks = self.registry.load_blocks()
        required = self._expand_targets(blocks, target_blocks)
        workflow_plan = build_workflow_plan(self.registry, blocks=blocks)
        if target_blocks:
            workflow_plan = prune_plan(workflow_plan, sorted(required))
        output_dir = options.output_dir or str(
            ensure_dir(case.case_dir.parent.parent / "artifacts" / "runs" / f"{case.case_id}_{int(time.time())}")
        )
        trace_store = TraceStore(output_dir)
        context = ExecutionContext(
            case=case,
            options=options,
            trace_store=trace_store,
            llm_client=build_llm_client(options.llm_mode),
            event_handler=event_handler,
        )
        unit_runtime: Dict[str, Dict[str, Any]] = {}
        stage_runtime: Dict[str, Dict[str, Any]] = {}
        await self._run_plan(workflow_plan, blocks, context, unit_runtime, stage_runtime)
        trace_store.save_run_summary(
            {
                "case": case.to_dict(),
                "options": options.to_dict(),
                "workflow": self._workflow_snapshot(workflow_plan, context, unit_runtime, stage_runtime),
                "results": {block_id: result.to_dict() for block_id, result in context.results.items()},
            }
        )
        return context

    async def _run_plan(
        self,
        plan: WorkflowPlan,
        blocks: Dict[str, BaseBlock],
        context: ExecutionContext,
        unit_runtime: Dict[str, Dict[str, Any]],
        stage_runtime: Dict[str, Dict[str, Any]],
    ) -> None:
        units = plan.unit_map()
        for stage in plan.stages:
            stage_started = time.time()
            await asyncio.gather(
                *[
                    self._run_unit(
                        units[unit_id],
                        blocks,
                        context,
                        unit_runtime,
                        stage_runtime,
                        stage.index,
                    )
                    for unit_id in stage.unit_ids
                ]
            )
            stage_runtime[stage.stage_id] = {
                "status": summarize_status([unit_runtime[unit_id]["status"] for unit_id in stage.unit_ids]),
                "duration_ms": int((time.time() - stage_started) * 1000),
                "unit_ids": list(stage.unit_ids),
            }

    async def _run_unit(
        self,
        unit: AtomicUnit | CompositeUnit,
        blocks: Dict[str, BaseBlock],
        context: ExecutionContext,
        unit_runtime: Dict[str, Dict[str, Any]],
        stage_runtime: Dict[str, Dict[str, Any]],
        stage_index: int,
    ) -> None:
        started_at = time.time()
        if isinstance(unit, CompositeUnit):
            await self._run_plan(unit.plan, blocks, context, unit_runtime, stage_runtime)
            child_statuses = [unit_runtime[child.unit_id]["status"] for child in unit.plan.units]
            status = summarize_status(child_statuses)
            unit_runtime[unit.unit_id] = {
                "status": status,
                "duration_ms": int((time.time() - started_at) * 1000),
                "stage_index": stage_index,
                "unit_type": unit.unit_type,
                "execution_mode": unit.execution_mode,
            }
            return
        block = blocks[unit.block_id]
        result = await self._run_block(block, context)
        context.results[unit.block_id] = result
        unit_runtime[unit.unit_id] = {
            "status": result.status,
            "duration_ms": int((time.time() - started_at) * 1000),
            "stage_index": stage_index,
            "unit_type": unit.unit_type,
            "execution_mode": unit.execution_mode,
        }

    def _workflow_snapshot(
        self,
        plan: WorkflowPlan,
        context: ExecutionContext,
        unit_runtime: Dict[str, Dict[str, Any]],
        stage_runtime: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        units_payload = []
        for unit in plan.units:
            payload = unit.to_dict()
            payload["runtime"] = unit_runtime.get(unit.unit_id) or {}
            if isinstance(unit, AtomicUnit):
                result = context.results.get(unit.block_id)
                payload["result"] = {
                    "status": result.status if result else "pending",
                    "summary": result.summary if result else "",
                    "flags": list(result.flags or []) if result else [],
                    "needs_human_review": bool(result.needs_human_review) if result else False,
                }
            else:
                payload["plan"] = self._workflow_snapshot(unit.plan, context, unit_runtime, stage_runtime)
            units_payload.append(payload)
        return {
            "plan_id": plan.plan_id,
            "name": plan.name,
            "description": plan.description,
            "metadata": dict(plan.metadata),
            "stages": [
                {
                    **stage.to_dict(),
                    "runtime": stage_runtime.get(stage.stage_id) or {},
                }
                for stage in plan.stages
            ],
            "edges": [edge.to_dict() for edge in plan.edges],
            "units": units_payload,
        }

    async def _run_block(self, block: BaseBlock, context: ExecutionContext) -> BlockResult:
        if context.event_handler:
            context.event_handler("start", block.block_id, None)
        started_at = time.time()
        payload_in = block.build_trace_input(context)
        try:
            result = await block.run(context)
        except Exception as exc:
            result = BlockResult(
                block_id=block.block_id,
                status="error",
                summary=str(exc),
                flags=["exception"],
                data={"exception": repr(exc)},
                needs_human_review=True,
            )
        duration_ms = int((time.time() - started_at) * 1000)
        context.trace_store.save_block_trace(block.block_id, payload_in, result, duration_ms)
        if context.event_handler:
            context.event_handler("finish", block.block_id, result)
        return result
