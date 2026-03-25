from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from .base import BaseBlock
from .registry import BlockRegistry
from .workflow import (
    AtomicUnit,
    CompositeUnit,
    Edge,
    Port,
    WorkflowPlan,
    attach_stages,
    validate_plan,
)


@dataclass(frozen=True)
class CompositeSpec:
    unit_id: str
    name: str
    description: str
    execution_mode: str
    children: List[str]


COMPOSITE_SPECS: List[CompositeSpec] = [
    CompositeSpec(
        unit_id="source_collection",
        name="Source Collection",
        description="Loads the public and recorded signals that seed the whole validation flow.",
        execution_mode="parallel",
        children=["github_repo", "telegram_channel", "project_registry"],
    ),
    CompositeSpec(
        unit_id="repo_analysis",
        name="Repository Analysis",
        description="Expands repository structure and freshness signals after the raw repo metadata is available.",
        execution_mode="parallel",
        children=["github_tree", "github_activity"],
    ),
    CompositeSpec(
        unit_id="community_analysis",
        name="Community Analysis",
        description="Evaluates public community feed quality and semantics.",
        execution_mode="sequence",
        children=["telegram_semantics"],
    ),
    CompositeSpec(
        unit_id="deep_validation",
        name="Deep Validation",
        description="Runs the heavier validators that need classified project context and richer artifacts.",
        execution_mode="parallel",
        children=["contract_validator", "project_similarity", "sonar_research"],
    ),
]

ROOT_UNIT_ORDER = [
    "source_collection",
    "address_signal",
    "identity_confirmation",
    "repo_analysis",
    "community_analysis",
    "project_type",
    "deep_validation",
    "claim_consistency",
    "risk_validator",
    "rule_engine",
    "llm_explainer",
]
ATOMIC_PRESENTATION_TAGS = {
    "telegram_semantics": ["hybrid", "ai", "llm", "verified"],
    "sonar_research": ["ai", "tool"],
    "llm_explainer": ["ai", "llm"],
}


def _safe_relative(path: str | Path) -> str:
    root = Path(__file__).resolve().parent.parent
    resolved = Path(path).resolve()
    try:
        return str(resolved.relative_to(root.resolve()))
    except ValueError:
        return str(resolved)


def _atomic_output_ports(block_id: str) -> List[Port]:
    return [
        Port(name="result", direction="output", description="Full block result envelope."),
        Port(name=block_id, direction="output", description="Named export of the full block result."),
        Port(name="status", direction="output", contract="str", description="Block result status."),
        Port(name="summary", direction="output", contract="str", description="Human-readable summary."),
        Port(name="metrics", direction="output", contract="dict", description="Metrics map."),
        Port(name="data", direction="output", contract="dict", description="Structured payload."),
        Port(name="flags", direction="output", contract="list", description="Raised flags."),
        Port(name="text", direction="output", contract="str", description="Narrative text output."),
    ]


def _atomic_input_ports(block: BaseBlock) -> List[Port]:
    return [
        Port(
            name=dependency,
            direction="input",
            description=f"Consumes the full result of `{dependency}`.",
        )
        for dependency in block.manifest.dependencies
    ]


def _atomic_tags(block_id: str, block: BaseBlock) -> List[str]:
    tags = [block.manifest.kind, "atomic"]
    extra = ATOMIC_PRESENTATION_TAGS.get(block_id)
    if extra:
        tags.extend(extra)
    else:
        tags.append("deterministic")
    return tags


def _build_atomic_units(blocks: Dict[str, BaseBlock], validators_root: str | Path) -> Dict[str, AtomicUnit]:
    validators_root = Path(validators_root)
    units: Dict[str, AtomicUnit] = {}
    for block_id, block in blocks.items():
        units[block_id] = AtomicUnit(
            unit_id=block_id,
            name=block.manifest.name,
            description=block.manifest.description,
            kind=block.manifest.kind,
            unit_type="atomic",
            execution_mode="atomic",
            input_ports=_atomic_input_ports(block),
            output_ports=_atomic_output_ports(block_id),
            tags=_atomic_tags(block_id, block),
            source_path=_safe_relative(validators_root / block_id / "validator.py"),
            manifest_path=_safe_relative(validators_root / block_id / "manifest.json"),
            block_id=block_id,
            manifest_dependencies=list(block.manifest.dependencies),
        )
    return units


def _external_edges_for_owner(
    owner_id: str,
    owner_map: Dict[str, str],
    blocks: Dict[str, BaseBlock],
) -> tuple[List[Edge], Dict[str, Port], Dict[str, Port]]:
    incoming: Dict[str, Port] = {}
    outgoing: Dict[str, Port] = {}
    edges: List[Edge] = []
    seen = set()
    for block_id, block in blocks.items():
        block_owner = owner_map.get(block_id, block_id)
        for dependency in block.manifest.dependencies:
            dependency_owner = owner_map.get(dependency, dependency)
            if dependency_owner == block_owner:
                continue
            source_port = dependency
            target_port = dependency if block_owner == block_id else f"{block_id}.{dependency}"
            edge_key = (dependency_owner, source_port, block_owner, target_port)
            if edge_key in seen:
                continue
            seen.add(edge_key)
            if dependency_owner == owner_id:
                outgoing.setdefault(
                    source_port,
                    Port(
                        name=source_port,
                        direction="output",
                        description=f"Exports `{dependency}` result from the grouped workflow.",
                    ),
                )
            if block_owner == owner_id:
                incoming.setdefault(
                    target_port,
                    Port(
                        name=target_port,
                        direction="input",
                        description=f"Feeds `{block_id}` from `{dependency_owner}`.",
                    ),
                )
            if dependency_owner == owner_id or block_owner == owner_id:
                edges.append(
                    Edge(
                        source_unit_id=dependency_owner,
                        source_port=source_port,
                        target_unit_id=block_owner,
                        target_port=target_port,
                        description=f"{dependency} -> {block_id}",
                    )
                )
    return edges, incoming, outgoing


def _build_composite_unit(
    spec: CompositeSpec,
    atomic_units: Dict[str, AtomicUnit],
    blocks: Dict[str, BaseBlock],
    owner_map: Dict[str, str],
) -> CompositeUnit:
    child_units = [atomic_units[child_id] for child_id in spec.children]
    child_edges = []
    for child_id in spec.children:
        for dependency in blocks[child_id].manifest.dependencies:
            if owner_map.get(dependency, dependency) == spec.unit_id:
                child_edges.append(
                    Edge(
                        source_unit_id=dependency,
                        source_port=dependency,
                        target_unit_id=child_id,
                        target_port=dependency,
                        description=f"{dependency} -> {child_id}",
                    )
                )
    child_plan = attach_stages(
        WorkflowPlan(
            plan_id=f"{spec.unit_id}.plan",
            name=spec.name,
            description=spec.description,
            units=child_units,
            edges=child_edges,
            metadata={"scope": "composite"},
        )
    )
    _, incoming, outgoing = _external_edges_for_owner(spec.unit_id, owner_map, blocks)
    child_tags = {tag for child in child_units for tag in child.tags}
    composite_tags = ["group", spec.execution_mode, "composite"]
    if "hybrid" in child_tags or ("ai" in child_tags and "deterministic" in child_tags):
        composite_tags.append("hybrid")
    elif "ai" in child_tags and "deterministic" not in child_tags:
        composite_tags.append("ai")
    else:
        composite_tags.append("deterministic")
    for tag in ("llm", "tool", "verified"):
        if tag in child_tags:
            composite_tags.append(tag)
    return CompositeUnit(
        unit_id=spec.unit_id,
        name=spec.name,
        description=spec.description,
        kind="group",
        unit_type="composite",
        execution_mode=spec.execution_mode,
        input_ports=sorted(incoming.values(), key=lambda item: item.name),
        output_ports=sorted(outgoing.values(), key=lambda item: item.name),
        tags=composite_tags,
        plan=child_plan,
    )


def build_workflow_plan(
    registry: BlockRegistry | None = None,
    blocks: Dict[str, BaseBlock] | None = None,
) -> WorkflowPlan:
    block_registry = registry or BlockRegistry()
    blocks = blocks or block_registry.load_blocks()
    validators_root = Path(block_registry.root_dir)
    atomic_units = _build_atomic_units(blocks, validators_root)
    owner_map: Dict[str, str] = {}
    grouped_children = set()
    specs_by_id = {spec.unit_id: spec for spec in COMPOSITE_SPECS}
    for spec in COMPOSITE_SPECS:
        for child_id in spec.children:
            owner_map[child_id] = spec.unit_id
            grouped_children.add(child_id)
    for block_id in blocks:
        owner_map.setdefault(block_id, block_id)

    root_units: Dict[str, AtomicUnit | CompositeUnit] = {}
    for spec in COMPOSITE_SPECS:
        root_units[spec.unit_id] = _build_composite_unit(spec, atomic_units, blocks, owner_map)
    for block_id, unit in atomic_units.items():
        if block_id not in grouped_children:
            root_units[block_id] = unit

    edge_map: Dict[tuple[str, str, str, str], Edge] = {}
    for block_id, block in blocks.items():
        block_owner = owner_map.get(block_id, block_id)
        for dependency in block.manifest.dependencies:
            dependency_owner = owner_map.get(dependency, dependency)
            if dependency_owner == block_owner:
                continue
            source_port = dependency
            target_port = dependency if block_owner == block_id else f"{block_id}.{dependency}"
            edge_key = (dependency_owner, source_port, block_owner, target_port)
            edge_map[edge_key] = Edge(
                source_unit_id=dependency_owner,
                source_port=source_port,
                target_unit_id=block_owner,
                target_port=target_port,
                description=f"{dependency} -> {block_id}",
            )

    ordered_root_units = [
        root_units[unit_id]
        for unit_id in ROOT_UNIT_ORDER
        if unit_id in root_units
    ]
    for unit_id in sorted(root_units):
        if unit_id not in ROOT_UNIT_ORDER:
            ordered_root_units.append(root_units[unit_id])

    plan = attach_stages(
        WorkflowPlan(
            plan_id="project_validation.workflow",
            name="Project Validation Workflow",
            description="Hierarchical validation workflow with grouped phases, explicit ports and computed execution stages.",
            units=ordered_root_units,
            edges=[edge_map[key] for key in sorted(edge_map)],
            metadata={"scope": "root"},
        )
    )
    validate_plan(plan)
    return plan
