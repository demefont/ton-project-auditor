from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Sequence


@dataclass
class Port:
    name: str
    direction: str
    contract: str = "BlockResult"
    description: str = ""
    required: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Edge:
    source_unit_id: str
    source_port: str
    target_unit_id: str
    target_port: str
    kind: str = "dependency"
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Stage:
    stage_id: str
    index: int
    unit_ids: List[str]
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Unit:
    unit_id: str
    name: str
    description: str
    kind: str
    unit_type: str
    execution_mode: str
    input_ports: List[Port] = field(default_factory=list)
    output_ports: List[Port] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    source_path: str = ""
    manifest_path: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "unit_id": self.unit_id,
            "name": self.name,
            "description": self.description,
            "kind": self.kind,
            "unit_type": self.unit_type,
            "execution_mode": self.execution_mode,
            "input_ports": [item.to_dict() for item in self.input_ports],
            "output_ports": [item.to_dict() for item in self.output_ports],
            "tags": list(self.tags),
            "source_path": self.source_path,
            "manifest_path": self.manifest_path,
        }


@dataclass
class AtomicUnit(Unit):
    block_id: str = ""
    manifest_dependencies: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        payload = super().to_dict()
        payload["block_id"] = self.block_id
        payload["manifest_dependencies"] = list(self.manifest_dependencies)
        return payload


@dataclass
class CompositeUnit(Unit):
    plan: "WorkflowPlan" = field(default_factory=lambda: WorkflowPlan(plan_id="", name="", description=""))

    def to_dict(self) -> Dict[str, Any]:
        payload = super().to_dict()
        payload["plan"] = self.plan.to_dict()
        return payload


@dataclass
class WorkflowPlan:
    plan_id: str
    name: str
    description: str
    units: List[Unit] = field(default_factory=list)
    edges: List[Edge] = field(default_factory=list)
    stages: List[Stage] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def unit_map(self) -> Dict[str, Unit]:
        return {unit.unit_id: unit for unit in self.units}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "name": self.name,
            "description": self.description,
            "units": [unit.to_dict() for unit in self.units],
            "edges": [edge.to_dict() for edge in self.edges],
            "stages": [stage.to_dict() for stage in self.stages],
            "metadata": dict(self.metadata),
        }


def compute_stages(plan: WorkflowPlan) -> List[Stage]:
    units = plan.unit_map()
    order = {unit.unit_id: index for index, unit in enumerate(plan.units)}
    indegree = {unit_id: 0 for unit_id in units}
    outgoing = {unit_id: set() for unit_id in units}
    for edge in plan.edges:
        if edge.source_unit_id not in units or edge.target_unit_id not in units:
            continue
        if edge.target_unit_id not in outgoing[edge.source_unit_id]:
            outgoing[edge.source_unit_id].add(edge.target_unit_id)
            indegree[edge.target_unit_id] += 1
    remaining = set(units.keys())
    ready = sorted([unit_id for unit_id, degree in indegree.items() if degree == 0], key=order.get)
    stages: List[Stage] = []
    stage_index = 0
    while ready:
        stages.append(
            Stage(
                stage_id=f"{plan.plan_id}:stage:{stage_index}",
                index=stage_index,
                unit_ids=list(ready),
                description=f"Execution wave {stage_index}",
            )
        )
        stage_index += 1
        next_ready: List[str] = []
        for unit_id in ready:
            if unit_id not in remaining:
                continue
            remaining.remove(unit_id)
            for target in sorted(outgoing[unit_id], key=order.get):
                indegree[target] -= 1
        for unit_id in sorted(remaining, key=order.get):
            if indegree[unit_id] == 0:
                next_ready.append(unit_id)
        ready = next_ready
    if remaining:
        raise ValueError(f"Workflow plan contains a cycle or unresolved edges: {sorted(remaining)}")
    return stages


def attach_stages(plan: WorkflowPlan) -> WorkflowPlan:
    for unit in plan.units:
        if isinstance(unit, CompositeUnit):
            attach_stages(unit.plan)
    plan.stages = compute_stages(plan)
    return plan


def validate_plan(plan: WorkflowPlan) -> None:
    units = plan.unit_map()
    if len(units) != len(plan.units):
        raise ValueError(f"Workflow plan {plan.plan_id} contains duplicate unit ids")
    for edge in plan.edges:
        if edge.source_unit_id not in units:
            raise ValueError(f"Unknown edge source: {edge.source_unit_id}")
        if edge.target_unit_id not in units:
            raise ValueError(f"Unknown edge target: {edge.target_unit_id}")
        if edge.source_unit_id == edge.target_unit_id:
            raise ValueError(f"Self edge is not allowed: {edge.source_unit_id}")
    compute_stages(plan)
    for unit in plan.units:
        if isinstance(unit, CompositeUnit):
            validate_plan(unit.plan)


def iter_atomic_units(plan: WorkflowPlan) -> Iterable[AtomicUnit]:
    for unit in plan.units:
        if isinstance(unit, AtomicUnit):
            yield unit
        elif isinstance(unit, CompositeUnit):
            yield from iter_atomic_units(unit.plan)


def atomic_unit_ids(plan: WorkflowPlan) -> List[str]:
    return [unit.unit_id for unit in iter_atomic_units(plan)]


def _unit_contains_required(unit: Unit, required_atomic_ids: set[str]) -> bool:
    if isinstance(unit, AtomicUnit):
        return unit.unit_id in required_atomic_ids
    if isinstance(unit, CompositeUnit):
        return any(_unit_contains_required(child, required_atomic_ids) for child in unit.plan.units)
    return False


def prune_plan(plan: WorkflowPlan, required_atomic_ids: Sequence[str]) -> WorkflowPlan:
    required = set(str(item) for item in required_atomic_ids)
    kept_units: List[Unit] = []
    for unit in plan.units:
        if isinstance(unit, AtomicUnit):
            if unit.unit_id in required:
                kept_units.append(unit)
            continue
        if isinstance(unit, CompositeUnit):
            child_plan = prune_plan(unit.plan, required)
            if child_plan.units:
                kept_units.append(
                    CompositeUnit(
                        unit_id=unit.unit_id,
                        name=unit.name,
                        description=unit.description,
                        kind=unit.kind,
                        unit_type=unit.unit_type,
                        execution_mode=unit.execution_mode,
                        input_ports=list(unit.input_ports),
                        output_ports=list(unit.output_ports),
                        tags=list(unit.tags),
                        source_path=unit.source_path,
                        manifest_path=unit.manifest_path,
                        plan=child_plan,
                    )
                )
    kept_ids = {unit.unit_id for unit in kept_units}
    kept_edges = [
        edge
        for edge in plan.edges
        if edge.source_unit_id in kept_ids and edge.target_unit_id in kept_ids
    ]
    pruned = WorkflowPlan(
        plan_id=plan.plan_id,
        name=plan.name,
        description=plan.description,
        units=kept_units,
        edges=kept_edges,
        metadata=dict(plan.metadata),
    )
    return attach_stages(pruned)


def summarize_status(statuses: Sequence[str]) -> str:
    ordered = [str(item or "pending") for item in statuses]
    if not ordered:
        return "pending"
    if any(item == "error" for item in ordered):
        return "error"
    if all(item == "skipped" for item in ordered):
        return "skipped"
    if any(item == "success" for item in ordered):
        if any(item in {"pending", "skipped"} for item in ordered):
            return "success"
        return "success"
    return ordered[0]
