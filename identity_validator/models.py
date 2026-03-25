from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class ProjectCase:
    case_id: str
    name: str
    requested_input: str = ""
    github_repo: str = ""
    telegram_handle: str = ""
    project_url: str = ""
    wallet_address: str = ""
    type_hint: str = ""
    description: str = ""
    expected: Dict[str, Any] = field(default_factory=dict)
    root_dir: str = ""

    @property
    def case_dir(self) -> Path:
        return Path(self.root_dir)

    @property
    def snapshots_dir(self) -> Path:
        return self.case_dir / "snapshots"

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["root_dir"] = self.root_dir
        return data

    @classmethod
    def load(cls, case_json_path: str | Path) -> "ProjectCase":
        path = Path(case_json_path)
        raw = json.loads(path.read_text("utf-8"))
        if "case_id" not in raw:
            raw["case_id"] = path.parent.name
        raw["root_dir"] = str(path.parent)
        return cls(**raw)


@dataclass
class BlockResult:
    block_id: str
    status: str
    summary: str = ""
    metrics: Dict[str, Any] = field(default_factory=dict)
    data: Dict[str, Any] = field(default_factory=dict)
    evidence: List[Any] = field(default_factory=list)
    flags: List[str] = field(default_factory=list)
    text: str = ""
    confidence: Optional[float] = None
    needs_human_review: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RunOptions:
    mode: str = "auto"
    llm_mode: str = "template"
    llm_model: str = "gpt-4o-mini"
    sonar_model: str = "sonar"
    enable_sonar: bool = False
    record_snapshots: bool = False
    speed_profile: str = "full"
    output_dir: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
