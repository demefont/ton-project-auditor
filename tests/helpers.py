from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import Iterable, Optional

from identity_validator.models import ProjectCase, RunOptions
from identity_validator.orchestrator import Orchestrator
from identity_validator.utils import ensure_dir

ROOT = Path(__file__).resolve().parents[1]
CASES_DIR = ROOT / "cases"


def load_case(case_id: str) -> ProjectCase:
    return ProjectCase.load(CASES_DIR / case_id / "case.json")


def run_pipeline(
    case_id: str,
    target_blocks: Optional[Iterable[str]] = None,
    mode: str = "recorded",
    llm_mode: str = "template",
    enable_sonar: bool = False,
):
    case = load_case(case_id)
    ensure_dir(ROOT / "artifacts")
    temp_dir = tempfile.mkdtemp(prefix=f"{case_id}_", dir=str(ROOT / "artifacts"))
    options = RunOptions(
        mode=mode,
        llm_mode=llm_mode,
        enable_sonar=enable_sonar,
        output_dir=temp_dir,
    )
    orchestrator = Orchestrator()
    return asyncio.run(orchestrator.run_case(case, options, target_blocks=target_blocks))
