from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict

from .models import BlockResult
from .utils import ensure_dir


class TraceStore:
    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = ensure_dir(root_dir)
        self.blocks_dir = ensure_dir(self.root_dir / "blocks")
        self.llm_dir = ensure_dir(self.root_dir / "llm")

    def save_block_trace(
        self,
        block_id: str,
        payload_in: Dict[str, Any],
        result: BlockResult,
        duration_ms: int,
    ) -> None:
        payload = {
            "block_id": block_id,
            "duration_ms": duration_ms,
            "saved_at": int(time.time()),
            "input": payload_in,
            "output": result.to_dict(),
        }
        path = self.blocks_dir / f"{block_id}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")

    def save_llm_trace(
        self,
        block_id: str,
        model: str,
        prompt: str,
        response: str,
        metadata: Dict[str, Any],
    ) -> None:
        payload = {
            "block_id": block_id,
            "model": model,
            "saved_at": int(time.time()),
            "prompt": prompt,
            "metadata": metadata,
            "response": response,
        }
        path = self.llm_dir / f"{block_id}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")

    def save_run_summary(self, payload: Dict[str, Any]) -> None:
        path = self.root_dir / "run_summary.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")
