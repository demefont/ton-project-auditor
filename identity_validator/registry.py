from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Dict

from .base import BaseBlock, BlockManifest


class BlockRegistry:
    def __init__(self, root_dir: str | Path | None = None) -> None:
        current = Path(__file__).resolve().parent
        self.root_dir = Path(root_dir) if root_dir else current / "validators"

    def load_blocks(self) -> Dict[str, BaseBlock]:
        blocks: Dict[str, BaseBlock] = {}
        for manifest_path in sorted(self.root_dir.glob("*/manifest.json")):
            payload = json.loads(manifest_path.read_text("utf-8"))
            manifest = BlockManifest.from_dict(payload)
            module_name = f"identity_validator.validators.{manifest.block_id}.validator"
            module = importlib.import_module(module_name)
            block = module.build_block(manifest)
            blocks[manifest.block_id] = block
        return blocks
