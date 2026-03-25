from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Iterable, List

from .models import ProjectCase, RunOptions
from .orchestrator import Orchestrator
from .viewer import serve_viewer


def _case_from_arg(case_arg: str) -> ProjectCase:
    path = Path(case_arg)
    if path.is_dir():
        return ProjectCase.load(path / "case.json")
    return ProjectCase.load(path)


def _all_case_paths(root: str | Path) -> List[Path]:
    base = Path(root)
    return sorted(path for path in base.glob("*/case.json"))


def _make_options(args: argparse.Namespace) -> RunOptions:
    return RunOptions(
        mode=args.mode,
        llm_mode=args.llm_mode,
        llm_model=args.llm_model,
        sonar_model=args.sonar_model,
        enable_sonar=bool(args.enable_sonar),
        record_snapshots=bool(args.record_snapshots),
        output_dir=args.output_dir or "",
    )


def _event_handler(event_type: str, block_id: str, result) -> None:
    if event_type == "start":
        print(f"start block={block_id}")
        return
    if result is None:
        return
    print(f"finish block={block_id} status={result.status} summary={result.summary}")


async def _run_case(case: ProjectCase, args: argparse.Namespace, target_blocks: Iterable[str] | None = None) -> None:
    orchestrator = Orchestrator()
    context = await orchestrator.run_case(
        case=case,
        options=_make_options(args),
        target_blocks=list(target_blocks) if target_blocks else None,
        event_handler=_event_handler,
    )
    rule_engine = context.results.get("rule_engine")
    if rule_engine:
        print(rule_engine.summary)
    explainer = context.results.get("llm_explainer")
    if explainer and explainer.text:
        print(explainer.text)
    print(f"artifacts={context.trace_store.root_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Identity project validator pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--mode", default="auto", choices=["auto", "recorded", "live"])
    common.add_argument("--llm-mode", default="template", choices=["template", "live"])
    common.add_argument("--llm-model", default="gpt-4o-mini")
    common.add_argument("--sonar-model", default="sonar")
    common.add_argument("--enable-sonar", action="store_true")
    common.add_argument("--record-snapshots", action="store_true")
    common.add_argument("--output-dir", default="")

    run_case = subparsers.add_parser("run-case", parents=[common])
    run_case.add_argument("case")

    run_block = subparsers.add_parser("run-block", parents=[common])
    run_block.add_argument("case")
    run_block.add_argument("block_id")

    run_suite = subparsers.add_parser("run-suite", parents=[common])
    run_suite.add_argument("--cases-root", default=str(Path(__file__).resolve().parent.parent / "cases"))

    record_case = subparsers.add_parser("record-case", parents=[common])
    record_case.add_argument("case")

    serve_viewer_cmd = subparsers.add_parser("serve-viewer")
    serve_viewer_cmd.add_argument("--host", default="127.0.0.1")
    serve_viewer_cmd.add_argument("--port", type=int, default=8008)
    serve_viewer_cmd.add_argument(
        "--artifacts-root",
        default=str(Path(__file__).resolve().parent.parent / "artifacts"),
    )

    args = parser.parse_args()
    if args.command == "run-case":
        asyncio.run(_run_case(_case_from_arg(args.case), args))
        return
    if args.command == "run-block":
        asyncio.run(_run_case(_case_from_arg(args.case), args, target_blocks=[args.block_id]))
        return
    if args.command == "record-case":
        args.mode = "live"
        args.record_snapshots = True
        asyncio.run(_run_case(_case_from_arg(args.case), args))
        return
    if args.command == "serve-viewer":
        serve_viewer(
            host=args.host,
            port=args.port,
            artifacts_root=args.artifacts_root,
        )
        return
    if args.command == "run-suite":
        for path in _all_case_paths(args.cases_root):
            print(f"run case={path.parent.name}")
            asyncio.run(_run_case(ProjectCase.load(path), args))


if __name__ == "__main__":
    main()
