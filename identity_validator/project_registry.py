from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .models import ProjectCase
from .utils import detect_contract_paths, keyword_hits, normalize_ws, read_json, read_text, sha256_text

REGISTRY_STOPWORDS = {
    "about",
    "after",
    "agent",
    "app",
    "blockchain",
    "build",
    "building",
    "built",
    "code",
    "contains",
    "contract",
    "contracts",
    "copy",
    "created",
    "data",
    "development",
    "ecosystem",
    "exact",
    "file",
    "files",
    "for",
    "from",
    "game",
    "github",
    "hackathon",
    "important",
    "includes",
    "including",
    "main",
    "network",
    "official",
    "parts",
    "platform",
    "production",
    "project",
    "repo",
    "repository",
    "smart",
    "system",
    "telegram",
    "the",
    "this",
    "ton",
    "used",
    "using",
    "web",
}
GENERIC_PATH_TOKENS = {
    "app",
    "assets",
    "bin",
    "build",
    "cmd",
    "config",
    "configs",
    "contracts",
    "dist",
    "doc",
    "docs",
    "example",
    "examples",
    "frontend",
    "lib",
    "package",
    "packages",
    "public",
    "scripts",
    "src",
    "test",
    "tests",
    "tools",
    "ui",
}
COPY_DISCLOSURE_KEYWORDS = (
    "exact copy",
    "copied from",
    "fork of",
    "mirror of",
    "production copy",
    "without github history",
    "missing github history",
)
TON_PROJECT_KEYWORDS = (
    "wallet",
    "nft",
    "jetton",
    "dex",
    "swap",
    "validator",
    "sdk",
    "tooling",
    "telegram",
    "gamefi",
    "marketplace",
    "defi",
    "staking",
    "bridge",
    "governance",
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _cases_root(cases_root: str | Path | None = None) -> Path:
    return Path(cases_root) if cases_root else _repo_root() / "cases"


def _tokenize_text(text: str) -> List[str]:
    tokens = []
    seen = set()
    for token in re.findall(r"[a-z0-9]{3,}", str(text or "").lower()):
        if token in REGISTRY_STOPWORDS:
            continue
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def _path_features(paths: Iterable[str]) -> Dict[str, Any]:
    path_tokens: List[str] = []
    root_dirs: List[str] = []
    extensions: Dict[str, int] = {}
    path_seen = set()
    root_seen = set()
    contract_paths = detect_contract_paths(paths)
    contract_tokens: List[str] = []
    contract_seen = set()
    for raw_path in paths:
        path = str(raw_path or "").strip()
        if not path:
            continue
        low = path.lower()
        parts = [part for part in low.split("/") if part]
        if len(parts) > 1 and parts[0] not in root_seen:
            root_seen.add(parts[0])
            root_dirs.append(parts[0])
        if "." in parts[-1]:
            ext = "." + parts[-1].rsplit(".", 1)[-1]
            extensions[ext] = extensions.get(ext, 0) + 1
        for token in re.findall(r"[a-z0-9]{3,}", low):
            if token in GENERIC_PATH_TOKENS or token in REGISTRY_STOPWORDS:
                continue
            if token not in path_seen:
                path_seen.add(token)
                path_tokens.append(token)
    for raw_path in contract_paths:
        for token in re.findall(r"[a-z0-9]{3,}", str(raw_path or "").lower()):
            if token in REGISTRY_STOPWORDS:
                continue
            if token not in contract_seen:
                contract_seen.add(token)
                contract_tokens.append(token)
    return {
        "path_tokens": path_tokens,
        "root_dirs": root_dirs,
        "file_extensions": extensions,
        "contract_paths": contract_paths,
        "contract_tokens": contract_tokens,
    }


def _commit_history_fingerprint(activity_payload: Dict[str, Any]) -> str:
    commits = [item for item in (activity_payload.get("commits") or []) if isinstance(item, dict)]
    subjects = []
    for commit in commits[:12]:
        subject = normalize_ws(str(commit.get("message") or "").splitlines()[0]).lower()
        if subject:
            subjects.append(subject)
    return sha256_text("\n".join(subjects))[:16] if subjects else ""


def build_project_profile(
    *,
    case_id: str,
    name: str,
    github_repo: str,
    description: str,
    repo_meta: Dict[str, Any],
    readme: str,
    paths: Iterable[str],
    project_type: str,
    recent_commit_history_fingerprint: str = "",
) -> Dict[str, Any]:
    path_features = _path_features(paths)
    topics = [str(item) for item in (repo_meta.get("topics") or []) if item]
    full_text = normalize_ws(
        " ".join(
            [
                name,
                github_repo,
                description,
                str(repo_meta.get("description") or ""),
                " ".join(topics),
                readme,
            ]
        )
    )
    text_tokens = _tokenize_text(full_text)
    distinctive_keywords = [token for token in text_tokens if token in TON_PROJECT_KEYWORDS]
    copy_hits = keyword_hits(full_text.lower(), COPY_DISCLOSURE_KEYWORDS)
    text_fingerprint = sha256_text(" ".join(sorted(text_tokens)))[:16]
    tree_fingerprint = sha256_text(" ".join(sorted(path_features["path_tokens"])))[:16]
    return {
        "case_id": case_id,
        "name": name,
        "github_repo": github_repo,
        "description": description,
        "project_type": project_type or "unknown",
        "language": str(repo_meta.get("language") or "").lower(),
        "topics": topics,
        "text_tokens": text_tokens,
        "path_tokens": path_features["path_tokens"],
        "root_dirs": path_features["root_dirs"],
        "file_extensions": path_features["file_extensions"],
        "contract_tokens": path_features["contract_tokens"],
        "contract_file_count": len(path_features["contract_paths"]),
        "ton_keywords": distinctive_keywords,
        "copy_disclosure_hits": copy_hits,
        "recent_commit_history_fingerprint": str(recent_commit_history_fingerprint or ""),
        "text_fingerprint": text_fingerprint,
        "tree_fingerprint": tree_fingerprint,
        "readme_excerpt": normalize_ws(readme)[:600],
        "total_files": len([path for path in paths if str(path or "").strip()]),
    }


def load_registry_profiles(
    *,
    cases_root: str | Path | None = None,
    exclude_case_id: str = "",
) -> List[Dict[str, Any]]:
    profiles: List[Dict[str, Any]] = []
    base = _cases_root(cases_root)
    for case_json in sorted(base.glob("*/case.json")):
        case = ProjectCase.load(case_json)
        if case.case_id == exclude_case_id:
            continue
        snapshots_dir = case.case_dir / "snapshots"
        repo_path = snapshots_dir / "github_repo.json"
        tree_path = snapshots_dir / "github_tree.json"
        readme_path = snapshots_dir / "github_readme.txt"
        if not (repo_path.is_file() and tree_path.is_file() and readme_path.is_file()):
            continue
        repo_meta = read_json(repo_path)
        tree = read_json(tree_path)
        readme = read_text(readme_path)
        activity_path = snapshots_dir / "github_activity.json"
        activity_fingerprint = ""
        if activity_path.is_file():
            activity_payload = read_json(activity_path)
            activity_fingerprint = str(
                activity_payload.get("recent_commit_history_fingerprint")
                or activity_payload.get("history_fingerprint")
                or _commit_history_fingerprint(activity_payload)
                or ""
            )
        expected_types = case.expected.get("project_types_any_of") or []
        project_type = str(expected_types[0] if expected_types else "unknown")
        paths = [str(item.get("path") or "") for item in (tree.get("tree") or []) if str(item.get("type") or "") == "blob"]
        profiles.append(
            build_project_profile(
                case_id=case.case_id,
                name=case.name,
                github_repo=case.github_repo,
                description=case.description,
                repo_meta=repo_meta,
                readme=readme,
                paths=paths,
                project_type=project_type,
                recent_commit_history_fingerprint=activity_fingerprint,
            )
        )
    return profiles


def jaccard_similarity(left: Iterable[str], right: Iterable[str]) -> float:
    left_set = {str(item) for item in left if str(item)}
    right_set = {str(item) for item in right if str(item)}
    if not left_set and not right_set:
        return 0.0
    return round(len(left_set & right_set) / len(left_set | right_set), 4)


def weighted_dict_similarity(left: Dict[str, int], right: Dict[str, int]) -> float:
    left_keys = set(left.keys())
    right_keys = set(right.keys())
    all_keys = left_keys | right_keys
    if not all_keys:
        return 0.0
    intersect = sum(min(int(left.get(key) or 0), int(right.get(key) or 0)) for key in all_keys)
    union = sum(max(int(left.get(key) or 0), int(right.get(key) or 0)) for key in all_keys)
    if union == 0:
        return 0.0
    return round(intersect / union, 4)


def compare_profiles(current: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    text_similarity = jaccard_similarity(current.get("text_tokens") or [], candidate.get("text_tokens") or [])
    path_similarity = jaccard_similarity(current.get("path_tokens") or [], candidate.get("path_tokens") or [])
    root_similarity = jaccard_similarity(current.get("root_dirs") or [], candidate.get("root_dirs") or [])
    extension_similarity = weighted_dict_similarity(
        dict(current.get("file_extensions") or {}),
        dict(candidate.get("file_extensions") or {}),
    )
    structure_similarity = round((path_similarity + root_similarity + extension_similarity) / 3, 4)
    contract_relevant = bool(
        current.get("contract_tokens")
        or candidate.get("contract_tokens")
        or current.get("contract_file_count")
        or candidate.get("contract_file_count")
    )
    contract_similarity = (
        jaccard_similarity(current.get("contract_tokens") or [], candidate.get("contract_tokens") or [])
        if contract_relevant
        else 0.0
    )
    same_type = str(current.get("project_type") or "") == str(candidate.get("project_type") or "") != "unknown"
    history_fingerprint_match = bool(
        current.get("recent_commit_history_fingerprint")
        and current.get("recent_commit_history_fingerprint") == candidate.get("recent_commit_history_fingerprint")
    )
    weight_sum = 0.55 + 0.35
    weighted_value = (text_similarity * 0.55) + (structure_similarity * 0.35)
    if contract_relevant:
        weight_sum += 0.10
        weighted_value += contract_similarity * 0.10
    overall_similarity = weighted_value / weight_sum
    if same_type:
        overall_similarity += 0.05
    if history_fingerprint_match:
        overall_similarity += 0.08
    overall_similarity = round(min(1.0, overall_similarity), 4)
    return {
        "case_id": candidate.get("case_id") or "",
        "name": candidate.get("name") or "",
        "github_repo": candidate.get("github_repo") or "",
        "project_type": candidate.get("project_type") or "unknown",
        "overall_similarity": overall_similarity,
        "text_similarity": text_similarity,
        "structure_similarity": structure_similarity,
        "contract_similarity": contract_similarity,
        "same_type": same_type,
        "history_fingerprint_match": history_fingerprint_match,
        "copy_disclosure_hits": candidate.get("copy_disclosure_hits") or [],
    }


def distinctive_features(current: Dict[str, Any], comparisons: List[Dict[str, Any]], registry_profiles: List[Dict[str, Any]], limit: int = 8) -> List[str]:
    candidate_ids = {str(item.get("case_id") or "") for item in comparisons[:3]}
    other_tokens = set()
    for profile in registry_profiles:
        if str(profile.get("case_id") or "") not in candidate_ids:
            continue
        other_tokens.update(str(token) for token in (profile.get("text_tokens") or []))
        other_tokens.update(str(token) for token in (profile.get("path_tokens") or []))
        other_tokens.update(str(token) for token in (profile.get("contract_tokens") or []))
    features: List[str] = []
    for token in (
        list(current.get("ton_keywords") or [])
        + list(current.get("text_tokens") or [])
        + list(current.get("path_tokens") or [])
        + list(current.get("contract_tokens") or [])
    ):
        normalized = str(token or "")
        if not normalized or normalized in REGISTRY_STOPWORDS or normalized in other_tokens:
            continue
        if normalized not in features:
            features.append(normalized)
        if len(features) >= limit:
            break
    return features
