from __future__ import annotations

import asyncio
import base64
import contextlib
import fcntl
import html as html_lib
import json
import os
import re
import select
import shlex
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import ProjectCase, RunOptions
from .utils import days_since_iso, ensure_dir, normalize_ws, parse_github_datetime, read_json, read_text, strip_html, write_json, write_text

GITHUB_HEADERS = {
    "Accept": "application/vnd.github+json",
    "User-Agent": "identity-validator/0.1",
}
TELEGRAM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
}
TGSTAT_HEADERS = {
    "User-Agent": TELEGRAM_HEADERS["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
GITHUB_HTML_HEADERS = dict(TGSTAT_HEADERS)
TGSTAT_MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
TGCHANNELS_HEADERS = {
    "User-Agent": TELEGRAM_HEADERS["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
}
TONCENTER_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "identity-validator/0.1",
}
TELEGRAM_FOOTER_MARKERS = (
    "TON Community",
    "TON Builders",
    "TON Hubs",
    "Gateway Tickets",
    "Tribute Tickets",
    "Moongate Tickets",
    "Hotel Booking",
    "YouTube",
    "LinkedIn",
    "TON.org",
)
TON_MCP_DEFAULT_COMMAND = ["npx", "-y", "-p", "node@20", "-p", "@ton/mcp@alpha", "mcp"]
TON_MCP_PROTOCOL_VERSION = "2025-06-18"
_TON_MCP_KNOWN_JETTONS_CACHE: Optional[Dict[str, Any]] = None
_TON_MCP_DNS_CACHE: Dict[str, Dict[str, Any]] = {}
_TON_MCP_REVERSE_DNS_CACHE: Dict[str, Dict[str, Any]] = {}
_TON_MCP_BALANCE_CACHE: Dict[str, Dict[str, Any]] = {}
_TON_ACCOUNT_ACTIVITY_CACHE: Dict[str, Dict[str, Any]] = {}
_GITHUB_GIT_BUNDLE_CACHE: Dict[str, Dict[str, Any]] = {}
_GITHUB_GIT_BUNDLE_LOCKS: Dict[str, threading.Lock] = {}


def _speed_profile(options: RunOptions) -> str:
    return "interactive" if normalize_ws(str(getattr(options, "speed_profile", "") or "")).lower() in {"interactive", "fast", "simple", "user"} else "full"


def _profile_timeout(options: RunOptions, interactive_timeout: int, full_timeout: int) -> int:
    return interactive_timeout if _speed_profile(options) == "interactive" else full_timeout


def _snapshot_path(case: ProjectCase, filename: str) -> Path:
    return case.snapshots_dir / filename


def _github_git_bundle_lock(github_repo: str) -> threading.Lock:
    key = normalize_ws(github_repo).lower()
    lock = _GITHUB_GIT_BUNDLE_LOCKS.get(key)
    if lock is None:
        lock = threading.Lock()
        _GITHUB_GIT_BUNDLE_LOCKS[key] = lock
    return lock


def _is_github_rate_limit_error(exc: Exception) -> bool:
    if isinstance(exc, urllib.error.HTTPError) and exc.code == 403:
        return True
    text = normalize_ws(str(exc)).lower()
    return "rate limit exceeded" in text or "api rate limit exceeded" in text


def _load_snapshot_json(case: ProjectCase, filename: str) -> Optional[Dict[str, Any]]:
    path = _snapshot_path(case, filename)
    if not path.is_file():
        return None
    return read_json(path)


def _load_snapshot_text(case: ProjectCase, filename: str) -> Optional[str]:
    path = _snapshot_path(case, filename)
    if not path.is_file():
        return None
    return read_text(path)


def _save_snapshot_json(case: ProjectCase, filename: str, payload: Dict[str, Any]) -> None:
    ensure_dir(case.snapshots_dir)
    write_json(_snapshot_path(case, filename), payload)


def _save_snapshot_text(case: ProjectCase, filename: str, payload: str) -> None:
    ensure_dir(case.snapshots_dir)
    write_text(_snapshot_path(case, filename), payload)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_tgstat_datetime(raw_value: str, reference_now: datetime) -> str:
    import re

    text = normalize_ws(raw_value)
    match = re.match(
        r"^(?P<day>\d{1,2})\s+(?P<month>[A-Za-z]{3})(?:\s+(?P<year>\d{4}))?,\s+(?P<hour>\d{1,2}):(?P<minute>\d{2})$",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return ""
    month = TGSTAT_MONTHS.get(str(match.group("month") or "").lower())
    if month is None:
        return ""
    year = int(match.group("year") or reference_now.year)
    parsed = datetime(
        year=year,
        month=month,
        day=int(match.group("day")),
        hour=int(match.group("hour")),
        minute=int(match.group("minute")),
        tzinfo=timezone.utc,
    )
    if match.group("year") is None and parsed > reference_now + timedelta(days=2):
        parsed = parsed.replace(year=parsed.year - 1)
    return _iso_utc(parsed)


def _parse_tgchannels_datetime(raw_value: str) -> str:
    text = normalize_ws(raw_value)
    if not text:
        return ""
    for pattern in ("%d %B %Y %H:%M", "%d %b %Y %H:%M"):
        try:
            parsed = datetime.strptime(text, pattern).replace(tzinfo=timezone.utc)
            return _iso_utc(parsed)
        except ValueError:
            continue
    return ""


def _normalize_telegram_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    handle = str(snapshot.get("handle") or "")
    source = str(snapshot.get("source") or "")
    fetched_at = str(snapshot.get("fetched_at") or "")
    entries: List[Dict[str, Any]] = []
    for item in snapshot.get("entries") or []:
        if not isinstance(item, dict):
            continue
        text = normalize_ws(str(item.get("text") or ""))
        if not text:
            continue
        entries.append(
            {
                "text": text,
                "date_text": normalize_ws(str(item.get("date_text") or "")),
                "published_at": str(item.get("published_at") or ""),
                "url": str(item.get("url") or ""),
            }
        )
    if not entries:
        for raw_text in snapshot.get("posts") or []:
            text = normalize_ws(str(raw_text or ""))
            if not text:
                continue
            entries.append(
                {
                    "text": text,
                    "date_text": "",
                    "published_at": "",
                    "url": "",
                }
            )
    posts = [str(item.get("text") or "") for item in entries]
    return {
        "handle": handle,
        "posts": posts,
        "post_count": len(posts),
        "source": source,
        "entries": entries,
        "fetched_at": fetched_at,
    }


def _clean_telegram_post_text(raw_text: str) -> str:
    text = normalize_ws(str(raw_text or ""))
    if not text:
        return ""
    replacements = {
        '">': " ",
        "ton_official_channel YouTube": "YouTube",
        "ton_official_channel \">YouTube": "YouTube",
        "Lin kedIn": "LinkedIn",
        "Li nkedIn": "LinkedIn",
        "T ON.org": "TON.org",
        "Ga teway": "Gateway",
        "T ON Foundation": "TON Foundation",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    footer_positions = [text.find(marker) for marker in TELEGRAM_FOOTER_MARKERS if text.find(marker) >= 0]
    if footer_positions:
        footer_start = min(footer_positions)
        tail = text[footer_start:]
        marker_count = sum(1 for marker in TELEGRAM_FOOTER_MARKERS if marker in tail)
        if marker_count >= 3 and "|" in tail:
            text = text[:footer_start].rstrip(" |")
    return normalize_ws(text)


def _dedupe_telegram_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique_entries: List[Dict[str, Any]] = []
    seen = set()
    for item in entries:
        if not isinstance(item, dict):
            continue
        text = _clean_telegram_post_text(str(item.get("text") or ""))
        if not text or text in seen:
            continue
        seen.add(text)
        unique_entries.append(
            {
                "text": text,
                "date_text": normalize_ws(str(item.get("date_text") or "")),
                "published_at": str(item.get("published_at") or ""),
                "url": str(item.get("url") or ""),
            }
        )
    return unique_entries[:25]


def _http_fetch_bytes(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> bytes:
    request = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


async def http_fetch_text(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> str:
    raw = await asyncio.to_thread(_http_fetch_bytes, url, headers, timeout)
    return raw.decode("utf-8", errors="replace")


async def http_fetch_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> Dict[str, Any]:
    text = await http_fetch_text(url, headers=headers, timeout=timeout)
    return json.loads(text)


def _github_repo_https_url(github_repo: str) -> str:
    return f"https://github.com/{github_repo.strip().removesuffix('.git')}.git"


def _extract_github_embedded_payload(page_html: str) -> Dict[str, Any]:
    match = re.search(
        r'<script type="application/json" data-target="react-app\.embeddedData">(.*?)</script>',
        str(page_html or ""),
        flags=re.DOTALL | re.IGNORECASE,
    )
    if not match:
        return {}
    raw = html_lib.unescape(match.group(1))
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return dict(payload.get("payload") or {})


def _extract_github_meta_content(page_html: str, property_name: str) -> str:
    patterns = [
        rf'<meta\s+property="{re.escape(property_name)}"\s+content="([^"]*)"',
        rf'<meta\s+content="([^"]*)"\s+property="{re.escape(property_name)}"',
        rf'<meta\s+name="{re.escape(property_name)}"\s+content="([^"]*)"',
        rf'<meta\s+content="([^"]*)"\s+name="{re.escape(property_name)}"',
    ]
    for pattern in patterns:
        match = re.search(pattern, str(page_html or ""), flags=re.IGNORECASE)
        if match:
            return normalize_ws(html_lib.unescape(match.group(1)))
    return ""


def _github_repo_brand_tokens(github_repo: str, description: str = "") -> List[str]:
    stopwords = {
        "app",
        "and",
        "blockchain",
        "build",
        "built",
        "contract",
        "contracts",
        "core",
        "dex",
        "fi",
        "for",
        "from",
        "git",
        "github",
        "io",
        "market",
        "marketplace",
        "nft",
        "official",
        "open",
        "protocol",
        "repo",
        "repository",
        "sdk",
        "smart",
        "source",
        "telegram",
        "the",
        "ton",
        "with",
        "wallet",
        "web",
    }
    def collect(text: str) -> List[str]:
        tokens: List[str] = []
        seen = set()
        for token in re.findall(r"[a-z0-9]{3,}", str(text or "").lower()):
            if len(token) < 4 or token in stopwords or token in seen:
                continue
            seen.add(token)
            tokens.append(token)
        return tokens

    repo_tokens = collect(github_repo)
    if repo_tokens:
        return repo_tokens
    return collect(description)


def _extract_external_candidates_from_text(text: str) -> List[str]:
    values: List[str] = []
    seen = set()
    raw = re.sub(
        r"\\u([0-9a-fA-F]{4})",
        lambda match: chr(int(match.group(1), 16)),
        str(text or ""),
    )
    for value in re.findall(r"https?://[^\s\"'<>]+", raw, flags=re.IGNORECASE):
        normalized = normalize_ws(html_lib.unescape(value)).rstrip("\\").rstrip(").,]")
        if normalized and normalized.lower() not in seen:
            seen.add(normalized.lower())
            values.append(normalized)
    for value in re.findall(r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}\b", raw, flags=re.IGNORECASE):
        normalized = normalize_ws(value).strip(".").lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        values.append(f"https://{normalized}")
    return values


def _select_canonical_project_url(urls: List[str], github_repo: str, description: str = "") -> str:
    excluded_domains = {
        "github.com",
        "raw.githubusercontent.com",
        "support.github.com",
        "docs.github.com",
        "pages.github.com",
        "opensource.org",
        "shields.io",
        "npmjs.com",
        "www.npmjs.com",
        "ton.org",
        "www.ton.org",
        "tonviewer.com",
        "www.tonviewer.com",
        "testnet.tonviewer.com",
    }
    file_like_suffixes = {
        "conf",
        "ini",
        "json",
        "jsx",
        "lock",
        "md",
        "py",
        "ts",
        "tsx",
        "toml",
        "txt",
        "yaml",
        "yml",
    }
    brand_tokens = _github_repo_brand_tokens(github_repo, description)
    best_url = ""
    best_score = 0.0
    for value in urls:
        candidate_value = normalize_ws(html_lib.unescape(str(value or ""))).rstrip("\\").rstrip(").,]")
        if not candidate_value:
            continue
        parsed = urllib.parse.urlparse(candidate_value if "://" in candidate_value else f"https://{candidate_value}")
        domain = (parsed.netloc or "").lower().removeprefix("www.")
        domain_suffix = domain.rsplit(".", 1)[-1] if "." in domain else ""
        if (
            not domain
            or domain in excluded_domains
            or domain.endswith(".git")
            or domain == "git"
            or "github" in domain
            or domain_suffix in file_like_suffixes
        ):
            continue
        compact_domain = re.sub(r"[^a-z0-9]+", "", domain)
        compact_url = re.sub(r"[^a-z0-9]+", "", candidate_value.lower())
        if brand_tokens and not any(token in compact_domain for token in brand_tokens):
            continue
        score = 0.2
        if compact_domain:
            score += 0.1
        if parsed.path:
            score -= min(0.15, max(0, len(parsed.path.strip("/").split("/")) - 1) * 0.05)
        for token in brand_tokens:
            if token in compact_domain:
                score += 0.55
            elif token in compact_url:
                score += 0.15
        if score > best_score:
            best_score = score
            best_url = f"https://{domain}{parsed.path or ''}".rstrip("/\\") if parsed.scheme.startswith("http") else candidate_value
    return best_url


def _extract_github_homepage_from_html(page_html: str) -> str:
    match = re.search(
        r'aria-label="Homepage".{0,800}?<a[^>]+href="([^"]+)"',
        str(page_html or ""),
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match:
        return normalize_ws(html_lib.unescape(match.group(1)))
    return ""


def _extract_github_readme_from_html(page_html: str) -> str:
    match = re.search(
        r"<article[^>]*markdown-body[^>]*>(.*?)</article>",
        str(page_html or ""),
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ""
    return normalize_ws(strip_html(match.group(1)))


def _parse_github_repo_html(github_repo: str, page_html: str) -> Dict[str, Any]:
    payload = _extract_github_embedded_payload(page_html)
    code_view = dict(payload.get("codeViewRepoRoute") or {})
    ref_info = dict(code_view.get("refInfo") or {})
    description = _extract_github_meta_content(page_html, "og:description")
    suffixes = [
        f" - {github_repo}",
        f". Contribute to {github_repo} development by creating an account on GitHub.",
        f" Contribute to {github_repo} development by creating an account on GitHub.",
    ]
    for suffix in suffixes:
        if description.endswith(suffix):
            description = normalize_ws(description[: -len(suffix)])
    readme_text = _extract_github_readme_from_html(page_html)
    homepage = _select_canonical_project_url(
        [_extract_github_homepage_from_html(page_html)],
        github_repo,
        description,
    )
    if not homepage:
        homepage = _select_canonical_project_url(
            _extract_external_candidates_from_text(f"{page_html}\n{readme_text}"),
            github_repo,
            description,
        )
    return {
        "full_name": github_repo,
        "description": description,
        "default_branch": str(ref_info.get("name") or "main"),
        "language": "",
        "topics": [],
        "stargazers_count": 0,
        "forks_count": 0,
        "open_issues_count": 0,
        "archived": False,
        "created_at": "",
        "updated_at": "",
        "pushed_at": "",
        "homepage": homepage,
        "html_url": f"https://github.com/{github_repo}",
        "_readme_text": readme_text,
    }


def _git_run(args: List[str], cwd: Optional[Path] = None, timeout: int = 180) -> str:
    completed = subprocess.run(
        args,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(normalize_ws(completed.stderr or completed.stdout or f"git command failed: {' '.join(args)}"))
    return completed.stdout


def _git_cache_path(github_repo: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "__", github_repo.strip())
    return Path(tempfile.gettempdir()) / "identity_validator_github_cache" / safe


def _git_cache_lock_path(github_repo: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "__", github_repo.strip())
    return Path(tempfile.gettempdir()) / "identity_validator_github_cache" / f"{safe}.lock"


@contextlib.contextmanager
def _github_git_bundle_file_lock(github_repo: str):
    lock_path = _git_cache_lock_path(github_repo)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _cleanup_git_lockfiles(repo_dir: Path) -> int:
    git_dir = repo_dir / ".git"
    if not git_dir.is_dir():
        return 0
    removed = 0
    for candidate in git_dir.rglob("*.lock"):
        if not candidate.is_file():
            continue
        try:
            candidate.unlink()
            removed += 1
        except FileNotFoundError:
            continue
    return removed


def _is_git_lock_error(exc: Exception) -> bool:
    text = normalize_ws(str(exc)).lower()
    return ".lock" in text and "file exists" in text and "git process" in text


def _git_run_repo_mutation(args: List[str], repo_dir: Path, timeout: int = 300) -> str:
    _cleanup_git_lockfiles(repo_dir)
    try:
        return _git_run(args, cwd=repo_dir, timeout=timeout)
    except RuntimeError as exc:
        if not _is_git_lock_error(exc):
            raise
        _cleanup_git_lockfiles(repo_dir)
        return _git_run(args, cwd=repo_dir, timeout=timeout)


def _git_history_depth_state(repo_dir: Path) -> Dict[str, Any]:
    oldest_date = normalize_ws(_git_run(["git", "log", "--reverse", "--format=%cI", "-n", "1"], cwd=repo_dir, timeout=120))
    commit_count_raw = normalize_ws(_git_run(["git", "rev-list", "--count", "HEAD"], cwd=repo_dir, timeout=120))
    is_shallow = normalize_ws(_git_run(["git", "rev-parse", "--is-shallow-repository"], cwd=repo_dir, timeout=120)).lower() == "true"
    return {
        "oldest_date": oldest_date,
        "commit_count": int(commit_count_raw or 0),
        "is_shallow": is_shallow,
    }


def _resolve_git_default_branch(github_repo: str) -> str:
    output = _git_run(["git", "ls-remote", "--symref", _github_repo_https_url(github_repo), "HEAD"], timeout=120)
    for line in output.splitlines():
        if line.startswith("ref: ") and line.endswith("\tHEAD"):
            ref = line.split()[1]
            return ref.rsplit("/", 1)[-1]
    return "main"


def _ensure_github_git_bundle_sync(github_repo: str, default_branch: str = "", history_days: int = 0) -> Dict[str, Any]:
    with _github_git_bundle_lock(github_repo):
        with _github_git_bundle_file_lock(github_repo):
            cache = dict(_GITHUB_GIT_BUNDLE_CACHE.get(github_repo) or {})
            repo_dir = Path(cache.get("path") or _git_cache_path(github_repo))
            branch = normalize_ws(default_branch) or str(cache.get("default_branch") or "")
            if not branch:
                branch = _resolve_git_default_branch(github_repo)
            if not (repo_dir / ".git").is_dir():
                repo_dir.parent.mkdir(parents=True, exist_ok=True)
                if repo_dir.exists():
                    shutil.rmtree(repo_dir, ignore_errors=True)
                _git_run(
                    [
                        "git",
                        "clone",
                        "--depth",
                        "1",
                        "--single-branch",
                        "--branch",
                        branch,
                        _github_repo_https_url(github_repo),
                        str(repo_dir),
                    ],
                    timeout=300,
                )
            if history_days > 0:
                previous_state: Optional[Dict[str, Any]] = None
                while True:
                    state = _git_history_depth_state(repo_dir)
                    oldest_date = str(state.get("oldest_date") or "")
                    commit_count = int(state.get("commit_count") or 0)
                    oldest_age_days = days_since_iso(oldest_date) if oldest_date else None
                    if oldest_age_days is not None and oldest_age_days >= history_days:
                        break
                    if commit_count >= 1600:
                        break
                    if not bool(state.get("is_shallow")):
                        break
                    if previous_state and commit_count <= int(previous_state.get("commit_count") or 0) and oldest_date == str(previous_state.get("oldest_date") or ""):
                        break
                    previous_state = state
                    _git_run_repo_mutation(["git", "fetch", "--deepen", "200", "origin", branch], repo_dir, timeout=300)
            payload = {"path": str(repo_dir), "default_branch": branch}
            _GITHUB_GIT_BUNDLE_CACHE[github_repo] = dict(payload)
            return payload


def _git_readme_text(repo_dir: Path) -> str:
    preferred = [
        "README.md",
        "README.rst",
        "README.txt",
        "README",
        "readme.md",
        "readme.rst",
        "readme.txt",
        "readme",
    ]
    for relative in preferred:
        candidate = repo_dir / relative
        if candidate.is_file():
            return candidate.read_text("utf-8", errors="replace")
    for candidate in repo_dir.glob("**/README*"):
        if candidate.is_file() and ".git/" not in str(candidate):
            return candidate.read_text("utf-8", errors="replace")
    return ""


def _git_tree_payload(repo_dir: Path) -> Dict[str, Any]:
    output = _git_run(["git", "ls-tree", "-r", "-z", "--name-only", "HEAD"], cwd=repo_dir, timeout=180)
    paths = [item for item in output.split("\x00") if item]
    return {
        "tree": [{"path": path, "type": "blob"} for path in paths],
    }


def _git_activity_payload(repo_dir: Path) -> Dict[str, Any]:
    commit_output = _git_run(
        [
            "git",
            "log",
            "--date=iso-strict",
            "--format=%H%x1f%cI%x1f%an%x1f%ae%x1f%s%x1e",
            "-n",
            "1600",
        ],
        cwd=repo_dir,
        timeout=240,
    )
    commits: List[Dict[str, Any]] = []
    for row in commit_output.split("\x1e"):
        normalized = row.strip()
        if not normalized:
            continue
        parts = normalized.split("\x1f")
        if len(parts) < 5:
            continue
        commits.append(
            {
                "sha": normalize_ws(parts[0]),
                "date": normalize_ws(parts[1]),
                "author_login": "",
                "author_name": normalize_ws(parts[2]) or normalize_ws(parts[3]),
                "message": normalize_ws(parts[4]),
                "parent_count": 0,
            }
        )
    tag_output = _git_run(
        [
            "git",
            "for-each-ref",
            "--sort=-creatordate",
            "--format=%(refname:short)%x1f%(creatordate:iso-strict)",
            "refs/tags",
        ],
        cwd=repo_dir,
        timeout=120,
    )
    releases: List[Dict[str, Any]] = []
    for row in tag_output.splitlines():
        parts = row.split("\x1f")
        if len(parts) != 2:
            continue
        releases.append(
            {
                "tag_name": normalize_ws(parts[0]),
                "name": normalize_ws(parts[0]),
                "draft": False,
                "prerelease": False,
                "published_at": normalize_ws(parts[1]),
            }
        )
    return {
        "commits": commits,
        "releases": releases,
        "commit_pages_loaded": 1 if commits else 0,
        "commit_page_limit_hit": False,
    }


async def fetch_github_repo_meta(github_repo: str, speed_profile: str = "full") -> Dict[str, Any]:
    repo = github_repo.strip().removesuffix(".git")
    api_meta: Dict[str, Any] = {}
    try:
        api_meta = await http_fetch_json(
            f"https://api.github.com/repos/{repo}",
            headers=GITHUB_HEADERS,
            timeout=10 if speed_profile == "interactive" else 30,
        )
    except Exception:
        api_meta = {}
    if api_meta:
        sanitized_homepage = _select_canonical_project_url(
            [str(api_meta.get("homepage") or "")],
            repo,
            str(api_meta.get("description") or ""),
        )
        if sanitized_homepage:
            api_meta["homepage"] = sanitized_homepage
        else:
            api_meta.pop("homepage", None)
    needs_html_enrichment = not api_meta or not normalize_ws(str(api_meta.get("homepage") or ""))
    if not needs_html_enrichment:
        return api_meta
    page_html = await http_fetch_text(
        f"https://github.com/{repo}",
        headers=GITHUB_HTML_HEADERS,
        timeout=8 if speed_profile == "interactive" else 45,
    )
    parsed = _parse_github_repo_html(repo, page_html)
    parsed.pop("_readme_text", None)
    if not api_meta:
        return parsed
    for key, value in parsed.items():
        if value not in (None, "", [], {}) and not api_meta.get(key):
            api_meta[key] = value
    return api_meta


def _ton_mcp_command() -> List[str]:
    raw = normalize_ws(str(os.getenv("TON_MCP_COMMAND") or ""))
    if raw:
        return shlex.split(raw)
    return list(TON_MCP_DEFAULT_COMMAND)


def _ton_mcp_env() -> Dict[str, str]:
    env = dict(os.environ)
    overrides = {
        "TON_MCP_MNEMONIC": "MNEMONIC",
        "TON_MCP_PRIVATE_KEY": "PRIVATE_KEY",
        "TON_MCP_CONFIG_PATH": "TON_CONFIG_PATH",
        "TON_MCP_NETWORK": "NETWORK",
    }
    for source_name, target_name in overrides.items():
        value = str(os.getenv(source_name) or "").strip()
        if value:
            env[target_name] = value
    return env


def ton_mcp_has_wallet_context() -> bool:
    return any(
        normalize_ws(str(os.getenv(name) or ""))
        for name in ("TON_MCP_MNEMONIC", "TON_MCP_PRIVATE_KEY", "TON_MCP_CONFIG_PATH")
    )


def _ton_mcp_send(proc: subprocess.Popen[str], payload: Dict[str, Any]) -> None:
    if proc.stdin is None:
        raise RuntimeError("TON MCP stdin is unavailable")
    proc.stdin.write(json.dumps(payload, ensure_ascii=False) + "\n")
    proc.stdin.flush()


def _ton_mcp_read_response(proc: subprocess.Popen[str], target_id: int, timeout: float = 45.0) -> Dict[str, Any]:
    deadline = time.time() + timeout
    last_stderr = ""
    while time.time() < deadline:
        streams = [stream for stream in (proc.stdout, proc.stderr) if stream is not None]
        ready, _, _ = select.select(streams, [], [], 0.5)
        if proc.stderr in ready and proc.stderr is not None:
            err_line = proc.stderr.readline()
            if err_line:
                last_stderr = err_line.rstrip()
        if proc.stdout in ready and proc.stdout is not None:
            line = proc.stdout.readline()
            if line:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("id") == target_id:
                    return payload
        if proc.poll() is not None:
            break
    raise TimeoutError(f"TON MCP did not answer request id={target_id}; last_stderr={last_stderr}")


def _ton_mcp_parse_tool_result(response: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(response.get("result") or {})
    content = [item for item in result.get("content") or [] if isinstance(item, dict)]
    text_parts = [str(item.get("text") or "").strip() for item in content if str(item.get("text") or "").strip()]
    text = "\n".join(text_parts).strip()
    data: Dict[str, Any] = {}
    if text.startswith("{") or text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                data = parsed
            else:
                data = {"items": parsed}
        except json.JSONDecodeError:
            data = {}
    return {
        "is_error": bool(result.get("isError") or False),
        "text": text,
        "data": data,
    }


def _ton_mcp_call_tool_sync(name: str, arguments: Optional[Dict[str, Any]] = None, request_timeout: float = 90.0) -> Dict[str, Any]:
    proc = subprocess.Popen(
        _ton_mcp_command(),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=_ton_mcp_env(),
    )
    try:
        _ton_mcp_send(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": TON_MCP_PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "clientInfo": {"name": "identity-validator", "version": "0.1"},
                },
            },
        )
        _ton_mcp_read_response(proc, 1, timeout=request_timeout)
        _ton_mcp_send(proc, {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})
        _ton_mcp_send(
            proc,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": name,
                    "arguments": arguments or {},
                },
            },
        )
        response = _ton_mcp_read_response(proc, 2, timeout=request_timeout)
        return _ton_mcp_parse_tool_result(response)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


async def get_ton_mcp_known_jettons(case: ProjectCase, options: RunOptions) -> Dict[str, Any]:
    del case
    if options.mode == "recorded":
        return {
            "status": "skipped",
            "summary": "TON MCP known jetton enrichment is disabled in recorded mode.",
            "jettons": [],
            "count": 0,
        }
    global _TON_MCP_KNOWN_JETTONS_CACHE
    if _TON_MCP_KNOWN_JETTONS_CACHE is not None:
        return dict(_TON_MCP_KNOWN_JETTONS_CACHE)
    request_timeout = 20.0 if _speed_profile(options) == "interactive" else 90.0
    try:
        payload = await asyncio.to_thread(_ton_mcp_call_tool_sync, "get_known_jettons", {}, request_timeout)
    except Exception as exc:
        return {
            "status": "error",
            "summary": f"TON MCP known jetton lookup failed: {exc}",
            "jettons": [],
            "count": 0,
        }
    if payload.get("is_error"):
        return {
            "status": "error",
            "summary": str(payload.get("text") or "TON MCP returned an error for get_known_jettons."),
            "jettons": [],
            "count": 0,
        }
    data = dict(payload.get("data") or {})
    jettons = [item for item in data.get("jettons") or [] if isinstance(item, dict)]
    result = {
        "status": "success",
        "summary": f"TON MCP returned {len(jettons)} known jettons.",
        "jettons": jettons,
        "count": len(jettons),
    }
    _TON_MCP_KNOWN_JETTONS_CACHE = dict(result)
    return result


async def get_ton_mcp_resolve_dns(case: ProjectCase, options: RunOptions, domain: str) -> Dict[str, Any]:
    del case
    normalized_domain = normalize_ws(domain).lower()
    if not normalized_domain:
        return {"status": "skipped", "summary": "TON DNS domain is unavailable.", "domain": "", "address": ""}
    if options.mode == "recorded":
        return {"status": "skipped", "summary": "TON MCP DNS resolution is disabled in recorded mode.", "domain": normalized_domain, "address": ""}
    if not ton_mcp_has_wallet_context():
        return {"status": "skipped", "summary": "TON MCP wallet context is not configured for DNS resolution.", "domain": normalized_domain, "address": ""}
    if normalized_domain in _TON_MCP_DNS_CACHE:
        return dict(_TON_MCP_DNS_CACHE[normalized_domain])
    request_timeout = 20.0 if _speed_profile(options) == "interactive" else 90.0
    try:
        payload = await asyncio.to_thread(_ton_mcp_call_tool_sync, "resolve_dns", {"domain": normalized_domain}, request_timeout)
    except Exception as exc:
        return {
            "status": "error",
            "summary": f"TON MCP resolve_dns failed: {exc}",
            "domain": normalized_domain,
            "address": "",
        }
    if payload.get("is_error"):
        return {
            "status": "error",
            "summary": str(payload.get("text") or "TON MCP returned an error for resolve_dns."),
            "domain": normalized_domain,
            "address": "",
        }
    data = dict(payload.get("data") or {})
    result = {
        "status": "success",
        "summary": f"TON MCP resolved {normalized_domain}.",
        "domain": str(data.get("domain") or normalized_domain),
        "address": str(data.get("address") or ""),
    }
    _TON_MCP_DNS_CACHE[normalized_domain] = dict(result)
    return result


async def get_ton_mcp_back_resolve_dns(case: ProjectCase, options: RunOptions, address: str) -> Dict[str, Any]:
    del case
    normalized_address = normalize_ws(address)
    if not normalized_address:
        return {"status": "skipped", "summary": "TON address is unavailable for reverse DNS lookup.", "address": "", "domain": ""}
    if options.mode == "recorded":
        return {
            "status": "skipped",
            "summary": "TON MCP reverse DNS lookup is disabled in recorded mode.",
            "address": normalized_address,
            "domain": "",
        }
    if not ton_mcp_has_wallet_context():
        return {
            "status": "skipped",
            "summary": "TON MCP wallet context is not configured for reverse DNS lookup.",
            "address": normalized_address,
            "domain": "",
        }
    if normalized_address in _TON_MCP_REVERSE_DNS_CACHE:
        return dict(_TON_MCP_REVERSE_DNS_CACHE[normalized_address])
    request_timeout = 20.0 if _speed_profile(options) == "interactive" else 90.0
    try:
        payload = await asyncio.to_thread(_ton_mcp_call_tool_sync, "back_resolve_dns", {"address": normalized_address}, request_timeout)
    except Exception as exc:
        return {
            "status": "error",
            "summary": f"TON MCP back_resolve_dns failed: {exc}",
            "address": normalized_address,
            "domain": "",
        }
    if payload.get("is_error"):
        return {
            "status": "error",
            "summary": str(payload.get("text") or "TON MCP returned an error for back_resolve_dns."),
            "address": normalized_address,
            "domain": "",
        }
    data = dict(payload.get("data") or {})
    result = {
        "status": "success",
        "summary": f"TON MCP reverse-resolved {normalized_address}.",
        "address": str(data.get("address") or normalized_address),
        "domain": str(data.get("domain") or ""),
    }
    _TON_MCP_REVERSE_DNS_CACHE[normalized_address] = dict(result)
    return result


async def get_ton_mcp_balance_by_address(case: ProjectCase, options: RunOptions, address: str) -> Dict[str, Any]:
    del case
    normalized_address = normalize_ws(address)
    if not normalized_address:
        return {"status": "skipped", "summary": "TON address is unavailable for balance lookup.", "address": "", "balance": "", "balance_nano": ""}
    if options.mode == "recorded":
        return {
            "status": "skipped",
            "summary": "TON MCP balance lookup is disabled in recorded mode.",
            "address": normalized_address,
            "balance": "",
            "balance_nano": "",
        }
    if not ton_mcp_has_wallet_context():
        return {
            "status": "skipped",
            "summary": "TON MCP wallet context is not configured for balance lookup.",
            "address": normalized_address,
            "balance": "",
            "balance_nano": "",
        }
    if normalized_address in _TON_MCP_BALANCE_CACHE:
        return dict(_TON_MCP_BALANCE_CACHE[normalized_address])
    request_timeout = 20.0 if _speed_profile(options) == "interactive" else 90.0
    try:
        payload = await asyncio.to_thread(_ton_mcp_call_tool_sync, "get_balance_by_address", {"address": normalized_address}, request_timeout)
    except Exception as exc:
        return {
            "status": "error",
            "summary": f"TON MCP get_balance_by_address failed: {exc}",
            "address": normalized_address,
            "balance": "",
            "balance_nano": "",
        }
    if payload.get("is_error"):
        return {
            "status": "error",
            "summary": str(payload.get("text") or "TON MCP returned an error for get_balance_by_address."),
            "address": normalized_address,
            "balance": "",
            "balance_nano": "",
        }
    data = dict(payload.get("data") or {})
    result = {
        "status": "success",
        "summary": f"TON MCP returned a balance for {normalized_address}.",
        "address": str(data.get("address") or normalized_address),
        "balance": str(data.get("balance") or ""),
        "balance_nano": str(data.get("balanceNano") or ""),
    }
    _TON_MCP_BALANCE_CACHE[normalized_address] = dict(result)
    return result


def _normalize_ton_account_activity(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    raw = dict(payload or {})
    last_tx_age_value = raw.get("last_tx_age_days")
    sample_transactions = []
    for item in raw.get("sample_transactions") or []:
        if not isinstance(item, dict):
            continue
        sample_transactions.append(
            {
                "hash": str(item.get("hash") or ""),
                "lt": str(item.get("lt") or ""),
                "published_at": str(item.get("published_at") or ""),
                "timestamp": int(item.get("timestamp") or 0),
            }
        )
    return {
        "status": str(raw.get("status") or "unknown"),
        "summary": str(raw.get("summary") or ""),
        "address": str(raw.get("address") or ""),
        "last_tx_at": str(raw.get("last_tx_at") or ""),
        "last_tx_age_days": -1 if last_tx_age_value in (None, "") else int(last_tx_age_value),
        "tx_count_7d": int(raw.get("tx_count_7d") or 0),
        "tx_count_30d": int(raw.get("tx_count_30d") or 0),
        "tx_count_30d_limit_hit": bool(raw.get("tx_count_30d_limit_hit") or False),
        "sample_transactions": sample_transactions[:5],
    }


def _normalize_toncenter_transaction_item(item: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = int(item.get("now") or 0)
    published_at = ""
    if timestamp > 0:
        published_at = _iso_utc(datetime.fromtimestamp(timestamp, tz=timezone.utc))
    return {
        "hash": str(item.get("hash") or ""),
        "lt": str(item.get("lt") or ""),
        "published_at": published_at,
        "timestamp": timestamp,
    }


async def get_ton_account_activity(case: ProjectCase, options: RunOptions, address: str) -> Dict[str, Any]:
    normalized_address = normalize_ws(address)
    cache_key = f"{_speed_profile(options)}:{normalized_address}"
    cached = _load_snapshot_json(case, "ton_account_activity.json")
    if options.mode in ("recorded", "auto") and cached is not None:
        return _normalize_ton_account_activity(cached)
    if not normalized_address:
        return {
            "status": "skipped",
            "summary": "TON address is unavailable for on-chain activity lookup.",
            "address": "",
            "last_tx_at": "",
            "last_tx_age_days": -1,
            "tx_count_7d": 0,
            "tx_count_30d": 0,
            "tx_count_30d_limit_hit": False,
            "sample_transactions": [],
        }
    if options.mode == "recorded":
        return {
            "status": "skipped",
            "summary": "Recorded TON account activity snapshot is unavailable.",
            "address": normalized_address,
            "last_tx_at": "",
            "last_tx_age_days": -1,
            "tx_count_7d": 0,
            "tx_count_30d": 0,
            "tx_count_30d_limit_hit": False,
            "sample_transactions": [],
        }
    if cache_key in _TON_ACCOUNT_ACTIVITY_CACHE:
        return dict(_TON_ACCOUNT_ACTIVITY_CACHE[cache_key])
    now_dt = datetime.now(timezone.utc)
    recent_30d_start = int((now_dt - timedelta(days=30)).timestamp())
    recent_7d_start = int((now_dt - timedelta(days=7)).timestamp())
    interactive_mode = _speed_profile(options) == "interactive"
    recent_limit = 200 if interactive_mode else 1000
    request_timeout = _profile_timeout(options, 10, 30)
    latest_query = urllib.parse.urlencode(
        [("account", normalized_address), ("limit", "1"), ("sort", "desc")],
        doseq=True,
    )
    recent_query = urllib.parse.urlencode(
        [("account", normalized_address), ("start_utime", str(recent_30d_start)), ("limit", str(recent_limit)), ("sort", "desc")],
        doseq=True,
    )
    latest_url = f"https://toncenter.com/api/v3/transactions?{latest_query}"
    recent_url = f"https://toncenter.com/api/v3/transactions?{recent_query}"
    try:
        latest_payload, recent_payload = await asyncio.gather(
            http_fetch_json(latest_url, headers=TONCENTER_HEADERS, timeout=request_timeout),
            http_fetch_json(recent_url, headers=TONCENTER_HEADERS, timeout=request_timeout),
        )
    except Exception as exc:
        return {
            "status": "error",
            "summary": f"TON account activity lookup failed: {exc}",
            "address": normalized_address,
            "last_tx_at": "",
            "last_tx_age_days": -1,
            "tx_count_7d": 0,
            "tx_count_30d": 0,
            "tx_count_30d_limit_hit": False,
            "sample_transactions": [],
        }
    latest_transactions = [
        _normalize_toncenter_transaction_item(item)
        for item in latest_payload.get("transactions") or []
        if isinstance(item, dict)
    ]
    recent_transactions = [
        _normalize_toncenter_transaction_item(item)
        for item in recent_payload.get("transactions") or []
        if isinstance(item, dict)
    ]
    last_tx_at = str(latest_transactions[0].get("published_at") or "") if latest_transactions else ""
    last_tx_age_days = days_since_iso(last_tx_at, now=now_dt)
    tx_count_7d = sum(1 for item in recent_transactions if int(item.get("timestamp") or 0) >= recent_7d_start)
    tx_count_30d = len(recent_transactions)
    tx_count_30d_limit_hit = tx_count_30d >= recent_limit
    if last_tx_at:
        if tx_count_30d > 0:
            summary = (
                f"Observed {tx_count_30d}+ address transactions in the last 30 days."
                if tx_count_30d_limit_hit
                else f"Observed {tx_count_30d} address transactions in the last 30 days."
            )
            summary += f" Latest transaction is {int(last_tx_age_days or 0)} days old."
        else:
            summary = f"Latest on-chain transaction is {int(last_tx_age_days or 0)} days old."
    else:
        summary = "No on-chain transactions were returned for the project address."
    result = _normalize_ton_account_activity(
        {
            "status": "success",
            "summary": summary,
            "address": normalized_address,
            "last_tx_at": last_tx_at,
            "last_tx_age_days": -1 if last_tx_age_days is None else int(last_tx_age_days),
            "tx_count_7d": tx_count_7d,
            "tx_count_30d": tx_count_30d,
            "tx_count_30d_limit_hit": tx_count_30d_limit_hit,
            "sample_transactions": recent_transactions[:5] or latest_transactions[:1],
        }
    )
    if options.record_snapshots or options.mode == "live":
        _save_snapshot_json(case, "ton_account_activity.json", result)
    _TON_ACCOUNT_ACTIVITY_CACHE[cache_key] = dict(result)
    return result


async def get_github_repo_bundle(case: ProjectCase, options: RunOptions) -> Dict[str, Any]:
    cached_meta = _load_snapshot_json(case, "github_repo.json")
    cached_readme = _load_snapshot_text(case, "github_readme.txt")
    if options.mode in ("recorded", "auto") and cached_meta is not None and cached_readme is not None:
        return {"repo": cached_meta, "readme": cached_readme}
    if options.mode == "recorded":
        raise FileNotFoundError(f"Recorded snapshots are missing for case={case.case_id} block=github_repo")
    if not case.github_repo:
        return {"repo": {}, "readme": ""}

    repo_meta = await fetch_github_repo_meta(case.github_repo, _speed_profile(options))
    decoded = ""
    readme_url = f"https://api.github.com/repos/{case.github_repo}/readme"
    try:
        readme_obj = await http_fetch_json(
            readme_url,
            headers=GITHUB_HEADERS,
            timeout=_profile_timeout(options, 10, 30),
        )
    except Exception:
        page_html = await http_fetch_text(
            f"https://github.com/{case.github_repo}",
            headers=GITHUB_HTML_HEADERS,
            timeout=_profile_timeout(options, 8, 45),
        )
        parsed_meta = _parse_github_repo_html(case.github_repo, page_html)
        decoded = str(parsed_meta.pop("_readme_text", "") or "")
        if not decoded:
            git_bundle = await asyncio.to_thread(
                _ensure_github_git_bundle_sync,
                case.github_repo,
                str(repo_meta.get("default_branch") or parsed_meta.get("default_branch") or ""),
                0,
            )
            repo_dir = Path(str(git_bundle.get("path") or ""))
            if repo_dir.is_dir():
                decoded = await asyncio.to_thread(_git_readme_text, repo_dir)
                if not repo_meta.get("pushed_at") or not repo_meta.get("created_at"):
                    git_activity = await asyncio.to_thread(_git_activity_payload, repo_dir)
                    commits = [item for item in git_activity.get("commits") or [] if isinstance(item, dict)]
                    if commits:
                        repo_meta["pushed_at"] = str(commits[0].get("date") or repo_meta.get("pushed_at") or "")
                        repo_meta["created_at"] = str(commits[-1].get("date") or repo_meta.get("created_at") or "")
        for key, value in parsed_meta.items():
            if key == "_readme_text":
                continue
            if value not in (None, "", [], {}) and not repo_meta.get(key):
                repo_meta[key] = value
    else:
        raw_content = str(readme_obj.get("content") or "")
        decoded = base64.b64decode(raw_content).decode("utf-8", errors="replace") if raw_content else ""
    if options.record_snapshots or options.mode == "live":
        _save_snapshot_json(case, "github_repo.json", repo_meta)
        _save_snapshot_text(case, "github_readme.txt", decoded)
    return {"repo": repo_meta, "readme": decoded}


async def get_github_tree(case: ProjectCase, default_branch: str, options: RunOptions) -> Dict[str, Any]:
    cached = _load_snapshot_json(case, "github_tree.json")
    if options.mode in ("recorded", "auto") and cached is not None:
        return cached
    if options.mode == "recorded":
        raise FileNotFoundError(f"Recorded snapshots are missing for case={case.case_id} block=github_tree")
    if not case.github_repo or not default_branch:
        return {"tree": []}
    branch = urllib.parse.quote(default_branch, safe="")
    url = f"https://api.github.com/repos/{case.github_repo}/git/trees/{branch}?recursive=1"
    try:
        tree = await http_fetch_json(url, headers=GITHUB_HEADERS, timeout=_profile_timeout(options, 10, 30))
    except Exception as exc:
        if _speed_profile(options) == "interactive" and _is_github_rate_limit_error(exc):
            tree = {
                "tree": [],
                "_source_status": "incomplete",
                "_source_summary": "GitHub tree API is rate limited in interactive mode.",
            }
        else:
            git_bundle = await asyncio.to_thread(_ensure_github_git_bundle_sync, case.github_repo, default_branch, 0)
            repo_dir = Path(str(git_bundle.get("path") or ""))
            tree = await asyncio.to_thread(_git_tree_payload, repo_dir)
    if options.record_snapshots or options.mode == "live":
        _save_snapshot_json(case, "github_tree.json", tree)
    return tree


def _normalize_commit_item(item: Dict[str, Any]) -> Dict[str, Any]:
    commit = item.get("commit") or {}
    author = commit.get("author") or {}
    committer = commit.get("committer") or {}
    author_user = item.get("author") or {}
    return {
        "sha": str(item.get("sha") or ""),
        "date": str(committer.get("date") or author.get("date") or ""),
        "author_login": str(author_user.get("login") or ""),
        "author_name": str(author.get("name") or committer.get("name") or ""),
        "message": str(commit.get("message") or ""),
        "parent_count": len(item.get("parents") or []),
    }


def _normalize_release_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "tag_name": str(item.get("tag_name") or ""),
        "name": str(item.get("name") or ""),
        "draft": bool(item.get("draft") or False),
        "prerelease": bool(item.get("prerelease") or False),
        "published_at": str(item.get("published_at") or ""),
    }


async def get_github_activity_bundle(case: ProjectCase, default_branch: str, options: RunOptions) -> Dict[str, Any]:
    cached = _load_snapshot_json(case, "github_activity.json")
    if options.mode in ("recorded", "auto") and cached is not None:
        return cached
    if options.mode == "recorded":
        raise FileNotFoundError(f"Recorded snapshots are missing for case={case.case_id} block=github_activity")
    if not case.github_repo or not default_branch:
        return {"commits": [], "releases": [], "commit_pages_loaded": 0, "commit_page_limit_hit": False}

    branch = urllib.parse.quote(default_branch, safe="")
    try:
        commits: List[Dict[str, Any]] = []
        page_limit = 2 if _speed_profile(options) == "interactive" else 5
        page = 1
        while page <= page_limit:
            commits_url = (
                f"https://api.github.com/repos/{case.github_repo}/commits"
                f"?sha={branch}&per_page=100&page={page}"
            )
            page_items = await http_fetch_json(
                commits_url,
                headers=GITHUB_HEADERS,
                timeout=_profile_timeout(options, 10, 30),
            )
            if not isinstance(page_items, list) or not page_items:
                break
            normalized_items = [_normalize_commit_item(item) for item in page_items]
            commits.extend(normalized_items)
            oldest_date = parse_github_datetime(normalized_items[-1].get("date") or "")
            if len(page_items) < 100:
                break
            if oldest_date is not None:
                days_old = int((datetime.now(timezone.utc) - oldest_date).total_seconds() // 86400)
                if days_old > 370:
                    break
            page += 1

        releases_limit = 30 if _speed_profile(options) == "interactive" else 100
        releases_url = f"https://api.github.com/repos/{case.github_repo}/releases?per_page={releases_limit}"
        releases_raw = await http_fetch_json(releases_url, headers=GITHUB_HEADERS, timeout=_profile_timeout(options, 10, 30))
        releases = [_normalize_release_item(item) for item in releases_raw] if isinstance(releases_raw, list) else []
        payload = {
            "commits": commits,
            "releases": releases,
            "commit_pages_loaded": min(page_limit, page) if commits else 0,
            "commit_page_limit_hit": bool(commits and page > page_limit),
        }
    except Exception as exc:
        if _speed_profile(options) == "interactive" and _is_github_rate_limit_error(exc):
            payload = {
                "commits": [],
                "releases": [],
                "commit_pages_loaded": 0,
                "commit_page_limit_hit": False,
                "_source_status": "incomplete",
                "_source_summary": "GitHub activity API is rate limited in interactive mode.",
            }
        else:
            git_bundle = await asyncio.to_thread(
                _ensure_github_git_bundle_sync,
                case.github_repo,
                default_branch,
                120 if _speed_profile(options) == "interactive" else 400,
            )
            repo_dir = Path(str(git_bundle.get("path") or ""))
            payload = await asyncio.to_thread(_git_activity_payload, repo_dir)
    if options.record_snapshots or options.mode == "live":
        _save_snapshot_json(case, "github_activity.json", payload)
    return payload


def _extract_telegram_posts(page_html: str) -> List[str]:
    return [str(item.get("text") or "") for item in _extract_telegram_entries(page_html)]


def _extract_telegram_entries(page_html: str) -> List[Dict[str, Any]]:
    import re

    matches = re.findall(
        r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
        page_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    posts: List[Dict[str, Any]] = []
    for raw in matches:
        clean = _clean_telegram_post_text(strip_html(raw))
        if clean:
            posts.append({"text": clean, "date_text": "", "published_at": "", "url": ""})
    unique_posts: List[Dict[str, Any]] = []
    seen = set()
    for post in posts:
        key = normalize_ws(str(post.get("text") or ""))
        if key and key not in seen:
            seen.add(key)
            unique_posts.append(post)
    if unique_posts:
        return unique_posts[:25]

    chunks = re.split(r"\n{2,}", str(page_html or ""))
    for chunk in chunks:
        clean = _clean_telegram_post_text(strip_html(chunk))
        low = clean.lower()
        if len(clean) < 50:
            continue
        if "view in telegram" in low:
            continue
        if low.startswith("telegram:"):
            continue
        if "monthly users" in low:
            continue
        if clean not in seen:
            seen.add(clean)
            unique_posts.append({"text": clean, "date_text": "", "published_at": "", "url": ""})
    return _dedupe_telegram_entries(unique_posts)


def _extract_tgstat_posts(page_html: str) -> List[str]:
    return [str(item.get("text") or "") for item in _extract_tgstat_entries(page_html)]


def _extract_tgstat_entries(page_html: str) -> List[Dict[str, Any]]:
    import re

    blocks = [
        block
        for block in re.split(
            r'<hr class="m-0 mb-2">',
            str(page_html or ""),
            flags=re.IGNORECASE,
        )
        if "post-container" in block.lower()
    ]
    fetched_at = datetime.now(timezone.utc)
    posts: List[Dict[str, Any]] = []
    for block in blocks:
        date_match = re.search(r"<small>(.*?)</small>", block, flags=re.IGNORECASE | re.DOTALL)
        date_text = normalize_ws(date_match.group(1)) if date_match else ""
        published_at = _parse_tgstat_datetime(date_text, fetched_at) if date_text else ""
        url_match = re.search(
            r'href="(https://[^"]+/\d+)"',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        post_url = str(url_match.group(1)) if url_match else ""
        text_match = re.search(
            r'<div class="post-text">(.*?)</div>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not text_match:
            continue
        clean = _clean_telegram_post_text(strip_html(text_match.group(1)))
        if not clean:
            continue
        low = clean.lower()
        if low in {"open in telegram", "share", "report"}:
            continue
        posts.append(
            {
                "text": clean,
                "date_text": date_text,
                "published_at": published_at,
                "url": post_url,
            }
        )
    return _dedupe_telegram_entries(posts)


def _extract_tgchannels_entries(page_html: str) -> List[Dict[str, Any]]:
    import re

    blocks = re.findall(
        r'<div class="channel-post">(.*?)(?=<div class="channel-post">|<div class="container">|<footer>)',
        str(page_html or ""),
        flags=re.IGNORECASE | re.DOTALL,
    )
    entries: List[Dict[str, Any]] = []
    for block in blocks:
        date_match = re.search(
            r'<small class="channel-post__post-date">(.*?)</small>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        text_match = re.search(
            r'<p class="channel-post__post-text"[^>]*>(.*?)</p>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        url_match = re.search(
            r'href="(https://t\.me/[^"]+/\d+)"',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not text_match:
            continue
        date_text = normalize_ws(date_match.group(1)) if date_match else ""
        raw_text = text_match.group(1)
        post_url = str(url_match.group(1)) if url_match else ""
        clean = _clean_telegram_post_text(strip_html(raw_text))
        if not clean:
            continue
        entries.append(
            {
                "text": clean,
                "date_text": date_text,
                "published_at": _parse_tgchannels_datetime(str(date_text or "")),
                "url": str(post_url or ""),
            }
        )
    if entries:
        return _dedupe_telegram_entries(entries)
    matches = re.findall(
        r'<small class="channel-post__post-date">(.*?)</small>.*?<p class="channel-post__post-text"[^>]*>(.*?)</p>.*?href="(https://t\.me/[^"]+/\d+)"',
        str(page_html or ""),
        flags=re.IGNORECASE | re.DOTALL,
    )
    fallback_entries: List[Dict[str, Any]] = []
    for date_text, raw_text, post_url in matches:
        clean = _clean_telegram_post_text(strip_html(raw_text))
        if not clean:
            continue
        normalized_date_text = normalize_ws(date_text)
        fallback_entries.append(
            {
                "text": clean,
                "date_text": normalized_date_text,
                "published_at": _parse_tgchannels_datetime(str(normalized_date_text or "")),
                "url": str(post_url or ""),
            }
        )
    return _dedupe_telegram_entries(fallback_entries)


def _telegram_candidate_quality(entries: List[Dict[str, Any]]) -> tuple[int, int, int]:
    dated_count = sum(1 for item in entries if str(item.get("published_at") or ""))
    linked_count = sum(1 for item in entries if str(item.get("url") or ""))
    return (len(entries), dated_count, linked_count)


async def _fetch_telegram_candidate(
    source: str,
    url: str,
    headers: Dict[str, str],
    timeout: int,
    extractor: Any,
    error_label: str,
) -> Dict[str, Any]:
    page_html = await http_fetch_text(url, headers=headers, timeout=timeout)
    entries = extractor(page_html)
    if not entries:
        raise ValueError(f"{error_label} returned no posts")
    return {
        "source": source,
        "entries": entries,
        "page_html": page_html,
    }


def _select_best_telegram_candidate(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    priority = {"tgstat": 3, "tgchannels": 2, "telegram_web": 1}
    return max(
        candidates,
        key=lambda item: (
            _telegram_candidate_quality([entry for entry in item.get("entries") or [] if isinstance(entry, dict)]),
            priority.get(str(item.get("source") or ""), 0),
        ),
    )


async def get_telegram_snapshot(case: ProjectCase, options: RunOptions) -> Dict[str, Any]:
    cached = _load_snapshot_json(case, "telegram_snapshot.json")
    if options.mode in ("recorded", "auto") and cached is not None:
        return _normalize_telegram_snapshot(cached)
    if options.mode == "recorded":
        raise FileNotFoundError(f"Recorded snapshots are missing for case={case.case_id} block=telegram_channel")
    if not case.telegram_handle:
        return {"handle": "", "posts": [], "post_count": 0, "source": "", "entries": [], "fetched_at": ""}
    tgstat_url = f"https://tgstat.com/channel/@{case.telegram_handle}"
    tgchannels_url = f"https://tgchannels.org/channel/{case.telegram_handle}?lang=all&start=0"
    official_url = f"https://t.me/s/{case.telegram_handle}"
    fetched_at = _iso_utc(datetime.now(timezone.utc))
    request_timeout = _profile_timeout(options, 8, 30)
    interactive_mode = _speed_profile(options) == "interactive"
    candidates: List[Dict[str, Any]] = []
    source_errors: List[str] = []
    primary_sources = [
        ("tgstat", tgstat_url, TGSTAT_HEADERS, _extract_tgstat_entries, "TGStat"),
        ("tgchannels", tgchannels_url, TGCHANNELS_HEADERS, _extract_tgchannels_entries, "TGChannels"),
    ]
    primary_results = await asyncio.gather(
        *[
            _fetch_telegram_candidate(source, url, headers, request_timeout, extractor, error_label)
            for source, url, headers, extractor, error_label in primary_sources
        ],
        return_exceptions=True,
    )
    for (source, _, _, _, _), payload in zip(primary_sources, primary_results):
        if isinstance(payload, Exception):
            source_errors.append(f"{source}: {payload}")
            continue
        candidates.append(payload)
    if not candidates:
        try:
            official_candidate = await _fetch_telegram_candidate(
                "telegram_web",
                official_url,
                TELEGRAM_HEADERS,
                request_timeout,
                _extract_telegram_entries,
                "Telegram web",
            )
        except Exception as exc:
            source_errors.append(f"telegram_web: {exc}")
        else:
            candidates.append(official_candidate)
    if not candidates:
        raise RuntimeError("; ".join(source_errors[:4]) or "No public Telegram source returned posts")
    selected = _select_best_telegram_candidate(candidates)
    source = str(selected.get("source") or "")
    entries = [item for item in selected.get("entries") or [] if isinstance(item, dict)]
    page_html = str(selected.get("page_html") or "")
    posts = [str(item.get("text") or "") for item in entries]
    snapshot = {
        "handle": case.telegram_handle,
        "posts": posts,
        "post_count": len(posts),
        "source": source,
        "entries": entries,
        "fetched_at": fetched_at,
        "source_errors": source_errors,
    }
    if options.record_snapshots or options.mode == "live":
        _save_snapshot_text(case, "telegram_page.html", page_html)
        _save_snapshot_json(case, "telegram_snapshot.json", snapshot)
    return _normalize_telegram_snapshot(snapshot)
