from __future__ import annotations

import html
import hashlib
import json
import re
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

TON_ADDRESS_RE = re.compile(r"\b(?:EQ|UQ|kQ|Ef|Uf)[A-Za-z0-9_-]{20,}\b")
TON_DNS_DOMAIN_RE = re.compile(r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:ton|t\.me)\b", re.IGNORECASE)
CONTRACT_FILE_EXTENSIONS = (".fc", ".func", ".fif", ".tact", ".tolk")
CONTRACT_ARTIFACT_EXTENSIONS = (".boc", ".base64", ".cell", ".abi", ".tlb")
CONTRACT_CODE_EXTENSIONS = (".ts", ".tsx", ".js", ".jsx", ".py", ".go", ".rs", ".java")
NON_CONTRACT_DOC_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".md", ".txt")
SCAM_KEYWORDS = (
    "guaranteed profit",
    "x100",
    "double your",
    "dm for investment",
    "seed phrase",
    "wallet connect and claim",
)
SCAM_CONTEXTUAL_KEYWORDS = frozenset({"seed phrase"})
ANTI_SCAM_CONTEXT_MARKERS = (
    "never share",
    "do not share",
    "don't share",
    "never reveal",
    "do not reveal",
    "don't reveal",
    "team will never",
    "support will never",
    "never dm you",
    "never dm you first",
    "will never dm",
    "fake support",
    "impersonation",
    "impersonator",
    "scammers",
    "don't click",
    "do not click",
    "never click",
    "don't connect your wallet",
    "do not connect your wallet",
)
TON_KEYWORDS = (
    "ton",
    "ton blockchain",
    "the open network",
    "tonconnect",
    "tonkeeper",
    "tonapi",
    "tact",
    "func",
    "jetton",
    "nft",
    "liteserver",
    "validator",
)


def ensure_dir(path: str | Path) -> Path:
    resolved = Path(path)
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def read_json(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text("utf-8"))


def write_json(path: str | Path, payload: Dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")


def write_text(path: str | Path, text: str) -> None:
    Path(path).write_text(text, "utf-8")


def read_text(path: str | Path) -> str:
    return Path(path).read_text("utf-8")


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


@lru_cache(maxsize=None)
def _keyword_pattern(keyword: str) -> re.Pattern[str]:
    normalized = normalize_ws(str(keyword or "")).lower()
    if not normalized:
        return re.compile(r"(?!x)x")
    prefix = r"(?<![a-z0-9])" if normalized[:1].isalnum() else ""
    suffix = r"(?![a-z0-9])" if normalized[-1:].isalnum() else ""
    return re.compile(f"{prefix}{re.escape(normalized)}{suffix}")


def _keyword_matches(text: str, keyword: str) -> List[re.Match]:
    normalized_text = normalize_ws(str(text or "")).lower()
    normalized_keyword = normalize_ws(str(keyword or "")).lower()
    if not normalized_text or not normalized_keyword:
        return []
    return list(_keyword_pattern(normalized_keyword).finditer(normalized_text))


def lower_text(parts: Iterable[str]) -> str:
    return normalize_ws(" ".join(str(part or "") for part in parts)).lower()


def clip_text(text: str, limit: int = 1500) -> str:
    raw = str(text or "")
    if len(raw) <= limit:
        return raw
    return raw[: limit - 3] + "..."


def strip_html(raw_html: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", str(raw_html or ""), flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return normalize_ws(text)


def keyword_hits(text: str, keywords: Iterable[str]) -> List[str]:
    hits: List[str] = []
    for keyword in keywords:
        value = normalize_ws(str(keyword or ""))
        if not value:
            continue
        if _keyword_matches(text, value) and value not in hits:
            hits.append(value)
    return hits


def scam_keyword_hits(text: str) -> List[str]:
    normalized_text = normalize_ws(str(text or "")).lower()
    hits: List[str] = []
    for keyword in SCAM_KEYWORDS:
        matches = _keyword_matches(normalized_text, keyword)
        if not matches:
            continue
        accepted = False
        for match in matches:
            if keyword in SCAM_CONTEXTUAL_KEYWORDS:
                window = normalized_text[max(0, match.start() - 160) : min(len(normalized_text), match.end() + 160)]
                if any(marker in window for marker in ANTI_SCAM_CONTEXT_MARKERS):
                    continue
            accepted = True
            break
        if accepted:
            hits.append(keyword)
    return hits


def count_keyword_hits(text: str, keywords: Iterable[str]) -> int:
    return len(keyword_hits(text, keywords))


def extract_ton_addresses(text: str) -> List[str]:
    seen = set()
    out: List[str] = []
    for match in TON_ADDRESS_RE.findall(str(text or "")):
        if match not in seen:
            seen.add(match)
            out.append(match)
    return out


def extract_ton_dns_domains(text: str) -> List[str]:
    seen = set()
    out: List[str] = []
    for match in TON_DNS_DOMAIN_RE.findall(str(text or "").lower()):
        value = normalize_ws(match).strip(".")
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def detect_contract_paths(paths: Iterable[str]) -> List[str]:
    contract_name_re = re.compile(r"(?:^|[-_/])(contract|jetton|minter|nft|sbt|sale|swap|auction|marketplace|collection|deployer|vesting|staking)(?:[-_/\.]|$)")
    contract_context_markers = (
        "contracts/",
        "contract/",
        "wrappers/",
        "sources/",
        "blueprints/",
        "templates/",
    )
    out: List[str] = []
    seen = set()
    for path in paths:
        low = str(path or "").lower()
        if not low:
            continue
        if low.endswith(NON_CONTRACT_DOC_EXTENSIONS):
            continue
        if low.endswith(CONTRACT_FILE_EXTENSIONS + CONTRACT_ARTIFACT_EXTENSIONS):
            if low not in seen:
                seen.add(low)
                out.append(str(path))
            continue
        if any(marker in low for marker in contract_context_markers):
            if low not in seen:
                seen.add(low)
                out.append(str(path))
            continue
        if contract_name_re.search(low) and low.endswith(CONTRACT_CODE_EXTENSIONS):
            if "/docs/" in low or low.startswith("docs/"):
                continue
            if low not in seen:
                seen.add(low)
                out.append(str(path))
    return out


def compact_repo_meta(repo: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "full_name": repo.get("full_name", ""),
        "description": repo.get("description", ""),
        "default_branch": repo.get("default_branch", ""),
        "language": repo.get("language", ""),
        "topics": repo.get("topics") or [],
        "stargazers_count": int(repo.get("stargazers_count") or 0),
        "forks_count": int(repo.get("forks_count") or 0),
        "open_issues_count": int(repo.get("open_issues_count") or 0),
        "archived": bool(repo.get("archived") or False),
        "created_at": repo.get("created_at", ""),
        "updated_at": repo.get("updated_at", ""),
        "homepage": repo.get("homepage", ""),
    }


def stable_score(value: float) -> int:
    return max(0, min(100, int(round(value))))


def parse_github_datetime(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def days_since_iso(value: str, now: Optional[datetime] = None) -> Optional[int]:
    parsed = parse_github_datetime(value)
    if parsed is None:
        return None
    current = now or datetime.now(timezone.utc)
    return max(0, int((current - parsed).total_seconds() // 86400))


def sha256_text(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()
