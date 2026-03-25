from __future__ import annotations

import asyncio
import copy
import html
import json
import mimetypes
import os
import re
import threading
import time
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, quote_plus, unquote, urlparse
from uuid import uuid4

from .llm import build_llm_client
from .models import ProjectCase, RunOptions
from .orchestrator import Orchestrator
from .project_registry import REGISTRY_STOPWORDS
from .registry import BlockRegistry
from .sources import GITHUB_HEADERS, GITHUB_HTML_HEADERS, TELEGRAM_HEADERS, TGSTAT_HEADERS, fetch_github_repo_meta, http_fetch_json, http_fetch_text
from .utils import clip_text, ensure_dir, extract_ton_addresses, normalize_ws, read_json, read_text, strip_html, write_json
from .workflow import summarize_status
from .workflow_builder import build_workflow_plan

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ARTIFACTS_ROOT = PROJECT_ROOT / "artifacts"
DEFAULT_VALIDATORS_ROOT = Path(__file__).resolve().parent / "validators"
DEFAULT_STATIC_ROOT = Path(__file__).resolve().parent / "viewer_static"
DEFAULT_INPUT_CASES_ROOT = DEFAULT_ARTIFACTS_ROOT / "input_cases"
MARKET_API_HEADERS = {
    "Accept": "application/json",
    "User-Agent": TELEGRAM_HEADERS["User-Agent"],
    "Referer": "https://www.geckoterminal.com/",
    "Origin": "https://www.geckoterminal.com",
}
UNIT_PRESENTATION_PROFILES = {
    "telegram_semantics": {
        "kind": "hybrid",
        "label": "Hybrid analysis",
        "badges": ["Hybrid", "Model", "Verified"],
        "note": "Combines deterministic heuristics with model-assisted semantic scoring to judge community quality and scam-like content.",
    },
    "sonar_research": {
        "kind": "ai",
        "label": "External research",
        "badges": ["Model", "Tool"],
        "note": "Public research block that can use external retrieval for additional project evidence.",
    },
    "llm_explainer": {
        "kind": "ai",
        "label": "Result synthesis",
        "badges": ["Model"],
        "note": "Turns structured evidence into a compact explanation for reviewers.",
    },
}


def _safe_relative(path: str | Path) -> str:
    resolved = Path(path).resolve()
    try:
        return str(resolved.relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(resolved)


def _has_llm_api_key(env_name: str) -> bool:
    return bool(str(os.getenv(env_name, "") or "").strip())


def _preview_json(value: Any, depth: int = 0) -> Any:
    if depth >= 4:
        if isinstance(value, dict):
            return f"<dict keys={len(value)}>"
        if isinstance(value, list):
            return f"<list items={len(value)}>"
        if isinstance(value, str):
            return clip_text(value, 240)
        return value
    if isinstance(value, dict):
        items = list(value.items())
        out = {str(key): _preview_json(item, depth + 1) for key, item in items[:14]}
        if len(items) > 14:
            out["__more_keys__"] = len(items) - 14
        return out
    if isinstance(value, list):
        out = [_preview_json(item, depth + 1) for item in value[:14]]
        if len(value) > 14:
            out.append(f"... {len(value) - 14} more items")
        return out
    if isinstance(value, str):
        return clip_text(value, 700)
    return value


def _run_candidates(artifacts_root: str | Path = DEFAULT_ARTIFACTS_ROOT) -> List[Path]:
    base = Path(artifacts_root)
    candidates: List[Path] = []
    runs_root = base / "runs"
    if runs_root.is_dir():
        candidates.extend(sorted(runs_root.glob("*/run_summary.json")))
    candidates.extend(
        sorted(path for path in base.glob("*/run_summary.json") if path.parent.name != "runs")
    )
    return sorted(candidates, key=lambda path: path.stat().st_mtime, reverse=True)


def _resolve_run_dir(run_id: str, artifacts_root: str | Path = DEFAULT_ARTIFACTS_ROOT) -> Path:
    safe_id = Path(unquote(run_id)).name
    base = Path(artifacts_root)
    direct_candidates = [base / "runs" / safe_id, base / safe_id]
    for candidate in direct_candidates:
        if (candidate / "run_summary.json").is_file():
            return candidate
    for summary_path in _run_candidates(artifacts_root):
        if summary_path.parent.name == safe_id:
            return summary_path.parent
    raise FileNotFoundError(f"Unknown run_id={safe_id}")


def _load_trace(run_dir: str | Path, block_id: str) -> Dict[str, Any]:
    path = Path(run_dir) / "blocks" / f"{block_id}.json"
    if not path.is_file():
        return {}
    return read_json(path)


def _load_llm_trace(run_dir: str | Path, block_id: str) -> Dict[str, Any]:
    path = Path(run_dir) / "llm" / f"{block_id}.json"
    if not path.is_file():
        return {}
    return read_json(path)


def _summary_path(run_dir: str | Path) -> Path:
    return Path(run_dir) / "run_summary.json"


def _load_summary(run_dir: str | Path) -> Dict[str, Any]:
    return read_json(_summary_path(run_dir))


def _slugify(value: str, fallback: str = "project") -> str:
    raw = normalize_ws(value).lower()
    slug = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    return slug or fallback


def _extract_github_repo(value: str) -> str:
    raw = normalize_ws(value)
    if not raw:
        return ""
    match = re.search(r"github\.com/([^/\s]+)/([^/\s?#]+)", raw, flags=re.IGNORECASE)
    if match:
        owner = match.group(1).strip()
        repo = match.group(2).strip().removesuffix(".git")
        return f"{owner}/{repo}" if owner and repo else ""
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", raw):
        return raw
    return ""


def _extract_telegram_handle(value: str) -> str:
    raw = normalize_ws(value)
    if not raw:
        return ""
    if raw.startswith("@"):
        return raw[1:]
    match = re.search(r"(?:https?://)?t\.me/([A-Za-z0-9_]+)", raw, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return ""


def _extract_html_title(value: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", str(value or ""), flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    title = re.sub(r"<[^>]+>", " ", match.group(1))
    return normalize_ws(html.unescape(title))


def _display_name_from_title(title: str, fallback: str) -> str:
    normalized = normalize_ws(title)
    if normalized:
        for separator in (" | ", " - ", " — ", ": "):
            if separator in normalized:
                head = normalize_ws(normalized.split(separator, 1)[0])
                if len(head) >= 3:
                    return head
        if len(normalized) >= 3:
            return normalized
    return normalize_ws(fallback)


def _case_field(case: ProjectCase | Dict[str, Any], key: str) -> str:
    if isinstance(case, dict):
        return str(case.get(key) or "")
    return str(getattr(case, key, "") or "")


def _project_key_from_case(case: ProjectCase | Dict[str, Any]) -> str:
    for key in ("github_repo", "project_url", "telegram_handle", "wallet_address", "case_id"):
        value = _case_field(case, key).strip().lower()
        if value:
            return value
    return ""


def _result_summary_payload(result: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = result or {}
    return {
        "status": str(payload.get("status") or "pending"),
        "summary": str(payload.get("summary") or ""),
        "flags": [str(item) for item in payload.get("flags") or []],
        "needs_human_review": bool(payload.get("needs_human_review") or False),
    }


def _discovery_result(status: str, summary: str, *, flags: Optional[List[str]] = None) -> Dict[str, Any]:
    return {
        "status": str(status or "pending"),
        "summary": str(summary or ""),
        "flags": [str(item) for item in (flags or [])],
        "needs_human_review": False,
    }


def _dedupe_text_list(values: Iterable[Any]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        text = normalize_ws(str(value or ""))
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _github_repo_url(repo: str) -> str:
    normalized = normalize_ws(repo)
    return f"https://github.com/{normalized}" if normalized else ""


def _github_commits_url(repo: str) -> str:
    base = _github_repo_url(repo)
    return f"{base}/commits" if base else ""


def _github_readme_url(repo: str) -> str:
    base = _github_repo_url(repo)
    return f"{base}#readme" if base else ""


def _telegram_url(handle: str) -> str:
    normalized = normalize_ws(handle).lstrip("@")
    return f"https://t.me/{normalized}" if normalized else ""


def _tonviewer_address_url(address: str) -> str:
    normalized = normalize_ws(address)
    return f"https://tonviewer.com/{normalized}" if normalized else ""


def _normalize_locale(value: str) -> str:
    normalized = normalize_ws(value).lower()
    return "ru" if normalized == "ru" else "en"


def _locale_text(locale: str, *, en: str, ru: str) -> str:
    return ru if _normalize_locale(locale) == "ru" else en


def _closest_project_payload(item: Dict[str, Any]) -> Dict[str, Any]:
    github_repo = str(item.get("github_repo") or "")
    return {
        "case_id": str(item.get("case_id") or ""),
        "name": str(item.get("name") or ""),
        "github_repo": github_repo,
        "project_type": str(item.get("project_type") or ""),
        "overall_similarity": item.get("overall_similarity"),
        "url": _github_repo_url(github_repo),
    }


def _copy_statement_excerpt(readme_excerpt: str, copy_hits: List[str]) -> str:
    text = normalize_ws(readme_excerpt)
    if not text:
        return ""
    if not copy_hits:
        return ""
    lowered = text.lower()
    for hit in copy_hits:
        needle = normalize_ws(str(hit or "")).lower()
        if not needle:
            continue
        index = lowered.find(needle)
        if index < 0:
            continue
        start = max(0, index - 72)
        end = min(len(text), index + len(needle) + 148)
        return clip_text(text[start:end], 220)
    return ""


def _clone_analysis(case: Dict[str, Any], results: Dict[str, Any], locale: str = "en") -> Dict[str, Any]:
    locale = _normalize_locale(locale)
    similarity_result = (results or {}).get("project_similarity") or {}
    similarity_metrics = dict(similarity_result.get("metrics") or {})
    similarity_data = dict(similarity_result.get("data") or {})
    repo_result = (results or {}).get("github_repo") or {}
    readme_excerpt = str(((repo_result.get("data") or {}) if isinstance(repo_result, dict) else {}).get("readme_excerpt") or "")
    copy_hits = [str(item) for item in similarity_data.get("copy_disclosure_hits") or [] if str(item or "").strip()]
    current_repo_key = normalize_ws(str(case.get("github_repo") or "")).lower()
    raw_closest_projects = [
        item for item in similarity_data.get("closest_projects") or [] if isinstance(item, dict)
    ]
    if current_repo_key:
        raw_closest_projects = [
            item for item in raw_closest_projects if normalize_ws(str(item.get("github_repo") or "")).lower() != current_repo_key
        ]
    closest_projects = [_closest_project_payload(item) for item in raw_closest_projects]
    top_match = closest_projects[0] if closest_projects else {}
    top_similarity = float(top_match.get("overall_similarity") or 0.0) if top_match else 0.0
    top_history_match = bool((raw_closest_projects[0] if raw_closest_projects else {}).get("history_fingerprint_match"))
    clone_risk = "low"
    if top_similarity >= 0.82 or (top_history_match and top_similarity >= 0.72):
        clone_risk = "high"
    elif top_similarity >= 0.62 or (top_history_match and top_similarity >= 0.55):
        clone_risk = "moderate"
    source_project = top_match if clone_risk in {"moderate", "high"} and top_match else {}
    self_declared_copy_excerpt = _copy_statement_excerpt(readme_excerpt, copy_hits)
    if source_project:
        if locale == "ru":
            summary = (
                f"Ближайший подтверждённый проект из реестра: "
                f"{source_project.get('name') or source_project.get('github_repo') or 'проект'}, "
                f"общая схожесть {top_similarity:.4f}."
            )
        else:
            summary = (
                f"Closest matched registry project: {source_project.get('name') or source_project.get('github_repo') or 'project'} "
                f"with overall similarity {top_similarity:.4f}."
            )
    elif self_declared_copy_excerpt:
        if top_match:
            if locale == "ru":
                summary = (
                    "В README есть самоописание о копии репозитория, но ближайший известный проект из реестра "
                    f"имеет схожесть только {top_similarity:.4f}, поэтому сильного подтверждения внешнего клонирования нет."
                )
            else:
                summary = (
                    "The repository README contains a self-declared copy statement, but the closest known registry project "
                    f"is only {top_similarity:.4f} similar, so there is no strong evidence of cloning another catalogued project."
                )
        else:
            if locale == "ru":
                summary = (
                    "В README есть самоописание о копии репозитория, но среди известных проектов реестра "
                    "не найдено достаточно близкого совпадения для внешнего обвинения в клонировании."
                )
            else:
                summary = (
                    "The repository README contains a self-declared copy statement, but no matching registry project was "
                    "close enough to support an external clone claim."
                )
    else:
        summary = _locale_text(
            locale,
            en="No strong clone evidence was found against the current project registry.",
            ru="Сильных признаков клонирования относительно текущего реестра проектов не найдено.",
        )
    return {
        "clone_risk": clone_risk,
        "closest_projects": closest_projects[:3],
        "source_project": source_project,
        "top_similarity": round(top_similarity, 4),
        "self_declared_copy_hits": copy_hits,
        "self_declared_copy_excerpt": self_declared_copy_excerpt,
        "self_declared_copy_url": _github_readme_url(str(case.get("github_repo") or "")),
        "summary": summary,
    }


def _project_fact_items(case: Dict[str, Any], results: Optional[Dict[str, Any]] = None, locale: str = "en") -> List[Dict[str, str]]:
    locale = _normalize_locale(locale)
    github_repo = str(case.get("github_repo") or "")
    project_url = str(case.get("project_url") or "")
    telegram_handle = str(case.get("telegram_handle") or "")
    wallet_address = str(case.get("wallet_address") or "")
    items = []
    if github_repo:
        items.append(
            {
                "key": "github_repo",
                "label": _locale_text(locale, en="GitHub", ru="GitHub"),
                "value": github_repo,
                "url": _github_repo_url(github_repo),
            }
        )
    if project_url:
        items.append(
            {
                "key": "project_url",
                "label": _locale_text(locale, en="Website", ru="Сайт"),
                "value": project_url,
                "url": project_url,
            }
        )
    if telegram_handle:
        items.append(
            {
                "key": "telegram_handle",
                "label": _locale_text(locale, en="Telegram", ru="Telegram"),
                "value": f"@{telegram_handle.lstrip('@')}",
                "url": _telegram_url(telegram_handle),
            }
        )
    if wallet_address:
        items.append(
            {
                "key": "wallet_address",
                "label": _locale_text(locale, en="Contract", ru="Контракт"),
                "value": wallet_address,
                "url": _tonviewer_address_url(wallet_address),
            }
        )
    address_result = (results or {}).get("address_signal") or {}
    ton_activity = dict((address_result.get("data") or {}).get("ton_activity") or {})
    ton_mcp_data = dict((address_result.get("data") or {}).get("ton_mcp") or {})
    matched_jetton = dict(ton_mcp_data.get("matched_jetton") or {})
    reverse_dns = dict(ton_mcp_data.get("reverse_dns") or {})
    balance = dict(ton_mcp_data.get("balance") or {})
    dns_match = dict(ton_mcp_data.get("dns_match") or {})
    if ton_activity.get("status") == "success" and (
        ton_activity.get("last_tx_at") or int(ton_activity.get("tx_count_30d") or 0) > 0
    ):
        last_tx_age_days = -1 if ton_activity.get("last_tx_age_days") in (None, "") else int(ton_activity.get("last_tx_age_days"))
        tx_count_30d = int(ton_activity.get("tx_count_30d") or 0)
        items.append(
            {
                "key": "ton_onchain_activity",
                "label": _locale_text(locale, en="TON on-chain", ru="TON в блокчейне"),
                "value": _locale_text(
                    locale,
                    en=(
                        f"last tx {last_tx_age_days}d ago | {tx_count_30d} tx / 30d"
                        if last_tx_age_days >= 0
                        else f"{tx_count_30d} tx / 30d"
                    ),
                    ru=(
                        f"последний tx {last_tx_age_days} дн. назад | {tx_count_30d} tx / 30д"
                        if last_tx_age_days >= 0
                        else f"{tx_count_30d} tx / 30д"
                    ),
                ),
                "url": _tonviewer_address_url(str(ton_activity.get("address") or wallet_address)),
            }
        )
    if matched_jetton:
        jetton_symbol = str(matched_jetton.get("symbol") or "")
        jetton_name = str(matched_jetton.get("name") or "")
        jetton_address = str(matched_jetton.get("address") or "")
        items.append(
            {
                "key": "ton_mcp_known_jetton",
                "label": _locale_text(locale, en="TON MCP", ru="TON MCP"),
                "value": normalize_ws(" | ".join(item for item in [jetton_symbol, jetton_name, jetton_address] if item)),
                "url": _tonviewer_address_url(jetton_address),
            }
        )
    if reverse_dns.get("status") == "success" and reverse_dns.get("domain"):
        items.append(
            {
                "key": "ton_mcp_reverse_dns",
                "label": _locale_text(locale, en="TON DNS", ru="TON DNS"),
                "value": str(reverse_dns.get("domain") or ""),
                "url": "",
            }
        )
    if balance.get("status") == "success" and balance.get("balance"):
        items.append(
            {
                "key": "ton_mcp_balance",
                "label": _locale_text(locale, en="TON balance", ru="TON баланс"),
                "value": str(balance.get("balance") or ""),
                "url": _tonviewer_address_url(str(balance.get("address") or wallet_address)),
            }
        )
    if dns_match:
        items.append(
            {
                "key": "ton_mcp_dns_match",
                "label": _locale_text(locale, en="TON DNS match", ru="TON DNS совпадение"),
                "value": f"{str(dns_match.get('domain') or '')} -> {str(dns_match.get('address') or '')}",
                "url": "",
            }
        )
    return items


def _risk_evidence_items(
    case: Dict[str, Any],
    results: Dict[str, Any],
    clone_analysis: Dict[str, Any],
    locale: str = "en",
) -> List[Dict[str, str]]:
    locale = _normalize_locale(locale)
    items: List[Dict[str, str]] = []
    github_repo = str(case.get("github_repo") or "")
    telegram_handle = str(case.get("telegram_handle") or "")
    wallet_address = str(case.get("wallet_address") or "")
    identity_result = (results or {}).get("identity_confirmation") or {}
    identity_data = dict(identity_result.get("data") or {})
    identity_status = str(identity_data.get("identity_status") or "")
    evidence_status = str(identity_data.get("evidence_status") or "")
    source_failures = [str(item) for item in identity_data.get("source_failures") or []]
    brand_overlap = [str(item) for item in identity_data.get("brand_overlap_tokens") or []]
    corroborating_signals = [str(item) for item in identity_data.get("corroborating_signals") or []]
    localized_failures = _localized_identity_signal_list(source_failures, locale)
    localized_signals = _localized_identity_signal_list(corroborating_signals, locale)
    noncanonical_reference = str(identity_data.get("noncanonical_reference_domain") or "")
    if identity_status in {"weak", "mismatch", "incomplete"} or evidence_status == "incomplete":
        summary_en = f"Identity status: {identity_status or 'unknown'}; evidence status: {evidence_status or 'unknown'}."
        summary_ru = f"Статус принадлежности: {identity_status or 'unknown'}; статус доказательств: {evidence_status or 'unknown'}."
        if localized_failures:
            summary_en += f" Unavailable sources: {localized_failures}."
            summary_ru += f" Недоступные источники: {localized_failures}."
        if localized_signals:
            summary_en += f" Corroborating signals: {localized_signals}."
            summary_ru += f" Подтверждающие сигналы: {localized_signals}."
        if brand_overlap:
            summary_en += f" Brand overlap tokens: {', '.join(brand_overlap[:3])}."
            summary_ru += f" Совпадающие брендовые токены: {', '.join(brand_overlap[:3])}."
        if noncanonical_reference:
            summary_en += f" Primary reference still points to non-canonical domain {noncanonical_reference}."
            summary_ru += f" Основная ссылка всё ещё указывает на неканонический домен {noncanonical_reference}."
        items.append(
            {
                "key": "identity_confirmation",
                "label": _locale_text(locale, en="Identity review", ru="Проверка принадлежности проекту"),
                "summary": _locale_text(locale, en=summary_en, ru=summary_ru),
                "url": f"https://{str(identity_data.get('canonical_domain') or '')}" if identity_data.get("canonical_domain") else "",
            }
        )
    activity_result = (results or {}).get("github_activity") or {}
    activity_metrics = dict(activity_result.get("metrics") or {})
    last_commit_age_days = int(activity_metrics.get("last_commit_age_days") or -1)
    commits_90d = int(activity_metrics.get("commits_90d") or 0)
    if last_commit_age_days >= 0 and (last_commit_age_days >= 180 or commits_90d == 0):
        items.append(
            {
                "key": "repo_freshness",
                "label": _locale_text(locale, en="GitHub activity", ru="Активность GitHub"),
                "summary": _locale_text(
                    locale,
                    en=f"Last visible commit is {last_commit_age_days} days old; commits in the last 90 days: {commits_90d}.",
                    ru=f"Последний видимый коммит был {last_commit_age_days} дней назад; коммитов за последние 90 дней: {commits_90d}.",
                ),
                "url": _github_commits_url(github_repo) or _github_repo_url(github_repo),
            }
        )
    tg_result = (results or {}).get("telegram_channel") or {}
    tg_metrics = dict(tg_result.get("metrics") or {})
    last_post_age_days = int(tg_metrics.get("last_post_age_days") or -1)
    posts_30d = int(tg_metrics.get("posts_30d") or 0)
    community_activity_score = int(tg_metrics.get("community_activity_score") or 0)
    if telegram_handle and (last_post_age_days >= 30 or community_activity_score < 20):
        age_note_en = (
            f"Last public post is {last_post_age_days} days old"
            if last_post_age_days >= 0
            else "Last public post age could not be determined"
        )
        age_note_ru = (
            f"Последний публичный пост был {last_post_age_days} дней назад"
            if last_post_age_days >= 0
            else "Возраст последнего публичного поста определить не удалось"
        )
        items.append(
            {
                "key": "telegram_activity",
                "label": _locale_text(locale, en="Telegram activity", ru="Активность Telegram"),
                "summary": _locale_text(
                    locale,
                    en=f"{age_note_en}; posts in the last 30 days: {posts_30d}; community activity score: {community_activity_score}.",
                    ru=f"{age_note_ru}; постов за последние 30 дней: {posts_30d}; балл активности сообщества: {community_activity_score}.",
                ),
                "url": _telegram_url(telegram_handle),
            }
        )
    tg_semantics_result = (results or {}).get("telegram_semantics") or {}
    tg_semantics_metrics = dict(tg_semantics_result.get("metrics") or {})
    tg_semantics_flags = [str(item) for item in tg_semantics_result.get("flags") or []]
    semantic_risk_score = int(tg_semantics_metrics.get("semantic_risk_score") or 0)
    promo_ratio = float(tg_semantics_metrics.get("promo_post_ratio") or 0.0)
    if telegram_handle and (semantic_risk_score >= 30 or "telegram_feed_is_overly_promotional" in tg_semantics_flags):
        items.append(
            {
                "key": "telegram_feed_quality",
                "label": _locale_text(locale, en="Telegram content quality", ru="Качество Telegram-контента"),
                "summary": _locale_text(
                    locale,
                    en=f"Semantic risk score: {semantic_risk_score}; promotional post ratio: {promo_ratio:.2f}.",
                    ru=f"Семантический риск: {semantic_risk_score}; доля промо-постов: {promo_ratio:.2f}.",
                ),
                "url": _telegram_url(telegram_handle),
            }
        )
    if clone_analysis.get("source_project"):
        source = clone_analysis.get("source_project") or {}
        items.append(
            {
                "key": "clone_analysis",
                "label": _locale_text(locale, en="Originality review", ru="Проверка оригинальности"),
                "summary": _locale_text(
                    locale,
                    en=(
                        f"Closest supported source project: {source.get('name') or source.get('github_repo') or 'project'}; "
                        f"overall similarity: {clone_analysis.get('top_similarity')}; clone risk: {clone_analysis.get('clone_risk')}."
                    ),
                    ru=(
                        f"Ближайший подтверждённый источник: {source.get('name') or source.get('github_repo') or 'проект'}; "
                        f"общая схожесть: {clone_analysis.get('top_similarity')}; риск клона: {clone_analysis.get('clone_risk')}."
                    ),
                ),
                "url": str(source.get("url") or ""),
            }
        )
    elif clone_analysis.get("self_declared_copy_excerpt"):
        items.append(
            {
                "key": "repository_lineage_note",
                "label": _locale_text(locale, en="Repository lineage note", ru="Комментарий о происхождении репозитория"),
                "summary": _locale_text(
                    locale,
                    en=(
                        f"README note: {clone_analysis.get('self_declared_copy_excerpt')}. "
                        f"Closest registry similarity: {clone_analysis.get('top_similarity')}."
                    ),
                    ru=(
                        f"Фрагмент README: {clone_analysis.get('self_declared_copy_excerpt')}. "
                        f"Ближайшая схожесть с проектами реестра: {clone_analysis.get('top_similarity')}."
                    ),
                ),
                "url": str(clone_analysis.get("self_declared_copy_url") or ""),
            }
        )
    address_result = (results or {}).get("address_signal") or {}
    ton_activity = dict((address_result.get("data") or {}).get("ton_activity") or {})
    ton_mcp_data = dict((address_result.get("data") or {}).get("ton_mcp") or {})
    matched_jetton = dict(ton_mcp_data.get("matched_jetton") or {})
    reverse_dns = dict(ton_mcp_data.get("reverse_dns") or {})
    balance = dict(ton_mcp_data.get("balance") or {})
    dns_match = dict(ton_mcp_data.get("dns_match") or {})
    if ton_activity.get("status") == "success" and (
        ton_activity.get("last_tx_at") or int(ton_activity.get("tx_count_30d") or 0) > 0
    ):
        last_tx_age_days = -1 if ton_activity.get("last_tx_age_days") in (None, "") else int(ton_activity.get("last_tx_age_days"))
        tx_count_7d = int(ton_activity.get("tx_count_7d") or 0)
        tx_count_30d = int(ton_activity.get("tx_count_30d") or 0)
        items.append(
            {
                "key": "ton_onchain_activity",
                "label": _locale_text(locale, en="TON on-chain activity", ru="TON-активность в блокчейне"),
                "summary": _locale_text(
                    locale,
                    en=(
                        f"Latest on-chain transaction is {last_tx_age_days} days old; "
                        f"transactions in the last 7 days: {tx_count_7d}; in the last 30 days: {tx_count_30d}."
                    ),
                    ru=(
                        f"Последняя транзакция в блокчейне была {last_tx_age_days} дней назад; "
                        f"транзакций за последние 7 дней: {tx_count_7d}; за последние 30 дней: {tx_count_30d}."
                    ),
                ),
                "url": _tonviewer_address_url(str(ton_activity.get("address") or wallet_address)),
            }
        )
    if matched_jetton:
        jetton_symbol = str(matched_jetton.get("symbol") or matched_jetton.get("name") or "jetton")
        jetton_name = str(matched_jetton.get("name") or jetton_symbol)
        jetton_address = str(matched_jetton.get("address") or "")
        items.append(
            {
                "key": "ton_mcp_known_jetton",
                "label": _locale_text(locale, en="TON MCP verification", ru="Проверка TON MCP"),
                "summary": _locale_text(
                    locale,
                    en=f"TON MCP matched the project address to the known jetton {jetton_name} ({jetton_symbol}).",
                    ru=f"TON MCP сопоставил адрес проекта с известным jetton {jetton_name} ({jetton_symbol}).",
                ),
                "url": _tonviewer_address_url(jetton_address),
            }
        )
    if reverse_dns.get("status") == "success" and reverse_dns.get("domain"):
        items.append(
            {
                "key": "ton_mcp_reverse_dns",
                "label": _locale_text(locale, en="TON DNS reverse lookup", ru="Обратный поиск TON DNS"),
                "summary": _locale_text(
                    locale,
                    en=f"TON MCP reverse-resolved the project address to {str(reverse_dns.get('domain') or '')}.",
                    ru=f"TON MCP выполнил обратное разрешение адреса проекта в домен {str(reverse_dns.get('domain') or '')}.",
                ),
                "url": _tonviewer_address_url(str(reverse_dns.get("address") or "")),
            }
        )
    if balance.get("status") == "success" and balance.get("balance"):
        items.append(
            {
                "key": "ton_mcp_balance",
                "label": _locale_text(locale, en="TON balance check", ru="Проверка TON-баланса"),
                "summary": _locale_text(
                    locale,
                    en=f"TON MCP returned on-chain balance {str(balance.get('balance') or '')} for the project address.",
                    ru=f"TON MCP вернул баланс в блокчейне {str(balance.get('balance') or '')} для адреса проекта.",
                ),
                "url": _tonviewer_address_url(str(balance.get("address") or "")),
            }
        )
    if dns_match:
        items.append(
            {
                "key": "ton_mcp_dns_match",
                "label": _locale_text(locale, en="TON DNS verification", ru="TON DNS-проверка"),
                "summary": _locale_text(
                    locale,
                    en=f"TON MCP resolved {str(dns_match.get('domain') or '')} to the detected project address.",
                    ru=f"TON MCP разрешил {str(dns_match.get('domain') or '')} в найденный адрес проекта.",
                ),
                "url": _tonviewer_address_url(str(dns_match.get("address") or "")),
            }
        )
    contract_result = (results or {}).get("contract_validator") or {}
    contract_metrics = dict(contract_result.get("metrics") or {})
    contract_data = dict(contract_result.get("data") or {})
    contract_flags = [str(item) for item in contract_result.get("flags") or []]
    detected_addresses = [str(item) for item in contract_data.get("addresses") or [] if str(item or "").strip()]
    contract_address = wallet_address or (detected_addresses[0] if detected_addresses else "")
    contract_score = int(contract_metrics.get("contract_score") or 0)
    contract_file_count = int(contract_metrics.get("contract_file_count") or 0)
    address_signal_count = int(contract_metrics.get("address_signal_count") or 0)
    if str(contract_result.get("status") or "") == "success" and (
        contract_address or contract_file_count or address_signal_count or contract_flags
    ):
        summary_en = (
            f"Contract score: {contract_score}; contract-like files: {contract_file_count}; "
            f"resolved addresses: {address_signal_count}."
        )
        summary_ru = (
            f"Балл контрактной части: {contract_score}; контрактоподобных файлов: {contract_file_count}; "
            f"найденных адресов: {address_signal_count}."
        )
        if "missing_contract_files" in contract_flags:
            summary_en += " Required contract files were not found."
            summary_ru += " Обязательные контрактные файлы не найдены."
        if "missing_address_signal" in contract_flags:
            summary_en += " A public contract address was not resolved."
            summary_ru += " Публичный адрес контракта не определён."
        items.append(
            {
                "key": "contract_health",
                "label": _locale_text(locale, en="Contract signal", ru="Сигнал по контракту"),
                "summary": _locale_text(locale, en=summary_en, ru=summary_ru),
                "url": _tonviewer_address_url(contract_address),
            }
        )
    return items


def _compose_final_explanation(
    case: Dict[str, Any],
    results: Dict[str, Any],
    clone_analysis: Dict[str, Any],
    locale: str = "en",
) -> str:
    locale = _normalize_locale(locale)
    llm_explainer = (results or {}).get("llm_explainer") or {}
    llm_data = dict(llm_explainer.get("data") or {})
    llm_explanation = _llm_localized_field(llm_data, "explanation_text", locale)
    if llm_explanation:
        return llm_explanation
    rule_engine = (results or {}).get("rule_engine") or {}
    rule_data = dict(rule_engine.get("data") or {})
    rule_metrics = dict(rule_engine.get("metrics") or {})
    overall_score = rule_metrics.get("overall_score")
    risk_level = str(rule_data.get("risk_level") or "unknown")
    localized_risk_level = _localized_explanation_token(risk_level, locale)
    identity_status = str(rule_data.get("identity_status") or "unknown")
    evidence_status = str(rule_data.get("evidence_status") or "partial")
    identity_result = (results or {}).get("identity_confirmation") or {}
    identity_data = dict(identity_result.get("data") or {})
    parts: List[str] = []
    if overall_score not in (None, ""):
        parts.append(
            _locale_text(
                locale,
                en=f"Overall score: {overall_score}. Overall risk level: {localized_risk_level}.",
                ru=f"Итоговый балл: {overall_score}. Общий уровень риска: {localized_risk_level}.",
            )
        )
    if identity_status == "confirmed":
        corroborating_signals = [str(item) for item in identity_data.get("corroborating_signals") or []]
        localized_signals = _localized_identity_signal_list(corroborating_signals, locale)
        canonical_domain = str(identity_data.get("canonical_domain") or "")
        parts.append(
            _locale_text(
                locale,
                en=(
                    f"Identity confirmation status is confirmed"
                    + (f"; canonical domain: {canonical_domain}" if canonical_domain else "")
                    + (f"; corroborating signals: {localized_signals}." if localized_signals else ".")
                ),
                ru=(
                    f"Статус проверки принадлежности проекту: подтверждено"
                    + (f"; канонический домен: {canonical_domain}" if canonical_domain else "")
                    + (f"; подтверждающие сигналы: {localized_signals}." if localized_signals else ".")
                ),
            )
        )
    elif identity_status == "mismatch":
        parts.append(
            _locale_text(
                locale,
                en="Identity status is mismatch: the selected candidate does not align with the requested project brand.",
                ru="Статус принадлежности: mismatch. Выбранный кандидат не совпадает с брендом проекта из запроса.",
            )
        )
    elif identity_status in {"weak", "incomplete"}:
        source_failures = [str(item) for item in identity_data.get("source_failures") or []]
        localized_failures = _localized_identity_signal_list(source_failures, locale)
        parts.append(
            _locale_text(
                locale,
                en=(
                    f"Identity status is {identity_status}."
                    + (f" Unavailable sources: {localized_failures}." if localized_failures else "")
                ),
                ru=(
                    f"Статус принадлежности: {identity_status}."
                    + (f" Недоступные источники: {localized_failures}." if localized_failures else "")
                ),
            )
        )
    if evidence_status == "incomplete" and identity_status != "mismatch":
        parts.append(
            _locale_text(
                locale,
                en="Part of the external evidence is incomplete, so missing source data should not be interpreted as a direct project weakness.",
                ru="Часть внешних доказательств неполная, поэтому отсутствие данных источника нельзя напрямую считать слабостью самого проекта.",
            )
        )
    activity_result = (results or {}).get("github_activity") or {}
    activity_metrics = dict(activity_result.get("metrics") or {})
    last_commit_age_days = int(activity_metrics.get("last_commit_age_days") or -1)
    commits_90d = int(activity_metrics.get("commits_90d") or 0)
    if last_commit_age_days >= 365:
        parts.append(
            _locale_text(
                locale,
                en=f"GitHub metrics: last visible commit is {last_commit_age_days} days old; commits in the last 90 days: {commits_90d}.",
                ru=f"Метрики GitHub: последний видимый коммит был {last_commit_age_days} дней назад; коммитов за последние 90 дней: {commits_90d}.",
            )
        )
    elif last_commit_age_days >= 180:
        parts.append(
            _locale_text(
                locale,
                en=f"GitHub metrics: last visible commit is {last_commit_age_days} days old.",
                ru=f"Метрики GitHub: последний видимый коммит был {last_commit_age_days} дней назад.",
            )
        )
    if clone_analysis.get("source_project"):
        source = clone_analysis.get("source_project") or {}
        parts.append(
            _locale_text(
                locale,
                en=f"Originality evidence: closest supported match is {source.get('name') or source.get('github_repo') or 'project'} with similarity {clone_analysis.get('top_similarity')} and clone risk {clone_analysis.get('clone_risk')}.",
                ru=f"Данные по оригинальности: ближайшее подтверждённое совпадение — {source.get('name') or source.get('github_repo') or 'проект'}, схожесть {clone_analysis.get('top_similarity')}, риск клона {clone_analysis.get('clone_risk')}.",
            )
        )
    elif clone_analysis.get("self_declared_copy_excerpt"):
        parts.append(
            _locale_text(
                locale,
                en=f"Repository lineage note in README: {clone_analysis.get('self_declared_copy_excerpt')}.",
                ru=f"В README найдено примечание о происхождении репозитория: {clone_analysis.get('self_declared_copy_excerpt')}.",
            )
        )
    address_result = (results or {}).get("address_signal") or {}
    ton_activity = dict((address_result.get("data") or {}).get("ton_activity") or {})
    ton_mcp_data = dict((address_result.get("data") or {}).get("ton_mcp") or {})
    matched_jetton = dict(ton_mcp_data.get("matched_jetton") or {})
    dns_match = dict(ton_mcp_data.get("dns_match") or {})
    if matched_jetton:
        parts.append(
            _locale_text(
                locale,
                en=(
                    f"TON MCP confirmed that the detected project address matches the known jetton "
                    f"{matched_jetton.get('name') or matched_jetton.get('symbol') or 'jetton'}."
                ),
                ru=(
                    f"TON MCP подтвердил, что найденный адрес проекта совпадает с известным jetton "
                    f"{matched_jetton.get('name') or matched_jetton.get('symbol') or 'jetton'}."
                ),
            )
        )
    elif dns_match:
        parts.append(
            _locale_text(
                locale,
                en=f"TON MCP confirmed that {str(dns_match.get('domain') or 'the TON DNS domain')} resolves to the detected project address.",
                ru=f"TON MCP подтвердил, что {str(dns_match.get('domain') or 'TON DNS домен')} разрешается в найденный адрес проекта.",
            )
        )
    if ton_activity.get("status") == "success":
        last_tx_age_days = -1 if ton_activity.get("last_tx_age_days") in (None, "") else int(ton_activity.get("last_tx_age_days"))
        tx_count_30d = int(ton_activity.get("tx_count_30d") or 0)
        if last_tx_age_days >= 0 and tx_count_30d > 0:
            parts.append(
                _locale_text(
                    locale,
                    en=(
                        f"The project address still shows on-chain movement: the latest transaction is "
                        f"{last_tx_age_days} days old and the last 30 days include {tx_count_30d} transactions."
                    ),
                    ru=(
                        f"Адрес проекта всё ещё показывает движение в блокчейне: последняя транзакция была "
                        f"{last_tx_age_days} дней назад, а за последние 30 дней прошло {tx_count_30d} транзакций."
                    ),
                )
            )
        elif last_tx_age_days >= 180:
            parts.append(
                _locale_text(
                    locale,
                    en=f"On-chain metrics: latest visible transaction is {last_tx_age_days} days old.",
                    ru=f"On-chain метрики: последняя видимая транзакция была {last_tx_age_days} дней назад.",
                )
            )
        elif last_tx_age_days >= 30:
            parts.append(
                _locale_text(
                    locale,
                    en=f"On-chain metrics: latest visible transaction is {last_tx_age_days} days old.",
                    ru=f"On-chain метрики: последняя видимая транзакция была {last_tx_age_days} дней назад.",
                )
            )
    tg_result = (results or {}).get("telegram_channel") or {}
    tg_metrics = dict(tg_result.get("metrics") or {})
    last_post_age_days = int(tg_metrics.get("last_post_age_days") or -1)
    posts_30d = int(tg_metrics.get("posts_30d") or 0)
    community_activity_score = int(tg_metrics.get("community_activity_score") or 0)
    if last_post_age_days >= 30:
        parts.append(
            _locale_text(
                locale,
                en=f"Telegram metrics: last public post is {last_post_age_days} days old; posts in the last 30 days: {posts_30d}; activity score: {community_activity_score}.",
                ru=f"Метрики Telegram: последний публичный пост был {last_post_age_days} дней назад; постов за последние 30 дней: {posts_30d}; балл активности: {community_activity_score}.",
            )
        )
    tg_semantics_result = (results or {}).get("telegram_semantics") or {}
    tg_semantics_metrics = dict(tg_semantics_result.get("metrics") or {})
    promo_ratio = float(tg_semantics_metrics.get("promo_post_ratio") or 0.0)
    semantic_risk_score = int(tg_semantics_metrics.get("semantic_risk_score") or 0)
    if semantic_risk_score >= 30:
        parts.append(
            _locale_text(
                locale,
                en=f"Telegram semantic metrics: semantic risk score {semantic_risk_score}; promotional post ratio {promo_ratio:.2f}.",
                ru=f"Семантические метрики Telegram: риск {semantic_risk_score}; доля промо-постов {promo_ratio:.2f}.",
            )
        )
    contract_result = (results or {}).get("contract_validator") or {}
    contract_metrics = dict(contract_result.get("metrics") or {})
    contract_flags = [str(item) for item in contract_result.get("flags") or []]
    contract_score = int(contract_metrics.get("contract_score") or 0)
    if str(contract_result.get("status") or "") == "success" and (
        contract_score < 50 or "missing_contract_files" in contract_flags or "missing_address_signal" in contract_flags
    ):
        parts.append(
            _locale_text(
                locale,
                en=f"Contract metrics: contract score {contract_score}; flags: {', '.join(contract_flags) or 'none'}.",
                ru=f"Метрики контрактной части: балл {contract_score}; флаги: {', '.join(contract_flags) or 'нет'}.",
            )
        )
    return " ".join(parts)


def _meaningful_project_type(value: Any) -> str:
    project_type = normalize_ws(str(value or ""))
    if not project_type or project_type.lower() == "unknown":
        return ""
    return project_type


EXPLANATION_TOKEN_LABELS = {
    "low": {"en": "low", "ru": "низкий"},
    "moderate": {"en": "moderate", "ru": "умеренный"},
    "high": {"en": "high", "ru": "высокий"},
    "unknown": {"en": "unknown", "ru": "неизвестный"},
}

IDENTITY_SIGNAL_LABELS = {
    "canonical_domain": {"en": "official website", "ru": "официальный сайт"},
    "github_repo": {"en": "GitHub repository", "ru": "GitHub-репозиторий"},
    "telegram_channel": {"en": "Telegram channel", "ru": "Telegram-канал"},
    "telegram_reference": {"en": "Telegram entrypoint", "ru": "Telegram-вход проекта"},
    "wallet_signal": {"en": "TON address evidence", "ru": "TON-адрес и сигналы из блокчейна"},
}


def _localized_explanation_token(value: Any, locale: str) -> str:
    normalized = normalize_ws(str(value or "")).lower()
    if not normalized:
        return ""
    mapping = EXPLANATION_TOKEN_LABELS.get(normalized) or {}
    return str(mapping.get(locale) or mapping.get("en") or normalized)


def _localized_identity_signal(value: Any, locale: str) -> str:
    normalized = normalize_ws(str(value or ""))
    if not normalized:
        return ""
    mapping = IDENTITY_SIGNAL_LABELS.get(normalized) or {}
    return str(mapping.get(locale) or mapping.get("en") or normalized)


def _localized_identity_signal_list(values: Iterable[Any], locale: str, limit: int = 3) -> str:
    labels = []
    seen = set()
    for value in values:
        label = _localized_identity_signal(value, locale)
        key = label.lower()
        if not label or key in seen:
            continue
        seen.add(key)
        labels.append(label)
        if len(labels) >= limit:
            break
    return ", ".join(labels)


PROJECT_TYPE_OVERVIEW_LABELS = {
    "wallet_app": {"en": "a wallet application on TON", "ru": "кошелёк на TON"},
    "tooling_sdk": {"en": "a TON SDK or developer tooling project", "ru": "SDK или набор инструментов для TON"},
    "tooling_api": {"en": "a TON API or developer tooling service", "ru": "API или инструментальный сервис для TON"},
    "protocol_infra": {"en": "TON infrastructure or protocol software", "ru": "протокольная или инфраструктурная часть TON"},
    "protocol_service": {"en": "a protocol service on TON", "ru": "протокольный сервис на TON"},
    "dapp_product": {"en": "a TON application or protocol product", "ru": "продуктовый dApp или протокол на TON"},
    "smart_contracts": {"en": "a smart-contract project on TON", "ru": "смарт-контрактный проект на TON"},
    "dex": {"en": "a decentralized exchange on TON", "ru": "децентрализованная биржа на TON"},
    "derivatives_dex": {"en": "a derivatives trading protocol on TON", "ru": "деривативный торговый протокол на TON"},
    "staking_protocol": {"en": "a staking protocol on TON", "ru": "стейкинг-протокол на TON"},
    "explorer": {"en": "an explorer or analytics product for TON", "ru": "обозреватель или аналитический сервис для TON"},
    "nft_collection": {"en": "an NFT collection on TON", "ru": "NFT-коллекция на TON"},
    "nft_marketplace": {"en": "an NFT marketplace on TON", "ru": "NFT-маркетплейс на TON"},
    "gamefi": {"en": "a GameFi project on TON", "ru": "GameFi-проект на TON"},
    "token": {"en": "a token or jetton project on TON", "ru": "токен или jetton-проект на TON"},
    "meme": {"en": "a meme token project on TON", "ru": "мем-токен на TON"},
}

GENERIC_OVERVIEW_FEATURES = {
    "ton",
    "project",
    "app",
    "web",
    "telegram",
    "github",
    "repo",
    "repository",
    "open",
    "source",
    "blockchain",
}


def _text_matches_locale(text: Any, locale: str) -> bool:
    raw = normalize_ws(str(text or ""))
    if not raw:
        return False
    latin_count = len(re.findall(r"[A-Za-z]", raw))
    cyrillic_count = len(re.findall(r"[А-Яа-яЁё]", raw))
    locale = _normalize_locale(locale)
    if locale == "ru" and latin_count >= 8 and latin_count > cyrillic_count * 2:
        return False
    if locale == "en" and cyrillic_count >= 8 and cyrillic_count > latin_count * 2:
        return False
    return True


def _display_project_name(case: Dict[str, Any], discovery: Optional[Dict[str, Any]] = None) -> str:
    selected_candidate = _selected_discovery_candidate(discovery or {}) if discovery else {}
    candidates = [
        selected_candidate.get("name"),
        case.get("name"),
        case.get("github_repo"),
        _url_domain(str(case.get("project_url") or "")),
        case.get("project_url"),
    ]
    for value in candidates:
        raw = normalize_ws(str(value or ""))
        if not raw:
            continue
        if " | " in raw:
            raw = normalize_ws(raw.split(" | ", 1)[0])
        if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", raw):
            raw = raw.split("/", 1)[1]
        if raw:
            return raw
    return ""


def _normalized_project_type_for_overview(project_type: str) -> str:
    normalized = normalize_ws(str(project_type or "")).lower().replace("-", "_").replace(" ", "_")
    if "gamefi" in normalized or "gaming" in normalized:
        return "gamefi"
    return project_type


def _overview_sentence(text: Any, limit: int = 220) -> str:
    raw = strip_html(normalize_ws(str(text or "")))
    if not raw:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", raw)
    for part in parts:
        sentence = normalize_ws(part)
        if len(sentence) < 24:
            continue
        return clip_text(sentence.rstrip(". "), limit).strip()
    return clip_text(raw.rstrip(". "), limit).strip()


def _project_type_overview_label(project_type: str, locale: str) -> str:
    project_type = _normalized_project_type_for_overview(project_type)
    mapping = PROJECT_TYPE_OVERVIEW_LABELS.get(project_type) or {}
    localized = str(mapping.get(locale) or mapping.get("en") or "")
    if localized:
        return localized
    if not project_type or project_type == "unknown":
        return ""
    return normalize_ws(project_type.replace("_", " ").replace("-", " "))


def _llm_localized_field(payload: Dict[str, Any], base_key: str, locale: str) -> str:
    locale = _normalize_locale(locale)
    candidates = [
        normalize_ws(str(payload.get(f"{base_key}_{locale}") or "")),
        normalize_ws(str(payload.get(base_key) or "")),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        for raw_signal in ("canonical_domain", "github_repo", "telegram_channel", "wallet_signal"):
            candidate = re.sub(rf"\b{raw_signal}\b", _localized_identity_signal(raw_signal, locale), candidate)
        if _text_matches_locale(candidate, locale):
            return candidate
    return ""


def _compose_project_overview(
    case: Dict[str, Any],
    results: Dict[str, Any],
    *,
    discovery: Optional[Dict[str, Any]] = None,
    locale: str = "en",
) -> str:
    locale = _normalize_locale(locale)
    project_name = _display_project_name(case, discovery)
    if not project_name:
        project_name = _locale_text(locale, en="This project", ru="Этот проект")
    repo_result = (results or {}).get("github_repo") or {}
    repo_data = dict(repo_result.get("data") or {})
    repo_meta = dict(repo_data.get("repo") or {})
    raw_repo_description = normalize_ws(str(repo_meta.get("description") or ""))
    repo_description = raw_repo_description
    if not _text_matches_locale(repo_description, locale):
        repo_description = ""
    raw_readme_sentence = _overview_sentence(repo_data.get("readme_excerpt") or "")
    readme_sentence = raw_readme_sentence
    if not _text_matches_locale(readme_sentence, locale):
        readme_sentence = ""
    selected_candidate = _selected_discovery_candidate(discovery or {}) if discovery else {}
    candidate_description = _overview_sentence(selected_candidate.get("description") or "", limit=260)
    if not _text_matches_locale(candidate_description, locale):
        candidate_description = ""
    source_text = repo_description or readme_sentence or candidate_description
    llm_explainer = (results or {}).get("llm_explainer") or {}
    llm_data = dict(llm_explainer.get("data") or {})
    llm_overview = _llm_localized_field(llm_data, "project_overview", locale)
    source_word_count = len(re.findall(r"[A-Za-zА-Яа-я0-9]{3,}", source_text))
    rich_primary_description = bool(raw_repo_description and len(raw_repo_description) >= 40) or bool(raw_readme_sentence and len(raw_readme_sentence) >= 80)
    if llm_overview and (rich_primary_description or source_word_count >= 10):
        return llm_overview
    if source_text:
        if locale == "ru":
            return normalize_ws(f"{project_name} — {source_text.rstrip('. ')}.")
        return normalize_ws(f"{project_name} is {source_text.rstrip('. ')}.")
    if llm_overview:
        project_type = _best_project_type(case, llm_data.get("project_type"), ((results or {}).get("rule_engine") or {}).get("data", {}).get("project_type"))
        project_type_label = _project_type_overview_label(project_type, locale)
        if project_type_label:
            if locale == "ru":
                return normalize_ws(f"{project_name} — {project_type_label}.")
            return normalize_ws(f"{project_name} is {project_type_label}.")
    return _locale_text(
        locale,
        en="Insufficient grounded public evidence to build a project overview.",
        ru="Не удалось собрать достаточно подтвержденных публичных данных для обзора проекта.",
    )


def _best_project_type(case: Dict[str, Any], *values: Any) -> str:
    for value in values:
        resolved = _meaningful_project_type(value)
        if resolved:
            return resolved
    for value in values:
        raw_value = normalize_ws(str(value or ""))
        if raw_value:
            return raw_value
    return "unknown"


def _final_result_payload(
    case: Dict[str, Any],
    results: Dict[str, Any],
    discovery: Optional[Dict[str, Any]] = None,
    locale: str = "en",
) -> Dict[str, Any]:
    locale = _normalize_locale(locale)
    rule_engine = (results or {}).get("rule_engine") or {}
    llm_explainer = (results or {}).get("llm_explainer") or {}
    rule_data = dict(rule_engine.get("data") or {})
    rule_metrics = dict(rule_engine.get("metrics") or {})
    llm_data = dict(llm_explainer.get("data") or {})
    clone_analysis = _clone_analysis(case, results, locale=locale)
    closest_projects = list(clone_analysis.get("closest_projects") or [])
    status = str(llm_explainer.get("status") or "pending")
    metrics = {
        "overall_score": llm_data.get("overall_score", rule_metrics.get("overall_score")),
        "activity_score": llm_data.get("activity_score"),
        "originality_score": llm_data.get("originality_score"),
        "community_activity_score": llm_data.get("community_activity_score"),
        "community_quality_score": llm_data.get("community_quality_score"),
        "identity_score": llm_data.get("identity_score", rule_metrics.get("identity_score")),
        "last_commit_age_days": llm_data.get("last_commit_age_days", (((results or {}).get("github_activity") or {}).get("metrics") or {}).get("last_commit_age_days")),
        "onchain_tx_count_30d": llm_data.get("onchain_tx_count_30d", rule_metrics.get("onchain_tx_count_30d")),
        "last_onchain_tx_age_days": llm_data.get("last_onchain_tx_age_days", rule_metrics.get("last_onchain_tx_age_days")),
    }
    metrics = {key: value for key, value in metrics.items() if value not in (None, "")}
    clone_risk = str(clone_analysis.get("clone_risk") or rule_data.get("clone_risk") or "unknown")
    overall_score = llm_data.get("overall_score", rule_metrics.get("overall_score"))
    risk_level = str(llm_data.get("risk_level") or rule_data.get("risk_level") or "")
    identity_status = str(llm_data.get("identity_status") or rule_data.get("identity_status") or "")
    evidence_status = str(llm_data.get("evidence_status") or rule_data.get("evidence_status") or "")
    project_type = _best_project_type(case, llm_data.get("project_type"), rule_data.get("project_type"))
    summary = (
        f"type={project_type} "
        f"overall={overall_score} risk={risk_level} identity={identity_status or 'unknown'} clone={clone_risk}"
    )
    project_overview_text = _compose_project_overview(case, results, discovery=discovery or {}, locale=locale)
    explanation_text = _compose_final_explanation(case, results, clone_analysis, locale=locale)
    return {
        "status": status,
        "summary": summary,
        "project_overview_text": project_overview_text,
        "explanation_text": explanation_text,
        "project_name": str(llm_data.get("project_name") or _display_project_name(case, discovery) or ""),
        "project_type": project_type,
        "overall_score": overall_score,
        "risk_level": risk_level,
        "clone_risk": clone_risk,
        "identity_status": identity_status,
        "evidence_status": evidence_status,
        "needs_human_review": bool(rule_engine.get("needs_human_review") or llm_explainer.get("needs_human_review") or False),
        "strengths": _dedupe_text_list(llm_data.get("strengths") or rule_data.get("strengths") or []),
        "risks": _dedupe_text_list(llm_data.get("risks") or rule_data.get("risks") or []),
        "next_checks": _dedupe_text_list(llm_data.get("next_checks") or rule_data.get("next_checks") or []),
        "flags": _dedupe_text_list(list(rule_engine.get("flags") or []) + list(llm_explainer.get("flags") or [])),
        "metrics": metrics,
        "closest_projects": closest_projects[:3],
        "facts": _project_fact_items(case, results, locale=locale),
        "risk_evidence": _risk_evidence_items(case, results, clone_analysis, locale=locale),
        "clone_analysis": clone_analysis,
    }


def _runtime_status(statuses: Iterable[str]) -> str:
    ordered = [str(item or "pending") for item in statuses]
    if not ordered:
        return "pending"
    if any(item == "error" for item in ordered):
        return "error"
    if any(item == "running" for item in ordered):
        return "running"
    if any(item == "pending" for item in ordered):
        if any(item == "success" for item in ordered):
            return "running"
        return "pending"
    if all(item == "skipped" for item in ordered):
        return "skipped"
    if any(item == "success" for item in ordered):
        return "success"
    return ordered[0]


def _overview_from_results(case: Dict[str, Any], results: Dict[str, Any]) -> Dict[str, Any]:
    rule_engine = (results or {}).get("rule_engine") or {}
    rule_metrics = rule_engine.get("metrics") or {}
    rule_data = rule_engine.get("data") or {}
    clone_analysis = _clone_analysis(case, results)
    return {
        "overall_score": rule_metrics.get("overall_score"),
        "risk_level": rule_data.get("risk_level"),
        "project_type": _best_project_type(case, rule_data.get("project_type")),
        "clone_risk": clone_analysis.get("clone_risk"),
        "strengths": rule_data.get("strengths") or [],
        "risks": rule_data.get("risks") or [],
    }


def _first_error_context(
    workflow: Dict[str, Any],
    *,
    stage_id_override: str = "",
    stage_index_override: Optional[int] = None,
    parent_unit_id: str = "",
    parent_unit_name: str = "",
) -> Dict[str, Any]:
    unit_map = {str(unit.get("unit_id") or ""): unit for unit in workflow.get("units") or []}
    for stage in workflow.get("stages") or []:
        stage_id = stage_id_override or str(stage.get("stage_id") or "")
        stage_index = stage_index_override if stage_index_override is not None else int(stage.get("index") or 0)
        for unit_id in stage.get("unit_ids") or []:
            unit = unit_map.get(str(unit_id)) or {}
            if not unit:
                continue
            if unit.get("unit_type") == "composite":
                nested = _first_error_context(
                    unit.get("plan") or {},
                    stage_id_override=stage_id,
                    stage_index_override=stage_index,
                    parent_unit_id=str(unit.get("unit_id") or ""),
                    parent_unit_name=str(unit.get("name") or unit.get("unit_id") or ""),
                )
                if nested:
                    return nested
            runtime = unit.get("runtime") or {}
            if str(runtime.get("status") or "") != "error":
                continue
            result = unit.get("result") or {}
            return {
                "stage_id": stage_id,
                "stage_index": stage_index,
                "unit_id": str(unit.get("unit_id") or ""),
                "unit_name": str(unit.get("name") or unit.get("unit_id") or "unit"),
                "parent_unit_id": parent_unit_id,
                "parent_unit_name": parent_unit_name,
                "summary": str(result.get("summary") or "Unit failed"),
                "flags": [str(item) for item in result.get("flags") or []],
            }
    return {}


def _default_unit_id(workflow: Dict[str, Any]) -> str:
    error_context = _first_error_context(workflow)
    if error_context.get("unit_id"):
        return str(error_context.get("unit_id") or "")
    unit_ids = _collect_unit_ids(workflow)
    return "rule_engine" if "rule_engine" in unit_ids else ""


def _workflow_run_status(workflow: Dict[str, Any]) -> str:
    stage_statuses = [str((stage.get("runtime") or {}).get("status") or "pending") for stage in workflow.get("stages") or []]
    return _runtime_status(stage_statuses)


def _build_run_payload_from_workflow(
    run_id: str,
    root_dir: str,
    case: Dict[str, Any],
    options: Dict[str, Any],
    workflow: Dict[str, Any],
    results: Dict[str, Any],
    discovery: Optional[Dict[str, Any]] = None,
    *,
    run_status: str,
    is_live: bool,
    updated_at: int,
    project_key: str,
    fatal_error: str = "",
    error_workflow: Optional[Dict[str, Any]] = None,
    locale: str = "en",
) -> Dict[str, Any]:
    error_context = _first_error_context(error_workflow or workflow)
    if fatal_error:
        error_context = {
            **error_context,
            "summary": str(error_context.get("summary") or fatal_error),
            "fatal_error": fatal_error,
        }
    return {
        "run": {
            "run_id": run_id,
            "root_dir": root_dir,
            "status": run_status,
            "is_live": is_live,
            "updated_at": updated_at,
            "project_key": project_key,
            "error": error_context,
        },
        "case": case,
        "options": options,
        "overview": _overview_from_results(case, results),
        "result": _final_result_payload(case, results, discovery=discovery or {}, locale=locale),
        "presentation": workflow.get("presentation") or {},
        "workflow": workflow,
        "default_unit_id": _default_unit_id(workflow),
    }


def _build_discovery_payload(
    *,
    session_id: str,
    query: str,
    workflow: Dict[str, Any],
    summary: str,
    candidates: List[Dict[str, Any]],
    source_statuses: Dict[str, Dict[str, Any]],
    selected_candidate_key: str,
    status: str,
    updated_at: int,
    fatal_error: str = "",
) -> Dict[str, Any]:
    error = {"summary": fatal_error} if fatal_error else {}
    return {
        "session": {
            "session_id": session_id,
            "status": status,
            "updated_at": updated_at,
            "error": error,
        },
        "query": query,
        "summary": summary,
        "selected_candidate_key": selected_candidate_key,
        "candidates": candidates,
        "source_statuses": source_statuses,
        "workflow": workflow,
        "default_unit_id": _default_unit_id(workflow),
    }


def _build_run_list_item(
    run_id: str,
    root_dir: str,
    case: Dict[str, Any],
    options: Dict[str, Any],
    workflow: Dict[str, Any],
    results: Dict[str, Any],
    *,
    mtime: int,
    is_live: bool,
    run_status: str,
) -> Dict[str, Any]:
    overview = _overview_from_results(case, results)
    return {
        "run_id": run_id,
        "root_dir": root_dir,
        "case_id": str(case.get("case_id") or ""),
        "case_name": str(case.get("name") or run_id),
        "mode": str(options.get("mode") or ""),
        "mtime": mtime,
        "overall_score": overview.get("overall_score"),
        "risk_level": overview.get("risk_level"),
        "stage_count": len(workflow.get("stages") or []),
        "project_key": _project_key_from_case(case),
        "status": run_status,
        "is_live": is_live,
    }


def _materialize_atomic_runtime(unit: Dict[str, Any], runtime: Dict[str, Any], now_ts: float) -> Dict[str, Any]:
    payload = dict(runtime or {})
    status = str(payload.get("status") or "pending")
    started_at = payload.get("started_at")
    duration_ms = int(payload.get("duration_ms") or 0)
    if status == "running" and started_at:
        duration_ms = max(duration_ms, int((now_ts - float(started_at)) * 1000))
    return {
        "status": status,
        "duration_ms": duration_ms,
        "unit_type": unit.get("unit_type"),
        "execution_mode": unit.get("execution_mode"),
    }


def _hydrate_live_workflow(
    workflow_template: Dict[str, Any],
    atomic_runtimes: Dict[str, Dict[str, Any]],
    atomic_results: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    plan = copy.deepcopy(workflow_template)
    now_ts = time.time()
    unit_map: Dict[str, Dict[str, Any]] = {}
    for unit in plan.get("units") or []:
        unit_id = str(unit.get("unit_id") or "")
        if unit.get("unit_type") == "composite":
            unit["plan"] = _hydrate_live_workflow(unit.get("plan") or {}, atomic_runtimes, atomic_results)
            child_units = (unit.get("plan") or {}).get("units") or []
            child_statuses = [str((child.get("runtime") or {}).get("status") or "pending") for child in child_units]
            child_durations = [int((child.get("runtime") or {}).get("duration_ms") or 0) for child in child_units]
            unit["runtime"] = {
                "status": _runtime_status(child_statuses),
                "duration_ms": sum(child_durations),
                "unit_type": unit.get("unit_type"),
                "execution_mode": unit.get("execution_mode"),
            }
            unit["result"] = {}
        else:
            unit["runtime"] = _materialize_atomic_runtime(unit, atomic_runtimes.get(unit_id) or {}, now_ts)
            unit["result"] = _result_summary_payload(atomic_results.get(unit_id) or {})
        unit_map[unit_id] = unit
    for stage in plan.get("stages") or []:
        stage_units = [unit_map.get(unit_id) for unit_id in stage.get("unit_ids") or [] if unit_map.get(unit_id)]
        statuses: List[str] = []
        durations: List[int] = []
        for stage_unit in stage_units:
            runtime = stage_unit.get("runtime") or {}
            runtime["stage_index"] = int(stage.get("index") or 0)
            statuses.append(str(runtime.get("status") or "pending"))
            durations.append(int(runtime.get("duration_ms") or 0))
        stage["runtime"] = {
            "status": _runtime_status(statuses),
            "duration_ms": max(durations) if durations else 0,
            "unit_ids": list(stage.get("unit_ids") or []),
        }
    return plan


def _read_request_json(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    content_length = str(handler.headers.get("Content-Length") or "0")
    try:
        length = int(content_length)
    except ValueError as exc:
        raise ValueError("Invalid Content-Length") from exc
    raw = handler.rfile.read(length)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise ValueError("Invalid JSON body") from exc
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object")
    return payload


def _make_project_case(payload: Dict[str, Any], input_cases_root: str | Path = DEFAULT_INPUT_CASES_ROOT) -> ProjectCase:
    project_input = normalize_ws(str(payload.get("project") or ""))
    github_repo = normalize_ws(str(payload.get("github_repo") or ""))
    project_url = normalize_ws(str(payload.get("project_url") or ""))
    telegram_handle = normalize_ws(str(payload.get("telegram_handle") or ""))
    wallet_address = normalize_ws(str(payload.get("wallet_address") or ""))
    name = normalize_ws(str(payload.get("name") or ""))
    description = normalize_ws(str(payload.get("description") or ""))
    type_hint = normalize_ws(str(payload.get("type_hint") or ""))
    discovery = dict(payload.get("discovery") or {})
    selected_candidate = _selected_discovery_candidate(discovery) if discovery else {}
    selected_description = normalize_ws(str(selected_candidate.get("description") or ""))
    if selected_description:
        description = selected_description
    if project_input:
        guessed_repo = _extract_github_repo(project_input)
        guessed_telegram = _extract_telegram_handle(project_input)
        if guessed_repo and not github_repo:
            github_repo = guessed_repo
        elif guessed_telegram and not telegram_handle:
            telegram_handle = guessed_telegram
        elif project_input.startswith(("http://", "https://")) and not project_url:
            project_url = project_input
        elif not github_repo and "/" in project_input and re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", project_input):
            github_repo = project_input
    if project_url and not github_repo:
        github_repo = _extract_github_repo(project_url) or github_repo
    if github_repo and not project_url:
        project_url = f"https://github.com/{github_repo}"
    if not telegram_handle and project_url:
        telegram_handle = _extract_telegram_handle(project_url)
    identity_value = github_repo or project_url or telegram_handle or wallet_address
    if not identity_value:
        raise ValueError("Project input must include at least one project identifier")
    display_name = name or github_repo or project_url or telegram_handle or wallet_address
    case_base = _slugify(display_name)
    case_id = f"{case_base}_{int(time.time() * 1000)}"
    root_dir = ensure_dir(Path(input_cases_root) / case_id)
    return ProjectCase(
        case_id=case_id,
        name=name or display_name,
        requested_input=project_input,
        github_repo=github_repo,
        telegram_handle=telegram_handle,
        project_url=project_url,
        wallet_address=wallet_address,
        type_hint=type_hint,
        description=description,
        root_dir=str(root_dir),
    )


def _extract_first_url(value: str) -> str:
    return _clean_project_url(value)


def _clean_project_url(value: str) -> str:
    raw = html.unescape(normalize_ws(str(value or "")))
    if not raw:
        return ""
    raw = raw.replace("\\n", " ").replace("\\t", " ").replace("\\r", " ").strip().strip("\"'`")
    match = re.search(r"https?://[^\s\"'<>\\]+", raw, flags=re.IGNORECASE)
    if match:
        candidate = match.group(0)
    else:
        match = re.search(
            r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}(?:/[^\s\"'<>\\]*)?",
            raw,
            flags=re.IGNORECASE,
        )
        if not match:
            return ""
        candidate = f"https://{match.group(0)}"
    candidate = candidate.rstrip(".,;)]}\"'")
    parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
    if not parsed.netloc and parsed.path:
        parsed = urlparse(f"https://{parsed.path}")
    host = (parsed.netloc or "").lower().strip("\\")
    path = parsed.path.replace("\\", "").strip()
    if not host:
        return ""
    host_suffix = host.rsplit(".", 1)[-1]
    if host_suffix in {"css", "js", "mjs", "jsx", "ts", "tsx", "json", "yaml", "yml", "toml", "md", "txt", "xml", "map"}:
        return ""
    lower_path = path.lower()
    if re.search(r"\.(css|js|mjs|png|jpe?g|svg|gif|webp|ico|map|txt|xml|json|md|pdf|zip)$", lower_path):
        return ""
    return f"{parsed.scheme or 'https'}://{host}{path}".rstrip("/")


def _normalized_url(value: str) -> str:
    raw = _clean_project_url(value)
    if not raw:
        return ""
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    if not parsed.netloc and parsed.path:
        parsed = urlparse(f"https://{parsed.path}")
    host = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    normalized = f"{host}{path}".strip("/")
    return normalized


def _url_domain(value: str) -> str:
    normalized = _normalized_url(value)
    if not normalized:
        return ""
    return normalized.split("/", 1)[0].removeprefix("www.")


def _extract_external_url_candidates(text: str) -> List[str]:
    values: List[str] = []
    seen = set()
    raw = str(text or "")
    for value in re.findall(r"https?://[^\s\"'<>]+", raw, flags=re.IGNORECASE):
        normalized = _clean_project_url(value)
        key = normalized.lower()
        if not normalized or key in seen:
            continue
        seen.add(key)
        values.append(normalized)
    for value in re.findall(r"(?<![\\/@])\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}\b", raw, flags=re.IGNORECASE):
        normalized = _clean_project_url(value)
        if not normalized or normalized.lower() in seen:
            continue
        seen.add(normalized.lower())
        values.append(normalized)
    return values


def _brand_identity_tokens(text: str) -> List[str]:
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
        "for",
        "from",
        "github",
        "io",
        "marketplace",
        "nft",
        "official",
        "open",
        "project",
        "protocol",
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
    out: List[str] = []
    seen = set()
    for token in re.findall(r"[a-z0-9]{3,}", str(text or "").lower()):
        if len(token) < 4 or token in stopwords or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _canonical_project_url_from_text(text: str, identity_text: str) -> str:
    excluded_domains = {
        "github.com",
        "raw.githubusercontent.com",
        "npmjs.com",
        "www.npmjs.com",
        "opensource.org",
        "shields.io",
        "ton.org",
        "www.ton.org",
        "tonviewer.com",
        "www.tonviewer.com",
        "testnet.tonviewer.com",
    }
    brand_tokens = _brand_identity_tokens(identity_text)
    best_url = ""
    best_score = 0.0
    for value in _extract_external_url_candidates(text):
        domain = _url_domain(value)
        if not domain or domain in excluded_domains:
            continue
        compact_domain = re.sub(r"[^a-z0-9]+", "", domain.lower())
        compact_value = re.sub(r"[^a-z0-9]+", "", value.lower())
        if brand_tokens and not any(token in compact_domain for token in brand_tokens):
            continue
        score = 0.2
        if _is_canonical_project_url(value):
            score += 0.15
        path = urlparse(value if "://" in value else f"https://{value}").path.strip("/")
        if path:
            score -= min(0.15, max(0, len(path.split("/")) - 1) * 0.05)
        for token in brand_tokens:
            if token in compact_domain:
                score += 0.55
            elif token in compact_value:
                score += 0.15
        if score > best_score:
            best_score = score
            best_url = value
    return best_url


def _registry_candidate_project_url(case: ProjectCase) -> str:
    project_url = normalize_ws(str(case.project_url or ""))
    if project_url and _is_canonical_project_url(project_url):
        return project_url
    identity_text = " ".join([case.name, case.github_repo, case.description])
    if case.github_repo:
        repo_snapshot_path = case.snapshots_dir / "github_repo.json"
        if repo_snapshot_path.is_file():
            repo_meta = read_json(repo_snapshot_path)
            homepage = normalize_ws(str(repo_meta.get("homepage") or ""))
            if homepage and _is_canonical_project_url(homepage):
                return homepage
    readme_snapshot_path = case.snapshots_dir / "github_readme.txt"
    if readme_snapshot_path.is_file():
        inferred = _canonical_project_url_from_text(read_text(readme_snapshot_path), identity_text)
        if inferred:
            return inferred
    return project_url


def _discovery_tokens(text: str) -> List[str]:
    tokens: List[str] = []
    seen = set()
    for token in re.findall(r"[a-z0-9]{3,}", str(text or "").lower()):
        if token in REGISTRY_STOPWORDS:
            continue
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def _market_search_terms(text: str) -> List[str]:
    terms: List[str] = []
    seen = set()

    def add(value: str) -> None:
        normalized = normalize_ws(value)
        if len(normalized) < 3:
            return
        key = normalized.lower()
        if key in seen:
            return
        seen.add(key)
        terms.append(normalized)

    add(text)
    raw_text = str(text or "")
    raw_tokens = re.findall(r"[a-z0-9]{3,}", raw_text.lower())
    raw_symbol_tokens = re.findall(r"[A-Za-z0-9]{3,}", raw_text)
    has_ton_hint = "ton" in raw_tokens
    concise_market_query = len([token for token in raw_tokens if token not in REGISTRY_STOPWORDS]) <= 2
    compound_terms = _compound_discovery_terms(text)
    prioritize_compounds = bool(compound_terms) and not any(
        3 <= len(token) <= 4 and token.isupper() and token.lower() != "ton" for token in raw_symbol_tokens
    )
    if prioritize_compounds:
        for compound in compound_terms:
            add(compound)
    for token in raw_symbol_tokens:
        lowered = token.lower()
        if lowered in REGISTRY_STOPWORDS or lowered == "ton" or not token.isalnum():
            continue
        if 3 <= len(lowered) <= 4 and (token.isupper() or (has_ton_hint and concise_market_query)):
            add(token)
            add(f"{token} ton")
    if not prioritize_compounds:
        for compound in compound_terms:
            add(compound)
    filtered_tokens = raw_tokens if not compound_terms else [token for token in raw_tokens if token not in REGISTRY_STOPWORDS and len(token) >= 5]
    for token in filtered_tokens:
        add(token)
        letters_only = re.sub(r"\d+", "", token)
        if letters_only and letters_only != token and len(letters_only) >= 3:
            add(letters_only)
        if token != "ton":
            add(f"{token} ton")
            if letters_only and letters_only != token and len(letters_only) >= 3:
                add(f"{letters_only} ton")
    return terms[:8]


def _select_market_identity_url(urls: Iterable[str], identity_text: str) -> str:
    normalized_urls = [normalize_ws(str(item)) for item in urls if normalize_ws(str(item))]
    if not normalized_urls:
        return ""
    canonical = _canonical_project_url_from_text(" ".join(normalized_urls), identity_text)
    if canonical:
        return canonical
    for url in normalized_urls:
        domain = _url_domain(url)
        if domain and domain not in NON_CANONICAL_DOMAIN_ALIASES:
            return _clean_project_url(url)
    return _clean_project_url(normalized_urls[0])


def _compound_discovery_terms(text: str) -> List[str]:
    raw_tokens = re.findall(r"[a-z0-9]{3,}", str(text or "").lower())
    if len(raw_tokens) < 2:
        return []
    out: List[str] = []
    seen = set()
    joined = "".join(raw_tokens)
    joined_letters_only = "".join(re.sub(r"\d+", "", token) for token in raw_tokens)
    for candidate in (joined, joined_letters_only):
        normalized = normalize_ws(candidate)
        if len(normalized) < 3:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _github_search_terms(text: str) -> List[str]:
    terms: List[str] = []
    seen = set()

    def add(value: str) -> None:
        normalized = normalize_ws(value)
        if len(normalized) < 3:
            return
        key = normalized.lower()
        if key in seen:
            return
        seen.add(key)
        terms.append(normalized)

    add(text)
    raw_tokens = [token for token in re.findall(r"[a-z0-9]{3,}", str(text or "").lower()) if token not in REGISTRY_STOPWORDS]
    compound_terms = _compound_discovery_terms(text)
    for compound in compound_terms:
        add(compound)
    filtered_tokens = raw_tokens if not compound_terms else [token for token in raw_tokens if len(token) >= 5]
    for token in filtered_tokens:
        add(token)
        letters_only = re.sub(r"\d+", "", token)
        if letters_only and letters_only != token and len(letters_only) >= 3:
            add(letters_only)
    return terms[:6]


def _discovery_match_tokens(text: str) -> List[str]:
    tokens = _discovery_tokens(text)
    seen = set(tokens)
    raw_tokens = re.findall(r"[a-z0-9]{3,}", str(text or "").lower())
    for compound in _compound_discovery_terms(text):
        if len(compound) >= 3 and compound not in REGISTRY_STOPWORDS and compound not in seen:
            seen.add(compound)
            tokens.append(compound)
    for token in raw_tokens:
        normalized = re.sub(r"\d+", "", token)
        if len(normalized) < 3 or normalized in REGISTRY_STOPWORDS or normalized in seen:
            continue
        seen.add(normalized)
        tokens.append(normalized)
    return tokens


def _token_overlap_score(left: Iterable[str], right: Iterable[str]) -> float:
    left_set = {str(item) for item in left if str(item)}
    right_set = {str(item) for item in right if str(item)}
    if not left_set or not right_set:
        return 0.0
    return round(len(left_set & right_set) / len(left_set), 4)


def _candidate_identity(candidate: Dict[str, Any]) -> str:
    github_repo = str(candidate.get("github_repo") or "").lower()
    if github_repo:
        return f"github:{github_repo}"
    wallet_address = normalize_ws(str(candidate.get("wallet_address") or ""))
    if wallet_address:
        return f"wallet:{wallet_address}"
    telegram_handle = str(candidate.get("telegram_handle") or "").lower().lstrip("@")
    if telegram_handle:
        return f"telegram:{telegram_handle}"
    project_url = _normalized_url(str(candidate.get("project_url") or ""))
    if project_url:
        return f"url:{project_url}"
    return f"name:{_slugify(str(candidate.get('name') or 'candidate'))}"


def _candidate_key(candidate: Dict[str, Any]) -> str:
    return _candidate_identity(candidate)


def _source_labels(candidate: Dict[str, Any]) -> List[str]:
    labels = [str(item) for item in candidate.get("source_labels") or [] if str(item)]
    seen = set()
    out: List[str] = []
    for label in labels:
        if label in seen:
            continue
        seen.add(label)
        out.append(label)
    return out


def _finalize_candidate(candidate: Dict[str, Any]) -> Dict[str, Any]:
    finalized = {
        "candidate_key": _candidate_key(candidate),
        "name": str(candidate.get("name") or candidate.get("github_repo") or "candidate"),
        "github_repo": str(candidate.get("github_repo") or ""),
        "project_url": _clean_project_url(str(candidate.get("project_url") or "")),
        "telegram_handle": str(candidate.get("telegram_handle") or ""),
        "wallet_address": str(candidate.get("wallet_address") or ""),
        "description": str(candidate.get("description") or ""),
        "project_type": str(candidate.get("project_type") or ""),
        "source_labels": _source_labels(candidate),
        "match_reason": str(candidate.get("match_reason") or ""),
        "score": float(candidate.get("score") or 0.0),
    }
    return finalized


NON_CANONICAL_DOMAIN_ALIASES = {
    "github.com",
    "support.github.com",
    "docs.github.com",
    "pages.github.com",
    "t.me",
    "telegram.me",
    "geckoterminal.com",
    "www.geckoterminal.com",
    "coingecko.com",
    "www.coingecko.com",
    "coinmarketcap.com",
    "www.coinmarketcap.com",
    "holder.io",
    "www.holder.io",
}

DISCOVERY_SEED_PROJECTS: List[Dict[str, Any]] = [
    {
        "name": "TON Wallet",
        "aliases": ["wallet tg", "telegram wallet", "ton wallet", "wallet telegram"],
        "github_repo": "",
        "project_url": "https://wallet.tg",
        "telegram_handle": "wallet",
        "wallet_address": "",
        "description": "Self-custodial TON wallet in Telegram.",
        "project_type": "wallet_app",
    },
    {
        "name": "bemo",
        "aliases": ["bemo liquid staking", "bmton", "bemo staking"],
        "github_repo": "",
        "project_url": "https://bemo.fi",
        "telegram_handle": "bemofinance",
        "wallet_address": "",
        "description": "Liquid staking protocol on TON.",
        "project_type": "staking_protocol",
    },
    {
        "name": "Hipo",
        "aliases": ["hipo finance", "hipo liquid staking", "hton"],
        "github_repo": "HipoFinance/website",
        "project_url": "https://hipo.finance",
        "telegram_handle": "",
        "wallet_address": "",
        "description": "Liquid staking protocol on TON.",
        "project_type": "staking_protocol",
    },
    {
        "name": "Storm Trade",
        "aliases": ["stormtrade", "storm trade bot", "storm perpetuals"],
        "github_repo": "",
        "project_url": "https://storm.tg",
        "telegram_handle": "storm_trade_fam",
        "wallet_address": "",
        "description": "Perpetual futures trading platform on TON.",
        "project_type": "derivatives_dex",
    },
    {
        "name": "Tonswap",
        "aliases": ["ton swap", "tonswap dex"],
        "github_repo": "",
        "project_url": "https://tonswap.org",
        "telegram_handle": "tonswap_org",
        "wallet_address": "",
        "description": "Decentralized exchange on TON.",
        "project_type": "dex",
    },
    {
        "name": "Bidask Finance",
        "aliases": ["bidask", "bidask dex", "bidask finance"],
        "github_repo": "",
        "project_url": "https://www.bidask.finance",
        "telegram_handle": "bidask",
        "wallet_address": "",
        "description": "Concentrated liquidity DEX on TON.",
        "project_type": "dex",
    },
    {
        "name": "TON Diamonds",
        "aliases": ["ton diamonds", "ton.diamonds", "diamonds marketplace"],
        "github_repo": "",
        "project_url": "https://ton.diamonds",
        "telegram_handle": "",
        "wallet_address": "",
        "description": "NFT marketplace on TON.",
        "project_type": "nft_marketplace",
    },
    {
        "name": "DeDust",
        "aliases": ["dedust", "dedust dex", "dedust io", "de dust", "dust dex"],
        "github_repo": "",
        "project_url": "https://dedust.io",
        "telegram_handle": "dedust",
        "wallet_address": "",
        "description": "Decentralized exchange on TON.",
        "project_type": "dex",
    },
    {
        "name": "EVAA",
        "aliases": ["evaa", "evaa protocol", "evaa finance", "evaa lending", "evaa defi"],
        "github_repo": "",
        "project_url": "https://evaa.finance",
        "telegram_handle": "evaatg",
        "wallet_address": "",
        "description": "Decentralized lending protocol on TON and Telegram.",
        "project_type": "dapp_product",
    },
    {
        "name": "TON DNS",
        "aliases": ["ton domains", "ton dns", "dns ton", "dot ton domains"],
        "github_repo": "ton-blockchain/dns-contract",
        "project_url": "https://dns.ton.org",
        "telegram_handle": "",
        "wallet_address": "",
        "description": "TON DNS and .ton domain service.",
        "project_type": "protocol_service",
    },
    {
        "name": "Tonviewer",
        "aliases": ["ton viewer", "tonviewer explorer", "ton explorer"],
        "github_repo": "",
        "project_url": "https://tonviewer.com",
        "telegram_handle": "",
        "wallet_address": "",
        "description": "Blockchain explorer for TON.",
        "project_type": "explorer",
    },
    {
        "name": "Tonapi",
        "aliases": ["ton api", "tonapi api", "ton api platform"],
        "github_repo": "tonkeeper/tonapi-go",
        "project_url": "https://tonapi.io",
        "telegram_handle": "",
        "wallet_address": "",
        "description": "Public API platform for TON.",
        "project_type": "tooling_api",
    },
]


def _candidate_aliases(candidate: Dict[str, Any]) -> List[str]:
    aliases: List[str] = []
    github_repo = str(candidate.get("github_repo") or "").lower()
    wallet_address = normalize_ws(str(candidate.get("wallet_address") or ""))
    telegram_handle = str(candidate.get("telegram_handle") or "").lower().lstrip("@")
    project_url = _normalized_url(str(candidate.get("project_url") or ""))
    project_domain = _url_domain(str(candidate.get("project_url") or ""))
    if github_repo:
        aliases.append(f"github:{github_repo}")
    if wallet_address:
        aliases.append(f"wallet:{wallet_address}")
    if telegram_handle:
        aliases.append(f"telegram:{telegram_handle}")
    if project_url:
        aliases.append(f"url:{project_url}")
    if project_domain and project_domain not in NON_CANONICAL_DOMAIN_ALIASES:
        aliases.append(f"domain:{project_domain}")
    return aliases


def _candidate_priority_score(candidate: Dict[str, Any]) -> float:
    score = float(candidate.get("score") or 0.0)
    source_labels = set(_source_labels(candidate))
    bonus = 0.0
    if candidate.get("github_repo"):
        bonus += 0.18
    if candidate.get("telegram_handle"):
        bonus += 0.16
    if candidate.get("project_url"):
        bonus += 0.24 if _is_canonical_project_url(str(candidate.get("project_url") or "")) else 0.04
    if candidate.get("wallet_address"):
        bonus += 0.08
    bonus += 0.04 * max(0, len(source_labels) - 1)
    if "registry" in source_labels:
        bonus += 0.12
    if "direct_github" in source_labels:
        bonus += 0.18
    if "public_web" in source_labels:
        bonus += 0.18
    return round(score + bonus, 4)


def _is_canonical_project_url(value: str) -> bool:
    domain = _url_domain(value)
    if not domain or domain in NON_CANONICAL_DOMAIN_ALIASES:
        return False
    if domain.endswith(".git") or domain == "git":
        return False
    if "github" in domain:
        return False
    return True


def _query_selection_intent(query: str) -> Dict[str, bool]:
    low = normalize_ws(query).lower()
    return {
        "sdk": bool(re.search(r"\b(sdk|library|tooling|api|cli|package|wrapper)\b", low)),
        "wallet": bool(re.search(r"\bwallet\b", low)),
        "dex": bool(re.search(r"\b(dex|swap|exchange|liquidity)\b", low)),
        "marketplace": bool(re.search(r"\bmarketplace\b", low)),
        "lending": bool(re.search(r"\b(lending|borrow|loan)\b", low)),
        "contracts": bool(re.search(r"\b(contract|contracts|jetton|tact|func)\b", low)),
        "token": bool(re.search(r"\b(token|coin|memecoin|meme|pool)\b", low)),
    }


def _candidate_query_rank_score(candidate: Dict[str, Any], query: str) -> float:
    base = float(candidate.get("score") or 0.0)
    source_labels = set(_source_labels(candidate))
    github_repo = str(candidate.get("github_repo") or "").lower()
    project_url = str(candidate.get("project_url") or "")
    repo_owner = github_repo.split("/", 1)[0] if "/" in github_repo else github_repo
    repo_name = github_repo.split("/", 1)[-1] if "/" in github_repo else github_repo
    compact_owner = re.sub(r"[^a-z0-9]+", "", repo_owner)
    compact_domain = re.sub(r"[^a-z0-9]+", "", _url_domain(project_url))
    haystack = normalize_ws(
        " ".join(
            [
                str(candidate.get("name") or ""),
                str(candidate.get("description") or ""),
                str(candidate.get("project_type") or ""),
                github_repo,
                project_url,
                str(candidate.get("telegram_handle") or ""),
            ]
        )
    ).lower()
    compact_haystack = re.sub(r"[^a-z0-9]+", "", haystack)
    intent = _query_selection_intent(query)
    product_intent = any(intent[key] for key in ("wallet", "dex", "marketplace", "lending"))
    brand_stopwords = {"ton", "wallet", "dex", "swap", "exchange", "marketplace", "lending", "borrow", "loan", "protocol", "nft", "sdk", "tooling", "contract", "contracts", "app"}
    brand_tokens = [
        token
        for token in {
            re.sub(r"[^a-z0-9]+", "", item.lower())
            for item in _discovery_match_tokens(query)
        }
        if len(token) >= 4 and token not in brand_stopwords
    ]
    brand_owner_match = any(token and token in compact_owner for token in brand_tokens)
    brand_domain_match = any(token and token in compact_domain for token in brand_tokens)
    brand_haystack_match = any(token and token in compact_haystack for token in brand_tokens)
    official_brand_signal = brand_owner_match or "registry" in source_labels or bool(candidate.get("telegram_handle") or candidate.get("wallet_address"))
    score = base
    if _is_canonical_project_url(project_url):
        score += 0.28 if official_brand_signal else (0.16 if brand_domain_match else 0.04)
    elif project_url:
        score -= 0.08
    if project_url and _is_canonical_project_url(project_url) and not brand_domain_match and not source_labels.intersection({"registry", "public_web", "direct_github"}):
        score -= 0.18
    if candidate.get("telegram_handle"):
        score += 0.10
    if candidate.get("wallet_address"):
        score += 0.08
    if "public_web" in source_labels:
        score += 0.16
    if "github_search" in source_labels:
        score += 0.08
    if "registry" in source_labels:
        score += 0.05
        if _candidate_identity_signal_count(candidate) >= 2:
            score += 0.08
    if brand_tokens:
        if brand_owner_match:
            score += 0.20
        elif brand_domain_match:
            score += 0.16
    if intent["sdk"]:
        if "sdk" in haystack or "tooling" in haystack:
            score += 0.24
    else:
        if "sdk" in haystack or "tooling" in haystack:
            score -= 0.24
    if re.search(r"\b(example|sample|demo|tutorial|boilerplate|starter|template|test|learn|learning|course|guide)\b", haystack) or re.search(
        r"(example|sample|demo|tutorial|boilerplate|starter|template|test|learn|course|guide)",
        repo_name,
    ):
        score -= 0.40
    if intent["wallet"] and "wallet" in haystack:
        score += 0.20
    if intent["dex"] and re.search(r"\b(dex|swap|liquidity|exchange)\b", haystack):
        score += 0.18
    if intent["marketplace"] and "marketplace" in haystack:
        score += 0.18
    if intent["lending"] and re.search(r"\b(lending|borrow|loan)\b", haystack):
        score += 0.18
    if intent["contracts"] and "contract" in haystack:
        score += 0.12
    if any(intent[key] for key in ("wallet", "dex", "marketplace", "lending")) and "sdk" in haystack and not intent["sdk"]:
        score -= 0.22
    if "github_search" in source_labels and not source_labels.intersection({"registry", "direct_github", "public_web"}):
        if _candidate_identity_signal_count(candidate) <= 1 and not brand_owner_match:
            score -= 0.08
        if product_intent and _candidate_identity_signal_count(candidate) <= 1 and not (brand_owner_match or brand_domain_match):
            score -= 0.28
        if product_intent and re.search(r"\b(dashboard|analytics|sql|queries?|visualization|fork|clone|unofficial|inspired)\b", haystack):
            score -= 0.34
    if brand_tokens and not (brand_owner_match or brand_domain_match or brand_haystack_match):
        score -= 0.40
    return round(max(0.0, score), 4)


def _normalized_speed_profile(value: str) -> str:
    normalized = normalize_ws(value).lower()
    return "interactive" if normalized in {"interactive", "fast", "simple", "user"} else "full"


def _profile_timeout(speed_profile: str, interactive_timeout: int, full_timeout: int) -> int:
    return interactive_timeout if _normalized_speed_profile(speed_profile) == "interactive" else full_timeout


def _profile_limit(speed_profile: str, interactive_limit: int, full_limit: int) -> int:
    return interactive_limit if _normalized_speed_profile(speed_profile) == "interactive" else full_limit


def _candidate_identity_signal_count(candidate: Dict[str, Any]) -> int:
    count = 0
    if str(candidate.get("github_repo") or ""):
        count += 1
    if str(candidate.get("telegram_handle") or ""):
        count += 1
    if str(candidate.get("wallet_address") or ""):
        count += 1
    if _is_canonical_project_url(str(candidate.get("project_url") or "")):
        count += 1
    return count


def _should_skip_public_web_identity(candidates: List[Dict[str, Any]], query: str, speed_profile: str) -> bool:
    if _normalized_speed_profile(speed_profile) != "interactive":
        return False
    if not candidates:
        return False
    ranked = sorted(candidates, key=lambda item: _candidate_query_rank_score(item, query), reverse=True)
    top = ranked[0]
    if _candidate_query_rank_score(top, query) < 0.9:
        return False
    return _candidate_identity_signal_count(top) >= 2 and (
        _is_canonical_project_url(str(top.get("project_url") or "")) or str(top.get("telegram_handle") or "")
    )


def _should_skip_external_discovery_from_registry(
    registry_candidates: List[Dict[str, Any]],
    query: str,
    speed_profile: str,
    *,
    explicit_repo: str,
    explicit_url: str,
    explicit_telegram: str,
) -> bool:
    if explicit_repo or explicit_url or explicit_telegram:
        return False
    if not registry_candidates:
        return False
    ranked = sorted(registry_candidates, key=lambda item: _candidate_query_rank_score(item, query), reverse=True)
    candidate = ranked[0]
    rank_score = _candidate_query_rank_score(candidate, query)
    second_score = _candidate_query_rank_score(ranked[1], query) if len(ranked) > 1 else 0.0
    score_gap = rank_score - second_score
    match_reason = normalize_ws(str(candidate.get("match_reason") or "")).lower()
    strong_registry_match = any(
        marker in match_reason
        for marker in (
            "project name mentioned in query",
            "catalog alias match",
            "exact github repo match",
            "exact telegram handle match",
            "exact website domain match",
        )
    )
    if rank_score < 0.75 and not strong_registry_match:
        return False
    has_primary_identity = bool(_is_canonical_project_url(str(candidate.get("project_url") or "")) or str(candidate.get("github_repo") or ""))
    if not has_primary_identity:
        return False
    identity_signal_count = _candidate_identity_signal_count(candidate)
    if identity_signal_count >= 2:
        return True
    if len(ranked) > 1 and identity_signal_count >= 1 and strong_registry_match and score_gap >= 0.45:
        return True
    return False


def _should_skip_discovery_llm(candidates: List[Dict[str, Any]], query: str, speed_profile: str) -> bool:
    if not candidates:
        return False
    if len(candidates) == 1:
        return True
    ranked = sorted(candidates, key=lambda item: _candidate_query_rank_score(item, query), reverse=True)
    top = ranked[0]
    second = ranked[1]
    top_score = _candidate_query_rank_score(top, query)
    second_score = _candidate_query_rank_score(second, query)
    if top_score >= 0.95 and top_score - second_score >= 0.12:
        return True
    if _candidate_identity_signal_count(top) >= 3 and top_score - second_score >= 0.08:
        return True
    if _is_canonical_project_url(str(top.get("project_url") or "")) and top_score - second_score >= 0.15:
        return True
    return False


def _deterministic_discovery_summary(candidate: Dict[str, Any]) -> str:
    identity_bits: List[str] = []
    if _is_canonical_project_url(str(candidate.get("project_url") or "")):
        identity_bits.append("canonical website")
    if str(candidate.get("telegram_handle") or ""):
        identity_bits.append("telegram")
    if str(candidate.get("github_repo") or ""):
        identity_bits.append("github")
    if str(candidate.get("wallet_address") or ""):
        identity_bits.append("wallet")
    if identity_bits:
        return (
            f"Selected candidate {candidate.get('name') or candidate.get('candidate_key') or 'candidate'} "
            f"from strong identity signals: {', '.join(identity_bits[:3])}."
        )
    return f"Selected candidate {candidate.get('name') or candidate.get('candidate_key') or 'candidate'} from deterministic ranking."


def _merge_candidate_payloads(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    merged = dict(candidates[0])
    for finalized in candidates[1:]:
        merged["score"] = max(float(merged.get("score") or 0.0), float(finalized.get("score") or 0.0))
        merged["source_labels"] = _source_labels(
            {"source_labels": list(merged.get("source_labels") or []) + list(finalized.get("source_labels") or [])}
        )
        current_name = normalize_ws(str(merged.get("name") or ""))
        next_name = normalize_ws(str(finalized.get("name") or ""))
        if next_name and (not current_name or (_looks_like_raw_identity_name(current_name) and not _looks_like_raw_identity_name(next_name))):
            merged["name"] = finalized["name"]
        if not merged.get("github_repo") and finalized.get("github_repo"):
            merged["github_repo"] = finalized["github_repo"]
        if (
            finalized.get("project_url")
            and (
                not merged.get("project_url")
                or (not _is_canonical_project_url(str(merged.get("project_url") or "")) and _is_canonical_project_url(str(finalized.get("project_url") or "")))
            )
        ):
            merged["project_url"] = finalized["project_url"]
        if not merged.get("telegram_handle") and finalized.get("telegram_handle"):
            merged["telegram_handle"] = finalized["telegram_handle"]
        if not merged.get("wallet_address") and finalized.get("wallet_address"):
            merged["wallet_address"] = finalized["wallet_address"]
        if not merged.get("description") and finalized.get("description"):
            merged["description"] = finalized["description"]
        if not merged.get("project_type") and finalized.get("project_type"):
            merged["project_type"] = finalized["project_type"]
        if not merged.get("match_reason") and finalized.get("match_reason"):
            merged["match_reason"] = finalized["match_reason"]
    return _finalize_candidate(merged)


def _candidate_hint(candidate: Dict[str, Any]) -> str:
    telegram_handle = str(candidate.get("telegram_handle") or "").strip().lstrip("@")
    if telegram_handle:
        return f"@{telegram_handle}"
    domain = _url_domain(str(candidate.get("project_url") or ""))
    if domain and _is_canonical_project_url(str(candidate.get("project_url") or "")):
        return domain
    github_repo = str(candidate.get("github_repo") or "").strip()
    if github_repo:
        return github_repo
    wallet_address = normalize_ws(str(candidate.get("wallet_address") or ""))
    if wallet_address:
        if len(wallet_address) <= 14:
            return wallet_address
        return f"{wallet_address[:6]}...{wallet_address[-4:]}"
    source_labels = _source_labels(candidate)
    return source_labels[0] if source_labels else ""


def _looks_like_raw_identity_name(value: str) -> bool:
    normalized = normalize_ws(str(value or ""))
    low = normalized.lower()
    if not normalized:
        return False
    if low.startswith(("http://", "https://", "@")):
        return True
    if re.fullmatch(r"[a-z0-9_.-]+/[a-z0-9_.-]+", low):
        return True
    if "." in low and " " not in low and bool(_clean_project_url(low)):
        return True
    return False


def _disambiguate_candidate_names(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    for candidate in candidates:
        key = normalize_ws(str(candidate.get("name") or "")).lower()
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1
    out: List[Dict[str, Any]] = []
    for candidate in candidates:
        updated = dict(candidate)
        name = normalize_ws(str(updated.get("name") or ""))
        key = name.lower()
        if name and counts.get(key, 0) > 1:
            hint = _candidate_hint(updated)
            if hint and hint.lower() not in key:
                updated["name"] = f"{name} | {hint}"
        out.append(updated)
    return out


def _merge_candidates(candidates: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    finalized_candidates = [_finalize_candidate(candidate) for candidate in candidates]
    if not finalized_candidates:
        return []
    parent = list(range(len(finalized_candidates)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    alias_map: Dict[str, int] = {}
    for index, candidate in enumerate(finalized_candidates):
        aliases = _candidate_aliases(candidate)
        candidate_key = str(candidate.get("candidate_key") or "")
        if candidate_key:
            aliases.append(f"key:{candidate_key}")
        for alias in aliases:
            existing = alias_map.get(alias)
            if existing is None:
                alias_map[alias] = index
                continue
            union(existing, index)

    groups: Dict[int, List[Dict[str, Any]]] = {}
    for index, candidate in enumerate(finalized_candidates):
        root = find(index)
        groups.setdefault(root, []).append(candidate)
    merged = [_merge_candidate_payloads(group) for group in groups.values()]
    merged.sort(key=_candidate_priority_score, reverse=True)
    return _disambiguate_candidate_names(merged)


def _user_hint_candidate(query: str, explicit_repo: str, explicit_url: str, explicit_telegram: str) -> Optional[Dict[str, Any]]:
    if not (explicit_repo or explicit_url or explicit_telegram):
        return None
    return _finalize_candidate(
        {
            "name": explicit_repo or explicit_url or explicit_telegram,
            "github_repo": explicit_repo,
            "project_url": explicit_url,
            "telegram_handle": explicit_telegram,
            "description": query,
            "project_type": "",
            "source_labels": ["user_input"],
            "match_reason": "Used the explicit project reference from the free-form request.",
            "score": 0.55 if explicit_repo else 0.35,
        }
    )


def _registry_candidate(case: ProjectCase, query: str, explicit_repo: str, explicit_url: str, explicit_telegram: str) -> Optional[Dict[str, Any]]:
    project_url = _registry_candidate_project_url(case)
    query_tokens = _discovery_match_tokens(query)
    haystack = normalize_ws(
        " ".join(
            [
                case.name,
                case.description,
                case.github_repo,
                project_url,
                case.telegram_handle,
                case.type_hint,
                " ".join(case.expected.get("project_types_any_of") or []),
            ]
        )
    )
    candidate_tokens = _discovery_tokens(haystack)
    score = _token_overlap_score(query_tokens, candidate_tokens)
    boost = 0.0
    reasons: List[str] = []
    if explicit_repo and case.github_repo.lower() == explicit_repo.lower():
        boost += 0.9
        reasons.append("exact GitHub repo match")
    if explicit_telegram and case.telegram_handle.lower().lstrip("@") == explicit_telegram.lower().lstrip("@"):
        boost += 0.8
        reasons.append("exact Telegram handle match")
    if explicit_url and _url_domain(project_url) and _url_domain(project_url) == _url_domain(explicit_url):
        boost += 0.8
        reasons.append("exact website domain match")
    if case.name and normalize_ws(case.name).lower() in normalize_ws(query).lower():
        boost += 0.2
        reasons.append("project name mentioned in query")
    total = round(score + boost, 4)
    if total <= 0:
        return None
    project_types = case.expected.get("project_types_any_of") or []
    return _finalize_candidate(
        {
            "name": case.name,
            "github_repo": case.github_repo,
            "project_url": project_url,
            "telegram_handle": case.telegram_handle,
            "wallet_address": case.wallet_address,
            "description": case.description,
            "project_type": str(case.type_hint or (project_types[0] if project_types else "")),
            "source_labels": ["registry"],
            "match_reason": ", ".join(reasons) if reasons else "Matched against known TON project registry.",
            "score": total,
        }
    )


def _seed_registry_candidate(seed: Dict[str, Any], query: str, explicit_repo: str, explicit_url: str, explicit_telegram: str) -> Optional[Dict[str, Any]]:
    query_tokens = _discovery_match_tokens(query)
    aliases = [normalize_ws(str(item)) for item in seed.get("aliases") or [] if normalize_ws(str(item))]
    name = normalize_ws(str(seed.get("name") or ""))
    github_repo = normalize_ws(str(seed.get("github_repo") or ""))
    project_url = _clean_project_url(str(seed.get("project_url") or ""))
    telegram_handle = normalize_ws(str(seed.get("telegram_handle") or ""))
    wallet_address = normalize_ws(str(seed.get("wallet_address") or ""))
    description = normalize_ws(str(seed.get("description") or ""))
    project_type = normalize_ws(str(seed.get("project_type") or ""))
    haystack = normalize_ws(
        " ".join(
            [
                name,
                " ".join(aliases),
                github_repo,
                project_url,
                telegram_handle,
                description,
                project_type,
            ]
        )
    )
    candidate_tokens = _discovery_tokens(haystack)
    score = _token_overlap_score(query_tokens, candidate_tokens)
    boost = 0.0
    reasons: List[str] = []
    query_low = normalize_ws(query).lower()
    if explicit_repo and github_repo and github_repo.lower() == explicit_repo.lower():
        boost += 0.9
        reasons.append("exact GitHub repo match")
    if explicit_telegram and telegram_handle and telegram_handle.lower().lstrip("@") == explicit_telegram.lower().lstrip("@"):
        boost += 0.8
        reasons.append("exact Telegram handle match")
    if explicit_url and _url_domain(project_url) and _url_domain(project_url) == _url_domain(explicit_url):
        boost += 0.8
        reasons.append("exact website domain match")
    if name and name.lower() in query_low:
        boost += 0.2
        reasons.append("project name mentioned in query")
    if any(alias.lower() in query_low for alias in aliases):
        boost += 0.35
        reasons.append("catalog alias match")
    total = round(score + boost, 4)
    if total <= 0:
        return None
    return _finalize_candidate(
        {
            "name": name,
            "github_repo": github_repo,
            "project_url": project_url,
            "telegram_handle": telegram_handle,
            "wallet_address": wallet_address,
            "description": description,
            "project_type": project_type,
            "source_labels": ["registry"],
            "match_reason": ", ".join(reasons) if reasons else "Matched against curated TON discovery catalog.",
            "score": total,
        }
    )


def _registry_discovery_candidates(
    query: str,
    explicit_repo: str,
    explicit_url: str,
    explicit_telegram: str,
    cases_root: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    base = cases_root or (PROJECT_ROOT / "cases")
    candidates: List[Dict[str, Any]] = []
    for seed in DISCOVERY_SEED_PROJECTS:
        candidate = _seed_registry_candidate(seed, query, explicit_repo, explicit_url, explicit_telegram)
        if candidate:
            candidates.append(candidate)
    for case_json in sorted(base.glob("*/case.json")):
        case = ProjectCase.load(case_json)
        candidate = _registry_candidate(case, query, explicit_repo, explicit_url, explicit_telegram)
        if candidate:
            candidates.append(candidate)
    return sorted(candidates, key=lambda item: float(item.get("score") or 0.0), reverse=True)[:5]


def _candidate_from_repo_meta(repo_meta: Dict[str, Any], *, score: float, source_label: str, reason: str) -> Dict[str, Any]:
    topics = [str(item) for item in repo_meta.get("topics") or [] if item]
    project_type = topics[0] if topics else ""
    identity_text = " ".join([str(repo_meta.get("full_name") or ""), str(repo_meta.get("description") or "")])
    project_url = _canonical_project_url_from_text(str(repo_meta.get("homepage") or ""), identity_text)
    return _finalize_candidate(
        {
            "name": str(repo_meta.get("name") or repo_meta.get("full_name") or ""),
            "github_repo": str(repo_meta.get("full_name") or ""),
            "project_url": project_url,
            "telegram_handle": "",
            "wallet_address": "",
            "description": str(repo_meta.get("description") or ""),
            "project_type": project_type,
            "source_labels": [source_label],
            "match_reason": reason,
            "score": score,
        }
    )


def _market_candidate(
    *,
    name: str,
    symbol: str,
    project_url: str,
    telegram_handle: str,
    wallet_address: str,
    description: str,
    project_type: str,
    score: float,
    source_label: str,
    reason: str,
) -> Dict[str, Any]:
    display_name = normalize_ws(name or symbol or wallet_address or project_url or "candidate")
    return _finalize_candidate(
        {
            "name": display_name,
            "github_repo": "",
            "project_url": project_url,
            "telegram_handle": telegram_handle,
            "wallet_address": wallet_address,
            "description": description,
            "project_type": project_type or "token",
            "source_labels": [source_label],
            "match_reason": reason,
            "score": score,
        }
    )


PUBLIC_WEB_WALLET_DOMAINS = (
    "getgems.io",
    "tonviewer.com",
    "fragment.com",
)


def _public_wallet_search_queries(candidate: Dict[str, Any], query: str) -> List[str]:
    seeds: List[str] = []
    seen = set()

    def add(value: str) -> None:
        normalized = normalize_ws(value)
        if len(normalized) < 3:
            return
        key = normalized.lower()
        if key in seen:
            return
        seen.add(key)
        seeds.append(normalized)

    add(str(candidate.get("name") or ""))
    add(str(candidate.get("github_repo") or ""))
    repo_name = str(candidate.get("github_repo") or "").split("/", 1)[-1].replace("-", " ").replace("_", " ")
    add(repo_name)
    add(str(candidate.get("telegram_handle") or ""))
    add(query)

    project_type = str(candidate.get("project_type") or "").lower()
    suffixes = ["ton contract address", "ton collection address", "ton getgems", "ton tonviewer"]
    if any(token in project_type for token in ("token", "coin", "jetton", "meme")):
        suffixes.insert(1, "ton jetton address")
    queries: List[str] = []
    query_seen = set()
    for seed in seeds:
        for suffix in suffixes:
            search_query = normalize_ws(f"{seed} {suffix}")
            key = search_query.lower()
            if key in query_seen:
                continue
            query_seen.add(key)
            queries.append(search_query)
    return queries[:6]


def _duckduckgo_result_urls(page_html: str, allowed_domains: Optional[Iterable[str]] = None) -> List[str]:
    urls: List[str] = []
    seen = set()
    allowed = tuple(str(item).lower() for item in (allowed_domains or ()))
    for raw_href in re.findall(r'href="([^"]+)"', str(page_html or "")):
        href = html.unescape(str(raw_href))
        candidate_url = ""
        if "duckduckgo.com/l/?" in href or "duckduckgo.com/l/?" in href.replace("&amp;", "&"):
            parsed = urlparse(href)
            target = parse_qs(parsed.query).get("uddg") or []
            candidate_url = str(target[0] or "") if target else ""
        elif href.startswith("http://") or href.startswith("https://"):
            candidate_url = href
        if not candidate_url:
            continue
        domain = _url_domain(candidate_url)
        if allowed and not any(domain.endswith(item) for item in allowed):
            continue
        if candidate_url in seen:
            continue
        seen.add(candidate_url)
        urls.append(candidate_url)
    return urls[:6]


async def _enrich_candidate_with_public_wallet(candidate: Dict[str, Any], query: str, speed_profile: str = "full") -> Dict[str, Any]:
    original_key = str(candidate.get("candidate_key") or _candidate_key(candidate))
    if str(candidate.get("wallet_address") or "").strip():
        return {
            "original_key": original_key,
            "candidate": _finalize_candidate(candidate),
            "resolved": False,
            "addresses": [],
            "source_urls": [],
        }
    seen_addresses = set()
    addresses: List[str] = []
    source_urls: List[str] = []
    for search_query in _public_wallet_search_queries(candidate, query)[: _profile_limit(speed_profile, 2, 6)]:
        search_html = await http_fetch_text(
            f"https://html.duckduckgo.com/html/?q={quote_plus(search_query)}",
            headers=TGSTAT_HEADERS,
            timeout=_profile_timeout(speed_profile, 8, 45),
        )
        for result_url in _duckduckgo_result_urls(search_html, PUBLIC_WEB_WALLET_DOMAINS):
            if result_url not in source_urls:
                source_urls.append(result_url)
            for address in extract_ton_addresses(result_url):
                if address in seen_addresses:
                    continue
                seen_addresses.add(address)
                addresses.append(address)
            if addresses:
                continue
            try:
                page_html = await http_fetch_text(
                    result_url,
                    headers=TELEGRAM_HEADERS,
                    timeout=_profile_timeout(speed_profile, 8, 45),
                )
            except Exception:
                continue
            page_text = strip_html(page_html)
            for address in extract_ton_addresses(f"{result_url} {page_text}"):
                if address in seen_addresses:
                    continue
                seen_addresses.add(address)
                addresses.append(address)
        if addresses:
            break
    enriched = dict(candidate)
    if addresses:
        enriched["wallet_address"] = addresses[0]
        enriched["source_labels"] = _source_labels(
            {"source_labels": list(candidate.get("source_labels") or []) + ["public_web"]}
        )
        reason = str(enriched.get("match_reason") or "")
        suffix = f"Public TON web search resolved contract address {addresses[0]}."
        enriched["match_reason"] = normalize_ws(f"{reason} {suffix}")
    return {
        "original_key": original_key,
        "candidate": _finalize_candidate(enriched),
        "resolved": bool(addresses),
        "addresses": addresses[:5],
        "source_urls": source_urls[:5],
    }


async def _enrich_candidates_with_public_wallets(
    candidates: List[Dict[str, Any]],
    query: str,
    speed_profile: str = "full",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    pending_candidates = [
        dict(candidate)
        for candidate in candidates[: _profile_limit(speed_profile, 2, 3)]
        if not str(candidate.get("wallet_address") or "").strip()
    ]
    if not pending_candidates:
        return candidates, {
            "status": "skipped",
            "candidate_count": 0,
            "summary": "Public wallet search was skipped because the top candidates already had wallet or contract identifiers.",
        }
    results = await asyncio.gather(
        *[_enrich_candidate_with_public_wallet(candidate, query, speed_profile) for candidate in pending_candidates],
        return_exceptions=True,
    )
    replacements: Dict[str, Dict[str, Any]] = {}
    resolved_count = 0
    errors: List[str] = []
    for result in results:
        if isinstance(result, Exception):
            errors.append(str(result))
            continue
        replacements[str(result.get("original_key") or "")] = dict(result.get("candidate") or {})
        if result.get("resolved"):
            resolved_count += 1
    enriched = [replacements.get(str(candidate.get("candidate_key") or ""), candidate) for candidate in candidates]
    if resolved_count:
        return enriched, {
            "status": "success",
            "candidate_count": resolved_count,
            "summary": f"Public TON web search resolved wallet or collection addresses for {resolved_count} candidate(s).",
        }
    if errors:
        return enriched, {
            "status": "error",
            "candidate_count": 0,
            "summary": clip_text(errors[0], 240),
        }
    return enriched, {
        "status": "skipped",
        "candidate_count": 0,
        "summary": "Public TON web search did not resolve wallet or collection addresses for the top candidates.",
    }


PUBLIC_WEB_IDENTITY_EXCLUDED_DOMAINS = {
    "duckduckgo.com",
    "coingecko.com",
    "www.coingecko.com",
    "geckoterminal.com",
    "www.geckoterminal.com",
    "coinmarketcap.com",
    "www.coinmarketcap.com",
    "holder.io",
    "www.holder.io",
    "youtube.com",
    "www.youtube.com",
}


def _public_identity_search_queries(query: str) -> List[str]:
    queries: List[str] = []
    seen = set()

    def add(value: str) -> None:
        normalized = normalize_ws(value)
        if len(normalized) < 3:
            return
        key = normalized.lower()
        if key in seen:
            return
        seen.add(key)
        queries.append(normalized)

    low_query = normalize_ws(query).lower()
    brand_terms = _brand_identity_tokens(query)
    raw_symbol_tokens = re.findall(r"[A-Za-z0-9]{3,}", str(query or ""))
    lowered_tokens = [token.lower() for token in raw_symbol_tokens]
    has_ton_hint = "ton" in lowered_tokens
    concise_query = len([token for token in lowered_tokens if token not in REGISTRY_STOPWORDS]) <= 2

    add(query)
    for token in raw_symbol_tokens:
        lowered = token.lower()
        if lowered in REGISTRY_STOPWORDS or lowered == "ton" or not token.isalnum():
            continue
        if 3 <= len(lowered) <= 4 and (token.isupper() or (has_ton_hint and concise_query)):
            add(token)
            add(f"{token} telegram")
            add(f"{token} official")
    for term in brand_terms:
        add(term)
        add(f"{term} ton")
        add(f"{term} official")
    for term in _github_search_terms(query):
        add(term)
    add(f"{query} telegram")
    add(f"{query} github")
    add(f"{query} official")
    if "telegram" in low_query and "wallet" in low_query:
        add("wallet.tg")
        add("telegram wallet ton")
    return queries[:8]


def _match_brand_link_value(values: Iterable[str], brand_tokens: List[str], extractor: Callable[[str], str]) -> str:
    extracted_values: List[str] = []
    seen = set()
    for value in values:
        extracted = extractor(str(value))
        if not extracted:
            continue
        key = extracted.lower()
        if key in seen:
            continue
        seen.add(key)
        extracted_values.append(extracted)
    if not extracted_values:
        return ""
    if not brand_tokens:
        return ""
    for extracted in extracted_values:
        compact = re.sub(r"[^a-z0-9]+", "", extracted.lower())
        if any(token in compact for token in brand_tokens):
            return extracted
    return ""


async def _public_identity_result_candidate(result_url: str, query: str, speed_profile: str = "full") -> Optional[Dict[str, Any]]:
    domain = _url_domain(result_url)
    if not domain or domain in PUBLIC_WEB_IDENTITY_EXCLUDED_DOMAINS:
        return None
    query_tokens = _discovery_match_tokens(query)
    brand_stopwords = {"ton", "wallet", "dex", "swap", "exchange", "marketplace", "lending", "borrow", "loan", "protocol", "nft", "sdk", "tooling", "contract", "contracts", "app"}
    brand_tokens = [
        token
        for token in {
            re.sub(r"[^a-z0-9]+", "", item.lower())
            for item in query_tokens
        }
        if len(token) >= 4 and token not in brand_stopwords
    ]
    compact_domain = re.sub(r"[^a-z0-9]+", "", domain.lower())
    domain_brand_match = any(token and token in compact_domain for token in brand_tokens)
    path_depth = len([segment for segment in urlparse(result_url).path.split("/") if segment])
    telegram_handle = _extract_telegram_handle(result_url)
    github_repo = _extract_github_repo(result_url)
    if github_repo:
        try:
            repo_meta = await _fetch_github_repo_meta(github_repo, speed_profile)
        except Exception:
            repo_meta = {}
        if repo_meta:
            haystack = normalize_ws(
                " ".join(
                    [
                        str(repo_meta.get("full_name") or ""),
                        str(repo_meta.get("description") or ""),
                        str(repo_meta.get("homepage") or ""),
                    ]
                )
            )
            score = _token_overlap_score(query_tokens, _discovery_match_tokens(haystack))
            if score <= 0:
                return None
            return _candidate_from_repo_meta(
                repo_meta,
                score=round(score + 0.45, 4),
                source_label="public_web",
                reason="Matched a GitHub repository in public web search results.",
            )
    if telegram_handle:
        haystack = normalize_ws(" ".join([query, telegram_handle, result_url]))
        score = _token_overlap_score(query_tokens, _discovery_match_tokens(haystack))
        return _finalize_candidate(
            {
                "name": _display_name_from_title(query, telegram_handle),
                "github_repo": "",
                "project_url": f"https://t.me/{telegram_handle}",
                "telegram_handle": telegram_handle,
                "wallet_address": "",
                "description": f"Public Telegram channel candidate @{telegram_handle}.",
                "project_type": "",
                "source_labels": ["public_web"],
                "match_reason": "Matched a public Telegram channel in web search results.",
                "score": round(score + 0.6, 4),
            }
        )
    try:
        page_html = await http_fetch_text(
            result_url,
            headers=TELEGRAM_HEADERS,
            timeout=_profile_timeout(speed_profile, 8, 45),
        )
    except Exception:
        return None
    page_text = strip_html(page_html)
    page_title = _extract_html_title(page_html)
    page_urls = _extract_external_url_candidates(page_html)
    telegram_handle = _match_brand_link_value(page_urls, brand_tokens, _extract_telegram_handle)
    github_repo = _match_brand_link_value(page_urls, brand_tokens, _extract_github_repo)
    if telegram_handle and brand_tokens and not domain_brand_match:
        compact_handle = re.sub(r"[^a-z0-9]+", "", telegram_handle.lower())
        if not any(token in compact_handle for token in brand_tokens):
            telegram_handle = ""
    if github_repo and brand_tokens and not domain_brand_match:
        compact_repo = re.sub(r"[^a-z0-9]+", "", github_repo.lower())
        if not any(token in compact_repo for token in brand_tokens):
            github_repo = ""
    wallet_address = ""
    haystack = normalize_ws(" ".join([page_title, clip_text(page_text, 1200), result_url, telegram_handle, github_repo]))
    score = _token_overlap_score(query_tokens, _discovery_match_tokens(haystack))
    if telegram_handle:
        score += 0.35
    if github_repo:
        score += 0.25
    if domain_brand_match and path_depth <= 1:
        score += 0.18
    elif not domain_brand_match and path_depth >= 2:
        score -= 0.35
    domain_compact = domain.replace(".", "").replace("-", "")
    for term in _github_search_terms(query):
        compact_term = re.sub(r"[^a-z0-9]+", "", term.lower())
        if compact_term and compact_term in domain_compact:
            score += 0.25
            break
    if score <= 0:
        return None
    return _finalize_candidate(
        {
            "name": _display_name_from_title(page_title, domain),
            "github_repo": github_repo,
            "project_url": result_url,
            "telegram_handle": telegram_handle,
            "wallet_address": wallet_address,
            "description": page_title or f"Public website candidate {domain}.",
            "project_type": "",
            "source_labels": ["public_web"],
            "match_reason": "Matched a public website result and resolved project identity signals.",
            "score": round(score, 4),
        }
    )


async def _public_identity_candidates(query: str, speed_profile: str = "full") -> List[Dict[str, Any]]:
    result_urls: List[str] = []
    seen_urls = set()
    last_error: Exception | None = None
    successful_search = False
    query_limit = _profile_limit(speed_profile, 4, 8)
    for search_query in _public_identity_search_queries(query)[:query_limit]:
        try:
            search_html = await http_fetch_text(
                f"https://html.duckduckgo.com/html/?q={quote_plus(search_query)}",
                headers=TGSTAT_HEADERS,
                timeout=_profile_timeout(speed_profile, 8, 45),
            )
            successful_search = True
        except Exception as exc:
            last_error = exc
            continue
        for result_url in _duckduckgo_result_urls(search_html):
            if result_url in seen_urls:
                continue
            seen_urls.add(result_url)
            result_urls.append(result_url)
            if len(result_urls) >= _profile_limit(speed_profile, 4, 8):
                break
        if len(result_urls) >= _profile_limit(speed_profile, 4, 8):
            break
    if not result_urls:
        if last_error is not None and not successful_search:
            raise last_error
        return []
    results = await asyncio.gather(
        *[_public_identity_result_candidate(result_url, query, speed_profile) for result_url in result_urls],
        return_exceptions=True,
    )
    candidates: List[Dict[str, Any]] = []
    for result in results:
        if isinstance(result, Exception) or not isinstance(result, dict):
            continue
        if result:
            candidates.append(result)
    return _merge_candidates(candidates)


async def _enrich_candidates_with_public_identity(
    candidates: List[Dict[str, Any]],
    query: str,
    speed_profile: str = "full",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    identity_candidates: List[Dict[str, Any]] = []
    identity_error = ""
    if _should_skip_public_web_identity(candidates, query, speed_profile):
        return candidates, {
            "status": "skipped",
            "candidate_count": 0,
            "summary": "Skipped public web enrichment because the current top candidate already has strong identity signals.",
        }
    try:
        identity_candidates = await _public_identity_candidates(query, speed_profile)
    except Exception as exc:
        identity_error = clip_text(str(exc), 240)
        combined = candidates
    else:
        combined = _merge_candidates(list(candidates) + identity_candidates)
    enriched, wallet_status = await _enrich_candidates_with_public_wallets(combined, query, speed_profile)
    public_identity_count = len(identity_candidates)
    wallet_count = int(wallet_status.get("candidate_count") or 0)
    if public_identity_count or wallet_count:
        parts: List[str] = []
        if public_identity_count:
            parts.append(f"Public web search identified {public_identity_count} candidate(s) with website or Telegram evidence.")
        if wallet_count:
            parts.append(f"Wallet enrichment resolved {wallet_count} candidate(s).")
        return enriched, {
            "status": "success",
            "candidate_count": public_identity_count + wallet_count,
            "summary": " ".join(parts),
        }
    if identity_error:
        return enriched, {
            "status": "error",
            "candidate_count": 0,
            "summary": identity_error,
        }
    return enriched, wallet_status


def _github_search_repo_names_from_html(page_html: str) -> List[str]:
    match = re.search(
        r'<script type="application/json" data-target="react-app\.embeddedData">(.*?)</script>',
        str(page_html or ""),
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return []
    try:
        payload = json.loads(html.unescape(match.group(1)))
    except json.JSONDecodeError:
        return []
    results = [item for item in ((payload.get("payload") or {}).get("results") or []) if isinstance(item, dict)]
    out: List[str] = []
    seen = set()
    for item in results:
        repo_payload = (((item.get("repo") or {}).get("repository") or {}))
        owner_login = normalize_ws(str(repo_payload.get("owner_login") or ""))
        repo_name = normalize_ws(str(repo_payload.get("name") or ""))
        full_name = f"{owner_login}/{repo_name}".strip("/")
        if not owner_login or not repo_name or full_name.lower() in seen:
            continue
        seen.add(full_name.lower())
        out.append(full_name)
    return out


async def _fetch_github_repo_meta(github_repo: str, speed_profile: str = "full") -> Dict[str, Any]:
    return await fetch_github_repo_meta(github_repo, speed_profile)


async def _direct_github_candidate(github_repo: str, speed_profile: str = "full") -> Optional[Dict[str, Any]]:
    if not github_repo:
        return None
    repo_meta = await _fetch_github_repo_meta(github_repo, speed_profile)
    return _candidate_from_repo_meta(
        repo_meta,
        score=1.6,
        source_label="direct_github",
        reason="Exact GitHub repository was provided in the request.",
    )


async def _github_search_candidates(query: str, explicit_repo: str, explicit_url: str, speed_profile: str = "full") -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    seen_repo_names = set()
    last_error: Exception | None = None
    successful_search = False
    query_has_ton_hint = bool(re.search(r"\bton\b", str(query or "").lower()))
    term_limit = _profile_limit(speed_profile, 3, 6)
    for term in _github_search_terms(query)[:term_limit]:
        try:
            payload = await http_fetch_json(
                f"https://api.github.com/search/repositories?q={quote_plus(term)}&per_page=5",
                headers=GITHUB_HEADERS,
                timeout=_profile_timeout(speed_profile, 10, 30),
            )
            successful_search = True
        except Exception as exc:
            last_error = exc
            continue
        for item in payload.get("items") or []:
            if not isinstance(item, dict):
                continue
            full_name = str(item.get("full_name") or "")
            if not full_name or full_name in seen_repo_names:
                continue
            seen_repo_names.add(full_name)
            items.append(item)
        if len(items) >= 8:
            break
    if len(items) < 6:
        for term in _github_search_terms(query)[:term_limit]:
            try:
                page_html = await http_fetch_text(
                    f"https://github.com/search?q={quote_plus(term)}&type=repositories",
                    headers=GITHUB_HTML_HEADERS,
                    timeout=_profile_timeout(speed_profile, 8, 45),
                )
                successful_search = True
            except Exception as exc:
                last_error = exc
                continue
            for full_name in _github_search_repo_names_from_html(page_html):
                if not full_name or full_name in seen_repo_names:
                    continue
                seen_repo_names.add(full_name)
                items.append({"full_name": full_name})
            if len(items) >= 8:
                break
    if not items and last_error is not None and not successful_search:
        raise last_error
    query_tokens = _discovery_match_tokens(query)
    repo_names = [str(item.get("full_name") or "") for item in items[:8] if str(item.get("full_name") or "")]
    detailed = await asyncio.gather(
        *[_fetch_github_repo_meta(repo_name, speed_profile) for repo_name in repo_names],
        return_exceptions=True,
    )
    candidates: List[Dict[str, Any]] = []
    for raw_meta in detailed:
        if isinstance(raw_meta, Exception):
            continue
        repo_meta = dict(raw_meta)
        haystack = normalize_ws(
            " ".join(
                [
                    str(repo_meta.get("full_name") or ""),
                    str(repo_meta.get("description") or ""),
                    " ".join(str(item) for item in (repo_meta.get("topics") or []) if item),
                    str(repo_meta.get("homepage") or ""),
                ]
            )
        )
        compact_haystack = re.sub(r"[^a-z0-9]+", "", haystack.lower())
        score = _token_overlap_score(query_tokens, _discovery_match_tokens(haystack))
        reasons = ["Matched GitHub repository search."]
        joined_match = False
        for term in _compound_discovery_terms(query):
            compact_term = re.sub(r"[^a-z0-9]+", "", term.lower())
            if len(compact_term) >= 4 and compact_term in compact_haystack:
                score += 0.45
                joined_match = True
                reasons.append("joined project name match")
                break
        haystack_has_ton_hint = bool(re.search(r"\bton\b", haystack.lower()))
        if query_has_ton_hint and not haystack_has_ton_hint and not joined_match:
            continue
        if explicit_repo and str(repo_meta.get("full_name") or "").lower() == explicit_repo.lower():
            score += 1.0
            reasons = ["Exact GitHub repository match."]
        homepage = str(repo_meta.get("homepage") or "")
        if explicit_url and homepage and _url_domain(homepage) == _url_domain(explicit_url):
            score += 0.6
            reasons.append("homepage domain matches the request")
        if score <= 0:
            continue
        candidates.append(
            _candidate_from_repo_meta(
                repo_meta,
                score=round(score, 4),
                source_label="github_search",
                reason=", ".join(reasons),
            )
        )
    return sorted(candidates, key=lambda item: _candidate_query_rank_score(item, query), reverse=True)


async def _coingecko_candidates(query: str, speed_profile: str = "full") -> List[Dict[str, Any]]:
    seen_coin_ids = set()
    coins: List[Dict[str, Any]] = []
    last_error: Exception | None = None
    successful_search = False
    term_limit = _profile_limit(speed_profile, 3, 8)
    request_timeout = _profile_timeout(speed_profile, 10, 45)
    for term in _market_search_terms(query)[:term_limit]:
        try:
            search_payload = await http_fetch_json(
                f"https://api.coingecko.com/api/v3/search?query={quote_plus(term)}",
                headers=MARKET_API_HEADERS,
                timeout=request_timeout,
            )
            successful_search = True
        except Exception as exc:
            last_error = exc
            continue
        for item in search_payload.get("coins") or []:
            if not isinstance(item, dict):
                continue
            coin_id = str(item.get("id") or "")
            if not coin_id or coin_id in seen_coin_ids:
                continue
            seen_coin_ids.add(coin_id)
            coins.append(item)
    if not coins and last_error is not None and not successful_search:
        raise last_error
    details = await asyncio.gather(
        *[
            http_fetch_json(
                f"https://api.coingecko.com/api/v3/coins/{quote_plus(str(coin.get('id') or ''))}"
                "?localization=false&tickers=false&market_data=false&community_data=false&developer_data=false&sparkline=false",
                headers=MARKET_API_HEADERS,
                timeout=request_timeout,
            )
            for coin in coins[: _profile_limit(speed_profile, 4, 6)]
            if str(coin.get("id") or "")
        ],
        return_exceptions=True,
    )
    query_tokens = _discovery_match_tokens(query)
    candidates: List[Dict[str, Any]] = []
    for detail in details:
        if isinstance(detail, Exception):
            continue
        payload = dict(detail)
        categories = [str(item) for item in payload.get("categories") or [] if item]
        description = normalize_ws(str((payload.get("description") or {}).get("en") or ""))
        links = payload.get("links") or {}
        homepage_values = [normalize_ws(str(item)) for item in (links.get("homepage") or []) if normalize_ws(str(item))]
        telegram_handle = str(links.get("telegram_channel_identifier") or "")
        twitter_handle = str(links.get("twitter_screen_name") or "")
        wallet_address = normalize_ws(
            str(
                payload.get("contract_address")
                or ((payload.get("detail_platforms") or {}).get("the-open-network") or {}).get("contract_address")
                or ""
            )
        )
        identity_text = normalize_ws(
            " ".join(
                [
                    str(payload.get("name") or ""),
                    str(payload.get("symbol") or ""),
                    description,
                ]
            )
        )
        homepage = _select_market_identity_url(homepage_values, identity_text)
        haystack = normalize_ws(
            " ".join(
                [
                    str(payload.get("name") or ""),
                    str(payload.get("symbol") or ""),
                    description,
                    " ".join(categories),
                    homepage,
                    telegram_handle,
                    twitter_handle,
                    wallet_address,
                    str(payload.get("asset_platform_id") or ""),
                ]
            )
        )
        score = _token_overlap_score(query_tokens, _discovery_match_tokens(haystack))
        if str(payload.get("asset_platform_id") or "") == "the-open-network":
            score += 0.6
        if any("TON" in category.upper() for category in categories):
            score += 0.2
        if homepage and _url_domain(homepage):
            score += 0.05
        if score <= 0:
            continue
        candidates.append(
            _market_candidate(
                name=str(payload.get("name") or ""),
                symbol=str(payload.get("symbol") or "").upper(),
                project_url=homepage,
                telegram_handle=telegram_handle,
                wallet_address=wallet_address,
                description=description or f"CoinGecko listing for {payload.get('name') or payload.get('symbol') or 'listed asset'}.",
                project_type=categories[0] if categories else "",
                score=round(score, 4),
                source_label="coingecko",
                reason="Matched token and market listing on CoinGecko.",
            )
        )
    return sorted(candidates, key=lambda item: float(item.get("score") or 0.0), reverse=True)


def _extract_wallet_from_geckoterminal_id(value: str) -> str:
    raw = normalize_ws(value)
    if raw.startswith("ton_"):
        return raw.split("ton_", 1)[1]
    return raw


async def _geckoterminal_candidates(query: str, speed_profile: str = "full") -> List[Dict[str, Any]]:
    pools: List[Dict[str, Any]] = []
    seen_pool_ids = set()
    last_error: Exception | None = None
    successful_search = False
    term_limit = _profile_limit(speed_profile, 3, 8)
    request_timeout = _profile_timeout(speed_profile, 10, 45)
    for term in _market_search_terms(query)[:term_limit]:
        try:
            payload = await http_fetch_json(
                f"https://api.geckoterminal.com/api/v2/search/pools?query={quote_plus(term)}",
                headers=MARKET_API_HEADERS,
                timeout=request_timeout,
            )
            successful_search = True
        except Exception as exc:
            last_error = exc
            continue
        for item in payload.get("data") or []:
            if not isinstance(item, dict):
                continue
            pool_id = str(item.get("id") or "")
            if not pool_id or pool_id in seen_pool_ids:
                continue
            seen_pool_ids.add(pool_id)
            pools.append(item)
    if not pools and last_error is not None and not successful_search:
        raise last_error
    query_tokens = _discovery_match_tokens(query)
    candidates: List[Dict[str, Any]] = []
    for pool in pools[: _profile_limit(speed_profile, 6, 10)]:
        pool_id = str(pool.get("id") or "")
        if not pool_id.startswith("ton_"):
            continue
        attributes = pool.get("attributes") or {}
        relationships = pool.get("relationships") or {}
        base_token = (relationships.get("base_token") or {}).get("data") or {}
        quote_token = (relationships.get("quote_token") or {}).get("data") or {}
        base_token_id = str(base_token.get("id") or "")
        quote_token_id = str(quote_token.get("id") or "")
        if not base_token_id.startswith("ton_") or (quote_token_id and not quote_token_id.startswith("ton_")):
            continue
        wallet_address = _extract_wallet_from_geckoterminal_id(str(base_token.get("id") or ""))
        if not wallet_address:
            continue
        name = str(attributes.get("name") or "")
        pool_address = str(attributes.get("address") or "")
        project_url = f"https://www.geckoterminal.com/ton/pools/{pool_address}" if pool_address else ""
        haystack = normalize_ws(" ".join([name, wallet_address, project_url, pool_id]))
        score = _token_overlap_score(query_tokens, _discovery_match_tokens(haystack))
        if " / TON" in name.upper():
            score += 0.25
        if score <= 0:
            continue
        symbol = name.split("/", 1)[0].strip()
        candidates.append(
            _market_candidate(
                name=symbol or name,
                symbol=symbol,
                project_url=project_url,
                telegram_handle="",
                wallet_address=wallet_address,
                description=f"GeckoTerminal pool candidate {name}.",
                project_type="",
                score=round(score, 4),
                source_label="geckoterminal",
                reason="Matched TON pool and token candidate on GeckoTerminal.",
            )
        )
    return sorted(candidates, key=lambda item: float(item.get("score") or 0.0), reverse=True)


class _NullTraceStore:
    def save_llm_trace(self, block_id: str, model: str, prompt: str, response: str, metadata: Dict[str, Any]) -> None:
        del block_id, model, prompt, response, metadata


async def _rank_candidates_with_llm(
    query: str,
    candidates: List[Dict[str, Any]],
    *,
    llm_mode: str,
    model: str,
    speed_profile: str = "full",
) -> Dict[str, Any]:
    ordered_candidates = sorted(
        [_finalize_candidate(candidate) for candidate in candidates],
        key=lambda item: _candidate_query_rank_score(item, query),
        reverse=True,
    )
    if _should_skip_discovery_llm(ordered_candidates, query, speed_profile):
        out: List[Dict[str, Any]] = []
        for candidate in ordered_candidates:
            merged = dict(candidate)
            merged["score"] = _candidate_query_rank_score(candidate, query)
            out.append(_finalize_candidate(merged))
        top_candidate = out[0]
        return {
            "query": query,
            "summary": _deterministic_discovery_summary(top_candidate),
            "selected_candidate_key": str(top_candidate.get("candidate_key") or ""),
            "candidates": out,
        }
    if str(llm_mode or "").lower() != "live":
        out: List[Dict[str, Any]] = []
        for candidate in ordered_candidates:
            merged = dict(candidate)
            merged["score"] = _candidate_query_rank_score(candidate, query)
            out.append(_finalize_candidate(merged))
        return {
            "query": query,
            "summary": "Deterministic candidate list prepared. Live model ranking is disabled in template mode.",
            "selected_candidate_key": "",
            "candidates": out,
        }
    llm_client = build_llm_client(llm_mode)
    trace_store = _NullTraceStore()
    prompt = json.dumps(
        {
            "task": "Rank project identification candidates for a free-form user request.",
            "rules": [
                "Use only the provided candidates.",
                "Do not invent new candidate keys.",
                "Prefer exact repo, website, or telegram matches when present.",
                "Prefer candidates with consistent cross-source identity signals over bare market pool matches.",
                "When the request targets a product or protocol, prefer the official product site over a tooling SDK or adjacent repository.",
                "Return only strict JSON.",
            ],
            "response_format": {
                "summary": "short string",
                "selected_candidate_key": "candidate key string",
                "ranked_candidates": [
                    {
                        "candidate_key": "candidate key string from input",
                        "score": "number between 0 and 1",
                        "reason": "short explanation",
                        "name": "better human-readable name or empty string",
                        "project_url": "canonical project site or empty string",
                        "telegram_handle": "telegram handle without @ or empty string",
                        "project_type": "short type/domain or empty string",
                    }
                ],
            },
            "query": query,
            "candidates": ordered_candidates,
        },
        ensure_ascii=False,
        indent=2,
    )
    raw = await llm_client.complete(
        block_id="project_discovery",
        model=model,
        prompt=prompt,
        trace_store=trace_store,  # type: ignore[arg-type]
        metadata={"query": query, "candidate_count": len(ordered_candidates)},
    )
    payload = _parse_llm_json_object(raw)
    summary = str(payload.get("summary") or "")
    selected_candidate_key = str(payload.get("selected_candidate_key") or "")
    ranked_candidates = [item for item in payload.get("ranked_candidates") or [] if isinstance(item, dict)]
    ranked_map = {str(item.get("candidate_key") or ""): item for item in ranked_candidates}
    deterministic_scores = {
        str(candidate.get("candidate_key") or ""): _candidate_query_rank_score(candidate, query)
        for candidate in ordered_candidates
    }
    out: List[Dict[str, Any]] = []
    for candidate in ordered_candidates:
        key = str(candidate.get("candidate_key") or "")
        ranked = ranked_map.get(key) or {}
        merged = dict(candidate)
        if ranked.get("name") and not merged.get("name"):
            merged["name"] = str(ranked.get("name") or "")
        if ranked.get("project_url"):
            ranked_project_url = _canonical_project_url_from_text(
                str(ranked.get("project_url") or ""),
                " ".join(
                    [
                        query,
                        str(merged.get("github_repo") or ""),
                        str(merged.get("name") or ""),
                        str(merged.get("description") or ""),
                    ]
                ),
            )
            if ranked_project_url:
                merged["project_url"] = ranked_project_url
        if ranked.get("telegram_handle"):
            merged["telegram_handle"] = str(ranked.get("telegram_handle") or "")
        if ranked.get("project_type"):
            merged["project_type"] = str(ranked.get("project_type") or "")
        if ranked.get("reason"):
            merged["match_reason"] = str(ranked.get("reason") or "")
        deterministic_score = float(deterministic_scores.get(key) or 0.0)
        llm_score = float(ranked.get("score") or 0.0) if ranked.get("score") is not None else None
        merged["score"] = (
            round((llm_score * 0.55) + (deterministic_score * 0.45), 4)
            if llm_score is not None
            else deterministic_score
        )
        out.append(_finalize_candidate(merged))
    out.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    valid_candidate_keys = {str(item.get("candidate_key") or "") for item in out}
    if selected_candidate_key not in valid_candidate_keys:
        selected_candidate_key = ""
    if selected_candidate_key and out:
        selected_candidate = next((item for item in out if str(item.get("candidate_key") or "") == selected_candidate_key), {})
        top_candidate = out[0]
        selected_score = float(selected_candidate.get("score") or 0.0)
        top_score = float(top_candidate.get("score") or 0.0)
        selected_is_canonical = _is_canonical_project_url(str(selected_candidate.get("project_url") or ""))
        top_is_canonical = _is_canonical_project_url(str(top_candidate.get("project_url") or ""))
        if (
            str(top_candidate.get("candidate_key") or "") != selected_candidate_key
            and (
                top_score > selected_score
                or (top_is_canonical and not selected_is_canonical)
            )
        ):
            selected_candidate_key = str(top_candidate.get("candidate_key") or "")
    return {
        "query": query,
        "summary": summary,
        "selected_candidate_key": selected_candidate_key,
        "candidates": out,
    }


async def discover_project_candidates(
    payload: Dict[str, Any],
    event_handler: Optional[Callable[[str, str, Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    query = normalize_ws(str(payload.get("query") or ""))
    speed_profile = _normalized_speed_profile(str(payload.get("speed_profile") or "interactive"))
    if not query:
        raise ValueError("Query is required")

    def emit(event_type: str, unit_id: str, result: Optional[Dict[str, Any]] = None) -> None:
        if event_handler is None:
            return
        event_handler(event_type, unit_id, dict(result or {}))

    explicit_repo = _extract_github_repo(query)
    explicit_url = _extract_first_url(query)
    explicit_telegram = _extract_telegram_handle(query)
    candidates: List[Dict[str, Any]] = []
    source_statuses: Dict[str, Dict[str, Any]] = {}

    emit("start", "discovery_parse_query")
    hint_candidate = _user_hint_candidate(query, explicit_repo, explicit_url, explicit_telegram)
    parse_summary = "Normalized the free-form project request into a candidate search query."
    if hint_candidate:
        candidates.append(hint_candidate)
        source_statuses["user_input"] = {
            "status": "success",
            "candidate_count": 1,
            "summary": "Detected an explicit reference in the free-form input.",
        }
        parse_summary = f"{parse_summary} Detected an explicit project reference in the input."
    emit("finish", "discovery_parse_query", _discovery_result("success", parse_summary))

    emit("start", "discovery_registry_search")
    registry_candidates = _registry_discovery_candidates(query, explicit_repo, explicit_url, explicit_telegram)
    candidates.extend(registry_candidates)
    source_statuses["registry"] = {
        "status": "success" if registry_candidates else "skipped",
        "candidate_count": len(registry_candidates),
        "summary": "Searched the local TON project registry.",
    }
    emit(
        "finish",
        "discovery_registry_search",
        _discovery_result(
            source_statuses["registry"]["status"],
            f"{source_statuses['registry']['summary']} Candidates: {len(registry_candidates)}.",
        ),
    )
    skip_external_discovery = _should_skip_external_discovery_from_registry(
        registry_candidates,
        query,
        speed_profile,
        explicit_repo=explicit_repo,
        explicit_url=explicit_url,
        explicit_telegram=explicit_telegram,
    )

    async def run_github_search() -> List[Dict[str, Any]]:
        emit("start", "discovery_github_search")
        github_candidates: List[Dict[str, Any]] = []
        github_status = "skipped"
        direct_status = "skipped"
        direct_count = 0
        try:
            github_candidates = await _github_search_candidates(query, explicit_repo, explicit_url, speed_profile)
        except Exception as exc:
            source_statuses["github_search"] = {
                "status": "error",
                "candidate_count": 0,
                "summary": clip_text(str(exc), 240),
            }
            github_status = "error"
        else:
            source_statuses["github_search"] = {
                "status": "success" if github_candidates else "skipped",
                "candidate_count": len(github_candidates),
                "summary": "Searched GitHub repositories for matching projects.",
            }
            github_status = source_statuses["github_search"]["status"]
        direct_candidates: List[Dict[str, Any]] = []
        if explicit_repo:
            try:
                direct_candidate = await _direct_github_candidate(explicit_repo, speed_profile)
            except Exception as exc:
                source_statuses["direct_github"] = {
                    "status": "error",
                    "candidate_count": 0,
                    "summary": clip_text(str(exc), 240),
                }
                direct_status = "error"
            else:
                if direct_candidate:
                    direct_candidates.append(direct_candidate)
                    source_statuses["direct_github"] = {
                        "status": "success",
                        "candidate_count": 1,
                        "summary": "Loaded the exact GitHub repository provided in the input.",
                    }
                    direct_status = "success"
                    direct_count = 1
                else:
                    source_statuses["direct_github"] = {
                        "status": "skipped",
                        "candidate_count": 0,
                        "summary": "The explicit GitHub repository hint did not return a candidate.",
                    }
        unit_status = _runtime_status([github_status, direct_status])
        unit_count = len(github_candidates) + direct_count
        unit_summary = "Searched GitHub repositories and exact repo hints."
        emit(
            "finish",
            "discovery_github_search",
            _discovery_result(unit_status, f"{unit_summary} Candidates: {unit_count}."),
        )
        return direct_candidates + github_candidates

    async def run_market_search() -> List[Dict[str, Any]]:
        emit("start", "discovery_market_search")
        coingecko_candidates, geckoterminal_candidates = await asyncio.gather(
            _coingecko_candidates(query, speed_profile),
            _geckoterminal_candidates(query, speed_profile),
            return_exceptions=True,
        )
        out: List[Dict[str, Any]] = []
        if isinstance(coingecko_candidates, Exception):
            source_statuses["coingecko"] = {
                "status": "error",
                "candidate_count": 0,
                "summary": clip_text(str(coingecko_candidates), 240),
            }
        else:
            out.extend(coingecko_candidates)
            source_statuses["coingecko"] = {
                "status": "success" if coingecko_candidates else "skipped",
                "candidate_count": len(coingecko_candidates),
                "summary": "Searched CoinGecko for token and market listings.",
            }
        if isinstance(geckoterminal_candidates, Exception):
            source_statuses["geckoterminal"] = {
                "status": "error",
                "candidate_count": 0,
                "summary": clip_text(str(geckoterminal_candidates), 240),
            }
        else:
            out.extend(geckoterminal_candidates)
            source_statuses["geckoterminal"] = {
                "status": "success" if geckoterminal_candidates else "skipped",
                "candidate_count": len(geckoterminal_candidates),
                "summary": "Searched GeckoTerminal TON pools and token candidates.",
            }
        market_status = _runtime_status(
            [
                str((source_statuses.get("coingecko") or {}).get("status") or "skipped"),
                str((source_statuses.get("geckoterminal") or {}).get("status") or "skipped"),
            ]
        )
        emit(
            "finish",
            "discovery_market_search",
            _discovery_result(market_status, f"Searched CoinGecko and GeckoTerminal for token and market candidates. Candidates: {len(out)}."),
        )
        return out

    if skip_external_discovery:
        source_statuses["github_search"] = {
            "status": "skipped",
            "candidate_count": 0,
            "summary": "Skipped GitHub search because the local registry already produced a strong candidate.",
        }
        source_statuses["coingecko"] = {
            "status": "skipped",
            "candidate_count": 0,
            "summary": "Skipped CoinGecko search because the local registry already produced a strong candidate.",
        }
        source_statuses["geckoterminal"] = {
            "status": "skipped",
            "candidate_count": 0,
            "summary": "Skipped GeckoTerminal search because the local registry already produced a strong candidate.",
        }
        source_statuses["public_web"] = {
            "status": "skipped",
            "candidate_count": 0,
            "summary": "Skipped public web enrichment because the local registry already produced a strong candidate.",
        }
        emit(
            "finish",
            "discovery_github_search",
            _discovery_result("skipped", "Skipped GitHub search because the local registry already produced a strong candidate."),
        )
        emit(
            "finish",
            "discovery_market_search",
            _discovery_result("skipped", "Skipped market discovery because the local registry already produced a strong candidate."),
        )
        emit(
            "finish",
            "discovery_public_web_search",
            _discovery_result("skipped", "Skipped public web enrichment because the local registry already produced a strong candidate."),
        )
        merged = _merge_candidates(candidates)
    else:
        github_candidates, market_candidates = await asyncio.gather(
            asyncio.create_task(run_github_search()),
            asyncio.create_task(run_market_search()),
        )
        candidates.extend(github_candidates)
        candidates.extend(market_candidates)

        emit("start", "discovery_public_web_search")
        merged = _merge_candidates(candidates)[:6]
        try:
            merged, public_web_status = await _enrich_candidates_with_public_identity(merged, query, speed_profile)
        except Exception as exc:
            public_web_status = {
                "status": "error",
                "candidate_count": 0,
                "summary": clip_text(str(exc), 240),
            }
        merged = _merge_candidates(merged)
        source_statuses["public_web"] = public_web_status
        emit(
            "finish",
            "discovery_public_web_search",
            _discovery_result(
                str(public_web_status.get("status") or "pending"),
                f"{public_web_status.get('summary') or 'Searched public TON web sources.'} Resolved candidates: {int(public_web_status.get('candidate_count') or 0)}.",
            ),
        )

    emit("start", "discovery_rank_candidates")
    if not merged:
        summary = "No project candidates were found for this input."
        emit("finish", "discovery_rank_candidates", _discovery_result("skipped", summary))
        return {
            "query": query,
            "summary": summary,
            "selected_candidate_key": "",
            "candidates": [],
            "source_statuses": source_statuses,
            "ranking_status": "skipped",
            "ranking_summary": summary,
        }

    try:
        ranked = await _rank_candidates_with_llm(
            query,
            merged,
            llm_mode=str(payload.get("llm_mode") or "live"),
            model=str(payload.get("discovery_model") or payload.get("llm_model") or "gpt-4o-mini"),
            speed_profile=speed_profile,
        )
    except Exception as exc:
        emit("finish", "discovery_rank_candidates", _discovery_result("error", clip_text(str(exc), 320)))
        raise

    ranked["source_statuses"] = source_statuses
    if not ranked.get("summary"):
        ranked["summary"] = "Choose the most relevant detected project before starting the deep check."
    ranked["ranking_status"] = "success"
    selected_candidate = _selected_discovery_candidate(ranked)
    ranking_summary = str(ranked.get("summary") or "")
    if selected_candidate:
        ranking_summary = (
            f"{ranking_summary} Selected candidate: "
            f"{selected_candidate.get('name') or selected_candidate.get('candidate_key') or 'candidate'}."
        )
    ranked["ranking_summary"] = ranking_summary
    ranked["candidates"] = _disambiguate_candidate_names(
        [dict(item) for item in ranked.get("candidates") or [] if isinstance(item, dict)]
    )
    emit("finish", "discovery_rank_candidates", _discovery_result("success", ranking_summary))
    return ranked


def _selected_discovery_candidate(discovery: Dict[str, Any]) -> Dict[str, Any]:
    candidates = [item for item in discovery.get("candidates") or [] if isinstance(item, dict)]
    selected_key = str(discovery.get("selected_candidate_key") or "")
    if selected_key:
        for candidate in candidates:
            if str(candidate.get("candidate_key") or "") == selected_key:
                return candidate
    return {}


def _source_statuses(discovery: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    raw = discovery.get("source_statuses") or {}
    return {str(key): dict(value) for key, value in raw.items() if isinstance(value, dict)}


def _discovery_status_summary(source_status: Dict[str, Any], default_summary: str) -> Tuple[str, int, str]:
    status = str(source_status.get("status") or "pending")
    candidate_count = int(source_status.get("candidate_count") or 0)
    summary = str(source_status.get("summary") or default_summary)
    return status, candidate_count, summary


def _parse_llm_json_object(raw: Any) -> Dict[str, Any]:
    text = str(raw or "").strip().lstrip("\ufeff")
    if not text:
        raise ValueError("LLM response is empty")
    decoder = json.JSONDecoder()
    candidates: List[str] = [text]
    fenced = re.findall(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.IGNORECASE | re.DOTALL)
    candidates.extend(item.strip() for item in fenced if item.strip())
    seen = set()
    for candidate in candidates:
        normalized = candidate.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        try:
            payload = json.loads(normalized)
        except Exception:
            pass
        else:
            if isinstance(payload, dict):
                return payload
        for index, char in enumerate(normalized):
            if char != "{":
                continue
            try:
                payload, end = decoder.raw_decode(normalized[index:])
            except Exception:
                continue
            trailing = normalized[index + end :].strip()
            if isinstance(payload, dict) and (not trailing or trailing.startswith("```")):
                return payload
    preview = clip_text(text, 320)
    raise ValueError(f"Discovery model returned invalid JSON: {preview}")


def _discovery_atomic_unit(
    *,
    unit_id: str,
    name: str,
    description: str,
    status: str,
    summary: str,
    tags: List[str],
) -> Dict[str, Any]:
    return {
        "unit_id": unit_id,
        "name": name,
        "unit_type": "atomic",
        "execution_mode": "sequential",
        "description": description,
        "summary": summary,
        "tags": tags,
        "input_ports": [],
        "output_ports": [],
        "dependencies": [],
        "runtime": {
            "status": status,
            "duration_ms": 0,
            "unit_type": "atomic",
            "execution_mode": "sequential",
        },
        "result": {
            "status": status,
            "summary": summary,
            "flags": [],
            "needs_human_review": False,
        },
    }


def _build_discovery_plan(discovery: Dict[str, Any]) -> Dict[str, Any]:
    query = normalize_ws(str(discovery.get("query") or ""))
    if not query:
        return {}
    statuses = _source_statuses(discovery)
    selected_candidate = _selected_discovery_candidate(discovery)
    parse_summary = "Normalized the free-form request into a candidate search query."
    registry_status, registry_count, registry_summary = _discovery_status_summary(
        statuses.get("registry") or {},
        "Searched the local TON project registry.",
    )
    github_status = _runtime_status(
        [
            str((statuses.get("github_search") or {}).get("status") or "skipped"),
            str((statuses.get("direct_github") or {}).get("status") or "skipped"),
        ]
    )
    github_count = int((statuses.get("github_search") or {}).get("candidate_count") or 0) + int(
        (statuses.get("direct_github") or {}).get("candidate_count") or 0
    )
    github_summary = "Searched GitHub repositories and exact repo hints."
    market_status = _runtime_status(
        [
            str((statuses.get("coingecko") or {}).get("status") or "skipped"),
            str((statuses.get("geckoterminal") or {}).get("status") or "skipped"),
        ]
    )
    market_count = int((statuses.get("coingecko") or {}).get("candidate_count") or 0) + int(
        (statuses.get("geckoterminal") or {}).get("candidate_count") or 0
    )
    market_summary = "Searched CoinGecko and GeckoTerminal for token and market candidates."
    public_web_status, public_web_count, public_web_summary = _discovery_status_summary(
        statuses.get("public_web") or {},
        "Searched public TON web sources for wallet and collection addresses.",
    )
    ranking_status = str(discovery.get("ranking_status") or ("success" if discovery.get("candidates") else "skipped"))
    ranking_summary = str(discovery.get("ranking_summary") or discovery.get("summary") or "Ranked discovered candidates.")
    if selected_candidate:
        ranking_summary = f"{ranking_summary} Selected candidate: {selected_candidate.get('name') or selected_candidate.get('candidate_key') or 'candidate'}."
    units = [
        _discovery_atomic_unit(
            unit_id="discovery_parse_query",
            name="Normalize query",
            description="Parse and normalize the free-form project request before discovery.",
            status="success",
            summary=parse_summary,
            tags=["deterministic", "discovery"],
        ),
        _discovery_atomic_unit(
            unit_id="discovery_registry_search",
            name="Registry search",
            description="Check the local TON project registry for matching entities.",
            status=registry_status,
            summary=f"{registry_summary} Candidates: {registry_count}.",
            tags=["deterministic", "discovery", "registry"],
        ),
        _discovery_atomic_unit(
            unit_id="discovery_github_search",
            name="GitHub search",
            description="Search GitHub when the project looks code-centric or includes repo hints.",
            status=github_status,
            summary=f"{github_summary} Candidates: {github_count}.",
            tags=["deterministic", "discovery", "github"],
        ),
        _discovery_atomic_unit(
            unit_id="discovery_market_search",
            name="Market search",
            description="Search market and token sources so non-Git TON entities can still be resolved.",
            status=market_status,
            summary=f"{market_summary} Candidates: {market_count}.",
            tags=["hybrid", "discovery", "market", "ton"],
        ),
        _discovery_atomic_unit(
            unit_id="discovery_public_web_search",
            name="Public web wallet search",
            description="Search public TON collection and explorer pages to resolve wallet or contract identifiers for top candidates.",
            status=public_web_status,
            summary=f"{public_web_summary} Resolved candidates: {public_web_count}.",
            tags=["deterministic", "discovery", "wallet", "ton"],
        ),
        _discovery_atomic_unit(
            unit_id="discovery_rank_candidates",
            name="Rank candidates",
            description="Use the model to rank and select the most relevant candidate before deep validation.",
            status=ranking_status,
            summary=ranking_summary,
            tags=["ai", "discovery", "ranking"],
        ),
    ]
    plan = {
        "plan_id": "project_discovery.plan",
        "name": "Project discovery",
        "units": units,
        "stages": [
            {
                "stage_id": "discovery_stage_0",
                "index": 0,
                "unit_ids": ["discovery_parse_query"],
                "runtime": {"status": "success", "duration_ms": 0, "unit_ids": ["discovery_parse_query"]},
            },
            {
                "stage_id": "discovery_stage_1",
                "index": 1,
                "unit_ids": [
                    "discovery_registry_search",
                    "discovery_github_search",
                    "discovery_market_search",
                    "discovery_public_web_search",
                ],
                "runtime": {
                    "status": _runtime_status([registry_status, github_status, market_status, public_web_status]),
                    "duration_ms": 0,
                    "unit_ids": [
                        "discovery_registry_search",
                        "discovery_github_search",
                        "discovery_market_search",
                        "discovery_public_web_search",
                    ],
                },
            },
            {
                "stage_id": "discovery_stage_2",
                "index": 2,
                "unit_ids": ["discovery_rank_candidates"],
                "runtime": {"status": ranking_status, "duration_ms": 0, "unit_ids": ["discovery_rank_candidates"]},
            },
        ],
    }
    return _annotate_presentation(plan)


def _augment_workflow_with_discovery(workflow: Dict[str, Any], discovery: Dict[str, Any]) -> Dict[str, Any]:
    if not discovery or not normalize_ws(str(discovery.get("query") or "")):
        return workflow
    base = copy.deepcopy(workflow)
    discovery_plan = _build_discovery_plan(discovery)
    if not discovery_plan:
        return base
    selected_candidate = _selected_discovery_candidate(discovery)
    discovery_status = _workflow_run_status(discovery_plan)
    discovery_unit = {
        "unit_id": "project_discovery",
        "name": "Project discovery",
        "unit_type": "composite",
        "execution_mode": "sequential",
        "description": "Resolve the project candidate from free-form user input before deep validation.",
        "summary": str(discovery.get("summary") or ""),
        "tags": ["hybrid", "discovery", "precheck"],
        "input_ports": [{"name": "query", "kind": "input", "description": "Free-form project input from the user."}],
        "output_ports": [
            {
                "name": "selected_candidate",
                "kind": "output",
                "description": str(selected_candidate.get("name") or "Resolved project candidate for deep validation."),
            }
        ],
        "dependencies": [],
        "runtime": {
            "status": discovery_status,
            "duration_ms": 0,
            "unit_type": "composite",
            "execution_mode": "sequential",
        },
        "result": {
            "status": discovery_status,
            "summary": str(discovery.get("summary") or ""),
            "flags": [],
            "needs_human_review": False,
        },
        "plan": discovery_plan,
    }
    for stage in base.get("stages") or []:
        stage["index"] = int(stage.get("index") or 0) + 1
    discovery_stage = {
        "stage_id": "stage_discovery",
        "index": 0,
        "unit_ids": ["project_discovery"],
        "runtime": {"status": discovery_status, "duration_ms": 0, "unit_ids": ["project_discovery"]},
    }
    base["units"] = [discovery_unit] + list(base.get("units") or [])
    base["stages"] = [discovery_stage] + list(base.get("stages") or [])
    return _annotate_presentation(base)


@dataclass
class ActiveRunSession:
    run_id: str
    run_dir: Path
    case: ProjectCase
    options: RunOptions
    workflow_template: Dict[str, Any]
    project_key: str
    discovery_payload: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    status: str = "running"
    fatal_error: str = ""
    atomic_runtimes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    atomic_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


class ActiveRunRegistry:
    def __init__(self, artifacts_root: str | Path, validators_root: str | Path) -> None:
        self.artifacts_root = Path(artifacts_root)
        self.validators_root = Path(validators_root)
        self.registry = BlockRegistry(self.validators_root)
        self.sessions: Dict[str, ActiveRunSession] = {}
        self.lock = threading.Lock()

    def list_runs(self) -> List[Dict[str, Any]]:
        active_runs = [self._list_item(session) for session in self._sessions_snapshot()]
        stored_runs = discover_runs(self.artifacts_root)
        combined = sorted(active_runs + stored_runs, key=lambda item: int(item.get("mtime") or 0), reverse=True)
        deduped: List[Dict[str, Any]] = []
        seen_keys = set()
        for item in combined:
            dedupe_key = str(item.get("project_key") or item.get("case_id") or item.get("run_id") or "")
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            deduped.append(item)
        return deduped

    def has(self, run_id: str) -> bool:
        with self.lock:
            return Path(unquote(run_id)).name in self.sessions

    def get_payload(self, run_id: str, locale: str = "en") -> Dict[str, Any]:
        locale = _normalize_locale(locale)
        session = self._get_session(run_id)
        with session.lock:
            core_workflow = _hydrate_live_workflow(session.workflow_template, session.atomic_runtimes, session.atomic_results)
            workflow = _augment_workflow_with_discovery(core_workflow, session.discovery_payload)
            return _build_run_payload_from_workflow(
                run_id=session.run_id,
                root_dir=_safe_relative(session.run_dir),
                case=session.case.to_dict(),
                options=session.options.to_dict(),
                workflow=workflow,
                results=dict(session.atomic_results),
                discovery=session.discovery_payload,
                run_status=session.status or _workflow_run_status(core_workflow),
                is_live=session.status == "running",
                updated_at=int(session.updated_at),
                project_key=session.project_key,
                fatal_error=session.fatal_error,
                error_workflow=core_workflow,
                locale=locale,
            )

    def get_block_detail(self, run_id: str, unit_id: str, raw: bool = False, locale: str = "en") -> Dict[str, Any]:
        locale = _normalize_locale(locale)
        session = self._get_session(run_id)
        with session.lock:
            workflow = _hydrate_live_workflow(session.workflow_template, session.atomic_runtimes, session.atomic_results)
            workflow = _augment_workflow_with_discovery(workflow, session.discovery_payload)
        return _build_block_detail_from_workflow(
            run_dir=session.run_dir,
            workflow=workflow,
            block_id=unit_id,
            validators_root=self.validators_root,
            raw=raw,
            locale=locale,
        )

    def start_run(self, payload: Dict[str, Any], locale: str = "en") -> Dict[str, Any]:
        case = _make_project_case(payload, self.artifacts_root / "input_cases")
        run_dir = ensure_dir(self.artifacts_root / "runs" / case.case_id)
        discovery_payload = dict(payload.get("discovery") or {})
        requested_enable_sonar = bool(payload.get("enable_sonar", True))
        effective_enable_sonar = requested_enable_sonar and _has_llm_api_key("PERPLEXITY_API_KEY")
        options = RunOptions(
            mode=str(payload.get("mode") or "live"),
            llm_mode=str(payload.get("llm_mode") or "live"),
            llm_model=str(payload.get("llm_model") or "gpt-4o-mini"),
            sonar_model=str(payload.get("sonar_model") or "sonar"),
            enable_sonar=effective_enable_sonar,
            record_snapshots=bool(payload.get("record_snapshots", True)),
            speed_profile=_normalized_speed_profile(str(payload.get("speed_profile") or "interactive")),
            output_dir=str(run_dir),
        )
        workflow_template = _annotate_presentation(build_workflow_plan(self.registry).to_dict())
        session = ActiveRunSession(
            run_id=case.case_id,
            run_dir=Path(run_dir),
            case=case,
            options=options,
            workflow_template=workflow_template,
            project_key=_project_key_from_case(case),
            discovery_payload=discovery_payload,
        )
        with self.lock:
            self.sessions[session.run_id] = session
        thread = threading.Thread(target=self._run_session, args=(session,), daemon=True)
        thread.start()
        return self.get_payload(session.run_id, locale=locale)

    def _list_item(self, session: ActiveRunSession) -> Dict[str, Any]:
        payload = self.get_payload(session.run_id)
        return _build_run_list_item(
            run_id=session.run_id,
            root_dir=payload["run"]["root_dir"],
            case=payload["case"],
            options=payload["options"],
            workflow=payload["workflow"],
            results=dict(session.atomic_results),
            mtime=int(session.updated_at),
            is_live=payload["run"]["is_live"],
            run_status=str(payload["run"]["status"] or "pending"),
        )

    def _get_session(self, run_id: str) -> ActiveRunSession:
        with self.lock:
            session = self.sessions.get(Path(unquote(run_id)).name)
        if session is None:
            raise FileNotFoundError(f"Unknown run_id={run_id}")
        return session

    def _sessions_snapshot(self) -> List[ActiveRunSession]:
        with self.lock:
            return list(self.sessions.values())

    def _run_session(self, session: ActiveRunSession) -> None:
        orchestrator = Orchestrator()

        def event_handler(event_type: str, block_id: str, result: Any) -> None:
            with session.lock:
                session.updated_at = time.time()
                current = dict(session.atomic_runtimes.get(block_id) or {})
                if event_type == "start":
                    session.atomic_runtimes[block_id] = {
                        **current,
                        "status": "running",
                        "started_at": time.time(),
                    }
                    return
                trace = _load_trace(session.run_dir, block_id)
                duration_ms = int(trace.get("duration_ms") or current.get("duration_ms") or 0)
                session.atomic_runtimes[block_id] = {
                    **current,
                    "status": str(getattr(result, "status", "pending") or "pending"),
                    "duration_ms": duration_ms,
                }
                if result is not None and hasattr(result, "to_dict"):
                    session.atomic_results[block_id] = result.to_dict()

        try:
            context = asyncio.run(orchestrator.run_case(session.case, session.options, event_handler=event_handler))
            with session.lock:
                session.atomic_results = {block_id: result.to_dict() for block_id, result in context.results.items()}
                summary = _load_summary(session.run_dir)
                if session.discovery_payload:
                    summary["discovery"] = session.discovery_payload
                    write_json(session.run_dir / "run_summary.json", summary)
                workflow = _load_workflow_snapshot(summary, session.run_dir, self.validators_root)
                session.workflow_template = workflow
                session.status = _workflow_run_status(workflow)
                session.updated_at = time.time()
        except Exception as exc:
            with session.lock:
                session.status = "error"
                session.fatal_error = str(exc)
                session.updated_at = time.time()


@dataclass
class ActiveDiscoverySession:
    session_id: str
    query: str
    workflow_template: Dict[str, Any]
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    status: str = "running"
    fatal_error: str = ""
    summary: str = ""
    candidates: List[Dict[str, Any]] = field(default_factory=list)
    selected_candidate_key: str = ""
    source_statuses: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    atomic_runtimes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    atomic_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


class ActiveDiscoveryRegistry:
    def __init__(self) -> None:
        self.sessions: Dict[str, ActiveDiscoverySession] = {}
        self.lock = threading.Lock()

    def has(self, session_id: str) -> bool:
        with self.lock:
            return Path(unquote(session_id)).name in self.sessions

    def start_search(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        query = normalize_ws(str(payload.get("query") or ""))
        if not query:
            raise ValueError("Query is required")
        template = _build_discovery_plan(
            {
                "query": query,
                "summary": "",
                "candidates": [],
                "source_statuses": {},
                "ranking_status": "pending",
                "ranking_summary": "Waiting for ranked candidates.",
            }
        )
        session = ActiveDiscoverySession(
            session_id=f"discovery_{uuid4().hex[:12]}",
            query=query,
            workflow_template=template,
        )
        with self.lock:
            self.sessions[session.session_id] = session
        thread = threading.Thread(target=self._run_session, args=(session, dict(payload)), daemon=True)
        thread.start()
        return self.get_payload(session.session_id)

    def get_payload(self, session_id: str) -> Dict[str, Any]:
        session = self._get_session(session_id)
        with session.lock:
            workflow = _hydrate_live_workflow(session.workflow_template, session.atomic_runtimes, session.atomic_results)
            status = session.status or _workflow_run_status(workflow)
            return _build_discovery_payload(
                session_id=session.session_id,
                query=session.query,
                workflow=workflow,
                summary=session.summary,
                candidates=list(session.candidates),
                source_statuses=dict(session.source_statuses),
                selected_candidate_key=session.selected_candidate_key,
                status=status,
                updated_at=int(session.updated_at),
                fatal_error=session.fatal_error,
            )

    def _get_session(self, session_id: str) -> ActiveDiscoverySession:
        with self.lock:
            session = self.sessions.get(Path(unquote(session_id)).name)
        if session is None:
            raise FileNotFoundError(f"Unknown discovery_session_id={session_id}")
        return session

    def _run_session(self, session: ActiveDiscoverySession, payload: Dict[str, Any]) -> None:
        def event_handler(event_type: str, unit_id: str, result: Dict[str, Any]) -> None:
            with session.lock:
                session.updated_at = time.time()
                current = dict(session.atomic_runtimes.get(unit_id) or {})
                if event_type == "start":
                    session.atomic_runtimes[unit_id] = {
                        **current,
                        "status": "running",
                        "started_at": time.time(),
                    }
                    return
                duration_ms = int(current.get("duration_ms") or 0)
                if current.get("started_at"):
                    duration_ms = max(duration_ms, int((time.time() - float(current.get("started_at"))) * 1000))
                session.atomic_runtimes[unit_id] = {
                    **current,
                    "status": str(result.get("status") or "pending"),
                    "duration_ms": duration_ms,
                }
                session.atomic_results[unit_id] = _discovery_result(
                    str(result.get("status") or "pending"),
                    str(result.get("summary") or ""),
                    flags=[str(item) for item in result.get("flags") or []],
                )

        try:
            result = asyncio.run(discover_project_candidates(payload, event_handler=event_handler))
            with session.lock:
                session.summary = str(result.get("summary") or "")
                session.candidates = [item for item in result.get("candidates") or [] if isinstance(item, dict)]
                session.selected_candidate_key = str(result.get("selected_candidate_key") or "")
                session.source_statuses = {
                    str(key): dict(value)
                    for key, value in (result.get("source_statuses") or {}).items()
                    if isinstance(value, dict)
                }
                workflow = _hydrate_live_workflow(session.workflow_template, session.atomic_runtimes, session.atomic_results)
                session.status = _workflow_run_status(workflow)
                session.updated_at = time.time()
        except Exception as exc:
            with session.lock:
                session.status = "error"
                session.fatal_error = str(exc)
                session.updated_at = time.time()

def _default_presentation_note(unit: Dict[str, Any], kind: str) -> str:
    unit_type = str(unit.get("unit_type") or "")
    name = str(unit.get("name") or unit.get("unit_id") or "unit")
    if kind == "ai":
        return f"{name} is a model-assisted unit in the workflow."
    if kind == "hybrid":
        return f"{name} combines deterministic processing with model-assisted reasoning or synthesis."
    if unit_type == "composite":
        return f"{name} is a grouped deterministic phase with nested execution units."
    return f"{name} is a deterministic and auditable workflow unit."


def _unit_display_name(unit: Dict[str, Any]) -> str:
    return str(unit.get("unit_id") or unit.get("name") or "unit")


def _format_name_list(names: List[str]) -> str:
    return ", ".join(names) if names else "none"


def _stage_presentation(units: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    items = [item for item in units if item]
    counts = {"deterministic": 0, "hybrid": 0, "ai": 0}
    badge_set = set()
    deterministic_units: List[str] = []
    hybrid_units: List[str] = []
    ai_units: List[str] = []
    for unit in items:
        presentation = unit.get("presentation") or {}
        kind = str(presentation.get("kind") or "deterministic")
        counts[kind] = counts.get(kind, 0) + 1
        badge_set.update(str(item) for item in presentation.get("badges") or [])
        unit_name = _unit_display_name(unit)
        if kind == "ai":
            ai_units.append(unit_name)
        elif kind == "hybrid":
            hybrid_units.append(unit_name)
        else:
            deterministic_units.append(unit_name)
    if counts["hybrid"] or (counts["ai"] and counts["deterministic"]):
        kind = "hybrid"
    elif counts["ai"] and not counts["deterministic"]:
        kind = "ai"
    else:
        kind = "deterministic"
    label = {
        "deterministic": "Deterministic stage",
        "hybrid": "Hybrid stage",
        "ai": "Model-assisted stage",
    }[kind]
    note_parts = [f"Units: {_format_name_list([_unit_display_name(unit) for unit in items])}."]
    if hybrid_units:
        note_parts.append(f"Hybrid units: {_format_name_list(hybrid_units)}.")
    if ai_units:
        note_parts.append(f"Model-assisted units: {_format_name_list(ai_units)}.")
    if not hybrid_units and not ai_units and deterministic_units:
        note_parts.append("No model-assisted units in this stage.")
    note = " ".join(note_parts)
    badges = [label]
    if "Model" in badge_set or "LLM" in badge_set:
        badges.append("Model")
    if "Tool" in badge_set:
        badges.append("Tool")
    if "Verified" in badge_set:
        badges.append("Verified")
    return {
        "kind": kind,
        "label": label,
        "badges": badges,
        "note": note,
        "counts": counts,
    }


def _annotate_presentation(plan: Dict[str, Any]) -> Dict[str, Any]:
    summary_counts = {"deterministic": 0, "hybrid": 0, "ai": 0}
    for unit in plan.get("units") or []:
        if unit.get("unit_type") == "composite":
            unit["plan"] = _annotate_presentation(unit.get("plan") or {})
            child_units = (unit.get("plan") or {}).get("units") or []
            child_kinds = [str((child.get("presentation") or {}).get("kind") or "deterministic") for child in child_units]
            if any(kind == "hybrid" for kind in child_kinds) or ("ai" in child_kinds and "deterministic" in child_kinds):
                kind = "hybrid"
            elif child_kinds and all(kind == "ai" for kind in child_kinds):
                kind = "ai"
            else:
                kind = "deterministic"
            badge_set = {"Composite"}
            for child in child_units:
                badge_set.update(str(item) for item in ((child.get("presentation") or {}).get("badges") or []))
            label = {
                "deterministic": "Deterministic group",
                "hybrid": "Hybrid group",
                "ai": "Model-assisted group",
            }[kind]
            child_names = [_unit_display_name(child) for child in child_units]
            child_ai = [_unit_display_name(child) for child in child_units if str((child.get("presentation") or {}).get("kind") or "") == "ai"]
            child_hybrid = [_unit_display_name(child) for child in child_units if str((child.get("presentation") or {}).get("kind") or "") == "hybrid"]
            note_parts = [f"Contains: {_format_name_list(child_names)}."]
            if child_hybrid:
                note_parts.append(f"Hybrid units inside: {_format_name_list(child_hybrid)}.")
            if child_ai:
                note_parts.append(f"Model-assisted units inside: {_format_name_list(child_ai)}.")
            if not child_ai and not child_hybrid:
                note_parts.append("No model-assisted units inside this group.")
            note = " ".join(note_parts)
            badges = [label]
            for badge in ("Model", "LLM", "Tool", "Verified", "Composite"):
                if badge in badge_set and badge not in badges:
                    badges.append("Model" if badge == "LLM" else badge)
            unit["presentation"] = {
                "kind": kind,
                "label": label,
                "badges": badges,
                "note": note,
            }
        else:
            unit_id = str(unit.get("unit_id") or "")
            profile = UNIT_PRESENTATION_PROFILES.get(unit_id, {})
            tags = set(str(item) for item in unit.get("tags") or [])
            if profile.get("kind"):
                tags.add(str(profile["kind"]))
            kind = "deterministic"
            if "hybrid" in tags:
                kind = "hybrid"
            elif "ai" in tags:
                kind = "ai"
            label = {
                "deterministic": "Deterministic unit",
                "hybrid": "Hybrid unit",
                "ai": "Model-assisted unit",
            }[kind]
            badges = [label]
            for badge in profile.get("badges") or []:
                if badge not in badges:
                    badges.append(badge)
            if kind == "deterministic" and "Rules" not in badges:
                badges.append("Rules")
            unit["presentation"] = {
                "kind": kind,
                "label": str(profile.get("label") or label),
                "badges": badges,
                "note": str(profile.get("note") or _default_presentation_note(unit, kind)),
            }
        presentation_kind = str((unit.get("presentation") or {}).get("kind") or "deterministic")
        summary_counts[presentation_kind] = summary_counts.get(presentation_kind, 0) + 1
    unit_map = {unit["unit_id"]: unit for unit in plan.get("units") or []}
    stage_counts = {"deterministic": 0, "hybrid": 0, "ai": 0}
    for stage in plan.get("stages") or []:
        stage_units = [unit_map.get(unit_id) for unit_id in stage.get("unit_ids") or []]
        stage["presentation"] = _stage_presentation(stage_units)
        stage_kind = str((stage.get("presentation") or {}).get("kind") or "deterministic")
        stage_counts[stage_kind] = stage_counts.get(stage_kind, 0) + 1
    plan["presentation"] = {
        "unit_counts": summary_counts,
        "stage_counts": stage_counts,
    }
    return plan


def _enrich_workflow_snapshot(plan: Dict[str, Any], results: Dict[str, Any], run_dir: str | Path) -> Dict[str, Any]:
    unit_map = {unit["unit_id"]: unit for unit in plan.get("units") or []}
    for unit in plan.get("units") or []:
        if unit.get("unit_type") == "composite":
            unit["plan"] = _enrich_workflow_snapshot(unit.get("plan") or {}, results, run_dir)
            child_units = (unit.get("plan") or {}).get("units") or []
            child_statuses = [str((child.get("runtime") or {}).get("status") or "pending") for child in child_units]
            child_durations = [int((child.get("runtime") or {}).get("duration_ms") or 0) for child in child_units]
            unit["runtime"] = {
                "status": summarize_status(child_statuses),
                "duration_ms": sum(child_durations),
                "unit_type": unit.get("unit_type"),
                "execution_mode": unit.get("execution_mode"),
            }
            continue
        unit_id = str(unit.get("unit_id") or "")
        result = results.get(unit_id) or {}
        trace = _load_trace(run_dir, unit_id)
        unit["runtime"] = {
            "status": str(result.get("status") or "pending"),
            "duration_ms": int(trace.get("duration_ms") or 0),
            "unit_type": unit.get("unit_type"),
            "execution_mode": unit.get("execution_mode"),
        }
        unit["result"] = {
            "status": str(result.get("status") or "pending"),
            "summary": str(result.get("summary") or ""),
            "flags": [str(item) for item in result.get("flags") or []],
            "needs_human_review": bool(result.get("needs_human_review") or False),
        }
    for stage in plan.get("stages") or []:
        stage_units = [unit_map.get(unit_id) for unit_id in stage.get("unit_ids") or []]
        stage_units = [unit for unit in stage_units if unit]
        statuses = [str((unit.get("runtime") or {}).get("status") or "pending") for unit in stage_units]
        durations = [int((unit.get("runtime") or {}).get("duration_ms") or 0) for unit in stage_units]
        stage["runtime"] = {
            "status": summarize_status(statuses),
            "duration_ms": max(durations) if durations else 0,
            "unit_ids": list(stage.get("unit_ids") or []),
        }
    return plan


def _load_workflow_snapshot(
    summary: Dict[str, Any],
    run_dir: str | Path,
    validators_root: str | Path = DEFAULT_VALIDATORS_ROOT,
) -> Dict[str, Any]:
    snapshot = summary.get("workflow")
    if snapshot:
        return _annotate_presentation(snapshot)
    registry = BlockRegistry(validators_root)
    fallback = build_workflow_plan(registry=registry).to_dict()
    enriched = _enrich_workflow_snapshot(fallback, summary.get("results") or {}, run_dir)
    return _annotate_presentation(enriched)


def discover_runs(artifacts_root: str | Path = DEFAULT_ARTIFACTS_ROOT) -> List[Dict[str, Any]]:
    runs: List[Dict[str, Any]] = []
    seen_dirs = set()
    seen_projects = set()
    for summary_path in _run_candidates(artifacts_root):
        run_dir = summary_path.parent.resolve()
        if run_dir in seen_dirs:
            continue
        seen_dirs.add(run_dir)
        try:
            payload = read_json(summary_path)
        except Exception:
            continue
        case = payload.get("case") or {}
        workflow = payload.get("workflow") or {}
        project_key = _project_key_from_case(case) or str(case.get("case_id") or run_dir.name)
        if project_key in seen_projects:
            continue
        seen_projects.add(project_key)
        runs.append(
            _build_run_list_item(
                run_id=run_dir.name,
                root_dir=_safe_relative(run_dir),
                case=case,
                options=payload.get("options") or {},
                workflow=workflow,
                results=payload.get("results") or {},
                mtime=int(summary_path.stat().st_mtime),
                is_live=False,
                run_status=_workflow_run_status(workflow),
            )
        )
    return runs


def _collect_unit_ids(plan: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for unit in plan.get("units") or []:
        out.append(str(unit.get("unit_id") or ""))
        if unit.get("unit_type") == "composite":
            out.extend(_collect_unit_ids(unit.get("plan") or {}))
    return out


def _find_unit(plan: Dict[str, Any], unit_id: str, parent: Dict[str, Any] | None = None) -> Tuple[Dict[str, Any], Dict[str, Any] | None]:
    for unit in plan.get("units") or []:
        if str(unit.get("unit_id") or "") == unit_id:
            return unit, parent
        if unit.get("unit_type") == "composite":
            try:
                return _find_unit(unit.get("plan") or {}, unit_id, unit)
            except FileNotFoundError:
                pass
    raise FileNotFoundError(f"Unknown unit_id={unit_id}")


def _find_plan_containing(plan: Dict[str, Any], unit_id: str) -> Dict[str, Any]:
    unit_ids = {str(item.get("unit_id") or "") for item in plan.get("units") or []}
    if unit_id in unit_ids:
        return plan
    for unit in plan.get("units") or []:
        if unit.get("unit_type") == "composite":
            child_plan = unit.get("plan") or {}
            try:
                return _find_plan_containing(child_plan, unit_id)
            except FileNotFoundError:
                pass
    raise FileNotFoundError(f"Unknown unit_id={unit_id}")


def _stage_for_unit(plan: Dict[str, Any], unit_id: str) -> Dict[str, Any]:
    for stage in plan.get("stages") or []:
        if unit_id in (stage.get("unit_ids") or []):
            return stage
    return {}


def _edges_for_unit(plan: Dict[str, Any], unit_id: str) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    upstream = [edge for edge in plan.get("edges") or [] if edge.get("target_unit_id") == unit_id]
    downstream = [edge for edge in plan.get("edges") or [] if edge.get("source_unit_id") == unit_id]
    return upstream, downstream


def _unit_topology_mode(unit: Dict[str, Any]) -> str:
    if str(unit.get("unit_type") or "") != "composite":
        return "none"
    child_plan = unit.get("plan") or {}
    child_units = child_plan.get("units") or []
    child_stages = child_plan.get("stages") or []
    if not child_units or not child_stages:
        return "none"
    if len(child_units) == 1:
        return "single"
    stage_sizes = [len(stage.get("unit_ids") or []) for stage in child_stages]
    if len(child_stages) == 1:
        return "parallel" if (stage_sizes[0] if stage_sizes else 0) > 1 else "single"
    if all(size <= 1 for size in stage_sizes):
        return "sequential"
    return "mixed"


def _auto_description(unit: Dict[str, Any], plan: Dict[str, Any], locale: str = "en") -> str:
    locale = _normalize_locale(locale)
    unit_type = str(unit.get("unit_type") or "unknown")
    upstream, downstream = _edges_for_unit(plan, str(unit.get("unit_id") or ""))
    type_label = _locale_text(
        locale,
        en="Composite block." if unit_type == "composite" else "Atomic block.",
        ru="Составной блок." if unit_type == "composite" else "Атомарный блок.",
    )
    parts = [type_label]
    if upstream:
        parts.append(
            _locale_text(
                locale,
                en=f"Consumes {len(upstream)} explicit upstream links from the same plan scope.",
                ru=f"Получает {len(upstream)} явных входящих связей в рамках текущей области плана.",
            )
        )
    else:
        parts.append(
            _locale_text(
                locale,
                en="Starts from inputs already available in its current scope.",
                ru="Стартует от входных данных, уже доступных в текущей области выполнения.",
            )
        )
    if downstream:
        parts.append(
            _locale_text(
                locale,
                en=f"Exports outputs to {len(downstream)} downstream links.",
                ru=f"Передаёт результаты в {len(downstream)} исходящих связей.",
            )
        )
    else:
        parts.append(
            _locale_text(
                locale,
                en="Does not feed further units in the same scope.",
                ru="Не передаёт результат дальше в рамках той же области.",
            )
        )
    if unit_type == "composite":
        child_plan = unit.get("plan") or {}
        child_count = len(child_plan.get("units") or [])
        child_stage_count = len(child_plan.get("stages") or [])
        topology = _unit_topology_mode(unit)
        topology_label = {
            "parallel": _locale_text(locale, en="parallel", ru="параллельная"),
            "sequential": _locale_text(locale, en="sequential", ru="последовательная"),
            "mixed": _locale_text(locale, en="mixed", ru="смешанная"),
            "single": _locale_text(locale, en="single nested unit", ru="один вложенный блок"),
            "none": _locale_text(locale, en="no internal graph", ru="внутреннего графа нет"),
        }[topology]
        parts.append(
            _locale_text(
                locale,
                en=f"Internal topology: {topology_label}. Contains {child_stage_count} internal stages and {child_count} nested units.",
                ru=f"Внутренняя топология: {topology_label}. Внутри {child_stage_count} этапов и {child_count} вложенных блоков.",
            )
        )
    else:
        parts.append(
            _locale_text(
                locale,
                en="No internal graph.",
                ru="Внутреннего графа нет.",
            )
        )
    result = unit.get("result") or {}
    summary = str(result.get("summary") or "")
    if summary:
        parts.append(
            _locale_text(
                locale,
                en=f"Latest result: {summary}",
                ru=f"Последний результат: {summary}",
            )
        )
    return " ".join(parts)


def _build_block_detail_from_workflow(
    run_dir: str | Path,
    workflow: Dict[str, Any],
    block_id: str,
    validators_root: str | Path = DEFAULT_VALIDATORS_ROOT,
    raw: bool = False,
    locale: str = "en",
) -> Dict[str, Any]:
    del validators_root
    locale = _normalize_locale(locale)
    run_dir = Path(run_dir)
    unit_id = Path(unquote(block_id)).name
    unit, parent_unit = _find_unit(workflow, unit_id)
    containing_plan = _find_plan_containing(workflow, unit_id)
    stage = _stage_for_unit(containing_plan, unit_id)
    upstream, downstream = _edges_for_unit(containing_plan, unit_id)
    trace = _load_trace(run_dir, unit_id)
    llm_trace = _load_llm_trace(run_dir, unit_id)
    source_path = str(unit.get("source_path") or "")
    manifest_path = str(unit.get("manifest_path") or "")
    validator_source = read_text(PROJECT_ROOT / source_path) if source_path else ""
    manifest_payload = read_json(PROJECT_ROOT / manifest_path) if manifest_path else {}
    return {
        "block_id": unit_id,
        "unit_id": unit_id,
        "name": unit.get("name") or unit_id,
        "unit_type": unit.get("unit_type") or "unknown",
        "execution_mode": unit.get("execution_mode") or "unknown",
        "kind": unit.get("kind") or "",
        "description": unit.get("description") or "",
        "manifest": manifest_payload,
        "manifest_path": manifest_path,
        "source_path": source_path,
        "parent_unit_id": str(parent_unit.get("unit_id") or "") if parent_unit else "",
        "parent_plan_id": str(containing_plan.get("plan_id") or ""),
        "stage": stage,
        "runtime": unit.get("runtime") or {},
        "result": unit.get("result") or {},
        "presentation": unit.get("presentation") or {},
        "auto_description": _auto_description(unit, containing_plan, locale=locale),
        "input_ports": unit.get("input_ports") or [],
        "output_ports": unit.get("output_ports") or [],
        "upstream_edges": upstream,
        "downstream_edges": downstream,
        "raw_mode": raw,
        "trace_input": trace.get("input") if raw else _preview_json(trace.get("input") or {}),
        "trace_output": trace.get("output") if raw else _preview_json(trace.get("output") or {}),
        "llm_trace": llm_trace if raw else _preview_json(llm_trace),
        "validator_source": validator_source,
        "child_plan": unit.get("plan") or {},
    }


def build_run_payload(
    run_dir: str | Path,
    validators_root: str | Path = DEFAULT_VALIDATORS_ROOT,
    locale: str = "en",
) -> Dict[str, Any]:
    locale = _normalize_locale(locale)
    run_dir = Path(run_dir)
    summary = _load_summary(run_dir)
    core_workflow = _load_workflow_snapshot(summary, run_dir, validators_root)
    workflow = _augment_workflow_with_discovery(core_workflow, summary.get("discovery") or {})
    return _build_run_payload_from_workflow(
        run_id=run_dir.name,
        root_dir=_safe_relative(run_dir),
        case=summary.get("case") or {},
        options=summary.get("options") or {},
        workflow=workflow,
        results=summary.get("results") or {},
        discovery=summary.get("discovery") or {},
        run_status=_workflow_run_status(core_workflow),
        is_live=False,
        updated_at=int(_summary_path(run_dir).stat().st_mtime),
        project_key=_project_key_from_case(summary.get("case") or {}),
        error_workflow=core_workflow,
        locale=locale,
    )


def build_block_detail_payload(
    run_dir: str | Path,
    block_id: str,
    validators_root: str | Path = DEFAULT_VALIDATORS_ROOT,
    raw: bool = False,
    locale: str = "en",
) -> Dict[str, Any]:
    run_dir = Path(run_dir)
    summary = _load_summary(run_dir)
    workflow = _load_workflow_snapshot(summary, run_dir, validators_root)
    workflow = _augment_workflow_with_discovery(workflow, summary.get("discovery") or {})
    return _build_block_detail_from_workflow(run_dir, workflow, block_id, validators_root, raw=raw, locale=locale)


def _frontend_bundle_instructions(static_root: str | Path = DEFAULT_STATIC_ROOT) -> str:
    expected = Path(static_root).resolve() / "index.html"
    return (
        "Frontend bundle is missing. Build it from the repository root with:\n"
        "cd viewer_frontend\n"
        "npm ci\n"
        "npm run build\n"
        f"Expected file: {expected}"
    )


def _frontend_bundle_missing_page(static_root: str | Path = DEFAULT_STATIC_ROOT) -> bytes:
    message = _frontend_bundle_instructions(static_root)
    escaped_message = html.escape(message)
    return (
        "<!doctype html>"
        "<html><head><meta charset=\"utf-8\"><title>Frontend bundle missing</title></head>"
        "<body style=\"font-family: sans-serif; max-width: 840px; margin: 40px auto; line-height: 1.5;\">"
        "<h1>Frontend bundle is missing</h1>"
        "<p>The backend is running, but the reviewer UI has not been built yet.</p>"
        f"<pre style=\"background: #f5f5f5; padding: 16px; overflow-x: auto;\">{escaped_message}</pre>"
        "</body></html>"
    ).encode("utf-8")


def _read_static_file(path: str | Path, static_root: str | Path = DEFAULT_STATIC_ROOT) -> tuple[bytes, str]:
    root = Path(static_root).resolve()
    target = (root / str(path).lstrip("/")).resolve()
    if not str(target).startswith(str(root)):
        raise FileNotFoundError("Path traversal is not allowed")
    if target.is_dir():
        target = target / "index.html"
    if not target.is_file():
        raise FileNotFoundError(str(target))
    content_type = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
    return target.read_bytes(), content_type


def create_http_server(
    host: str = "127.0.0.1",
    port: int = 8008,
    artifacts_root: str | Path = DEFAULT_ARTIFACTS_ROOT,
    validators_root: str | Path = DEFAULT_VALIDATORS_ROOT,
    static_root: str | Path = DEFAULT_STATIC_ROOT,
) -> ThreadingHTTPServer:
    artifacts_root = Path(artifacts_root)
    validators_root = Path(validators_root)
    static_root = Path(static_root)
    active_runs = ActiveRunRegistry(artifacts_root, validators_root)
    active_discovery = ActiveDiscoveryRegistry()

    class ViewerRequestHandler(BaseHTTPRequestHandler):
        server_version = "IdentityViewer/1.0"

        def log_message(self, format: str, *args: Any) -> None:
            return

        def _send_json(self, payload: Dict[str, Any], status: int = 200, send_body: bool = True) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if send_body:
                self.wfile.write(body)

        def _send_bytes(self, body: bytes, content_type: str, status: int = 200, send_body: bool = True) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if send_body:
                self.wfile.write(body)

        def _handle_read(self, send_body: bool = True) -> None:
            parsed = urlparse(self.path)
            parts = [part for part in parsed.path.split("/") if part]
            query = parse_qs(parsed.query)
            locale = _normalize_locale(str(query.get("lang", ["en"])[0]))
            try:
                if parsed.path == "/api/runs":
                    self._send_json({"runs": active_runs.list_runs()}, send_body=send_body)
                    return
                if len(parts) == 3 and parts[:2] == ["api", "runs"]:
                    run_id = parts[2]
                    if active_runs.has(run_id):
                        self._send_json(active_runs.get_payload(run_id, locale=locale), send_body=send_body)
                        return
                    run_dir = _resolve_run_dir(run_id, artifacts_root)
                    self._send_json(build_run_payload(run_dir, validators_root, locale=locale), send_body=send_body)
                    return
                if len(parts) == 5 and parts[:2] == ["api", "runs"] and parts[3] == "blocks":
                    run_id = parts[2]
                    raw = str(query.get("raw", ["0"])[0]) == "1"
                    if active_runs.has(run_id):
                        payload = active_runs.get_block_detail(run_id, parts[4], raw=raw, locale=locale)
                        self._send_json(payload, send_body=send_body)
                        return
                    run_dir = _resolve_run_dir(run_id, artifacts_root)
                    payload = build_block_detail_payload(run_dir, parts[4], validators_root, raw=raw, locale=locale)
                    self._send_json(payload, send_body=send_body)
                    return
                if len(parts) == 4 and parts[:3] == ["api", "discovery", "sessions"]:
                    session_id = parts[3]
                    self._send_json(active_discovery.get_payload(session_id), send_body=send_body)
                    return
                relative = "index.html" if parsed.path in {"", "/"} else parsed.path.lstrip("/")
                try:
                    body, content_type = _read_static_file(relative, static_root)
                except FileNotFoundError:
                    if "." in Path(parsed.path).name:
                        raise
                    try:
                        body, content_type = _read_static_file("index.html", static_root)
                    except FileNotFoundError:
                        body = _frontend_bundle_missing_page(static_root)
                        content_type = "text/html; charset=utf-8"
                        self._send_bytes(body, content_type, status=HTTPStatus.SERVICE_UNAVAILABLE, send_body=send_body)
                        return
                self._send_bytes(body, content_type, send_body=send_body)
            except FileNotFoundError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND, send_body=send_body)
            except Exception as exc:
                self._send_json({"error": repr(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR, send_body=send_body)

        def do_GET(self) -> None:
            self._handle_read(send_body=True)

        def do_HEAD(self) -> None:
            self._handle_read(send_body=False)

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            try:
                if parsed.path == "/api/discovery/search":
                    payload = _read_request_json(self)
                    result = asyncio.run(discover_project_candidates(payload))
                    self._send_json(result, status=HTTPStatus.OK)
                    return
                if parsed.path == "/api/discovery/sessions/new":
                    payload = _read_request_json(self)
                    created = active_discovery.start_search(payload)
                    self._send_json(created, status=HTTPStatus.CREATED)
                    return
                if parsed.path == "/api/runs/new":
                    payload = _read_request_json(self)
                    locale = _normalize_locale(str(payload.get("locale") or "en"))
                    created = active_runs.start_run(payload, locale=locale)
                    self._send_json(created, status=HTTPStatus.CREATED)
                    return
                self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
            except ValueError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            except FileNotFoundError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
            except Exception as exc:
                self._send_json({"error": repr(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    return ThreadingHTTPServer((host, port), ViewerRequestHandler)


def serve_viewer(
    host: str = "127.0.0.1",
    port: int = 8008,
    artifacts_root: str | Path = DEFAULT_ARTIFACTS_ROOT,
    validators_root: str | Path = DEFAULT_VALIDATORS_ROOT,
    static_root: str | Path = DEFAULT_STATIC_ROOT,
) -> None:
    static_index = Path(static_root).resolve() / "index.html"
    if not static_index.is_file():
        print(_frontend_bundle_instructions(static_root))
    server = create_http_server(
        host=host,
        port=port,
        artifacts_root=artifacts_root,
        validators_root=validators_root,
        static_root=static_root,
    )
    actual_host, actual_port = server.server_address[:2]
    print(f"viewer=http://{actual_host}:{actual_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
