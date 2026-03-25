from __future__ import annotations

import re
from urllib.parse import urlparse

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult
from identity_validator.utils import normalize_ws, stable_score

NON_CANONICAL_IDENTITY_DOMAINS = {
    "github.com",
    "www.github.com",
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
    "tonviewer.com",
    "www.tonviewer.com",
}

IDENTITY_STOPWORDS = {
    "about",
    "app",
    "and",
    "blockchain",
    "chain",
    "coin",
    "com",
    "contracts",
    "crypto",
    "dao",
    "defi",
    "dex",
    "exchange",
    "finance",
    "github",
    "http",
    "https",
    "io",
    "market",
    "marketplace",
    "nft",
    "official",
    "open",
    "org",
    "platform",
    "product",
    "project",
    "protocol",
    "sdk",
    "smart",
    "telegram",
    "the",
    "ton",
    "wallet",
    "web",
    "www",
}


def _url_domain(value: str) -> str:
    raw = normalize_ws(value)
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    return (parsed.netloc or "").lower().removeprefix("www.")


def _tokenize_identity(text: str) -> list[str]:
    tokens = []
    seen = set()
    for token in re.findall(r"[a-z0-9]{3,}", str(text or "").lower()):
        if token in IDENTITY_STOPWORDS:
            continue
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def _brand_overlap(left: list[str], right: list[str]) -> list[str]:
    out = []
    seen = set()
    for left_token in left:
        for right_token in right:
            if left_token == right_token or left_token in right_token or right_token in left_token:
                if left_token not in seen:
                    seen.add(left_token)
                    out.append(left_token)
                break
    return out


def _identity_tokens(context: ExecutionContext, repo_meta: dict[str, str], canonical_domain: str) -> list[str]:
    repo_name = str(context.case.github_repo or "").split("/", 1)[-1]
    homepage = str(repo_meta.get("homepage") or "")
    parts = [
        str(context.case.name or ""),
        str(context.case.github_repo or ""),
        repo_name,
        str(context.case.telegram_handle or "").lstrip("@"),
        str(context.case.project_url or ""),
        canonical_domain,
        homepage,
        str(repo_meta.get("full_name") or ""),
    ]
    return _tokenize_identity(" ".join(parts))


def _requested_identity_tokens(context: ExecutionContext) -> list[str]:
    requested_input = normalize_ws(str(context.case.requested_input or ""))
    if requested_input:
        return _tokenize_identity(requested_input)
    fallback_parts = [
        str(context.case.name or ""),
        str(context.case.github_repo or ""),
        str(context.case.telegram_handle or "").lstrip("@"),
        str(context.case.project_url or ""),
    ]
    fallback_tokens = _tokenize_identity(" ".join(fallback_parts))
    if fallback_tokens:
        return fallback_tokens
    return _tokenize_identity(str(context.case.description or ""))


def _explicit_source_failures(context: ExecutionContext, repo_status: str, telegram_status: str) -> list[str]:
    failures: list[str] = []
    if context.case.github_repo and repo_status != "success":
        failures.append("github_repo")
    if context.case.telegram_handle and telegram_status != "success":
        failures.append("telegram_channel")
    return failures


class IdentityConfirmationBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        repo_result = context.get_result("github_repo")
        tg_result = context.get_result("telegram_channel")
        address_result = context.get_result("address_signal")
        project_registry_result = context.get_result("project_registry")

        repo_status = str(repo_result.status or "pending") if repo_result else "pending"
        telegram_status = str(tg_result.status or "pending") if tg_result else "pending"
        address_status = str(address_result.status or "pending") if address_result else "pending"

        repo_data = dict(repo_result.data or {}) if repo_result and repo_result.status == "success" else {}
        repo_meta = dict(repo_data.get("repo") or {})
        address_data = dict(address_result.data or {}) if address_result and address_result.status == "success" else {}
        ton_activity = dict(address_data.get("ton_activity") or {})
        ton_mcp = dict(address_data.get("ton_mcp") or {})
        reverse_dns = dict(ton_mcp.get("reverse_dns") or {})
        dns_match = dict(ton_mcp.get("dns_match") or {})
        matched_jetton = dict(ton_mcp.get("matched_jetton") or {})

        case_domain = _url_domain(str(context.case.project_url or ""))
        repo_homepage_domain = _url_domain(str(repo_meta.get("homepage") or ""))
        canonical_domain = ""
        if repo_homepage_domain and repo_homepage_domain not in NON_CANONICAL_IDENTITY_DOMAINS:
            canonical_domain = repo_homepage_domain
        elif case_domain and case_domain not in NON_CANONICAL_IDENTITY_DOMAINS:
            canonical_domain = case_domain

        target_repo = normalize_ws(str(context.case.github_repo or "")).lower()
        repo_full_name = normalize_ws(str(repo_meta.get("full_name") or "")).lower()
        exact_repo_match = bool(repo_status == "success" and target_repo and repo_full_name == target_repo)

        primary_reference_domain = case_domain or repo_homepage_domain
        noncanonical_reference = (
            primary_reference_domain
            if primary_reference_domain
            and primary_reference_domain in NON_CANONICAL_IDENTITY_DOMAINS
            and not canonical_domain
            else ""
        )

        query_brand_tokens = _requested_identity_tokens(context)
        candidate_brand_tokens = _identity_tokens(context, repo_meta, canonical_domain)
        overlap_tokens = _brand_overlap(query_brand_tokens, candidate_brand_tokens)
        brand_mismatch = bool(query_brand_tokens) and not overlap_tokens

        corroborating_signals: list[str] = []
        if canonical_domain:
            corroborating_signals.append("canonical_domain")
        if repo_status == "success":
            corroborating_signals.append("github_repo")
        if exact_repo_match:
            corroborating_signals.append("github_repo_exact")
        if telegram_status == "success":
            corroborating_signals.append("telegram_channel")
        elif canonical_domain and not brand_mismatch and normalize_ws(str(context.case.telegram_handle or "")).lstrip("@"):
            corroborating_signals.append("telegram_reference")

        wallet_has_identity_signal = bool(
            (context.case.wallet_address or address_data.get("unique_addresses"))
            and (
                matched_jetton
                or reverse_dns.get("domain")
                or dns_match
                or ton_activity.get("status") == "success"
            )
        )
        if wallet_has_identity_signal:
            corroborating_signals.append("wallet_signal")

        source_failures = _explicit_source_failures(context, repo_status, telegram_status)
        registry_repo_matches = []
        if project_registry_result and project_registry_result.status == "success" and target_repo:
            for profile in (project_registry_result.data or {}).get("profiles") or []:
                if not isinstance(profile, dict):
                    continue
                if normalize_ws(str(profile.get("github_repo") or "")).lower() == target_repo:
                    registry_repo_matches.append(str(profile.get("case_id") or ""))

        corroborating_count = len(corroborating_signals)
        evidence_status = "partial"
        if corroborating_count >= 2 or (wallet_has_identity_signal and not brand_mismatch):
            evidence_status = "sufficient"
        elif source_failures and corroborating_count == 0:
            evidence_status = "incomplete"

        identity_status = "weak"
        if brand_mismatch:
            identity_status = "mismatch"
        elif wallet_has_identity_signal and not brand_mismatch and (matched_jetton or reverse_dns.get("domain") or dns_match):
            identity_status = "confirmed"
        elif exact_repo_match and not brand_mismatch and (
            telegram_status == "success" or canonical_domain or wallet_has_identity_signal or registry_repo_matches
        ):
            identity_status = "confirmed"
        elif canonical_domain and not brand_mismatch and corroborating_count >= 2:
            identity_status = "confirmed"
        elif corroborating_count >= 2 and not noncanonical_reference and not brand_mismatch:
            identity_status = "confirmed"
        elif evidence_status == "incomplete":
            identity_status = "incomplete"

        identity_score = stable_score(
            35
            + (20 if canonical_domain else 0)
            + (15 if repo_status == "success" else 0)
            + (12 if exact_repo_match else 0)
            + (10 if telegram_status == "success" else 0)
            + (8 if "telegram_reference" in corroborating_signals else 0)
            + (15 if wallet_has_identity_signal else 0)
            + (15 if overlap_tokens else 0)
            + (5 if registry_repo_matches else 0)
            - (12 if noncanonical_reference else 0)
            - (35 if brand_mismatch else 0)
        )

        flags: list[str] = []
        if identity_status in {"weak", "incomplete"}:
            flags.append("identity_unconfirmed")
        if brand_mismatch:
            flags.append("identity_brand_mismatch")
        if noncanonical_reference:
            flags.append("identity_based_on_noncanonical_reference")
        if "github_repo" in source_failures:
            flags.append("github_source_unavailable")
        if "telegram_channel" in source_failures:
            flags.append("telegram_source_unavailable")
        if evidence_status == "incomplete":
            flags.append("identity_evidence_incomplete")

        summary_parts = [f"identity={identity_status}", f"evidence={evidence_status}", f"signals={corroborating_count}"]
        if canonical_domain:
            summary_parts.append(f"domain={canonical_domain}")
        elif noncanonical_reference:
            summary_parts.append(f"noncanonical={noncanonical_reference}")
        if overlap_tokens:
            summary_parts.append(f"brand_overlap={','.join(overlap_tokens[:3])}")
        elif query_brand_tokens:
            summary_parts.append("brand_overlap=none")
        summary = " ".join(summary_parts)

        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=summary,
            metrics={
                "identity_score": identity_score,
                "corroborating_signal_count": corroborating_count,
                "brand_overlap_count": len(overlap_tokens),
                "query_brand_token_count": len(query_brand_tokens),
                "canonical_domain_present": 1 if canonical_domain else 0,
                "source_failure_count": len(source_failures),
                "wallet_identity_signal": 1 if wallet_has_identity_signal else 0,
                "exact_repo_match": 1 if exact_repo_match else 0,
            },
            data={
                "identity_status": identity_status,
                "evidence_status": evidence_status,
                "canonical_domain": canonical_domain,
                "primary_reference_domain": primary_reference_domain,
                "noncanonical_reference_domain": noncanonical_reference,
                "query_brand_tokens": query_brand_tokens,
                "candidate_brand_tokens": candidate_brand_tokens,
                "brand_overlap_tokens": overlap_tokens,
                "corroborating_signals": corroborating_signals,
                "source_failures": source_failures,
                "wallet_identity_signal": wallet_has_identity_signal,
                "registry_repo_matches": registry_repo_matches[:5],
                "repo_status": repo_status,
                "telegram_status": telegram_status,
                "address_status": address_status,
                "exact_repo_match": exact_repo_match,
            },
            evidence=corroborating_signals + overlap_tokens,
            flags=flags,
            confidence=0.82,
            needs_human_review=identity_status != "confirmed" or evidence_status != "sufficient",
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return IdentityConfirmationBlock(manifest)
