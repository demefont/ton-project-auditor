from __future__ import annotations

import json
import re
import urllib.parse
from typing import Any, Dict, List

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult
from identity_validator.utils import clip_text, normalize_ws, strip_html


def _clean_project_name(value: Any) -> str:
    raw = normalize_ws(str(value or ""))
    if " | " in raw:
        raw = normalize_ws(raw.split(" | ", 1)[0])
    return raw or "project"


def _first_sentence(value: Any, limit: int = 260) -> str:
    raw = strip_html(normalize_ws(str(value or "")))
    if not raw:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", raw)
    for part in parts:
        sentence = normalize_ws(part)
        if len(sentence) < 24:
            continue
        return clip_text(sentence.rstrip(". "), limit).strip()
    return clip_text(raw.rstrip(". "), limit).strip()


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
    raise ValueError(f"LLM explainer returned invalid JSON: {clip_text(text, 320)}")


def _structured_evidence_payload(context: ExecutionContext, rule_result, activity_result, similarity_result) -> Dict[str, Any]:
    rule_data = dict(rule_result.data or {})
    rule_metrics = dict(rule_result.metrics or {})
    repo_result = context.get_result("github_repo")
    repo_data = dict((repo_result.data or {})) if repo_result else {}
    repo_meta = dict(repo_data.get("repo") or {})
    telegram_result = context.get_result("telegram_channel")
    telegram_data = dict((telegram_result.data or {})) if telegram_result else {}
    telegram_metrics = dict((telegram_result.metrics or {})) if telegram_result else {}
    telegram_semantics_result = context.get_result("telegram_semantics")
    telegram_semantics_data = dict((telegram_semantics_result.data or {})) if telegram_semantics_result else {}
    telegram_semantics_metrics = dict((telegram_semantics_result.metrics or {})) if telegram_semantics_result else {}
    identity_result = context.get_result("identity_confirmation")
    identity_data = dict((identity_result.data or {})) if identity_result else {}
    address_result = context.get_result("address_signal")
    address_data = dict((address_result.data or {})) if address_result else {}
    ton_activity = dict(address_data.get("ton_activity") or {})
    ton_mcp = dict(address_data.get("ton_mcp") or {})
    contract_result = context.get_result("contract_validator")
    contract_data = dict((contract_result.data or {})) if contract_result else {}
    contract_metrics = dict((contract_result.metrics or {})) if contract_result else {}
    similarity_data = dict((similarity_result.data or {})) if similarity_result else {}
    clone_source_project = dict(similarity_data.get("clone_source_project") or {})
    closest_projects = [
        {
            "name": str(item.get("name") or ""),
            "github_repo": str(item.get("github_repo") or ""),
            "similarity": item.get("similarity"),
            "url": str(item.get("url") or ""),
        }
        for item in (similarity_data.get("closest_projects") or [])[:3]
        if isinstance(item, dict)
    ]
    matched_jetton = dict(ton_mcp.get("matched_jetton") or {})
    dns_match = dict(ton_mcp.get("dns_match") or {})
    reverse_dns = dict(ton_mcp.get("reverse_dns") or {})
    balance = dict(ton_mcp.get("balance") or {})
    wallet_address = normalize_ws(str(context.case.wallet_address or "")) or normalize_ws(str(ton_activity.get("address") or ""))
    ton_onchain_available = str(ton_activity.get("status") or "") == "success" and bool(wallet_address)
    project_name = _clean_project_name(context.case.name)
    case_description = _first_sentence(context.case.description or "", limit=260)
    project_type = str(rule_data.get("project_type") or "unknown")
    contract_evidence: Dict[str, Any] = {}
    if project_type == "smart_contracts" or contract_data.get("addresses") or (contract_result.flags if contract_result else []) or int(contract_metrics.get("address_signal_count") or 0) > 0:
        contract_evidence = {
            "contract_score": contract_metrics.get("contract_score"),
            "contract_file_count": contract_metrics.get("contract_file_count"),
            "address_signal_count": contract_metrics.get("address_signal_count"),
            "flags": [str(item) for item in (contract_result.flags or [])[:8]] if contract_result else [],
            "addresses": [str(item) for item in (contract_data.get("addresses") or [])[:6]],
        }
    originality_evidence: Dict[str, Any] = {
        "clone_risk": str(similarity_data.get("clone_risk") or rule_data.get("clone_risk") or ""),
        "top_similarity": similarity_data.get("top_similarity"),
        "clone_source_project": clone_source_project,
        "self_declared_copy_excerpt": str(similarity_data.get("self_declared_copy_excerpt") or ""),
    }
    if clone_source_project or str(similarity_data.get("clone_risk") or "") in {"moderate", "high"}:
        originality_evidence["closest_projects"] = closest_projects
    return {
        "project_name": project_name,
        "project_type": project_type,
        "overall_score": rule_metrics.get("overall_score"),
        "risk_level": str(rule_data.get("risk_level") or "unknown"),
        "identity_status": str(rule_data.get("identity_status") or "unknown"),
        "evidence_status": str(rule_data.get("evidence_status") or "partial"),
        "activity_score": rule_metrics.get("activity_score"),
        "originality_score": rule_metrics.get("originality_score"),
        "community_activity_score": rule_metrics.get("community_activity_score"),
        "community_quality_score": rule_metrics.get("community_quality_score"),
        "identity_score": rule_metrics.get("identity_score"),
        "last_commit_age_days": (activity_result.metrics or {}).get("last_commit_age_days") if activity_result else None,
        "onchain_tx_count_30d": ton_activity.get("tx_count_30d") if ton_onchain_available else None,
        "last_onchain_tx_age_days": ton_activity.get("last_tx_age_days") if ton_onchain_available else None,
        "clone_risk": str(rule_data.get("clone_risk") or "unknown"),
        "clone_source_project": clone_source_project,
        "self_declared_copy_excerpt": str(similarity_data.get("self_declared_copy_excerpt") or rule_data.get("self_declared_copy_excerpt") or ""),
        "evidence_pack": {
            "requested_entity": {
                "raw_name": normalize_ws(str(context.case.name or "")),
                "requested_input": normalize_ws(str(context.case.requested_input or "")),
                "github_repo": normalize_ws(str(context.case.github_repo or "")),
                "project_url": normalize_ws(str(context.case.project_url or "")),
                "telegram_handle": normalize_ws(str(context.case.telegram_handle or "")),
                "wallet_address": wallet_address,
                "type_hint": normalize_ws(str(context.case.type_hint or "")),
                "case_description": case_description,
            },
            "repository": {
                "full_name": str(repo_meta.get("full_name") or context.case.github_repo or ""),
                "description": _first_sentence(repo_meta.get("description") or "", limit=260),
                "readme_excerpt": _first_sentence(repo_data.get("readme_excerpt") or "", limit=320),
                "topics": [str(item) for item in (repo_meta.get("topics") or [])[:8]],
                "homepage": str(repo_meta.get("homepage") or ""),
                "last_commit_age_days": (activity_result.metrics or {}).get("last_commit_age_days") if activity_result else None,
                "commits_90d": (activity_result.metrics or {}).get("commits_90d") if activity_result else None,
            },
            "identity_confirmation": {
                "identity_status": str(identity_data.get("identity_status") or ""),
                "evidence_status": str(identity_data.get("evidence_status") or ""),
                "canonical_domain": str(identity_data.get("canonical_domain") or ""),
                "corroborating_signals": [str(item) for item in (identity_data.get("corroborating_signals") or [])[:6]],
                "source_failures": [str(item) for item in (identity_data.get("source_failures") or [])[:6]],
                "brand_overlap_tokens": [str(item) for item in (identity_data.get("brand_overlap_tokens") or [])[:6]],
                "noncanonical_reference_domain": str(identity_data.get("noncanonical_reference_domain") or ""),
            },
            "telegram": {
                "handle": str(telegram_data.get("handle") or context.case.telegram_handle or ""),
                "last_post_age_days": telegram_metrics.get("last_post_age_days"),
                "posts_30d": telegram_metrics.get("posts_30d"),
                "active_days_30d": telegram_metrics.get("active_days_30d"),
                "community_activity_score": telegram_metrics.get("community_activity_score"),
                "community_health_score": telegram_semantics_metrics.get("community_health_score"),
                "dominant_topics": [str(item) for item in (telegram_semantics_data.get("dominant_topics") or [])[:6]],
                "content_labels": [str(item) for item in (telegram_semantics_data.get("content_labels") or [])[:6]],
                "semantic_risk_level": str(telegram_semantics_data.get("semantic_risk_level") or ""),
                "semantic_risk_score": telegram_semantics_metrics.get("semantic_risk_score"),
                "promo_post_ratio": telegram_semantics_metrics.get("promo_post_ratio"),
                "duplicate_post_ratio": telegram_semantics_metrics.get("duplicate_post_ratio"),
                "near_duplicate_pair_ratio": telegram_semantics_metrics.get("near_duplicate_pair_ratio"),
            },
            "ton_onchain": {
                "address": wallet_address if ton_onchain_available else "",
                "tx_count_7d": ton_activity.get("tx_count_7d") if ton_onchain_available else None,
                "tx_count_30d": ton_activity.get("tx_count_30d") if ton_onchain_available else None,
                "last_tx_age_days": ton_activity.get("last_tx_age_days") if ton_onchain_available else None,
                "matched_jetton": {
                    "name": str(matched_jetton.get("name") or ""),
                    "symbol": str(matched_jetton.get("symbol") or ""),
                    "address": str(matched_jetton.get("address") or ""),
                },
                "dns_match": {
                    "domain": str(dns_match.get("domain") or ""),
                    "address": str(dns_match.get("address") or ""),
                },
                "reverse_dns": {
                    "domain": str(reverse_dns.get("domain") or ""),
                    "address": str(reverse_dns.get("address") or ""),
                },
                "balance": {
                    "balance": str(balance.get("balance") or ""),
                    "address": str(balance.get("address") or ""),
                },
            },
            "originality": {
                **originality_evidence,
            },
            "contract": contract_evidence,
        },
    }


def _normalized_output_value(payload: Dict[str, Any], key: str) -> str:
    return normalize_ws(str(payload.get(key) or ""))


def _flatten_evidence_keys(value: Any, prefix: str = "") -> set[str]:
    out: set[str] = set()
    if prefix:
        out.add(prefix)
    if isinstance(value, dict):
        for key, item in value.items():
            normalized_key = str(key or "").strip()
            if not normalized_key:
                continue
            next_prefix = f"{prefix}.{normalized_key}" if prefix else normalized_key
            out.update(_flatten_evidence_keys(item, next_prefix))
        return out
    if isinstance(value, list):
        if prefix:
            out.add(prefix)
        return out
    if prefix:
        out.add(prefix)
    return out


def _available_evidence_keys(payload: Dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for key, value in payload.items():
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        if normalized_key == "evidence_pack" and isinstance(value, dict):
            out.update(_flatten_evidence_keys(value))
            continue
        out.update(_flatten_evidence_keys(value, normalized_key))
    return out


def _collect_scalar_strings(value: Any) -> List[str]:
    out: List[str] = []
    if isinstance(value, dict):
        for item in value.values():
            out.extend(_collect_scalar_strings(item))
        return out
    if isinstance(value, list):
        for item in value:
            out.extend(_collect_scalar_strings(item))
        return out
    normalized = normalize_ws(str(value or ""))
    if normalized:
        out.append(normalized)
    return out


def _normalize_identifier_domain(raw_value: Any) -> str:
    raw = normalize_ws(str(raw_value or "")).lower().strip(".,);")
    if not raw:
        return ""
    if "://" in raw:
        parsed = urllib.parse.urlparse(raw)
        raw = (parsed.netloc or parsed.path or "").lower()
    raw = raw.removeprefix("www.").strip("/")
    if re.fullmatch(r"(?:[a-z0-9-]+\.)+[a-z]{2,}", raw):
        return raw
    return ""


def _extract_domain_tokens(text: Any) -> set[str]:
    out: set[str] = set()
    for item in re.findall(
        r"(?:https?://)?(?:www\.)?(?:[a-z0-9-]+\.)+[a-z]{2,}",
        str(text or ""),
        flags=re.IGNORECASE,
    ):
        normalized = _normalize_identifier_domain(item)
        if normalized:
            out.add(normalized)
    return out


def _extract_handle_tokens(text: Any) -> set[str]:
    return {item.lower() for item in re.findall(r"@([A-Za-z0-9_]{3,})\b", str(text or ""))}


def _extract_repo_tokens(text: Any) -> set[str]:
    return {
        item.lower()
        for item in re.findall(r"\b([A-Za-z0-9_-]+/[A-Za-z0-9_.-]+)\b", str(text or ""))
    }


def _allowed_identifier_sets(payload: Dict[str, Any]) -> Dict[str, set[str]]:
    domains: set[str] = set()
    handles: set[str] = set()
    repos: set[str] = set()
    for item in _collect_scalar_strings(payload.get("evidence_pack") or {}):
        domains.update(_extract_domain_tokens(item))
        handles.update(_extract_handle_tokens(item))
        repos.update(_extract_repo_tokens(item))
    return {
        "domains": domains,
        "handles": handles,
        "repos": repos,
    }


def _invalid_identifier_mentions(payload: Dict[str, Any], merged: Dict[str, Any]) -> Dict[str, List[str]]:
    output_text = "\n".join(
        [
            str(merged.get("project_overview_en") or ""),
            str(merged.get("project_overview_ru") or ""),
            str(merged.get("explanation_text_en") or ""),
            str(merged.get("explanation_text_ru") or ""),
        ]
    )
    allowed = _allowed_identifier_sets(payload)
    invalid_domains = sorted(item for item in _extract_domain_tokens(output_text) if item not in allowed["domains"])
    invalid_handles = sorted(item for item in _extract_handle_tokens(output_text) if item not in allowed["handles"])
    invalid_repos = sorted(item for item in _extract_repo_tokens(output_text) if item not in allowed["repos"])
    out: Dict[str, List[str]] = {}
    if invalid_domains:
        out["domains"] = invalid_domains[:8]
    if invalid_handles:
        out["handles"] = invalid_handles[:8]
    if invalid_repos:
        out["repos"] = invalid_repos[:8]
    return out


class LLMExplainerBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        rule_result = context.get_result("rule_engine")
        activity_result = context.get_result("github_activity")
        similarity_result = context.get_result("project_similarity")
        if not rule_result or rule_result.status != "success":
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary="Rule engine output is unavailable",
                flags=["missing_rule_engine"],
            )
        payload = _structured_evidence_payload(context, rule_result, activity_result, similarity_result)
        prompt = (
            "You are writing a grounded project-validation summary from collected evidence.\n"
            "Return STRICT JSON only. No markdown. No prose outside JSON.\n"
            "Use only information explicitly present in the input JSON. Never invent product features, audience, "
            "TON integrations, strengths, weaknesses, or activity that are not directly supported.\n"
            "Every meaningful claim must be traceable to one or more evidence keys in the input JSON.\n"
            "Prefer primary descriptive evidence such as repository.description, repository.readme_excerpt and "
            "requested_entity.case_description over short token lists.\n"
            "When these primary descriptive fields are available, stay close to their wording. Minimal paraphrase is better than a broader but less grounded summary.\n"
            "Do not invert explicit source meaning. For example, if a source says non-custodial, you must not describe the project as custodial.\n"
            "Do not introduce custody-model terms such as custodial, non-custodial, self-custody, self-custodial, "
            "кастодиальный or некастодиальный unless the input JSON explicitly contains that concept.\n"
            "For Russian text, preserve established crypto terminology. If you are not sure about a domain translation, "
            "keep the original English term instead of inventing a different meaning.\n"
            "If a source says non-custodial, the Russian output must preserve that meaning as 'некастодиальный' or 'non-custodial'.\n"
            "Do not expand broad source terms into narrower claims. For example, do not rewrite Web3 as DeFi, payments, infrastructure or staking unless the source explicitly says so.\n"
            "Do not introduce generic benefit language such as 'provides advantages', 'improves security', 'supports finance', or similar unless the benefit is explicitly stated in the evidence.\n"
            "Do not describe the product as safe, secure, convenient, seamless, user-friendly, simple, powerful, innovative or reliable unless that exact concept is present in repository.description, repository.readme_excerpt or requested_entity.case_description.\n"
            "If repository.description, repository.readme_excerpt and requested_entity.case_description only say something short like 'decentralized exchange on TON' or 'wallet on TON', keep that literal meaning. Do not expand it into benefits, audience claims or use-case advantages such as safer trading, easier payments or better yields.\n"
            "Do not repeat website URLs, Telegram handles, GitHub repo names or addresses unless they are needed "
            "as evidence in a sentence.\n"
            "If you mention a domain name, Telegram handle or GitHub repository, it must appear verbatim in the input JSON.\n"
            "Treat null or missing on-chain counters as unavailable evidence, not as zero activity.\n"
            "For telegram.semantic_risk_score, higher means worse. A value near 100 means high semantic risk, not low.\n"
            "Use telegram.semantic_risk_level or telegram.community_health_score when you need a positive/negative interpretation.\n"
            "Do not call the project a clone unless clone_source_project is present.\n"
            "Do not use generic phrases like 'identity confirmed', 'strong project', 'suitable for users', "
            "'significant strengths' or similar wording unless you immediately name the supporting signals or metrics.\n"
            "If evidence is insufficient to explain what the project does, say that the available public evidence is "
            "insufficient instead of guessing.\n"
            "The JSON must contain these string keys exactly:\n"
            "project_name\n"
            "project_type\n"
            "project_overview_en\n"
            "project_overview_ru\n"
            "explanation_text_en\n"
            "explanation_text_ru\n"
            "The JSON must also contain array key evidence_used.\n"
            "evidence_used must list 3-8 concrete evidence keys taken from the input JSON, for example "
            "repository.description, repository.readme_excerpt, identity_confirmation.corroborating_signals, "
            "repository.last_commit_age_days, telegram.posts_30d, telegram.semantic_risk_score, "
            "ton_onchain.tx_count_30d, originality.closest_projects.\n"
            "project_overview_en and project_overview_ru must be 3-5 sentences and answer: what the project does, "
            "who it is for if evidence supports that, how it relates to TON, and the most important verified strengths or limits.\n"
            "At least two sentences in each project_overview field must contain concrete evidence rather than generic evaluation.\n"
            "explanation_text_en and explanation_text_ru must be 2-4 sentences and summarize the validation result with concrete metrics or named evidence.\n"
            "explanation_text fields must mention the overall score and at least two additional metrics or named signals.\n"
            "Treat overall_score and risk_level as separate reported facts. Do not imply that the score causes the risk level or that one directly proves the other.\n"
            "Avoid wording such as 'the score indicates low risk' or similar. Prefer direct formulations like 'Overall score: X. Risk level: Y.' before the supporting evidence.\n"
            "If identity_confirmation.corroborating_signals is present, explanation_text fields must explicitly name those signals.\n"
            "Russian fields must be fully in Russian except official product names, tickers, handles or repository names.\n"
            "English fields must be fully in English except official product names, tickers, handles or repository names.\n"
            f"Input JSON:\n{json.dumps(payload, ensure_ascii=False, indent=2)}"
        )
        raw_text = await context.call_llm(
            block_id=self.block_id,
            model=context.options.llm_model,
            prompt=prompt,
            metadata=payload,
        )
        flags: List[str] = []
        try:
            llm_payload = _parse_llm_json_object(raw_text)
        except Exception:
            llm_payload = {}
            flags.append("llm_invalid_json")
        valid_evidence_keys = _available_evidence_keys(payload)
        raw_evidence_used = [str(item) for item in (llm_payload.get("evidence_used") or []) if normalize_ws(str(item or ""))]
        filtered_evidence_used = [item for item in raw_evidence_used if item in valid_evidence_keys][:12]
        if raw_evidence_used and len(filtered_evidence_used) != len(raw_evidence_used):
            flags.append("llm_invalid_evidence_keys")
        merged = dict(payload)
        merged.update(
            {
                "project_name": _normalized_output_value(llm_payload, "project_name") or str(payload.get("project_name") or ""),
                "project_type": _normalized_output_value(llm_payload, "project_type") or str(payload.get("project_type") or "unknown"),
                "project_overview_en": _normalized_output_value(llm_payload, "project_overview_en"),
                "project_overview_ru": _normalized_output_value(llm_payload, "project_overview_ru"),
                "explanation_text_en": _normalized_output_value(llm_payload, "explanation_text_en"),
                "explanation_text_ru": _normalized_output_value(llm_payload, "explanation_text_ru"),
                "evidence_used": filtered_evidence_used,
            }
        )
        invalid_identifier_mentions = _invalid_identifier_mentions(payload, merged)
        if invalid_identifier_mentions:
            flags.append("llm_invalid_identifier_mentions")
            merged["invalid_identifier_mentions"] = invalid_identifier_mentions
        has_grounding = bool(merged.get("evidence_used")) and not invalid_identifier_mentions
        if not has_grounding:
            if "llm_grounding_incomplete" not in flags:
                flags.append("llm_grounding_incomplete")
            merged["project_overview_en"] = ""
            merged["project_overview_ru"] = ""
            merged["explanation_text_en"] = ""
            merged["explanation_text_ru"] = ""
        explanation_text = merged.get("explanation_text_en") or merged.get("explanation_text_ru") or ""
        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary="Built grounded bilingual explanation" if explanation_text else "Prepared grounded explanation payload",
            metrics={
                "text_chars": len(explanation_text),
                "overview_chars_en": len(str(merged.get("project_overview_en") or "")),
                "overview_chars_ru": len(str(merged.get("project_overview_ru") or "")),
            },
            data=merged,
            text=explanation_text,
            flags=flags,
            confidence=0.72 if explanation_text else 0.35,
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return LLMExplainerBlock(manifest)
