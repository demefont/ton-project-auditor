from __future__ import annotations

import asyncio

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult
from identity_validator.sources import (
    get_ton_account_activity,
    get_ton_mcp_back_resolve_dns,
    get_ton_mcp_balance_by_address,
    get_ton_mcp_known_jettons,
    get_ton_mcp_resolve_dns,
)
from identity_validator.utils import extract_ton_addresses, extract_ton_dns_domains, normalize_ws


class AddressSignalBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        repo_result = context.get_result("github_repo")
        tg_result = context.get_result("telegram_channel")
        repo_parts = [
            str(context.case.wallet_address or ""),
            str(context.case.project_url or ""),
            str(context.case.github_repo or ""),
            str(context.case.description or ""),
        ]
        telegram_parts = [
            str(context.case.wallet_address or ""),
            str(context.case.telegram_handle or ""),
        ]
        if repo_result and repo_result.status == "success":
            repo_data = repo_result.data or {}
            repo_meta = (repo_data.get("repo") or {}) if isinstance(repo_data, dict) else {}
            repo_parts.extend(
                [
                    str(repo_meta.get("homepage") or ""),
                    str(repo_meta.get("description") or ""),
                    str(repo_meta.get("full_name") or ""),
                    str(repo_data.get("readme_excerpt") or ""),
                ]
            )
        if tg_result and tg_result.status == "success":
            tg_data = tg_result.data or {}
            telegram_parts.extend(
                [
                    str(tg_data.get("posts_excerpt") or ""),
                    " ".join(str(item.get("url") or "") for item in tg_data.get("entries") or [] if isinstance(item, dict)),
                ]
            )
        repo_text = " ".join(repo_parts)
        telegram_text = " ".join(telegram_parts)
        docs_addresses = extract_ton_addresses(repo_text)
        tg_addresses = extract_ton_addresses(telegram_text)
        unique_addresses = []
        for address in docs_addresses + tg_addresses:
            if address not in unique_addresses:
                unique_addresses.append(address)
        dns_candidates = []
        for domain in extract_ton_dns_domains(" ".join(repo_parts + telegram_parts)):
            if domain not in dns_candidates:
                dns_candidates.append(domain)
        ton_mcp_payload = await get_ton_mcp_known_jettons(context.case, context.options)
        matched_jetton = {}
        known_jettons = [item for item in ton_mcp_payload.get("jettons") or [] if isinstance(item, dict)]
        known_by_address = {
            normalize_ws(str(item.get("address") or "")): item
            for item in known_jettons
            if normalize_ws(str(item.get("address") or ""))
        }
        for address in unique_addresses:
            candidate = known_by_address.get(normalize_ws(address))
            if candidate:
                matched_jetton = dict(candidate)
                break
        primary_address = normalize_ws(str(context.case.wallet_address or "")) or (unique_addresses[0] if unique_addresses else "")
        ton_activity = {
            "status": "skipped",
            "summary": "",
            "address": primary_address,
            "last_tx_at": "",
            "last_tx_age_days": -1,
            "tx_count_7d": 0,
            "tx_count_30d": 0,
            "tx_count_30d_limit_hit": False,
            "sample_transactions": [],
        }
        reverse_dns = {"status": "skipped", "summary": "", "address": primary_address, "domain": ""}
        balance = {"status": "skipped", "summary": "", "address": primary_address, "balance": "", "balance_nano": ""}
        resolved_domains = []
        dns_tasks = []
        if primary_address:
            dns_tasks.extend(
                [
                    get_ton_account_activity(context.case, context.options, primary_address),
                    get_ton_mcp_back_resolve_dns(context.case, context.options, primary_address),
                    get_ton_mcp_balance_by_address(context.case, context.options, primary_address),
                ]
            )
        for domain in dns_candidates[:3]:
            dns_tasks.append(get_ton_mcp_resolve_dns(context.case, context.options, domain))
        if dns_tasks:
            dns_results = await asyncio.gather(*dns_tasks)
            cursor = 0
            if primary_address:
                ton_activity = dict(dns_results[cursor] or {})
                cursor += 1
                reverse_dns = dict(dns_results[cursor] or {})
                cursor += 1
                balance = dict(dns_results[cursor] or {})
                cursor += 1
            resolved_domains = [dict(item or {}) for item in dns_results[cursor:]]
        dns_match = {}
        for item in resolved_domains:
            if normalize_ws(str(item.get("address") or "")) and normalize_ws(str(item.get("address") or "")) == primary_address:
                dns_match = item
                break
        summary = f"Extracted {len(unique_addresses)} TON-style addresses"
        if matched_jetton:
            symbol = str(matched_jetton.get("symbol") or matched_jetton.get("name") or matched_jetton.get("address") or "jetton")
            summary += f"; TON MCP matched known jetton {symbol}"
        elif ton_mcp_payload.get("status") == "success":
            summary += f"; TON MCP checked {int(ton_mcp_payload.get('count') or 0)} known jettons"
        elif ton_mcp_payload.get("status") == "error":
            summary += f"; TON MCP lookup error: {str(ton_mcp_payload.get('summary') or '')}"
        if reverse_dns.get("status") == "success" and reverse_dns.get("domain"):
            summary += f"; reverse DNS {str(reverse_dns.get('domain') or '')}"
        if balance.get("status") == "success" and balance.get("balance"):
            summary += f"; on-chain balance {str(balance.get('balance') or '')}"
        if dns_match:
            summary += f"; TON DNS matched {str(dns_match.get('domain') or '')}"
        if ton_activity.get("status") == "success" and int(ton_activity.get("tx_count_30d") or 0) > 0:
            summary += (
                f"; {int(ton_activity.get('tx_count_30d') or 0)} tx / 30d"
                f", {int(ton_activity.get('tx_count_7d') or 0)} tx / 7d"
            )
        elif ton_activity.get("status") == "success" and ton_activity.get("last_tx_age_days") not in (None, "") and int(ton_activity.get("last_tx_age_days")) >= 0:
            summary += f"; last on-chain tx {int(ton_activity.get('last_tx_age_days'))} days ago"
        evidence = unique_addresses[:10]
        if matched_jetton.get("address") and matched_jetton.get("address") not in evidence:
            evidence.append(str(matched_jetton.get("address")))
        if reverse_dns.get("domain") and reverse_dns.get("domain") not in evidence:
            evidence.append(str(reverse_dns.get("domain")))
        if balance.get("balance"):
            evidence.append(str(balance.get("balance")))
        if ton_activity.get("last_tx_at"):
            evidence.append(str(ton_activity.get("last_tx_at")))
        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=summary,
            metrics={
                "docs_address_count": len(docs_addresses),
                "telegram_address_count": len(tg_addresses),
                "unique_address_count": len(unique_addresses),
                "ton_mcp_known_jetton_match": 1 if matched_jetton else 0,
                "ton_mcp_known_jetton_count": int(ton_mcp_payload.get("count") or 0),
                "ton_mcp_reverse_dns_found": 1 if reverse_dns.get("status") == "success" and reverse_dns.get("domain") else 0,
                "ton_mcp_balance_checked": 1 if balance.get("status") == "success" and balance.get("balance") else 0,
                "ton_mcp_dns_match": 1 if dns_match else 0,
                "ton_mcp_dns_candidates_count": len(dns_candidates),
                "ton_address_activity_checked": 1 if ton_activity.get("status") == "success" else 0,
                "ton_address_last_tx_age_days": (
                    -1
                    if ton_activity.get("last_tx_age_days") in (None, "")
                    else int(ton_activity.get("last_tx_age_days"))
                ),
                "ton_address_tx_count_7d": int(ton_activity.get("tx_count_7d") or 0),
                "ton_address_tx_count_30d": int(ton_activity.get("tx_count_30d") or 0),
            },
            data={
                "docs_addresses": docs_addresses[:10],
                "telegram_addresses": tg_addresses[:10],
                "unique_addresses": unique_addresses[:20],
                "ton_activity": ton_activity,
                "ton_mcp": {
                    "status": str(ton_mcp_payload.get("status") or "unknown"),
                    "summary": str(ton_mcp_payload.get("summary") or ""),
                    "matched_jetton": matched_jetton,
                    "dns_candidates": dns_candidates[:10],
                    "resolved_domains": resolved_domains,
                    "dns_match": dns_match,
                    "reverse_dns": reverse_dns,
                    "balance": balance,
                },
            },
            evidence=evidence,
            confidence=1.0,
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return AddressSignalBlock(manifest)
