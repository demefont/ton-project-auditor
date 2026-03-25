from __future__ import annotations

import json
import re
from typing import Iterable, List

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult
from identity_validator.utils import keyword_hits, normalize_ws, scam_keyword_hits, stable_score

CTA_KEYWORDS = (
    "buy",
    "claim",
    "join",
    "play",
    "register",
    "follow",
    "mint",
    "watch",
    "visit",
    "shop",
    "connect a wallet",
    "subscribe",
)
PROMO_KEYWORDS = (
    "discount",
    "bonus",
    "reward",
    "rewards",
    "airdrop",
    "giveaway",
    "gift",
    "free battle",
    "free lootbox",
    "lootbox",
    "shop",
    "buy",
    "claim",
    "mint",
    "campaign",
)
URGENCY_KEYWORDS = (
    "limited",
    "last chance",
    "only today",
    "ending soon",
    "spots left",
    "hurry",
    "join now",
    "act now",
)
TOPIC_RULES = {
    "gamefi_updates": ("game", "district clash", "mercenary", "battle", "lootbox", "play"),
    "nft_marketplace": ("nft", "marketplace", "collection", "holders", "mint", "getgems", "hot craft"),
    "rewards_campaigns": ("discount", "reward", "gift", "bonus", "airdrop", "giveaway", "free"),
    "product_updates": ("update", "release", "launch", "available", "new game"),
}


def _contains_any(text: str, keywords: Iterable[str]) -> bool:
    low = str(text or "").lower()
    return any(str(keyword).lower() in low for keyword in keywords)


def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]{3,}", str(text or "").lower())


def _jaccard_similarity(left: str, right: str) -> float:
    left_tokens = set(_tokenize(left))
    right_tokens = set(_tokenize(right))
    if not left_tokens and not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


class TelegramSemanticsBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        tg_result = context.get_result("telegram_channel")
        if not tg_result or tg_result.status != "success":
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary="Telegram snapshot is unavailable",
                flags=["missing_telegram_snapshot"],
            )
        tg_metrics = tg_result.metrics or {}
        entries = [item for item in (tg_result.data or {}).get("entries") or [] if isinstance(item, dict)]
        posts = [normalize_ws(str(item.get("text") or "")) for item in entries if normalize_ws(str(item.get("text") or ""))]
        if not posts:
            posts = [normalize_ws(str(item)) for item in (tg_result.data or {}).get("posts") or [] if normalize_ws(str(item))]
        total_posts = len(posts)
        normalized_posts = [post.lower() for post in posts]
        exact_duplicate_count = total_posts - len(set(normalized_posts))
        duplicate_post_ratio = round(exact_duplicate_count / total_posts, 4) if total_posts else 0.0

        near_duplicate_pairs = 0
        comparisons = 0
        for index in range(total_posts):
            for next_index in range(index + 1, total_posts):
                comparisons += 1
                if _jaccard_similarity(posts[index], posts[next_index]) >= 0.78:
                    near_duplicate_pairs += 1
        near_duplicate_pair_ratio = round(near_duplicate_pairs / comparisons, 4) if comparisons else 0.0

        cta_posts = sum(1 for post in posts if _contains_any(post, CTA_KEYWORDS))
        promo_posts = sum(1 for post in posts if _contains_any(post, PROMO_KEYWORDS))
        urgency_posts = sum(1 for post in posts if _contains_any(post, URGENCY_KEYWORDS))
        scam_posts = sum(1 for post in posts if scam_keyword_hits(post))
        cta_post_ratio = round(cta_posts / total_posts, 4) if total_posts else 0.0
        promo_post_ratio = round(promo_posts / total_posts, 4) if total_posts else 0.0
        urgency_post_ratio = round(urgency_posts / total_posts, 4) if total_posts else 0.0

        topic_counts = {}
        for label, keywords in TOPIC_RULES.items():
            topic_counts[label] = sum(1 for post in posts if _contains_any(post, keywords))
        dominant_topics = [label for label, count in sorted(topic_counts.items(), key=lambda item: item[1], reverse=True) if count > 0][:3]

        penalties = 0
        penalties += min(60, scam_posts * 30)
        penalties += min(25, int(round(promo_post_ratio * 35)))
        penalties += min(15, int(round(cta_post_ratio * 20)))
        penalties += min(10, int(round(urgency_post_ratio * 20)))
        if duplicate_post_ratio >= 0.1:
            penalties += 15
        if near_duplicate_pair_ratio >= 0.18:
            penalties += 15
        if int(tg_metrics.get("posts_30d") or 0) >= 8 and int(tg_metrics.get("active_days_30d") or 0) <= 2:
            penalties += 10
        if total_posts >= 8 and len(dominant_topics) <= 1:
            penalties += 10
        semantic_risk_score = stable_score(penalties)
        community_health_score = stable_score(100 - semantic_risk_score)
        semantic_risk_level = "low"
        if semantic_risk_score >= 60:
            semantic_risk_level = "high"
        elif semantic_risk_score >= 30:
            semantic_risk_level = "moderate"

        flags = []
        if scam_posts:
            flags.append("telegram_semantic_scam_signals")
        if promo_post_ratio >= 0.6:
            flags.append("telegram_feed_is_overly_promotional")
        if duplicate_post_ratio >= 0.1 or near_duplicate_pair_ratio >= 0.18:
            flags.append("telegram_feed_is_repetitive")
        if urgency_post_ratio >= 0.25:
            flags.append("telegram_uses_urgency_marketing")
        if int(tg_metrics.get("posts_30d") or 0) >= 8 and int(tg_metrics.get("active_days_30d") or 0) <= 2:
            flags.append("telegram_activity_is_bursty")

        content_labels = []
        if dominant_topics:
            content_labels.extend(dominant_topics)
        if promo_post_ratio >= 0.5:
            content_labels.append("promotional_feed")
        if duplicate_post_ratio > 0 or near_duplicate_pair_ratio >= 0.1:
            content_labels.append("repetitive_patterns")
        if scam_posts:
            content_labels.append("scam_risk")

        post_samples = [
            {
                "date_text": str(item.get("date_text") or ""),
                "published_at": str(item.get("published_at") or ""),
                "text": normalize_ws(str(item.get("text") or "")),
            }
            for item in entries[:8]
            if normalize_ws(str(item.get("text") or ""))
        ]
        if not post_samples:
            post_samples = [{"date_text": "", "published_at": "", "text": post} for post in posts[:8]]

        metadata = {
            "project_name": context.case.name,
            "analysis_type": "telegram_semantics",
            "semantic_risk_level": semantic_risk_level,
            "semantic_risk_score": semantic_risk_score,
            "community_health_score": community_health_score,
            "content_labels": content_labels,
            "dominant_topics": dominant_topics,
            "flags": flags,
            "promo_post_ratio": promo_post_ratio,
            "cta_post_ratio": cta_post_ratio,
            "duplicate_post_ratio": duplicate_post_ratio,
            "near_duplicate_pair_ratio": near_duplicate_pair_ratio,
            "last_post_age_days": int(tg_metrics.get("last_post_age_days") or -1),
            "posts_30d": int(tg_metrics.get("posts_30d") or 0),
            "active_days_30d": int(tg_metrics.get("active_days_30d") or 0),
            "post_samples": post_samples,
        }
        prompt = (
            "Analyze recent public Telegram channel posts for project health.\n"
            "Focus on four points: main content themes, whether activity looks recent and organic, "
            "whether the feed is mostly promotional or repetitive, and whether there are scam-like signs.\n"
            "Be specific and concise.\n\n"
            f"Input JSON:\n{json.dumps(metadata, ensure_ascii=False, indent=2)}"
        )
        text = await context.call_llm(
            block_id=self.block_id,
            model=context.options.llm_model,
            prompt=prompt,
            metadata=metadata,
        )

        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=f"Telegram semantic risk={semantic_risk_level} topics={','.join(dominant_topics) or 'none'}",
            metrics={
                "semantic_risk_score": semantic_risk_score,
                "community_health_score": community_health_score,
                "promo_post_ratio": promo_post_ratio,
                "cta_post_ratio": cta_post_ratio,
                "urgency_post_ratio": urgency_post_ratio,
                "duplicate_post_ratio": duplicate_post_ratio,
                "near_duplicate_pair_ratio": near_duplicate_pair_ratio,
                "dominant_topic_count": len(dominant_topics),
                "scam_post_count": scam_posts,
            },
            data={
                "dominant_topics": dominant_topics,
                "content_labels": content_labels,
                "post_samples": post_samples,
                "keyword_scam_hits": scam_keyword_hits("\n".join(posts)),
                "semantic_risk_level": semantic_risk_level,
            },
            evidence=flags + dominant_topics,
            flags=flags,
            text=text,
            confidence=0.78,
            needs_human_review=semantic_risk_level == "high",
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return TelegramSemanticsBlock(manifest)
