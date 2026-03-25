from __future__ import annotations

from datetime import datetime, timezone

from identity_validator.base import BaseBlock, BlockManifest, ExecutionContext
from identity_validator.models import BlockResult
from identity_validator.sources import get_telegram_snapshot
from identity_validator.utils import TON_KEYWORDS, clip_text, days_since_iso, keyword_hits, parse_github_datetime, scam_keyword_hits, stable_score


class TelegramChannelBlock(BaseBlock):
    async def run(self, context: ExecutionContext) -> BlockResult:
        if not context.case.telegram_handle:
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary="Telegram handle is not provided",
                flags=["missing_telegram_handle"],
            )
        try:
            snapshot = await get_telegram_snapshot(context.case, context.options)
        except Exception as exc:
            return BlockResult(
                block_id=self.block_id,
                status="skipped",
                summary=f"Telegram snapshot is unavailable: {exc}",
                data={
                    "handle": context.case.telegram_handle,
                    "exception": repr(exc),
                },
                flags=["telegram_unavailable"],
            )
        posts = [str(item) for item in snapshot.get("posts") or []]
        entries = [item for item in snapshot.get("entries") or [] if isinstance(item, dict)]
        joined = "\n".join(posts)
        ton_hits = keyword_hits(joined, TON_KEYWORDS)
        scam_hits = scam_keyword_hits(joined)
        source = str(snapshot.get("source") or "")
        reference_now = parse_github_datetime(str(snapshot.get("fetched_at") or "")) or datetime.now(timezone.utc)
        dated_entries = [item for item in entries if parse_github_datetime(str(item.get("published_at") or ""))]

        def posts_within(days: int) -> int:
            count = 0
            for item in dated_entries:
                age_days = days_since_iso(str(item.get("published_at") or ""), now=reference_now)
                if age_days is not None and age_days <= days:
                    count += 1
            return count

        def active_days(days: int) -> int:
            seen = set()
            for item in dated_entries:
                published_at = str(item.get("published_at") or "")
                age_days = days_since_iso(published_at, now=reference_now)
                if age_days is not None and age_days <= days and published_at:
                    seen.add(published_at[:10])
            return len(seen)

        last_post_age_days = days_since_iso(str(dated_entries[0].get("published_at") or ""), now=reference_now) if dated_entries else None
        posts_7d = posts_within(7)
        posts_30d = posts_within(30)
        posts_90d = posts_within(90)
        active_days_30d = active_days(30)
        active_days_90d = active_days(90)
        median_gap_between_posts_days = -1.0
        if len(dated_entries) >= 2:
            parsed_dates = [
                parse_github_datetime(str(item.get("published_at") or ""))
                for item in dated_entries
            ]
            parsed_dates = [item for item in parsed_dates if item is not None]
            gaps = []
            for left, right in zip(parsed_dates, parsed_dates[1:]):
                gaps.append(round((left - right).total_seconds() / 86400, 2))
            if gaps:
                gaps_sorted = sorted(gaps)
                middle = len(gaps_sorted) // 2
                if len(gaps_sorted) % 2:
                    median_gap_between_posts_days = gaps_sorted[middle]
                else:
                    median_gap_between_posts_days = round((gaps_sorted[middle - 1] + gaps_sorted[middle]) / 2, 2)

        freshness_points = 0
        if last_post_age_days is not None:
            if last_post_age_days <= 3:
                freshness_points = 35
            elif last_post_age_days <= 7:
                freshness_points = 28
            elif last_post_age_days <= 30:
                freshness_points = 18
            elif last_post_age_days <= 90:
                freshness_points = 8
        community_activity_score = stable_score(
            freshness_points
            + min(30, posts_30d * 3)
            + min(20, active_days_30d * 2)
            + min(15, posts_90d)
        )
        return BlockResult(
            block_id=self.block_id,
            status="success",
            summary=f"Loaded Telegram snapshot with {len(posts)} posts from {source or 'unknown_source'}",
            metrics={
                "post_count": len(posts),
                "dated_post_count": len(dated_entries),
                "ton_keyword_hits": len(ton_hits),
                "scam_keyword_hits": len(scam_hits),
                "last_post_age_days": last_post_age_days if last_post_age_days is not None else -1,
                "posts_7d": posts_7d,
                "posts_30d": posts_30d,
                "posts_90d": posts_90d,
                "active_days_30d": active_days_30d,
                "active_days_90d": active_days_90d,
                "median_gap_between_posts_days": median_gap_between_posts_days,
                "community_activity_score": community_activity_score,
            },
            data={
                "handle": snapshot.get("handle") or context.case.telegram_handle,
                "source": source,
                "fetched_at": snapshot.get("fetched_at") or "",
                "entries": entries[:20],
                "posts": posts[:10],
                "posts_excerpt": clip_text(joined, 2500),
            },
            evidence=posts[:3],
            flags=["telegram_scam_terms_detected"] if scam_hits else [],
            confidence=1.0,
        )


def build_block(manifest: BlockManifest) -> BaseBlock:
    return TelegramChannelBlock(manifest)
