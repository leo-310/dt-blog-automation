from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel
from pytrends.request import TrendReq


class KeywordIdea(BaseModel):
    keyword: str
    source: str
    kind: str
    score: int | None = None
    trend: str | None = None


class KeywordResearchResult(BaseModel):
    provider: str
    seed_keywords: list[str]
    geo: str
    timeframe: str
    ideas: list[KeywordIdea]
    notes: list[str]


@dataclass
class KeywordResearchRequest:
    seed_keywords: list[str]
    geo: str = "US"
    timeframe: str = "today 3-m"


class KeywordResearchService:
    def __init__(self) -> None:
        self.google_trends = GoogleTrendsResearchProvider()

    def research(self, request: KeywordResearchRequest) -> KeywordResearchResult:
        return self.google_trends.research(request)


class GoogleTrendsResearchProvider:
    provider_name = "google_trends"

    def research(self, request: KeywordResearchRequest) -> KeywordResearchResult:
        pytrends = TrendReq(hl="en-US", tz=360)
        pytrends.build_payload(
            request.seed_keywords,
            cat=0,
            timeframe=request.timeframe,
            geo=request.geo,
            gprop="",
        )

        related_queries = pytrends.related_queries()
        suggestions: list[KeywordIdea] = []

        for seed in request.seed_keywords:
            query_groups = related_queries.get(seed) or {}
            suggestions.extend(
                self._extract_query_group(query_groups.get("top"), seed, "top")
            )
            suggestions.extend(
                self._extract_query_group(query_groups.get("rising"), seed, "rising")
            )
            suggestions.extend(self._extract_suggestions(pytrends, seed))

        if not suggestions:
            for fallback_seed in expand_seed_keywords(request.seed_keywords):
                suggestions.extend(self._extract_suggestions(pytrends, fallback_seed))

        deduped = dedupe_keyword_ideas(suggestions)
        ordered = sorted(
            deduped,
            key=lambda idea: (
                0 if idea.kind == "rising" else 1,
                -(idea.score or -1),
                idea.keyword,
            ),
        )

        notes = [
            "Google Trends scores are relative, not absolute search volume.",
            "Rising queries can surface breakout demand before classic keyword tools catch up.",
            "Use this API to discover angles, then validate final targets in your paid SEO platform if needed.",
        ]

        return KeywordResearchResult(
            provider=self.provider_name,
            seed_keywords=request.seed_keywords,
            geo=request.geo,
            timeframe=request.timeframe,
            ideas=ordered[:30],
            notes=notes,
        )

    @staticmethod
    def _extract_query_group(frame, seed: str, kind: str) -> list[KeywordIdea]:
        if frame is None:
            return []

        ideas: list[KeywordIdea] = []
        for row in frame.to_dict(orient="records"):
            keyword = str(row.get("query", "")).strip()
            if not keyword:
                continue
            value = row.get("value")
            trend = row.get("formattedValue")
            score = int(value) if isinstance(value, (int, float)) else None
            ideas.append(
                KeywordIdea(
                    keyword=keyword,
                    source=f"related_to:{seed}",
                    kind=kind,
                    score=score,
                    trend=str(trend) if trend is not None else None,
                )
            )
        return ideas

    @staticmethod
    def _extract_suggestions(pytrends: TrendReq, seed: str) -> list[KeywordIdea]:
        ideas: list[KeywordIdea] = []
        for item in pytrends.suggestions(seed)[:8]:
            title = str(item.get("title", "")).strip()
            topic_type = str(item.get("type", "")).strip()
            if not title or not is_relevant_keyword_idea(title, topic_type):
                continue
            ideas.append(
                KeywordIdea(
                    keyword=title,
                    source=f"suggestion_for:{seed}",
                    kind="suggestion",
                    trend=topic_type or None,
                )
            )
        return ideas


def dedupe_keyword_ideas(ideas: list[KeywordIdea]) -> list[KeywordIdea]:
    by_keyword: dict[str, KeywordIdea] = {}
    for idea in ideas:
        existing = by_keyword.get(idea.keyword.lower())
        if existing is None:
            by_keyword[idea.keyword.lower()] = idea
            continue

        if rank_idea(idea) > rank_idea(existing):
            by_keyword[idea.keyword.lower()] = idea
    return list(by_keyword.values())


def rank_idea(idea: KeywordIdea) -> tuple[int, int]:
    return (
        2 if idea.kind == "rising" else 1 if idea.kind == "top" else 0,
        idea.score or -1,
    )


def expand_seed_keywords(seed_keywords: list[str]) -> list[str]:
    expanded: list[str] = []
    replacements = {
        "can your towel cause acne": ["acne towel", "face towel acne", "face towel"],
        "acne-safe towel": ["acne towel", "sensitive skin towel", "face towel"],
        "face towel acne": ["acne towel", "face towel", "acne prone skin"],
    }
    for seed in seed_keywords:
        normalized = seed.lower().strip()
        expanded.extend(replacements.get(normalized, []))
        tokens = [token for token in normalized.replace("-", " ").split() if token]
        compact = " ".join(
            token
            for token in tokens
            if token
            not in {"best", "for", "the", "your", "can", "does", "should", "with", "you", "cause"}
        )
        if compact and compact != normalized:
            expanded.append(compact)

    deduped: list[str] = []
    seen: set[str] = set()
    for keyword in expanded:
        if keyword in seen:
            continue
        seen.add(keyword)
        deduped.append(keyword)
    return deduped[:10]


def is_relevant_keyword_idea(title: str, topic_type: str) -> bool:
    haystack = f"{title} {topic_type}".lower()
    positive_tokens = {
        "acne",
        "skin",
        "sensitive",
        "towel",
        "face",
        "washcloth",
        "cloth",
        "cotton",
        "microfiber",
        "microfibre",
        "breakout",
        "benzoyl",
        "clean",
        "wipe",
        "facial",
    }
    negative_tokens = {
        "cartoon",
        "childrens",
        "children",
        "lavender",
        "printer",
        "vacuum",
        "electric",
        "artifact",
        "classic reprint",
        "gazette",
    }
    if any(token in haystack for token in negative_tokens):
        return False
    return any(token in haystack for token in positive_tokens)
