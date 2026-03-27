from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field


class KeywordCluster(BaseModel):
    name: str
    intent: Literal["transactional", "informational", "comparison"]
    queries: list[str]
    notes: str = ""
    pillar_id: str = ""
    pillar_name: str = ""
    pillar_claim: str = ""
    main_topic: str = ""
    sub_blog_tag: str = ""
    cadence_weight: int = 1
    supporting_keywords: list[str] = Field(default_factory=list)


class TopicHistoryItem(BaseModel):
    title: str
    slug: str
    query: str
    cluster: str
    created_on: date
    output_path: str


class BlogPlan(BaseModel):
    title: str
    slug: str
    target_query: str
    meta_description: str = Field(max_length=160)
    angle: str
    outline: list[str]
    internal_links: list[str]
    keywords_to_use: list[str] = Field(default_factory=list)


class BlogArticle(BaseModel):
    title: str
    slug: str
    meta_description: str = Field(max_length=160)
    excerpt: str
    body_markdown: str
    medical_citations: list[str] = Field(min_length=1)
    product_knowledge_used: list[str] = Field(min_length=1)
    customer_language_used: list[str] = Field(min_length=1)


class GuidelineCheck(BaseModel):
    name: str
    passed: bool
    detail: str


class GuidelineReport(BaseModel):
    score: int
    max_score: int
    checks: list[GuidelineCheck]
    summary: str


class GeneratedPost(BaseModel):
    title: str
    slug: str
    query: str
    cluster: str
    pillar_id: str = ""
    pillar_name: str = ""
    main_topic: str = ""
    sub_blog_tag: str = ""
    output_path: str
    date: date
    guideline_report: GuidelineReport


class PipelineItem(BaseModel):
    id: str
    post_id: str | None = None
    title: str
    query: str
    cluster: str
    pillar_id: str = ""
    pillar_name: str = ""
    pillar_claim: str = ""
    main_topic: str = ""
    sub_blog_tag: str = ""
    is_pillar_head: bool = False
    pillar_head_post_id: str | None = None
    pillar_head_slug: str | None = None
    planned_keywords: list[str] = Field(default_factory=list)
    path: str | None = None
    scheduled_for: date
    status: Literal["topic", "draft", "approved", "pushed", "rejected"] = "topic"
    topic_role: Literal["main", "side"] = "side"
    created_at: str
    approved_at: str | None = None
    pushed_at: str | None = None
    shopify_article_id: str | None = None
    shopify_blog_id: str | None = None
    shopify_article_handle: str | None = None
    topic_angle: str = ""
    topic_outline: list[str] = Field(default_factory=list)
    topic_internal_links: list[str] = Field(default_factory=list)
    guideline_report: GuidelineReport | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
