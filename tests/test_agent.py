from __future__ import annotations

import re
import unittest
from datetime import date

from blog_agent.agent import (
    ARTICLE_MAX_WORDS,
    ARTICLE_MIN_WORDS,
    ARTICLE_TARGET_WORDS,
    MAX_META_DESCRIPTION_LENGTH,
    parse_generation_json,
    normalize_generation_payload,
    resolve_article_target_word_count,
    supplement_plan_internal_blog_links,
    validate_article_requirements,
    validate_required_internal_blog_links,
)
from blog_agent.models import BlogArticle, BlogPlan, KeywordCluster, PipelineItem


class GenerationPayloadTests(unittest.TestCase):
    def test_generation_json_accepts_fenced_object(self) -> None:
        payload = parse_generation_json('```json\n{"title": "ok"}\n```')

        self.assertEqual(payload, {"title": "ok"})

    def test_generation_json_reports_malformed_location(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "line 1, column"):
            parse_generation_json('{"body_markdown": "A "broken" quote"}')

    def test_plan_meta_description_is_trimmed_before_validation(self) -> None:
        payload = {
            "title": "A safer towel routine",
            "slug": "safer-towel-routine",
            "target_query": "face towel acne",
            "meta_description": (
                "Your face towel might be making acne-prone skin feel more irritated "
                "through friction, repeated use, and bathroom moisture. Learn what to "
                "change for a gentler routine."
            ),
            "angle": "Explain the routine issue without making medical claims.",
            "outline": ["Problem", "Science", "Habits"],
            "internal_links": [],
            "keywords_to_use": ["face towel acne"],
        }

        plan = BlogPlan.model_validate(normalize_generation_payload(payload))

        self.assertLessEqual(
            len(plan.meta_description),
            MAX_META_DESCRIPTION_LENGTH,
        )

    def test_article_meta_description_is_trimmed_before_validation(self) -> None:
        payload = {
            "title": "A safer towel routine",
            "slug": "safer-towel-routine",
            "meta_description": (
                "Your face towel might be making acne-prone skin feel more irritated "
                "through friction, repeated use, and bathroom moisture. Learn what to "
                "change for a gentler routine."
            ),
            "excerpt": "A practical look at face towel habits.",
            "body_markdown": "## The Problem\n\nBody.",
            "medical_citations": ["Citation"],
            "product_knowledge_used": ["Product fact"],
            "customer_language_used": ["Customer phrase"],
        }

        article = BlogArticle.model_validate(normalize_generation_payload(payload))

        self.assertLessEqual(
            len(article.meta_description),
            MAX_META_DESCRIPTION_LENGTH,
        )

    def test_required_blog_link_validation_requires_multiple_links_when_available(self) -> None:
        body = (
            "Read [the acne guide](/blogs/acne-guide), "
            "[the hygiene guide](/blogs/hygiene-guide), and "
            "[the sensitive skin guide](/blogs/sensitive-skin-guide)."
        )

        validate_required_internal_blog_links(
            body=body,
            internal_links=[
                "/blogs/acne-guide",
                "/blogs/hygiene-guide",
                "/blogs/sensitive-skin-guide",
                "/blogs/bath-towel-guide",
            ],
        )

        with self.assertRaisesRegex(RuntimeError, "Expected at least 3"):
            validate_required_internal_blog_links(
                body="Read [the acne guide](/blogs/acne-guide).",
                internal_links=[
                    "/blogs/acne-guide",
                    "/blogs/hygiene-guide",
                    "/blogs/sensitive-skin-guide",
                ],
            )

    def test_article_guideline_accepts_larger_future_word_count(self) -> None:
        body = _valid_guideline_body(ARTICLE_MIN_WORDS + 40)
        article = BlogArticle.model_validate(
            {
                "title": "A longer skin-safe towel routine",
                "slug": "longer-skin-safe-towel-routine",
                "meta_description": "A longer guide to skin-safe towel routines.",
                "excerpt": "A practical guide to towel hygiene and sensitive skin routines.",
                "body_markdown": body,
                "medical_citations": ["Citation 1", "Citation 2", "Citation 3", "Citation 4"],
                "product_knowledge_used": [
                    "SkinShield technology",
                    "Dual-side design",
                    "Skin-safe fibers",
                    "160-wash durability",
                ],
                "customer_language_used": [
                    "My face feels rough after drying.",
                    "My towel smells musty.",
                    "My skin feels tight.",
                ],
            }
        )

        report = validate_article_requirements(article)

        self.assertTrue(report.checks[0].passed)
        self.assertIn(f"{ARTICLE_MIN_WORDS}-{ARTICLE_MAX_WORDS}", report.checks[0].name)

    def test_article_target_word_count_promotes_old_default_to_larger_target(self) -> None:
        self.assertEqual(resolve_article_target_word_count(2200), ARTICLE_TARGET_WORDS)
        self.assertEqual(resolve_article_target_word_count(3300), 3300)

    def test_plan_internal_links_are_supplemented_from_pipeline(self) -> None:
        plan = BlogPlan(
            title="Face towel acne",
            slug="face-towel-acne",
            target_query="face towel acne",
            meta_description="A guide to face towel acne.",
            angle="Explain the routine issue.",
            outline=["Problem", "Habits"],
            internal_links=["/blogs/existing-planned-link"],
            keywords_to_use=["face towel acne"],
        )

        supplement_plan_internal_blog_links(
            plan=plan,
            cluster=KeywordCluster(
                name="Acne",
                intent="informational",
                queries=["face towel acne"],
                pillar_id="pillar-1",
            ),
            pipeline=[
                _pipeline_item_for_links("one", "Pillar Main", "pillar-1", "pushed"),
                _pipeline_item_for_links("two", "Related Acne", "pillar-1", "approved"),
                _pipeline_item_for_links("three", "General Hygiene", "pillar-2", "pushed"),
            ],
        )

        blog_links = [link for link in plan.internal_links if "/blogs/" in link]
        self.assertGreaterEqual(len(blog_links), 4)


def _valid_guideline_body(target_words: int) -> str:
    sections = [
        "## The Problem They Didn't Know They Had",
        "## The Science Behind The Problem",
        "## The Mechanisms — How It's Actively Hurting You",
        "### Friction and barrier stress",
        "### Moisture and microbial load",
        "## Customer Language — What Real People Were Dealing With",
        "## Actionable Habits — What To Actually Do",
        "### 1. Use a dedicated face towel",
        "### 2. Rotate before it smells musty",
        "### 3. Press instead of rubbing",
        "### 4. Separate gym and face drying",
        "## Why Doctor Towels Was Built For This",
        "## The Bottom Line",
        "## Medical Sources & Further Reading",
    ]
    required = (
        "SkinShield technology Dual-side design Skin-safe fibers 160-wash Apollo Hospitals "
        "https://www.doctortowels.com/pages/research-page "
        "https://cdn.shopify.com/s/files/1/0376/8529/7196/files/Testing_Report.pdf?v=1758528655 "
    )
    filler_words = "gentle towel routine sensitive skin drying hygiene acne bacteria moisture friction ".split()
    words_needed = max(0, target_words - len(re.findall(r"\b[\w'-]+\b", required)) - 80)
    filler = " ".join(filler_words[index % len(filler_words)] for index in range(words_needed))
    return "\n\n".join([sections[0], required, *sections[1:], filler])


def _pipeline_item_for_links(
    slug: str,
    title: str,
    pillar_id: str,
    status: str,
) -> PipelineItem:
    return PipelineItem(
        id=slug,
        post_id=f"2026-05-01-{slug}.md",
        title=title,
        query=title,
        cluster="Acne" if pillar_id == "pillar-1" else "Hygiene",
        pillar_id=pillar_id,
        scheduled_for=date(2026, 5, 1),
        status=status,
        topic_role="side",
        created_at=f"2026-05-01T09:00:0{len(slug)}",
        metadata={"slug": slug},
    )


if __name__ == "__main__":
    unittest.main()
