from __future__ import annotations

import unittest

from blog_agent.agent import MAX_META_DESCRIPTION_LENGTH, normalize_generation_payload
from blog_agent.models import BlogArticle, BlogPlan


class GenerationPayloadTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
