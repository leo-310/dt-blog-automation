from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path

from .config import AgentConfig, CONTENT_DIR, DATA_DIR, PROMPTS_DIR
from .models import (
    BlogArticle,
    BlogPlan,
    GeneratedPost,
    GuidelineCheck,
    GuidelineReport,
    KeywordCluster,
    TopicHistoryItem,
)
from .provider import BlogAgentProvider
from .storage import (
    append_history,
    build_frontmatter,
    ensure_directories,
    load_history,
    load_keyword_clusters,
    load_required_markdown,
    load_source_library,
)


class BlogAgent:
    def __init__(self, config: AgentConfig) -> None:
        self.config = config
        self.provider = BlogAgentProvider(config)
        ensure_directories([DATA_DIR, CONTENT_DIR, PROMPTS_DIR])

    def _load_generation_context(self) -> dict:
        clusters = load_keyword_clusters(self.config.topic_file)
        history = load_history(self.config.history_file)
        brand_brief = self.config.brand_brief_file.read_text()
        approved_facts = self.config.approved_facts_file.read_text()
        style_guide = self.config.style_guide_file.read_text()
        product_knowledge = load_required_markdown(
            self.config.product_knowledge_file,
            "product knowledge",
        )
        medical_research = load_required_markdown(
            self.config.medical_research_file,
            "medical research",
        )
        customer_language = load_required_markdown(
            self.config.customer_language_file,
            "customer language",
        )
        source_library = load_source_library(self.config.sources_dir)
        recent_queries = [item.query for item in history[-20:]]
        system_prompt = (PROMPTS_DIR / "system_prompt.md").read_text()
        return {
            "clusters": clusters,
            "history": history,
            "brand_brief": brand_brief,
            "approved_facts": approved_facts,
            "style_guide": style_guide,
            "product_knowledge": product_knowledge,
            "medical_research": medical_research,
            "customer_language": customer_language,
            "source_library": source_library,
            "recent_queries": recent_queries,
            "system_prompt": system_prompt,
        }

    def plan_topic(
        self,
        *,
        blocked_queries: list[str] | None = None,
        preferred_pillar_id: str | None = None,
    ) -> tuple[BlogPlan, KeywordCluster]:
        context = self._load_generation_context()
        clusters = context["clusters"]
        if preferred_pillar_id:
            filtered = [
                cluster
                for cluster in clusters
                if getattr(cluster, "pillar_id", "") == preferred_pillar_id
            ]
            if filtered:
                clusters = filtered
        recent_queries = context["recent_queries"] + (blocked_queries or [])
        topic_prompt = (PROMPTS_DIR / "topic_planner_prompt.md").read_text().format(
            brand_name=self.config.brand_name,
            product_name=self.config.product_name,
            website_url=self.config.website_url,
            clusters=json.dumps(
                [cluster.model_dump(mode="json") for cluster in clusters], indent=2
            ),
            recent_queries=json.dumps(recent_queries, indent=2),
            brand_brief=context["brand_brief"],
            approved_facts=context["approved_facts"],
            style_guide=context["style_guide"],
            product_knowledge=context["product_knowledge"],
            medical_research=context["medical_research"],
            customer_language=context["customer_language"],
            source_library=context["source_library"],
        )
        plan_response = self.provider.complete(context["system_prompt"], topic_prompt)
        plan = BlogPlan.model_validate(json.loads(extract_json(plan_response)))
        cluster = find_cluster(plan.target_query, clusters)
        if cluster is None:
            fallback_pillar = clusters[0] if clusters else None
            cluster = KeywordCluster(
                name="custom",
                intent="informational",
                queries=[plan.target_query],
                notes="Fallback cluster when planner chose a custom query.",
                pillar_id=fallback_pillar.pillar_id if fallback_pillar else "",
                pillar_name=fallback_pillar.pillar_name if fallback_pillar else "",
                pillar_claim=fallback_pillar.pillar_claim if fallback_pillar else "",
                main_topic=fallback_pillar.main_topic if fallback_pillar else "",
                sub_blog_tag=sanitize_slug(plan.target_query),
            )
        ensure_plan_has_main_blog_link(plan, cluster)
        return plan, cluster

    def generate_post_from_plan(
        self,
        *,
        plan: BlogPlan,
        cluster: KeywordCluster,
        today: date | None = None,
    ) -> GeneratedPost:
        today = today or date.today()
        context = self._load_generation_context()

        article_prompt_template = (PROMPTS_DIR / "article_writer_prompt.md").read_text()
        article: BlogArticle | None = None
        guideline_report: GuidelineReport | None = None
        last_error: RuntimeError | None = None
        remediation = ""
        max_attempts = 3

        for _attempt in range(max_attempts):
            article_prompt = article_prompt_template.format(
                brand_name=self.config.brand_name,
                product_name=self.config.product_name,
                website_url=self.config.website_url,
                cta_url=self.config.primary_cta_url,
                author_name=self.config.author_name,
                target_word_count=self.config.default_word_count,
                plan_json=json.dumps(plan.model_dump(mode="json"), indent=2),
                brand_brief=context["brand_brief"],
                approved_facts=context["approved_facts"],
                style_guide=context["style_guide"],
                product_knowledge=context["product_knowledge"],
                medical_research=context["medical_research"],
                customer_language=context["customer_language"],
                source_library=context["source_library"],
            )
            if remediation:
                article_prompt = (
                    f"{article_prompt}\n\n"
                    "Previous attempt failed quality checks. Regenerate and fix all failures.\n"
                    f"Failure details: {remediation}\n"
                    "Return only one valid JSON object."
                )

            article_response = self.provider.complete(context["system_prompt"], article_prompt)
            article = BlogArticle.model_validate(json.loads(extract_json(article_response)))
            try:
                guideline_report = validate_article_requirements(article)
                validate_required_internal_blog_links(
                    body=article.body_markdown,
                    internal_links=plan.internal_links,
                )
                validate_required_keywords_in_body(
                    body=article.body_markdown,
                    required_keywords=plan.keywords_to_use,
                )
                last_error = None
                break
            except RuntimeError as exc:
                last_error = exc
                remediation = str(exc)

        if last_error:
            raise last_error
        assert article is not None
        assert guideline_report is not None

        slug = sanitize_slug(article.slug or plan.slug)
        frontmatter = build_frontmatter(
            title=article.title,
            description=article.meta_description,
            excerpt=article.excerpt,
            today=today,
        )
        output_path = CONTENT_DIR / f"{today.isoformat()}-{slug}.md"
        output_path.write_text(
            f"{frontmatter}\n\n{article.body_markdown.strip()}\n\n"
            f"{render_citations_section(article.medical_citations)}\n"
        )

        append_history(
            self.config.history_file,
            TopicHistoryItem(
                title=article.title,
                slug=slug,
                query=plan.target_query,
                cluster=cluster.name,
                created_on=today,
                output_path=str(output_path),
            ),
        )
        return GeneratedPost(
            title=article.title,
            slug=slug,
            query=plan.target_query,
            cluster=cluster.name,
            pillar_id=cluster.pillar_id,
            pillar_name=cluster.pillar_name,
            main_topic=cluster.main_topic,
            sub_blog_tag=cluster.sub_blog_tag,
            output_path=str(output_path),
            date=today,
            guideline_report=guideline_report,
        )

    def generate_post(self, today: date | None = None) -> GeneratedPost:
        plan, cluster = self.plan_topic()
        return self.generate_post_from_plan(
            plan=plan,
            cluster=cluster,
            today=today,
        )


def extract_json(text: str) -> str:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise RuntimeError("The model did not return a JSON object.")
    return match.group(0)


def sanitize_slug(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
    return re.sub(r"-{2,}", "-", cleaned)


def find_cluster(query: str, clusters: list[KeywordCluster]) -> KeywordCluster | None:
    normalized_query = query.strip().lower()
    for cluster in clusters:
        normalized_candidates = [candidate.strip().lower() for candidate in cluster.queries]
        if normalized_query in normalized_candidates:
            return cluster
    return None


def normalize_internal_links(links: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for link in links:
        cleaned = str(link).strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cleaned)
    return deduped


def build_cluster_main_blog_link(cluster: KeywordCluster) -> str:
    primary = cluster.main_topic.strip() or cluster.pillar_name.strip()
    if not primary:
        return ""
    return f"/blogs/{sanitize_slug(primary)}"


def ensure_plan_has_main_blog_link(plan: BlogPlan, cluster: KeywordCluster) -> None:
    main_blog_url = build_cluster_main_blog_link(cluster)
    if not main_blog_url:
        return
    plan.internal_links = normalize_internal_links([*plan.internal_links, main_blog_url])


def validate_required_internal_blog_links(body: str, internal_links: list[str]) -> None:
    required_blog_links = [
        link.strip()
        for link in internal_links
        if str(link).strip() and "/blogs/" in str(link).lower()
    ]
    if not required_blog_links:
        return

    body_targets = re.findall(r"\]\(\s*([^)]+?)\s*\)", body, re.IGNORECASE)
    normalized_body_targets = {
        re.sub(r"/+$", "", target.strip().lower())
        for target in body_targets
        if target.strip()
    }

    for link in required_blog_links:
        normalized_link = re.sub(r"/+$", "", link.lower())
        if normalized_link in normalized_body_targets:
            return

    raise RuntimeError(
        "Generated article is missing the required main blog internal link in body_markdown. "
        f"Expected one of: {required_blog_links}"
    )


def normalize_keyword_text(value: str) -> str:
    lowered = re.sub(r"[^a-z0-9\s]+", " ", str(value or "").lower())
    return re.sub(r"\s+", " ", lowered).strip()


def validate_required_keywords_in_body(body: str, required_keywords: list[str]) -> None:
    if not required_keywords:
        return
    normalized_body = normalize_keyword_text(body)
    missing: list[str] = []
    for keyword in required_keywords:
        normalized_keyword = normalize_keyword_text(keyword)
        if not normalized_keyword:
            continue
        if normalized_keyword not in normalized_body:
            missing.append(keyword)
    if missing:
        raise RuntimeError(
            "Generated article is missing required keywords in body_markdown: "
            + ", ".join(missing)
        )


def validate_article_requirements(article: BlogArticle) -> GuidelineReport:
    checks: list[GuidelineCheck] = []
    body = article.body_markdown
    words = len(re.findall(r"\b[\w'-]+\b", body))
    h2_headings = [line.strip().lower() for line in body.splitlines() if line.startswith("## ")]
    h3_headings = [line.strip() for line in body.splitlines() if line.startswith("### ")]
    numbered_h3 = [
        line for line in h3_headings if re.match(r"^###\s+\d+\.\s+", line)
    ]
    expected_sections = [
        "## the problem they didn't know they had",
        "## the science behind the problem",
        "## the mechanisms — how it's actively hurting you",
        "## customer language — what real people were dealing with",
        "## actionable habits — what to actually do",
        "## why doctor towels was built for this",
        "## the bottom line",
        "## medical sources & further reading",
    ]
    has_required_h2 = all(
        any(expected in heading for heading in h2_headings) for expected in expected_sections
    )
    numbered_habits = [line for line in h3_headings if re.match(r"^###\s+\d+\.\s+", line)]
    required_links = [
        "https://www.doctortowels.com/pages/research-page",
        "https://cdn.shopify.com/s/files/1/0376/8529/7196/files/Testing_Report.pdf?v=1758528655",
    ]
    body_lower = body.lower()
    has_required_links = all(link.lower() in body_lower for link in required_links)
    product_pillars = [
        "skinshield technology",
        "dual-side design",
        "skin-safe fibers",
        "160-wash",
        "apollo hospitals",
    ]
    product_pillar_mentions = sum(1 for term in product_pillars if term in body_lower)
    checks.append(
        GuidelineCheck(
            name="Word Count 1800-2500",
            passed=1800 <= words <= 2500,
            detail=f"Detected approximately {words} words.",
        )
    )
    checks.append(
        GuidelineCheck(
            name="Named Medical Citations",
            passed=len(article.medical_citations) >= 4,
            detail=f"Detected {len(article.medical_citations)} citations.",
        )
    )
    checks.append(
        GuidelineCheck(
            name="Mandatory H2 Structure",
            passed=has_required_h2,
            detail="Checked all 8 required H2 sections in the mandated order family.",
        )
    )
    checks.append(
        GuidelineCheck(
            name="Mechanism + Habit Subsections",
            passed=len(h3_headings) >= 6 and 4 <= len(numbered_habits) <= 6,
            detail=f"Detected {len(h3_headings)} H3 headings and {len(numbered_habits)} numbered habits.",
        )
    )
    checks.append(
        GuidelineCheck(
            name="Product Knowledge Coverage",
            passed=len(article.product_knowledge_used) >= 4,
            detail=f"Detected {len(article.product_knowledge_used)} product knowledge references.",
        )
    )
    checks.append(
        GuidelineCheck(
            name="Customer Language Coverage",
            passed=len(article.customer_language_used) >= 3,
            detail=f"Detected {len(article.customer_language_used)} customer-language references.",
        )
    )
    checks.append(
        GuidelineCheck(
            name="Medical Sources Section In Body",
            passed=any("## medical sources & further reading" in heading for heading in h2_headings),
            detail="Requires an explicit medical sources section before app-level citation append.",
        )
    )
    checks.append(
        GuidelineCheck(
            name="Doctor Towels Research Links Included",
            passed=has_required_links,
            detail="Checked required Doctor Towels research-page and testing report links.",
        )
    )
    checks.append(
        GuidelineCheck(
            name="Core Product Pillars Covered",
            passed=product_pillar_mentions >= 4,
            detail=f"Detected {product_pillar_mentions} product-pillar mentions.",
        )
    )
    checks.append(
        GuidelineCheck(
            name="No Hard CTA",
            passed="click here to buy" not in body.lower(),
            detail='Checked for direct "click here to buy" CTA phrasing.',
        )
    )
    passed_count = sum(1 for check in checks if check.passed)
    report = GuidelineReport(
        score=passed_count,
        max_score=len(checks),
        checks=checks,
        summary=f"{passed_count}/{len(checks)} guideline checks passed.",
    )
    if len(article.medical_citations) < 1:
        raise RuntimeError("Generated article is missing required medical citations.")
    if len(article.product_knowledge_used) < 1:
        raise RuntimeError("Generated article did not use product knowledge.")
    if len(article.customer_language_used) < 1:
        raise RuntimeError("Generated article did not use customer language.")
    if report.score < 8:
        raise RuntimeError(f"Generated article failed guideline quality gate: {report.summary}")
    return report


def render_citations_section(citations: list[str]) -> str:
    lines = ["## Medical Citations", ""]
    lines.extend(f"- {citation}" for citation in citations)
    return "\n".join(lines)
