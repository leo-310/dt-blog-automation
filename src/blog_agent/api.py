from __future__ import annotations

import json
import os
import re
import base64
import mimetypes
from datetime import date, datetime, timedelta
from http import HTTPStatus
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from wsgiref.simple_server import make_server

import markdown

from .agent import BlogAgent
from .config import AgentConfig, CONTENT_DIR, ROOT_DIR
from .keyword_research import KeywordResearchRequest, KeywordResearchService
from .models import BlogPlan, KeywordCluster, PipelineItem
from .provider import BlogAgentProvider
from .shopify import ShopifyPublisher
from .storage import (
    ensure_directories,
    load_history,
    load_keyword_clusters,
    load_pipeline,
    parse_markdown_file,
    save_pipeline,
)
from .visibility import load_latest_visibility_report

GENERATED_IMAGE_DIR = CONTENT_DIR.parent / "images"
DIST_DIR = ROOT_DIR / "dist"


def main() -> None:
    host = os.getenv("BLOG_AGENT_API_HOST", "0.0.0.0")
    port = int(os.getenv("BLOG_AGENT_API_PORT", os.getenv("PORT", "8124")))
    app = BlogAgentApi()
    with make_server(host, port, app.wsgi_app) as server:
        print(f"Blog agent API running at http://{host}:{port}")
        server.serve_forever()


class BlogAgentApi:
    def __init__(self) -> None:
        self.config = AgentConfig()
        self.agent = BlogAgent(self.config)
        self.keyword_research = KeywordResearchService()
        self.shopify = ShopifyPublisher()
        ensure_directories([CONTENT_DIR, GENERATED_IMAGE_DIR])

    def wsgi_app(self, environ, start_response):
        method = environ.get("REQUEST_METHOD", "GET").upper()
        parsed = urlparse(environ.get("PATH_INFO", "/"))

        if method == "OPTIONS":
            start_response(
                "204 No Content",
                cors_headers([("Content-Length", "0")]),
            )
            return [b""]

        try:
            if method == "GET" and parsed.path == "/api/health":
                return self.respond(start_response, {"ok": True})
            if method == "GET" and parsed.path == "/api/posts":
                return self.respond(start_response, {"posts": self.load_posts()})
            if method == "GET" and parsed.path == "/api/pipeline":
                return self.respond(
                    start_response,
                    {"pipeline": self.load_pipeline_items()},
                )
            if method == "GET" and parsed.path == "/api/pillars":
                return self.respond(start_response, {"pillars": self.load_pillars()})
            if method == "GET" and parsed.path.startswith("/api/images/"):
                image_name = parsed.path.removeprefix("/api/images/")
                return self.serve_generated_image(start_response, image_name)
            if method == "GET" and parsed.path == "/api/shopify/blogs":
                if not self.shopify.enabled:
                    return self.respond(
                        start_response,
                        {
                            "error": "Shopify credentials are missing. Set SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET, and MYSHOPIFY_DOMAIN.",
                        },
                        status=HTTPStatus.BAD_REQUEST,
                    )
                blogs = self.shopify.list_blogs(limit=50)
                return self.respond(start_response, {"blogs": blogs})
            if method == "GET" and parsed.path == "/api/keyword-research":
                query_params = parse_qs(environ.get("QUERY_STRING", ""))
                seeds = normalize_seed_keywords(query_params.get("seed", []))
                if not seeds:
                    return self.respond(
                        start_response,
                        {"error": "Pass at least one `seed` query parameter."},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                research = self.keyword_research.research(
                    KeywordResearchRequest(
                        seed_keywords=seeds,
                        geo=query_params.get("geo", ["US"])[0],
                        timeframe=query_params.get("timeframe", ["today 3-m"])[0],
                    )
                )
                return self.respond(
                    start_response,
                    {"research": research.model_dump(mode="json")},
                )
            if method == "GET" and parsed.path == "/api/insights/hot-feed":
                query_params = parse_qs(environ.get("QUERY_STRING", ""))
                topic_targets = normalize_seed_keywords(query_params.get("topic", []))
                feed = self.load_hot_feed(topic_targets=topic_targets)
                return self.respond(start_response, {"feed": feed})
            if method == "POST" and parsed.path == "/api/generate":
                generated = self.agent.generate_post()
                created = self.load_post_by_name(Path(generated.output_path).name)
                self.upsert_pipeline_item(generated)
                return self.respond(
                    start_response,
                    {"post": created, "message": "Generated a new post."},
                    status=HTTPStatus.CREATED,
                )
            if method == "POST" and parsed.path == "/api/pipeline/generate":
                payload = self.read_json_body(environ)
                count = int(payload.get("count", payload.get("weeks", 4)))
                count = max(1, min(20, count))
                pillar_id = str(payload.get("pillarId", "")).strip() or None
                topic_role = str(payload.get("role", "side")).strip().lower()
                if topic_role not in {"main", "side"}:
                    topic_role = "side"
                created = self.generate_pipeline(
                    count=count,
                    pillar_id=pillar_id,
                    topic_role=topic_role,
                )
                return self.respond(
                    start_response,
                    {
                        "created": created,
                        "pipeline": self.load_pipeline_items(),
                        "message": f"Generated {len(created)} {topic_role} topic(s). Approve a topic to generate the full blog draft.",
                    },
                    status=HTTPStatus.CREATED,
                )
            if method == "POST" and parsed.path == "/api/images/generate":
                payload = self.read_json_body(environ)
                prompt = str(payload.get("prompt", "")).strip()
                pipeline_id = str(payload.get("pipelineId", "")).strip() or None
                result = self.generate_pipeline_image(prompt=prompt, pipeline_id=pipeline_id)
                return self.respond(start_response, result, status=HTTPStatus.CREATED)
            if method == "POST" and parsed.path.startswith("/api/pipeline/"):
                parts = [part for part in parsed.path.split("/") if part]
                if len(parts) != 4:
                    return self.respond(
                        start_response,
                        {"error": "Invalid pipeline action path."},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                _, _, pipeline_id, action = parts
                payload = self.read_json_body(environ)
                transition = self.transition_pipeline_item(pipeline_id, action, payload)
                return self.respond(start_response, transition)
            if method == "GET" and not parsed.path.startswith("/api/"):
                return self.serve_web_app(start_response, parsed.path)
        except Exception as exc:  # noqa: BLE001
            return self.respond(
                start_response,
                {"error": str(exc)},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        return self.respond(
            start_response,
            {"error": "Not found"},
            status=HTTPStatus.NOT_FOUND,
        )

    def load_posts(self) -> list[dict]:
        pipeline = load_pipeline(self.config.pipeline_file)
        status_lookup = {item.post_id: item for item in pipeline if item.post_id}
        history = load_history(self.config.history_file)
        history_lookup = {Path(item.output_path).name: item for item in history}
        posts: list[dict] = []

        for path in sorted(CONTENT_DIR.glob("*.md"), reverse=True):
            frontmatter, body = parse_markdown_file(path)
            history_item = history_lookup.get(path.name)
            posts.append(
                {
                    "id": path.name,
                    "title": str(frontmatter.get("title", path.stem)),
                    "description": str(frontmatter.get("description", "")),
                    "excerpt": str(frontmatter.get("excerpt", "")),
                    "date": str(frontmatter.get("date", "")),
                    "query": history_item.query if history_item else "Not logged",
                    "cluster": history_item.cluster if history_item else "Unknown",
                    "pillarName": status_lookup.get(path.name).pillar_name
                    if status_lookup.get(path.name)
                    else "",
                    "mainTopic": status_lookup.get(path.name).main_topic
                    if status_lookup.get(path.name)
                    else "",
                    "subBlogTag": status_lookup.get(path.name).sub_blog_tag
                    if status_lookup.get(path.name)
                    else "",
                    "path": str(path),
                    "pipelineStatus": status_lookup.get(path.name).status
                    if status_lookup.get(path.name)
                    else "draft",
                    "guidelineReport": status_lookup.get(path.name).guideline_report.model_dump(mode="json")
                    if status_lookup.get(path.name) and status_lookup.get(path.name).guideline_report
                    else None,
                    "html": render_post_html(body, self.config.website_url),
                }
            )

        return posts

    def load_post_by_name(self, name: str) -> dict | None:
        for post in self.load_posts():
            if post["id"] == name:
                return post
        return None

    def load_pipeline_items(self) -> list[dict]:
        posts_lookup = {post["id"]: post for post in self.load_posts()}
        pipeline_items = load_pipeline(self.config.pipeline_file)
        clusters = load_keyword_clusters(self.config.topic_file)
        if backfill_pipeline_pillar_context(pipeline_items, clusters):
            save_pipeline(self.config.pipeline_file, pipeline_items)
        items = sorted(
            pipeline_items,
            key=lambda item: (item.scheduled_for.isoformat(), item.created_at),
            reverse=True,
        )
        payload: list[dict] = []
        for item in items:
            post = posts_lookup.get(item.post_id)
            payload.append(
                {
                    **item.model_dump(mode="json"),
                    "html": post["html"] if post else "",
                    "excerpt": post["excerpt"] if post else "",
                    "description": post["description"] if post else "",
                    "generatedImageUrl": build_generated_image_url(item),
                    "generatedImageStyle": str(item.metadata.get("generated_image_style", "")),
                    "hasGeneratedDraft": bool(item.post_id),
                }
            )
        return payload

    def generate_pipeline_image(self, *, prompt: str, pipeline_id: str | None) -> dict:
        pipeline = load_pipeline(self.config.pipeline_file)
        item = next((entry for entry in pipeline if entry.id == pipeline_id), None) if pipeline_id else None
        image_prompt = prompt or build_image_prompt(item=item, provider=self.agent.provider, config=self.config)
        if not image_prompt:
            raise RuntimeError("Provide `prompt` or `pipelineId` to generate an image.")

        image_response = self.agent.provider.generate_image(
            prompt=image_prompt,
            model=self.config.image_model,
            quality=self.config.image_quality,
            size=self.config.image_size,
            output_format=self.config.image_format,
        )
        image_payload = image_response.get("data", [{}])[0]
        image_base64 = str(image_payload.get("b64_json", "")).strip()
        if not image_base64:
            raise RuntimeError("Image generation succeeded but no image payload was returned.")
        image_bytes = base64.b64decode(image_base64, validate=True)

        image_name = build_generated_image_name(
            prompt=image_prompt,
            output_format=self.config.image_format,
        )
        image_file = GENERATED_IMAGE_DIR / image_name
        image_file.write_bytes(image_bytes)

        if item:
            item.metadata["generated_image_file"] = image_name
            item.metadata["generated_image_prompt"] = image_prompt
            item.metadata["generated_image_style"] = "Custom Prompt" if prompt else "Topic-Aware Abstract"
            save_pipeline(self.config.pipeline_file, pipeline)

        return {
            "imageUrl": f"/api/images/{image_name}",
            "prompt": image_prompt,
            "model": self.config.image_model,
            "quality": self.config.image_quality,
            "size": self.config.image_size,
            "pipelineId": pipeline_id,
            "styleName": "Custom Prompt" if prompt else "Topic-Aware Abstract",
        }

    def load_pillars(self) -> list[dict]:
        clusters = load_keyword_clusters(self.config.topic_file)
        grouped: dict[str, dict] = {}
        for cluster in clusters:
            pillar_id = cluster.pillar_id or sanitize_slug(cluster.pillar_name or cluster.name)
            if pillar_id not in grouped:
                grouped[pillar_id] = {
                    "pillarId": pillar_id,
                    "pillarName": cluster.pillar_name or cluster.name,
                    "pillarClaim": cluster.pillar_claim,
                    "mainTopic": cluster.main_topic,
                    "clusters": [],
                }
            grouped[pillar_id]["clusters"].append(cluster.model_dump(mode="json"))
        return list(grouped.values())

    def load_hot_feed(self, topic_targets: list[str] | None = None) -> dict:
        pipeline = self.load_pipeline_items()
        posts = self.load_posts()
        topic_targets = topic_targets or []

        if not topic_targets:
            topic_targets = []
            for item in pipeline:
                query = str(item.get("query", "")).strip()
                if query and query.lower() not in {value.lower() for value in topic_targets}:
                    topic_targets.append(query)
            topic_targets = topic_targets[:8]

        if not topic_targets:
            topic_targets = [
                "acne-safe towels",
                "face towel for sensitive skin",
                "how often should you change towels",
                "best towel material for acne-prone skin",
            ]

        derived_seeds = topic_targets[:5]
        trend_cards: list[dict] = []
        notes: list[str] = []

        try:
            research = self.keyword_research.research(
                KeywordResearchRequest(seed_keywords=derived_seeds)
            )
            for index, idea in enumerate(research.ideas[:12]):
                trend_cards.append(
                    {
                        "id": f"google-{index}",
                        "channel": "search" if idea.kind in {"top", "rising"} else "insight",
                        "rank": index + 1,
                        "keyword": idea.keyword,
                        "delta": format_trend_delta(idea),
                        "kind": idea.kind,
                        "source": idea.source,
                    }
                )
            notes.extend(research.notes)
        except Exception as exc:  # noqa: BLE001
            notes.append(f"Google Trends request failed: {exc}")

        market_cards = build_market_cards(trend_cards, derived_seeds)
        latest_visibility = load_latest_visibility_report()
        if latest_visibility:
            chatgpt_visibility = build_chatgpt_visibility_from_report(
                report=latest_visibility, pipeline=pipeline
            )
            visibility_score = int(
                latest_visibility.get("aggregateVisibilityScore", 0)
            )
            visibility_stats = {
                "mentions": int(latest_visibility.get("mentions", 0)),
                "citedPages": int(latest_visibility.get("citedPages", 0)),
                "providerMentions": latest_visibility.get("providerMentions", {}),
            }
        else:
            chatgpt_visibility = build_chatgpt_visibility_cards(topic_targets, posts, pipeline)
            visibility_score = score_chatgpt_visibility(chatgpt_visibility)
            visibility_stats = {}

        return {
            "updatedAt": datetime.now().isoformat(timespec="seconds"),
            "sections": [
                {
                    "id": "google-keywords",
                    "title": "Google Keywords",
                    "description": "Rising and top terms from Google Trends relevance pools.",
                    "cards": trend_cards,
                },
                {
                    "id": "hot-topics",
                    "title": "Hot Topics",
                    "description": "Breakout angles blended from trend movement and editorial focus.",
                    "cards": market_cards,
                },
                {
                    "id": "chatgpt-visibility",
                    "title": "ChatGPT Visibility",
                    "description": "Do we currently rank for our fixed discovery prompts across AI search responses?",
                    "score": visibility_score,
                    "cards": chatgpt_visibility,
                    "stats": visibility_stats,
                },
            ],
            "notes": notes[:4],
        }

    def generate_pipeline(
        self,
        *,
        count: int,
        pillar_id: str | None = None,
        topic_role: str = "side",
    ) -> list[dict]:
        pipeline = load_pipeline(self.config.pipeline_file)
        clusters = load_keyword_clusters(self.config.topic_file)
        start = date.today()
        created: list[dict] = []
        blocked_queries = [item.query for item in pipeline]
        for offset in range(count):
            target_date = start + timedelta(days=offset)
            plan, cluster = self.agent.plan_topic(
                blocked_queries=blocked_queries,
                preferred_pillar_id=pillar_id,
            )
            blocked_queries.append(plan.target_query)
            topic_id = f"topic-{target_date.isoformat()}-{offset}-{datetime.now().strftime('%H%M%S')}"
            item = PipelineItem(
                id=topic_id,
                title=plan.title,
                query=plan.target_query,
                cluster=cluster.name,
                pillar_id=cluster.pillar_id,
                pillar_name=cluster.pillar_name,
                main_topic=cluster.main_topic,
                sub_blog_tag=cluster.sub_blog_tag,
                pillar_head_post_id=None,
                pillar_head_slug=None,
                planned_keywords=plan.keywords_to_use or cluster.supporting_keywords[:8],
                scheduled_for=target_date,
                status="topic",
                topic_role="main" if topic_role == "main" else "side",
                created_at=datetime.now().isoformat(timespec="seconds"),
                topic_angle=plan.angle,
                topic_outline=plan.outline,
                topic_internal_links=plan.internal_links,
                metadata={
                    "slug": plan.slug,
                    "meta_description": plan.meta_description,
                },
            )
            enrich_item_pillar_context(item=item, pillar_id=pillar_id or "", clusters=clusters)
            pipeline.append(item)
            created.append(item.model_dump(mode="json"))
        save_pipeline(self.config.pipeline_file, pipeline)
        return created

    def upsert_pipeline_item(self, generated) -> PipelineItem:
        pipeline = load_pipeline(self.config.pipeline_file)
        post_id = Path(generated.output_path).name
        existing = next((item for item in pipeline if item.post_id == post_id), None)
        now = datetime.now().isoformat(timespec="seconds")
        if existing:
            existing.title = generated.title
            existing.query = generated.query
            existing.cluster = generated.cluster
            existing.pillar_id = generated.pillar_id
            existing.pillar_name = generated.pillar_name
            existing.main_topic = generated.main_topic
            existing.sub_blog_tag = generated.sub_blog_tag
            existing.path = generated.output_path
            existing.scheduled_for = generated.date
            existing.guideline_report = generated.guideline_report
            existing.status = "draft"
            existing.approved_at = None
            existing.pushed_at = None
        else:
            pipeline.append(
                PipelineItem(
                    id=f"{generated.date.isoformat()}-{generated.slug}",
                    post_id=post_id,
                    title=generated.title,
                    query=generated.query,
                    cluster=generated.cluster,
                    pillar_id=generated.pillar_id,
                    pillar_name=generated.pillar_name,
                    main_topic=generated.main_topic,
                    sub_blog_tag=generated.sub_blog_tag,
                    path=generated.output_path,
                    scheduled_for=generated.date,
                    status="draft",
                    created_at=now,
                    guideline_report=generated.guideline_report,
                )
            )
        save_pipeline(self.config.pipeline_file, pipeline)
        return next(item for item in pipeline if item.post_id == post_id)

    def transition_pipeline_item(self, pipeline_id: str, action: str, payload: dict | None = None) -> dict:
        pipeline = load_pipeline(self.config.pipeline_file)
        item = next((entry for entry in pipeline if entry.id == pipeline_id), None)
        if not item:
            raise RuntimeError("Pipeline item not found.")
        payload = payload or {}
        if action in {"approve", "push"}:
            enrich_item_pillar_context(
                item=item,
                pillar_id=str(payload.get("pillarId", "")).strip(),
                clusters=load_keyword_clusters(self.config.topic_file),
            )
        now = datetime.now().isoformat(timespec="seconds")
        if action == "approve":
            if item.status == "topic":
                ensure_sub_blog_has_main_blog_link(item=item, pipeline=pipeline)
                plan = BlogPlan(
                    title=item.title,
                    slug=str(item.metadata.get("slug", item.title.lower().replace(" ", "-"))),
                    target_query=item.query,
                    meta_description=str(item.metadata.get("meta_description", ""))[:160],
                    angle=item.topic_angle or "Educational explainer",
                    outline=item.topic_outline or ["Introduction", "Key points", "Conclusion"],
                    internal_links=item.topic_internal_links,
                    keywords_to_use=item.planned_keywords,
                )
                generated = self.agent.generate_post_from_plan(
                    plan=plan,
                    cluster=resolve_cluster_from_item(item),
                    today=item.scheduled_for,
                )
                item.post_id = Path(generated.output_path).name
                item.path = generated.output_path
                item.guideline_report = generated.guideline_report
                item.status = "approved"
                item.approved_at = now
                if item.is_pillar_head:
                    item.pillar_head_post_id = item.post_id
                ensure_sub_blog_backlink_in_markdown(item=item, pipeline=pipeline)
            else:
                item.status = "approved"
                item.approved_at = now
                ensure_sub_blog_has_main_blog_link(item=item, pipeline=pipeline)
                ensure_sub_blog_backlink_in_markdown(item=item, pipeline=pipeline)
        elif action == "reject":
            item.status = "rejected"
            item.approved_at = None
            item.pushed_at = None
            item.shopify_article_id = None
            item.shopify_blog_id = None
            item.shopify_article_handle = None
        elif action == "push":
            if item.status != "approved":
                raise RuntimeError("Only approved drafts can be pushed.")
            if not item.post_id:
                raise RuntimeError("Approve a topic first to generate its full blog content.")
            blog_id = str(payload.get("blogId", "")).strip()
            if not blog_id:
                raise RuntimeError("Missing blogId in request payload for push action.")
            if not self.shopify.enabled:
                raise RuntimeError(
                    "Shopify is not configured. Set SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET, and MYSHOPIFY_DOMAIN."
                )
            ensure_sub_blog_has_main_blog_link(item=item, pipeline=pipeline)
            ensure_sub_blog_backlink_in_markdown(item=item, pipeline=pipeline)
            post = self.load_post_by_name(item.post_id)
            if not post:
                raise RuntimeError("Cannot push: post content file is missing.")
            publish_now = bool(payload.get("publishNow", True))
            publish_date = str(payload.get("publishDate", "")).strip() or None
            article = self.shopify.create_article(
                blog_id=blog_id,
                title=post["title"],
                author_name=self.config.author_name,
                body_html=post["html"],
                summary_html=post.get("excerpt") or post.get("description") or None,
                tags=build_shopify_tags(item),
                is_published=publish_now,
                publish_date=publish_date,
            )
            item.status = "pushed"
            item.pushed_at = now
            item.shopify_article_id = article.get("id")
            item.shopify_blog_id = blog_id
            item.shopify_article_handle = article.get("handle")
            save_pipeline(self.config.pipeline_file, pipeline)
            return {
                "item": item.model_dump(mode="json"),
                "shopifyArticle": article,
            }
        else:
            raise RuntimeError("Unsupported pipeline action.")
        save_pipeline(self.config.pipeline_file, pipeline)
        return {"item": item.model_dump(mode="json")}

    @staticmethod
    def read_json_body(environ) -> dict:
        content_length = int(environ.get("CONTENT_LENGTH", "0") or "0")
        if content_length <= 0:
            return {}
        body = environ["wsgi.input"].read(content_length).decode("utf-8")
        if not body.strip():
            return {}
        return json.loads(body)

    @staticmethod
    def respond(start_response, payload: dict, status: HTTPStatus = HTTPStatus.OK):
        body = json.dumps(payload).encode("utf-8")
        start_response(
            f"{status.value} {status.phrase}",
            cors_headers(
                [
                    ("Content-Type", "application/json; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ]
            ),
        )
        return [body]

    @staticmethod
    def respond_binary(
        start_response,
        payload: bytes,
        *,
        content_type: str,
        status: HTTPStatus = HTTPStatus.OK,
    ):
        start_response(
            f"{status.value} {status.phrase}",
            cors_headers(
                [
                    ("Content-Type", content_type),
                    ("Content-Length", str(len(payload))),
                    ("Cache-Control", "public, max-age=86400"),
                ]
            ),
        )
        return [payload]

    def serve_generated_image(self, start_response, image_name: str):
        safe_name = Path(image_name).name
        if (
            not safe_name
            or safe_name != image_name
            or not re.fullmatch(r"[a-zA-Z0-9_.-]+", safe_name)
        ):
            return self.respond(
                start_response,
                {"error": "Invalid image name."},
                status=HTTPStatus.BAD_REQUEST,
            )
        image_path = GENERATED_IMAGE_DIR / safe_name
        if not image_path.exists():
            return self.respond(
                start_response,
                {"error": "Image not found."},
                status=HTTPStatus.NOT_FOUND,
            )
        guessed_type, _ = mimetypes.guess_type(image_path.name)
        content_type = guessed_type or "application/octet-stream"
        return self.respond_binary(
            start_response,
            image_path.read_bytes(),
            content_type=content_type,
        )

    def serve_web_app(self, start_response, request_path: str):
        if not DIST_DIR.exists():
            return self.respond(
                start_response,
                {"error": "UI build not found. Run `npm run build` before starting the API."},
                status=HTTPStatus.NOT_FOUND,
            )
        normalized = request_path.lstrip("/")
        requested_file = DIST_DIR / normalized if normalized else DIST_DIR / "index.html"
        if requested_file.exists() and requested_file.is_file():
            return self._serve_static_file(start_response, requested_file)
        return self._serve_static_file(start_response, DIST_DIR / "index.html")

    def _serve_static_file(self, start_response, file_path: Path):
        if not file_path.exists():
            return self.respond(
                start_response,
                {"error": "Requested file not found."},
                status=HTTPStatus.NOT_FOUND,
            )
        payload = file_path.read_bytes()
        guessed_type, _ = mimetypes.guess_type(file_path.name)
        content_type = guessed_type or "application/octet-stream"
        cache = "public, max-age=86400"
        if file_path.name == "index.html":
            cache = "no-store"
        start_response(
            f"{HTTPStatus.OK.value} {HTTPStatus.OK.phrase}",
            cors_headers(
                [
                    ("Content-Type", content_type),
                    ("Content-Length", str(len(payload))),
                    ("Cache-Control", cache),
                ]
            ),
        )
        return [payload]


def cors_headers(extra_headers: list[tuple[str, str]]) -> list[tuple[str, str]]:
    return [
        ("Access-Control-Allow-Origin", "*"),
        ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
        ("Access-Control-Allow-Headers", "Content-Type"),
        *extra_headers,
    ]


def build_generated_image_name(*, prompt: str, output_format: str) -> str:
    suffix = sanitize_slug(prompt)[:60] or "blog-cover"
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    normalized_format = output_format.strip().lower() or "png"
    ext = {"jpg": "jpeg"}.get(normalized_format, normalized_format)
    if ext not in {"png", "jpeg", "webp"}:
        ext = "png"
    return f"{timestamp}-{suffix}.{ext}"


IMAGE_PROMPT_WRITER_SYSTEM = """You are an art director for a premium D2C skin-health textile brand called Doctor Towels. You write image generation prompts in a specific visual style. Every prompt you produce must follow these rules:

BACKGROUND: A seamless dark gradient backdrop. Colors are always deep and saturated - choose from: dark navy/indigo blue, dark burnt orange/amber, dark forest/emerald green, dark crimson/burgundy red. The gradient shifts between two tones within the same color family (e.g. deep burnt orange to dark rust). Describe the gradient with a poetic simile (e.g. "like molten metal cooling", "like light through dense canopy").

SUBJECT: A thin white or cream line illustration - hairline-thin, single-weight linework, slightly imperfect like hand-drawn with a technical pen. Pure outline, no fill, no shading unless stipple dots are used sparingly. The subject should be abstract or diagrammatic - not literal product shots. Think: scientific cross-sections, molecular structures, anatomical blueprints, textile weave patterns, topographic maps, constellation-style scatter diagrams. The illustration should visually map to one of these brand themes: skin science, bacteria/microbiome, textile/fiber, wellness rituals, skin barrier, cellular biology.

COMPOSITION: Ultra-minimal. Generous negative space. Either centered or intentionally asymmetric (weighted to one side with heavy negative space opposite). Never cluttered.

ANNOTATION ELEMENTS (optional): Faint dashed annotation lines, dotted connection lines, tiny open circles at endpoints. No text, no labels - just the gesture of a diagram.

STYLE TAGS: Always end with "Editorial [domain] meets [domain], minimalist poster design/composition. Aspect Ratio 16:9" - where domains reference the two worlds colliding (e.g. "Editorial microbiology meets celestial cartography").

COLOR CODING SYSTEM (loose guideline):
- Dark blue -> molecular / chemistry / science
- Dark orange -> textile / material / fiber
- Dark green -> biology / growth / microbiome
- Dark red -> skin / anatomy / body

When given a blog title, article topic, or brand theme - produce one prompt following all the above rules. The illustration subject should be a clever visual metaphor for the topic, never a literal depiction of the product.

Return exactly one prompt as plain text. No markdown. No bullets. No surrounding quotes."""


def build_image_prompt(*, item: PipelineItem | None, provider: BlogAgentProvider, config: AgentConfig) -> str:
    title = (item.title if item else "").strip()
    topic = (item.query if item else "").strip()
    cluster = (item.cluster if item else "").strip()
    pillar = (item.pillar_name if item else "").strip()
    main_topic = (item.main_topic if item else "").strip()
    user_prompt = (
        "Create one image-generation prompt for this article context.\n"
        f"Blog title: {title or 'N/A'}\n"
        f"Article topic/query: {topic or 'N/A'}\n"
        f"Cluster/theme: {cluster or 'N/A'}\n"
        f"Pillar: {pillar or 'N/A'}\n"
        f"Main topic: {main_topic or 'N/A'}"
    )
    generated = provider.complete(
        IMAGE_PROMPT_WRITER_SYSTEM,
        user_prompt,
        model=config.image_prompt_model,
        max_output_tokens=500,
    ).strip()
    return generated.strip('"').strip("'")


def build_generated_image_url(item: PipelineItem) -> str:
    image_name = str(item.metadata.get("generated_image_file", "")).strip()
    if not image_name:
        return ""
    return f"/api/images/{image_name}"


def normalize_seed_keywords(values: list[str]) -> list[str]:
    parsed: list[str] = []
    for value in values:
        for part in value.split(","):
            cleaned = part.strip()
            if cleaned:
                parsed.append(cleaned)
    deduped: list[str] = []
    seen: set[str] = set()
    for value in parsed:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped[:5]


def ensure_pillar_head_topics(
    *,
    pipeline: list[PipelineItem],
    clusters: list[KeywordCluster],
    start_date: date,
) -> list[PipelineItem]:
    created: list[PipelineItem] = []
    blueprints = build_pillar_blueprints(clusters)
    existing_pillars = {item.pillar_id for item in pipeline if item.is_pillar_head and item.pillar_id}

    for index, blueprint in enumerate(blueprints):
        if blueprint["pillar_id"] in existing_pillars:
            continue
        slug = sanitize_slug(f"{blueprint['pillar_id']}-pillar-head-{blueprint['main_topic']}")
        query = blueprint["keywords"][0] if blueprint["keywords"] else blueprint["main_topic"]
        item = PipelineItem(
            id=f"pillar-head-{blueprint['pillar_id']}",
            title=f"{blueprint['main_topic']}: The Definitive Guide",
            query=query,
            cluster=blueprint["cluster_name"],
            pillar_id=blueprint["pillar_id"],
            pillar_name=blueprint["pillar_name"],
            main_topic=blueprint["main_topic"],
            sub_blog_tag="pillar-head",
            is_pillar_head=True,
            pillar_head_slug=slug,
            planned_keywords=blueprint["keywords"][:12],
            scheduled_for=start_date + timedelta(days=index),
            status="topic",
            created_at=datetime.now().isoformat(timespec="seconds"),
            topic_angle=(
                "Flagship pillar-head post that establishes authority, summarizes the pillar claim, "
                "and becomes the primary internal reference for all related sub-blogs."
            ),
            topic_outline=[
                "H2: The Core Claim and Why It Matters",
                "H2: The Science and Mechanisms",
                "H2: Real-World Scenarios and Mistakes",
                "H2: Practical Prevention Playbook",
                "H2: How This Pillar Connects to Daily Habits",
            ],
            topic_internal_links=["/products/doctor-towels"],
            metadata={
                "slug": slug,
                "meta_description": (
                    f"A comprehensive guide to {blueprint['main_topic'].lower()} and the daily habits that support healthier skin."
                )[:160],
            },
        )
        pipeline.append(item)
        created.append(item)
    return created


def build_pillar_blueprints(clusters: list[KeywordCluster]) -> list[dict]:
    by_pillar: dict[str, dict] = {}
    for cluster in clusters:
        pillar_id = cluster.pillar_id.strip() or sanitize_slug(cluster.pillar_name or cluster.main_topic)
        if not pillar_id:
            continue
        row = by_pillar.setdefault(
            pillar_id,
            {
                "pillar_id": pillar_id,
                "pillar_name": cluster.pillar_name or cluster.main_topic,
                "main_topic": cluster.main_topic or cluster.pillar_name or cluster.name,
                "cluster_name": cluster.name,
                "keywords": [],
            },
        )
        for keyword in cluster.supporting_keywords + cluster.queries:
            if keyword and keyword not in row["keywords"]:
                row["keywords"].append(keyword)
    return sorted(by_pillar.values(), key=lambda row: row["pillar_id"])


def sanitize_slug(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
    return re.sub(r"-{2,}", "-", cleaned)


def resolve_cluster_from_item(item: PipelineItem) -> KeywordCluster:
    return KeywordCluster(
        name=item.cluster,
        intent="informational",
        queries=[item.query],
        notes="Resolved from pipeline item context.",
        pillar_id=item.pillar_id,
        pillar_name=item.pillar_name,
        main_topic=item.main_topic,
        sub_blog_tag=item.sub_blog_tag,
        supporting_keywords=item.planned_keywords,
    )


def find_best_cluster_for_item(item: PipelineItem, clusters: list[KeywordCluster]) -> KeywordCluster | None:
    if not clusters:
        return None

    normalized_query = (item.query or "").strip().lower()
    normalized_cluster = (item.cluster or "").strip().lower()
    normalized_main_topic = (item.main_topic or "").strip().lower()
    normalized_pillar_name = (item.pillar_name or "").strip().lower()

    def tokenize(value: str) -> set[str]:
        return {
            token
            for token in re.findall(r"[a-z0-9]+", value.lower())
            if len(token) > 2 and token not in {"and", "for", "with", "the", "you", "your", "how", "why"}
        }

    item_tokens = tokenize(" ".join(filter(None, [item.query or "", item.title or "", item.cluster or ""])))
    best: tuple[int, KeywordCluster] | None = None
    for cluster in clusters:
        score = 0
        if item.pillar_id and cluster.pillar_id == item.pillar_id:
            score += 16
        if normalized_cluster and (cluster.name or "").strip().lower() == normalized_cluster:
            score += 12
        if normalized_query:
            query_candidates = {
                value.strip().lower()
                for value in [*cluster.queries, *cluster.supporting_keywords]
                if value and value.strip()
            }
            if normalized_query in query_candidates:
                score += 10
        if normalized_main_topic and (cluster.main_topic or "").strip().lower() == normalized_main_topic:
            score += 6
        if normalized_pillar_name and (cluster.pillar_name or "").strip().lower() == normalized_pillar_name:
            score += 6
        if item_tokens:
            cluster_tokens = tokenize(
                " ".join(
                    filter(
                        None,
                        [
                            cluster.name,
                            cluster.main_topic,
                            cluster.pillar_name,
                            *cluster.queries,
                            *cluster.supporting_keywords,
                        ],
                    )
                )
            )
            if cluster_tokens:
                shared = len(item_tokens & cluster_tokens)
                score += min(shared, 8)
        if score <= 0:
            continue
        if best is None or score > best[0]:
            best = (score, cluster)
    return best[1] if best else None


def backfill_pipeline_pillar_context(items: list[PipelineItem], clusters: list[KeywordCluster]) -> bool:
    changed = False
    for item in items:
        if item.pillar_id and item.pillar_name and item.main_topic and item.sub_blog_tag:
            continue
        match = find_best_cluster_for_item(item, clusters)
        if not match:
            continue
        before = (
            item.pillar_id,
            item.pillar_name,
            item.main_topic,
            item.pillar_claim,
            item.sub_blog_tag,
        )
        enrich_item_pillar_context(item=item, pillar_id=match.pillar_id, clusters=clusters)
        if (
            item.pillar_id,
            item.pillar_name,
            item.main_topic,
            item.pillar_claim,
            item.sub_blog_tag,
        ) != before:
            changed = True
    return changed


def enrich_item_pillar_context(item: PipelineItem, pillar_id: str, clusters: list[KeywordCluster]) -> None:
    if not pillar_id:
        return

    matching_cluster = next((cluster for cluster in clusters if cluster.pillar_id == pillar_id), None)
    if not item.pillar_id:
        item.pillar_id = pillar_id
    if matching_cluster:
        if not item.pillar_name:
            item.pillar_name = matching_cluster.pillar_name or matching_cluster.name
        if not item.main_topic:
            item.main_topic = matching_cluster.main_topic or matching_cluster.pillar_name or matching_cluster.name
        if not item.pillar_claim:
            item.pillar_claim = matching_cluster.pillar_claim
    if not item.sub_blog_tag:
        item.sub_blog_tag = sanitize_slug(item.query or item.title)


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


def blog_slug_from_post_id(post_id: str | None) -> str:
    if not post_id:
        return ""
    stem = Path(post_id).stem
    match = re.match(r"^\d{4}-\d{2}-\d{2}-(.+)$", stem)
    return match.group(1) if match else stem


def build_main_blog_url(item: PipelineItem) -> str:
    handle = (item.shopify_article_handle or "").strip()
    if handle:
        return f"/blogs/{handle}"
    slug = blog_slug_from_post_id(item.post_id)
    if slug:
        return f"/blogs/{slug}"
    metadata_slug = str(item.metadata.get("slug", "")).strip()
    if metadata_slug:
        return f"/blogs/{sanitize_slug(metadata_slug)}"
    if item.pillar_head_slug:
        return f"/blogs/{sanitize_slug(item.pillar_head_slug)}"
    if item.main_topic:
        return f"/blogs/{sanitize_slug(item.main_topic)}"
    return ""


def resolve_connected_main_blog_url(item: PipelineItem, pipeline: list[PipelineItem]) -> str:
    if item.topic_role != "side":
        return ""

    candidates = [
        entry
        for entry in pipeline
        if entry.id != item.id
        and entry.topic_role == "main"
        and entry.status in {"approved", "pushed"}
        and (
            (item.pillar_id and entry.pillar_id == item.pillar_id)
            or (item.main_topic and entry.main_topic and entry.main_topic == item.main_topic)
        )
    ]
    status_rank = {"pushed": 2, "approved": 1}
    candidates.sort(
        key=lambda entry: (
            status_rank.get(entry.status, 0),
            int(bool(entry.shopify_article_handle)),
            int(bool(entry.post_id)),
            entry.created_at,
        ),
        reverse=True,
    )
    if candidates:
        return build_main_blog_url(candidates[0])

    return build_main_blog_url(item)


def ensure_sub_blog_has_main_blog_link(item: PipelineItem, pipeline: list[PipelineItem]) -> None:
    if item.topic_role != "side":
        return
    main_blog_url = resolve_connected_main_blog_url(item, pipeline)
    if not main_blog_url:
        return
    item.topic_internal_links = normalize_internal_links([*item.topic_internal_links, main_blog_url])


def inject_backlink_into_markdown(body: str, main_blog_url: str, anchor_text: str) -> str:
    if not main_blog_url:
        return body

    link_pattern = re.compile(rf"\]\(\s*{re.escape(main_blog_url)}\s*\)", re.IGNORECASE)
    if link_pattern.search(body):
        return body

    safe_anchor = anchor_text.strip() or "the main pillar guide"
    backlink_line = f"For a full foundation on this pillar, read [{safe_anchor}]({main_blog_url})."
    marker = re.search(r"(?mi)^##\s+Medical Sources & Further Reading\s*$", body)
    if marker:
        before = body[: marker.start()].rstrip()
        after = body[marker.start() :].lstrip("\n")
        return f"{before}\n\n{backlink_line}\n\n{after}"
    return f"{body.rstrip()}\n\n{backlink_line}\n"


def ensure_sub_blog_backlink_in_markdown(item: PipelineItem, pipeline: list[PipelineItem]) -> None:
    if item.topic_role != "side" or not item.path:
        return
    file_path = Path(item.path)
    if not file_path.exists():
        return

    main_blog_url = resolve_connected_main_blog_url(item, pipeline)
    if not main_blog_url:
        return

    raw = file_path.read_text()
    split_token = "\n---\n"
    if raw.startswith("---\n") and split_token in raw:
        _head, rest = raw.split("---\n", 1)
        frontmatter_raw, body = rest.split(split_token, 1)
        updated_body = inject_backlink_into_markdown(body.strip(), main_blog_url, item.main_topic)
        new_raw = f"---\n{frontmatter_raw}{split_token}{updated_body.strip()}\n"
    else:
        updated_body = inject_backlink_into_markdown(raw.strip(), main_blog_url, item.main_topic)
        new_raw = f"{updated_body.strip()}\n"

    if new_raw != raw:
        file_path.write_text(new_raw)


def build_shopify_tags(item: PipelineItem) -> list[str]:
    tags = ["doctor-towels", "blog-agent", item.cluster.lower().replace(" ", "-")]
    if item.pillar_name:
        tags.append(item.pillar_name.lower().replace(" ", "-"))
    if item.sub_blog_tag:
        tags.append(item.sub_blog_tag.lower().replace(" ", "-"))
    return tags


def format_trend_delta(idea) -> str:
    if idea.trend:
        trend = str(idea.trend).strip()
        if trend:
            return trend
    if idea.score is None:
        return "N/A"
    return f"{idea.score}%"


def build_market_cards(trend_cards: list[dict], seeds: list[str]) -> list[dict]:
    cards: list[dict] = []
    for index, card in enumerate(trend_cards[:8]):
        keyword = str(card.get("keyword", "")).strip()
        if not keyword:
            continue
        cards.append(
            {
                "id": f"market-{index}",
                "channel": "market",
                "rank": index + 1,
                "keyword": keyword,
                "delta": card.get("delta", "N/A"),
                "kind": "breakout" if card.get("kind") == "rising" else "steady",
                "source": "google+editorial",
            }
        )

    if not cards:
        cards = [
            {
                "id": f"market-seed-{index}",
                "channel": "market",
                "rank": index + 1,
                "keyword": seed,
                "delta": "Watch",
                "kind": "seed",
                "source": "editorial",
            }
            for index, seed in enumerate(seeds[:8])
        ]
    return cards


def build_chatgpt_visibility_cards(topics: list[str], posts: list[dict], pipeline: list[dict]) -> list[dict]:
    cards: list[dict] = []
    for index, topic in enumerate(topics[:10]):
        ranked_posts = rank_posts_for_topic(topic, posts)
        ranked = len(ranked_posts) > 0
        status_rank = index + 1 if ranked else None
        coverage = [
            {
                "title": post["title"],
                "id": post["id"],
                "score": round(score, 2),
            }
            for post, score in ranked_posts[:3]
        ]
        related_pipeline = [
            item
            for item in pipeline
            if lexical_overlap_score(topic, item.get("query", "")) >= 0.45
        ][:2]
        cards.append(
            {
                "id": f"visibility-{index}",
                "channel": "chatgpt-search",
                "rank": status_rank,
                "keyword": topic,
                "delta": "Ranked" if ranked else "Not ranked",
                "kind": "ranked" if ranked else "missing",
                "source": "content-audit",
                "visible": ranked,
                "coverage": coverage,
                "pipelineMatches": [
                    {
                        "title": item.get("title", ""),
                        "status": item.get("status", ""),
                        "query": item.get("query", ""),
                    }
                    for item in related_pipeline
                ],
            }
        )
    return cards


def score_chatgpt_visibility(cards: list[dict]) -> int:
    if not cards:
        return 0
    ranked_count = len([card for card in cards if card.get("visible")])
    base = int(round((ranked_count / len(cards)) * 100))
    bonus = min(15, sum(2 for card in cards if card.get("visible") and card.get("rank") == 1))
    return max(0, min(100, base + bonus))


def build_chatgpt_visibility_from_report(report: dict, pipeline: list[dict]) -> list[dict]:
    provider_lookup = {
        str(item.get("provider", "")).lower(): item
        for item in report.get("providers", [])
    }
    provider = (
        provider_lookup.get("openai")
        or provider_lookup.get("chatgpt")
        or next(iter(provider_lookup.values()), {})
    )
    results = provider.get("results", [])
    result_by_prompt_id = {
        str(item.get("promptId", "")).strip(): item for item in results if item.get("promptId")
    }
    prompts = report.get("prompts", [])
    cards: list[dict] = []
    for index, prompt_row in enumerate(prompts):
        prompt_id = str(prompt_row.get("id", f"prompt-{index + 1}")).strip()
        result = result_by_prompt_id.get(prompt_id, {})
        prompt = str(result.get("prompt", "") or prompt_row.get("prompt", "")).strip()
        if not prompt:
            continue
        ranked = bool(result.get("foundDomain", False))
        linked_domains = [domain for domain in result.get("linkedDomains", []) if domain][:3]
        related_pipeline = [
            item
            for item in pipeline
            if lexical_overlap_score(prompt, item.get("query", "")) >= 0.45
        ][:3]
        cards.append(
            {
                "id": prompt_id,
                "channel": "chatgpt-search",
                "rank": index + 1,
                "keyword": prompt,
                "delta": "Ranked" if ranked else "Not ranked",
                "kind": "ranked" if ranked else "missing",
                "source": "llm-visibility",
                "visible": ranked,
                "coverage": [
                    {
                        "title": title,
                        "id": f"title-{index}-{pos}",
                        "score": result.get("score", 0),
                    }
                    for pos, title in enumerate(result.get("foundTitles", [])[:4], start=1)
                ],
                "linkedDomains": linked_domains,
                "pipelineMatches": [
                    {
                        "title": item.get("title", ""),
                        "status": item.get("status", ""),
                        "query": item.get("query", ""),
                    }
                    for item in related_pipeline
                ],
            }
        )
    if cards:
        return cards

    for index, result in enumerate(results):
        prompt = str(result.get("prompt", "")).strip()
        if not prompt:
            continue
        ranked = bool(result.get("foundDomain"))
        linked_domains = [domain for domain in result.get("linkedDomains", []) if domain][:3]
        related_pipeline = [
            item
            for item in pipeline
            if lexical_overlap_score(prompt, item.get("query", "")) >= 0.45
        ][:3]
        cards.append(
            {
                "id": str(result.get("promptId", f"visibility-{index}")),
                "channel": "chatgpt-search",
                "rank": index + 1,
                "keyword": prompt,
                "delta": "Ranked" if ranked else "Not ranked",
                "kind": "ranked" if ranked else "missing",
                "source": "llm-visibility",
                "visible": ranked,
                "coverage": [
                    {"title": title, "id": f"title-{index}-{pos}", "score": result.get("score", 0)}
                    for pos, title in enumerate(result.get("foundTitles", [])[:4], start=1)
                ],
                "linkedDomains": linked_domains,
                "pipelineMatches": [
                    {
                        "title": item.get("title", ""),
                        "status": item.get("status", ""),
                        "query": item.get("query", ""),
                    }
                    for item in related_pipeline
                ],
            }
        )
    return cards


def render_post_html(body_markdown: str, website_url: str) -> str:
    html = markdown.markdown(
        body_markdown,
        extensions=["extra", "sane_lists", "smarty"],
    )
    return absolutize_blog_links(html, website_url)


def absolutize_blog_links(html: str, website_url: str) -> str:
    origin = normalize_origin(website_url)
    if not origin:
        return html
    return re.sub(
        r'href=(["\'])(/blogs/[^"\']*)\1',
        lambda match: f'href={match.group(1)}{origin}{match.group(2)}{match.group(1)}',
        html,
        flags=re.IGNORECASE,
    )


def normalize_origin(website_url: str) -> str:
    value = str(website_url or "").strip()
    if not value:
        return ""
    if "://" not in value:
        value = f"https://{value}"
    parsed = urlparse(value)
    if not parsed.netloc:
        return ""
    scheme = parsed.scheme or "https"
    return f"{scheme}://{parsed.netloc}"


def rank_posts_for_topic(topic: str, posts: list[dict]) -> list[tuple[dict, float]]:
    scored: list[tuple[dict, float]] = []
    for post in posts:
        combined = " ".join(
            [
                str(post.get("title", "")),
                str(post.get("description", "")),
                str(post.get("excerpt", "")),
                str(post.get("query", "")),
            ]
        )
        score = lexical_overlap_score(topic, combined)
        if score >= 0.42:
            scored.append((post, score))
    scored.sort(key=lambda item: item[1], reverse=True)
    return scored


def lexical_overlap_score(left: str, right: str) -> float:
    left_tokens = tokenize_phrase(left)
    right_tokens = tokenize_phrase(right)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens.intersection(right_tokens))
    return overlap / max(1, len(left_tokens))


def tokenize_phrase(value: str) -> set[str]:
    stop_words = {
        "the",
        "a",
        "an",
        "for",
        "to",
        "of",
        "and",
        "or",
        "with",
        "is",
        "in",
        "on",
        "you",
        "your",
        "best",
        "how",
        "what",
    }
    tokens = {
        token
        for token in re.findall(r"[a-z0-9]+", value.lower())
        if len(token) > 2 and token not in stop_words
    }
    return tokens
