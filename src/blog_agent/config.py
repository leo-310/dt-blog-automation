from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, Field


ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
CONTENT_DIR = ROOT_DIR / "content" / "posts"
PROMPTS_DIR = ROOT_DIR / "prompts"


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


load_dotenv(ROOT_DIR / ".env")


class AgentConfig(BaseModel):
    brand_name: str = "Doctor Towels"
    product_name: str = "Doctor Towels"
    website_url: str = "https://doctortowels.com"
    primary_cta_url: str = "https://doctortowels.com"
    model: str = Field(
        default_factory=lambda: os.getenv("BLOG_AGENT_MODEL", "gpt-5.4")
    )
    topic_planner_model: str = Field(
        default_factory=lambda: os.getenv("BLOG_AGENT_TOPIC_MODEL")
        or os.getenv("BLOG_AGENT_TOPIC_PLANNER_MODEL")
        or "gpt-5.4-mini"
    )
    article_writer_model: str = Field(
        default_factory=lambda: os.getenv("BLOG_AGENT_ARTICLE_MODEL")
        or os.getenv("BLOG_AGENT_MODEL", "gpt-5.4")
    )
    api_key: str = Field(
        default_factory=lambda: os.getenv("BLOG_AGENT_API_KEY")
        or os.getenv("OPENAI_API_KEY", "")
    )
    api_base_url: str = Field(
        default_factory=lambda: os.getenv(
            "BLOG_AGENT_API_BASE_URL", "https://api.openai.com/v1"
        )
    )
    api_mode: str = Field(
        default_factory=lambda: os.getenv("BLOG_AGENT_API_MODE", "auto")
    )
    temperature: float = 0.7
    max_output_tokens: int | None = Field(
        default_factory=lambda: _resolve_max_output_tokens(
            os.getenv("BLOG_AGENT_MAX_OUTPUT_TOKENS", "0")
        )
    )
    image_model: str = Field(
        default_factory=lambda: os.getenv("BLOG_AGENT_IMAGE_MODEL", "gpt-image-1.5")
    )
    image_prompt_model: str = Field(
        default_factory=lambda: os.getenv("BLOG_AGENT_IMAGE_PROMPT_MODEL", "gpt-5.4-mini")
    )
    image_quality: str = Field(
        default_factory=lambda: os.getenv("BLOG_AGENT_IMAGE_QUALITY", "low")
    )
    image_size: str = Field(
        default_factory=lambda: os.getenv("BLOG_AGENT_IMAGE_SIZE", "1536x1024")
    )
    image_format: str = Field(
        default_factory=lambda: os.getenv("BLOG_AGENT_IMAGE_FORMAT", "png")
    )
    author_name: str = "Doctor Towels Editorial Team"
    default_word_count: int = Field(
        default_factory=lambda: int(os.getenv("BLOG_AGENT_DEFAULT_WORD_COUNT", "2200"))
    )

    @property
    def topic_file(self) -> Path:
        return DATA_DIR / "keyword_clusters.yaml"

    @property
    def history_file(self) -> Path:
        return DATA_DIR / "topic_history.yaml"

    @property
    def pipeline_file(self) -> Path:
        return DATA_DIR / "pipeline.yaml"

    @property
    def brand_brief_file(self) -> Path:
        return DATA_DIR / "brand_brief.md"

    @property
    def approved_facts_file(self) -> Path:
        return DATA_DIR / "approved_facts.md"

    @property
    def style_guide_file(self) -> Path:
        return DATA_DIR / "style_guide.md"

    @property
    def sources_dir(self) -> Path:
        return DATA_DIR / "sources"

    @property
    def product_knowledge_file(self) -> Path:
        return self.sources_dir / "brand" / "product_knowledge.md"

    @property
    def medical_research_file(self) -> Path:
        return self.sources_dir / "research" / "derm_notes.md"

    @property
    def customer_language_file(self) -> Path:
        return self.sources_dir / "customer_language" / "reviews_and_forums.md"


def _resolve_max_output_tokens(raw: str) -> int | None:
    cleaned = raw.strip()
    if not cleaned:
        return None
    value = int(cleaned)
    if value <= 0:
        return None
    return value
