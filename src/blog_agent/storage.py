from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml

from .models import KeywordCluster, PipelineItem, TopicHistoryItem


def ensure_directories(paths: list[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def load_keyword_clusters(path: Path) -> list[KeywordCluster]:
    raw = yaml.safe_load(path.read_text()) or {}
    items = raw.get("clusters", [])
    return [KeywordCluster.model_validate(item) for item in items]


def load_history(path: Path) -> list[TopicHistoryItem]:
    if not path.exists():
        return []
    raw = yaml.safe_load(path.read_text()) or {}
    items = raw.get("history", [])
    return [TopicHistoryItem.model_validate(item) for item in items]


def append_history(path: Path, item: TopicHistoryItem) -> None:
    history = load_history(path)
    history.append(item)
    payload = {"history": [entry.model_dump(mode="json") for entry in history]}
    path.write_text(yaml.safe_dump(payload, sort_keys=False))


def load_pipeline(path: Path) -> list[PipelineItem]:
    if not path.exists():
        return []
    raw = yaml.safe_load(path.read_text()) or {}
    items = raw.get("pipeline", [])
    return [PipelineItem.model_validate(item) for item in items]


def save_pipeline(path: Path, items: list[PipelineItem]) -> None:
    payload = {"pipeline": [item.model_dump(mode="json") for item in items]}
    path.write_text(yaml.safe_dump(payload, sort_keys=False))


def build_frontmatter(title: str, description: str, excerpt: str, today: date) -> str:
    payload = {
        "title": title,
        "description": description,
        "excerpt": excerpt,
        "date": today.isoformat(),
    }
    lines = ["---"]
    lines.extend(yaml.safe_dump(payload, sort_keys=False).strip().splitlines())
    lines.append("---")
    return "\n".join(lines)


def parse_markdown_file(path: Path) -> tuple[dict, str]:
    raw = path.read_text()
    if not raw.startswith("---\n"):
        return {}, raw

    _, rest = raw.split("---\n", 1)
    frontmatter_raw, body = rest.split("\n---\n", 1)
    frontmatter = yaml.safe_load(frontmatter_raw) or {}
    return frontmatter, body.strip()


def load_source_library(path: Path) -> str:
    if not path.exists():
        return ""

    sections: list[str] = []
    for file_path in sorted(path.rglob("*.md")):
        if file_path.name == "README.md":
            continue
        relative = file_path.relative_to(path)
        sections.append(f"## Source: {relative}\n\n{file_path.read_text().strip()}")
    return "\n\n".join(section for section in sections if section.strip())


def load_required_markdown(path: Path, label: str) -> str:
    if not path.exists():
        raise RuntimeError(f"Missing required source file: {label} ({path})")

    content = path.read_text().strip()
    if len(content) < 60:
        raise RuntimeError(f"Required source file is too thin: {label} ({path})")
    return content
