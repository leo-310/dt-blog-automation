from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
import yaml

from .config import DATA_DIR
from .storage import load_pipeline, parse_markdown_file


PROMPT_FILE = DATA_DIR / "visibility_prompts.yaml"
GENERIC_WEIGHT = 1.0
BRANDED_WEIGHT = 0.35


@dataclass
class PromptTarget:
    id: str
    prompt: str
    kind: str
    weight: float


@dataclass
class PromptResult:
    prompt_id: str
    prompt: str
    kind: str
    weight: float
    found_domain: bool
    found_titles: list[str]
    citations: list[str]
    linked_domains: list[str]
    our_domain_rank: int | None
    status: str
    score: int
    response_text: str


@dataclass
class ProviderRun:
    provider: str
    model: str
    enabled: bool
    error: str | None
    results: list[PromptResult]
    score: int


def run_visibility_scan(
    topics: list[str],
    domain: str,
    max_topics: int = 8,
    prompt_file: Path | None = None,
    providers: list[str] | None = None,
) -> dict:
    prompt_targets = load_prompt_targets(
        topics=topics,
        max_topics=max_topics,
        prompt_file=prompt_file or PROMPT_FILE,
    )
    if not prompt_targets:
        raise RuntimeError("No prompts available. Add prompts or pipeline topics.")

    titles = load_content_titles(limit=250)
    provider_names = normalize_provider_names(providers)
    provider_runs = build_provider_runs(
        provider_names=provider_names,
        prompts=prompt_targets,
        domain=domain,
        titles=titles,
    )

    enabled = [item for item in provider_runs if item.enabled and item.results]
    aggregate_score = (
        int(round(sum(item.score for item in enabled) / len(enabled))) if enabled else 0
    )
    mentions, cited_pages, by_provider = summarize_visibility(provider_runs, domain)
    report = {
        "generatedAt": datetime.now().isoformat(timespec="seconds"),
        "domain": domain,
        "prompts": [
            {
                "id": item.id,
                "prompt": item.prompt,
                "kind": item.kind,
                "weight": item.weight,
            }
            for item in prompt_targets
        ],
        "providers": [
            {
                "provider": item.provider,
                "model": item.model,
                "enabled": item.enabled,
                "error": item.error,
                "score": item.score,
                "results": [
                    {
                        "promptId": result.prompt_id,
                        "prompt": result.prompt,
                        "kind": result.kind,
                        "weight": result.weight,
                        "foundDomain": result.found_domain,
                        "foundTitles": result.found_titles,
                        "citations": result.citations,
                        "linkedDomains": result.linked_domains,
                        "ourDomainRank": result.our_domain_rank,
                        "status": result.status,
                        "score": result.score,
                        "response": result.response_text,
                    }
                    for result in item.results
                ],
            }
            for item in provider_runs
        ],
        "aggregateVisibilityScore": aggregate_score,
        "mentions": mentions,
        "citedPages": cited_pages,
        "providerMentions": by_provider,
    }
    return report


def write_visibility_report(report: dict, output_dir: Path | None = None) -> tuple[Path, Path]:
    output_dir = output_dir or (DATA_DIR / "visibility")
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    json_path = output_dir / f"visibility-{timestamp}.json"
    md_path = output_dir / f"visibility-{timestamp}.md"
    latest_json = output_dir / "latest.json"
    latest_md = output_dir / "latest.md"
    json_path.write_text(json.dumps(report, indent=2))
    md_path.write_text(render_report_markdown(report))
    latest_json.write_text(json_path.read_text())
    latest_md.write_text(md_path.read_text())
    return json_path, md_path


def load_latest_visibility_report(output_dir: Path | None = None) -> dict | None:
    output_dir = output_dir or (DATA_DIR / "visibility")
    latest = output_dir / "latest.json"
    if not latest.exists():
        return None
    try:
        return json.loads(latest.read_text())
    except Exception:  # noqa: BLE001
        return None


def run_openai(prompts: list[PromptTarget], domain: str, titles: list[str]) -> ProviderRun:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("VISIBILITY_OPENAI_MODEL", "gpt-5.4-mini")
    if not api_key:
        return ProviderRun("openai", model, False, "OPENAI_API_KEY not set.", [], 0)

    url = "https://api.openai.com/v1/responses"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    results: list[PromptResult] = []
    try:
        with httpx.Client(timeout=90.0) as client:
            for prompt_target in prompts:
                prompt = build_visibility_prompt(prompt_target.prompt, domain, titles)
                payload = {
                    "model": model,
                    "input": prompt,
                    "text": {"format": {"type": "text"}},
                }
                response = client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                text = extract_openai_text(response.json())
                results.append(score_prompt_response(prompt_target, text, domain, titles))
    except Exception as exc:  # noqa: BLE001
        return ProviderRun("openai", model, True, str(exc), results, weighted_average(results))
    return ProviderRun("openai", model, True, None, results, weighted_average(results))


def run_gemini(prompts: list[PromptTarget], domain: str, titles: list[str]) -> ProviderRun:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    model = os.getenv("VISIBILITY_GEMINI_MODEL", "gemini-2.0-flash")
    if not api_key:
        return ProviderRun("gemini", model, False, "GEMINI_API_KEY not set.", [], 0)

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    headers = {"Content-Type": "application/json"}
    results: list[PromptResult] = []
    try:
        with httpx.Client(timeout=90.0) as client:
            for prompt_target in prompts:
                prompt = build_visibility_prompt(prompt_target.prompt, domain, titles)
                payload = {"contents": [{"parts": [{"text": prompt}]}]}
                response = client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                text = extract_gemini_text(response.json())
                results.append(score_prompt_response(prompt_target, text, domain, titles))
    except Exception as exc:  # noqa: BLE001
        return ProviderRun("gemini", model, True, str(exc), results, weighted_average(results))
    return ProviderRun("gemini", model, True, None, results, weighted_average(results))


def run_perplexity(prompts: list[PromptTarget], domain: str, titles: list[str]) -> ProviderRun:
    api_key = os.getenv("PERPLEXITY_API_KEY", "").strip()
    model = os.getenv("VISIBILITY_PERPLEXITY_MODEL", "sonar")
    if not api_key:
        return ProviderRun(
            "perplexity",
            model,
            False,
            "PERPLEXITY_API_KEY not set.",
            [],
            0,
        )

    url = "https://api.perplexity.ai/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    results: list[PromptResult] = []
    try:
        with httpx.Client(timeout=90.0) as client:
            for prompt_target in prompts:
                prompt = build_visibility_prompt(prompt_target.prompt, domain, titles)
                payload = {
                    "model": model,
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a search assistant. Include concrete URLs where possible.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                }
                response = client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                text = extract_chat_message_text(response.json())
                results.append(score_prompt_response(prompt_target, text, domain, titles))
    except Exception as exc:  # noqa: BLE001
        return ProviderRun("perplexity", model, True, str(exc), results, weighted_average(results))
    return ProviderRun("perplexity", model, True, None, results, weighted_average(results))


def run_claude(prompts: list[PromptTarget], domain: str, titles: list[str]) -> ProviderRun:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    model = os.getenv("VISIBILITY_CLAUDE_MODEL", "claude-3-7-sonnet-latest")
    if not api_key:
        return ProviderRun("claude", model, False, "ANTHROPIC_API_KEY not set.", [], 0)

    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    results: list[PromptResult] = []
    try:
        with httpx.Client(timeout=90.0) as client:
            for prompt_target in prompts:
                prompt = build_visibility_prompt(prompt_target.prompt, domain, titles)
                payload = {
                    "model": model,
                    "max_tokens": 700,
                    "messages": [{"role": "user", "content": prompt}],
                }
                response = client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                text = extract_claude_text(response.json())
                results.append(score_prompt_response(prompt_target, text, domain, titles))
    except Exception as exc:  # noqa: BLE001
        return ProviderRun("claude", model, True, str(exc), results, weighted_average(results))
    return ProviderRun("claude", model, True, None, results, weighted_average(results))


def build_visibility_prompt(prompt: str, domain: str, titles: list[str]) -> str:
    title_hint = ", ".join(titles[:20])
    return (
        "A user asks this discovery query:\n"
        f"QUERY: {prompt}\n\n"
        "Return:\n"
        "1) Best answer (max 160 words)\n"
        "2) Top recommended sources with direct URLs\n"
        f"3) Whether {domain} should be included\n"
        "4) Which of these known titles are relevant by exact title\n"
        f"KNOWN_TITLES: {title_hint}\n"
    )


def score_prompt_response(
    prompt_target: PromptTarget, text: str, domain: str, titles: list[str]
) -> PromptResult:
    citations = extract_urls(text)
    linked_domains = [extract_hostname(url) for url in citations if extract_hostname(url)]
    normalized_domain = domain.lower().replace("https://", "").replace("http://", "").strip("/")
    rank = None
    for index, linked_domain in enumerate(linked_domains, start=1):
        if normalized_domain in linked_domain:
            rank = index
            break
    found_domain = rank is not None or normalized_domain in text.lower()
    found_titles = [title for title in titles if title and title.lower() in text.lower()][:6]

    score = 0
    if found_domain:
        score += 65
    if rank == 1:
        score += 20
    elif rank and rank <= 3:
        score += 10
    if found_titles:
        score += min(12, len(found_titles) * 4)
    if found_domain and citations:
        score += 3
    status = "ranked" if found_domain else "not_ranked"

    return PromptResult(
        prompt_id=prompt_target.id,
        prompt=prompt_target.prompt,
        kind=prompt_target.kind,
        weight=prompt_target.weight,
        found_domain=found_domain,
        found_titles=found_titles,
        citations=citations[:12],
        linked_domains=linked_domains[:12],
        our_domain_rank=rank,
        status=status,
        score=min(100, score),
        response_text=text.strip(),
    )


def weighted_average(results: list[PromptResult]) -> int:
    if not results:
        return 0
    weighted_total = sum(item.score * item.weight for item in results)
    weight_sum = sum(item.weight for item in results)
    if weight_sum <= 0:
        return 0
    return int(round(weighted_total / weight_sum))


def summarize_visibility(
    providers: list[ProviderRun], domain: str
) -> tuple[int, int, dict[str, int]]:
    mentions = 0
    cited_pages = 0
    by_provider: dict[str, int] = {}
    normalized_domain = domain.lower().replace("https://", "").replace("http://", "").strip("/")
    for provider in providers:
        provider_mentions = 0
        for result in provider.results:
            if result.found_domain:
                mentions += 1
                provider_mentions += 1
            cited_pages += sum(
                1 for url in result.citations if normalized_domain in url.lower()
            )
        by_provider[provider.provider] = provider_mentions
    return mentions, cited_pages, by_provider


def extract_openai_text(payload: dict) -> str:
    output = payload.get("output", [])
    chunks: list[str] = []
    for item in output:
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                chunks.append(content.get("text", ""))
    return "".join(chunks).strip()


def extract_gemini_text(payload: dict) -> str:
    candidates = payload.get("candidates", [])
    if not candidates:
        return ""
    parts = candidates[0].get("content", {}).get("parts", [])
    return "\n".join(str(part.get("text", "")).strip() for part in parts if part.get("text"))


def extract_chat_message_text(payload: dict) -> str:
    choices = payload.get("choices", [])
    if not choices:
        return ""
    content = choices[0].get("message", {}).get("content", "")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        return "".join(
            str(part.get("text", "")) for part in content if isinstance(part, dict)
        ).strip()
    return ""


def extract_claude_text(payload: dict) -> str:
    content = payload.get("content", [])
    blocks: list[str] = []
    for item in content:
        if item.get("type") == "text":
            blocks.append(str(item.get("text", "")))
    return "\n".join(blocks).strip()


def extract_urls(text: str) -> list[str]:
    return list(dict.fromkeys(re.findall(r"https?://[^\s)\]]+", text)))


def extract_hostname(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:  # noqa: BLE001
        return ""


def normalize_topics(topics: list[str], max_topics: int = 8) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in topics:
        value = raw.strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(value)
    return cleaned[:max_topics]


def infer_prompt_kind(prompt: str) -> str:
    lowered = prompt.lower()
    brand_tokens = {"doctor towels", "doctortowels", "dr towel", "drtowel"}
    return "branded" if any(token in lowered for token in brand_tokens) else "generic"


def load_prompt_targets(
    topics: list[str], max_topics: int, prompt_file: Path | None = None
) -> list[PromptTarget]:
    explicit = normalize_topics(topics, max_topics=max_topics)
    if explicit:
        return [
            PromptTarget(
                id=f"prompt-{index + 1}",
                prompt=prompt,
                kind=infer_prompt_kind(prompt),
                weight=BRANDED_WEIGHT if infer_prompt_kind(prompt) == "branded" else GENERIC_WEIGHT,
            )
            for index, prompt in enumerate(explicit)
        ]

    file_prompts = load_prompts_from_file(prompt_file or PROMPT_FILE, max_topics=max_topics)
    if file_prompts:
        return file_prompts

    return []


def load_prompts_from_file(path: Path, max_topics: int = 8) -> list[PromptTarget]:
    if not path.exists():
        return []
    raw = yaml.safe_load(path.read_text()) or {}
    rows = raw.get("prompts", [])
    prompts: list[PromptTarget] = []
    for index, row in enumerate(rows):
        prompt = str(row.get("prompt", "")).strip()
        if not prompt:
            continue
        kind = str(row.get("kind", "")).strip().lower() or infer_prompt_kind(prompt)
        if kind not in {"generic", "branded"}:
            kind = infer_prompt_kind(prompt)
        weight = (
            float(row.get("weight", BRANDED_WEIGHT if kind == "branded" else GENERIC_WEIGHT))
            if row.get("weight") is not None
            else (BRANDED_WEIGHT if kind == "branded" else GENERIC_WEIGHT)
        )
        prompt_id = str(row.get("id", f"prompt-{index + 1}")).strip() or f"prompt-{index + 1}"
        prompts.append(
            PromptTarget(
                id=prompt_id,
                prompt=prompt,
                kind=kind,
                weight=weight,
            )
        )
        if len(prompts) >= max_topics:
            break
    return prompts


def load_default_topics(max_topics: int = 8) -> list[str]:
    from .config import AgentConfig

    config = AgentConfig()
    topics: list[str] = []
    seen: set[str] = set()
    for item in load_pipeline(config.pipeline_file):
        value = item.query.strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        topics.append(value)
        if len(topics) >= max_topics:
            break
    return topics


def normalize_provider_names(providers: list[str] | None) -> list[str]:
    allowed = {"openai", "gemini", "perplexity", "claude"}
    if not providers:
        return ["openai", "gemini", "perplexity", "claude"]
    ordered: list[str] = []
    seen: set[str] = set()
    for value in providers:
        key = value.strip().lower()
        if key not in allowed or key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return ordered


def build_provider_runs(
    provider_names: list[str],
    prompts: list[PromptTarget],
    domain: str,
    titles: list[str],
) -> list[ProviderRun]:
    runners = {
        "openai": run_openai,
        "gemini": run_gemini,
        "perplexity": run_perplexity,
        "claude": run_claude,
    }
    runs: list[ProviderRun] = []
    for name in provider_names:
        runner = runners.get(name)
        if not runner:
            continue
        runs.append(runner(prompts, domain, titles))
    return runs


def load_content_titles(limit: int = 200) -> list[str]:
    from .config import CONTENT_DIR

    titles: list[str] = []
    for path in sorted(CONTENT_DIR.glob("*.md"), reverse=True):
        frontmatter, _ = parse_markdown_file(path)
        title = str(frontmatter.get("title", "")).strip()
        if title:
            titles.append(title)
        if len(titles) >= limit:
            break
    return titles


def render_report_markdown(report: dict) -> str:
    lines: list[str] = []
    lines.append("# AI Visibility Report")
    lines.append("")
    lines.append(f"- Generated: {report.get('generatedAt', '')}")
    lines.append(f"- Domain: {report.get('domain', '')}")
    lines.append(
        f"- Aggregate visibility score: {report.get('aggregateVisibilityScore', 0)}/100"
    )
    lines.append(f"- Mentions: {report.get('mentions', 0)}")
    lines.append(f"- Cited pages: {report.get('citedPages', 0)}")
    lines.append("")
    lines.append("## Prompt Set")
    for prompt in report.get("prompts", []):
        lines.append(
            f"- [{prompt.get('kind', 'generic')}] {prompt.get('prompt', '')} (weight {prompt.get('weight', 1)})"
        )
    lines.append("")
    lines.append("## Providers")
    for provider in report.get("providers", []):
        lines.append(
            f"### {provider.get('provider', 'unknown').title()} ({provider.get('model', '')})"
        )
        lines.append(f"- Enabled: {provider.get('enabled', False)}")
        lines.append(f"- Score: {provider.get('score', 0)}/100")
        if provider.get("error"):
            lines.append(f"- Error: {provider.get('error')}")
        for result in provider.get("results", []):
            lines.append(
                f"- {result.get('prompt', '')}: {result.get('status', 'not_ranked')} ({result.get('score', 0)})"
            )
            if result.get("foundTitles"):
                lines.append(f"  - Showing in content: {', '.join(result.get('foundTitles', []))}")
            if result.get("citations"):
                lines.append(f"  - Citations: {', '.join(result.get('citations', [])[:4])}")
    lines.append("")
    return "\n".join(lines)
