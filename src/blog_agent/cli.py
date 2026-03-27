from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from .agent import BlogAgent
from .config import AgentConfig
from .visibility import run_visibility_scan, write_visibility_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a Doctor Towels blog post.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="Generate one blog post.")
    generate.add_argument(
        "--date",
        dest="date_value",
        help="Optional ISO date override, e.g. 2026-03-26",
    )

    visibility = subparsers.add_parser(
        "visibility",
        help="Run cross-model visibility checks (OpenAI, Gemini, Perplexity, Claude).",
    )
    visibility.add_argument(
        "--topic",
        action="append",
        default=[],
        help="Topic to test. Repeat for multiple topics. If omitted, uses pipeline queries.",
    )
    visibility.add_argument(
        "--domain",
        default="doctortowels.com",
        help="Domain to track for visibility scoring.",
    )
    visibility.add_argument(
        "--max-topics",
        type=int,
        default=8,
        help="Maximum number of topics to test (default: 8).",
    )
    visibility.add_argument(
        "--output-dir",
        default="",
        help="Optional output directory for report files.",
    )
    visibility.add_argument(
        "--prompt-file",
        default="",
        help="Optional YAML file containing prompt set.",
    )
    visibility.add_argument(
        "--provider",
        action="append",
        default=[],
        help="Provider to run. Repeat for multiple. Allowed: openai, gemini, perplexity, claude.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = AgentConfig()
    agent = BlogAgent(config)

    if args.command == "generate":
        target_date = date.fromisoformat(args.date_value) if args.date_value else None
        output = agent.generate_post(today=target_date)
        print(output.output_path)
        return

    if args.command == "visibility":
        report = run_visibility_scan(
            topics=args.topic,
            domain=args.domain,
            max_topics=max(1, min(20, args.max_topics)),
            prompt_file=Path(args.prompt_file).expanduser() if args.prompt_file else None,
            providers=args.provider,
        )
        output_dir = Path(args.output_dir).expanduser() if args.output_dir else None
        json_path, md_path = write_visibility_report(report, output_dir=output_dir)
        print(f"Aggregate visibility score: {report.get('aggregateVisibilityScore', 0)}/100")
        print(f"JSON report: {json_path}")
        print(f"Markdown report: {md_path}")
        return


if __name__ == "__main__":
    main()
