Choose one fresh daily blog topic for {brand_name} using a pillar-first content strategy.

Brand context:
{brand_brief}

Approved facts:
{approved_facts}

Style guide:
{style_guide}

Product knowledge:
{product_knowledge}

Medical research:
{medical_research}

Customer language:
{customer_language}

Source library:
{source_library}

Pillar-linked keyword clusters:
{clusters}

Recent queries already covered:
{recent_queries}

Return JSON with this shape:
{{
  "title": "string",
  "slug": "kebab-case",
  "target_query": "string",
  "meta_description": "max 160 chars",
  "angle": "why this topic matters and how it connects to Doctor Towels",
  "outline": ["H2 or H3 heading", "H2 or H3 heading"],
  "internal_links": ["/products/doctor-towels", "/blogs/acne-safe-towels-guide"],
  "keywords_to_use": ["keyword phrase 1", "keyword phrase 2", "keyword phrase 3"]
}}

Rules:
- Pick a query that is not in the recent list.
- Prioritize low-competition acne, towel, fabric, breakouts, hygiene, and face-drying topics.
- Treat each cluster as a tagged sub-blog under a main pillar topic.
- Keep a cadence bias toward pillar 1 style topics unless recent queries already over-index there.
- Choose one pillar-aligned cluster and make the title/angle explicitly grounded in that pillar claim.
- `internal_links` must include the selected pillar's main-blog URL in `/blogs/...` format so sub-blogs always link back to their main pillar post.
- Populate "keywords_to_use" from the selected cluster's supporting keywords plus tightly related variants for that query (5-10 items).
- Prefer informational or comparison intent over hard-sell transactional topics.
- Keep the angle grounded in skincare education.
- Prefer ideas that clearly connect to the source library, especially product knowledge, customer language, and research-backed concerns.
- Only choose topics where the article can naturally satisfy the strict article structure and sourcing rules.
- Ensure the topic can support the strict 8-section structure and all mandatory product/source requirements.
- Ensure the topic naturally supports:
  - The Problem They Didn't Know They Had
  - The Science Behind The Problem
  - Mechanisms section with 2-4 mechanisms
  - Customer-language section
  - Actionable habits section
- Ensure the topic can credibly include the Doctor Towels research links and at least 4 named expert/study references.
