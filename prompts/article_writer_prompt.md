Write one blog article for {brand_name}.

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

Article plan:
{plan_json}

Return JSON with this shape:
{{
  "title": "string",
  "slug": "kebab-case",
  "meta_description": "max 160 chars",
  "excerpt": "1-2 sentence teaser",
  "body_markdown": "full markdown article",
  "medical_citations": ["Title - Organization or journal - URL"],
  "product_knowledge_used": ["specific product or brand knowledge point used in the draft"],
  "customer_language_used": ["specific customer problem phrasing used in the draft"]
}}

Mandatory structure (exact H2s, in this order):
1. ## The Problem They Didn't Know They Had
2. ## The Science Behind The Problem
3. ## The Mechanisms — How It's Actively Hurting You
4. ## Customer Language — What Real People Were Dealing With
5. ## Actionable Habits — What To Actually Do
6. ## Why Doctor Towels Was Built For This
7. ## The Bottom Line
8. ## Medical Sources & Further Reading

Non-negotiable rules:
- Target 1,800 to 2,500 words.
- Open with a relatable scenario and an aha moment in the first 3–4 sentences.
- Mechanisms section: 2–4 H3 sub-sections with medical backing.
- Habits section: 4–6 numbered H3s formatted exactly like "### 1. ...".
- Use proper capitalization throughout.
- Use bullet points for lists; only the Habits section uses numbered H3s.
- Add horizontal rules (`---`) between major sections.
- Mention and link these Doctor Towels research sources:
  - https://www.doctortowels.com/pages/research-page
  - https://cdn.shopify.com/s/files/1/0376/8529/7196/files/Testing_Report.pdf?v=1758528655
- Include at least 4 named experts/studies with data points/quotes from provided sources.
- Include expert full name, credential, and institution/affiliation whenever available.
- Reference these proprietary points when relevant and source-backed:
  - IADVL 2023: 74% of acne patients show C. acnes on towels
  - Apollo Hospitals 2024 RCT: 112 patients, 21% average reduction in inflammatory acne lesions in 14 days
  - 160-wash efficacy
  - 890M CFUs after 7 days unwashed
- In the Doctor Towels section, naturally cover:
  - SkinShield Technology™
  - Dual-Side Design (Patented)
  - Skin-Safe Fibers
  - 160-Wash Efficacy
  - Clinical Validation
- Use 3–5 customer pain lines naturally.
- Keep education-first tone. No hard CTA. End with perspective shift, not pitch.
- Mandatory internal-link rule: include at least one `/blogs/...` main-blog link from `internal_links` in the article body, written naturally in-context.
- Do not invent citations or unsupported claims.
