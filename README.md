# Doctor Towels Blog Agent

This project generates SEO blog drafts for a weekly Doctor Towels pipeline, scores each draft against strict editorial guidelines, and lets you approve before push.
It is now organized around a 4-pillar strategy where each sub-blog topic is tagged to a parent main topic.

## What it does

- Generates one or many weekly drafts from pillar-linked keyword clusters
- Auto-bootstraps one pillar-head topic for each pillar before regular sub-blog generation
- Auto-assigns each sub-blog to its pillar-head reference link (`/blogs/{pillar-head-slug}`)
- Enforces a hard rule that sub-blogs include a natural in-body link to their pillar main blog (`/blogs/...`)
- Avoids repeating recently used queries
- Scores each draft against guideline checks (structure, citations, customer language, product coverage)
- Saves markdown output to `content/posts/`
- Tracks pipeline status in `data/pipeline.yaml` (`draft`, `approved`, `pushed`, `rejected`)
- Tracks `pillar_name`, `main_topic`, and `sub_blog_tag` per pipeline item for hierarchy integrity
- Tracks `planned_keywords` per topic so keyword targeting is visible before approval
- Tracks topic history in `data/topic_history.yaml`

## Setup

1. Create a virtual environment and install dependencies.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

2. Set environment variables.

```bash
cp .env.example .env
```

Then update `.env` with your values:

```bash
BLOG_AGENT_API_KEY="your-key"
OPENAI_API_KEY="your-key"
BLOG_AGENT_MODEL="gpt-5.4"
BLOG_AGENT_TOPIC_MODEL="gpt-5.4-mini"
BLOG_AGENT_ARTICLE_MODEL="gpt-5.4"
BLOG_AGENT_API_BASE_URL="https://api.openai.com/v1"
BLOG_AGENT_API_MODE="auto"
BLOG_AGENT_IMAGE_MODEL="gpt-image-1.5"
BLOG_AGENT_IMAGE_PROMPT_MODEL="gpt-5.4-mini"
BLOG_AGENT_IMAGE_QUALITY="low"
BLOG_AGENT_IMAGE_SIZE="1536x1024"
BLOG_AGENT_IMAGE_FORMAT="png"
SHOPIFY_CLIENT_ID=""
SHOPIFY_CLIENT_SECRET=""
MYSHOPIFY_DOMAIN=""
SHOPIFY_API_VERSION="2026-01"
```

`OPENAI_API_KEY` is supported directly. `BLOG_AGENT_API_KEY` is also supported and takes priority if both are set.

`BLOG_AGENT_API_MODE=auto` uses OpenAI `responses` endpoint when `api.openai.com` is configured, and falls back to `chat/completions` for compatibility providers.

3. Add your reusable writing inputs.

- Update `data/style_guide.md`
- Update `data/approved_facts.md`
- Add monthly notes under `data/sources/`

## Usage

Generate today's article:

```bash
blog-agent generate
```

Generate a post for a specific date:

```bash
blog-agent generate --date 2026-03-26
```

Fetch live keyword ideas from the API:

```bash
curl -G "http://127.0.0.1:8124/api/keyword-research" \
  --data-urlencode "seed=acne-safe towel" \
  --data-urlencode "seed=can your towel cause acne"
```

Run the full local app (API + UI):

```bash
npm install
npm run dev:full
```

Then open `http://127.0.0.1:4173`.

The UI is API-backed and uses the Python backend as source-of-truth. Local storage is only used as a lightweight cache.

## Cloud deploy (single service)

This repo now supports a single-process cloud deploy: the Python API serves both `/api/*` and the built React app.

1. Push this repo to GitHub.
2. In Render, create a new Blueprint service from the repo root (`render.yaml`).
3. Add required secrets in Render:
   - `OPENAI_API_KEY`
   - `SHOPIFY_CLIENT_ID`
   - `SHOPIFY_CLIENT_SECRET`
   - `MYSHOPIFY_DOMAIN`
   - `NOTION_API_TOKEN`
   - `NOTION_PARENT_PAGE_ID`
   - `BLOG_AGENT_USE_NOTION=1`
   - Prefer setting these explicitly in production to avoid relying on ephemeral `data/notion_state.yaml`:
     - `NOTION_PILLARS_DB_ID`
     - `NOTION_BLOG_PIPELINE_DB_ID`
     - `NOTION_SETTINGS_DB_ID`
4. Deploy and open your Render URL.

Notes on Notion IDs:
- You can pass either raw UUIDs or full Notion URLs; the API now normalizes URL values to UUIDs.
- Check `/api/notion/state` for `diagnostics.missing` if deployed UI is not showing Notion rows.

## Netlify frontend

The frontend can still be deployed statically to Netlify, but it must target a running API host (`VITE_API_BASE_URL` or same-origin proxy) because workflow actions are server-backed.

Generate a four-week draft pipeline by API:

```bash
curl -X POST "http://127.0.0.1:8124/api/pipeline/generate" \
  -H "Content-Type: application/json" \
  -d '{"weeks":4}'
```

## Daily automation

You can still run the legacy CLI scheduler command:

```bash
cd /path/to/blog-agent && ./run_daily.sh
```

On Windows PowerShell:

```powershell
.\run-daily.ps1
```

For settings-driven automation from the UI (`dailyTime`, `timezone`, `enabled`), call the API tick endpoint every minute:

```bash
curl -X POST "https://<your-api-host>/api/automation/tick"
```

Render blueprint includes a cron service (`blog-agent-automation-tick`) scheduled every minute.
Set `BLOG_AGENT_TICK_URL` to your public API endpoint, for example:

```bash
BLOG_AGENT_TICK_URL="https://<your-render-web-service>/api/automation/tick"
```

The cron job now uses Python stdlib HTTP calls (not `curl`) so it runs on the default Docker image.
Render blueprint also sets `BLOG_AGENT_BACKGROUND_LOOP=0` so cron is the single scheduler source in production.

Important:
- Time-based automation on deployed environments depends on periodic calls to `/api/automation/tick` (cron or external scheduler).
- If `BLOG_AGENT_TICK_URL` is not set in Render cron, scheduled push/approve/generate will not run.
- If Notion is not configured, settings now persist to `data/automation_settings.yaml` as fallback, but on ephemeral deploys this file is not durable across redeploys/restarts.

Each scheduled run now performs the full workflow automatically:
1. Generate pipeline topic(s) (`BLOG_AGENT_AUTOMATION_TOPIC_COUNT`, default `1`)
2. Approve and generate full draft markdown
3. Generate cover image via the same configured API key/provider
4. Push to Shopify using resolved/default blog id

Useful env controls:
- `BLOG_AGENT_AUTOMATION_TOPIC_ROLE` (`main` or `side`, default `side`)
- `BLOG_AGENT_AUTOMATION_TOPIC_COUNT` (default `1`)
- `BLOG_AGENT_AUTOMATION_GENERATE_IMAGE` (`1`/`0`, default `1`)
- `BLOG_AGENT_AUTOMATION_PUSH` (`1`/`0`, default `1`)
- `SHOPIFY_DEFAULT_BLOG_ID` (optional explicit fallback blog id)

## What to customize

- `data/brand_brief.md`: brand voice and positioning
- `data/style_guide.md`: banned words, preferred phrasing, CTA behavior
- `data/approved_facts.md`: exact claims the model is allowed to make
- `data/sources/`: product knowledge, research notes, customer language, competitor gaps
- `data/keyword_clusters.yaml`: topic universe
- `prompts/`: planning and writing behavior
- `blog-agent-api`: optional Python API for CLI/server workflows
- `/api/keyword-research`: Google Trends-backed keyword discovery endpoint
- `/api/images/generate`: OpenAI image generation endpoint (defaults: `gpt-image-1.5`, `low`, landscape `1536x1024`)
  - Uses `BLOG_AGENT_IMAGE_PROMPT_MODEL` (default `gpt-5.4-mini`) to write a topic-aware abstract art-direction prompt before image generation.
- `/api/shopify/blogs`: fetch Shopify blogs for publish targeting
- `src/ui/`: React app for generation and preview
- `/api/settings`: load/update automation schedule (`dailyTime`, `timezone`, `enabled`)
- `/api/notion/setup`: auto-create `SEO Pillars`, `Blog Pipeline`, `Automation Settings` databases
- `/api/notion/migrate`: one-time import from local `pipeline.yaml` and markdown files to Notion
- `/api/automation/run-now`: set run-now and execute full automation workflow immediately
- `/api/automation/tick`: idempotent scheduler endpoint for daily workflow checks

## Cross-model AI visibility tracking

You can run the same topic checks across multiple AI providers and score whether your domain/content is surfaced.

Set keys in `.env`:

```bash
OPENAI_API_KEY="..."
GEMINI_API_KEY="..."
PERPLEXITY_API_KEY="..."
ANTHROPIC_API_KEY="..."
```

Prompt set is stored in:

- `data/visibility_prompts.yaml`

Use 5-8 prompts and mark each as `generic` or `branded`. Generic prompts are weighted higher by default so brand-name wins do not overstate category visibility.

Run with explicit topics:

```bash
blog-agent visibility \
  --topic "acne-safe towels" \
  --topic "best towel material for acne-prone skin" \
  --topic "how often should you change face towels"
```

Or run using the current pipeline's target queries:

```bash
blog-agent visibility
```

Output reports are written to:

- `data/visibility/latest.json`
- `data/visibility/latest.md`

You can also choose a custom output directory:

```bash
blog-agent visibility --output-dir ./data/visibility
```

Use a custom prompt file if needed:

```bash
blog-agent visibility --prompt-file ./data/visibility_prompts.yaml
```

## Supabase backup for blog entries (including images)

Supabase support remains available as fallback. Notion is now the primary backend when `BLOG_AGENT_USE_NOTION=1`.

1. In Supabase SQL Editor, run:
   - `sql/supabase_blog_entries.sql`
2. Set these env vars in your API host and locally:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `SUPABASE_BLOG_TABLE` (optional, defaults to `table_name`)
   - `SUPABASE_LOGICAL_NAMESPACE` (optional, defaults to `pillar_architecture_blog_entries`)
   - `SUPABASE_SYNC_CHUNK_SIZE` (optional, defaults to `3`)
   - `BLOG_AGENT_USE_SUPABASE_NAMESPACE` (set to `1` on deployed API)
3. Sync existing local data:

```bash
source .venv/bin/activate
blog-agent supabase-sync
```

The sync upserts each pipeline item by `pipeline_id` and includes:
- pipeline/topic status fields
- markdown body/frontmatter
- generated image filename + mime type + base64

## Notes

- The agent is intentionally conservative about medical and scientific claims.
- If you want stronger clinical specificity, add approved references and claims to `data/approved_facts.md` before generating posts.
- Every generated post now requires medical citations, at least one product-knowledge input, and at least one customer-language input.
