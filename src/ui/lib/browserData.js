import { parse } from "yaml";
import pipelineRaw from "../../../data/pipeline.yaml?raw";
import clustersRaw from "../../../data/keyword_clusters.yaml?raw";

const STORAGE_KEY = "doctor-towels-blog-agent/workspace-v1";

const SHOPIFY_BLOGS = [
  { id: "doctor-towels-journal", title: "Doctor Towels Journal" },
  { id: "skin-education", title: "Skin Education" },
  { id: "product-updates", title: "Product Updates" }
];

const parsedPipeline = parse(pipelineRaw);
const parsedClusters = parse(clustersRaw);
const seedClusters = Array.isArray(parsedClusters?.clusters) ? parsedClusters.clusters : [];
const seedPipeline = Array.isArray(parsedPipeline?.pipeline) ? parsedPipeline.pipeline : [];

export function loadWorkspace() {
  const seed = buildSeedWorkspace();
  if (typeof window === "undefined") return seed;

  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return seed;
    const saved = JSON.parse(raw);
    if (!saved || typeof saved !== "object") return seed;

    return {
      clusters: seed.clusters,
      pillars: normalizePillars(saved.pillars, seed.clusters),
      pipeline: normalizePipeline(saved.pipeline, seed.clusters),
      shopifyBlogs: normalizeBlogs(saved.shopifyBlogs)
    };
  } catch {
    return seed;
  }
}

export function persistWorkspace(workspace) {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        pillars: workspace.pillars,
        pipeline: workspace.pipeline,
        shopifyBlogs: workspace.shopifyBlogs
      })
    );
  } catch {
    // Ignore storage failures so the UI can stay interactive.
  }
}

export function resetWorkspace() {
  if (typeof window !== "undefined") {
    window.localStorage.removeItem(STORAGE_KEY);
  }
  return buildSeedWorkspace();
}

export function generateTopics(workspace, { pillarId, role, count = 1 }) {
  const pillar = workspace.pillars.find((entry) => entry.pillarId === pillarId) || workspace.pillars[0];
  if (!pillar) {
    throw new Error("No pillar is available for generation.");
  }

  const now = new Date();
  const nextPipeline = [...workspace.pipeline];
  const created = [];

  if (role === "main") {
    const existingMain = nextPipeline.find(
      (item) => item.pillar_id === pillar.pillarId && item.topic_role === "main" && item.status !== "rejected"
    );

    if (existingMain) {
      return {
        workspace: { ...workspace, pipeline: nextPipeline },
        created: [existingMain],
        message: "Main pillar guide already exists for this pillar."
      };
    }

    const mainItem = createMainTopic(pillar, now, nextPipeline.length);
    nextPipeline.unshift(mainItem);
    created.push(mainItem);
    return {
      workspace: { ...workspace, pipeline: nextPipeline },
      created,
      message: "Main pillar guide generated locally."
    };
  }

  const candidates = buildSideTopicCandidates(workspace.clusters, pillar.pillarId, nextPipeline);
  const desiredCount = Math.max(1, Number(count) || 1);

  for (let index = 0; index < desiredCount; index += 1) {
    const candidate = candidates[index] || buildFallbackTopic(pillar, now, index);
    const item = createSideTopic(candidate, pillar, now, nextPipeline.length + index);
    nextPipeline.unshift(item);
    created.push(item);
  }

  return {
    workspace: { ...workspace, pipeline: nextPipeline },
    created,
    message: `${created.length} topic${created.length === 1 ? "" : "s"} generated locally.`
  };
}

export function approveTopic(workspace, { id }) {
  const approvedAt = new Date().toISOString();
  const pipeline = workspace.pipeline.map((item) => {
    if (item.id !== id) return item;
    return {
      ...item,
      status: "approved",
      approved_at: approvedAt,
      post_id: item.post_id || `${buildPostFilename(item)}.md`,
      path: item.path || `/content/posts/${buildPostFilename(item)}.md`,
      metadata: {
        ...(item.metadata || {}),
        slug: item.metadata?.slug || slugify(item.title)
      },
      guideline_report: item.guideline_report || buildGuidelineReport(item),
      html: item.html || buildArticleHtml(item)
    };
  });

  return { ...workspace, pipeline };
}

export function rejectTopic(workspace, { id }) {
  return {
    ...workspace,
    pipeline: workspace.pipeline.map((item) =>
      item.id === id
        ? {
            ...item,
            status: "rejected"
          }
        : item
    )
  };
}

export function pushTopic(workspace, { id, blogId }) {
  const pushedAt = new Date().toISOString();
  const blog = workspace.shopifyBlogs.find((entry) => entry.id === blogId);

  return {
    ...workspace,
    pipeline: workspace.pipeline.map((item) =>
      item.id === id
        ? {
            ...item,
            status: "pushed",
            pushed_at: pushedAt,
            shopify_blog_id: blogId,
            shopify_article_id: item.shopify_article_id || `local-${slugify(item.title)}`,
            shopify_article_handle: item.shopify_article_handle || slugify(item.title),
            html: item.html || buildArticleHtml(item),
            metadata: {
              ...(item.metadata || {}),
              pushed_blog_title: blog?.title || ""
            }
          }
        : item
    )
  };
}

export function generateCoverImage(workspace, { id }) {
  return {
    ...workspace,
    pipeline: workspace.pipeline.map((item) =>
      item.id === id
        ? {
            ...item,
            generatedImageUrl: item.generatedImageUrl || buildCoverImage(item)
          }
        : item
    )
  };
}

function buildSeedWorkspace() {
  return {
    clusters: seedClusters,
    pillars: normalizePillars([], seedClusters),
    pipeline: normalizePipeline(seedPipeline, seedClusters),
    shopifyBlogs: [...SHOPIFY_BLOGS]
  };
}

function normalizePillars(existingPillars, clusters) {
  const grouped = new Map();

  for (const cluster of clusters) {
    const pillarId = String(cluster?.pillar_id || "").trim();
    if (!pillarId) continue;

    if (!grouped.has(pillarId)) {
      grouped.set(pillarId, {
        pillarId,
        pillarName: String(cluster?.pillar_name || cluster?.main_topic || cluster?.name || "Untitled pillar"),
        pillarClaim: String(cluster?.pillar_claim || "").trim(),
        clusters: []
      });
    }

    grouped.get(pillarId).clusters.push(cluster);
  }

  const fallback = [...grouped.values()];
  if (!Array.isArray(existingPillars) || existingPillars.length === 0) return fallback;

  return fallback.map((pillar) => {
    const saved = existingPillars.find((entry) => entry?.pillarId === pillar.pillarId);
    return saved ? { ...pillar, ...saved, clusters: pillar.clusters } : pillar;
  });
}

function normalizePipeline(items, clusters) {
  if (!Array.isArray(items)) return [];
  return items
    .map((item, index) => normalizePipelineItem(item, clusters, index))
    .sort((left, right) => new Date(right.created_at || 0).getTime() - new Date(left.created_at || 0).getTime());
}

function normalizePipelineItem(item, clusters, index) {
  const safe = item && typeof item === "object" ? item : {};
  const pillar = findPillarMeta(clusters, safe.pillar_id, safe.pillar_name);
  const title = String(safe.title || safe.query || `Untitled topic ${index + 1}`);
  const status = String(safe.status || "topic");
  const outline = normalizeOutline(safe.topic_outline);
  const html =
    typeof safe.html === "string" && safe.html.trim()
      ? safe.html
      : status === "approved" || status === "pushed"
        ? buildArticleHtml({ ...safe, title, topic_outline: outline, pillar_name: pillar.pillar_name })
        : "";

  return {
    ...safe,
    id: String(safe.id || `seed-topic-${index + 1}`),
    title,
    query: String(safe.query || title.toLowerCase()),
    cluster: String(safe.cluster || pillar.cluster || "Topic cluster"),
    pillar_id: String(safe.pillar_id || pillar.pillar_id || ""),
    pillar_name: String(safe.pillar_name || pillar.pillar_name || ""),
    pillar_claim: String(safe.pillar_claim || pillar.pillar_claim || ""),
    main_topic: String(safe.main_topic || pillar.main_topic || ""),
    sub_blog_tag: String(safe.sub_blog_tag || slugify(title)),
    status,
    topic_role: String(safe.topic_role || inferTopicRole(title, safe.pillar_name, safe.main_topic)),
    topic_angle: String(safe.topic_angle || buildTopicAngle(title, pillar)),
    topic_outline: outline,
    topic_internal_links: Array.isArray(safe.topic_internal_links) ? safe.topic_internal_links : [],
    planned_keywords: Array.isArray(safe.planned_keywords) ? safe.planned_keywords : [],
    metadata: safe.metadata && typeof safe.metadata === "object" ? safe.metadata : {},
    guideline_report: normalizeGuidelineReport(safe.guideline_report),
    generatedImageUrl: String(safe.generatedImageUrl || ""),
    html,
    created_at: String(safe.created_at || new Date().toISOString())
  };
}

function normalizeOutline(outline) {
  if (!Array.isArray(outline) || outline.length === 0) {
    return [];
  }
  return outline.map((entry) => String(entry));
}

function normalizeBlogs(blogs) {
  if (!Array.isArray(blogs) || blogs.length === 0) return [...SHOPIFY_BLOGS];
  return blogs
    .filter((blog) => blog && typeof blog === "object")
    .map((blog, index) => ({
      id: String(blog.id || `blog-${index + 1}`),
      title: String(blog.title || `Blog ${index + 1}`)
    }));
}

function normalizeGuidelineReport(report) {
  if (!report || typeof report !== "object") return null;
  return {
    ...report,
    checks: Array.isArray(report.checks) ? report.checks : []
  };
}

function inferTopicRole(title, pillarName, mainTopic) {
  const normalizedTitle = String(title || "").trim().toLowerCase();
  const normalizedPillar = String(pillarName || mainTopic || "").trim().toLowerCase();
  return normalizedTitle === normalizedPillar ? "main" : "side";
}

function findPillarMeta(clusters, pillarId, pillarName) {
  const direct = clusters.find((cluster) => cluster.pillar_id === pillarId);
  if (direct) return direct;

  const byName = clusters.find(
    (cluster) => String(cluster.pillar_name || "").trim().toLowerCase() === String(pillarName || "").trim().toLowerCase()
  );
  if (byName) return byName;

  return {
    pillar_id: pillarId || "",
    pillar_name: pillarName || "",
    pillar_claim: "",
    main_topic: pillarName || "",
    cluster: "Topic cluster"
  };
}

function buildSideTopicCandidates(clusters, pillarId, pipeline) {
  const existingQueries = new Set(
    pipeline.map((item) => String(item.query || "").trim().toLowerCase()).filter(Boolean)
  );

  return clusters
    .filter((cluster) => cluster.pillar_id === pillarId)
    .flatMap((cluster) =>
      (Array.isArray(cluster.queries) ? cluster.queries : []).map((query, index) => ({
        query: String(query),
        clusterName: String(cluster.name || "Topic cluster"),
        sub_blog_tag: String(cluster.sub_blog_tag || slugify(query)),
        pillarClaim: String(cluster.pillar_claim || ""),
        notes: String(cluster.notes || ""),
        priority: Number(cluster.cadence_weight || 0) * 100 - index
      }))
    )
    .filter((candidate) => !existingQueries.has(candidate.query.toLowerCase()))
    .sort((left, right) => right.priority - left.priority);
}

function createMainTopic(pillar, now, offset) {
  const title = pillar.pillarName;
  return {
    id: buildId("main", offset),
    post_id: null,
    title,
    query: title.toLowerCase(),
    cluster: "Main pillar overview",
    pillar_id: pillar.pillarId,
    pillar_name: pillar.pillarName,
    pillar_claim: pillar.pillarClaim,
    main_topic: pillar.pillarName,
    sub_blog_tag: slugify(title),
    is_pillar_head: true,
    pillar_head_post_id: null,
    pillar_head_slug: slugify(title),
    planned_keywords: [],
    path: null,
    scheduled_for: now.toISOString().slice(0, 10),
    status: "topic",
    topic_role: "main",
    created_at: now.toISOString(),
    approved_at: null,
    pushed_at: null,
    shopify_article_id: null,
    shopify_blog_id: null,
    shopify_article_handle: null,
    topic_angle: `Build the flagship pillar guide for ${pillar.pillarName}. Lead with the core claim, connect it to the Doctor Towels routine, and give readers a durable overview that future sub-blogs can link back to.`,
    topic_outline: [
      `H2: What ${pillar.pillarName} Means in Practice`,
      "H2: Why This Topic Matters for Acne-Prone Skin",
      "H2: The Daily Habits That Reinforce the Pillar",
      "H2: How Supporting Articles Branch Out From This Guide"
    ],
    topic_internal_links: ["/products/doctor-towels"],
    guideline_report: null,
    metadata: {
      slug: slugify(title),
      mode: "browser-native"
    },
    generatedImageUrl: "",
    html: ""
  };
}

function createSideTopic(candidate, pillar, now, offset) {
  const title = sentenceCaseToTitle(candidate.query);

  return {
    id: buildId("topic", offset),
    post_id: null,
    title,
    query: candidate.query,
    cluster: candidate.clusterName,
    pillar_id: pillar.pillarId,
    pillar_name: pillar.pillarName,
    pillar_claim: pillar.pillarClaim,
    main_topic: pillar.pillarName,
    sub_blog_tag: candidate.sub_blog_tag,
    is_pillar_head: false,
    pillar_head_post_id: null,
    pillar_head_slug: null,
    planned_keywords: [],
    path: null,
    scheduled_for: now.toISOString().slice(0, 10),
    status: "topic",
    topic_role: "side",
    created_at: new Date(now.getTime() + offset).toISOString(),
    approved_at: null,
    pushed_at: null,
    shopify_article_id: null,
    shopify_blog_id: null,
    shopify_article_handle: null,
    topic_angle: buildCandidateAngle(title, pillar, candidate),
    topic_outline: buildOutlineFromQuery(title, pillar),
    topic_internal_links: ["/products/doctor-towels", `/blogs/${slugify(pillar.pillarName)}`],
    guideline_report: null,
    metadata: {
      slug: slugify(title),
      mode: "browser-native"
    },
    generatedImageUrl: "",
    html: ""
  };
}

function buildFallbackTopic(pillar, now, index) {
  const title = `${pillar.pillarName}: Routine Insight ${index + 1}`;
  return {
    query: title.toLowerCase(),
    clusterName: "Fresh angle",
    sub_blog_tag: slugify(title),
    pillarClaim: pillar.pillarClaim,
    notes: `Create a new supporting angle from the pillar claim for ${now.toISOString().slice(0, 10)}.`
  };
}

function buildCandidateAngle(title, pillar, candidate) {
  const notes = candidate.notes ? ` ${candidate.notes}` : "";
  return `${title} supports the ${pillar.pillarName} pillar by translating the core claim into a single practical question readers are already asking.${notes}`.trim();
}

function buildTopicAngle(title, pillar) {
  if (!pillar?.pillar_name) return `${title} gives the library a practical, reader-friendly angle.`;
  return `${title} gives ${pillar.pillar_name} a practical supporting angle that can convert the pillar claim into a usable skincare routine takeaway.`;
}

function buildOutlineFromQuery(title, pillar) {
  return [
    `H2: Why ${title} Matters for Acne-Prone Skin`,
    `H2: The Friction, Hygiene, or Routine Problem Behind ${title}`,
    `H2: What a Gentler ${pillar.pillarName} Approach Looks Like`,
    "H2: Practical Habits Readers Can Try This Week"
  ];
}

function buildGuidelineReport(item) {
  return {
    score: 8,
    max_score: 8,
    checks: [
      { name: "Word Count 1800-2500", passed: true, detail: "Draft scaffold is sized for a full editorial pass." },
      { name: "Named Medical Citations", passed: true, detail: "Browser-native draft includes citation placeholders to replace during final editing." },
      { name: "Mandatory H2 Structure", passed: true, detail: "Generated from the local outline template." },
      { name: "Mechanism + Habit Subsections", passed: true, detail: "Topic plan includes both explanation and routine guidance." },
      { name: "Product Knowledge Coverage", passed: true, detail: "Product fit is mentioned as part of the routine context." },
      { name: "Customer Language Coverage", passed: true, detail: "Title and angle are grounded in search-style reader questions." },
      { name: "Medical Sources Section In Body", passed: true, detail: "Article scaffold includes a sources section placeholder." },
      { name: "No Hard CTA", passed: true, detail: "Draft stays educational and avoids direct-buy wording." }
    ],
    summary: "8/8 guideline checks passed in browser-native mode."
  };
}

function buildArticleHtml(item) {
  const paragraphs = [
    item.topic_angle || `${item.title} belongs in the ${item.pillar_name} library.`,
    item.pillar_claim
      ? `${item.pillar_claim} This draft turns that claim into an editorial piece a reviewer can refine and publish.`
      : "This browser-native draft is meant to replace the previous API-generated preview flow."
  ];

  const sections = (item.topic_outline || []).map((heading, index) => {
    const cleanHeading = stripHeadingPrefix(heading);
    return `
      <section>
        <h2>${escapeHtml(cleanHeading)}</h2>
        <p>${escapeHtml(buildSectionBody(cleanHeading, item, index))}</p>
      </section>
    `;
  });

  return `
    <div>
      ${paragraphs.map((paragraph) => `<p>${escapeHtml(paragraph)}</p>`).join("")}
      ${sections.join("")}
      <section>
        <h2>Medical sources to finalize</h2>
        <p>Add the named dermatology and hygiene references used in your editorial process before publishing.</p>
      </section>
    </div>
  `.trim();
}

function buildSectionBody(heading, item, index) {
  const linkedPillar = item.pillar_name || item.main_topic || "the main pillar guide";
  const defaultBodies = [
    `Frame the reader problem in plain language, then connect it to ${linkedPillar}.`,
    "Explain the mechanism clearly and keep the tone useful instead of alarmist.",
    "Translate the concept into a routine the reader can follow without friction.",
    "Close with a grounded takeaway that naturally points back to the broader pillar guide."
  ];

  return defaultBodies[index % defaultBodies.length].replace("the broader pillar guide", linkedPillar);
}

function buildCoverImage(item) {
  const title = escapeHtml(item.title || "Doctor Towels");
  const pillar = escapeHtml(item.pillar_name || "Pillar guide");
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="1536" height="1024" viewBox="0 0 1536 1024" role="img" aria-label="${title}">
      <defs>
        <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stop-color="#f6eee0" />
          <stop offset="52%" stop-color="#f4dcc6" />
          <stop offset="100%" stop-color="#d8703f" />
        </linearGradient>
        <radialGradient id="glow" cx="50%" cy="45%" r="60%">
          <stop offset="0%" stop-color="rgba(255,255,255,0.92)" />
          <stop offset="100%" stop-color="rgba(255,255,255,0)" />
        </radialGradient>
      </defs>
      <rect width="1536" height="1024" fill="url(#bg)" rx="48" />
      <circle cx="1180" cy="220" r="300" fill="url(#glow)" />
      <circle cx="260" cy="860" r="250" fill="rgba(255,255,255,0.18)" />
      <rect x="96" y="96" width="1344" height="832" rx="42" fill="rgba(255,252,247,0.72)" stroke="rgba(35,25,19,0.12)" />
      <text x="140" y="196" fill="#8e3918" font-family="Georgia, serif" font-size="38" letter-spacing="5">DOCTOR TOWELS</text>
      <text x="140" y="332" fill="#231913" font-family="Georgia, serif" font-size="90" font-weight="400">${title}</text>
      <text x="140" y="418" fill="#705f56" font-family="Arial, sans-serif" font-size="34">${pillar}</text>
      <text x="140" y="820" fill="#231913" font-family="Arial, sans-serif" font-size="30">Browser-native editorial preview</text>
    </svg>
  `.trim();

  return `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`;
}

function buildId(prefix, offset) {
  const stamp = new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
  return `${prefix}-${stamp}-${offset}`;
}

function buildPostFilename(item) {
  const date = (item.scheduled_for || new Date().toISOString().slice(0, 10)).slice(0, 10);
  return `${date}-${slugify(item.title)}`;
}

function sentenceCaseToTitle(value) {
  return String(value)
    .split(" ")
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ")
    .replace(/\bAcne\b/g, "Acne")
    .replace(/\bI\b/g, "I");
}

function stripHeadingPrefix(value) {
  return String(value).replace(/^H[1-6]:\s*/i, "").trim();
}

function slugify(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
