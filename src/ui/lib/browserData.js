const STORAGE_KEY = "doctor-towels-blog-agent/workspace-v2";

const SHOPIFY_BLOGS = [
  { id: "doctor-towels-journal", title: "Doctor Towels Journal" },
  { id: "skin-education", title: "Skin Education" },
  { id: "product-updates", title: "Product Updates" }
];

const EMPTY_WORKSPACE = {
  pillars: [],
  pipeline: [],
  shopifyBlogs: [...SHOPIFY_BLOGS]
};

const BUILD_TIME_API_BASE = "__VITE_API_BASE_URL__";
const DEFAULT_API_TIMEOUT_MS = 30000;
const NOTION_API_TIMEOUT_MS = 60000;
const LONG_RUNNING_API_TIMEOUT_MS = 240000;

function apiBase() {
  if (typeof window === "undefined") return "";
  const custom = String(window.localStorage.getItem("doctor-towels-blog-agent/api-base") || "").trim();
  return custom;
}

function envApiBase() {
  const value = BUILD_TIME_API_BASE.startsWith("__VITE_API_BASE_URL") ? "" : BUILD_TIME_API_BASE;
  return String(value || "").trim().replace(/\/+$/, "");
}

function inferDefaultApiBase() {
  if (typeof window === "undefined") return "";
  const { protocol, hostname, port } = window.location;
  if (!/^https?:$/i.test(protocol)) return "";
  if (port === "4173" || port === "5173") {
    return `${protocol}//${hostname || "127.0.0.1"}:8124`;
  }
  return "";
}

function resolveApiBaseForAssets() {
  const configuredBase = apiBase();
  if (configuredBase) return configuredBase;
  const envBase = envApiBase();
  if (envBase) return envBase;
  return inferDefaultApiBase();
}

function absolutizeImageUrl(url) {
  const value = String(url || "").trim();
  if (!value) return "";
  if (/^data:/i.test(value)) return value;
  if (/^https?:\/\//i.test(value)) return value;
  const base = resolveApiBaseForAssets();
  if (!base) return value;
  if (value.startsWith("/")) return `${base}${value}`;
  return `${base}/${value}`;
}

function normalizePipelineItems(items) {
  return (Array.isArray(items) ? items : []).map((item) => ({
    ...item,
    generatedImageUrl: absolutizeImageUrl(item?.generatedImageUrl)
  }));
}

export function getCustomApiBase() {
  return apiBase();
}

export function setCustomApiBase(value) {
  if (typeof window === "undefined") return "";
  const normalized = String(value || "").trim().replace(/\/+$/, "");
  if (normalized) {
    window.localStorage.setItem("doctor-towels-blog-agent/api-base", normalized);
    return normalized;
  }
  window.localStorage.removeItem("doctor-towels-blog-agent/api-base");
  return "";
}

function isLongRunningPath(path) {
  const normalized = String(path || "").toLowerCase();
  return (
    normalized === "/api/pipeline" ||
    normalized.startsWith("/api/pipeline?") ||
    normalized.includes("/api/pipeline/generate") ||
    normalized.includes("/api/images/generate") ||
    normalized.includes("/api/pipeline/") ||
    normalized.includes("/approve") ||
    normalized.includes("/push")
  );
}

function requestTimeoutMsForPath(path) {
  const normalized = String(path || "").toLowerCase();
  if (normalized.includes("/api/notion/actions/run")) {
    return NOTION_API_TIMEOUT_MS;
  }
  if (isLongRunningPath(normalized)) {
    return LONG_RUNNING_API_TIMEOUT_MS;
  }
  return DEFAULT_API_TIMEOUT_MS;
}

function buildCandidateUrls(path, configuredBase, envBase, defaultBase) {
  const values = [configuredBase, envBase, defaultBase].filter(Boolean);
  const seen = new Set();
  const candidates = [];
  for (const base of values) {
    const normalized = `${base}${path}`;
    if (seen.has(normalized)) continue;
    seen.add(normalized);
    candidates.push(normalized);
  }
  if (!candidates.length) {
    candidates.push(path);
  }
  return candidates;
}

function describeApiOrigin(url) {
  try {
    return new URL(url, window.location.origin).origin;
  } catch {
    return String(url || "current origin");
  }
}

function normalizeRequestError(error, { url, timeoutMs }) {
  if (!(error instanceof Error)) {
    return new Error("API request failed.");
  }

  const lowerMessage = String(error.message || "").toLowerCase();
  if (error.name === "AbortError") {
    return new Error(
      `API request to ${describeApiOrigin(url)} timed out after ${Math.ceil(timeoutMs / 1000)}s. ` +
        "The API is slow or unavailable. Start API + UI with `npm run dev:full`, then retry."
    );
  }

  if (
    lowerMessage.includes("failed to fetch") ||
    lowerMessage.includes("networkerror") ||
    lowerMessage.includes("connection refused") ||
    lowerMessage.includes("load failed")
  ) {
    return new Error(
      `Cannot reach Blog Agent API at ${describeApiOrigin(url)}. ` +
        "Start API + UI with `npm run dev:full` or update Settings > API Base URL."
    );
  }

  return error;
}

async function request(path, options = {}) {
  const { timeoutMs: timeoutOverride, ...fetchOptions } = options;
  const configuredBase = apiBase();
  const envBase = envApiBase();
  const defaultBase = inferDefaultApiBase();
  const candidates = buildCandidateUrls(path, configuredBase, envBase, defaultBase);
  const timeoutMs = Number(timeoutOverride || requestTimeoutMsForPath(path));
  let lastError = null;

  for (const url of candidates) {
    const controller = new AbortController();
    const timer = window.setTimeout(() => {
      controller.abort();
    }, timeoutMs);
    try {
      const response = await fetch(url, {
        method: "GET",
        headers: { "Content-Type": "application/json" },
        signal: controller.signal,
        ...fetchOptions
      });
      const contentType = String(response.headers.get("content-type") || "").toLowerCase();
      const text = await response.text();
      let payload = {};
      if (text) {
        try {
          payload = JSON.parse(text);
        } catch {
          if (contentType.includes("text/html") || /^\s*</.test(text)) {
            throw new Error(
              "API request returned HTML instead of JSON. Set API Base URL to http://127.0.0.1:8124 in Settings."
            );
          }
          payload = { error: text };
        }
      }
      if (!response.ok) {
        throw new Error(payload.error || `Request failed: ${response.status}`);
      }
      window.clearTimeout(timer);
      return payload;
    } catch (error) {
      lastError = normalizeRequestError(error, { url, timeoutMs });
    } finally {
      window.clearTimeout(timer);
    }
  }

  if (lastError instanceof Error) {
    throw lastError;
  }
  throw new Error("API request failed.");
}

export function loadWorkspace() {
  if (typeof window === "undefined") return EMPTY_WORKSPACE;
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return EMPTY_WORKSPACE;
    const parsed = JSON.parse(raw);
    return {
      pillars: Array.isArray(parsed.pillars) ? parsed.pillars : [],
      pipeline: Array.isArray(parsed.pipeline) ? parsed.pipeline : [],
      shopifyBlogs: Array.isArray(parsed.shopifyBlogs) && parsed.shopifyBlogs.length ? parsed.shopifyBlogs : [...SHOPIFY_BLOGS]
    };
  } catch {
    return EMPTY_WORKSPACE;
  }
}

export function persistWorkspace(workspace) {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        pillars: workspace.pillars || [],
        pipeline: workspace.pipeline || [],
        shopifyBlogs: workspace.shopifyBlogs || SHOPIFY_BLOGS
      })
    );
  } catch {
    // Ignore cache failures.
  }
}

export function resetWorkspace() {
  if (typeof window !== "undefined") {
    window.localStorage.removeItem(STORAGE_KEY);
  }
  return EMPTY_WORKSPACE;
}

export async function fetchWorkspace() {
  const [pipelineResult, pillarsResult, blogsResult] = await Promise.all([
    request("/api/pipeline", { timeoutMs: LONG_RUNNING_API_TIMEOUT_MS }),
    request("/api/pillars"),
    request("/api/shopify/blogs").catch(() => ({ blogs: SHOPIFY_BLOGS }))
  ]);
  return {
    pillars: Array.isArray(pillarsResult.pillars) ? pillarsResult.pillars : [],
    pipeline: normalizePipelineItems(pipelineResult.pipeline),
    shopifyBlogs: Array.isArray(blogsResult.blogs) && blogsResult.blogs.length ? blogsResult.blogs : [...SHOPIFY_BLOGS]
  };
}

export async function fetchSettings() {
  const result = await request("/api/settings");
  return result.settings || {};
}

export async function saveSettings(settings) {
  const result = await request("/api/settings", {
    method: "PUT",
    body: JSON.stringify({ settings })
  });
  return result.settings || {};
}

export async function setupNotion({ parentPageId, overwriteExisting = false }) {
  return request("/api/notion/setup", {
    method: "POST",
    body: JSON.stringify({
      parentPageId: String(parentPageId || "").trim(),
      overwriteExisting: Boolean(overwriteExisting)
    })
  });
}

export async function runAutomationNow() {
  return request("/api/automation/run-now", {
    method: "POST",
    body: JSON.stringify({})
  });
}

export async function syncNotion() {
  return request("/api/notion/sync", {
    method: "POST",
    body: JSON.stringify({})
  });
}

export async function runNotionActions() {
  return request("/api/notion/actions/run", {
    method: "POST",
    body: JSON.stringify({})
  });
}

export async function notionState() {
  return request("/api/notion/state");
}

export async function generateTopics(workspace, { pillarId, role, count = 1, keywords = [] }) {
  const payload = {
    count: Number(count || 1),
    role: role || "side",
    pillarId: pillarId || "",
    keywords: Array.isArray(keywords) ? keywords : []
  };
  const result = await request("/api/pipeline/generate", {
    method: "POST",
    body: JSON.stringify(payload)
  });
  const serverPipeline = normalizePipelineItems(result.pipeline);
  const serverCreated = normalizePipelineItems(result.created);
  let refreshed = await fetchWorkspace().catch(() => ({
    pillars: Array.isArray(workspace.pillars) ? workspace.pillars : [],
    pipeline: [],
    shopifyBlogs: Array.isArray(workspace.shopifyBlogs) ? workspace.shopifyBlogs : [...SHOPIFY_BLOGS]
  }));

  if (!Array.isArray(refreshed.pillars) || !refreshed.pillars.length) {
    refreshed = {
      ...refreshed,
      pillars: Array.isArray(workspace.pillars) ? workspace.pillars : []
    };
  }
  if (!Array.isArray(refreshed.pipeline) || !refreshed.pipeline.length) {
    const fallbackPipeline =
      serverPipeline.length
        ? serverPipeline
        : [...serverCreated, ...(Array.isArray(workspace.pipeline) ? workspace.pipeline : [])];
    refreshed = {
      ...refreshed,
      pipeline: normalizePipelineItems(fallbackPipeline)
    };
  }

  return {
    workspace: refreshed,
    created: serverCreated.length
      ? serverCreated
      : refreshed.pipeline.slice(0, Math.max(1, payload.count)),
    message: String(result.message || "Topics generated.")
  };
}

export async function approveTopic(workspace, { id, pillarId }) {
  await request(`/api/pipeline/${encodeURIComponent(id)}/approve`, {
    method: "POST",
    body: JSON.stringify({ pillarId })
  });
  return fetchWorkspace();
}

export async function rejectTopic(workspace, { id }) {
  await request(`/api/pipeline/${encodeURIComponent(id)}/reject`, {
    method: "POST",
    body: JSON.stringify({})
  });
  return fetchWorkspace();
}

export async function pushTopic(workspace, { id, blogId }) {
  await request(`/api/pipeline/${encodeURIComponent(id)}/push`, {
    method: "POST",
    body: JSON.stringify({ blogId })
  });
  return fetchWorkspace();
}

export async function generateCoverImage(workspace, { id, prompt = "" }) {
  await request("/api/images/generate", {
    method: "POST",
    body: JSON.stringify({ pipelineId: id, prompt: String(prompt || "") })
  });
  return fetchWorkspace();
}
