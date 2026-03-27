import { useEffect, useMemo, useState } from "react";

const API_BASE_URL = resolveApiBaseUrl();

const emptyState = {
  loading: true,
  generating: false,
  generatingImage: false,
  runningTransition: false,
  operationMessage: "",
  blogCount: 4,
  pipeline: [],
  pillars: [],
  selectedPillarId: "",
  selectedId: "",
  shopifyBlogs: [],
  selectedBlogId: "",
  error: "",
  banner: "",
  retryTask: null
};

export function App() {
  const [state, setState] = useState(emptyState);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      const [pipelineResult, pillarsResult, blogsResult] = await Promise.all([
        fetchJsonSafe("/api/pipeline"),
        fetchJsonSafe("/api/pillars"),
        fetchJsonSafe("/api/shopify/blogs")
      ]);

      if (cancelled) return;
      if (!pipelineResult.ok) {
        setState((current) => ({
          ...current,
          loading: false,
          error: normalizeApiError(
            pipelineResult.status,
            pipelineResult.data?.error || pipelineResult.error,
            "Unable to load pipeline."
          )
        }));
        return;
      }

      const pillars = pillarsResult.ok ? pillarsResult.data.pillars || [] : [];
      const selectedPillarId = pillars[0]?.pillarId || "";
      const pipeline = pipelineResult.data.pipeline || [];
      const firstForPillar =
        pipeline.find((item) => !item.status?.includes("rejected") && item.pillar_id === selectedPillarId) || null;

      const shopifyBlogs = blogsResult.ok ? blogsResult.data.blogs || [] : [];

      setState((current) => ({
        ...current,
        loading: false,
        pillars,
        pipeline,
        selectedPillarId: current.selectedPillarId || selectedPillarId,
        selectedId: current.selectedId || firstForPillar?.id || "",
        shopifyBlogs,
        selectedBlogId: current.selectedBlogId || shopifyBlogs[0]?.id || "",
        error: blogsResult.ok ? "" : normalizeApiError(blogsResult.status, blogsResult.data?.error, "")
      }));
    }

    load();
    return () => {
      cancelled = true;
    };
  }, []);

  const activePillar = useMemo(
    () => state.pillars.find((pillar) => pillar.pillarId === state.selectedPillarId) || state.pillars[0],
    [state.pillars, state.selectedPillarId]
  );

  const selectedPillarId = activePillar?.pillarId || "";
  const visible = useMemo(
    () => state.pipeline.filter((item) => item.status !== "rejected" && item.pillar_id === selectedPillarId),
    [state.pipeline, selectedPillarId]
  );
  const mainItems = visible.filter(
    (item) => item.pillar_id === selectedPillarId && item.topic_role === "main"
  );
  const sideItems = visible.filter(
    (item) => item.pillar_id === selectedPillarId && item.topic_role !== "main"
  );
  const selected =
    visible.find((item) => item.id === state.selectedId) ||
    mainItems[0] ||
    sideItems[0] ||
    null;
  const controlsDisabled = state.generating || state.runningTransition || state.generatingImage;

  async function refreshPipeline() {
    const result = await fetchJsonSafe("/api/pipeline");
    if (!result.ok) {
      throw new Error(
        normalizeApiError(result.status, result.data?.error || result.error, "Unable to refresh pipeline.")
      );
    }
    const nextPipeline = result.data.pipeline || [];
    setState((current) => ({
      ...current,
      pipeline: nextPipeline,
      selectedId: nextPipeline.some((item) => item.id === current.selectedId)
        ? current.selectedId
        : nextPipeline.find((item) => item.status !== "rejected" && item.pillar_id === current.selectedPillarId)
            ?.id || ""
    }));
  }

  async function generateTopics(role) {
    setState((current) => ({
      ...current,
      generating: true,
      operationMessage:
        role === "main" ? "Generating a fresh main blog topic..." : "Generating supporting topic ideas...",
      error: "",
      banner: "",
      retryTask: null
    }));
    try {
      const count = role === "main" ? 1 : state.blogCount;
      const result = await fetchJsonSafe("/api/pipeline/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          count,
          role,
          pillarId: selectedPillarId
        })
      });
      if (!result.ok) {
        throw new Error(normalizeApiError(result.status, result.data?.error || result.error, "Unable to generate topics."));
      }
      setState((current) => ({
        ...current,
        generating: false,
        operationMessage: "",
        pipeline: result.data.pipeline,
        selectedId: result.data.created?.[0]?.id || current.selectedId,
        banner: result.data.message || "Topics generated.",
        retryTask: null
      }));
    } catch (error) {
      setState((current) => ({
        ...current,
        generating: false,
        operationMessage: "",
        retryTask: { kind: "generate-topics", role },
        error: error instanceof Error ? error.message : "Unable to generate topics."
      }));
    }
  }

  async function handleTransition(action, selectedOverrideId = "") {
    const selectedItem = selectedOverrideId
      ? state.pipeline.find((item) => item.id === selectedOverrideId) || null
      : selected;
    if (!selectedItem) return;
    setState((current) => ({
      ...current,
      runningTransition: true,
      operationMessage:
        action === "approve"
          ? "Generating full blog draft..."
          : action === "push"
            ? "Pushing approved blog live to Shopify..."
            : "Updating topic status...",
      error: "",
      banner: "",
      retryTask: null
    }));
    try {
      const payload = action === "approve" || action === "push"
        ? {
            pillarId: selectedPillarId,
            ...(action === "push" ? { blogId: state.selectedBlogId } : {})
          }
        : undefined;
      const result = await fetchJsonSafe(`/api/pipeline/${selectedItem.id}/${action}`, {
        method: "POST",
        headers: payload ? { "Content-Type": "application/json" } : undefined,
        body: payload ? JSON.stringify(payload) : undefined
      });
      if (!result.ok) {
        throw new Error(normalizeApiError(result.status, result.data?.error || result.error, "Action failed."));
      }
      await refreshPipeline();
      const banner =
        action === "approve"
          ? "Topic approved and full blog generated."
          : action === "push"
            ? "Blog published live to Shopify."
            : "Topic rejected.";
      setState((current) => ({
        ...current,
        runningTransition: false,
        operationMessage: "",
        retryTask: null,
        banner
      }));
    } catch (error) {
      setState((current) => ({
        ...current,
        runningTransition: false,
        operationMessage: "",
        retryTask: action === "approve" ? { kind: "transition", action, selectedId: selectedItem.id } : null,
        error: error instanceof Error ? error.message : "Action failed."
      }));
    }
  }

  async function generateCoverImage() {
    if (!selected) return;
    setState((current) => ({
      ...current,
      generatingImage: true,
      operationMessage: "Generating landscape blog image...",
      error: "",
      banner: "",
      retryTask: null
    }));
    try {
      const result = await fetchJsonSafe("/api/images/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          pipelineId: selected.id
        })
      });
      if (!result.ok) {
        throw new Error(normalizeApiError(result.status, result.data?.error || result.error, "Unable to generate image."));
      }
      await refreshPipeline();
      setState((current) => ({
        ...current,
        generatingImage: false,
        operationMessage: "",
        banner: "Cover image generated with gpt-image-1.5 (low, landscape).",
        retryTask: null
      }));
    } catch (error) {
      setState((current) => ({
        ...current,
        generatingImage: false,
        operationMessage: "",
        error: error instanceof Error ? error.message : "Unable to generate image."
      }));
    }
  }

  function retryLastAction() {
    const retryTask = state.retryTask;
    if (!retryTask) return;
    if (retryTask.kind === "generate-topics") {
      generateTopics(retryTask.role);
      return;
    }
    if (retryTask.kind === "transition") {
      setState((current) => ({ ...current, selectedId: retryTask.selectedId }));
      handleTransition(retryTask.action, retryTask.selectedId);
    }
  }

  if (state.loading) {
    return (
      <div className="app-shell">
        <section className="panel pillar-library loading-panel" aria-live="polite" aria-busy="true">
          <div className="spinner" />
          <h2>Preparing your blog workspace</h2>
          <p>Loading pillars, pipeline topics, and Shopify destinations.</p>
        </section>
      </div>
    );
  }

  return (
    <div className="app-shell">
      <section className="panel pillar-library">
        <div className="pillar-library-top">
          <div>
            <span className="section-heading">Main Pillar Blogs</span>
            <h2>Pillar Blog Library</h2>
            <p>Pick a pillar, generate topics, approve one, then push live from this area.</p>
          </div>
          <div className="hero-actions">
            <label className="weeks-field" htmlFor="blog-count">
              Blogs
              <input
                id="blog-count"
                type="number"
                min={1}
                max={12}
                value={state.blogCount}
                onChange={(event) =>
                  setState((current) => ({
                    ...current,
                    blogCount: Number(event.target.value || 4)
                  }))
                }
              />
            </label>
            <button className="secondary-button" onClick={() => generateTopics("main")} disabled={controlsDisabled}>
              Generate Main Blog
            </button>
            <button className="primary-button" onClick={() => generateTopics("side")} disabled={controlsDisabled}>
              {state.generating ? "Generating..." : "Generate Topics"}
            </button>
          </div>
        </div>

        <div className="pillar-strip">
          {state.pillars.map((pillar) => (
            <button
              key={pillar.pillarId}
              className={`pillar-nav-card ${pillar.pillarId === selectedPillarId ? "active" : ""}`}
              disabled={controlsDisabled}
              onClick={() =>
                setState((current) => ({
                  ...current,
                  selectedPillarId: pillar.pillarId,
                  selectedId:
                    current.pipeline.find(
                      (item) => item.pillar_id === pillar.pillarId && item.status !== "rejected"
                    )?.id || ""
                }))
              }
            >
              <span className="pillar-label">{pillar.pillarId?.replace("pillar-", "Pillar ") || "Pillar"}</span>
              <strong>{pillar.pillarName}</strong>
            </button>
          ))}
        </div>

        {state.banner ? <div className="banner success">{state.banner}</div> : null}
        {state.error ? (
          <div className={`banner error ${state.retryTask ? "actionable" : ""}`}>
            <span>{state.error}</span>
            {state.retryTask ? (
              <button className="retry-button" onClick={retryLastAction} disabled={controlsDisabled}>
                Retry
              </button>
            ) : null}
          </div>
        ) : null}
        {state.operationMessage ? (
          <div className="banner pending" aria-live="polite" aria-busy="true">
            <span className="spinner small" />
            <span>{state.operationMessage}</span>
          </div>
        ) : null}

        <div className="pillar-layout">
          <aside className="pillar-queue">
            <div className="section-heading">Main Blog</div>
            {mainItems.map((item) => (
              <QueueCard
                key={item.id}
                item={item}
                active={item.id === selected?.id}
                disabled={controlsDisabled}
                onPick={(id) => setState((c) => ({ ...c, selectedId: id }))}
              />
            ))}
            <div className="section-heading">Sub Blogs</div>
            {sideItems.map((item) => (
              <QueueCard
                key={item.id}
                item={item}
                active={item.id === selected?.id}
                disabled={controlsDisabled}
                onPick={(id) => setState((c) => ({ ...c, selectedId: id }))}
              />
            ))}
          </aside>

          <section className="panel article-panel">
            {!selected ? (
              <div className="empty-box">Generate a topic and pick it to review.</div>
            ) : (
              <>
                <div className="article-header">
                  <h2>{selected.title}</h2>
                  <p>{selected.topic_angle || selected.description || selected.excerpt}</p>
                </div>
                <div className="meta-grid">
                  <MetaCard label="Pillar" value={selected.pillar_name || selected.cluster} />
                  <MetaCard label="Main Topic" value={selected.main_topic || "n/a"} />
                  <MetaCard label="Tag" value={selected.sub_blog_tag || "n/a"} />
                  <MetaCard label="Status" value={selected.status} />
                </div>
                <div className="hero-actions review-actions">
                  <button className="secondary-button" onClick={generateCoverImage} disabled={controlsDisabled}>
                    {state.generatingImage ? "Generating Image..." : "Generate Cover Image"}
                  </button>
                  <button className="secondary-button" onClick={() => handleTransition("reject")} disabled={controlsDisabled}>
                    Reject
                  </button>
                  {selected.status !== "approved" && selected.status !== "pushed" ? (
                    <button className="primary-button" onClick={() => handleTransition("approve")} disabled={controlsDisabled}>
                      {state.runningTransition ? "Working..." : "Approve"}
                    </button>
                  ) : null}
                  {selected.status === "approved" || selected.status === "pushed" ? (
                    <>
                      <label className="weeks-field" htmlFor="shopify-blog-inline">
                        Shopify Blog
                        <select
                          id="shopify-blog-inline"
                          value={state.selectedBlogId}
                          disabled={controlsDisabled}
                          onChange={(event) =>
                            setState((current) => ({
                              ...current,
                              selectedBlogId: event.target.value
                            }))
                          }
                        >
                          {state.shopifyBlogs.map((blog) => (
                            <option key={blog.id} value={blog.id}>
                              {blog.title}
                            </option>
                          ))}
                        </select>
                      </label>
                      <button
                        className="primary-button"
                        onClick={() => handleTransition("push")}
                        disabled={controlsDisabled || selected.status !== "approved" || !state.selectedBlogId}
                      >
                        Push
                      </button>
                    </>
                  ) : null}
                </div>

                {selected.generatedImageUrl ? (
                  <figure className="generated-image-block">
                    <img src={resolveApiUrl(selected.generatedImageUrl)} alt={`${selected.title} cover`} loading="lazy" />
                  </figure>
                ) : null}

                {selected.status === "topic" ? (
                  <div className="guideline-block">
                    <div className="section-heading">Content Plan</div>
                    <ul>
                      {(selected.topic_outline || []).map((point, index) => (
                        <li key={`${selected.id}-outline-${index}`}>{point}</li>
                      ))}
                    </ul>
                  </div>
                ) : (
                  <>
                    {selected.guideline_report ? (
                      <div className="guideline-block">
                        <div className="section-heading">Guideline Checks</div>
                        <p>{selected.guideline_report.summary}</p>
                      </div>
                    ) : null}
                    <article className="article-body" dangerouslySetInnerHTML={{ __html: selected.html }} />
                  </>
                )}
              </>
            )}
          </section>
        </div>
      </section>
    </div>
  );
}

function QueueCard({ item, active, onPick, disabled = false }) {
  return (
    <button className={`post-card ${active ? "active" : ""}`} onClick={() => onPick(item.id)} disabled={disabled}>
      <strong>{item.title}</strong>
      <span>{item.query}</span>
      <span className={`status-pill ${item.status}`}>{item.status}</span>
    </button>
  );
}

function MetaCard({ label, value }) {
  return (
    <section className="meta-card">
      <small>{label}</small>
      <span>{value}</span>
    </section>
  );
}

function normalizeApiError(status, raw, fallback) {
  if (status === 0) {
    const isLocalPage =
      typeof window !== "undefined" &&
      (window.location.hostname === "127.0.0.1" || window.location.hostname === "localhost");
    const base = API_BASE_URL || (typeof window !== "undefined" ? window.location.origin : "");
    const apiTarget = base || "the configured API";
    if (isLocalPage) {
      return `Cannot reach API at ${apiTarget}. Start it with \`source .venv/bin/activate && blog-agent-api\`, then retry.`;
    }
    return `Cannot reach API at ${apiTarget}. Set \`VITE_API_BASE_URL\` to your deployed API URL if your API is hosted separately.`;
  }
  if (status === 404 || raw === "Not found") {
    return "API route not found. Restart `blog-agent-api`.";
  }
  return raw || fallback;
}

async function fetchJsonSafe(url, options) {
  try {
    const response = await fetch(resolveApiUrl(url), options);
    const text = await response.text();
    let data = {};
    if (text.trim()) {
      try {
        data = JSON.parse(text);
      } catch {
        data = {};
      }
    }
    const error = !response.ok && !data.error ? text.trim() : "";
    return { ok: response.ok, status: response.status, data, error };
  } catch (error) {
    const message = error instanceof Error ? error.message.trim() : "";
    return {
      ok: false,
      status: 0,
      data: {},
      error: message || "Network error"
    };
  }
}

function resolveApiBaseUrl() {
  const configured = (import.meta.env.VITE_API_BASE_URL || "").trim();
  if (configured) {
    return configured.replace(/\/+$/, "");
  }
  if (typeof window === "undefined") return "";
  const { hostname, port } = window.location;
  const runningInKnownProxyDevHost =
    (hostname === "127.0.0.1" || hostname === "localhost") && port === "4173";
  if (runningInKnownProxyDevHost) return "";

  const runningLocallyWithoutProxy = hostname === "127.0.0.1" || hostname === "localhost";
  return runningLocallyWithoutProxy ? "http://127.0.0.1:8124" : "";
}

function resolveApiUrl(path) {
  if (!path) return path;
  if (/^https?:\/\//i.test(path)) return path;
  if (!path.startsWith("/")) return path;
  return `${API_BASE_URL}${path}`;
}
