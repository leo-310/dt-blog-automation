import { useEffect, useMemo, useState } from "react";
import {
  approveTopic,
  generateCoverImage as generateLocalCoverImage,
  generateTopics as generateLocalTopics,
  loadWorkspace,
  persistWorkspace,
  pushTopic,
  rejectTopic,
  resetWorkspace
} from "./lib/browserData";

const emptyState = {
  loading: false,
  generating: false,
  generatingImage: false,
  runningTransition: false,
  operationMessage: "",
  blogCount: 4,
  ...createWorkspaceState(loadWorkspace()),
  error: "",
  banner: "Browser-native workspace ready. Changes persist in this browser only.",
  retryTask: null
};

export function App() {
  const [state, setState] = useState(() => emptyState);

  useEffect(() => {
    persistWorkspace({
      pillars: state.pillars,
      pipeline: state.pipeline,
      shopifyBlogs: state.shopifyBlogs
    });
  }, [state.pillars, state.pipeline, state.shopifyBlogs]);

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
      const result = await runNativeTask(() =>
        generateLocalTopics(
          {
            pillars: state.pillars,
            pipeline: state.pipeline,
            shopifyBlogs: state.shopifyBlogs,
            clusters: loadWorkspace().clusters
          },
          {
            count,
            role,
            pillarId: selectedPillarId
          }
        )
      );

      setState((current) => ({
        ...current,
        generating: false,
        operationMessage: "",
        pipeline: result.workspace.pipeline,
        selectedId: result.created?.[0]?.id || current.selectedId,
        banner: result.message || "Topics generated.",
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
      const nextWorkspace = await runNativeTask(() => {
        const workspace = {
          pillars: state.pillars,
          pipeline: state.pipeline,
          shopifyBlogs: state.shopifyBlogs,
          clusters: loadWorkspace().clusters
        };

        if (action === "approve") {
          return approveTopic(workspace, { id: selectedItem.id, pillarId: selectedPillarId });
        }
        if (action === "push") {
          return pushTopic(workspace, { id: selectedItem.id, blogId: state.selectedBlogId });
        }
        return rejectTopic(workspace, { id: selectedItem.id });
      });

      const banner =
        action === "approve"
          ? "Topic approved and full blog preview generated locally."
          : action === "push"
            ? "Blog marked as pushed in browser-native mode."
            : "Topic rejected.";
      setState((current) => ({
        ...current,
        runningTransition: false,
        operationMessage: "",
        pipeline: nextWorkspace.pipeline,
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
      const nextWorkspace = await runNativeTask(() =>
        generateLocalCoverImage(
          {
            pillars: state.pillars,
            pipeline: state.pipeline,
            shopifyBlogs: state.shopifyBlogs,
            clusters: loadWorkspace().clusters
          },
          {
            id: selected.id
          }
        )
      );

      setState((current) => ({
        ...current,
        generatingImage: false,
        operationMessage: "",
        pipeline: nextWorkspace.pipeline,
        banner: "Cover image generated locally as an SVG preview.",
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
            <button
              className="secondary-button"
              onClick={() => {
                const workspace = resetWorkspace();
                setState((current) => ({
                  ...current,
                  ...createWorkspaceState(workspace),
                  banner: "Workspace reset to the repo seed data.",
                  error: "",
                  retryTask: null
                }));
              }}
              disabled={controlsDisabled}
            >
              Reset Workspace
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
                    <img src={selected.generatedImageUrl} alt={`${selected.title} cover`} loading="lazy" />
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

function createWorkspaceState(workspace) {
  const selectedPillarId = workspace.pillars[0]?.pillarId || "";
  const selectedId =
    workspace.pipeline.find((item) => item.status !== "rejected" && item.pillar_id === selectedPillarId)?.id || "";

  return {
    pillars: workspace.pillars,
    pipeline: workspace.pipeline,
    selectedPillarId,
    selectedId,
    shopifyBlogs: workspace.shopifyBlogs,
    selectedBlogId: workspace.shopifyBlogs[0]?.id || ""
  };
}

async function runNativeTask(task) {
  await new Promise((resolve) => {
    window.setTimeout(resolve, 180);
  });
  return task();
}
