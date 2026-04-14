import { useEffect, useMemo, useState } from "react";
import {
  approveTopic,
  fetchSettings,
  fetchWorkspace,
  getCustomApiBase,
  generateCoverImage as generateLocalCoverImage,
  generateTopics as generateLocalTopics,
  loadWorkspace,
  notionState,
  persistWorkspace,
  pushTopic,
  rejectTopic,
  resetWorkspace,
  runAutomationNow,
  runNotionActions,
  saveSettings,
  setCustomApiBase,
  setupNotion,
  syncNotion
} from "./lib/browserData";

const emptyState = {
  loading: true,
  generating: false,
  generatingImage: false,
  runningTransition: false,
  operationMessage: "",
  blogCount: 4,
  ...createWorkspaceState(loadWorkspace()),
  settingsOpen: false,
  settingsLoading: false,
  settingsSaving: false,
  notionBusy: false,
  notionSyncing: false,
  notionConfigured: false,
  notionEnabled: false,
  notionDiagnostics: null,
  apiBaseInput: getCustomApiBase(),
  notionSetupParentPageId: "",
  settings: {
    enabled: true,
    dailyTime: "09:00",
    timezone: "Asia/Kolkata",
    runNow: false,
    lastRunAt: "",
    nextRunAt: "",
    notionLinks: { pillars: "", blogs: "", settings: "" }
  },
  error: "",
  banner: "",
  retryTask: null,
  keywordModalOpen: false,
  keywordRole: "side",
  keywordInput: "",
  imageModalOpen: false,
  imagePromptInput: ""
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

  useEffect(() => {
    void initializeWorkspace();
  }, []);

  useEffect(() => {
    const message = String(state.error || "").toLowerCase();
    const shouldRetry =
      message.includes("failed to fetch") ||
      message.includes("api request") ||
      message.includes("connection refused") ||
      message.includes("cannot reach blog agent api") ||
      message.includes("timed out");
    if (!shouldRetry) return;
    const timer = window.setTimeout(() => {
      void initializeWorkspace();
    }, 8000);
    return () => window.clearTimeout(timer);
  }, [state.error]);

  useEffect(() => {
    if (!state.notionEnabled || !state.notionConfigured) return;

    let cancelled = false;
    let inFlight = false;

    const pollNotionActions = async () => {
      if (cancelled || inFlight) return;
      if (typeof document !== "undefined" && document.visibilityState !== "visible") return;

      let shouldRun = false;
      setState((current) => {
        const blocked =
          current.loading ||
          current.generating ||
          current.runningTransition ||
          current.generatingImage ||
          current.notionBusy;
        shouldRun = !blocked;
        return current;
      });

      if (!shouldRun) return;

      inFlight = true;
      try {
        const result = await runNativeTask(() => runNotionActions());
        if (cancelled) return;
        const processed = Number(result?.processed || 0);
        const errors = Number(result?.errors || 0);
        if (processed > 0 || errors > 0) {
          await refreshWorkspace(
            errors > 0
              ? `Notion actions processed with ${errors} error(s).`
              : `Notion actions processed (${processed} update${processed === 1 ? "" : "s"}).`
          );
        }
      } catch {
        // keep quiet in background polling
      } finally {
        inFlight = false;
      }
    };

    const kickoff = window.setTimeout(() => {
      void pollNotionActions();
    }, 12000);
    const interval = window.setInterval(() => {
      void pollNotionActions();
    }, 45000);

    return () => {
      cancelled = true;
      window.clearTimeout(kickoff);
      window.clearInterval(interval);
    };
  }, [state.notionEnabled, state.notionConfigured]);

  useEffect(() => {
    let cancelled = false;
    let inFlight = false;

    const refreshFromServer = async () => {
      if (cancelled || inFlight) return;
      if (typeof document !== "undefined" && document.visibilityState !== "visible") return;

      let shouldRun = false;
      setState((current) => {
        const blocked =
          current.loading ||
          current.generating ||
          current.runningTransition ||
          current.generatingImage ||
          current.notionBusy ||
          current.notionSyncing;
        shouldRun = !blocked;
        return current;
      });
      if (!shouldRun) return;

      inFlight = true;
      try {
        await refreshWorkspace();
      } catch {
        // keep quiet for periodic background refresh
      } finally {
        inFlight = false;
      }
    };

    const interval = window.setInterval(() => {
      void refreshFromServer();
    }, 60000);

    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, []);

  const activePillar = useMemo(
    () => state.pillars.find((pillar) => pillar.pillarId === state.selectedPillarId) || state.pillars[0],
    [state.pillars, state.selectedPillarId]
  );

  const selectedPillarId = activePillar?.pillarId || "";
  const visible = useMemo(() => {
    const candidates = state.pipeline.filter((item) => item.status !== "rejected");
    if (!selectedPillarId) return candidates;
    const matches = candidates.filter((item) => item.pillar_id === selectedPillarId);
    return matches.length ? matches : candidates;
  }, [state.pipeline, selectedPillarId]);
  const mainItems = visible.filter((item) => item.topic_role === "main");
  const sideItems = visible.filter((item) => item.topic_role !== "main");
  const selected =
    visible.find((item) => item.id === state.selectedId) ||
    mainItems[0] ||
    sideItems[0] ||
    null;
  const selectedNotionRowUrl = getItemNotionRowUrl(selected);
  const notionPipelineUrl = String(state.settings?.notionLinks?.blogs || "").trim();
  const controlsDisabled = state.loading || state.generating || state.runningTransition || state.generatingImage;

  async function initializeWorkspace() {
    setState((current) => ({
      ...current,
      loading: true,
      error: "",
      banner: "",
      operationMessage: "Loading workspace..."
    }));
    try {
      const [workspace, settings, nState] = await Promise.all([
        runNativeTask(() => fetchWorkspace()),
        runNativeTask(() => fetchSettings()),
        runNativeTask(() => notionState())
      ]);
      setState((current) => ({
        ...current,
        ...createWorkspaceState(workspace),
        loading: false,
        operationMessage: "",
        settings,
        notionEnabled: Boolean(nState.enabled),
        notionConfigured: Boolean(nState.configured),
        notionDiagnostics: nState.diagnostics || null,
        banner: "API-connected workspace loaded.",
        retryTask: null
      }));
    } catch (error) {
      setState((current) => ({
        ...current,
        loading: false,
        operationMessage: "",
        error: error instanceof Error ? error.message : "Unable to load workspace."
      }));
    }
  }

  async function refreshWorkspace(message = "") {
    const [workspace, settings, nState] = await Promise.all([fetchWorkspace(), fetchSettings(), notionState()]);
    setState((current) => ({
      ...current,
      ...createWorkspaceState(workspace),
      settings,
      notionEnabled: Boolean(nState.enabled),
      notionConfigured: Boolean(nState.configured),
      notionDiagnostics: nState.diagnostics || null,
      banner: message || current.banner
    }));
  }

  function openKeywordModal(role) {
    setState((current) => ({
      ...current,
      keywordModalOpen: true,
      keywordRole: role === "main" ? "main" : "side",
      keywordInput: ""
    }));
  }

  function closeKeywordModal() {
    setState((current) => ({
      ...current,
      keywordModalOpen: false,
      keywordInput: ""
    }));
  }

  function parseKeywordInput(value) {
    return String(value || "")
      .split(/\r?\n|,/g)
      .map((entry) => entry.trim())
      .filter(Boolean);
  }

  async function generateTopics(role, keywordInput = "") {
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
      const keywords = parseKeywordInput(keywordInput);
      const generated = await runNativeTask(() =>
        generateLocalTopics(
          {
            pillars: state.pillars,
            pipeline: state.pipeline,
            shopifyBlogs: state.shopifyBlogs
          },
          {
            count,
            role,
            pillarId: selectedPillarId,
            keywords
          }
        )
      );
      const notionSuffix =
        state.notionEnabled && state.notionConfigured
          ? role === "main"
            ? " Main topic row synced to Notion (status: topic)."
            : " Topic rows synced to Notion (status: topic)."
          : "";
      setState((current) => ({
        ...current,
        ...createWorkspaceState(generated.workspace),
        generating: false,
        operationMessage: "",
        banner: `${generated.message || "Topics generated."}${notionSuffix}`,
        keywordModalOpen: false,
        keywordInput: "",
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
      await runNativeTask(() => {
        const workspace = {
          pillars: state.pillars,
          pipeline: state.pipeline,
          shopifyBlogs: state.shopifyBlogs
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
          ? state.notionEnabled && state.notionConfigured
            ? "Topic approved, full blog preview generated, and synced to Notion."
            : "Topic approved and full blog preview generated."
          : action === "push"
            ? state.notionEnabled && state.notionConfigured
              ? "Blog marked as pushed and synced to Notion."
              : "Blog marked as pushed."
            : "Topic rejected.";

      await refreshWorkspace(banner);
      setState((current) => ({
        ...current,
        runningTransition: false,
        operationMessage: "",
        retryTask: null
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

  function openImageModal() {
    if (!selected) return;
    setState((current) => ({
      ...current,
      imageModalOpen: true,
      imagePromptInput: ""
    }));
  }

  function closeImageModal() {
    setState((current) => ({
      ...current,
      imageModalOpen: false,
      imagePromptInput: ""
    }));
  }

  async function generateCoverImage(prompt = "") {
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
      await runNativeTask(() =>
        generateLocalCoverImage(
          {
            pillars: state.pillars,
            pipeline: state.pipeline,
            shopifyBlogs: state.shopifyBlogs
          },
          {
            id: selected.id,
            prompt
          }
        )
      );
      await refreshWorkspace("Cover image generated.");
      setState((current) => ({
        ...current,
        generatingImage: false,
        operationMessage: "",
        imageModalOpen: false,
        imagePromptInput: "",
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

  async function handleSettingsSave() {
    setState((current) => ({ ...current, settingsSaving: true, error: "", banner: "" }));
    try {
      const updated = await runNativeTask(() => saveSettings(state.settings));
      setState((current) => ({
        ...current,
        settings: updated,
        settingsSaving: false,
        banner: "Automation settings saved."
      }));
    } catch (error) {
      setState((current) => ({
        ...current,
        settingsSaving: false,
        error: error instanceof Error ? error.message : "Unable to save settings."
      }));
    }
  }

  async function handleNotionSetup() {
    setState((current) => ({
      ...current,
      notionBusy: true,
      error: "",
      banner: ""
    }));
    try {
      await runNativeTask(() =>
        setupNotion({
          parentPageId: state.notionSetupParentPageId,
          overwriteExisting: true
        })
      );
      await refreshWorkspace("Notion databases connected.");
      setState((current) => ({
        ...current,
        notionBusy: false
      }));
    } catch (error) {
      setState((current) => ({
        ...current,
        notionBusy: false,
        error: error instanceof Error ? error.message : "Unable to connect Notion."
      }));
    }
  }

  async function handleApiBaseSave() {
    const value = setCustomApiBase(state.apiBaseInput);
    setState((current) => ({
      ...current,
      apiBaseInput: value,
      banner: value ? `API base set to ${value}` : "API base reset to current origin.",
      error: ""
    }));
    await initializeWorkspace();
  }

  async function handleRunNow() {
    setState((current) => ({
      ...current,
      notionBusy: true,
      operationMessage: "Running daily automation now...",
      error: "",
      banner: ""
    }));
    try {
      await runNativeTask(() => runAutomationNow());
      await refreshWorkspace("Daily automation run completed.");
      setState((current) => ({
        ...current,
        notionBusy: false,
        operationMessage: ""
      }));
    } catch (error) {
      setState((current) => ({
        ...current,
        notionBusy: false,
        operationMessage: "",
        error: error instanceof Error ? error.message : "Unable to run automation."
      }));
    }
  }

  async function handleSyncNow() {
    setState((current) => ({
      ...current,
      notionBusy: true,
      operationMessage: "Syncing pipeline to Notion...",
      error: "",
      banner: ""
    }));
    try {
      await runNativeTask(() => syncNotion());
      await refreshWorkspace("Pipeline synced to Notion.");
      setState((current) => ({
        ...current,
        notionBusy: false,
        operationMessage: ""
      }));
    } catch (error) {
      setState((current) => ({
        ...current,
        notionBusy: false,
        operationMessage: "",
        error: error instanceof Error ? error.message : "Unable to sync Notion."
      }));
    }
  }

  async function handleProcessNotionActionsNow() {
    setState((current) => ({
      ...current,
      notionBusy: true,
      notionSyncing: true,
      operationMessage: "Processing Notion actions...",
      error: "",
      banner: ""
    }));
    try {
      const result = await runNativeTask(() => runNotionActions());
      const processed = Number(result?.processed || 0);
      const errors = Number(result?.errors || 0);
      await refreshWorkspace(
        errors > 0
          ? `Processed Notion actions with ${errors} error(s).`
          : processed > 0
            ? `Processed ${processed} Notion action${processed === 1 ? "" : "s"}.`
            : "No pending Notion actions."
      );
      setState((current) => ({
        ...current,
        notionBusy: false,
        notionSyncing: false,
        operationMessage: ""
      }));
    } catch (error) {
      setState((current) => ({
        ...current,
        notionBusy: false,
        notionSyncing: false,
        operationMessage: "",
        error: error instanceof Error ? error.message : "Unable to process Notion actions."
      }));
    }
  }

  function retryLastAction() {
    const retryTask = state.retryTask;
    if (!retryTask) return;
    if (retryTask.kind === "generate-topics") {
      generateTopics(retryTask.role, state.keywordInput);
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
            <button
              className="secondary-button settings-button"
              disabled={controlsDisabled}
              onClick={() => setState((current) => ({ ...current, settingsOpen: true }))}
              aria-label="Open settings"
              title="Settings"
            >
              ⚙ Settings
            </button>
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
            <button className="secondary-button" onClick={() => openKeywordModal("main")} disabled={controlsDisabled}>
              Generate Main Topic
            </button>
            <button className="primary-button" onClick={() => openKeywordModal("side")} disabled={controlsDisabled}>
              {state.generating ? "Generating..." : "Generate Topic Ideas"}
            </button>
            <button
              className="secondary-button"
              onClick={() => {
                resetWorkspace();
                void initializeWorkspace();
              }}
              disabled={controlsDisabled}
            >
              Refresh Workspace
            </button>
          </div>
        </div>

        <div className="pillar-strip">
          {state.pillars.length ? (
            state.pillars.map((pillar) => (
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
            ))
          ) : (
            <div className="empty-pillars">No pillars loaded yet. Open Settings and click Sync Now.</div>
          )}
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
        {state.notionSyncing ? (
          <div className="banner pending" aria-live="polite" aria-busy="true">
            <span className="spinner small" />
            <span>Syncing Notion actions...</span>
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
                  <MetaCard label="Hierarchy" value={formatHierarchyLabel(selected)} />
                  <MetaCard label="Reports To" value={selected.reports_to_main_title || "n/a"} />
                  <MetaCard label="Status" value={selected.status} />
                  <MetaCard label="Generated At" value={formatTimestamp(selected.created_at)} />
                  <MetaCard label="Approved At" value={formatTimestamp(selected.approved_at)} />
                  <MetaCard label="Pushed At" value={formatTimestamp(selected.pushed_at)} />
                </div>
                {state.notionEnabled && state.notionConfigured ? (
                  <div className="notion-sync-strip">
                    <span>{notionSyncHint(selected)}</span>
                    {selectedNotionRowUrl ? (
                      <a href={selectedNotionRowUrl} target="_blank" rel="noreferrer">
                        Open Notion Row
                      </a>
                    ) : null}
                    {notionPipelineUrl ? (
                      <a href={notionPipelineUrl} target="_blank" rel="noreferrer">
                        Open Blog Pipeline DB
                      </a>
                    ) : null}
                  </div>
                ) : null}
                <div className="hero-actions review-actions">
                  {selected.status === "approved" || selected.status === "pushed" ? (
                    <>
                      <button className="secondary-button" onClick={openImageModal} disabled={controlsDisabled}>
                        {state.generatingImage
                          ? "Generating Image..."
                          : selected.generatedImageUrl
                            ? "Regenerate Image"
                            : "Generate Cover Image"}
                      </button>
                      {selected.generatedImageStyle ? (
                        <span className="image-style-chip" title="Last image generation mode">
                          {selected.generatedImageStyle}
                        </span>
                      ) : null}
                    </>
                  ) : null}
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

      {state.settingsOpen ? (
        <div className="settings-overlay" onClick={() => setState((current) => ({ ...current, settingsOpen: false }))}>
          <section className="settings-modal" onClick={(event) => event.stopPropagation()}>
            <div className="settings-head">
              <h3>Automation Settings</h3>
              <button
                className="secondary-button"
                onClick={() => setState((current) => ({ ...current, settingsOpen: false }))}
              >
                Close
              </button>
            </div>
            <div className="settings-grid">
              <label className="weeks-field">
                API Base URL (optional)
                <input
                  type="text"
                  value={state.apiBaseInput}
                  onChange={(event) =>
                    setState((current) => ({
                      ...current,
                      apiBaseInput: event.target.value
                    }))
                  }
                  placeholder="leave empty to use current site"
                />
              </label>
              <label className="weeks-field">
                Enabled
                <input
                  type="checkbox"
                  checked={Boolean(state.settings.enabled)}
                  onChange={(event) =>
                    setState((current) => ({
                      ...current,
                      settings: { ...current.settings, enabled: event.target.checked }
                    }))
                  }
                />
              </label>
              <label className="weeks-field">
                Daily Time
                <input
                  type="time"
                  value={state.settings.dailyTime || "09:00"}
                  onChange={(event) =>
                    setState((current) => ({
                      ...current,
                      settings: { ...current.settings, dailyTime: event.target.value }
                    }))
                  }
                />
              </label>
              <label className="weeks-field">
                Timezone
                <input
                  type="text"
                  value={state.settings.timezone || "Asia/Kolkata"}
                  onChange={(event) =>
                    setState((current) => ({
                      ...current,
                      settings: { ...current.settings, timezone: event.target.value }
                    }))
                  }
                />
              </label>
              <label className="weeks-field">
                Notion Parent Page ID
                <input
                  type="text"
                  value={state.notionSetupParentPageId}
                  onChange={(event) =>
                    setState((current) => ({
                      ...current,
                      notionSetupParentPageId: event.target.value
                    }))
                  }
                  placeholder="paste Notion parent page id"
                />
              </label>
            </div>

            <div className="settings-links">
              <a href={state.settings.notionLinks?.pillars || "#"} target="_blank" rel="noreferrer">
                SEO Pillars DB
              </a>
              <a href={state.settings.notionLinks?.blogs || "#"} target="_blank" rel="noreferrer">
                Blog Pipeline DB
              </a>
              <a href={state.settings.notionLinks?.settings || "#"} target="_blank" rel="noreferrer">
                Automation Settings DB
              </a>
            </div>

            <div className="hero-actions">
              <button className="secondary-button" onClick={handleApiBaseSave}>
                Save API Base
              </button>
              <button className="primary-button" disabled={state.settingsSaving} onClick={handleSettingsSave}>
                {state.settingsSaving ? "Saving..." : "Save Settings"}
              </button>
              <button className="secondary-button" disabled={state.notionBusy} onClick={handleNotionSetup}>
                {state.notionBusy ? "Working..." : "Connect Notion / Recreate DBs"}
              </button>
              <button className="secondary-button" disabled={state.notionBusy} onClick={handleRunNow}>
                Run Now
              </button>
              <button className="secondary-button" disabled={state.notionBusy} onClick={handleSyncNow}>
                Sync Now
              </button>
              <button className="secondary-button" disabled={state.notionBusy} onClick={handleProcessNotionActionsNow}>
                Process Notion Actions Now
              </button>
            </div>

            <div className="settings-meta">
              <span>Notion Enabled: {String(state.notionEnabled)}</span>
              <span>Notion Configured: {String(state.notionConfigured)}</span>
              <span>Last Run: {state.settings.lastRunAt || "n/a"}</span>
              <span>Next Run: {state.settings.nextRunAt || "n/a"}</span>
            </div>
            {state.notionEnabled && !state.notionConfigured ? (
              <div className="settings-meta">
                <span>
                  Notion missing:
                  {" "}
                  {Array.isArray(state.notionDiagnostics?.missing) && state.notionDiagnostics.missing.length
                    ? state.notionDiagnostics.missing.join(", ")
                    : "unknown configuration fields"}
                </span>
              </div>
            ) : null}
          </section>
        </div>
      ) : null}

      {state.keywordModalOpen ? (
        <div className="settings-overlay" onClick={closeKeywordModal}>
          <section className="settings-modal keyword-modal" onClick={(event) => event.stopPropagation()}>
            <div className="settings-head">
              <h3>{state.keywordRole === "main" ? "Generate Main Topic" : "Generate Topic Ideas"}</h3>
              <button className="secondary-button" onClick={closeKeywordModal}>
                Close
              </button>
            </div>
            <p className="keyword-modal-note">
              Enter keywords (comma or new line separated). If empty, the system uses best SEO keywords automatically.
            </p>
            <label className="keyword-input-wrap">
              Keywords
              <textarea
                rows={6}
                value={state.keywordInput}
                onChange={(event) =>
                  setState((current) => ({
                    ...current,
                    keywordInput: event.target.value
                  }))
                }
                placeholder="Example: towels cause acne, antimicrobial towel for sensitive skin"
              />
            </label>
            <div className="hero-actions">
              <button className="secondary-button" onClick={closeKeywordModal} disabled={controlsDisabled}>
                Cancel
              </button>
              <button
                className="primary-button"
                onClick={() => generateTopics(state.keywordRole, state.keywordInput)}
                disabled={controlsDisabled}
              >
                {state.generating ? "Generating..." : "Generate"}
              </button>
            </div>
          </section>
        </div>
      ) : null}

      {state.imageModalOpen ? (
        <div className="settings-overlay" onClick={closeImageModal}>
          <section className="settings-modal keyword-modal" onClick={(event) => event.stopPropagation()}>
            <div className="settings-head">
              <h3>{selected?.generatedImageUrl ? "Regenerate Cover Image" : "Generate Cover Image"}</h3>
              <button className="secondary-button" onClick={closeImageModal}>
                Close
              </button>
            </div>
            <p className="keyword-modal-note">
              Leave prompt empty for automatic topic-aware image generation, or provide a custom prompt to regenerate.
            </p>
            <label className="keyword-input-wrap">
              Custom Prompt (optional)
              <textarea
                rows={6}
                value={state.imagePromptInput}
                onChange={(event) =>
                  setState((current) => ({
                    ...current,
                    imagePromptInput: event.target.value
                  }))
                }
                placeholder="Example: minimal bathroom scene, soft daylight, acne-safe skincare mood, no text overlay"
              />
            </label>
            <div className="hero-actions">
              <button className="secondary-button" onClick={closeImageModal} disabled={controlsDisabled}>
                Cancel
              </button>
              <button
                className="primary-button"
                onClick={() => generateCoverImage(state.imagePromptInput)}
                disabled={controlsDisabled}
              >
                {state.generatingImage ? "Generating..." : "Generate Image"}
              </button>
            </div>
          </section>
        </div>
      ) : null}
    </div>
  );
}

function QueueCard({ item, active, onPick, disabled = false }) {
  return (
    <button className={`post-card ${active ? "active" : ""}`} onClick={() => onPick(item.id)} disabled={disabled}>
      <strong>{item.title}</strong>
      <span>{item.query}</span>
      <span>Generated: {formatTimestamp(item.created_at)}</span>
      <span>{formatHierarchyLabel(item)}</span>
      {item.reports_to_main_title ? <span>Reports to: {item.reports_to_main_title}</span> : null}
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
  const pipeline = Array.isArray(workspace.pipeline) ? workspace.pipeline : [];
  const pillars = derivePillars(workspace.pillars, pipeline);
  const shopifyBlogs = Array.isArray(workspace.shopifyBlogs) ? workspace.shopifyBlogs : [];
  const selectedPillarId = resolveInitialPillarId(pillars, pipeline);
  const selectedId =
    pipeline.find(
      (item) =>
        item.status !== "rejected" &&
        (!selectedPillarId || item.pillar_id === selectedPillarId)
    )?.id || "";

  return {
    pillars,
    pipeline,
    selectedPillarId,
    selectedId,
    shopifyBlogs,
    selectedBlogId: shopifyBlogs[0]?.id || ""
  };
}

function resolveInitialPillarId(pillars, pipeline) {
  const firstPillarId = pillars[0]?.pillarId;
  if (firstPillarId) return firstPillarId;
  const fallback = pipeline.find((item) => String(item.pillar_id || "").trim());
  return fallback?.pillar_id || "";
}

function derivePillars(rawPillars, pipeline) {
  const explicit = Array.isArray(rawPillars) ? rawPillars.filter(Boolean) : [];
  if (explicit.length) return explicit;

  const byId = new Map();
  for (const item of Array.isArray(pipeline) ? pipeline : []) {
    const pillarId = String(item?.pillar_id || "").trim();
    if (!pillarId || byId.has(pillarId)) continue;
    byId.set(pillarId, {
      pillarId,
      pillarName: String(item?.pillar_name || humanizePillarId(pillarId)),
      pillarClaim: String(item?.pillar_claim || ""),
      mainTopic: String(item?.main_topic || ""),
      clusters: []
    });
  }
  return Array.from(byId.values());
}

function humanizePillarId(pillarId) {
  const raw = String(pillarId || "").trim();
  if (!raw) return "Pillar";
  const cleaned = raw.replace(/^pillar[-_\s]*/i, "").replace(/[-_]+/g, " ").trim();
  if (!cleaned) return "Pillar";
  return `Pillar ${cleaned.charAt(0).toUpperCase()}${cleaned.slice(1)}`;
}

function formatHierarchyLabel(item) {
  const role = String(item?.hierarchy_role || "").trim();
  if (role === "main-ceo") return "Main Blog (CEO)";
  if (role === "sub-reports-to-main") return "Sub Blog (Reports to Main)";
  if (role === "pillar-company") return "Pillar (Company)";
  if (item?.topic_role === "main") return "Main Blog (CEO)";
  return "Sub Blog (Reports to Main)";
}

function getItemNotionRowUrl(item) {
  if (!item || typeof item !== "object") return "";
  const direct = String(item.notionPageUrl || "").trim();
  if (direct) return direct;
  const metadata = item.metadata && typeof item.metadata === "object" ? item.metadata : {};
  return String(metadata.notion_page_url || "").trim();
}

function notionSyncHint(item) {
  const status = String(item?.status || "").trim().toLowerCase();
  if (status === "topic") return "Topic is synced to Notion. Full draft content is written after Approve.";
  if (status === "approved" || status === "draft" || status === "pushed") {
    return "Draft content and metadata are synced to Notion for this row.";
  }
  return "This row is synced to Notion.";
}

function formatTimestamp(value) {
  const raw = String(value || "").trim();
  if (!raw) return "n/a";
  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toLocaleString();
  }
  return raw;
}

async function runNativeTask(task) {
  await new Promise((resolve) => {
    window.setTimeout(resolve, 140);
  });
  return task();
}
