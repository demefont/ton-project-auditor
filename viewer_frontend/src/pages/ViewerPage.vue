<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import InspectorPanel from "../components/InspectorPanel.vue";
import NewRunDialog from "../components/NewRunDialog.vue";
import RunToolbar from "../components/RunToolbar.vue";
import WorkflowBoard from "../components/WorkflowBoard.vue";
import { useViewerStore } from "../stores/viewer";
import type { WorkflowPlan, WorkflowUnit } from "../types";
import {
  activeRootStage,
  asText,
  entityLabel,
  formatDuration,
  formatMetricLabel,
  initializeViewerLocale,
  leafExecutionUnits,
  localeQueryValue,
  setViewerLocale,
  stageVisualState,
  statusCountByUnit,
  statusLabel,
  t,
  tokenLabel,
  unitStatus,
  viewerLocale,
} from "../viewer-utils";

const store = useViewerStore();
const route = useRoute();
const router = useRouter();
initializeViewerLocale(String(route.query.lang || ""));
const newDialogOpen = ref(false);
const graphWrapper = ref<HTMLElement | null>(null);
const resultSection = ref<HTMLElement | null>(null);
const autoScrollTarget = ref("");
const telegramMiniApp = ref(false);
const telegramCompact = ref(false);
const compactPane = ref<"workflow" | "inspector">("workflow");
const simpleQuery = ref("");
const simplePhase = ref<"discover" | "candidates">("discover");

const SIMPLE_STAGE_TITLES: Record<string, string> = {
  project_discovery: "stage_find_project",
  source_collection: "stage_collect_signals",
  address_signal: "stage_resolve_ton_address",
  identity_confirmation: "stage_confirm_identity",
  repo_analysis: "stage_analyze_code_activity",
  community_analysis: "stage_check_community",
  project_type: "stage_classify_project",
  deep_validation: "stage_compare_validate",
  claim_consistency: "stage_cross_check_claims",
  risk_validator: "stage_score_risks",
  rule_engine: "stage_build_verdict",
  llm_explainer: "stage_write_final_explanation",
};

const SIMPLE_DISCOVERY_STAGE_TITLES: Record<string, string> = {
  discovery_stage_0: "stage_prepare_query",
  discovery_stage_1: "stage_search_sources",
  discovery_stage_2: "stage_rank_candidates",
};

const SIMPLE_DISCOVERY_BLUEPRINT = [
  {
    key: "discovery_stage_0",
    units: ["discovery_parse_query"],
  },
  {
    key: "discovery_stage_1",
    units: ["discovery_registry_search", "discovery_github_search", "discovery_market_search", "discovery_public_web_search"],
  },
  {
    key: "discovery_stage_2",
    units: ["discovery_rank_candidates"],
  },
];

function telegramWebApp(): any {
  return window.Telegram?.WebApp || null;
}

function updateTelegramCompact(): void {
  telegramMiniApp.value = Boolean(document.body.classList.contains("telegram-webapp"));
  telegramCompact.value = Boolean(telegramMiniApp.value && window.innerWidth <= 820);
  if (!telegramCompact.value) {
    compactPane.value = "workflow";
  }
}

const forcedMode = computed(() => String(route.query.mode || ""));
const isSimpleMode = computed(() => forcedMode.value === "simple" || (telegramMiniApp.value && forcedMode.value !== "advanced"));

const graphCaption = computed(() => {
  if (!store.currentPayload) {
    return t("loading_available_runs");
  }
  const caseName = String(store.currentPayload.case.name || store.currentPayload.case.case_id || "run");
  return caseName;
});

const overviewChips = computed(() => {
  if (!store.currentPayload) {
    return [];
  }
  const payload = store.currentPayload;
  const chips = [
    [t("case"), String(payload.case.name || payload.case.case_id || "-")],
    [t("mode"), tokenLabel(payload.options.mode || "-")],
    [t("status"), statusLabel(payload.run.status || "-")],
    [t("project_type"), resolvedProjectTypeLabel.value],
    [t("overall"), asText(payload.overview.overall_score)],
    [t("risk"), tokenLabel(payload.overview.risk_level || "-")],
    [t("clone"), tokenLabel(payload.overview.clone_risk || "-")],
    [t("stages"), String(payload.workflow.stages.length)],
  ];
  if (store.viewMode === "analysis") {
    chips.push([t("ai_units"), asText(payload.presentation.unit_counts?.ai || 0)]);
    chips.push([t("hybrid_units"), asText(payload.presentation.unit_counts?.hybrid || 0)]);
    chips.push([t("ai_stages"), asText(payload.presentation.stage_counts?.ai || 0)]);
    chips.push([t("hybrid_stages"), asText(payload.presentation.stage_counts?.hybrid || 0)]);
  }
  return chips;
});

const workflowUnitMap = computed(() => new Map((store.workflow?.units || []).map((unit) => [unit.unit_id, unit])));

function preservedModeQuery(targetMode: string | null = forcedMode.value || null): Record<string, string> {
  const mode = String(targetMode || "");
  const lang = localeQueryValue();
  const query: Record<string, string> = {};
  if (mode) {
    query.mode = mode;
  }
  if (lang) {
    query.lang = lang;
  }
  return query;
}

function absoluteAppUrl(relativePath = "", mode: string | null = null): string {
  const base = new URL(import.meta.env.BASE_URL || "/", window.location.origin);
  const target = relativePath ? new URL(relativePath.replace(/^\/+/, ""), base) : new URL(base.toString());
  if (mode) {
    target.searchParams.set("mode", mode);
  }
  const lang = localeQueryValue();
  if (lang) {
    target.searchParams.set("lang", lang);
  }
  return target.toString();
}

function handleLocaleChange(nextLocale: string): void {
  setViewerLocale(nextLocale);
  void router.replace({ path: route.path, query: preservedModeQuery() });
  if (store.currentRunId) {
    void store.refetchCurrentPayload();
  }
}

function isSuccessfulStageStatus(status: string): boolean {
  return status === "success" || status === "skipped";
}

function simpleStageStatusIcon(status: string): string {
  if (isSuccessfulStageStatus(status)) {
    return "✓";
  }
  if (status === "error") {
    return "!";
  }
  if (status === "running") {
    return "…";
  }
  return "";
}

function simpleStageStatusText(status: string, fallback: string): string {
  if (isSuccessfulStageStatus(status)) {
    return t("completed");
  }
  return fallback || statusLabel(status || "pending");
}

const advancedBrowserUrl = computed(() => {
  if (!store.currentRunId) {
    return absoluteAppUrl("", "advanced");
  }
  return absoluteAppUrl(`runs/${encodeURIComponent(store.currentRunId)}`, "advanced");
});

function openAdvancedBrowser(): void {
  const targetUrl = advancedBrowserUrl.value;
  const webApp = telegramWebApp();
  if (webApp?.openLink) {
    webApp.openLink(targetUrl);
    return;
  }
  const opened = window.open(targetUrl, "_blank", "noopener,noreferrer");
  if (!opened) {
    window.location.href = targetUrl;
  }
}

const simpleHasFreshCandidates = computed(() => {
  if (!store.discoveryCandidates.length) {
    return false;
  }
  const normalized = simpleQuery.value.trim();
  return normalized !== "" && normalized === store.discoveryQuery;
});

const simpleSearchNeedsRefresh = computed(() => {
  if (!store.discoveryCandidates.length) {
    return false;
  }
  const normalized = simpleQuery.value.trim();
  return normalized !== "" && normalized !== store.discoveryQuery;
});

const simpleShowDiscoveryStages = computed(() => {
  return Boolean(
    store.discoveryPending ||
    store.discoverySessionId ||
    store.discoveryQuery ||
    store.discoveryError ||
    store.discoveryWorkflow?.stages?.length,
  );
});

const simpleCanContinue = computed(() => {
  if (store.discoveryPending || store.createPending || !simpleHasFreshCandidates.value) {
    return false;
  }
  const candidate = store.selectedDiscoveryCandidate;
  if (!candidate || !store.selectedDiscoveryCandidateKey) {
    return false;
  }
  return Boolean(candidate.github_repo || candidate.project_url || candidate.telegram_handle || candidate.wallet_address);
});

function compactUnitNames(units: WorkflowUnit[]): string {
  const labels = units.map((unit) => entityLabel(unit.unit_id, unit.name || unit.unit_id)).filter(Boolean);
  if (!labels.length) {
    return t("waiting_for_runtime_update");
  }
  if (labels.length === 1) {
    return labels[0];
  }
  return `${labels[0]} +${labels.length - 1}`;
}

function compactWorkflowStageCards(workflow: WorkflowPlan | null, titleMap: Record<string, string>) {
  if (!workflow?.stages?.length) {
    return [];
  }
  const unitMap = new Map((workflow.units || []).map((unit) => [unit.unit_id, unit]));
  return workflow.stages.map((stage, index) => {
    const rootUnits = stage.unit_ids.map((unitId) => unitMap.get(unitId)).filter(Boolean) as WorkflowUnit[];
    const executionUnits = leafExecutionUnits(rootUnits);
    const progressUnits = executionUnits.length ? executionUnits : rootUnits;
    const counts = statusCountByUnit(progressUnits);
    const doneCount = Number(counts.success || 0) + Number(counts.skipped || 0);
    const totalCount = progressUnits.length || rootUnits.length || 1;
    const runningUnits = progressUnits.filter((unit) => unitStatus(unit) === "running");
    const errorUnits = progressUnits.filter((unit) => unitStatus(unit) === "error");
    const pendingUnits = progressUnits.filter((unit) => unitStatus(unit) === "pending");
    const primaryUnit = rootUnits[0];
    const titleKey = String(primaryUnit?.unit_id || "");
    const localizedTitleKey = titleMap[stage.stage_id] || titleMap[titleKey] || "";
    const title = localizedTitleKey
      ? t(localizedTitleKey)
      : primaryUnit
        ? entityLabel(primaryUnit.unit_id, primaryUnit.name || primaryUnit.unit_id)
        : t("stage_number", { index: stage.index });
    let substep = t("waiting_for_runtime_update");
    if (runningUnits.length) {
      substep = compactUnitNames(runningUnits);
    } else if (errorUnits.length) {
      substep = compactUnitNames(errorUnits);
    } else if (doneCount >= totalCount) {
      substep = t("completed");
    } else if (pendingUnits.length) {
      substep = compactUnitNames(pendingUnits);
    }
    const stageStatus = String(stage.runtime?.status || "pending");
    const progressPercent =
      stageStatus === "success" || stageStatus === "skipped"
        ? 100
        : totalCount
          ? Math.max(0, Math.min(100, Math.round((doneCount / totalCount) * 100)))
          : 0;
    return {
      stage,
      title,
      substep,
      counts,
      totalCount,
      doneCount,
      progressPercent,
      durationLabel: stageStatus === "running" ? t("elapsed", { duration: formatDuration(stage.runtime?.duration_ms) }) : formatDuration(stage.runtime?.duration_ms),
      status: stageStatus,
      visualState: stageVisualState(stage),
      description: String(primaryUnit?.description || stage.description || "").trim(),
      order: index + 1,
      units: progressUnits.map((unit) => ({
        key: unit.unit_id,
        name: entityLabel(unit.unit_id, unit.name || unit.unit_id),
        status: unitStatus(unit),
        summary: String(unit.result?.summary || unit.description || "").trim(),
        durationLabel: formatDuration(unit.runtime?.duration_ms),
      })),
    };
  });
}

const simpleDiscoveryStageCards = computed(() => {
  const cards = compactWorkflowStageCards(store.discoveryWorkflow, SIMPLE_DISCOVERY_STAGE_TITLES);
  if (cards.length) {
    return cards;
  }
  return SIMPLE_DISCOVERY_BLUEPRINT.map((item, index) => ({
    stage: null,
    title: t(SIMPLE_DISCOVERY_STAGE_TITLES[item.key]),
    substep: index === 0 ? t("waiting_for_project_input") : t("waiting_for_previous_stage"),
    counts: { success: 0, skipped: 0, error: 0, pending: item.units.length },
    totalCount: item.units.length,
    doneCount: 0,
    progressPercent: 0,
    durationLabel: "",
    status: "pending",
    visualState: "future",
    description: "",
    order: index + 1,
    units: item.units.map((unitId, unitIndex) => ({
      key: `${item.key}-${unitIndex}`,
      name: entityLabel(unitId, unitId),
      status: "pending",
      summary: "",
      durationLabel: "",
    })),
  }));
});

const simpleStageCards = computed(() => compactWorkflowStageCards(store.workflow, SIMPLE_STAGE_TITLES));

const simpleCurrentStage = computed(() => {
  return (
    simpleStageCards.value.find((item) => item.status === "running") ||
    simpleStageCards.value.find((item) => item.status === "error") ||
    (store.currentPayload?.run.status === "success" ? simpleStageCards.value[simpleStageCards.value.length - 1] || null : null)
  );
});

const simpleRunStatus = computed(() => String(store.currentPayload?.run.status || "pending"));

const simpleRunHeadline = computed(() => {
  if (!store.currentPayload) {
    return "";
  }
  const current = simpleCurrentStage.value;
  if (!current) {
    return "";
  }
  return `${t("step_of", { index: current.order, total: simpleStageCards.value.length })} | ${current.title}`;
});

const simpleRunDetail = computed(() => {
  const current = simpleCurrentStage.value;
  if (!current) {
    return store.createPending ? t("starting_validation_run") : t("waiting_next_runtime_update");
  }
  const duration = current.durationLabel || formatDuration(0);
  return `${duration} | ${current.substep}`;
});

const simpleRunSummary = computed(() => [simpleRunHeadline.value, simpleRunDetail.value].filter(Boolean).join(" | "));

function localizedResultSignalLabel(key: string, fallback: string): string {
  const map: Record<string, string> = {
    github_repo: "github",
    github: "github",
    website: "website",
    project_url: "website",
    telegram: "telegram",
    telegram_handle: "telegram",
    wallet: "contract",
    wallet_address: "contract",
    contract_health: "contract",
    project_type: "project_type",
  };
  const translatedKey = map[key];
  return translatedKey ? t(translatedKey) : fallback;
}

function resultSignalTargetKey(key: string): string {
  const map: Record<string, string> = {
    repo_freshness: "github_repo",
    clone_analysis: "github_repo",
    repository_lineage_note: "github_repo",
    telegram_activity: "telegram_handle",
    telegram_feed_quality: "telegram_handle",
    contract_health: "wallet_address",
  };
  return map[key] || "";
}

function meaningfulProjectTypeValue(value: unknown): string {
  const normalized = String(value || "").trim();
  if (!normalized || normalized === "-" || normalized.toLowerCase() === "unknown") {
    return "";
  }
  return normalized;
}

const resolvedProjectTypeLabel = computed(() => {
  const caseData = (store.currentPayload?.case || {}) as Record<string, unknown>;
  const resolved =
    meaningfulProjectTypeValue(finalResult.value?.project_type) ||
    meaningfulProjectTypeValue(store.currentPayload?.overview.project_type) ||
    meaningfulProjectTypeValue(caseData.type_hint) ||
    String(finalResult.value?.project_type || store.currentPayload?.overview.project_type || caseData.type_hint || "-");
  return tokenLabel(resolved);
});

function tonviewerAddressUrl(value: string): string {
  const normalized = String(value || "").trim();
  return normalized ? `https://tonviewer.com/${normalized}` : "";
}

function externalUrl(value: string): string {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return "";
  }
  return /^https?:\/\//i.test(normalized) ? normalized : `https://${normalized}`;
}

function githubRepoUrl(value: string): string {
  const normalized = String(value || "").trim().replace(/^\/+/, "");
  return normalized ? `https://github.com/${normalized}` : "";
}

function telegramHandleUrl(value: string): string {
  const normalized = String(value || "").trim().replace(/^@+/, "");
  return normalized ? `https://t.me/${normalized}` : "";
}

function telegramHandleLabel(value: string): string {
  const normalized = String(value || "").trim().replace(/^@+/, "");
  return normalized ? `@${normalized}` : "-";
}

const resultSignalItems = computed(() => {
  const result = finalResult.value;
  if (!result) {
    return [];
  }
  const items = new Map<
    string,
    {
      key: string;
      label: string;
      value: string;
      url: string;
      notes: Array<{ key: string; text: string; url: string }>;
    }
  >();
  const ensureItem = (key: string, fallbackLabel: string, value = "", url = "") => {
    const itemKey = key || `signal_${items.size + 1}`;
    const existing = items.get(itemKey);
    if (existing) {
      if (!existing.value && value) {
        existing.value = value;
      }
      if (!existing.url && url) {
        existing.url = url;
      }
      if (!existing.label && fallbackLabel) {
        existing.label = fallbackLabel;
      }
      return existing;
    }
    const created = {
      key: itemKey,
      label: localizedResultSignalLabel(itemKey, fallbackLabel),
      value,
      url,
      notes: [] as Array<{ key: string; text: string; url: string }>,
    };
    items.set(itemKey, created);
    return created;
  };
  for (const fact of result.facts || []) {
    if (!fact?.value) {
      continue;
    }
    const fallbackUrl = fact.key === "wallet_address" ? tonviewerAddressUrl(fact.value) : "";
    ensureItem(String(fact.key || ""), String(fact.label || ""), String(fact.value || ""), String(fact.url || fallbackUrl));
  }
  for (const evidence of result.risk_evidence || []) {
    if (!evidence?.summary) {
      continue;
    }
    const targetKey = resultSignalTargetKey(String(evidence.key || ""));
    const item = targetKey
      ? ensureItem(targetKey, String(evidence.label || ""), "", String(evidence.url || ""))
      : ensureItem(String(evidence.key || ""), String(evidence.label || ""), "", String(evidence.url || ""));
    if (!item.notes.some((note) => note.text === evidence.summary && note.url === evidence.url)) {
      item.notes.push({
        key: String(evidence.key || ""),
        text: String(evidence.summary || ""),
        url: String(evidence.url || ""),
      });
    }
  }
  return Array.from(items.values()).filter((item) => item.value || item.url || item.notes.length);
});

const simpleResultCards = computed(() => {
  const metrics = finalResult.value?.metrics || {};
  return [
    {
      key: "project_type",
      label: t("project_type"),
      value: resolvedProjectTypeLabel.value,
    },
    { key: "overall_score", label: formatMetricLabel("overall_score"), value: asText(finalResult.value?.overall_score) },
    {
      key: "risk_level",
      label: formatMetricLabel("risk_level"),
      value: tokenLabel(finalResult.value?.risk_level || asText(store.currentPayload?.overview.risk_level)),
    },
    {
      key: "clone_risk",
      label: formatMetricLabel("clone_risk"),
      value: tokenLabel(finalResult.value?.clone_risk || asText(store.currentPayload?.overview.clone_risk)),
    },
    { key: "identity_score", label: formatMetricLabel("identity_score"), value: asText(metrics.identity_score) },
    { key: "activity_score", label: formatMetricLabel("activity_score"), value: asText(metrics.activity_score) },
    { key: "onchain_tx_count_30d", label: formatMetricLabel("onchain_tx_count_30d"), value: asText(metrics.onchain_tx_count_30d) },
    { key: "originality_score", label: formatMetricLabel("originality_score"), value: asText(metrics.originality_score) },
    { key: "community_quality_score", label: formatMetricLabel("community_quality_score"), value: asText(metrics.community_quality_score) },
  ].filter((item) => item.value !== "-");
});

const simpleStrengths = computed(() => (finalResult.value?.strengths || []).slice(0, 4).map((item) => tokenLabel(item)));
const simpleRisks = computed(() => (finalResult.value?.risks || []).slice(0, 4).map((item) => tokenLabel(item)));
const simpleNextChecks = computed(() => (finalResult.value?.next_checks || []).slice(0, 3).map((item) => tokenLabel(item)));

const activeStage = computed(() => {
  const workflow = store.workflow;
  if (!workflow?.stages?.length) {
    return null;
  }
  const running = activeRootStage(workflow);
  if (running) {
    return running;
  }
  const errorStageId = String(store.currentPayload?.run.error?.stage_id || "");
  if (errorStageId) {
    return workflow.stages.find((stage) => stage.stage_id === errorStageId) || null;
  }
  if (store.currentPayload?.run.status === "running") {
    return workflow.stages.find((stage) => String(stage.runtime?.status || "pending") === "pending") || workflow.stages[workflow.stages.length - 1] || null;
  }
  return null;
});

const activeStageUnits = computed(() => {
  if (!activeStage.value) {
    return [] as WorkflowUnit[];
  }
  return activeStage.value.unit_ids.map((unitId) => workflowUnitMap.value.get(unitId)).filter(Boolean) as WorkflowUnit[];
});

const activeStageCounts = computed(() => statusCountByUnit(activeStageUnits.value));

const activeStageSummary = computed(() => {
  if (!activeStage.value) {
    return "";
  }
  const parts = [
    formatDuration(activeStage.value.runtime?.duration_ms),
    t("stage_number", { index: activeStage.value.index }),
  ];
  if (activeStageCounts.value.running) {
    parts.push(t("running_count", { count: activeStageCounts.value.running }));
  }
  if (activeStageCounts.value.pending) {
    parts.push(t("pending_count", { count: activeStageCounts.value.pending }));
  }
  if (activeStageCounts.value.error) {
    parts.push(t("error_count", { count: activeStageCounts.value.error }));
  }
  return parts.join(" | ");
});

const activeStageDetail = computed(() => {
  const runningUnits = activeStageUnits.value
    .filter((unit) => String(unit?.runtime?.status || unit?.result?.status || "pending") === "running")
    .map((unit) => entityLabel(unit.unit_id, unit?.name || unit?.unit_id))
    .filter(Boolean);
  if (runningUnits.length) {
    return t("running_now", { names: runningUnits.join(", ") });
  }
  return activeStage.value?.description || t("waiting_next_runtime_update");
});

const finalResult = computed(() => store.currentPayload?.result || null);
const resultAvailable = computed(() => Boolean(finalResult.value && String(finalResult.value.status || "pending") !== "pending"));

const resultMetricEntries = computed(() => {
  const metrics = finalResult.value?.metrics || {};
  const preferredOrder = [
    "overall_score",
    "identity_score",
    "activity_score",
    "onchain_tx_count_30d",
    "last_onchain_tx_age_days",
    "originality_score",
    "community_activity_score",
    "community_quality_score",
    "last_commit_age_days",
  ];
  const seen = new Set<string>();
  const entries: Array<[string, unknown]> = [];
  for (const key of preferredOrder) {
    if (!(key in metrics)) {
      continue;
    }
    seen.add(key);
    entries.push([key, metrics[key]]);
  }
  for (const [key, value] of Object.entries(metrics)) {
    if (seen.has(key)) {
      continue;
    }
    entries.push([key, value]);
  }
  return entries;
});

const runErrorText = computed(() => {
  const error = store.currentPayload?.run.error;
  if (!error?.summary) {
    return "";
  }
  const parts = [];
  if (typeof error.stage_index === "number" && error.stage_index >= 0) {
    parts.push(t("stage_number", { index: error.stage_index }));
  }
  if (error.unit_name || error.unit_id) {
    parts.push(entityLabel(String(error.unit_id || ""), String(error.unit_name || error.unit_id)));
  }
  const location = parts.length ? `${parts.join(" | ")}: ` : "";
  return `${location}${error.summary}`;
});

function scrollGraphToElement(target: HTMLElement | null): boolean {
  const wrapper = graphWrapper.value;
  if (!wrapper || !target) {
    return false;
  }
  const top = wrapper.scrollTop + target.getBoundingClientRect().top - wrapper.getBoundingClientRect().top - 16;
  wrapper.scrollTo({
    top: Math.max(0, top),
    behavior: "smooth",
  });
  return true;
}

async function scrollToCurrentStage(): Promise<boolean> {
  if (telegramCompact.value) {
    compactPane.value = "workflow";
    await nextTick();
  }
  const stageId = String(activeStage.value?.stage_id || "");
  if (!stageId) {
    return false;
  }
  await nextTick();
  const target = document.getElementById(`stage-${stageId}`);
  if (target instanceof HTMLElement) {
    return scrollGraphToElement(target);
  }
  return false;
}

async function scrollToResult(): Promise<boolean> {
  if (telegramCompact.value) {
    compactPane.value = "workflow";
    await nextTick();
  }
  await nextTick();
  if (resultSection.value) {
    return scrollGraphToElement(resultSection.value);
  }
  return false;
}

async function handleSelectUnit(unitId: string): Promise<void> {
  await store.selectUnit(unitId);
  if (telegramCompact.value) {
    compactPane.value = "inspector";
  }
}

function handleSelectStage(selection: { stage_id: string; parent_unit_id?: string }): void {
  store.selectStage(selection);
  if (telegramCompact.value) {
    compactPane.value = "inspector";
  }
}

function setCompactPane(nextPane: "workflow" | "inspector"): void {
  compactPane.value = nextPane;
}

async function handleTelegramBack(): Promise<void> {
  if (telegramCompact.value && compactPane.value === "inspector") {
    compactPane.value = "workflow";
    return;
  }
  if (!route.params.runId && isSimpleMode.value && simplePhase.value === "candidates") {
    simplePhase.value = "discover";
    return;
  }
  const runId = typeof route.params.runId === "string" ? route.params.runId : "";
  if (runId) {
    await router.replace({ name: "viewer-root", query: preservedModeQuery() });
  }
}

function syncTelegramBackButton(): void {
  const webApp = telegramWebApp();
  if (!webApp?.BackButton) {
    return;
  }
  const shouldShow = Boolean(
    (telegramCompact.value && compactPane.value === "inspector") ||
    route.params.runId ||
    (isSimpleMode.value && simplePhase.value === "candidates"),
  );
  if (shouldShow) {
    webApp.BackButton.show();
  } else {
    webApp.BackButton.hide();
  }
}

async function boot(): Promise<void> {
  const runId = typeof route.params.runId === "string" ? route.params.runId : "";
  if (isSimpleMode.value && !runId) {
    simpleQuery.value = "";
    simplePhase.value = "discover";
    store.resetDiscovery();
    store.clearCurrentRun();
    return;
  }
  await store.loadRuns(runId);
}

async function handleSelectRun(runId: string): Promise<void> {
  await store.selectRun(runId);
  if (runId) {
    await router.replace({ name: "viewer-run", params: { runId }, query: preservedModeQuery() });
  }
}

function openNewDialog(): void {
  store.resetDiscovery();
  newDialogOpen.value = true;
}

async function handleDiscover(query: string): Promise<void> {
  await store.searchDiscovery(query);
}

async function handleContinueDeepCheck(): Promise<void> {
  try {
    const runId = await store.createRunFromDiscovery();
    newDialogOpen.value = false;
    await router.replace({ name: "viewer-run", params: { runId }, query: preservedModeQuery() });
  } catch {
    simplePhase.value = "candidates";
  }
}

async function handleSimpleDiscover(): Promise<void> {
  const normalized = simpleQuery.value.trim();
  if (!normalized) {
    return;
  }
  simplePhase.value = "discover";
  try {
    await handleDiscover(normalized);
    simplePhase.value = store.discoveryCandidates.length ? "candidates" : "discover";
  } catch {
    simplePhase.value = "discover";
  }
  syncTelegramBackButton();
}

function backToSimpleSearch(): void {
  simplePhase.value = "discover";
  syncTelegramBackButton();
}

function handleSimpleCandidateChange(event: Event): void {
  const target = event.target as HTMLSelectElement | null;
  store.selectDiscoveryCandidate(target?.value || "");
}

async function openSimpleDiscovery(): Promise<void> {
  simpleQuery.value = "";
  simplePhase.value = "discover";
  autoScrollTarget.value = "";
  store.resetDiscovery();
  store.clearCurrentRun();
  await router.replace({ name: "viewer-root", query: preservedModeQuery() });
}

onMounted(() => {
  if (String(route.query.lang || "") !== localeQueryValue()) {
    void router.replace({ path: route.path, query: preservedModeQuery() });
  }
  updateTelegramCompact();
  window.addEventListener("resize", updateTelegramCompact);
  const webApp = telegramWebApp();
  if (webApp?.BackButton?.onClick) {
    webApp.BackButton.onClick(handleTelegramBack);
  }
  syncTelegramBackButton();
  void boot();
});

onBeforeUnmount(() => {
  window.removeEventListener("resize", updateTelegramCompact);
  const webApp = telegramWebApp();
  if (webApp?.BackButton?.offClick) {
    webApp.BackButton.offClick(handleTelegramBack);
  }
  webApp?.BackButton?.hide?.();
  store.dispose();
});

watch(
  () => store.currentRunId,
  () => {
    autoScrollTarget.value = "";
  },
);

watch(
  [() => route.params.runId, isSimpleMode],
  async ([runId, simpleMode]) => {
    const nextRunId = typeof runId === "string" ? runId : "";
    if (!nextRunId) {
      if (simpleMode) {
        store.clearCurrentRun();
      }
      syncTelegramBackButton();
      return;
    }
    if (nextRunId === store.currentRunId) {
      syncTelegramBackButton();
      return;
    }
    await store.selectRun(nextRunId);
    syncTelegramBackButton();
  },
);

watch(
  () => String(route.query.lang || ""),
  (lang) => {
    if (!lang) {
      return;
    }
    initializeViewerLocale(lang);
  },
);

watch([telegramCompact, compactPane, simplePhase], () => {
  syncTelegramBackButton();
});

watch(
  [
    () => store.currentRunId,
    () => String(activeStage.value?.stage_id || ""),
    () => String(store.currentPayload?.run.status || ""),
    () => String(finalResult.value?.status || "pending"),
  ],
  async ([runId, stageId, runStatus, resultStatus]) => {
    if (isSimpleMode.value) {
      return;
    }
    if (!runId) {
      return;
    }
    if (stageId && (runStatus === "running" || runStatus === "error")) {
      const key = `stage:${runId}:${runStatus}:${stageId}`;
      if (autoScrollTarget.value !== key) {
        const moved = await scrollToCurrentStage();
        if (moved) {
          autoScrollTarget.value = key;
        }
      }
      return;
    }
    if (resultStatus !== "pending") {
      const key = `result:${runId}:${resultStatus}`;
      if (autoScrollTarget.value !== key) {
        const moved = await scrollToResult();
        if (moved) {
          autoScrollTarget.value = key;
        }
      }
    }
  },
  { immediate: true },
);
</script>

<template>
  <div class="app-shell" :class="{ 'app-shell--simple': isSimpleMode }">
    <template v-if="isSimpleMode">
      <section class="simple-shell">
        <div class="simple-topbar">
          <div class="locale-switch">
            <button
              type="button"
              class="locale-switch__button"
              :class="{ 'locale-switch__button--active': viewerLocale === 'en' }"
              @click="handleLocaleChange('en')"
            >
              {{ t("locale_en") }}
            </button>
            <button
              type="button"
              class="locale-switch__button"
              :class="{ 'locale-switch__button--active': viewerLocale === 'ru' }"
              @click="handleLocaleChange('ru')"
            >
              {{ t("locale_ru") }}
            </button>
          </div>
          <div class="simple-topbar__actions">
            <button type="button" class="simple-topbar__button" @click="openAdvancedBrowser">{{ t("open_advanced_browser") }}</button>
            <button
              v-if="route.params.runId"
              type="button"
              class="simple-topbar__button simple-topbar__button--primary"
              @click="openSimpleDiscovery"
            >
              {{ t("new_audit") }}
            </button>
          </div>
        </div>

        <p v-if="store.fatalError" class="form-error top-error simple-inline-error">{{ store.fatalError }}</p>

        <template v-if="!route.params.runId">
          <template v-if="simplePhase === 'discover'">
            <section class="panel simple-panel simple-input-panel">
              <div class="panel-header simple-panel__head simple-panel__head--compact">
                <div>
                  <h2>{{ t("find_project") }}</h2>
                  <p>{{ t("find_project_subtitle") }}</p>
                </div>
              </div>

              <div class="simple-panel__body">
                <label class="field field--full">
                  <span>{{ t("project_input") }}</span>
                  <textarea
                    v-model.trim="simpleQuery"
                    rows="2"
                    :placeholder="t('project_input_placeholder')"
                  />
                </label>

                <div class="simple-action-row">
                  <button type="button" class="button-primary" :disabled="store.discoveryPending || !simpleQuery.trim()" @click="handleSimpleDiscover">
                    {{ store.discoveryPending ? t("searching") : t("find_candidates") }}
                  </button>
                </div>

                <p v-if="simpleSearchNeedsRefresh" class="inspector-subtitle">{{ t("input_changed_refresh") }}</p>
                <p v-if="store.discoveryError" class="form-error">{{ store.discoveryError }}</p>
                <p v-else-if="store.discoveryQuery && !store.discoveryPending && !simpleHasFreshCandidates" class="inspector-subtitle">
                  {{ t("no_candidates_found") }}
                </p>
              </div>
            </section>

            <section v-if="simpleShowDiscoveryStages" class="panel simple-panel simple-stage-panel">
              <div class="simple-stage-list simple-stage-list--compact">
                <article
                  v-for="item in simpleDiscoveryStageCards"
                  :key="item.stage?.stage_id || item.order"
                  class="simple-stage-card simple-stage-card--compact"
                  :class="[
                    `simple-stage-card--${item.status}`,
                    `simple-stage-card--${item.visualState}`,
                  ]"
                >
                  <div class="simple-stage-card__head">
                    <div>
                      <div class="simple-stage-card__eyebrow">{{ t("step_label", { index: item.order }) }}</div>
                      <h3>{{ item.title }}</h3>
                    </div>
                    <span class="badge simple-stage-card__status-badge" :class="`simple-stage-card__status-badge--${item.status}`">
                      {{ simpleStageStatusIcon(item.status) || statusLabel(item.status) }}
                    </span>
                  </div>

                  <div class="simple-stage-unit-list">
                    <div
                      v-for="unit in item.units"
                      :key="unit.key"
                      class="simple-stage-unit-row"
                      :class="`simple-stage-unit-row--${unit.status}`"
                    >
                      <span class="simple-stage-unit-row__name">{{ unit.name }}</span>
                      <span class="badge status-badge" :class="`status-badge--${unit.status}`">{{ statusLabel(unit.status) }}</span>
                    </div>
                  </div>

                  <div class="simple-stage-card__meta">
                    <span class="simple-stage-card__time">{{ item.durationLabel || formatDuration(0) }}</span>
                    <span class="simple-stage-card__state" :class="`simple-stage-card__state--${item.status}`">
                      <span v-if="simpleStageStatusIcon(item.status)" class="simple-stage-card__state-icon">{{ simpleStageStatusIcon(item.status) }}</span>
                      <span>{{ simpleStageStatusText(item.status, item.substep) }}</span>
                    </span>
                    <span v-if="item.counts.error" class="simple-stage-card__error">{{ t("errors_count", { count: item.counts.error }) }}</span>
                  </div>

                  <div class="simple-progress">
                    <div class="simple-progress__bar" :style="{ width: `${item.progressPercent}%` }"></div>
                  </div>
                </article>
              </div>
            </section>
          </template>

          <section v-else class="panel simple-panel simple-candidate-panel">
            <div class="panel-header simple-panel__head simple-panel__head--compact">
              <div>
                <h2>{{ t("choose_candidate") }}</h2>
                <p>{{ store.discoverySummary || t("choose_candidate_subtitle") }}</p>
              </div>
            </div>

            <div class="simple-panel__body">
              <label class="field field--full">
                <span>{{ t("detected_candidates") }}</span>
                <select :value="store.selectedDiscoveryCandidateKey" @change="handleSimpleCandidateChange">
                  <option v-for="candidate in store.discoveryCandidates" :key="candidate.candidate_key" :value="candidate.candidate_key">
                    {{ candidate.name }} · {{ t("score") }} {{ candidate.score.toFixed(2) }}
                  </option>
                </select>
              </label>

              <article v-if="store.selectedDiscoveryCandidate" class="candidate-card simple-selected-candidate">
                <div class="candidate-card-head">
                  <div>
                    <h4>{{ store.selectedDiscoveryCandidate.name }}</h4>
                    <p>{{ store.selectedDiscoveryCandidate.match_reason || t("selected_project_candidate") }}</p>
                  </div>
                  <span class="badge badge--toolbar">{{ t("score") }} {{ store.selectedDiscoveryCandidate.score.toFixed(2) }}</span>
                </div>

                <div class="candidate-grid candidate-grid--compact">
                  <div class="kv-card">
                    <span class="kv-label">{{ t("website") }}</span>
                    <div class="kv-value">
                      <a v-if="store.selectedDiscoveryCandidate.project_url" :href="externalUrl(store.selectedDiscoveryCandidate.project_url)" target="_blank" rel="noreferrer">
                        {{ store.selectedDiscoveryCandidate.project_url }}
                      </a>
                      <span v-else>-</span>
                    </div>
                  </div>
                  <div class="kv-card">
                    <span class="kv-label">{{ t("github") }}</span>
                    <div class="kv-value">
                      <a v-if="store.selectedDiscoveryCandidate.github_repo" :href="githubRepoUrl(store.selectedDiscoveryCandidate.github_repo)" target="_blank" rel="noreferrer">
                        {{ store.selectedDiscoveryCandidate.github_repo }}
                      </a>
                      <span v-else>-</span>
                    </div>
                  </div>
                  <div class="kv-card">
                    <span class="kv-label">{{ t("telegram") }}</span>
                    <div class="kv-value">
                      <a v-if="store.selectedDiscoveryCandidate.telegram_handle" :href="telegramHandleUrl(store.selectedDiscoveryCandidate.telegram_handle)" target="_blank" rel="noreferrer">
                        {{ telegramHandleLabel(store.selectedDiscoveryCandidate.telegram_handle) }}
                      </a>
                      <span v-else>-</span>
                    </div>
                  </div>
                  <div class="kv-card">
                    <span class="kv-label">{{ t("wallet") }}</span>
                    <div class="kv-value">
                      <a v-if="store.selectedDiscoveryCandidate.wallet_address" :href="tonviewerAddressUrl(store.selectedDiscoveryCandidate.wallet_address)" target="_blank" rel="noreferrer">
                        {{ store.selectedDiscoveryCandidate.wallet_address }}
                      </a>
                      <span v-else>{{ t("not_resolved_yet") }}</span>
                    </div>
                  </div>
                  <div class="kv-card">
                    <span class="kv-label">{{ t("project_type") }}</span>
                    <div class="kv-value">{{ tokenLabel(store.selectedDiscoveryCandidate.project_type || "-") }}</div>
                  </div>
                </div>

                <div class="candidate-meta">
                  <span v-for="sourceLabel in store.selectedDiscoveryCandidate.source_labels" :key="sourceLabel" class="badge badge--presentation">
                    {{ tokenLabel(sourceLabel) }}
                  </span>
                </div>
              </article>

              <div class="simple-action-row simple-action-row--between">
                <button type="button" class="button-ghost" @click="backToSimpleSearch">{{ t("back") }}</button>
                <button type="button" class="button-primary" :disabled="store.createPending || !simpleCanContinue" @click="handleContinueDeepCheck">
                  {{ store.createPending ? t("starting_audit") : t("start_audit") }}
                </button>
              </div>

              <p v-if="store.createError" class="form-error">{{ store.createError }}</p>
            </div>
          </section>
        </template>

        <template v-else-if="store.currentPayload">
          <section class="panel simple-panel simple-run-header">
            <div class="simple-run-header__top">
              <div>
                <h2>{{ asText(store.currentPayload.case.name || store.currentPayload.case.case_id, t("final_result")) }}</h2>
                <p>{{ simpleRunSummary }}</p>
              </div>

              <span class="badge status-badge" :class="`status-badge--${simpleRunStatus}`">{{ statusLabel(simpleRunStatus) }}</span>
            </div>

            <p v-if="runErrorText" class="form-error simple-inline-error">{{ runErrorText }}</p>
          </section>

          <section class="panel simple-panel">
            <div class="simple-stage-list">
              <article
                v-for="item in simpleStageCards"
                :key="item.stage.stage_id"
                class="simple-stage-card"
                :class="[
                  `simple-stage-card--${item.status}`,
                  `simple-stage-card--${item.visualState}`,
                ]"
              >
                <div class="simple-stage-card__head">
                  <div>
                    <div class="simple-stage-card__eyebrow">{{ t("step_label", { index: item.order }) }}</div>
                    <h3>{{ item.title }}</h3>
                  </div>

                  <span class="badge simple-stage-card__status-badge" :class="`simple-stage-card__status-badge--${item.status}`">
                    {{ simpleStageStatusIcon(item.status) || statusLabel(item.status) }}
                  </span>
                </div>

                <div class="simple-stage-card__meta">
                  <span class="simple-stage-card__time">{{ item.durationLabel || formatDuration(0) }}</span>
                  <span class="simple-stage-card__state" :class="`simple-stage-card__state--${item.status}`">
                    <span v-if="simpleStageStatusIcon(item.status)" class="simple-stage-card__state-icon">{{ simpleStageStatusIcon(item.status) }}</span>
                    <span>{{ simpleStageStatusText(item.status, item.substep) }}</span>
                  </span>
                  <span v-if="item.counts.error" class="simple-stage-card__error">{{ t("errors_count", { count: item.counts.error }) }}</span>
                </div>

                <div class="simple-progress">
                  <div class="simple-progress__bar" :style="{ width: `${item.progressPercent}%` }"></div>
                </div>
              </article>
            </div>
          </section>

          <section v-if="resultAvailable && finalResult" class="panel simple-panel simple-result-panel">
            <div class="simple-result-head">
              <div>
                <h2>{{ t("final_result") }}</h2>
                <p>{{ t("audit_completed") }}</p>
              </div>

              <div class="badge-row">
                <span class="badge status-badge" :class="`status-badge--${finalResult.status || 'pending'}`">{{ statusLabel(finalResult.status || "pending") }}</span>
                <span v-if="finalResult.needs_human_review" class="badge badge--toolbar">{{ t("needs_review") }}</span>
              </div>
            </div>

            <section v-if="finalResult.project_overview_text" class="section">
              <h4>{{ t("about_project") }}</h4>
              <p>{{ finalResult.project_overview_text }}</p>
            </section>

            <section class="section">
              <h4>{{ t("explanation") }}</h4>
              <p>{{ finalResult.explanation_text || finalResult.summary || t("no_result_summary") }}</p>
            </section>

            <section v-if="resultSignalItems.length" class="section">
              <h4>{{ t("facts_and_notes") }}</h4>
              <div class="simple-signal-grid">
                <article v-for="item in resultSignalItems" :key="item.key" class="simple-signal-card">
                  <div class="simple-signal-card__head">
                    <span class="kv-label">{{ item.label }}</span>
                    <a v-if="item.url" :href="item.url" target="_blank" rel="noreferrer">{{ t("source_link") }}</a>
                  </div>
                  <div class="kv-value">{{ item.value || item.url || "-" }}</div>
                  <div v-if="item.notes.length" class="simple-signal-card__notes">
                    <article v-for="note in item.notes" :key="`${item.key}-${note.key}-${note.text}`" class="simple-signal-note">
                      <p>{{ note.text }}</p>
                      <a v-if="note.url && note.url !== item.url" :href="note.url" target="_blank" rel="noreferrer">{{ t("source_link") }}</a>
                    </article>
                  </div>
                </article>
              </div>
            </section>

            <section v-if="simpleResultCards.length" class="section section--compact">
              <h4>{{ t("important_metrics") }}</h4>
              <div class="simple-metric-grid">
                <div v-for="item in simpleResultCards" :key="item.key" class="simple-metric-card">
                  <span class="kv-label">{{ item.label }}</span>
                  <div class="kv-value">{{ item.value }}</div>
                </div>
              </div>
            </section>

            <section v-if="simpleStrengths.length" class="section">
              <h4>{{ t("top_strengths") }}</h4>
              <div class="badge-row">
                <span v-for="item in simpleStrengths" :key="item" class="badge badge--presentation">{{ item }}</span>
              </div>
            </section>

            <section v-if="simpleRisks.length" class="section">
              <h4>{{ t("main_risks") }}</h4>
              <div class="badge-row">
                <span v-for="item in simpleRisks" :key="item" class="badge stage-progress-chip stage-progress-chip--error">{{ item }}</span>
              </div>
            </section>

            <section v-if="simpleNextChecks.length" class="section">
              <h4>{{ t("recommended_next_checks") }}</h4>
              <div class="badge-row">
                <span v-for="item in simpleNextChecks" :key="item" class="badge stage-progress-chip stage-progress-chip--running">{{ item }}</span>
              </div>
            </section>

            <div class="simple-action-row simple-action-row--footer">
              <button type="button" class="button-ghost" @click="openAdvancedBrowser">{{ t("open_advanced_report_browser") }}</button>
              <button type="button" @click="openSimpleDiscovery">{{ t("new_audit") }}</button>
            </div>
          </section>

          <section v-else-if="simpleRunStatus === 'error'" class="panel simple-panel simple-result-panel">
            <div class="simple-result-head">
              <div>
                <h2>{{ t("audit_stopped_with_error") }}</h2>
                <p>{{ runErrorText || t("audit_error_before_result") }}</p>
              </div>
              <span class="badge status-badge status-badge--error">{{ statusLabel("error") }}</span>
            </div>

            <div class="simple-action-row simple-action-row--footer">
              <button type="button" class="button-ghost" @click="openAdvancedBrowser">{{ t("open_advanced_report_browser") }}</button>
            </div>
          </section>

          <section v-else class="panel simple-panel simple-result-panel">
            <div class="simple-result-head">
              <div>
                <h2>{{ t("final_result_on_the_way") }}</h2>
                <p>{{ t("final_result_on_the_way_subtitle") }}</p>
              </div>
              <span class="badge status-badge" :class="`status-badge--${simpleRunStatus}`">{{ statusLabel(simpleRunStatus) }}</span>
            </div>
          </section>
        </template>

        <section v-else class="panel simple-panel simple-loading-panel">
          <div class="simple-result-head">
            <div>
              <h2>{{ t("loading_audit") }}</h2>
              <p>{{ t("loading_audit_subtitle") }}</p>
            </div>
            <span class="badge status-badge status-badge--running">{{ statusLabel("running") }}</span>
          </div>
        </section>
      </section>
    </template>

    <template v-else>
      <header class="topbar">
        <RunToolbar
          :runs="store.runs"
          :run-id="store.currentRunId"
          :view-mode="store.viewMode"
          :run-status="store.currentPayload?.run.status || 'pending'"
          :loading="store.loadingRuns || store.loadingRun"
          :has-workflow="Boolean(store.workflow?.stages?.length)"
          :has-result="resultAvailable"
          @select-run="handleSelectRun"
          @change-view="store.setViewMode"
          @change-locale="handleLocaleChange"
          @reload="store.reloadCurrent"
          @scroll-current-stage="scrollToCurrentStage"
          @scroll-result="scrollToResult"
          @open-new="openNewDialog"
        />

        <div class="overview">
          <span v-for="[label, value] in overviewChips" :key="label" class="chip">
            <span class="chip-label">{{ label }}</span>
            {{ value }}
          </span>
        </div>

        <p v-if="runErrorText" class="form-error top-error">{{ runErrorText }}</p>
        <p v-if="store.fatalError" class="form-error top-error">{{ store.fatalError }}</p>
      </header>

      <div v-if="telegramCompact" class="compact-pane-toggle">
        <button
          type="button"
          :class="['compact-pane-toggle__button', { 'compact-pane-toggle__button--active': compactPane === 'workflow' }]"
          @click="setCompactPane('workflow')"
        >
          {{ t("workflow") }}
        </button>
        <button
          type="button"
          :class="['compact-pane-toggle__button', { 'compact-pane-toggle__button--active': compactPane === 'inspector' }]"
          @click="setCompactPane('inspector')"
        >
          {{ t("inspector") }}
        </button>
      </div>

      <main class="content" :class="{ 'content--compact': telegramCompact }">
        <section class="workflow-panel panel" :class="{ 'panel--hidden': telegramCompact && compactPane !== 'workflow' }">
          <div class="panel-header">
            <div>
              <h2>{{ t("execution_model") }}</h2>
              <p>{{ graphCaption }}</p>
            </div>
          </div>

          <div v-if="activeStageSummary" class="run-progress-banner">
            <div>
              <strong>{{ activeStageSummary }}</strong>
              <p>{{ activeStageDetail }}</p>
            </div>
            <span class="badge status-badge" :class="`status-badge--${activeStage?.runtime?.status || 'pending'}`">
              {{ statusLabel(activeStage?.runtime?.status || "pending") }}
            </span>
          </div>

          <div ref="graphWrapper" class="graph-wrapper">
            <WorkflowBoard
              :workflow="store.workflow"
              :view-mode="store.viewMode"
              :selected-unit-id="store.currentUnitId"
              :selected-stage-key="store.currentStageKey"
              @select-unit="handleSelectUnit"
              @select-stage="handleSelectStage"
            />

            <section ref="resultSection" class="result-section">
              <div class="result-section__head">
                <div>
                  <h3>{{ t("result") }}</h3>
                  <p>{{ t("result_subtitle") }}</p>
                </div>
                <span class="badge status-badge" :class="`status-badge--${finalResult?.status || 'pending'}`">
                  {{ statusLabel(finalResult?.status || "pending") }}
                </span>
              </div>

              <template v-if="resultAvailable && finalResult">
                <section v-if="finalResult.project_overview_text" class="section">
                  <h4>{{ t("about_project") }}</h4>
                  <p>{{ finalResult.project_overview_text }}</p>
                </section>

                <section class="section">
                  <h4>{{ t("explanation") }}</h4>
                  <p>{{ finalResult.explanation_text || finalResult.summary || t("no_result_summary") }}</p>
                </section>

                <section v-if="resultSignalItems.length" class="section">
                  <h4>{{ t("facts_and_notes") }}</h4>
                  <div class="result-summary-grid">
                    <article v-for="item in resultSignalItems" :key="item.key" class="kv-card">
                      <div class="simple-signal-card__head">
                        <span class="kv-label">{{ item.label }}</span>
                        <a v-if="item.url" :href="item.url" target="_blank" rel="noreferrer">{{ t("source_link") }}</a>
                      </div>
                      <div class="kv-value">{{ item.value || item.url || "-" }}</div>
                      <div v-if="item.notes.length" class="simple-signal-card__notes">
                        <article v-for="note in item.notes" :key="`${item.key}-${note.key}-${note.text}`" class="simple-signal-note">
                          <p>{{ note.text }}</p>
                          <a v-if="note.url && note.url !== item.url" :href="note.url" target="_blank" rel="noreferrer">{{ t("source_link") }}</a>
                        </article>
                      </div>
                    </article>
                  </div>
                </section>

                <div class="result-summary-grid">
                  <div class="kv-card">
                    <span class="kv-label">{{ t("project_type") }}</span>
                    <div class="kv-value">{{ resolvedProjectTypeLabel }}</div>
                  </div>
                  <div class="kv-card">
                    <span class="kv-label">{{ formatMetricLabel("overall_score") }}</span>
                    <div class="kv-value">{{ asText(finalResult.overall_score) }}</div>
                  </div>
                  <div class="kv-card">
                    <span class="kv-label">{{ formatMetricLabel("risk_level") }}</span>
                    <div class="kv-value">{{ tokenLabel(finalResult.risk_level || asText(store.currentPayload?.overview.risk_level)) }}</div>
                  </div>
                  <div class="kv-card">
                    <span class="kv-label">{{ formatMetricLabel("clone_risk") }}</span>
                    <div class="kv-value">{{ tokenLabel(finalResult.clone_risk || asText(store.currentPayload?.overview.clone_risk)) }}</div>
                  </div>
                </div>

                <section v-if="resultMetricEntries.length" class="section section--compact">
                  <h4>{{ t("important_metrics") }}</h4>
                  <div class="result-summary-grid">
                    <div v-for="[key, value] in resultMetricEntries" :key="key" class="kv-card">
                      <span class="kv-label">{{ formatMetricLabel(key) }}</span>
                      <div class="kv-value">{{ asText(value) }}</div>
                    </div>
                  </div>
                </section>

                <section v-if="finalResult.strengths.length" class="section">
                  <h4>{{ t("strengths") }}</h4>
                  <div class="badge-row">
                    <span v-for="item in finalResult.strengths" :key="item" class="badge badge--presentation">{{ tokenLabel(item) }}</span>
                  </div>
                </section>

                <section v-if="finalResult.risks.length" class="section">
                  <h4>{{ t("risks") }}</h4>
                  <div class="badge-row">
                    <span v-for="item in finalResult.risks" :key="item" class="badge stage-progress-chip stage-progress-chip--error">{{ tokenLabel(item) }}</span>
                  </div>
                </section>

                <section v-if="finalResult.next_checks.length" class="section">
                  <h4>{{ t("next_checks") }}</h4>
                  <div class="badge-row">
                    <span v-for="item in finalResult.next_checks" :key="item" class="badge stage-progress-chip stage-progress-chip--running">{{ tokenLabel(item) }}</span>
                  </div>
                </section>

                <section v-if="finalResult.flags.length" class="section">
                  <h4>{{ t("flags") }}</h4>
                  <div class="badge-row">
                    <span v-for="item in finalResult.flags" :key="item" class="badge">{{ tokenLabel(item) }}</span>
                  </div>
                </section>

                <section v-if="finalResult.closest_projects.length" class="section">
                  <h4>{{ t("closest_projects") }}</h4>
                  <div class="nested-summary">
                    <div v-for="item in finalResult.closest_projects" :key="`${item.case_id}-${item.github_repo}`" class="stage-entity-chip">
                      <strong>{{ item.name || item.case_id }}</strong>
                      <span>{{ tokenLabel(item.project_type || "-") }}</span>
                      <span>{{ item.github_repo || "-" }}</span>
                      <span>{{ t("similarity") }} {{ asText(item.overall_similarity) }}</span>
                    </div>
                  </div>
                </section>
              </template>

              <p v-else class="inspector-subtitle">
                {{
                  store.currentPayload?.run.status === "running"
                    ? t("no_final_result_running")
                    : t("no_final_result")
                }}
              </p>
            </section>
          </div>
        </section>

        <aside class="inspector-panel panel" :class="{ 'panel--hidden': telegramCompact && compactPane !== 'inspector' }">
          <InspectorPanel
            :detail="store.currentDetail"
            :raw-mode="store.rawMode"
            :loading="store.loadingDetail"
            @select-unit="handleSelectUnit"
            @update:raw-mode="store.setRawMode"
          />
        </aside>
      </main>

      <NewRunDialog
        :open="newDialogOpen"
        :discovery-pending="store.discoveryPending"
        :discovery-error="store.discoveryError"
        :discovery-query="store.discoveryQuery"
        :discovery-summary="store.discoverySummary"
        :discovery-source-statuses="store.discoverySourceStatuses"
        :candidates="store.discoveryCandidates"
        :selected-candidate-key="store.selectedDiscoveryCandidateKey"
        :selected-candidate="store.selectedDiscoveryCandidate"
        :create-pending="store.createPending"
        :create-error="store.createError"
        @close="newDialogOpen = false"
        @discover="handleDiscover"
        @select-candidate="store.selectDiscoveryCandidate"
        @continue="handleContinueDeepCheck"
      />
    </template>
  </div>
</template>
