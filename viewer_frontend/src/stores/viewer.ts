import { computed, ref } from "vue";
import { defineStore } from "pinia";
import { createRun, fetchBlockDetail, fetchDiscoverySession, fetchRun, fetchRuns, startDiscoverySession } from "../api";
import { buildStageDetail, makeStageSelectionKey, pickInitialUnit, t } from "../viewer-utils";
import type {
  CreateRunRequest,
  DiscoveryCandidate,
  DiscoverySearchResponse,
  DiscoverySessionPayload,
  DiscoverySourceStatus,
  InspectorDetail,
  RunPayload,
  RunSummary,
  StageSelection,
  ViewMode,
  WorkflowPlan,
} from "../types";

export const useViewerStore = defineStore("viewer", () => {
  const runs = ref<RunSummary[]>([]);
  const currentRunId = ref("");
  const currentPayload = ref<RunPayload | null>(null);
  const currentUnitId = ref("");
  const currentStageSelection = ref<StageSelection | null>(null);
  const currentDetail = ref<InspectorDetail | null>(null);
  const rawMode = ref(false);
  const viewMode = ref<ViewMode>("execution");
  const loadingRuns = ref(false);
  const loadingRun = ref(false);
  const loadingDetail = ref(false);
  const createPending = ref(false);
  const createError = ref("");
  const discoveryQuery = ref("");
  const discoverySummary = ref("");
  const discoveryCandidates = ref<DiscoveryCandidate[]>([]);
  const discoverySourceStatuses = ref<Record<string, DiscoverySourceStatus>>({});
  const discoverySessionId = ref("");
  const discoveryWorkflow = ref<WorkflowPlan | null>(null);
  const discoveryStatus = ref("pending");
  const selectedDiscoveryCandidateKey = ref("");
  const discoveryPending = ref(false);
  const discoveryError = ref("");
  const fatalError = ref("");

  let pollHandle: number | null = null;
  let pollInFlight = false;
  let discoveryRequestId = 0;

  const workflow = computed<WorkflowPlan | null>(() => currentPayload.value?.workflow || null);
  const currentStageKey = computed(() =>
    currentStageSelection.value
      ? makeStageSelectionKey(currentStageSelection.value.stage_id, currentStageSelection.value.parent_unit_id || "")
      : "",
  );
  const selectedDiscoveryCandidate = computed<DiscoveryCandidate | null>(() => {
    return discoveryCandidates.value.find((candidate) => candidate.candidate_key === selectedDiscoveryCandidateKey.value) || null;
  });

  function stopPolling(): void {
    if (pollHandle !== null) {
      window.clearInterval(pollHandle);
      pollHandle = null;
    }
  }

  function syncPolling(): void {
    if (currentPayload.value?.run.status === "running") {
      if (pollHandle === null) {
        pollHandle = window.setInterval(() => {
          void pollCurrentRun();
        }, 2000);
      }
      return;
    }
    stopPolling();
  }

  async function loadRuns(preferredRunId = ""): Promise<void> {
    loadingRuns.value = true;
    fatalError.value = "";
    try {
      runs.value = await fetchRuns();
      if (!runs.value.length) {
        currentRunId.value = "";
        currentPayload.value = null;
        currentUnitId.value = "";
        currentStageSelection.value = null;
        currentDetail.value = null;
        stopPolling();
        return;
      }
      const candidate =
        (preferredRunId && runs.value.some((item) => item.run_id === preferredRunId) && preferredRunId) ||
        (currentRunId.value && runs.value.some((item) => item.run_id === currentRunId.value) && currentRunId.value) ||
        runs.value[0].run_id;
      await selectRun(candidate);
    } catch (error) {
      fatalError.value = String((error as Error).message || error);
      throw error;
    } finally {
      loadingRuns.value = false;
    }
  }

  async function selectRun(runId: string, preferredUnitId = ""): Promise<void> {
    if (!runId) {
      return;
    }
    loadingRun.value = true;
    fatalError.value = "";
    stopPolling();
    try {
      const payload = await fetchRun(runId);
      currentRunId.value = runId;
      currentPayload.value = payload;
      currentStageSelection.value = null;
      const nextUnit = pickInitialUnit(payload.workflow, currentUnitId.value, preferredUnitId || payload.default_unit_id);
      if (nextUnit) {
        await selectUnit(nextUnit);
      } else {
        currentUnitId.value = "";
        currentDetail.value = null;
      }
      syncPolling();
    } catch (error) {
      fatalError.value = String((error as Error).message || error);
      throw error;
    } finally {
      loadingRun.value = false;
    }
  }

  async function selectUnit(unitId: string): Promise<void> {
    if (!currentRunId.value || !unitId) {
      return;
    }
    loadingDetail.value = true;
    try {
      currentUnitId.value = unitId;
      currentStageSelection.value = null;
      currentDetail.value = await fetchBlockDetail(currentRunId.value, unitId, rawMode.value);
    } finally {
      loadingDetail.value = false;
    }
  }

  function selectStage(selection: StageSelection): void {
    currentStageSelection.value = selection;
    currentUnitId.value = "";
    currentDetail.value = buildStageDetail(currentPayload.value?.workflow, selection);
  }

  async function setRawMode(value: boolean): Promise<void> {
    rawMode.value = value;
    if (currentUnitId.value) {
      await selectUnit(currentUnitId.value);
      return;
    }
    if (currentStageSelection.value) {
      currentDetail.value = buildStageDetail(currentPayload.value?.workflow, currentStageSelection.value);
    }
  }

  function setViewMode(value: ViewMode): void {
    viewMode.value = value;
  }

  async function refetchCurrentPayload(): Promise<void> {
    if (!currentRunId.value) {
      return;
    }
    const payload = await fetchRun(currentRunId.value);
    currentPayload.value = payload;
    if (currentStageSelection.value) {
      currentDetail.value = buildStageDetail(payload.workflow, currentStageSelection.value);
    } else if (currentUnitId.value) {
      currentDetail.value = await fetchBlockDetail(currentRunId.value, currentUnitId.value, rawMode.value);
    }
    syncPolling();
  }

  async function pollCurrentRun(): Promise<void> {
    if (pollInFlight || !currentRunId.value) {
      return;
    }
    pollInFlight = true;
    try {
      await refetchCurrentPayload();
      const payload = currentPayload.value;
      if (!payload) {
        return;
      }
      if (payload.run.status !== "running") {
        await refreshRunsList();
      }
    } catch (error) {
      fatalError.value = String((error as Error).message || error);
      stopPolling();
    } finally {
      pollInFlight = false;
    }
  }

  async function refreshRunsList(): Promise<void> {
    runs.value = await fetchRuns();
  }

  async function reloadCurrent(): Promise<void> {
    if (currentRunId.value) {
      await loadRuns(currentRunId.value);
      return;
    }
    await loadRuns();
  }

  function clearCurrentRun(): void {
    stopPolling();
    currentRunId.value = "";
    currentPayload.value = null;
    currentUnitId.value = "";
    currentStageSelection.value = null;
    currentDetail.value = null;
    loadingRun.value = false;
    loadingDetail.value = false;
    fatalError.value = "";
  }

  function resetDiscovery(): void {
    discoveryRequestId += 1;
    discoverySessionId.value = "";
    discoveryWorkflow.value = null;
    discoveryStatus.value = "pending";
    discoveryQuery.value = "";
    discoverySummary.value = "";
    discoveryCandidates.value = [];
    discoverySourceStatuses.value = {};
    selectedDiscoveryCandidateKey.value = "";
    discoveryPending.value = false;
    discoveryError.value = "";
    createError.value = "";
  }

  function applyDiscoveryPayload(payload: DiscoverySessionPayload): void {
    discoverySessionId.value = payload.session.session_id || "";
    discoveryWorkflow.value = payload.workflow || null;
    discoveryStatus.value = payload.session.status || "pending";
    discoveryQuery.value = payload.query || "";
    discoverySummary.value = payload.summary || "";
    discoveryCandidates.value = payload.candidates || [];
    discoverySourceStatuses.value = payload.source_statuses || {};
    selectedDiscoveryCandidateKey.value = payload.selected_candidate_key || "";
    discoveryError.value = payload.session.error?.summary || "";
  }

  async function waitMs(ms: number): Promise<void> {
    await new Promise((resolve) => {
      window.setTimeout(resolve, ms);
    });
  }

  async function searchDiscovery(query: string): Promise<void> {
    const normalizedQuery = query.trim();
    const requestId = ++discoveryRequestId;
    discoverySessionId.value = "";
    discoveryWorkflow.value = null;
    discoveryStatus.value = "pending";
    discoveryPending.value = true;
    discoveryError.value = "";
    createError.value = "";
    discoveryQuery.value = normalizedQuery;
    discoverySummary.value = "";
    discoveryCandidates.value = [];
    discoverySourceStatuses.value = {};
    selectedDiscoveryCandidateKey.value = "";
    try {
      let payload = await startDiscoverySession(normalizedQuery);
      if (requestId !== discoveryRequestId) {
        return;
      }
      applyDiscoveryPayload(payload);
      while (requestId === discoveryRequestId && payload.session.status === "running" && payload.session.session_id) {
        await waitMs(700);
        if (requestId !== discoveryRequestId) {
          return;
        }
        payload = await fetchDiscoverySession(payload.session.session_id);
        if (requestId !== discoveryRequestId) {
          return;
        }
        applyDiscoveryPayload(payload);
      }
      if (discoveryStatus.value === "error") {
        throw new Error(discoveryError.value || t("discovery_failed"));
      }
    } catch (error) {
      if (requestId !== discoveryRequestId) {
        return;
      }
      discoveryError.value = String((error as Error).message || error);
      throw error;
    } finally {
      if (requestId === discoveryRequestId) {
        discoveryPending.value = false;
      }
    }
  }

  function selectDiscoveryCandidate(candidateKey: string): void {
    selectedDiscoveryCandidateKey.value = candidateKey;
  }

  async function createNewRun(input: CreateRunRequest): Promise<string> {
    createPending.value = true;
    createError.value = "";
    try {
      const payload = await createRun(input);
      await loadRuns(payload.run.run_id);
      return payload.run.run_id;
    } catch (error) {
      createError.value = String((error as Error).message || error);
      throw error;
    } finally {
      createPending.value = false;
    }
  }

  async function createRunFromDiscovery(): Promise<string> {
    if (!discoveryCandidates.value.length || !selectedDiscoveryCandidateKey.value) {
      createError.value = t("select_confirmed_candidate");
      throw new Error(createError.value);
    }
    const candidate = selectedDiscoveryCandidate.value;
    if (!candidate) {
      createError.value = t("select_candidate_first");
      throw new Error(createError.value);
    }
    const actionable =
      candidate.github_repo ||
      candidate.project_url ||
      candidate.telegram_handle ||
      candidate.wallet_address;
    if (!actionable) {
      createError.value = t("insufficient_candidate_data");
      throw new Error(createError.value);
    }
    const discoveryPayload: DiscoverySearchResponse = {
      query: discoveryQuery.value,
      summary: discoverySummary.value,
      selected_candidate_key: selectedDiscoveryCandidateKey.value,
      candidates: discoveryCandidates.value,
      source_statuses: discoverySourceStatuses.value,
    };
    return createNewRun({
      project: actionable,
      name: candidate.name,
      project_url: candidate.project_url,
      telegram_handle: candidate.telegram_handle,
      wallet_address: candidate.wallet_address,
      description: discoveryQuery.value,
      type_hint: candidate.project_type,
      discovery: discoveryPayload,
      mode: "live",
      llm_mode: "live",
      llm_model: "gpt-4o-mini",
      sonar_model: "sonar",
      enable_sonar: true,
      record_snapshots: true,
      speed_profile: "interactive",
    });
  }

  function dispose(): void {
    stopPolling();
    discoveryRequestId += 1;
  }

  return {
    runs,
    currentRunId,
    currentPayload,
    currentUnitId,
    currentStageKey,
    currentDetail,
    rawMode,
    viewMode,
    loadingRuns,
    loadingRun,
    loadingDetail,
    createPending,
    createError,
    discoveryQuery,
    discoverySummary,
    discoveryCandidates,
    discoverySourceStatuses,
    discoverySessionId,
    discoveryWorkflow,
    discoveryStatus,
    selectedDiscoveryCandidateKey,
    selectedDiscoveryCandidate,
    discoveryPending,
    discoveryError,
    fatalError,
    workflow,
    loadRuns,
    selectRun,
    selectUnit,
    selectStage,
    setRawMode,
    setViewMode,
    refetchCurrentPayload,
    reloadCurrent,
    clearCurrentRun,
    resetDiscovery,
    searchDiscovery,
    selectDiscoveryCandidate,
    createNewRun,
    createRunFromDiscovery,
    dispose,
  };
});
