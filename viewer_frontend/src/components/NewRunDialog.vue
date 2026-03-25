<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { statusLabel, t, tokenLabel } from "../viewer-utils";
import type { DiscoveryCandidate, DiscoverySourceStatus } from "../types";

const props = defineProps<{
  open: boolean;
  discoveryPending: boolean;
  discoveryError: string;
  discoveryQuery: string;
  discoverySummary: string;
  discoverySourceStatuses: Record<string, DiscoverySourceStatus>;
  candidates: DiscoveryCandidate[];
  selectedCandidateKey: string;
  selectedCandidate: DiscoveryCandidate | null;
  createPending: boolean;
  createError: string;
}>();

const emit = defineEmits<{
  close: [];
  discover: [query: string];
  "select-candidate": [candidateKey: string];
  continue: [];
}>();

const query = ref("");

watch(
  () => props.open,
  (open) => {
    document.body.style.overflow = open ? "hidden" : "";
    if (!open) {
      return;
    }
    query.value = "";
  },
);

onBeforeUnmount(() => {
  document.body.style.overflow = "";
});

const hasFreshCandidates = computed(() => {
  if (!props.candidates.length) {
    return false;
  }
  return query.value.trim() !== "" && query.value.trim() === props.discoveryQuery;
});

const searchNeedsRefresh = computed(() => {
  if (!props.candidates.length) {
    return false;
  }
  return query.value.trim() !== "" && query.value.trim() !== props.discoveryQuery;
});

const sourceStatusEntries = computed(() => {
  const labelMap: Record<string, string> = {
    user_input: t("project_input"),
    registry: t("registry_source"),
    github_search: t("github"),
    direct_github: t("direct_github"),
    coingecko: "CoinGecko",
    geckoterminal: "GeckoTerminal",
    public_web: t("public_ton_web"),
  };
  return Object.entries(props.discoverySourceStatuses || {}).map(([key, value]) => ({
    key,
    label: labelMap[key] || key,
    status: value.status || "pending",
    candidateCount: Number(value.candidate_count || 0),
    summary: value.summary || "",
  }));
});

const canContinue = computed(() => {
  if (props.discoveryPending || props.createPending || !hasFreshCandidates.value || props.candidates.length === 0) {
    return false;
  }
  const candidate = props.selectedCandidate;
  if (!candidate || !props.selectedCandidateKey) {
    return false;
  }
  return Boolean(candidate.github_repo || candidate.project_url || candidate.telegram_handle || candidate.wallet_address);
});

const showContinueButton = computed(() => {
  return props.createPending || hasFreshCandidates.value;
});

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

function tonviewerAddressUrl(value: string): string {
  const normalized = String(value || "").trim();
  return normalized ? `https://tonviewer.com/${normalized}` : "";
}

function runDiscovery(): void {
  emit("discover", query.value.trim());
}
</script>

<template>
  <div v-if="open" class="modal-backdrop" @click.self="emit('close')">
    <section class="modal-card modal-card--discovery">
      <div class="modal-header">
        <div>
          <h3>{{ t("new_analysis") }}</h3>
          <p>{{ t("free_form_desc") }}</p>
        </div>
        <button type="button" class="button-ghost" @click="emit('close')">{{ t("close") }}</button>
      </div>

      <label class="field field--full">
        <span>{{ t("free_form_project_input") }}</span>
        <textarea
          v-model.trim="query"
          rows="4"
          :placeholder="t('project_input_placeholder')"
        />
      </label>

      <div class="modal-note">
        <span class="badge badge--toolbar">{{ t("llm_assisted") }}</span>
        <span class="badge badge--toolbar">{{ t("live_llm_ranking") }}</span>
        <span class="badge badge--toolbar">{{ t("market_token_discovery") }}</span>
        <span class="badge badge--toolbar">{{ t("deep_check_after_confirmation") }}</span>
      </div>

      <div class="modal-actions modal-actions--top">
        <button type="button" class="button-primary" :disabled="discoveryPending || !query" @click="runDiscovery">
          {{ discoveryPending ? t("searching") : t("find_project_candidates") }}
        </button>
      </div>

      <div v-if="discoveryPending || createPending || searchNeedsRefresh" class="process-banner">
        <span class="process-banner__dot" :class="{ 'process-banner__dot--muted': searchNeedsRefresh && !discoveryPending && !createPending }"></span>
        <span v-if="discoveryPending">{{ t("search_progress") }}</span>
        <span v-else-if="createPending">{{ t("start_validation_progress") }}</span>
        <span v-else>{{ t("input_changed_refresh") }}</span>
      </div>

      <p v-if="discoveryError" class="form-error">{{ discoveryError }}</p>

      <section v-if="discoveryQuery && sourceStatusEntries.length" class="candidate-section">
        <div class="candidate-section-head">
          <h4>{{ t("discovery_status") }}</h4>
          <p>{{ discoverySummary || t("candidate_search_completed") }}</p>
        </div>

        <div class="discovery-status-list">
          <article v-for="item in sourceStatusEntries" :key="item.key" class="discovery-status-card">
            <div class="discovery-status-card__head">
              <strong>{{ item.label }}</strong>
              <span class="badge" :class="`status-badge--${item.status}`">
                {{ statusLabel(item.status) }}
              </span>
            </div>
            <div class="discovery-status-card__meta">{{ t("detected_candidates") }}: {{ item.candidateCount }}</div>
            <p>{{ item.summary || t("no_extra_details") }}</p>
          </article>
        </div>
      </section>

      <section v-if="hasFreshCandidates" class="candidate-section">
        <div class="candidate-section-head">
          <h4>{{ t("project_candidates") }}</h4>
          <p>{{ discoverySummary }}</p>
        </div>

        <label class="field field--full">
          <span>{{ t("choose_most_relevant_project") }}</span>
          <select
            :value="selectedCandidateKey"
            :disabled="discoveryPending"
            @change="emit('select-candidate', String(($event.target as HTMLSelectElement).value || ''))"
          >
            <option v-for="candidate in candidates" :key="candidate.candidate_key" :value="candidate.candidate_key">
              {{ candidate.name }} | {{ t("score") }} {{ candidate.score.toFixed(2) }}
            </option>
          </select>
        </label>

        <article v-if="selectedCandidate" class="candidate-card">
          <div class="candidate-card-head">
            <div>
              <h4>{{ selectedCandidate.name }}</h4>
              <p>{{ selectedCandidate.match_reason || t("selected_project_candidate") }}</p>
            </div>
            <span class="badge badge--toolbar">{{ t("score") }} {{ selectedCandidate.score.toFixed(2) }}</span>
          </div>

          <div class="candidate-grid">
            <div class="kv-card">
              <span class="kv-label">{{ t("website") }}</span>
              <div class="kv-value">
                <a v-if="selectedCandidate.project_url" :href="externalUrl(selectedCandidate.project_url)" target="_blank" rel="noreferrer">
                  {{ selectedCandidate.project_url }}
                </a>
                <span v-else>-</span>
              </div>
            </div>
            <div class="kv-card">
              <span class="kv-label">{{ t("github") }}</span>
              <div class="kv-value">
                <a v-if="selectedCandidate.github_repo" :href="githubRepoUrl(selectedCandidate.github_repo)" target="_blank" rel="noreferrer">
                  {{ selectedCandidate.github_repo }}
                </a>
                <span v-else>-</span>
              </div>
            </div>
            <div class="kv-card">
              <span class="kv-label">{{ t("telegram") }}</span>
              <div class="kv-value">
                <a v-if="selectedCandidate.telegram_handle" :href="telegramHandleUrl(selectedCandidate.telegram_handle)" target="_blank" rel="noreferrer">
                  {{ telegramHandleLabel(selectedCandidate.telegram_handle) }}
                </a>
                <span v-else>-</span>
              </div>
            </div>
            <div class="kv-card">
              <span class="kv-label">{{ t("wallet") }}</span>
              <div class="kv-value">
                <a v-if="selectedCandidate.wallet_address" :href="tonviewerAddressUrl(selectedCandidate.wallet_address)" target="_blank" rel="noreferrer">
                  {{ selectedCandidate.wallet_address }}
                </a>
                <span v-else>{{ t("not_resolved_yet") }}</span>
              </div>
            </div>
            <div class="kv-card">
              <span class="kv-label">{{ t("project_type") }}</span>
              <div class="kv-value">{{ tokenLabel(selectedCandidate.project_type || "-") }}</div>
            </div>
          </div>

          <div class="candidate-meta">
            <span v-for="sourceLabel in selectedCandidate.source_labels" :key="sourceLabel" class="badge badge--presentation">
              {{ tokenLabel(sourceLabel) }}
            </span>
          </div>

          <p class="candidate-description">{{ selectedCandidate.description || t("no_extra_candidate_description") }}</p>
          <p v-if="!selectedCandidate.wallet_address" class="candidate-description">
            {{ t("unresolved_wallet_note") }}
          </p>
        </article>
      </section>

      <p v-if="createError" class="form-error">{{ createError }}</p>

      <div class="modal-actions modal-actions--footer">
        <button type="button" class="button-ghost" @click="emit('close')">{{ t("cancel") }}</button>
        <button
          v-if="showContinueButton"
          type="button"
          class="button-primary"
          :disabled="createPending || !canContinue"
          @click="emit('continue')"
        >
          {{ createPending ? t("starting_deep_check") : t("continue_deep_check") }}
        </button>
      </div>
    </section>
  </div>
</template>
