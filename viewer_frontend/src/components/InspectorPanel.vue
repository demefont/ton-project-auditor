<script setup lang="ts">
import { computed } from "vue";
import {
  entityLabel,
  formatDuration,
  localizedStageDescription,
  localizedUnitDescription,
  prettyJson,
  nestedStageCountLabel,
  nestedUnitCountLabel,
  primaryTypeLabel,
  secondaryPresentationBadges,
  statusLabel,
  t,
  unitAiUsageSummary,
  unitRoleLabel,
  unitTopologyLabel,
  unitTopologyNote,
  unitTypeLabel,
} from "../viewer-utils";
import type { BlockDetail, InspectorDetail, StageDetail } from "../types";

const props = defineProps<{
  detail: InspectorDetail | null;
  rawMode: boolean;
  loading: boolean;
}>();

const emit = defineEmits<{
  "update:raw-mode": [value: boolean];
  "select-unit": [unitId: string];
}>();

const isStageDetail = computed(() => Boolean(props.detail && "detail_type" in props.detail && props.detail.detail_type === "stage"));
const unitDetail = computed(() => (isStageDetail.value ? null : (props.detail as BlockDetail | null)));
const stageDetail = computed(() => (isStageDetail.value ? (props.detail as StageDetail) : null));

const stageLabel = computed(() => {
  const index = unitDetail.value?.stage && "index" in unitDetail.value.stage ? Number(unitDetail.value.stage.index) : Number.NaN;
  return Number.isFinite(index) ? t("stage_number", { index }) : "-";
});

const hasLlmTrace = computed(() => {
  const trace = unitDetail.value?.llm_trace;
  return Boolean(trace && typeof trace === "object" && Object.keys(trace as Record<string, unknown>).length);
});

const hasManifest = computed(() => {
  const manifest = unitDetail.value?.manifest;
  return Boolean(manifest && typeof manifest === "object" && Object.keys(manifest as Record<string, unknown>).length);
});

const unitDescription = computed(() => localizedUnitDescription(unitDetail.value));
const childUnitMap = computed(() => new Map((unitDetail.value?.child_plan?.units || []).map((item) => [item.unit_id, item])));
const childStages = computed(() => unitDetail.value?.child_plan?.stages || []);

function childUnitsForStage(unitIds: string[]) {
  return unitIds
    .map((unitId) => childUnitMap.value.get(unitId))
    .filter((item): item is NonNullable<typeof item> => Boolean(item));
}
</script>

<template>
  <div class="panel-header inspector-header">
    <div>
      <h2>{{ t("entity_inspector") }}</h2>
      <p>{{ t("inspector_desc") }}</p>
    </div>

    <label class="toggle-row">
      <input
        :checked="rawMode"
        type="checkbox"
        @change="emit('update:raw-mode', ($event.target as HTMLInputElement).checked)"
      />
      <span>{{ t("raw_json") }}</span>
    </label>
  </div>

  <div v-if="!detail" class="inspector-empty">
    {{ loading ? t("loading_entity_detail") : t("select_entity_detail") }}
  </div>

  <div v-else-if="isStageDetail && stageDetail" class="inspector">
    <div class="inspector-title-row">
      <div>
        <h3>{{ stageDetail.name }}</h3>
        <div class="inspector-subtitle">{{ stageDetail.description || t("no_description") }}</div>
      </div>
      <span class="badge">{{ stageDetail.stage_id }}</span>
    </div>

    <div class="badge-row">
      <span class="badge badge--marker" :class="`badge--marker-${stageDetail.presentation?.kind || 'deterministic'}`">
        {{ primaryTypeLabel(stageDetail.presentation) }}
      </span>
      <span class="badge status-badge" :class="`status-badge--${stageDetail.runtime?.status || 'pending'}`">
        {{ statusLabel(stageDetail.runtime?.status || "pending") }}
      </span>
      <span class="badge">{{ t("stage") }}</span>
      <span class="badge">{{ formatDuration(stageDetail.runtime?.duration_ms) }}</span>
      <span class="badge">{{ rawMode ? t("raw") : t("preview") }}</span>
      <span v-for="badge in secondaryPresentationBadges(stageDetail.presentation)" :key="badge" class="badge badge--presentation">
        {{ badge }}
      </span>
    </div>

    <section class="section">
      <h4>{{ t("what_stage_does") }}</h4>
      <p>{{ stageDetail.description || t("no_description") }}</p>
      <div class="badge-row badge-row--section">
        <span class="badge">{{ stageDetail.stage_label }}</span>
        <span class="badge">{{ stageDetail.parent_unit_name || t("root_workflow") }}</span>
      </div>
    </section>

    <section class="section">
      <h4>{{ t("units_in_stage") }}</h4>
      <div class="nested-summary">
        <button
          v-for="unit in stageDetail.units"
          :key="unit.unit_id"
          type="button"
          class="stage-entity-chip"
          @click="emit('select-unit', unit.unit_id)"
        >
          <strong>{{ unit.name }}</strong>
          <span>{{ unit.status }}</span>
          <span>{{ unit.summary }}</span>
        </button>
      </div>
    </section>

    <details class="section" :open="!rawMode">
      <summary>{{ t("stage_json") }}</summary>
      <pre>{{ prettyJson(stageDetail.raw_payload) }}</pre>
    </details>
  </div>

  <div v-else-if="unitDetail" class="inspector">
    <div class="inspector-title-row">
      <div>
        <h3>{{ entityLabel(unitDetail.unit_id, unitDetail.name || unitDetail.unit_id) }}</h3>
        <div class="inspector-subtitle">{{ unitDescription }}</div>
      </div>
      <span class="badge">{{ unitDetail.unit_id }}</span>
    </div>

    <div class="badge-row">
      <span class="badge badge--marker" :class="`badge--marker-${unitDetail.presentation?.kind || 'deterministic'}`">
        {{ primaryTypeLabel(unitDetail.presentation) }}
      </span>
      <span class="badge status-badge" :class="`status-badge--${unitDetail.runtime?.status || unitDetail.result?.status || 'pending'}`">
        {{ statusLabel(unitDetail.runtime?.status || unitDetail.result?.status || "pending") }}
      </span>
      <span class="badge">{{ unitTypeLabel(unitDetail) }}</span>
      <span v-if="unitDetail.unit_type === 'composite'" class="badge">{{ unitTopologyLabel(unitDetail, true) }}</span>
      <span class="badge">{{ unitRoleLabel(unitDetail) }}</span>
      <span class="badge">{{ formatDuration(unitDetail.runtime?.duration_ms) }}</span>
      <span class="badge">{{ rawMode ? t("raw") : t("preview") }}</span>
      <span v-for="badge in secondaryPresentationBadges(unitDetail.presentation)" :key="badge" class="badge badge--presentation">
        {{ badge }}
      </span>
    </div>

    <section class="section">
      <h4>{{ t("what_entity_does") }}</h4>
      <p>{{ unitDescription }}</p>
      <p class="section-secondary">{{ unitTopologyNote(unitDetail) }}</p>
      <div class="badge-row badge-row--section">
        <span class="badge">{{ stageLabel }}</span>
        <span class="badge">{{ unitDetail.parent_unit_id || t("root_workflow") }}</span>
      </div>
    </section>

    <section class="section">
      <h4>{{ t("block_structure") }}</h4>
      <div class="inspector-grid">
        <div class="kv-card">
          <div class="kv-label">{{ t("entity_type_label") }}</div>
          <div class="kv-value">{{ unitTypeLabel(unitDetail) }}</div>
        </div>
        <div class="kv-card">
          <div class="kv-label">{{ t("entity_role_label") }}</div>
          <div class="kv-value">{{ unitRoleLabel(unitDetail) }}</div>
        </div>
        <div class="kv-card">
          <div class="kv-label">{{ t("internal_topology_label") }}</div>
          <div class="kv-value">{{ unitTopologyLabel(unitDetail) }}</div>
        </div>
        <div class="kv-card">
          <div class="kv-label">{{ t("nested_stages_label") }}</div>
          <div class="kv-value">{{ nestedStageCountLabel(unitDetail) }}</div>
        </div>
        <div class="kv-card">
          <div class="kv-label">{{ t("nested_units_label") }}</div>
          <div class="kv-value">{{ nestedUnitCountLabel(unitDetail) }}</div>
        </div>
        <div class="kv-card">
          <div class="kv-label">{{ t("ai_usage_label") }}</div>
          <div class="kv-value">{{ unitAiUsageSummary(unitDetail) }}</div>
        </div>
      </div>
    </section>

    <section class="section">
      <h4>{{ unitDetail.unit_type === "composite" ? t("nested_execution") : t("latest_result") }}</h4>
      <template v-if="unitDetail.unit_type === 'composite'">
        <div v-if="childStages.length" class="nested-summary">
          <div v-for="stage in childStages" :key="stage.stage_id" class="mini-stage-summary">
            <strong>{{ t("child_stage", { index: stage.index }) }}</strong>
            <span>{{ localizedStageDescription(stage, childUnitsForStage(stage.unit_ids), unitDetail) }}</span>
          </div>
        </div>
        <p v-else>{{ t("no_child_units") }}</p>
      </template>
      <template v-else>
        <p>{{ unitDetail.result?.summary || t("no_result_summary") }}</p>
      </template>
    </section>

    <section class="section">
      <h4>{{ t("inputs_outputs") }}</h4>
      <div class="port-section">
        <div class="port-section-title">{{ t("inputs") }}</div>
        <div v-if="unitDetail.input_ports.length" class="port-row">
          <span v-for="port in unitDetail.input_ports" :key="`in-${port.name}`" class="port-chip" :title="port.description">
            {{ port.name }}
          </span>
        </div>
        <p v-else class="inspector-subtitle">{{ t("inputs_none") }}</p>
      </div>

      <div class="port-section">
        <div class="port-section-title">{{ t("outputs") }}</div>
        <div v-if="unitDetail.output_ports.length" class="port-row">
          <span v-for="port in unitDetail.output_ports" :key="`out-${port.name}`" class="port-chip" :title="port.description">
            {{ port.name }}
          </span>
        </div>
        <p v-else class="inspector-subtitle">{{ t("outputs_none") }}</p>
      </div>
    </section>

    <section class="section">
      <h4>{{ t("dependencies_scope") }}</h4>

      <div class="edge-section">
        <div class="port-section-title">{{ t("upstream") }}</div>
        <div v-if="unitDetail.upstream_edges.length" class="edge-list">
          <div v-for="edge in unitDetail.upstream_edges" :key="`up-${edge.source_unit_id}-${edge.target_unit_id}-${edge.target_port}`" class="edge-item">
            <div class="edge-main">
              <span>{{ edge.source_unit_id }}.{{ edge.source_port }}</span>
              <span class="edge-arrow">→</span>
              <span>{{ edge.target_unit_id }}.{{ edge.target_port }}</span>
            </div>
            <div class="edge-note">{{ edge.description || edge.kind }}</div>
          </div>
        </div>
        <p v-else class="inspector-subtitle">{{ t("upstream_none") }}</p>
      </div>

      <div class="edge-section">
        <div class="port-section-title">{{ t("downstream") }}</div>
        <div v-if="unitDetail.downstream_edges.length" class="edge-list">
          <div v-for="edge in unitDetail.downstream_edges" :key="`down-${edge.source_unit_id}-${edge.target_unit_id}-${edge.target_port}`" class="edge-item">
            <div class="edge-main">
              <span>{{ edge.source_unit_id }}.{{ edge.source_port }}</span>
              <span class="edge-arrow">→</span>
              <span>{{ edge.target_unit_id }}.{{ edge.target_port }}</span>
            </div>
            <div class="edge-note">{{ edge.description || edge.kind }}</div>
          </div>
        </div>
        <p v-else class="inspector-subtitle">{{ t("downstream_none") }}</p>
      </div>
    </section>

    <details v-if="hasManifest" class="section">
      <summary>{{ t("manifest_json") }}</summary>
      <pre>{{ prettyJson(unitDetail.manifest) }}</pre>
    </details>

    <details class="section">
      <summary>{{ t("trace_input_json") }}</summary>
      <pre>{{ prettyJson(unitDetail.trace_input) }}</pre>
    </details>

    <details class="section" open>
      <summary>{{ t("trace_output_json") }}</summary>
      <pre>{{ prettyJson(unitDetail.trace_output) }}</pre>
    </details>

    <details v-if="hasLlmTrace" class="section">
      <summary>{{ t("llm_prompt_response") }}</summary>
      <pre>{{ prettyJson(unitDetail.llm_trace) }}</pre>
    </details>
  </div>
</template>
