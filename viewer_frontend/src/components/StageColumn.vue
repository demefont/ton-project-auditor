<script setup lang="ts">
import { computed } from "vue";
import UnitCard from "./UnitCard.vue";
import {
  entityLabel,
  formatDuration,
  localizedStageDescription,
  makeStageSelectionKey,
  primaryTypeLabel,
  secondaryPresentationBadges,
  stageVisualState,
  statusLabel,
  statusCountByUnit,
  t,
} from "../viewer-utils";
import type { StageSelection, ViewMode, WorkflowStage, WorkflowUnit } from "../types";

const props = defineProps<{
  stage: WorkflowStage;
  units: WorkflowUnit[];
  viewMode: ViewMode;
  selectedUnitId: string;
  selectedStageKey: string;
}>();

const emit = defineEmits<{
  "select-unit": [unitId: string];
  "select-stage": [selection: StageSelection];
}>();

const visualState = computed(() => stageVisualState(props.stage));
const presentationKind = computed(() => String(props.stage.presentation?.kind || "deterministic"));
const stageSelectionKey = computed(() => makeStageSelectionKey(props.stage.stage_id));
const statusCounts = computed(() => statusCountByUnit(props.units));
const durationLabel = computed(() => {
  const raw = formatDuration(props.stage.runtime?.duration_ms);
  return String(props.stage.runtime?.status || "pending") === "running" ? t("elapsed", { duration: raw }) : raw;
});
const runningUnitNames = computed(() =>
  props.units
    .filter((unit) => String(unit.runtime?.status || unit.result?.status || "pending") === "running")
    .map((unit) => entityLabel(unit.unit_id, unit.name || unit.unit_id)),
);
const stageDescription = computed(() => {
  return localizedStageDescription(props.stage, props.units);
});
</script>

<template>
  <section
    :id="`stage-${stage.stage_id}`"
    class="stage-column"
    :class="[
      `stage-column--${presentationKind}`,
      `stage-column--${visualState}`,
      { 'stage-column--analysis': viewMode === 'analysis', 'stage-column--selected': stageSelectionKey === selectedStageKey },
    ]"
  >
    <div class="stage-header stage-header--interactive" @click="emit('select-stage', { stage_id: stage.stage_id })">
      <div>
        <h3>{{ t("stage_number", { index: stage.index }) }}</h3>
        <p>
          {{ stageDescription }}
        </p>
      </div>

      <div class="header-badge-stack">
        <span v-if="viewMode === 'analysis'" class="badge badge--marker" :class="`badge--marker-${presentationKind}`">
          {{ primaryTypeLabel(stage.presentation) }}
        </span>
        <span class="badge status-badge" :class="`status-badge--${stage.runtime?.status || 'pending'}`">
          {{
            ["success", "skipped"].includes(String(stage.runtime?.status || "pending"))
              ? "✓"
              : statusLabel(stage.runtime?.status || "pending")
          }}
        </span>
      </div>
    </div>

    <div class="stage-meta">
      <span class="badge">{{ durationLabel }}</span>
      <span class="badge">{{ t("units_count", { count: stage.unit_ids.length }) }}</span>
      <span
        v-for="badge in secondaryPresentationBadges(stage.presentation)"
        :key="`${stage.stage_id}-${badge}`"
        class="badge badge--presentation"
      >
        {{ badge }}
      </span>
    </div>

    <div v-if="statusCounts.running || statusCounts.pending || statusCounts.error || statusCounts.skipped" class="stage-progress-row">
      <span v-if="statusCounts.running" class="badge stage-progress-chip stage-progress-chip--running">{{ t("running_count", { count: statusCounts.running }) }}</span>
      <span v-if="statusCounts.pending" class="badge stage-progress-chip stage-progress-chip--pending">{{ t("pending_count", { count: statusCounts.pending }) }}</span>
      <span v-if="statusCounts.error" class="badge stage-progress-chip stage-progress-chip--error">{{ t("error_count", { count: statusCounts.error }) }}</span>
      <span v-if="statusCounts.skipped" class="badge stage-progress-chip stage-progress-chip--skipped">{{ t("skipped_count", { count: statusCounts.skipped }) }}</span>
    </div>

    <p v-if="runningUnitNames.length" class="stage-live-note">{{ t("running_now", { names: runningUnitNames.join(", ") }) }}</p>

    <div class="stage-units">
      <UnitCard
        v-for="unit in units"
        :key="unit.unit_id"
        :unit="unit"
        :selected="unit.unit_id === selectedUnitId"
        :selected-unit-id="selectedUnitId"
        :view-mode="viewMode"
        :stage-state="visualState"
        :selected-stage-key="selectedStageKey"
        @select-unit="emit('select-unit', $event)"
        @select-stage="emit('select-stage', $event)"
      />
    </div>
  </section>
</template>
