<script setup lang="ts">
import { computed } from "vue";
import {
  entityLabel,
  formatDuration,
  localizedStageDescription,
  localizedUnitSummary,
  makeStageSelectionKey,
  nestedStageCountLabel,
  nestedUnitCountLabel,
  primaryTypeLabel,
  secondaryPresentationBadges,
  statusLabel,
  t,
  unitTopologyLabel,
  unitTypeLabel,
} from "../viewer-utils";
import type { StageSelection, ViewMode, WorkflowUnit } from "../types";

const props = defineProps<{
  unit: WorkflowUnit;
  selected: boolean;
  selectedUnitId: string;
  viewMode: ViewMode;
  stageState: "future" | "active" | "done";
  selectedStageKey: string;
}>();

const emit = defineEmits<{
  "select-unit": [unitId: string];
  "select-stage": [selection: StageSelection];
}>();

const status = computed(() => String(props.unit.runtime?.status || props.unit.result?.status || "pending"));
const typeKind = computed(() => String(props.unit.presentation?.kind || "deterministic"));
const summary = computed(() => localizedUnitSummary(props.unit));
const childUnitMap = computed(() => new Map((props.unit.plan?.units || []).map((item) => [item.unit_id, item])));
function childStageKey(stageId: string): string {
  return makeStageSelectionKey(stageId, props.unit.unit_id);
}
function childUnitStatus(unitId: string): string {
  const child = childUnitMap.value.get(unitId);
  return String(child?.runtime?.status || child?.result?.status || "pending");
}
function childUnitsForStage(unitIds: string[]): WorkflowUnit[] {
  return unitIds
    .map((unitId) => childUnitMap.value.get(unitId))
    .filter((item): item is WorkflowUnit => Boolean(item));
}
</script>

<template>
  <article
    class="unit-card"
    :class="[
      `unit-card--${unit.unit_type || 'unknown'}`,
      `unit-card--${typeKind}`,
      `unit-card--${status}`,
      { selected, 'unit-card--analysis': viewMode === 'analysis', 'unit-card--future': status === 'pending' && stageState === 'future' },
    ]"
    @click="emit('select-unit', unit.unit_id)"
  >
    <div class="unit-head">
      <div>
        <div class="unit-title">{{ entityLabel(unit.unit_id, unit.name || unit.unit_id) }}</div>
        <span class="unit-id">{{ unit.unit_id }}</span>
      </div>

      <div class="header-badge-stack">
        <span v-if="viewMode === 'analysis'" class="badge badge--marker" :class="`badge--marker-${typeKind}`">
          {{ primaryTypeLabel(unit.presentation) }}
        </span>
        <span class="badge status-badge" :class="`status-badge--${status}`">{{ statusLabel(status) }}</span>
      </div>
    </div>

    <p class="unit-summary">{{ summary }}</p>

    <div class="unit-meta">
      <span class="badge">{{ unitTypeLabel(unit) }}</span>
      <span v-if="unit.unit_type === 'composite'" class="badge">{{ unitTopologyLabel(unit, true) }}</span>
      <span v-if="unit.unit_type === 'composite'" class="badge">{{ nestedStageCountLabel(unit) }}</span>
      <span v-if="unit.unit_type === 'composite'" class="badge">{{ nestedUnitCountLabel(unit) }}</span>
      <span class="badge">{{ formatDuration(unit.runtime?.duration_ms) }}</span>
      <span class="badge">{{ t("in_out", { inCount: unit.input_ports?.length || 0, outCount: unit.output_ports?.length || 0 }) }}</span>
      <span
        v-for="badge in secondaryPresentationBadges(unit.presentation)"
        :key="`${unit.unit_id}-${badge}`"
        class="badge badge--presentation"
      >
        {{ badge }}
      </span>
    </div>

    <div v-if="unit.unit_type === 'composite' && unit.plan?.stages?.length" class="nested-plan">
      <div
        v-for="stage in unit.plan.stages"
        :key="stage.stage_id"
        class="mini-stage"
        :class="[
          `mini-stage--${stage.runtime?.status || 'pending'}`,
          { 'mini-stage--selected': childStageKey(stage.stage_id) === selectedStageKey },
        ]"
        @click.stop="emit('select-stage', { stage_id: stage.stage_id, parent_unit_id: unit.unit_id })"
      >
        <div class="mini-stage-label">{{ t("child_stage", { index: stage.index }) }}</div>
        <div class="mini-stage-body">
          <div class="mini-stage-head">
            <span>{{ statusLabel(stage.runtime?.status || "pending") }}</span>
            <span>{{ formatDuration(stage.runtime?.duration_ms) }}</span>
          </div>
          <div class="mini-stage-text">
            {{ localizedStageDescription(stage, childUnitsForStage(stage.unit_ids), unit) }}
          </div>
          <div class="mini-stage-unit-row">
            <button
              v-for="childId in stage.unit_ids"
              :key="childId"
              type="button"
              class="mini-unit-chip"
              :class="[
                `mini-unit-chip--${childUnitStatus(childId)}`,
                { 'mini-unit-chip--selected': childId === selectedUnitId },
              ]"
              @click.stop="emit('select-unit', childId)"
            >
              {{ entityLabel(childId, childUnitMap.get(childId)?.name || childId) }}
            </button>
          </div>
        </div>
      </div>
    </div>
  </article>
</template>
