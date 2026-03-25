<script setup lang="ts">
import { computed } from "vue";
import StageColumn from "./StageColumn.vue";
import { t } from "../viewer-utils";
import type { StageSelection, ViewMode, WorkflowPlan, WorkflowUnit } from "../types";

const props = defineProps<{
  workflow: WorkflowPlan | null;
  viewMode: ViewMode;
  selectedUnitId: string;
  selectedStageKey: string;
}>();

const emit = defineEmits<{
  "select-unit": [unitId: string];
  "select-stage": [selection: StageSelection];
}>();

const unitMap = computed(() => new Map((props.workflow?.units || []).map((unit) => [unit.unit_id, unit])));

function unitsForStage(unitIds: string[]): WorkflowUnit[] {
  return unitIds.map((unitId) => unitMap.value.get(unitId)).filter(Boolean) as WorkflowUnit[];
}
</script>

<template>
  <div v-if="workflow?.units?.length" class="stage-board">
    <StageColumn
      v-for="stage in workflow.stages"
      :key="stage.stage_id"
      :stage="stage"
      :units="unitsForStage(stage.unit_ids)"
      :view-mode="viewMode"
      :selected-unit-id="selectedUnitId"
      :selected-stage-key="selectedStageKey"
      @select-unit="emit('select-unit', $event)"
      @select-stage="emit('select-stage', $event)"
    />
  </div>

  <div v-else class="empty-state">
    {{ t("no_workflow_available") }}
  </div>
</template>
