<script setup lang="ts">
import { statusLabel, t, viewerLocale } from "../viewer-utils";
import type { ViewerLocale } from "../viewer-utils";
import type { RunSummary, ViewMode } from "../types";

const props = defineProps<{
  runs: RunSummary[];
  runId: string;
  viewMode: ViewMode;
  runStatus: string;
  loading: boolean;
  hasWorkflow: boolean;
  hasResult: boolean;
}>();

const emit = defineEmits<{
  "select-run": [runId: string];
  "change-view": [viewMode: ViewMode];
  "change-locale": [locale: ViewerLocale];
  reload: [];
  "open-new": [];
  "scroll-current-stage": [];
  "scroll-result": [];
}>();
</script>

<template>
  <div class="toolbar-stack">
    <div>
      <h1>{{ t("app_title") }}</h1>
      <p>{{ t("app_subtitle") }}</p>
    </div>

    <div class="run-picker">
      <label for="run-select">{{ t("run") }}</label>
      <div class="run-picker-row">
        <select
          id="run-select"
          :value="runId"
          :disabled="loading"
          @change="emit('select-run', String(($event.target as HTMLSelectElement).value || ''))"
        >
          <option v-for="run in props.runs" :key="run.run_id" :value="run.run_id">
            {{ run.case_name }} | {{ run.status }}
          </option>
        </select>

        <select
          id="view-select"
          :value="viewMode"
          @change="emit('change-view', ($event.target as HTMLSelectElement).value as ViewMode)"
        >
          <option value="execution">{{ t("execution_view") }}</option>
          <option value="analysis">{{ t("analysis_view") }}</option>
        </select>

        <div class="locale-switch" :aria-label="t('language')">
          <button
            type="button"
            class="locale-switch__button"
            :class="{ 'locale-switch__button--active': viewerLocale === 'en' }"
            @click="emit('change-locale', 'en')"
          >
            {{ t("locale_en") }}
          </button>
          <button
            type="button"
            class="locale-switch__button"
            :class="{ 'locale-switch__button--active': viewerLocale === 'ru' }"
            @click="emit('change-locale', 'ru')"
          >
            {{ t("locale_ru") }}
          </button>
        </div>

        <button type="button" @click="emit('reload')">{{ t("reload") }}</button>
        <button type="button" class="button-ghost" :disabled="!hasWorkflow" @click="emit('scroll-current-stage')">
          {{ t("current_stage") }}
        </button>
        <button type="button" class="button-primary" :disabled="!hasResult" @click="emit('scroll-result')">{{ t("result") }}</button>
        <button type="button" @click="emit('open-new')">{{ t("new") }}</button>
      </div>

      <div class="toolbar-status-row">
        <span class="badge badge--toolbar">{{ t("latest_projects", { count: props.runs.length }) }}</span>
        <span class="badge" :class="`status-badge status-badge--${runStatus || 'pending'}`">
          {{ statusLabel(runStatus || "pending") }}
        </span>
      </div>
    </div>
  </div>
</template>
