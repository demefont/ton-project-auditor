import type {
  BlockDetail,
  CreateRunRequest,
  DiscoverySearchResponse,
  DiscoverySessionPayload,
  RunPayload,
  RunSummary,
} from "./types";
import { viewerLocale } from "./viewer-utils";

const appBaseUrl = import.meta.env.BASE_URL || "/";

function withBase(path: string): string {
  const normalizedBase = appBaseUrl.endsWith("/") ? appBaseUrl : `${appBaseUrl}/`;
  return `${normalizedBase}${String(path || "").replace(/^\/+/, "")}`;
}

function withLocaleQuery(path: string): string {
  const url = new URL(withBase(path), window.location.origin);
  if (viewerLocale.value === "ru") {
    url.searchParams.set("lang", "ru");
  }
  return `${url.pathname}${url.search}`;
}

async function readJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = typeof payload?.error === "string" ? payload.error : `Request failed: ${response.status}`;
    throw new Error(message);
  }
  return payload as T;
}

export async function fetchRuns(): Promise<RunSummary[]> {
  const payload = await readJson<{ runs: RunSummary[] }>(withBase("api/runs"));
  return payload.runs || [];
}

export async function fetchRun(runId: string): Promise<RunPayload> {
  return readJson<RunPayload>(withLocaleQuery(`api/runs/${encodeURIComponent(runId)}`));
}

export async function fetchBlockDetail(runId: string, unitId: string, raw: boolean): Promise<BlockDetail> {
  const suffix = raw ? "?raw=1" : "";
  return readJson<BlockDetail>(withLocaleQuery(`api/runs/${encodeURIComponent(runId)}/blocks/${encodeURIComponent(unitId)}${suffix}`));
}

export async function createRun(request: CreateRunRequest): Promise<RunPayload> {
  return readJson<RunPayload>(withBase("api/runs/new"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      ...request,
      locale: viewerLocale.value,
    }),
  });
}

export async function searchProjectCandidates(query: string): Promise<DiscoverySearchResponse> {
  return readJson<DiscoverySearchResponse>(withBase("api/discovery/search"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query,
      llm_mode: "live",
      discovery_model: "gpt-4o-mini",
      llm_model: "gpt-4o-mini",
      speed_profile: "interactive",
    }),
  });
}

export async function startDiscoverySession(query: string): Promise<DiscoverySessionPayload> {
  return readJson<DiscoverySessionPayload>(withBase("api/discovery/sessions/new"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query,
      llm_mode: "live",
      discovery_model: "gpt-4o-mini",
      llm_model: "gpt-4o-mini",
      speed_profile: "interactive",
    }),
  });
}

export async function fetchDiscoverySession(sessionId: string): Promise<DiscoverySessionPayload> {
  return readJson<DiscoverySessionPayload>(withBase(`api/discovery/sessions/${encodeURIComponent(sessionId)}`));
}
