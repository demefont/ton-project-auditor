export type ViewMode = "execution" | "analysis";

export interface RunSummary {
  run_id: string;
  root_dir: string;
  case_id: string;
  case_name: string;
  mode: string;
  mtime: number;
  overall_score?: number | null;
  risk_level?: string | null;
  stage_count: number;
  project_key: string;
  status: string;
  is_live: boolean;
}

export interface Port {
  name: string;
  direction: string;
  contract: string;
  description: string;
  required: boolean;
}

export interface Edge {
  source_unit_id: string;
  source_port: string;
  target_unit_id: string;
  target_port: string;
  kind: string;
  description: string;
}

export interface RuntimeInfo {
  status: string;
  duration_ms?: number;
  stage_index?: number;
  unit_ids?: string[];
  unit_type?: string;
  execution_mode?: string;
}

export interface ResultInfo {
  status: string;
  summary: string;
  flags: string[];
  needs_human_review: boolean;
}

export interface PresentationInfo {
  kind?: string;
  label?: string;
  badges?: string[];
  note?: string;
  counts?: Record<string, number>;
}

export interface WorkflowStage {
  stage_id: string;
  index: number;
  unit_ids: string[];
  description: string;
  runtime?: RuntimeInfo;
  presentation?: PresentationInfo;
}

export interface WorkflowUnit {
  unit_id: string;
  name: string;
  description: string;
  kind: string;
  unit_type: string;
  execution_mode: string;
  input_ports?: Port[];
  output_ports?: Port[];
  tags?: string[];
  source_path?: string;
  manifest_path?: string;
  block_id?: string;
  manifest_dependencies?: string[];
  runtime?: RuntimeInfo;
  result?: ResultInfo;
  presentation?: PresentationInfo;
  plan?: WorkflowPlan;
}

export interface WorkflowPlan {
  plan_id: string;
  name: string;
  description: string;
  units: WorkflowUnit[];
  edges: Edge[];
  stages: WorkflowStage[];
  metadata?: Record<string, unknown>;
  presentation?: {
    unit_counts?: Record<string, number>;
    stage_counts?: Record<string, number>;
  };
}

export interface StageSelection {
  stage_id: string;
  parent_unit_id?: string;
}

export interface StageDetailUnit {
  unit_id: string;
  name: string;
  status: string;
  summary: string;
}

export interface StageDetail {
  detail_type: "stage";
  selection_key: string;
  stage_id: string;
  name: string;
  description: string;
  stage_label: string;
  parent_unit_id: string;
  parent_unit_name: string;
  runtime: RuntimeInfo;
  presentation: PresentationInfo;
  unit_ids: string[];
  units: StageDetailUnit[];
  raw_payload: unknown;
}

export interface RunMeta {
  run_id: string;
  root_dir: string;
  status: string;
  is_live: boolean;
  updated_at: number;
  project_key: string;
  error?: RunErrorInfo;
}

export interface RunErrorInfo {
  stage_id?: string;
  stage_index?: number;
  unit_id?: string;
  unit_name?: string;
  parent_unit_id?: string;
  parent_unit_name?: string;
  summary?: string;
  fatal_error?: string;
  flags?: string[];
}

export interface RunOverview {
  overall_score?: number | null;
  risk_level?: string | null;
  project_type?: string | null;
  clone_risk?: string | null;
  strengths?: string[];
  risks?: string[];
}

export interface FinalResultClosestProject {
  case_id: string;
  name: string;
  github_repo: string;
  project_type: string;
  overall_similarity?: number | null;
  url?: string;
}

export interface FinalResultFact {
  key: string;
  label: string;
  value: string;
  url: string;
}

export interface FinalResultRiskEvidence {
  key: string;
  label: string;
  summary: string;
  url: string;
}

export interface FinalResultCloneAnalysis {
  clone_risk: string;
  closest_projects: FinalResultClosestProject[];
  source_project: Partial<FinalResultClosestProject>;
  top_similarity?: number | null;
  self_declared_copy_hits: string[];
  self_declared_copy_excerpt: string;
  self_declared_copy_url: string;
  summary: string;
}

export interface FinalResult {
  status: string;
  summary: string;
  project_overview_text: string;
  explanation_text: string;
  project_name: string;
  project_type: string;
  overall_score?: number | null;
  risk_level?: string | null;
  clone_risk?: string | null;
  identity_status?: string | null;
  evidence_status?: string | null;
  needs_human_review: boolean;
  strengths: string[];
  risks: string[];
  next_checks: string[];
  flags: string[];
  metrics: Record<string, unknown>;
  closest_projects: FinalResultClosestProject[];
  facts: FinalResultFact[];
  risk_evidence: FinalResultRiskEvidence[];
  clone_analysis: FinalResultCloneAnalysis;
}

export interface RunPayload {
  run: RunMeta;
  case: Record<string, unknown>;
  options: Record<string, unknown>;
  overview: RunOverview;
  result: FinalResult;
  presentation: {
    unit_counts?: Record<string, number>;
    stage_counts?: Record<string, number>;
  };
  workflow: WorkflowPlan;
  default_unit_id: string;
}

export interface BlockDetail {
  block_id: string;
  unit_id: string;
  name: string;
  unit_type: string;
  execution_mode: string;
  kind: string;
  description: string;
  manifest: Record<string, unknown>;
  manifest_path: string;
  source_path: string;
  parent_unit_id: string;
  parent_plan_id: string;
  stage: WorkflowStage | Record<string, never>;
  runtime: RuntimeInfo;
  result: ResultInfo | Record<string, never>;
  presentation: PresentationInfo;
  auto_description: string;
  input_ports: Port[];
  output_ports: Port[];
  upstream_edges: Edge[];
  downstream_edges: Edge[];
  raw_mode: boolean;
  trace_input: unknown;
  trace_output: unknown;
  llm_trace: unknown;
  validator_source: string;
  child_plan: WorkflowPlan;
}

export type InspectorDetail = BlockDetail | StageDetail;

export interface CreateRunRequest {
  project: string;
  name?: string;
  project_url?: string;
  telegram_handle?: string;
  wallet_address?: string;
  description?: string;
  type_hint?: string;
  discovery?: DiscoverySearchResponse;
  mode?: string;
  llm_mode?: string;
  llm_model?: string;
  sonar_model?: string;
  enable_sonar?: boolean;
  record_snapshots?: boolean;
  speed_profile?: string;
}

export interface DiscoveryCandidate {
  candidate_key: string;
  name: string;
  github_repo: string;
  project_url: string;
  telegram_handle: string;
  wallet_address: string;
  description: string;
  project_type: string;
  source_labels: string[];
  match_reason: string;
  score: number;
}

export interface DiscoverySourceStatus {
  status: string;
  candidate_count: number;
  summary: string;
}

export interface DiscoverySearchResponse {
  query: string;
  summary: string;
  selected_candidate_key: string;
  candidates: DiscoveryCandidate[];
  source_statuses: Record<string, DiscoverySourceStatus>;
}

export interface DiscoverySessionMeta {
  session_id: string;
  status: string;
  updated_at: number;
  error?: {
    summary?: string;
  };
}

export interface DiscoverySessionPayload {
  session: DiscoverySessionMeta;
  query: string;
  summary: string;
  selected_candidate_key: string;
  candidates: DiscoveryCandidate[];
  source_statuses: Record<string, DiscoverySourceStatus>;
  workflow: WorkflowPlan;
  default_unit_id: string;
}
