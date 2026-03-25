import { ref } from "vue";
import type {
  BlockDetail,
  FinalResult,
  PresentationInfo,
  StageDetail,
  StageSelection,
  WorkflowPlan,
  WorkflowStage,
  WorkflowUnit,
} from "./types";

export type ViewerLocale = "en" | "ru";

const LOCALE_STORAGE_KEY = "identity_viewer_locale";

export const viewerLocale = ref<ViewerLocale>("en");

const UI_TEXT: Record<ViewerLocale, Record<string, string>> = {
  en: {
    app_title: "TON Project Auditor",
    app_subtitle: "Workflow on the left, details on the right, run controls on top.",
    run: "Run",
    execution_view: "Execution view",
    analysis_view: "Analysis view",
    reload: "Reload",
    current_stage: "Current stage",
    result: "Result",
    new: "New",
    new_audit: "New audit",
    latest_projects: "{count} latest projects",
    workflow: "Workflow",
    inspector: "Inspector",
    loading_available_runs: "Loading available runs...",
    case: "Case",
    mode: "Mode",
    status: "Status",
    project_type: "Project type",
    overall: "Overall",
    risk: "Risk",
    clone: "Clone",
    stages: "Stages",
    ai_units: "Model-assisted units",
    hybrid_units: "Hybrid units",
    ai_stages: "Model-assisted stages",
    hybrid_stages: "Hybrid stages",
    open_advanced_browser: "Open detailed view in browser",
    open_advanced_report_browser: "Open full report in browser",
    language: "Language",
    source_link: "Link",
    similarity: "similarity",
    registry_source: "Registry",
    public_ton_web: "Public TON web",
    direct_github: "Direct GitHub",
    find_project: "Find project",
    find_project_subtitle: "Enter the project name plus website, GitHub, Telegram or wallet if you know them.",
    project_input: "Project input",
    project_input_placeholder: "Example: Tonkeeper Web, TON wallet, tonkeeper.com, @tonkeeper_news",
    find_candidates: "Find candidates",
    searching: "Searching...",
    input_changed_refresh: "Input changed. Search again to refresh candidates.",
    no_candidates_found:
      "No candidates were found for the current input. Refine the project name, website, GitHub or Telegram signal and search again.",
    choose_candidate: "Choose candidate",
    choose_candidate_subtitle: "Pick the best detected project before starting the audit.",
    detected_candidates: "Detected candidates",
    selected_project_candidate: "Selected project candidate.",
    score: "score",
    website: "Website",
    github: "GitHub",
    telegram: "Telegram",
    wallet: "Wallet",
    contract: "Contract",
    not_resolved_yet: "Not resolved yet",
    back: "Back",
    start_audit: "Start audit",
    starting_audit: "Starting audit...",
    step_label: "Step {index}",
    step_of: "Step {index} of {total}",
    current: "Current",
    completed: "Completed",
    waiting_for_runtime_update: "Waiting for runtime update",
    waiting_for_project_input: "Waiting for project input",
    waiting_for_previous_stage: "Waiting for previous stage",
    elapsed: "Elapsed {duration}",
    ready_short: "{done}/{total} ready",
    ready_full: "Ready {done}/{total}",
    errors_count: "Errors {count}",
    live_audit: "Live audit",
    preparing_audit: "Preparing audit",
    waiting_next_runtime_update: "Waiting for the next runtime update.",
    starting_validation_run: "Starting a new validation run...",
    checks_ready: "{done}/{total} checks ready",
    final_result: "Final result",
    about_project: "About project",
    audit_completed: "Audit completed.",
    needs_review: "Needs review",
    explanation: "Explanation",
    facts_and_notes: "Links and notes",
    clone_analysis: "Clone analysis",
    evidence: "Evidence",
    top_strengths: "Top strengths",
    main_risks: "Main risks",
    recommended_next_checks: "Recommended next checks",
    audit_stopped_with_error: "Audit stopped with an error",
    audit_error_before_result: "The run finished with an error before the final result was produced.",
    final_result_on_the_way: "Final result is on the way",
    final_result_on_the_way_subtitle: "The full verdict will appear here as soon as the last stage completes.",
    loading_audit: "Loading audit",
    loading_audit_subtitle: "Preparing the current run data.",
    execution_model: "Execution model",
    result_subtitle: "Final important parameters and explanation.",
    important_metrics: "Important metrics",
    strengths: "Strengths",
    risks: "Risks",
    next_checks: "Next checks",
    flags: "Flags",
    closest_projects: "Closest projects",
    no_final_result_running: "Final result will appear here after the last stage completes.",
    no_final_result: "No final result was produced for this run.",
    entity_inspector: "Entity inspector",
    inspector_desc: "Description, configuration, JSON traces and model interaction.",
    raw_json: "Raw JSON",
    loading_entity_detail: "Loading entity detail...",
    select_entity_detail: "Select any stage or unit to inspect its runtime and structure.",
    no_description: "No description.",
    stage: "stage",
    preview: "Preview",
    raw: "Raw",
    what_stage_does: "What this stage does",
    root_workflow: "root workflow",
    units_in_stage: "Units in this stage",
    stage_json: "Stage JSON",
    what_entity_does: "What this entity does",
    nested_execution: "Nested execution",
    latest_result: "Latest result",
    block_structure: "Block structure",
    entity_type_label: "Entity type",
    entity_role_label: "Role",
    internal_topology_label: "Internal topology",
    nested_stages_label: "Internal stages",
    nested_units_label: "Nested units",
    ai_usage_label: "Model usage",
    child_execution_by_stage: "Child execution by stage",
    no_child_units: "No child units in this group.",
    no_result_summary: "No result summary.",
    inputs_outputs: "Inputs and outputs",
    inputs: "Inputs",
    outputs: "Outputs",
    inputs_none: "Inputs: none",
    outputs_none: "Outputs: none",
    dependencies_scope: "Dependencies in this scope",
    upstream: "Upstream",
    downstream: "Downstream",
    upstream_none: "Upstream: none",
    downstream_none: "Downstream: none",
    manifest_json: "Manifest / configuration JSON",
    trace_input_json: "Trace input JSON",
    trace_output_json: "Trace output JSON",
    llm_prompt_response: "Model prompt and response",
    no_workflow_available: "No workflow model is available for this run.",
    computed_execution_wave: "Computed execution wave",
    running_now: "Running now: {names}",
    units_count: "{count} units",
    running_count: "Running {count}",
    pending_count: "Pending {count}",
    error_count: "Error {count}",
    skipped_count: "Skipped {count}",
    composite_execution_scope: "Composite execution scope.",
    atomic_execution_unit: "Atomic execution unit.",
    child_stage: "Nested stage {index}",
    stage_number: "Stage {index}",
    in_out: "{inCount} in / {outCount} out",
    new_analysis: "New analysis",
    free_form_desc:
      "Enter the project name and domain in free form. Add the website, GitHub, Telegram or other signals in the same text for a more accurate match.",
    close: "Close",
    free_form_project_input: "Free-form project input",
    llm_assisted: "Model-assisted",
    live_llm_ranking: "Live model ranking",
    market_token_discovery: "Market + token discovery",
    deep_check_after_confirmation: "Deep check after confirmation",
    find_project_candidates: "Find project candidates",
    discovery_status: "Discovery status",
    candidate_search_completed: "Candidate search completed.",
    no_extra_details: "No extra details.",
    project_candidates: "Project candidates",
    choose_most_relevant_project: "Choose the most relevant project",
    no_extra_candidate_description: "No extra candidate description.",
    unresolved_wallet_note:
      "Wallet or collection address is still unresolved. Deep validation will continue trying to extract public TON addresses from the repo, Telegram and public web sources.",
    cancel: "Cancel",
    continue_deep_check: "Continue deep check",
    starting_deep_check: "Starting deep check...",
    discovery_failed: "Project discovery failed",
    select_confirmed_candidate: "Select a confirmed project candidate before starting the deep check",
    select_candidate_first: "Select a project candidate first",
    insufficient_candidate_data: "The selected candidate does not have enough resolved project data for a deep check",
    search_progress:
      "Searching registry, GitHub, CoinGecko, GeckoTerminal, public TON web and model ranking...",
    start_validation_progress: "Starting deep validation run...",
    stage_find_project: "Find project",
    stage_collect_signals: "Load GitHub, Telegram and registry signals",
    stage_resolve_ton_address: "Find TON address and contract",
    stage_confirm_identity: "Confirm the exact project identity",
    stage_analyze_code_activity: "Check repository structure and activity",
    stage_check_community: "Check Telegram activity and content",
    stage_classify_project: "Determine the project type",
    stage_compare_validate: "Check contracts, similarity and public evidence",
    stage_cross_check_claims: "Cross-check site, GitHub, Telegram and address",
    stage_score_risks: "Build risk flags from collected data",
    stage_build_verdict: "Calculate the final score and verdict",
    stage_write_final_explanation: "Prepare the final explanation",
    stage_prepare_query: "Prepare query",
    stage_search_sources: "Search sources",
    stage_rank_candidates: "Rank candidates",
    unit_normalize_query: "Normalize query",
    unit_registry_search: "Registry search",
    unit_github_search: "GitHub search",
    unit_market_search: "Market search",
    unit_public_web_wallet_search: "Public web wallet search",
    unit_rank_candidates: "Rank candidates",
    locale_en: "En",
    locale_ru: "Ru",
    metric_overall_score: "Overall score",
    metric_activity_score: "Activity score",
    metric_originality_score: "Originality score",
    metric_risk_level: "Risk level",
    metric_clone_risk: "Clone risk",
    metric_community_activity_score: "Community activity score",
    metric_community_quality_score: "Community quality score",
    metric_identity_score: "Identity score",
    metric_last_commit_age_days: "Last commit age (days)",
    metric_onchain_tx_count_30d: "On-chain tx (30d)",
    metric_last_onchain_tx_age_days: "Last on-chain tx age (days)",
    metric_relevance_score: "Relevance score",
    metric_maturity_score: "Maturity score",
    metric_community_score: "Community score",
    metric_contract_score: "Contract score",
    metric_consistency_score: "Consistency score",
    metric_risk_score: "Risk score",
  },
  ru: {
    app_title: "TON Project Auditor",
    app_subtitle: "Слева процесс, справа детали, сверху управление запусками.",
    run: "Запуск",
    execution_view: "Режим выполнения",
    analysis_view: "Режим анализа",
    reload: "Обновить",
    current_stage: "Текущий этап",
    result: "Результат",
    new: "Новый",
    new_audit: "Новый аудит",
    latest_projects: "Последние проекты: {count}",
    workflow: "Процесс",
    inspector: "Инспектор",
    loading_available_runs: "Загрузка доступных запусков...",
    case: "Кейс",
    mode: "Режим",
    status: "Статус",
    project_type: "Тип проекта",
    overall: "Итог",
    risk: "Риск",
    clone: "Клон",
    stages: "Этапы",
    ai_units: "Блоки с моделью",
    hybrid_units: "Гибридные блоки",
    ai_stages: "Этапы с моделью",
    hybrid_stages: "Гибридные этапы",
    open_advanced_browser: "Перейти в подробный режим в браузере",
    open_advanced_report_browser: "Перейти к полному отчёту в браузере",
    language: "Язык",
    source_link: "Ссылка",
    similarity: "сходство",
    registry_source: "Реестр",
    public_ton_web: "Публичный TON web",
    direct_github: "Прямой GitHub",
    find_project: "Найти проект",
    find_project_subtitle: "Введите название проекта и при необходимости добавьте сайт, GitHub, Telegram или кошелёк.",
    project_input: "Ввод проекта",
    project_input_placeholder: "Пример: Tonkeeper Web, TON-кошелёк, tonkeeper.com, @tonkeeper_news",
    find_candidates: "Найти кандидатов",
    searching: "Поиск...",
    input_changed_refresh: "Ввод изменился. Перезапустите поиск, чтобы обновить кандидатов.",
    no_candidates_found:
      "Для текущего ввода кандидаты не найдены. Уточните название проекта, сайт, GitHub или Telegram и попробуйте снова.",
    choose_candidate: "Выбор кандидата",
    choose_candidate_subtitle: "Выберите лучший найденный вариант перед запуском аудита.",
    detected_candidates: "Найденные кандидаты",
    selected_project_candidate: "Выбранный кандидат проекта.",
    score: "оценка",
    website: "Сайт",
    github: "GitHub",
    telegram: "Telegram",
    wallet: "Кошелёк",
    contract: "Контракт",
    not_resolved_yet: "Пока не определён",
    back: "Назад",
    start_audit: "Запустить аудит",
    starting_audit: "Запуск аудита...",
    step_label: "Шаг {index}",
    step_of: "Шаг {index} из {total}",
    current: "Сейчас",
    completed: "Завершено",
    waiting_for_runtime_update: "Ожидание обновления статуса",
    waiting_for_project_input: "Ожидание ввода проекта",
    waiting_for_previous_stage: "Ожидание предыдущего этапа",
    elapsed: "Длительность {duration}",
    ready_short: "{done}/{total} готово",
    ready_full: "Готово {done}/{total}",
    errors_count: "Ошибки {count}",
    live_audit: "Живой аудит",
    preparing_audit: "Подготовка аудита",
    waiting_next_runtime_update: "Ожидание следующего обновления статуса.",
    starting_validation_run: "Запускается новая проверка...",
    checks_ready: "Проверок готово {done}/{total}",
    final_result: "Итоговый результат",
    about_project: "О проекте",
    audit_completed: "Аудит завершён.",
    needs_review: "Нужна проверка",
    explanation: "Пояснение",
    facts_and_notes: "Ссылки и комментарии",
    clone_analysis: "Анализ на клон",
    evidence: "Доказательства",
    top_strengths: "Сильные стороны",
    main_risks: "Основные риски",
    recommended_next_checks: "Рекомендуемые следующие проверки",
    audit_stopped_with_error: "Аудит остановился с ошибкой",
    audit_error_before_result: "Запуск завершился с ошибкой до формирования итогового результата.",
    final_result_on_the_way: "Итог ещё формируется",
    final_result_on_the_way_subtitle: "Полный вердикт появится здесь после завершения последнего этапа.",
    loading_audit: "Загрузка аудита",
    loading_audit_subtitle: "Подготавливаются данные текущего запуска.",
    execution_model: "Модель выполнения",
    result_subtitle: "Главные параметры и объяснение по результату.",
    important_metrics: "Важные метрики",
    strengths: "Сильные стороны",
    risks: "Риски",
    next_checks: "Следующие проверки",
    flags: "Флаги",
    closest_projects: "Ближайшие проекты",
    no_final_result_running: "Итоговый результат появится здесь после завершения последнего этапа.",
    no_final_result: "Для этого запуска итоговый результат не был сформирован.",
    entity_inspector: "Инспектор сущности",
    inspector_desc: "Описание, конфигурация, JSON-трейсы и работа модели.",
    raw_json: "Сырой JSON",
    loading_entity_detail: "Загрузка деталей сущности...",
    select_entity_detail: "Выберите этап или блок, чтобы посмотреть его структуру и runtime.",
    no_description: "Описание отсутствует.",
    stage: "этап",
    preview: "Предпросмотр",
    raw: "Сырой",
    what_stage_does: "Что делает этот этап",
    root_workflow: "корневой процесс",
    units_in_stage: "Блоки на этом этапе",
    stage_json: "JSON этапа",
    what_entity_does: "Что делает эта сущность",
    nested_execution: "Вложенное выполнение",
    latest_result: "Последний результат",
    block_structure: "Структура блока",
    entity_type_label: "Тип сущности",
    entity_role_label: "Роль",
    internal_topology_label: "Внутренняя топология",
    nested_stages_label: "Внутренние этапы",
    nested_units_label: "Вложенные блоки",
    ai_usage_label: "Использование модели",
    child_execution_by_stage: "Вложенное выполнение по этапам",
    no_child_units: "Во вложенной группе нет дочерних блоков.",
    no_result_summary: "Описание результата отсутствует.",
    inputs_outputs: "Входы и выходы",
    inputs: "Входы",
    outputs: "Выходы",
    inputs_none: "Входы: нет",
    outputs_none: "Выходы: нет",
    dependencies_scope: "Зависимости в этой области",
    upstream: "Входящие",
    downstream: "Исходящие",
    upstream_none: "Входящие: нет",
    downstream_none: "Исходящие: нет",
    manifest_json: "JSON манифеста / конфигурации",
    trace_input_json: "JSON входного трейса",
    trace_output_json: "JSON выходного трейса",
    llm_prompt_response: "Промпт и ответ модели",
    no_workflow_available: "Для этого запуска модель workflow недоступна.",
    computed_execution_wave: "Вычисленный этап выполнения",
    running_now: "Сейчас выполняется: {names}",
    units_count: "Блоков {count}",
    running_count: "Выполняется {count}",
    pending_count: "Ожидает {count}",
    error_count: "Ошибка {count}",
    skipped_count: "Пропущено {count}",
    composite_execution_scope: "Составная область выполнения.",
    atomic_execution_unit: "Атомарный блок выполнения.",
    child_stage: "Вложенный этап {index}",
    stage_number: "Этап {index}",
    in_out: "{inCount} вход / {outCount} выход",
    new_analysis: "Новый анализ",
    free_form_desc:
      "Введите название проекта и область в свободной форме. Для более точного совпадения добавьте сайт, GitHub, Telegram или другие сигналы в том же тексте.",
    close: "Закрыть",
    free_form_project_input: "Свободный ввод проекта",
    llm_assisted: "С помощью модели",
    live_llm_ranking: "Живое ранжирование моделью",
    market_token_discovery: "Поиск рынков и токенов",
    deep_check_after_confirmation: "Глубокая проверка после подтверждения",
    find_project_candidates: "Найти кандидатов проекта",
    discovery_status: "Статус discovery",
    candidate_search_completed: "Поиск кандидатов завершён.",
    no_extra_details: "Дополнительных деталей нет.",
    project_candidates: "Кандидаты проекта",
    choose_most_relevant_project: "Выберите наиболее релевантный проект",
    no_extra_candidate_description: "Дополнительного описания кандидата нет.",
    unresolved_wallet_note:
      "Кошелёк или адрес коллекции пока не определён. Глубокая проверка продолжит искать публичные TON-адреса в репозитории, Telegram и публичных web-источниках.",
    cancel: "Отмена",
    continue_deep_check: "Продолжить глубокую проверку",
    starting_deep_check: "Запуск глубокой проверки...",
    discovery_failed: "Не удалось выполнить поиск проекта",
    select_confirmed_candidate: "Выберите подтверждённого кандидата проекта перед запуском глубокой проверки",
    select_candidate_first: "Сначала выберите кандидата проекта",
    insufficient_candidate_data: "У выбранного кандидата недостаточно определённых данных проекта для глубокой проверки",
    search_progress:
      "Идёт поиск по реестру, GitHub, CoinGecko, GeckoTerminal, публичному TON web и модельному ранжированию...",
    start_validation_progress: "Запускается глубокая валидация...",
    stage_find_project: "Найти проект",
    stage_collect_signals: "Собрать сигналы из GitHub, Telegram и реестра",
    stage_resolve_ton_address: "Найти TON-адрес и контракт",
    stage_confirm_identity: "Подтвердить, что найден нужный проект",
    stage_analyze_code_activity: "Проверить структуру кода и активность репозитория",
    stage_check_community: "Проверить активность и качество Telegram",
    stage_classify_project: "Определить тип проекта",
    stage_compare_validate: "Проверить контракты, похожие проекты и внешние данные",
    stage_cross_check_claims: "Сверить сайт, GitHub, Telegram и адрес",
    stage_score_risks: "Собрать риски по найденным данным",
    stage_build_verdict: "Рассчитать итоговую оценку и вердикт",
    stage_write_final_explanation: "Подготовить итоговое объяснение",
    stage_prepare_query: "Подготовить запрос",
    stage_search_sources: "Поиск по источникам",
    stage_rank_candidates: "Ранжировать кандидатов",
    unit_normalize_query: "Нормализовать запрос",
    unit_registry_search: "Поиск в реестре",
    unit_github_search: "Поиск в GitHub",
    unit_market_search: "Поиск по рынкам",
    unit_public_web_wallet_search: "Поиск адреса в публичном web",
    unit_rank_candidates: "Ранжировать кандидатов",
    locale_en: "En",
    locale_ru: "Ru",
    metric_overall_score: "Общий балл",
    metric_activity_score: "Активность",
    metric_originality_score: "Оригинальность",
    metric_risk_level: "Уровень риска",
    metric_clone_risk: "Риск клона",
    metric_community_activity_score: "Активность сообщества",
    metric_community_quality_score: "Качество сообщества",
    metric_identity_score: "Балл identity",
    metric_last_commit_age_days: "Возраст последнего коммита (дни)",
    metric_onchain_tx_count_30d: "On-chain tx (30д)",
    metric_last_onchain_tx_age_days: "Возраст последнего on-chain tx (дни)",
    metric_relevance_score: "Релевантность",
    metric_maturity_score: "Зрелость",
    metric_community_score: "Сообщество",
    metric_contract_score: "Контракты",
    metric_consistency_score: "Согласованность",
    metric_risk_score: "Риск",
  },
};

const TOKEN_LABELS: Record<ViewerLocale, Record<string, string>> = {
  en: {
    pending: "pending",
    running: "running",
    success: "success",
    error: "error",
    skipped: "skipped",
    atomic: "atomic",
    composite: "composite",
    sequential: "sequential",
    parallel: "parallel",
    deterministic: "Deterministic",
    hybrid: "Hybrid",
    ai: "Model-assisted",
    collector: "collector",
    analyzer: "analyzer",
    validator: "validator",
    synthesizer: "synthesizer",
    rules: "Rules",
    tool: "Tool",
    verified: "Verified",
    composite_badge: "Composite",
    llm: "Model",
    "ai unit": "Model-assisted unit",
    "ai group": "Model-assisted group",
    "ai stage": "Model-assisted stage",
    "hybrid stage": "Hybrid stage",
    "hybrid group": "Hybrid group",
    "deterministic unit": "Deterministic unit",
    "deterministic group": "Deterministic group",
    "deterministic stage": "Deterministic stage",
    low: "low",
    moderate: "moderate",
    high: "high",
    unknown: "unknown",
    smart_contracts: "smart contracts",
    tooling_sdk: "tooling SDK",
    wallet_app: "wallet app",
    dapp_product: "dApp product",
    protocol_infra: "protocol infra",
    dex: "DEX",
    derivatives_dex: "derivatives DEX",
    staking_protocol: "staking protocol",
    protocol_service: "protocol service",
    explorer: "explorer",
    tooling_api: "tooling API",
    nft_collection: "NFT collection",
    nft_marketplace: "NFT marketplace",
    gamefi: "GameFi",
    token: "token",
    meme: "meme token",
    user_input: "input hint",
    registry: "Registry",
    github_search: "GitHub search",
    direct_github: "Direct GitHub",
    coingecko: "CoinGecko",
    geckoterminal: "GeckoTerminal",
    public_web: "Public TON web",
    strong_ton_relevance: "Strong TON relevance",
    solid_repository_depth: "Solid repository depth",
    contracts_are_visible: "Contracts are visible",
    recent_public_channel_activity: "Recent public channel activity",
    community_feed_looks_curated: "Community feed looks curated",
    recent_git_activity: "Recent Git activity",
    recent_onchain_activity: "Recent on-chain activity",
    distinct_from_registry: "Distinct from registry projects",
    identity_confirmed: "Identity confirmed",
    external_public_signal_checked: "External public signals checked",
    ton_mcp_known_jetton_verified: "TON MCP verified known jetton",
    onchain_activity_is_stale: "On-chain activity is stale",
    review_onchain_activity: "Review on-chain activity",
    identity_brand_mismatch: "Selected candidate mismatches the requested brand",
    identity_unconfirmed: "Identity is not fully confirmed",
    identity_based_on_noncanonical_reference: "Identity is based on a non-canonical reference",
    github_source_unavailable: "GitHub source is unavailable",
    telegram_source_unavailable: "Telegram source is unavailable",
    identity_evidence_incomplete: "Identity evidence is incomplete",
    telegram_recent_activity_is_cooling: "Telegram activity is cooling",
    telegram_public_activity_is_low: "Telegram public activity is low",
    telegram_semantic_risk_moderate: "Telegram semantic risk is moderate",
    telegram_semantic_risk_high: "Telegram semantic risk is high",
    telegram_feed_is_overly_promotional: "Telegram feed is overly promotional",
    telegram_feed_is_repetitive: "Telegram feed is repetitive",
    telegram_uses_urgency_marketing: "Telegram uses urgency marketing",
    telegram_activity_is_bursty: "Telegram activity is bursty",
    repo_is_stale: "Repository is stale",
    repo_activity_is_old: "Repository activity is old",
    no_recent_commits: "No recent commits",
    single_author_recent_history: "Single-author recent history",
    readme_is_too_short: "README is too short",
    ton_relevance_is_weak: "TON relevance is weak",
    repo_archived: "Repository is archived",
    project_type_unknown: "Project type is unknown",
    cross_source_mismatches_detected: "Cross-source mismatches detected",
    missing_contract_files: "Missing contract files",
    missing_address_signal: "Missing address signal",
    clone_risk_high: "High clone risk",
    clone_risk_moderate: "Moderate clone risk",
    self_declared_repository_copy: "Repository README declares copied production code",
    optional_manual_review: "Optional manual review",
    manual_review: "Manual review",
    confirm_project_identity: "Confirm project identity",
    reselect_project_candidate: "Reselect project candidate",
    retry_source_collection: "Retry source collection",
    refine_project_type: "Refine project type",
    verify_contract_addresses: "Verify contract addresses",
    review_project_originality: "Review project originality",
    review_project_activity: "Review project activity",
    review_community_activity: "Review community activity",
    review_community_feed_quality: "Review community feed quality",
    promotional_feed: "Promotional feed",
    repetitive_patterns: "Repetitive patterns",
    scam_risk: "Scam risk",
    gamefi_updates: "GameFi updates",
    rewards_campaigns: "Rewards and campaigns",
    product_updates: "Product updates",
  },
  ru: {
    pending: "ожидает",
    running: "выполняется",
    success: "успех",
    error: "ошибка",
    skipped: "пропущено",
    atomic: "атомарный",
    composite: "составной",
    sequential: "последовательный",
    parallel: "параллельный",
    deterministic: "Детерминированный",
    hybrid: "Гибридный",
    ai: "С моделью",
    collector: "сборщик",
    analyzer: "анализатор",
    validator: "валидатор",
    synthesizer: "синтезатор",
    rules: "Правила",
    tool: "Инструмент",
    verified: "Проверено",
    composite_badge: "Составной",
    llm: "Модель",
    "ai unit": "Блок с моделью",
    "ai group": "Группа с моделью",
    "ai stage": "Этап с моделью",
    "hybrid stage": "Гибридный этап",
    "hybrid group": "Гибридная группа",
    "deterministic unit": "Детерминированный блок",
    "deterministic group": "Детерминированная группа",
    "deterministic stage": "Детерминированный этап",
    low: "низкий",
    moderate: "умеренный",
    high: "высокий",
    unknown: "неизвестно",
    smart_contracts: "смарт-контракты",
    tooling_sdk: "SDK / инструменты",
    wallet_app: "кошелёк",
    dapp_product: "dApp-продукт",
    protocol_infra: "протокольная инфраструктура",
    dex: "DEX",
    derivatives_dex: "деривативный DEX",
    staking_protocol: "стейкинг-протокол",
    protocol_service: "протокольный сервис",
    explorer: "обозреватель",
    tooling_api: "API / инструменты",
    nft_collection: "NFT-коллекция",
    nft_marketplace: "NFT-маркетплейс",
    gamefi: "GameFi",
    token: "токен",
    meme: "мем-токен",
    user_input: "подсказка ввода",
    registry: "реестр",
    github_search: "поиск GitHub",
    direct_github: "прямой GitHub",
    coingecko: "CoinGecko",
    geckoterminal: "GeckoTerminal",
    public_web: "публичный TON web",
    strong_ton_relevance: "Высокая релевантность TON",
    solid_repository_depth: "Хорошая глубина репозитория",
    contracts_are_visible: "Контракты явно присутствуют",
    recent_public_channel_activity: "Есть недавняя публичная активность канала",
    community_feed_looks_curated: "Лента сообщества выглядит курируемой",
    recent_git_activity: "Есть недавняя Git-активность",
    recent_onchain_activity: "Есть недавняя on-chain активность",
    distinct_from_registry: "Проект отличается от проектов в реестре",
    identity_confirmed: "Identity подтверждена",
    external_public_signal_checked: "Проверены внешние публичные сигналы",
    ton_mcp_known_jetton_verified: "TON MCP подтвердил известный jetton",
    onchain_activity_is_stale: "On-chain активность устарела",
    review_onchain_activity: "Проверить on-chain активность",
    identity_brand_mismatch: "Выбранный кандидат не совпадает с брендом запроса",
    identity_unconfirmed: "Identity не подтверждена полностью",
    identity_based_on_noncanonical_reference: "Identity основана на неканонической ссылке",
    github_source_unavailable: "Источник GitHub недоступен",
    telegram_source_unavailable: "Источник Telegram недоступен",
    identity_evidence_incomplete: "Доказательства identity неполные",
    telegram_recent_activity_is_cooling: "Недавняя активность Telegram снижается",
    telegram_public_activity_is_low: "Публичная активность Telegram низкая",
    telegram_semantic_risk_moderate: "Семантический риск Telegram умеренный",
    telegram_semantic_risk_high: "Семантический риск Telegram высокий",
    telegram_feed_is_overly_promotional: "Лента Telegram слишком промо-ориентирована",
    telegram_feed_is_repetitive: "Лента Telegram повторяется",
    telegram_uses_urgency_marketing: "В Telegram используется давление срочностью",
    telegram_activity_is_bursty: "Активность Telegram выглядит всплесками",
    repo_is_stale: "Репозиторий устарел",
    repo_activity_is_old: "Активность репозитория старая",
    no_recent_commits: "Нет недавних коммитов",
    single_author_recent_history: "Недавняя история фактически от одного автора",
    readme_is_too_short: "README слишком короткий",
    ton_relevance_is_weak: "Слабая релевантность TON",
    repo_archived: "Репозиторий заархивирован",
    project_type_unknown: "Тип проекта не определён",
    cross_source_mismatches_detected: "Найдены расхождения между источниками",
    missing_contract_files: "Файлы контрактов не найдены",
    missing_address_signal: "Нет сигнала адреса",
    clone_risk_high: "Высокий риск клона",
    clone_risk_moderate: "Умеренный риск клона",
    self_declared_repository_copy: "README сам указывает на копию production-кода",
    optional_manual_review: "Опциональная ручная проверка",
    manual_review: "Нужна ручная проверка",
    confirm_project_identity: "Подтвердить identity проекта",
    reselect_project_candidate: "Выбрать кандидата заново",
    retry_source_collection: "Повторить сбор источников",
    refine_project_type: "Уточнить тип проекта",
    verify_contract_addresses: "Проверить адреса контрактов",
    review_project_originality: "Проверить оригинальность проекта",
    review_project_activity: "Проверить активность проекта",
    review_community_activity: "Проверить активность сообщества",
    review_community_feed_quality: "Проверить качество ленты сообщества",
    promotional_feed: "Промо-лента",
    repetitive_patterns: "Повторяющиеся паттерны",
    scam_risk: "Риск скама",
    gamefi_updates: "GameFi-обновления",
    rewards_campaigns: "Кампании и награды",
    product_updates: "Обновления продукта",
  },
};

const ENTITY_LABELS: Record<string, string> = {
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
  discovery_stage_0: "stage_prepare_query",
  discovery_stage_1: "stage_search_sources",
  discovery_stage_2: "stage_rank_candidates",
  discovery_parse_query: "unit_normalize_query",
  discovery_registry_search: "unit_registry_search",
  discovery_github_search: "unit_github_search",
  discovery_market_search: "unit_market_search",
  discovery_public_web_search: "unit_public_web_wallet_search",
  discovery_rank_candidates: "unit_rank_candidates",
};

const ENTITY_DESCRIPTIONS: Record<ViewerLocale, Record<string, string>> = {
  en: {
    project_discovery: "Resolves the most likely project candidate from the user's free-form request before deep validation.",
    source_collection: "Loads the public and recorded signals that seed the whole validation flow.",
    address_signal:
      "Extracts TON addresses from repository docs and Telegram, then enriches them with TON DNS, balance and activity signals.",
    identity_confirmation:
      "Confirms that the selected candidate matches the requested project and separates weak identity from incomplete evidence.",
    repo_analysis: "Expands repository structure and freshness signals after raw repository metadata is available.",
    community_analysis: "Evaluates recent public community content and interaction quality.",
    project_type: "Classifies the project type from repository signals.",
    deep_validation: "Runs heavier validators that need richer artifacts and classified project context.",
    claim_consistency: "Cross-checks repository, Telegram and contract signals.",
    risk_validator: "Builds risk flags from deterministic signals.",
    rule_engine: "Combines validator outputs into transparent scores.",
    llm_explainer: "Produces a human-readable explanation from structured validator outputs.",
    discovery_parse_query: "Normalizes the free-form user request into a structured discovery query.",
    discovery_registry_search: "Checks the local TON project registry for matching entities.",
    discovery_github_search: "Searches GitHub repositories and exact repository hints.",
    discovery_market_search: "Searches market and token sources so non-Git TON entities can still be resolved.",
    discovery_public_web_search:
      "Searches public TON collection and explorer pages to resolve wallet or contract identifiers.",
    discovery_rank_candidates: "Ranks discovered candidates and selects the best project match before deep validation.",
    github_repo: "Loads GitHub repository metadata and README.",
    telegram_channel: "Loads and normalizes public Telegram channel posts.",
    project_registry: "Builds a curated registry from recorded local project cases.",
    github_tree: "Loads the recursive repository tree from GitHub.",
    github_activity: "Loads recent commit and release activity for freshness and momentum metrics.",
    telegram_semantics:
      "Analyzes recent Telegram posts for activity quality, promotional density, repetition and scam-like signals.",
    contract_validator: "Checks whether contract-heavy projects expose contract artifacts and related address signals.",
    project_similarity: "Finds similar projects in the curated registry and estimates originality risk.",
    sonar_research: "Runs optional public web research with a Sonar-compatible model.",
  },
  ru: {
    project_discovery:
      "Определяет наиболее вероятного кандидата проекта из свободного пользовательского запроса до глубокой проверки.",
    source_collection: "Собирает публичные и сохранённые сигналы, которые запускают весь поток валидации.",
    address_signal:
      "Извлекает TON-адреса из репозитория и Telegram, затем дополняет их сигналами TON DNS, баланса и on-chain активности.",
    identity_confirmation:
      "Подтверждает, что выбранный кандидат соответствует проекту из запроса, и отделяет слабую identity от неполных внешних данных.",
    repo_analysis: "Раскрывает структуру репозитория и сигналы свежести после получения сырых метаданных GitHub.",
    community_analysis: "Оценивает качество недавнего публичного контента сообщества и характер взаимодействия.",
    project_type: "Определяет тип проекта по сигналам репозитория.",
    deep_validation: "Запускает более тяжёлые валидаторы, которым нужны богатые артефакты и уже определённый контекст проекта.",
    claim_consistency: "Сверяет между собой сигналы репозитория, Telegram и контрактов.",
    risk_validator: "Строит риск-флаги из детерминированных сигналов.",
    rule_engine: "Объединяет результаты валидаторов в прозрачные оценки.",
    llm_explainer: "Формирует человекочитаемое объяснение на основе структурированных результатов валидаторов.",
    discovery_parse_query: "Нормализует свободный пользовательский запрос в структурированный запрос на поиск кандидатов.",
    discovery_registry_search: "Проверяет локальный TON-реестр на совпадающие сущности.",
    discovery_github_search: "Ищет подходящие репозитории GitHub и точные repo-подсказки.",
    discovery_market_search:
      "Ищет по рыночным и токеновым источникам, чтобы можно было определить и не-Git TON-проекты.",
    discovery_public_web_search:
      "Ищет в публичных TON-страницах и обозревателях, чтобы разрешить кошельки и адреса контрактов.",
    discovery_rank_candidates: "Ранжирует найденных кандидатов и выбирает лучший проект перед глубокой проверкой.",
    github_repo: "Загружает метаданные GitHub-репозитория и README.",
    telegram_channel: "Загружает и нормализует публичные посты Telegram-канала.",
    project_registry: "Строит курируемый реестр из локально сохранённых кейсов проектов.",
    github_tree: "Загружает рекурсивное дерево репозитория из GitHub.",
    github_activity: "Загружает недавнюю активность коммитов и релизов для оценки свежести и темпа проекта.",
    telegram_semantics:
      "Анализирует недавние посты Telegram по качеству активности, плотности промо, повторяемости и скам-подобным сигналам.",
    contract_validator: "Проверяет, публикует ли контрактно-нагруженный проект артефакты контрактов и связанные адресные сигналы.",
    project_similarity: "Ищет похожие проекты в курируемом реестре и оценивает риск неоригинальности.",
    sonar_research: "Запускает дополнительное исследование публичного web через совместимую с Sonar модель.",
  },
};

function normalizeWhitespace(value: unknown): string {
  return String(value || "").trim().replace(/\s+/g, " ");
}

function capitalizeWords(value: string): string {
  return String(value || "").replace(/\b\w/g, (char) => char.toUpperCase());
}

function localeText(en: string, ru: string): string {
  return viewerLocale.value === "ru" ? ru : en;
}

function ruPlural(count: number, one: string, few: string, many: string): string {
  const value = Math.abs(Number(count) || 0) % 100;
  const tail = value % 10;
  if (value > 10 && value < 20) {
    return many;
  }
  if (tail > 1 && tail < 5) {
    return few;
  }
  if (tail === 1) {
    return one;
  }
  return many;
}

function normalizeLocale(value: unknown): ViewerLocale {
  return String(value || "").toLowerCase() === "ru" ? "ru" : "en";
}

function storedLocale(): ViewerLocale {
  if (typeof window === "undefined") {
    return "en";
  }
  try {
    return normalizeLocale(window.localStorage.getItem(LOCALE_STORAGE_KEY) || "");
  } catch {
    return "en";
  }
}

function persistLocale(locale: ViewerLocale): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(LOCALE_STORAGE_KEY, locale);
  } catch {
    return;
  }
}

export function initializeViewerLocale(preferredLocale = ""): ViewerLocale {
  const explicit = normalizeWhitespace(preferredLocale).toLowerCase();
  const nextLocale = explicit ? normalizeLocale(explicit) : storedLocale();
  viewerLocale.value = nextLocale;
  persistLocale(nextLocale);
  return nextLocale;
}

export function setViewerLocale(locale: string): ViewerLocale {
  const nextLocale = normalizeLocale(locale);
  viewerLocale.value = nextLocale;
  persistLocale(nextLocale);
  return nextLocale;
}

export function localeQueryValue(): string {
  return viewerLocale.value === "ru" ? "ru" : "";
}

export function t(key: string, params: Record<string, unknown> = {}): string {
  const table = UI_TEXT[viewerLocale.value] || UI_TEXT.en;
  const template = table[key] || UI_TEXT.en[key] || key;
  return String(template).replace(/\{(\w+)\}/g, (_, token) => String(params[token] ?? ""));
}

export function tokenLabel(value: unknown): string {
  const raw = normalizeWhitespace(value);
  if (!raw) {
    return "-";
  }
  const key = raw.toLowerCase();
  const translated = TOKEN_LABELS[viewerLocale.value][key] || TOKEN_LABELS.en[key];
  if (translated) {
    return translated;
  }
  const humanized = raw.replace(/_/g, " ").replace(/-/g, " ");
  return viewerLocale.value === "ru" ? humanized : capitalizeWords(humanized);
}

export function statusLabel(status: unknown): string {
  return tokenLabel(status);
}

export function entityLabel(entityId: string, fallback = ""): string {
  const translationKey = ENTITY_LABELS[String(entityId || "")];
  if (translationKey) {
    return t(translationKey);
  }
  if (fallback) {
    return tokenLabel(fallback);
  }
  return tokenLabel(entityId);
}

export function asText(value: unknown, fallback = "-"): string {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

export function formatDuration(value?: number): string {
  const duration = Number(value || 0);
  if (!Number.isFinite(duration) || duration <= 0) {
    return viewerLocale.value === "ru" ? "0 мс" : "0 ms";
  }
  if (duration < 1000) {
    return viewerLocale.value === "ru" ? `${duration} мс` : `${duration} ms`;
  }
  if (duration < 60000) {
    return viewerLocale.value === "ru" ? `${(duration / 1000).toFixed(1)} с` : `${(duration / 1000).toFixed(1)} s`;
  }
  return viewerLocale.value === "ru" ? `${(duration / 60000).toFixed(1)} мин` : `${(duration / 60000).toFixed(1)} min`;
}

export function primaryTypeLabel(presentation?: PresentationInfo): string {
  const kind = String(presentation?.kind || "deterministic");
  if (kind === "ai") {
    return tokenLabel("ai");
  }
  if (kind === "hybrid") {
    return tokenLabel("hybrid");
  }
  return tokenLabel("deterministic");
}

export function secondaryPresentationBadges(presentation?: PresentationInfo): string[] {
  const badges = [...(presentation?.badges || [])].map((badge) => String(badge || ""));
  const excluded = new Set([
    String(presentation?.label || ""),
    primaryTypeLabel(presentation),
    "Model-assisted unit",
    "Model-assisted group",
    "Model-assisted stage",
    "Hybrid stage",
    "Hybrid group",
    "Deterministic unit",
    "Deterministic group",
    "Deterministic stage",
    "Hybrid",
    "Model-assisted",
  ]);
  return badges.filter((badge) => !excluded.has(badge)).map((badge) => tokenLabel(badge));
}

type UnitLike = WorkflowUnit | BlockDetail;

function childPlanOf(unit: UnitLike | null | undefined): WorkflowPlan | null {
  const value = (unit as Partial<BlockDetail & WorkflowUnit> | null | undefined)?.child_plan
    || (unit as Partial<WorkflowUnit> | null | undefined)?.plan;
  if (!value || typeof value !== "object") {
    return null;
  }
  return value as WorkflowPlan;
}

function childUnitsOf(unit: UnitLike | null | undefined): WorkflowUnit[] {
  return childPlanOf(unit)?.units || [];
}

function childStagesOf(unit: UnitLike | null | undefined): WorkflowStage[] {
  return childPlanOf(unit)?.stages || [];
}

export function localizedUnitDescription(unit: UnitLike | null | undefined): string {
  const unitId = String(unit?.unit_id || "");
  const localized = ENTITY_DESCRIPTIONS[viewerLocale.value][unitId] || ENTITY_DESCRIPTIONS.en[unitId];
  if (localized) {
    return localized;
  }
  const fallback = normalizeWhitespace(unit?.description);
  return fallback || t("no_description");
}

export function localizedUnitSummary(unit: UnitLike | null | undefined): string {
  const description = localizedUnitDescription(unit);
  if (description && description !== t("no_description")) {
    return description;
  }
  const resultSummary = normalizeWhitespace((unit as Partial<BlockDetail & WorkflowUnit> | null | undefined)?.result?.summary);
  return resultSummary || t("no_result_summary");
}

export function unitTypeLabel(unit: UnitLike | null | undefined): string {
  return String(unit?.unit_type || "") === "composite"
    ? localeText("Composite block", "Составной блок")
    : localeText("Atomic block", "Атомарный блок");
}

export function unitRoleLabel(unit: UnitLike | null | undefined): string {
  if (!unit) {
    return "-";
  }
  if (String(unit.unit_type || "") === "composite") {
    return localeText("Group", "Группа");
  }
  return tokenLabel(unit.kind || "");
}

export function nestedStageCount(unit: UnitLike | null | undefined): number {
  return childStagesOf(unit).length;
}

export function nestedUnitCount(unit: UnitLike | null | undefined): number {
  return childUnitsOf(unit).length;
}

export function unitTopologyMode(unit: UnitLike | null | undefined): "none" | "single" | "parallel" | "sequential" | "mixed" {
  if (String(unit?.unit_type || "") !== "composite") {
    return "none";
  }
  const stages = childStagesOf(unit);
  const units = childUnitsOf(unit);
  if (!stages.length || !units.length) {
    return "none";
  }
  if (units.length === 1) {
    return "single";
  }
  const stageSizes = stages.map((stage) => (stage.unit_ids || []).length);
  if (stages.length === 1) {
    return stageSizes[0] > 1 ? "parallel" : "single";
  }
  if (stageSizes.every((count) => count <= 1)) {
    return "sequential";
  }
  return "mixed";
}

export function unitTopologyLabel(unit: UnitLike | null | undefined, short = false): string {
  const mode = unitTopologyMode(unit);
  switch (mode) {
    case "parallel":
      return short ? localeText("Inside: parallel", "Внутри: параллельно") : localeText("Parallel internal graph", "Параллельный внутренний граф");
    case "sequential":
      return short
        ? localeText("Inside: sequential", "Внутри: последовательно")
        : localeText("Sequential internal graph", "Последовательный внутренний граф");
    case "mixed":
      return short ? localeText("Inside: mixed", "Внутри: смешанно") : localeText("Mixed internal graph", "Смешанный внутренний граф");
    case "single":
      return short ? localeText("Inside: one block", "Внутри: один блок") : localeText("Single nested block", "Один вложенный блок");
    default:
      return localeText("No internal graph", "Внутреннего графа нет");
  }
}

export function unitTopologyNote(unit: UnitLike | null | undefined): string {
  const mode = unitTopologyMode(unit);
  switch (mode) {
    case "parallel":
      return localeText(
        "This block contains one internal stage with independent nested units that run in parallel and then return their outputs upward.",
        "Этот блок содержит один внутренний этап с независимыми вложенными блоками, которые выполняются параллельно и затем поднимают результаты выше.",
      );
    case "sequential":
      return localeText(
        "This block contains several internal stages. Each next internal stage starts only after the previous one has completed.",
        "Этот блок содержит несколько внутренних этапов. Каждый следующий внутренний этап стартует только после завершения предыдущего.",
      );
    case "mixed":
      return localeText(
        "This block contains several internal stages, and at least one of them runs multiple nested units in parallel.",
        "Этот блок содержит несколько внутренних этапов, и как минимум на одном из них несколько вложенных блоков выполняются параллельно.",
      );
    case "single":
      return localeText(
        "This composite block wraps a single nested unit and does not fan out internal work.",
        "Этот составной блок оборачивает один вложенный блок и не разветвляет внутреннее выполнение.",
      );
    default:
      return localeText(
        "This block is atomic and does not contain a nested execution graph.",
        "Этот блок атомарный и не содержит вложенного графа выполнения.",
      );
  }
}

export function nestedStageCountLabel(unit: UnitLike | null | undefined): string {
  const count = nestedStageCount(unit);
  return localeText(
    `${count} internal stage${count === 1 ? "" : "s"}`,
    `${count} ${ruPlural(count, "внутренний этап", "внутренних этапа", "внутренних этапов")}`,
  );
}

export function nestedUnitCountLabel(unit: UnitLike | null | undefined): string {
  const count = nestedUnitCount(unit);
  return localeText(
    `${count} nested unit${count === 1 ? "" : "s"}`,
    `${count} ${ruPlural(count, "вложенный блок", "вложенных блока", "вложенных блоков")}`,
  );
}

export function unitAiUsageSummary(unit: UnitLike | null | undefined): string {
  if (String(unit?.unit_type || "") !== "composite") {
    return localeText("Not applicable for atomic blocks.", "Не применяется к атомарным блокам.");
  }
  const children = childUnitsOf(unit);
  if (!children.length) {
    return localeText("No nested units.", "Вложенных блоков нет.");
  }
  const aiChildren = children.filter((child) => String(child.presentation?.kind || "") === "ai");
  const hybridChildren = children.filter((child) => String(child.presentation?.kind || "") === "hybrid");
  const parts: string[] = [];
  if (aiChildren.length) {
    const names = aiChildren.map((child) => entityLabel(child.unit_id, child.name || child.unit_id)).join(", ");
    parts.push(localeText(`Model-assisted blocks: ${names}.`, `Блоки с моделью: ${names}.`));
  }
  if (hybridChildren.length) {
    const names = hybridChildren.map((child) => entityLabel(child.unit_id, child.name || child.unit_id)).join(", ");
    parts.push(localeText(`Hybrid blocks: ${names}.`, `Гибридные блоки: ${names}.`));
  }
  if (!parts.length) {
    return localeText("No model-assisted or hybrid blocks inside.", "Внутри нет блоков с моделью или гибридных блоков.");
  }
  return parts.join(" ");
}

export function localizedStageDescription(
  stage: WorkflowStage | null | undefined,
  units: Array<Pick<WorkflowUnit, "unit_id" | "name">> = [],
  parentUnit: UnitLike | null | undefined = null,
): string {
  const names = units.map((unit) => entityLabel(unit.unit_id, unit.name || unit.unit_id)).filter(Boolean);
  if (!names.length) {
    return t("computed_execution_wave");
  }
  const subject = names.join(", ");
  const runsInParallel = names.length > 1;
  if (parentUnit) {
    return runsInParallel
      ? localeText(
          `Inside this block, the current internal stage runs in parallel: ${subject}.`,
          `Внутри этого блока на текущем внутреннем этапе параллельно выполняются: ${subject}.`,
        )
      : localeText(
          `Inside this block, the current internal stage runs: ${subject}.`,
          `Внутри этого блока на текущем внутреннем этапе выполняется: ${subject}.`,
        );
  }
  return runsInParallel
    ? localeText(
        `This execution wave runs in parallel: ${subject}.`,
        `На этой волне выполнения параллельно запускаются: ${subject}.`,
      )
    : localeText(
        `This execution wave runs: ${subject}.`,
        `На этой волне выполнения запускается: ${subject}.`,
      );
}

export function stageVisualState(stage: WorkflowStage): "future" | "active" | "done" {
  const status = String(stage.runtime?.status || "pending");
  if (status === "running") {
    return "active";
  }
  if (status === "pending") {
    return "future";
  }
  return "done";
}

export function activeRootStage(workflow: WorkflowPlan | null | undefined): WorkflowStage | null {
  const stages = workflow?.stages || [];
  return stages.find((stage) => String(stage.runtime?.status || "pending") === "running") || null;
}

export function statusCountByUnit(units: WorkflowUnit[]): Record<string, number> {
  const counts: Record<string, number> = {
    success: 0,
    running: 0,
    pending: 0,
    error: 0,
    skipped: 0,
  };
  for (const unit of units) {
    const status = String(unit.runtime?.status || unit.result?.status || "pending");
    counts[status] = (counts[status] || 0) + 1;
  }
  return counts;
}

export function unitStatus(unit: WorkflowUnit | null | undefined): string {
  return String(unit?.runtime?.status || unit?.result?.status || "pending");
}

export function flattenUnits(units: WorkflowUnit[]): WorkflowUnit[] {
  const out: WorkflowUnit[] = [];
  for (const unit of units || []) {
    out.push(unit);
    if (unit.unit_type === "composite" && unit.plan?.units?.length) {
      out.push(...flattenUnits(unit.plan.units));
    }
  }
  return out;
}

export function leafExecutionUnits(units: WorkflowUnit[]): WorkflowUnit[] {
  const out: WorkflowUnit[] = [];
  for (const unit of units || []) {
    if (unit.unit_type === "composite" && unit.plan?.units?.length) {
      out.push(...leafExecutionUnits(unit.plan.units));
      continue;
    }
    out.push(unit);
  }
  return out;
}

export function pickInitialUnit(
  workflow: WorkflowPlan | null | undefined,
  currentUnitId: string,
  suggestedUnitId: string,
): string {
  const knownIds = new Set(collectUnitIds(workflow));
  if (currentUnitId && knownIds.has(currentUnitId)) {
    return currentUnitId;
  }
  if (suggestedUnitId && knownIds.has(suggestedUnitId)) {
    return suggestedUnitId;
  }
  const lastStage = workflow?.stages?.length ? workflow.stages[workflow.stages.length - 1] : undefined;
  return lastStage?.unit_ids?.[0] || workflow?.units?.[0]?.unit_id || "";
}

export function collectUnitIds(plan: WorkflowPlan | null | undefined): string[] {
  const out: string[] = [];
  for (const unit of plan?.units || []) {
    out.push(unit.unit_id);
    if (unit.unit_type === "composite" && unit.plan) {
      out.push(...collectUnitIds(unit.plan));
    }
  }
  return out;
}

export function makeStageSelectionKey(stageId: string, parentUnitId = ""): string {
  return parentUnitId ? `child-stage:${parentUnitId}:${stageId}` : `stage:${stageId}`;
}

export function findUnit(plan: WorkflowPlan | null | undefined, unitId: string): WorkflowUnit | null {
  for (const unit of plan?.units || []) {
    if (unit.unit_id === unitId) {
      return unit;
    }
    if (unit.unit_type === "composite" && unit.plan) {
      const nested = findUnit(unit.plan, unitId);
      if (nested) {
        return nested;
      }
    }
  }
  return null;
}

export function buildStageDetail(
  workflow: WorkflowPlan | null | undefined,
  selection: StageSelection,
): StageDetail | null {
  if (!workflow) {
    return null;
  }
  const parentUnitId = String(selection.parent_unit_id || "");
  const parentUnit = parentUnitId ? findUnit(workflow, parentUnitId) : null;
  const plan = parentUnit?.plan || workflow;
  const stage = (plan?.stages || []).find((item) => item.stage_id === selection.stage_id);
  if (!stage) {
    return null;
  }
  const unitMap = new Map((plan?.units || []).map((unit) => [unit.unit_id, unit]));
  const units = stage.unit_ids.map((unitId) => unitMap.get(unitId)).filter(Boolean) as WorkflowUnit[];
  const isChildStage = Boolean(parentUnitId);
  const stageLabel = isChildStage ? t("child_stage", { index: stage.index }) : t("stage_number", { index: stage.index });
  const description = localizedStageDescription(stage, units, parentUnit);
  return {
    detail_type: "stage",
    selection_key: makeStageSelectionKey(stage.stage_id, parentUnitId),
    stage_id: stage.stage_id,
    name: parentUnit ? `${entityLabel(parentUnit.unit_id, parentUnit.name)} | ${stageLabel}` : stageLabel,
    description,
    stage_label: stageLabel,
    parent_unit_id: parentUnitId,
    parent_unit_name: parentUnit ? entityLabel(parentUnit.unit_id, parentUnit.name) : "",
    runtime: stage.runtime || { status: "pending" },
    presentation: stage.presentation || {},
    unit_ids: [...stage.unit_ids],
    units: units.map((unit) => ({
      unit_id: unit.unit_id,
      name: entityLabel(unit.unit_id, unit.name || unit.unit_id),
      status: String(unit.runtime?.status || unit.result?.status || "pending"),
      summary: localizedUnitSummary(unit),
    })),
    raw_payload: {
      stage,
      parent_unit_id: parentUnitId,
      parent_unit_name: parentUnit ? entityLabel(parentUnit.unit_id, parentUnit.name) : "",
      units,
    },
  };
}

export function prettyJson(value: unknown): string {
  return JSON.stringify(value ?? {}, null, 2);
}

export function formatMetricLabel(key: string): string {
  const metricKey = `metric_${String(key || "").toLowerCase()}`;
  const localized = UI_TEXT[viewerLocale.value][metricKey] || UI_TEXT.en[metricKey];
  if (localized) {
    return localized;
  }
  return capitalizeWords(String(key || "").replace(/_/g, " "));
}

export function hasFinalResult(result: FinalResult | null | undefined): boolean {
  return Boolean(result && String(result.status || "pending") === "success");
}
