# TON Project Auditor

TON Project Auditor validates TON projects from free-form input and produces an evidence-backed result from GitHub, Telegram, market and TON-native signals.

## Public Entry Points

- Telegram Mini App: https://t.me/ProjectValidatorBot/audit
- Telegram bot: https://t.me/ProjectValidatorBot
- Hosted reviewer web: https://ai-api.su/ton-viewer/

The Telegram Mini App above is the main public product entrypoint. The local `127.0.0.1` viewer is optional and exists only for local verification, offline review and development.

## Repository Model

This folder is the single source repository for the project.

It includes:

- `identity_validator/` for the Python backend, validators, workflow engine and local HTTP viewer backend;
- `viewer_frontend/` for the Vue frontend source;
- `cases/` for recorded cases and snapshots used by deterministic review mode;
- `tests/` for automated verification.

No second repository is required for this codebase. The Telegram bot and the deployed Mini App are delivery surfaces of the same product, not separate source trees stored elsewhere in this repository.

## Project Name

Use `TON Project Auditor` as the umbrella project name.

Use `ProjectValidatorBot` as the Telegram delivery surface name.

## What To Commit

The normal source repository includes:

- `identity_validator/`
- `viewer_frontend/`
- `cases/`
- `tests/`
- root project files such as `README.md`, `.env.example`, `.gitignore`, `LICENSE` and CI config

## What Must Stay Out Of Git

These paths are generated or local-only and should not be committed:

- `identity_validator/viewer_static/`
- `artifacts/`
- `logs/`
- `viewer_frontend/node_modules/`
- `viewer_frontend/dist/`
- `.env`
- any local secret file

## Repository Layout

```text
identity_validator/     Python backend, validators, discovery, viewer backend
viewer_frontend/        Vue frontend source and Vite build config
cases/                  Recorded TON project cases and snapshots
tests/                  Unit and integration tests
```

The frontend build writes into `identity_validator/viewer_static/`. That is a build artifact, not a separate repository.

## Validation Flow

Root flow:

`project discovery -> signal collection -> TON address resolution -> identity confirmation -> repository analysis -> community analysis -> project type -> deep validation -> claim consistency -> risk validation -> rule engine -> final explanation`

Deterministic validators remain the control layer. Model-assisted steps are used only where ranking, semantic interpretation or final explanation adds value.

## Local Verification

Local `http://127.0.0.1:8008/` is optional. It is not the main product entrypoint and is not required for normal end-user access through Telegram.

Use the local path only when you need one of these:

- reproduce a recorded review locally;
- inspect raw workflow stages and traces;
- verify the frontend bundle together with the backend viewer;
- run the project without deployment infrastructure.

### Prerequisites

- Python 3.11+
- Node.js 20+ with npm

### Build the frontend bundle

```bash
cd viewer_frontend
npm ci
npm run build
cd ..
```

### Run the backend test suite

```bash
python3 -m unittest discover -s tests -q
```

### Run one recorded case

```bash
python3 -m identity_validator.cli run-case cases/ton_punks/case.json --mode recorded --llm-mode template
```

### Start the local reviewer UI

```bash
python3 -m identity_validator.cli serve-viewer --host 127.0.0.1 --port 8008
```

Then open `http://127.0.0.1:8008/`.

## Execution Modes

### `recorded`

Uses saved public snapshots from `cases/<case>/snapshots`. This is the recommended deterministic review mode.

### `live`

Fetches fresh public signals from GitHub, Telegram, public web, market sources, TON account activity and optional wallet-backed TON MCP checks.

### `auto`

Uses recorded data when available, otherwise fetches live data.

## Mode Matrix

- `--mode recorded|live|auto` selects where evidence comes from
- `--llm-mode template|live` selects whether external model-backed ranking and explanation are enabled

Recommended combinations:

- `--mode recorded --llm-mode template`
- `--mode live --llm-mode template`
- `--mode live --llm-mode live`

`--enable-sonar` only matters when a Perplexity-compatible key is available.

## Live Mode Keys And External Dependencies

The repository intentionally contains no active API keys. `.env.example` lists variable names only.

Keys used only for `--llm-mode live`:

- `OPENAI_API_KEY`
- `OPENAI_API_URL`
- `PERPLEXITY_API_KEY`
- `PERPLEXITY_API_URL`

Optional TON-native live variables:

- `TON_MCP_MNEMONIC`
- `TON_MCP_PRIVATE_KEY`
- `TON_MCP_CONFIG_PATH`
- `TON_MCP_NETWORK`
- `TON_MCP_COMMAND`

External services used by live mode:

- GitHub API and GitHub repository HTML pages
- TGStat, TGChannels and the public Telegram mirror at `t.me/s/...`
- DuckDuckGo HTML search
- CoinGecko and GeckoTerminal
- Toncenter public API
- optional OpenAI-compatible endpoint
- optional Perplexity-compatible endpoint
- optional TON MCP process

Important notes:

- `recorded` mode does not require live API keys
- `.env.example` is a reference file, not an auto-loaded runtime config
- if direct `api.openai.com` access is blocked in the target environment, point `OPENAI_API_URL` to an accessible OpenAI-compatible endpoint

