# app-validator

Angular dashboard prototype with a local proxy backend for Turing labeling APIs.

## Setup

1. Create `.env.local` in the project root.
2. Set `TURING_COOKIE` in `.env.local`.
3. Optionally set `TASK_OUTPUT_DIR` in `.env.local` if your task output folder is not `D:\Turing\Projects\workspace\task-output`.
4. Optionally set `SCHEMA_CACHE_DIR` in `.env.local` if you want shared schema files cached outside the default `TASK_OUTPUT_DIR/schema` folder.
5. Optionally override Oracle connection settings in `.env.local`:
   - `ORACLE_SPIDER_2_LITE_HOST`
   - `ORACLE_SPIDER_2_LITE_PORT`
   - `ORACLE_SPIDER_2_LITE_SERVICE`
   - `ORACLE_SPIDER_2_LITE_USER`
   - `ORACLE_SPIDER_2_LITE_PASSWORD`
   - `ORACLE_SPIDER_2_LITE_MODE`
   - `ORACLE_BIGQUERY_PUBLIC_DATA_HOST`
   - `ORACLE_BIGQUERY_PUBLIC_DATA_PORT`
   - `ORACLE_BIGQUERY_PUBLIC_DATA_SERVICE`
   - `ORACLE_BIGQUERY_PUBLIC_DATA_USER`
   - `ORACLE_BIGQUERY_PUBLIC_DATA_PASSWORD`
   - `ORACLE_BIGQUERY_PUBLIC_DATA_MODE`
6. Optionally set `DEBUG_BACKEND=true` in `.env.local` to enable backend debug logs.
7. Optionally set `BACKEND_LOG_LEVEL=debug|info|warn|error` in `.env.local` to control backend log verbosity.
8. Optionally set workflow runner paths in `.env.local`:
   - `LLM_TRAINER_PROJECT_DIR`
   - `PYTHON_EXECUTABLE`
   - `VALIDATION_REPORTS_DIR`
9. Install dependencies with `npm install`.
10. Start the backend with `npm run start:server`.
11. In a second terminal, start the frontend with `npm run start:client`.

If you prefer one command, `npm start` starts both processes together.

The Angular app runs on `http://localhost:4200` and proxies `/api/*` to the local backend on `http://127.0.0.1:3000`.

## Troubleshooting

If Vite shows `http proxy error` with `ECONNREFUSED`, the frontend is running but the proxy backend is not listening on port `3000`.
Run `npm run start:server` directly and confirm you see `Proxy backend listening on http://127.0.0.1:3000`.

For backend debugging, set `DEBUG_BACKEND=true` in `.env.local` and restart the proxy. This enables route logs and upstream service-call logs in the terminal. Use `BACKEND_LOG_LEVEL=debug` if you want the most verbose output.

When the backend starts, it now performs a one-time shared schema warm-up instead of generating schema files task-by-task. The warm-up:
- checks the configured Oracle connection through the routed profile from task metadata
- discovers eligible schemas from the current project conversations
- writes shared schema files under `TASK_OUTPUT_DIR/schema` by default, or under `SCHEMA_CACHE_DIR` if you override it

Schema generation is no longer part of the per-task `Fetch Data` flow.

The report page also has three task actions backed by the existing Python agent workflows:
- `Re-Validate` runs phase 4 validation via the master validator
- `Generate Outputs` runs the phase 3 pipeline
- `Publish` submits the current task artifacts back to the labeling tool

Each action writes a per-task log file under `<TASK_OUTPUT_DIR>/<taskId>/_logs`, and the report page shows the command, log file path, and output tail for debugging.

## Available Scripts

- `npm start` starts the Angular dev server and the proxy backend together.
- `npm run start:server` starts only the proxy backend.
- `npm run start:client` starts only the Angular frontend.
- `npm run build` builds the Angular app.
- `npm run test:backend` runs backend helper tests.
