# Carlisle Energy – SiteWalk + Utility Bill Intake

Mobile-friendly field data collection for refrigeration site walks **plus** an optional utility-bill upload/extraction workflow (AI/OCR), built as a **single-page web app** served by a **Flask backend**.

This repo is intentionally “simple to deploy”: the core SiteWalk data is stored in local JSON files, while the bill-intake subsystem uses PostgreSQL.

---

## What you get

### SiteWalk (core app)
- **Field-friendly UI** (single-page app in `index.html`) for collecting site/customer info and refrigeration equipment data.
- **Project management**: create/update/duplicate projects, search/sort, pagination, and a “Recently Deleted” archive.
- **Autosave** (global and per-project) to reduce data loss during field use.
- **Excel-friendly output** (see `EXCEL_INSTRUCTIONS.md`) plus CSV import/export helpers.
- **Print view PDF** generation endpoint (server-side via WeasyPrint).

### Utility bill intake (optional feature)
- Upload utility bills (PDF + common image formats) per project.
- **Async extraction** with polling endpoints for status/progress.
- **Two extraction approaches**:
  - **Text-based pipeline (default)**: `/process?...method=text` (uses `bills/` + job queue)
  - **Vision-based legacy extractor**: `/process?...method=vision` (uses `bill_extractor.py`)
- Review/correct extracted data; corrections can be saved for future “training hints.”
- Grouped and detailed bill views, plus CSV export for downstream import.

---

## Architecture at a glance

### Frontend
- `index.html` is the entire UI (HTML/CSS/JS).
- The SPA calls backend endpoints under `/api/*`.

### Backend (Flask)
- `app.py` creates the Flask app and registers blueprints in `routes/`.
- `main.py` runs the server locally.
- Convenience scripts:
  - `./start.sh` (background start, logs to `.run/app.log`, default port **5001**)
  - `./stop.sh`

### Storage
- **Projects (SiteWalk)**: stored as JSON files on disk:
  - `projects_data.json` (projects by user bucket)
  - `users.json` (lightweight users/roles; no real auth)
  - `deleted_projects.json` (Recently Deleted archive; 30-day retention)
  - `autosave_data.json` (per-user autosave)
  - `project_autosaves.json` (per-project autosave)
- **Bills (Bill Intake)**:
  - Files saved on disk in `bill_uploads/`
  - Bill metadata + extracted data stored in Postgres via `bills_db.py`
  - Optional annotation uploads stored in `bill_screenshots/`

---

## Running locally

### Requirements
- Python **3.11+**
- Recommended: a virtualenv
- Optional (only if using bill intake): a running PostgreSQL database
- Optional (only if using OCR): a working **Tesseract** install
- Optional (only if generating PDFs): WeasyPrint system deps (platform-specific)

### Install
For full functionality, use `requirements.txt` (the `pyproject.toml` dependency list is minimal):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run (simple)
```bash
python main.py
```

- Defaults: `HOST=0.0.0.0`, `PORT=5000` (override via env vars)

Then open `http://localhost:5000`.

### Run (scripted)
```bash
./start.sh
```

- Defaults: `PORT=5001`
- Logs: `.run/app.log`

Stop with:

```bash
./stop.sh
```

---

## Configuration: `config.yml` + environment variables

- **`config.yml`**: non-secret defaults and deploy-safe settings (feature flags, paths, limits)
- **Environment variables / `.env`**: secrets and per-environment overrides (API keys, DB URL, etc)

**Precedence:** environment variables override `config.yml`.

The backend will load a `.env` file if present (via `python-dotenv`).

### Create a local `.env`
Option 1 (recommended):

```bash
./setup_env.sh
```

Option 2:

```bash
cp .env.example .env
```

Then edit `.env` and fill in your credentials.

### Suggested `.env` template (create a file named `.env`)
```bash
# Feature flags
UTILITY_BILLS_ENABLED=true

# Bill intake (required if bills enabled + used)
DATABASE_URL=postgresql://user:password@host:5432/dbname
XAI_API_KEY=your_key_here

# Optional integrations
GOOGLE_PLACES_API_KEY=your_key_here
DROPBOX_APP_KEY=your_key_here
DROPBOX_APP_SECRET=your_key_here
DROPBOX_REFRESH_TOKEN=your_refresh_token_here

# Optional overrides
MAX_UPLOAD_MB=50
BILL_UPLOADS_DIR=bill_uploads
CORS_ORIGINS=http://localhost:5000
LOG_LEVEL=INFO
APP_CONFIG_PATH=config.yml
```

---

## Important API endpoints (high-level)

### SPA + health
- `GET /` → serves `index.html`
- `GET /health` → health check (does not depend on DB)

### Configuration / feature flags
- `GET /api/config` → `{ billsEnabled, googlePlacesEnabled }`

### Projects (SiteWalk)
- `GET /api/projects` → list project summaries (paginated via `limit`/`offset`)
- `GET /api/projects/<project_id>` → fetch a project
- `PUT /api/projects/<project_id>` → update a project
- `POST /api/projects/create` → create an empty project (simple API-friendly create)
- `POST /api/projects/<project_id>/duplicate` (or `/api/projects/duplicate/<project_id>`) → duplicate project (attempts to clone bill data too)
- `POST /api/import-csv` → import a project from a CSV exported by the app
- `DELETE /api/data/<project_id>` → soft-delete (moves to Recently Deleted)

**Legacy / UI-specific endpoint**
- `POST /api/data` → used by the SPA; **requires headers**:
  - `X-Client-Action`: `create_new` | `duplicate` | `import`
  - `Idempotency-Key`: required
  - `X-Client-Version`: must match server build ID (currently `2025-12-17-v1`) or returns `409 BUILD_MISMATCH`

### Recently Deleted
- `GET /api/deleted-projects`
- `POST /api/deleted-projects/<project_id>/restore`
- `DELETE /api/deleted-projects/<project_id>` (permanent delete)
- `POST /api/deleted-projects/bulk-restore`
- `POST /api/deleted-projects/bulk-delete`

### Autosave
- `GET|POST|DELETE /api/autosave` (per-user)
- `GET|POST|DELETE /api/projects/<project_id>/autosave` (per-project)
- `GET /api/autosaves` (list autosaves for user)

### Print view PDF
- `GET /api/projects/<project_id>/print.pdf` → generates a PDF (attachment)

### Bills (per project)
- `GET /api/bills/enabled`
- `GET /api/projects/<project_id>/bills` → files + reads + summary
- `POST /api/projects/<project_id>/bills/upload` → upload a file (SHA-256 duplicate detection)
- `POST /api/projects/<project_id>/bills/process/<file_id>?method=text|vision` → trigger extraction (async; default `text`)
- `GET /api/projects/<project_id>/bills/status` → per-file polling status
- `GET /api/projects/<project_id>/bills/job-status` → aggregated counts for progress bars
- `GET /api/bills/file/<file_id>/progress` → legacy progress endpoint
- `GET /api/bills/status/<file_id>` → granular job queue status
- `DELETE /api/projects/<project_id>/bills/files/<file_id>` → delete a bill file
- `GET /api/projects/<project_id>/bills/grouped` → grouped accounts/meters view
- `GET /api/projects/<project_id>/bills/detailed` → detailed extraction payloads
- `GET /api/projects/<project_id>/bills/files/<file_id>/review` → file + extraction payload
- `PUT /api/projects/<project_id>/bills/files/<file_id>/update` → update extraction payload
- `POST /api/projects/<project_id>/bills/files/<file_id>/approve` → approve and upsert data
- `POST /api/projects/<project_id>/bills/files/<file_id>/corrections` → save corrections/training hints
- `GET /api/bills/training/<utility_name>` → fetch training hints
- `GET /api/bills/file/<file_id>/pdf` → serve original PDF
- `GET /api/bills/file/<file_id>/bills` → bills created from that file
- `GET /api/bills/<bill_id>/review` → bill review payload
- `PATCH /api/bills/<bill_id>` → update bill fields
- `PATCH /api/bills/<bill_id>/manual-fix` → update + mark parent file OK
- `POST /api/projects/<source_project_id>/bills/copy-to/<target_project_id>` → copy bill data between projects
- Analytics/export:
  - `GET /api/projects/<project_id>/bills/summary`
  - `GET /api/accounts/<account_id>/summary`
  - `GET /api/meters/<meter_id>/bills`
  - `GET /api/accounts/<account_id>/meters/<meter_id>/months`
  - `GET /api/bills/<bill_id>/detail`
  - `GET /api/projects/<project_id>/bills/export-csv`

### Dropbox export (optional)
- `POST /api/upload_csv_to_dropbox` → upload a CSV payload to Dropbox (uses `services/dropbox_service.py`)
- Additional legacy Dropbox/export endpoints live in `backend_upload_to_dropbox.py`.

### Google Places (optional)
- `GET /api/place-autocomplete`
- `GET /api/place-details`
- `GET /api/nearby-businesses`
- `GET /api/geocode`

---

## Where to look in the code

### Core app
- `app.py`: Flask app factory + blueprint registration + response-size limiter + `.env` loading
- `routes/`: API blueprints (projects, bills, autosave, deleted projects, config, etc.)
- `stores/project_store.py`: JSON file storage (projects/users/deleted/autosaves)

### Bills system
- `bills_db.py`: Postgres schema + DB access for bill files, normalized bills, training data, screenshots, exports
- `bills/`: text-based pipeline (normalize/clean/parse/cache/job queue)
- `bill_extractor.py`: legacy vision extractor + shared extraction helpers

---

## Notes / gotchas
- **Auth model**: there is no real authentication; projects are bucketed by `X-User-Id` header and default to a shared `default` admin user.
- **/api/data POST is strict**: requires `Idempotency-Key`, allowed `X-Client-Action`, and enforces a build ID match (`X-Client-Version`).
- **Bills DB is lazy-initialized**: bills tables are created on first bills request (so `/health` stays fast even if DB is down).
- **Large payload protections**: JSON responses over a size limit are blocked to prevent crashes in constrained hosts.
- **Replit deployment redirect**: if `REPLIT_DEPLOYMENT` is set, requests may be redirected to a custom domain.

---

## Agent rules (repo-specific)

### Duplicate input handling
If the user sends multiple messages containing overlapping or repeated task lists, instructions, or commands:
- Treat them as a single continuous instruction set.
- Do NOT reset progress.
- Do NOT duplicate work.
- Do NOT change course unless the user explicitly says **"override"**, **"replace"**, or **"ignore previous"**.
- Always assume later messages may be continuations or accidental re-sends caused by UI truncation.

Default behavior: continue current execution plan.