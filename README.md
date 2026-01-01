# Carlisle Energy – SiteWalk + Utility Bill Intake

Mobile-friendly field data collection for refrigeration site walks **plus** an optional utility-bill upload/extraction workflow (AI/OCR), built as a **single-page web app** served by a **Flask backend**.

This repo is intentionally “simple to deploy”: the core SiteWalk data is stored in local JSON files, while the bill-intake subsystem uses PostgreSQL.

---

## What you get

### SiteWalk (core app)
- **Field-friendly UI** (single-page app in `index.html`) for collecting site/customer info and refrigeration equipment data.
- **Project management**: create/update/duplicate projects, search/sort, pagination, and a “Recently Deleted” archive.
- **Autosave** to reduce data loss during field use.
- **Excel-friendly output** (see `EXCEL_INSTRUCTIONS.md`) and Dropbox export integration.

### Utility bill intake (optional feature)
- Upload utility bills (PDF + some image formats) per project.
- **Async extraction** with progress tracking.
- **Two extraction approaches**:
  - **Text-based pipeline** (`bills/`): normalize → OCR fallback → clean → LLM parse → cache
  - **Vision-based legacy extractor** (`bill_extractor.py`): PDF/images → LLM vision extraction
- Review/correct extracted data; corrections can be saved for future “training hints.”

---

## Architecture at a glance

### Frontend
- `index.html` is the entire UI (HTML/CSS/JS).
- The SPA calls backend endpoints under `/api/*`.

### Backend (Flask)
- `app.py` is the main server: serves the SPA and exposes JSON APIs.
- `main.py` runs the server locally (port 5000).

### Storage
- **Projects (SiteWalk)**: stored as JSON files on disk:
  - `projects_data.json` (projects by user bucket)
  - `users.json` (very lightweight users/roles; no real auth)
  - `deleted_projects.json` (Recently Deleted archive; 30-day retention)
  - `autosave_data.json` (autosave snapshots)
- **Bills (Bill Intake)**:
  - Files saved on disk in `bill_uploads/`
  - Metadata + extracted data stored in Postgres via `bills_db.py`

---

## Running locally

### Requirements
- Python **3.11+**
- Recommended: a virtualenv
- Optional (only if using bill intake): a running PostgreSQL database
- Optional (only if using OCR): a working **Tesseract** installation on your machine

### Install
Use either `requirements.txt` or `pyproject.toml` dependencies. The simplest path:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run

```bash
python main.py
```

Then open `http://localhost:5000`.

### Local Dev (with Google Places API)

To use Google Places features (business search, address autocomplete), set the API key when starting the server:

```bash
GOOGLE_PLACES_API_KEY=your_key_here python main.py
```

Or export it in your shell session:

```bash
export GOOGLE_PLACES_API_KEY=your_key_here
python main.py
```

**Note:** The app will work without the API key, but Google Places features (like "Show Businesses Nearby") will return an error message instead of results.

---

## Configuration: `config.yml` + `.env` (recommended)

This project uses **Option A** configuration:

- **`config.yml`**: non-secret defaults and deploy-safe settings (feature flags, paths, limits)
- **`.env`**: secrets and per-environment overrides (API keys, DB URL, etc). Not committed.

**Precedence:** environment variables (including those loaded from `.env`) override `config.yml`.

To create a local `.env`, copy the template:

```bash
cp .env.example .env
```

## Key environment variables

### Core (always useful)
- **`UTILITY_BILLS_ENABLED`**: `"true"`/`"false"` (defaults to true in code)

### Bill intake / extraction (Postgres + xAI)
- **`DATABASE_URL`**: Postgres connection string (required if bills feature is enabled and used)
- **`XAI_API_KEY`**: xAI key used via OpenAI-compatible client (required for extraction)

### Dropbox export (optional)
Used for exporting CSVs and creating proposal folder structures:
- **`DROPBOX_APP_KEY`**
- **`DROPBOX_APP_SECRET`**
- **`DROPBOX_REFRESH_TOKEN`**
Fallbacks exist in code for a static token (`DROPBOX_ACCESS_TOKEN`), but refresh-token auth is preferred.

### Google Places (optional)
Used for business search/address autocomplete/geocoding:
- **`GOOGLE_PLACES_API_KEY`**

---

## Important API endpoints (high-level)

### SPA + health
- `GET /` → serves `index.html`
- `GET /health` → health check

### Projects (SiteWalk)
- `POST /api/data` → create/update a project (expects “create action” + idempotency headers)
- `GET /api/data?project=<id>` → fetch a project
- `GET /api/projects` → list project summaries (paginated)
- `DELETE /api/data/<project_id>` → soft-delete (moves to Recently Deleted)

### Bills (per project)
- `GET /api/projects/<project_id>/bills` → bill files + reads + summary
- `POST /api/projects/<project_id>/bills/upload` → upload file (stores record, does not extract)
- `POST /api/projects/<project_id>/bills/process/<file_id>` → trigger extraction (async)

---

## Where to look in the code

### Core app
- `app.py`: Flask server, project CRUD, “Recently Deleted”, autosave, Google Places proxy endpoints
- `index.html`: entire SPA UI and client-side logic

### Dropbox export
- `backend_upload_to_dropbox.py`: Flask blueprint for uploads/exports and folder creation

### Bills system
- `bills_db.py`: Postgres schema + DB access for bill files, accounts/meters, normalized bills, training data, screenshots, cache
- `bills/normalizer.py`: PDF text extraction + OCR fallback + spreadsheet parsing
- `bills/text_cleaner.py`: reduce/clean text for cost-effective LLM parsing
- `bills/parser.py`: two-pass LLM text parser
- `bills/cache.py`: content-addressed cache (hash of normalized text)
- `bill_extractor.py`: legacy vision-based extractor (PDF/images → vision model)

---

## Notes / gotchas
- **Auth model**: there is no real authentication; projects are bucketed by `X-User-Id` header and default to a shared `default` admin user.
- **Concurrency**: project storage is file-based JSON and not designed for high-concurrency multi-user workloads.
- **Large payload protections**: `app.py` includes response-size blocking and “safe print” truncation to prevent crashes in constrained environments.

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