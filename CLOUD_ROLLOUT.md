# Cloud Rollout (Phase 1)

## Current deployment-ready pieces
- `wsgi.py` entrypoint for WSGI servers.
- `Procfile` for managed hosts (Render/Railway/Fly compatible style).
- `Dockerfile` for container deploys.
- `gunicorn` added to `requirements.txt`.
- Health endpoint:
  - `/health`
- App secret already supports env var:
  - `TASTERIST_SECRET_KEY`
- DB file path supports env var:
  - `TASTERIST_DB_FILE`

## Phase 1: Shared Cloud App (SQLite + Disk)
1. Deploy to Render or Railway from this repo.
2. Configure start command:
   - `gunicorn wsgi:app --bind 0.0.0.0:$PORT`
3. Add environment variables:
   - `TASTERIST_SECRET_KEY=<long random string>`
   - `TASTERIST_DB_FILE=/var/data/tasterist.db` (or provider disk mount path)
   - `TASTER_SHEETS_FOLDER=<shared synced folder path>`
   - `TASTERIST_LOGIN_IMPORT_ENABLED=1` (auto-import stays on)
   - `TASTERIST_LOGIN_IMPORT_MINUTES=15` (throttle import frequency)
   - `TASTERIST_IMPORT_TIMEOUT_SEC=120` (prevent hanging requests)
4. Attach persistent disk storage to keep the SQLite file.
5. Health check path:
   - `/health`
6. Validate with:
   - login,
   - import run,
   - add/toggle taster,
   - add leaver,
   - admin tasks page.

## Phase 2: Move DB to Managed Postgres
1. Create managed Postgres (Render/Railway/Neon/Supabase).
2. Add SQLAlchemy migration layer or direct migration scripts.
3. Port SQLite-specific SQL (`strftime`) to Postgres equivalents.
4. Move from local file transactions to pooled DB connections.
5. Add automated backups and retention policy.

## Phase 3: Scheduled Import Worker
1. Move import away from login-trigger model.
2. Add scheduled worker (hourly/nightly).
3. Persist import run status + errors in DB.
4. Surface latest status in dashboard monitor.

## Extra
- Detailed follow-on checklist:
  - `CLOUD_NEXT_STEPS.md`
