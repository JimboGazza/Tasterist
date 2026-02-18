# Cloud Next Steps

## Phase 1 (Now)
- Deploy with `render.yaml`.
- Follow: `docs/RENDER_PHASE1_DEPLOY.md`
- Share run/hosting basics with team: `docs/RUN_AND_HOSTING.md`
- Keep SQLite on mounted disk.
- Verify `/health` and manual import flow in shared environment.
- Verify `/cloud/preflight` is green after first deploy.
- Set `TASTER_SHEETS_FOLDER` to the shared cloud-accessible folder path.
- Set `TASTERIST_CANONICAL_HOST=tasterist.com` once custom domain is active.
- Keep import timeout configured:
  - `TASTERIST_IMPORT_TIMEOUT_SEC=120`

## Phase 1.5 (Hardening)
- Use `/cloud/backup` for manual admin-triggered DB backups.
- Add daily DB backup job for `/var/data/tasterist.db`.
- Add import retry logic for locked/unavailable workbook files.
- Add alerting if import status is `warn/error` for 2+ runs.

## Phase 2 (Database Upgrade)
- Move to managed Postgres.
- Migrate tables: `tasters`, `leavers`, `class_sessions`, `users`, `user_admin_days`, `audit_logs`.
- Update date/month SQL to Postgres equivalents.
- Add migration/versioning (Alembic or SQL migration scripts).

## Phase 3 (Importer Worker)
- Move imports out of login request path.
- Add scheduler/worker (every 15-30 minutes during business hours).
- Keep dashboard monitor sourced from latest worker run.

## Security & Access
- Keep only admin/staff accounts.
- Enforce strong password policy in app.
- Add account lockout/rate limit after repeated failures.
