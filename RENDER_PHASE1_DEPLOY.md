# Render Phase 1 Deploy Runbook

## 1. Create the service from GitHub
1. In Render: `New` -> `Blueprint`.
2. Select repo: `JimboGazza/Tasterist`.
3. Confirm it reads `render.yaml`.

## 2. Storage
1. Attach persistent disk:
   - Mount path: `/var/data`
   - Size: `5 GB` (or more)
2. Create sheets directory in shell once deployed:
   - `mkdir -p /var/data/taster-sheets`

## 3. Environment variables (verify)
- `TASTERIST_SECRET_KEY` (auto generated)
- `TASTERIST_DB_FILE=/var/data/tasterist.db`
- `TASTER_SHEETS_FOLDER=/var/data/taster-sheets`
- `TASTERIST_LOGIN_IMPORT_ENABLED=1`
- `TASTERIST_LOGIN_IMPORT_MINUTES=15`
- `TASTERIST_IMPORT_TIMEOUT_SEC=120`

## 4. First boot checks
1. Open `/health` and confirm JSON `status: ok`.
2. Login with admin.
3. Open `/cloud/preflight` and ensure cards are green.
4. Run one import from account settings.
5. Confirm Dashboard monitor is green/amber (not red).

## 5. Upload sheets for cloud import
- Cloud service cannot read Mac local paths.
- Place `.xlsx` taster sheets in `/var/data/taster-sheets` on the Render instance.
- Keep year folder structure if you want (e.g. `/var/data/taster-sheets/2026/...`).

## 6. Staff rollout
1. Create staff accounts.
2. Share app URL.
3. Ask staff to validate:
   - Add taster
   - Record leaver
   - Use Admin Tasks toggles
   - Confirm monitor status.

## 7. Backups
- Manual backup download (admin): `/cloud/backup`
- Store downloaded `.db` files in secure cloud storage.

## 8. Custom domain (`tasterist.com`)
1. In Render service:
   - `Settings` -> `Custom Domains`
   - Add `tasterist.com`
   - Add `www.tasterist.com`
2. In your DNS provider:
   - Create DNS records exactly shown by Render for both hostnames.
3. Optional redirect:
   - Redirect `www.tasterist.com` -> `tasterist.com` (or opposite, your choice).
4. Confirm HTTPS cert is active in Render before sharing with staff.
