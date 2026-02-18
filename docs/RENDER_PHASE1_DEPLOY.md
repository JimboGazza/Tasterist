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
- `TASTERIST_IMPORT_TIMEOUT_SEC=120`
- `TASTERIST_CANONICAL_HOST=tasterist.com`
- `TASTERIST_OWNER_EMAIL=james@penninegymnastics.com`
- `TASTERIST_OWNER_BOOTSTRAP_PASSWORD=<set a strong one>`
- `TASTERIST_DEV_TOOLS_ENABLED=0`
- `TASTERIST_EXCEL_SYNC_LOCAL_ONLY=0` (cloud writes to `/var/data/taster-sheets`)

Recommended for cloud stability:
- Keep imports manual from the import page/account actions (login no longer triggers imports).

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
2. Keep that Render page open and copy the DNS values it gives you.
3. In GoDaddy:
   - Open `My Products` -> `Domains` -> `tasterist.com` -> `DNS`.
   - Remove conflicting records for `@` and `www` (old A/AAAA/CNAME entries).
   - Add records exactly as Render shows:
     - Apex/root (`@`) record(s) for `tasterist.com`.
     - `CNAME` for `www`.
4. Wait for DNS propagation (can take a few minutes up to 24-48 hours).
5. Confirm HTTPS cert is active in Render before sharing with staff.
