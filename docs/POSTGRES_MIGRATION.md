# Postgres Migration (Phase 1)

This phase keeps the app running on SQLite while you migrate/sync data into Render Postgres.

## 1. Set Render env vars

In your Render web service:

- `DATABASE_URL` = your Render Postgres **Internal Database URL**
- Keep `TASTERIST_DB_FILE=/var/data/tasterist.db` for now

## 2. Install new dependency

`psycopg[binary]` is now in `requirements.txt`.

## 3. Run migration (one-time full seed)

From Render Shell (or local with network access to Postgres):

```bash
python scripts/migrate_sqlite_to_postgres.py \
  --sqlite /var/data/tasterist.db \
  --postgres-url "$DATABASE_URL" \
  --truncate-first
```

## 4. Run migration (incremental sync)

For subsequent syncs:

```bash
python scripts/migrate_sqlite_to_postgres.py \
  --sqlite /var/data/tasterist.db \
  --postgres-url "$DATABASE_URL"
```

## 5. Verify counts

Check logs from the script output for table counts:

- `users`
- `user_admin_days`
- `audit_logs`
- `class_sessions`
- `tasters`
- `leavers`

## 6. Important note

Phase 1 centralizes backup/state in Postgres, but the Flask app still reads/writes SQLite.
Phase 2 is switching app runtime to Postgres as the primary DB.
