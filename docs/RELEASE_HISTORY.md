# Tasterist Release History (Backfilled)

This is a backfilled version plan based on every commit currently in git history.

## Proposed Release Cut Points

| Version | Commit Range | Why this would have been a release |
|---|---|---|
| 0.1.0 | `d2535e6` | Initial app baseline and first stable UI/admin shape. |
| 0.1.1 | `379901f` -> `1cfc4f3` | Excel slot and leaver sync correctness fixes. |
| 0.2.0 | `ac0aac1` -> `46c2c61` | Admin followups unified, dashboard redesign/refinement. |
| 0.3.0 | `a52c8e3` -> `242f030` | PWA + domain/canonical-host deployment support. |
| 0.4.0 | `5e9bc7b` -> `24d5cdf` | Auth hardening, startup reliability, safer import behavior. |
| 0.5.0 | `902dc84` -> `326d40a` | Upload/manual import workflow and scheduling/import UX improvements. |
| 0.6.0 | `9fc9971` -> `b69e197` | SQLite->Postgres migration tooling, unknown-class diagnostics, PM fixer. |
| 1.0.0 | `0364f2e` -> `e4d6bab` | Runtime switched to Postgres primary; major app UX and admin flow stabilization. |
| 1.0.1 | `9628434` -> `46ece77` | Cloudflare email worker integration and MIME fix. |

## Full Change Ledger (Chronological)

1. `d2535e6` (2026-02-17) Initial app baseline with UI/admin improvements
2. `379901f` (2026-02-17) Fix Excel 4:45 slot matching and harden login import
3. `1cfc4f3` (2026-02-17) Fix leaver slot sync by day/time and add leaver checklist fields
4. `ac0aac1` (2026-02-17) Unify admin followups, add club fees flow, and cloud preflight
5. `65dd5bf` (2026-02-17) Redesign dashboard full-width with programme today lists and add Render runbook
6. `46c2c61` (2026-02-17) Refine dashboard clock/list sizing and add cloud backup action
7. `a52c8e3` (2026-02-17) Add PWA install support and run/domain hosting guides
8. `242f030` (2026-02-17) Add tasterist.com canonical host support and GoDaddy deploy steps
9. `5e9bc7b` (2026-02-17) Harden auth: owner-only account creation, CSRF, rate limiting, and secure defaults
10. `adbe1ce` (2026-02-17) Fix startup crash by running init_db after helper definitions
11. `fc3f07b` (2026-02-17) Reduce Render gunicorn worker count to prevent boot restart loops
12. `0a726c2` (2026-02-17) Fix Render sqlite lock boot loop with single worker and DB init retries
13. `5f66d3c` (2026-02-17) Add break-glass owner password reset via env var
14. `6418786` (2026-02-17) Relax password policy to 7 chars with upper/lowercase only
15. `fddd72c` (2026-02-17) Adjust password policy to uppercase + number + min 7
16. `bf7e75e` (2026-02-17) Switch cloud to manual imports and allow Excel sync in configured sheets folder
17. `24d5cdf` (2026-02-17) Make cloud import path safer and prevent table clear on missing sheets
18. `902dc84` (2026-02-18) Add upload-based manual import workflow and storage status visibility
19. `c862e86` (2026-02-18) Fix importer to use app DB path and bootstrap missing tables
20. `ae61f0b` (2026-02-18) Handle GET on import endpoints with friendly redirects
21. `9db9b56` (2026-02-18) Polish admin console, import flow, and name normalization
22. `8ad68bc` (2026-02-18) Make imports safe against empty/unreadable workbook runs
23. `600e93f` (2026-02-18) Pin 2025 imports to local archive and improve import guidance
24. `5fbac3b` (2026-02-18) Restore class grid fallback and relax 2025 local import strictness
25. `326d40a` (2026-02-18) Improve class scheduling UX and make imports merge-safe by default
26. `9fc9971` (2026-02-18) Add SQLite-to-Postgres migration tooling and runbook
27. `ecb64e8` (2026-02-18) Deploy latest app fixes (security, email, UI, admin)
28. `b69e197` (2026-02-18) Add unknown-class filter + diagnostics and PM time fixer
29. `0364f2e` (2026-02-19) Switch runtime DB to Postgres primary and harden startup
30. `4965f3e` (2026-02-19) Split manual add into dedicated screens and remove OneDrive sync toggle
31. `090fe54` (2026-02-19) Apply manual class timetable and tighten weekly schedule fallback
32. `ec06b3a` (2026-02-19) Fix admin password loop and disable forced strong-password flow
33. `1ae633e` (2026-02-19) Fix Postgres admin-day upserts and sidebar flash placement
34. `53d3562` (2026-02-19) Dock today summary cards and switch UI dates to UK format
35. `cec95a2` (2026-02-19) Trim Pennine Gymnastics prefix in admin to-action class labels
36. `0f65b0b` (2026-02-19) Simplify admin tasks subtitle to past 3 months
37. `3cee970` (2026-02-19) Skip orphan preschool Saturday rows during workbook import
38. `999de34` (2026-02-19) Harden taster date guardrails and polish button motion
39. `e4d6bab` (2026-02-19) Refine dashboard navigation, settings links, and app-taster export
40. `9628434` (2026-02-19) Add Cloudflare email worker config and leaver form/back-button fixes
41. `46ece77` (2026-02-19) Fix Cloudflare email MIME headers (Date/Message-ID)

## How to Bump Version Going Forward

1. Edit the root file `VERSION`.
2. Commit with a release message, for example `Release 1.0.2`.
3. Deploy.

The login page now reads from `VERSION` automatically.
