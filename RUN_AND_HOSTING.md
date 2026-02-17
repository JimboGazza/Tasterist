# Run and Hosting Guide

## Local run (on your Mac)
1. Open Terminal in project folder:
   - `cd /Users/jamesgardner/Documents/Tasterist`
2. Activate env:
   - `source .venv/bin/activate`
3. Run app:
   - `python app.py`
4. Open:
   - `http://127.0.0.1:8501`

## Cloud run (recommended for staff)
- Deploy on Render with `render.yaml`.
- Then staff only need the web URL and login.
- No local Python setup needed for staff.

## Can it be on `tasterist.com`?
Yes.
- Use a domain registrar (GoDaddy, Namecheap, Cloudflare, etc).
- In Render service:
  - `Settings` -> `Custom Domains` -> add `tasterist.com` and `www.tasterist.com`.
- In DNS:
  - add records exactly as Render asks (usually CNAME/ALIAS and optional redirect).
- Enable HTTPS in Render (automatic cert).

## Can it be a standalone app?
Yes, easiest path is PWA (already added).
- Users open the cloud URL in browser and choose:
  - iPhone/iPad Safari: `Share` -> `Add to Home Screen`
  - Chrome/Edge desktop: `Install app`
- It opens in app-style standalone window.

## Native app later (optional)
- If needed later, wrap the web app with Tauri/Electron for Mac/Windows app installers.
- Keep same cloud backend and login.
