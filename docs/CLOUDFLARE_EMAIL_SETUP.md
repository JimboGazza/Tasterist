# Cloudflare Email Setup (Current, Owner-Only, Step-by-Step)

This setup keeps all email locked to owner only, even if someone changes app settings.

## What this app expects

From `app.py`, these env vars are used:

- `TASTERIST_EMAIL_ENABLED`
- `TASTERIST_EMAIL_OWNER_ONLY`
- `TASTERIST_OWNER_EMAIL`
- `TASTERIST_EMAIL_FROM`
- `TASTERIST_EMAIL_WEBHOOK_URL`
- `TASTERIST_EMAIL_WEBHOOK_TOKEN`
- `TASTERIST_CRON_TOKEN`

## 1) Cloudflare dashboard setup (where to click)

### 1.1 Enable Email Routing and verify destination

In Cloudflare dashboard:

1. Open your zone (domain), for example `tasterist.com`.
2. Go to `Email` -> `Email Routing`.
3. Click `Get started`.
4. Add a custom address you will send from, for example `noreply@tasterist.com`.
5. Add destination address as your real inbox (owner email).
6. Complete destination verification email.
7. Click `Add records and enable` when Cloudflare prompts for DNS records.

### 1.2 Create the Worker

In Cloudflare dashboard:

1. Go to `Workers & Pages`.
2. Click `Create` -> `Worker`.
3. Name it `tasterist-email-webhook`.
4. Open the editor and replace code with the Worker code below.
5. Deploy.

Alternative (CLI in this repo): use the included Worker project at:

- `cloudflare/email-worker/wrangler.toml`
- `cloudflare/email-worker/src/index.js`

Deploy command from repo root:

```bash
npx wrangler deploy --config cloudflare/email-worker/wrangler.toml
```

Worker code:

```javascript
import { EmailMessage } from "cloudflare:email";

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function extractEmail(input) {
  const v = String(input || "").trim();
  const m = v.match(/<([^>]+)>/);
  return (m ? m[1] : v).trim().toLowerCase();
}

function escHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function mimePart(contentType, data) {
  return `Content-Type: ${contentType}; charset=UTF-8\r\nContent-Transfer-Encoding: 8bit\r\n\r\n${data}\r\n`;
}

function buildMime({ fromHeader, to, subject, text, html }) {
  const boundary = `b-${crypto.randomUUID()}`;
  const safeSubject = String(subject || "").replace(/\r|\n/g, " ").trim();
  const plain = String(text || "").trim();
  const rich = String(html || "").trim() || `<pre>${escHtml(plain)}</pre>`;
  return [
    `From: ${fromHeader}`,
    `To: ${to}`,
    `Subject: ${safeSubject}`,
    "MIME-Version: 1.0",
    `Content-Type: multipart/alternative; boundary="${boundary}"`,
    "",
    `--${boundary}`,
    mimePart("text/plain", plain || "No plain text body provided."),
    `--${boundary}`,
    mimePart("text/html", rich),
    `--${boundary}--`,
    "",
  ].join("\r\n");
}

async function handleWebhook(request, env) {
  if (request.method === "GET") {
    return json({ ok: true, service: "tasterist-email-webhook" });
  }
  if (request.method !== "POST") {
    return json({ error: "method_not_allowed" }, 405);
  }

  const auth = request.headers.get("Authorization") || "";
  if (auth !== `Bearer ${env.WEBHOOK_TOKEN}`) {
    return json({ error: "forbidden" }, 403);
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return json({ error: "invalid_json" }, 400);
  }

  const fromHeader = String(body?.from || env.DEFAULT_FROM || "").trim();
  const fromAddr = extractEmail(fromHeader);
  const subject = String(body?.subject || "").trim();
  const text = String(body?.text || "").trim();
  const html = String(body?.html || "").trim();

  // Hard lock: always send to owner.
  const toAddr = String(env.OWNER_EMAIL || "").trim().toLowerCase();

  if (!fromAddr || !subject || (!text && !html) || !toAddr) {
    return json({ error: "missing_required_fields" }, 400);
  }

  const raw = buildMime({ fromHeader, to: toAddr, subject, text, html });
  const msg = new EmailMessage(fromAddr, toAddr, raw);

  try {
    await env.TASTERIST_SEND.send(msg);
    return json({ ok: true, to: toAddr });
  } catch (err) {
    return json({ error: "send_failed", detail: String(err) }, 502);
  }
}

async function handleScheduled(env) {
  const url = String(env.RENDER_CRON_URL || "").trim();
  const token = String(env.RENDER_CRON_TOKEN || "").trim();
  if (!url || !token) {
    console.log("cron skipped: missing RENDER_CRON_URL or RENDER_CRON_TOKEN");
    return;
  }

  const resp = await fetch(url, {
    method: "POST",
    headers: { "X-Tasterist-Cron-Token": token },
  });
  const txt = await resp.text();
  console.log(`cron status=${resp.status} body=${txt.slice(0, 300)}`);
}

export default {
  async fetch(request, env) {
    return handleWebhook(request, env);
  },

  async scheduled(_event, env, _ctx) {
    await handleScheduled(env);
  },
};
```

### 1.3 Add Worker variables/secrets/binding

In Worker -> `Settings`:

1. `Variables and Secrets`:
   - Secret: `WEBHOOK_TOKEN` (random long value)
   - Secret: `RENDER_CRON_TOKEN` (random long value)
   - Variable: `OWNER_EMAIL` = your owner inbox
   - Variable: `DEFAULT_FROM` = `Tasterist <noreply@tasterist.com>`
   - Variable: `RENDER_CRON_URL` = `https://tasterist.com/cron/weekly-admin-report`
2. `Bindings`:
   - Add binding type `Send Email`
   - Binding name: `TASTERIST_SEND`
   - Destination address: same owner inbox
3. `Triggers`:
   - Add cron trigger, weekly (UTC), example: `0 8 * * 1` (Monday 08:00 UTC)
4. Redeploy Worker.

If you are using Cloudflare Git deployments, set the deploy command to:

```bash
npx wrangler deploy --config cloudflare/email-worker/wrangler.toml
```

Do not use bare `npx wrangler deploy` at repo root.

## 2) Render env vars to set

Set these on Render web service:

- `TASTERIST_EMAIL_ENABLED=1`
- `TASTERIST_EMAIL_OWNER_ONLY=1`
- `TASTERIST_OWNER_EMAIL=<same as OWNER_EMAIL>`
- `TASTERIST_EMAIL_FROM=Tasterist <noreply@tasterist.com>`
- `TASTERIST_EMAIL_WEBHOOK_URL=https://tasterist-email-webhook.<your-subdomain>.workers.dev`
- `TASTERIST_EMAIL_WEBHOOK_TOKEN=<same as WEBHOOK_TOKEN>`
- `TASTERIST_CRON_TOKEN=<same as RENDER_CRON_TOKEN>`

Then redeploy Render service.

## 3) Generate secure token values (pasteable)

Run locally:

```bash
openssl rand -base64 48
```

Run twice:

- first output -> `WEBHOOK_TOKEN` and `TASTERIST_EMAIL_WEBHOOK_TOKEN`
- second output -> `RENDER_CRON_TOKEN` and `TASTERIST_CRON_TOKEN`

## 4) Test checklist

### 4.1 Test Worker webhook directly

```bash
curl -sS -X POST "https://tasterist-email-webhook.<your-subdomain>.workers.dev" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <WEBHOOK_TOKEN>" \
  --data '{
    "from":"Tasterist <noreply@tasterist.com>",
    "to":"ignored@example.com",
    "subject":"Tasterist webhook test",
    "text":"If you got this, Worker send_email is working."
  }'
```

Expected: JSON with `"ok":true`.

### 4.2 Test Render cron endpoint directly

```bash
curl -sS -X POST "https://tasterist.com/cron/weekly-admin-report" \
  -H "X-Tasterist-Cron-Token: <RENDER_CRON_TOKEN>"
```

Expected: JSON with `"status":"ok"` or `"status":"disabled"` (if email disabled).

### 4.3 Test from app UI

1. Go to `Settings -> Admin Console`.
2. Click `Send Weekly Report Now`.
3. Confirm owner inbox receives it.

## 5) Important safety locks

- App lock: `TASTERIST_EMAIL_OWNER_ONLY=1`
- Worker lock: code always sends to `OWNER_EMAIL`
- Binding lock: `send_email` destination is owner address

All 3 together prevent accidental user-wide email sends.
