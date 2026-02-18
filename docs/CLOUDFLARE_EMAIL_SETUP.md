# Cloudflare Email Setup (Owner-Only Weekly Reports)

This app now supports sending weekly admin reports through a webhook.

## 1) Keep sending locked to owner only

Set these Render env vars:

- `TASTERIST_EMAIL_ENABLED=1`
- `TASTERIST_EMAIL_OWNER_ONLY=1`
- `TASTERIST_OWNER_EMAIL=<your email>`
- `TASTERIST_EMAIL_WEBHOOK_URL=<your Cloudflare Worker URL>`
- `TASTERIST_EMAIL_WEBHOOK_TOKEN=<shared secret>`
- `TASTERIST_CRON_TOKEN=<shared secret for /cron/weekly-admin-report>`

With `TASTERIST_EMAIL_OWNER_ONLY=1`, the app only sends to `TASTERIST_OWNER_EMAIL`.

## 2) Cloudflare Worker webhook (MailChannels transport)

Create a Worker and deploy this code:

```javascript
export default {
  async fetch(request, env) {
    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    const auth = request.headers.get("Authorization") || "";
    if (auth !== `Bearer ${env.WEBHOOK_TOKEN}`) {
      return new Response("Forbidden", { status: 403 });
    }

    const body = await request.json();
    const { from, to, subject, text, html } = body || {};
    if (!from || !to || !subject || (!text && !html)) {
      return new Response("Bad Request", { status: 400 });
    }

    const payload = {
      personalizations: [{ to: [{ email: to }] }],
      from: { email: from.includes("<") ? from.split("<")[1].replace(">", "").trim() : from },
      subject,
      content: [
        ...(text ? [{ type: "text/plain", value: text }] : []),
        ...(html ? [{ type: "text/html", value: html }] : []),
      ],
    };

    const resp = await fetch("https://api.mailchannels.net/tx/v1/send", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });

    const out = await resp.text();
    return new Response(out, { status: resp.status });
  },
};
```

Add Worker secret:

- `WEBHOOK_TOKEN=<same value as TASTERIST_EMAIL_WEBHOOK_TOKEN>`

## 3) Weekly cron trigger

In Cloudflare, create a Scheduled Trigger (weekly). From the cron worker, call:

- `POST https://tasterist.com/cron/weekly-admin-report`
- Header: `X-Tasterist-Cron-Token: <TASTERIST_CRON_TOKEN>`

Example call:

```bash
curl -X POST https://tasterist.com/cron/weekly-admin-report \
  -H "X-Tasterist-Cron-Token: YOUR_CRON_TOKEN"
```

## 4) Manual test

From Admin Console:

- Open `Settings -> Admin Console`
- Click `Send Weekly Report Now`

If delivery fails, check flash error and Worker logs.
