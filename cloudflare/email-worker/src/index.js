import { EmailMessage } from "cloudflare:email";

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function extractEmail(input) {
  const value = String(input || "").trim();
  const match = value.match(/<([^>]+)>/);
  return (match ? match[1] : value).trim().toLowerCase();
}

function escHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function mimePart(contentType, data) {
  return (
    `Content-Type: ${contentType}; charset=UTF-8\r\n` +
    "Content-Transfer-Encoding: 8bit\r\n\r\n" +
    `${data}\r\n`
  );
}

function buildMime({ fromHeader, to, subject, text, html }) {
  const boundary = `b-${crypto.randomUUID()}`;
  const messageIdDomain = extractEmail(fromHeader).split("@")[1] || "tasterist.com";
  const messageId = `<${crypto.randomUUID()}@${messageIdDomain}>`;
  const nowRfc2822 = new Date().toUTCString();
  const safeSubject = String(subject || "").replace(/\r|\n/g, " ").trim();
  const plainText = String(text || "").trim();
  const htmlBody = String(html || "").trim() || `<pre>${escHtml(plainText)}</pre>`;

  return [
    `From: ${fromHeader}`,
    `To: ${to}`,
    `Subject: ${safeSubject}`,
    `Date: ${nowRfc2822}`,
    `Message-ID: ${messageId}`,
    "MIME-Version: 1.0",
    `Content-Type: multipart/alternative; boundary="${boundary}"`,
    "",
    `--${boundary}`,
    mimePart("text/plain", plainText || "No plain text body provided."),
    `--${boundary}`,
    mimePart("text/html", htmlBody),
    `--${boundary}--`,
    "",
  ].join("\r\n");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatError(err) {
  if (!err) {
    return "Unknown error";
  }
  if (typeof err === "string") {
    return err;
  }
  const parts = [];
  if (err.name) {
    parts.push(String(err.name));
  }
  if (err.message) {
    parts.push(String(err.message));
  }
  if (err.cause) {
    parts.push(`cause=${String(err.cause)}`);
  }
  if (parts.length > 0) {
    return parts.join(": ");
  }
  try {
    return JSON.stringify(err);
  } catch (_jsonErr) {
    return String(err);
  }
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
  } catch (_err) {
    return json({ error: "invalid_json" }, 400);
  }

  const fromHeader = String(body?.from || env.DEFAULT_FROM || "").trim();
  const fromAddr = extractEmail(fromHeader);
  const subject = String(body?.subject || "").trim();
  const text = String(body?.text || "").trim();
  const html = String(body?.html || "").trim();

  // Hard lock: do not trust request "to". Always send to owner.
  const toAddr = String(env.OWNER_EMAIL || "").trim().toLowerCase();

  if (!fromAddr || !subject || (!text && !html) || !toAddr) {
    return json({ error: "missing_required_fields" }, 400);
  }

  const maxAttempts = 3;
  let lastErr = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      // EmailMessage body stream is single-use. Rebuild per attempt.
      const raw = buildMime({ fromHeader, to: toAddr, subject, text, html });
      const message = new EmailMessage(fromAddr, toAddr, raw);
      await env.TASTERIST_SEND.send(message);
      return json({ ok: true, to: toAddr, attempt });
    } catch (err) {
      lastErr = err;
      if (attempt < maxAttempts) {
        await sleep(attempt * 250);
      }
    }
  }
  return json({ error: "send_failed", detail: formatError(lastErr) }, 502);
}

async function handleScheduled(env) {
  const url = String(env.RENDER_CRON_URL || "").trim();
  const token = String(env.RENDER_CRON_TOKEN || "").trim();

  if (!url || !token) {
    console.log("cron skipped: missing RENDER_CRON_URL or RENDER_CRON_TOKEN");
    return;
  }

  const response = await fetch(url, {
    method: "POST",
    headers: { "X-Tasterist-Cron-Token": token },
  });
  const body = await response.text();
  console.log(`cron status=${response.status} body=${body.slice(0, 300)}`);
}

export default {
  async fetch(request, env) {
    return handleWebhook(request, env);
  },

  async scheduled(_event, env, _ctx) {
    await handleScheduled(env);
  },
};
