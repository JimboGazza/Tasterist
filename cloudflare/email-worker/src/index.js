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
    const propDump = {};
    for (const key of Object.getOwnPropertyNames(err)) {
      if (key === "stack") {
        continue;
      }
      try {
        propDump[key] = err[key];
      } catch (_readErr) {
        propDump[key] = "<unreadable>";
      }
    }
    if (Object.keys(propDump).length > 0) {
      parts.push(`props=${JSON.stringify(propDump)}`);
    }
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
    const hasSendBinding = Boolean(env?.TASTERIST_SEND && typeof env.TASTERIST_SEND.send === "function");
    return json({
      ok: true,
      service: "tasterist-email-webhook",
      has_send_binding: hasSendBinding,
      owner_email_set: Boolean(String(env?.OWNER_EMAIL || "").trim()),
      default_from_set: Boolean(String(env?.DEFAULT_FROM || "").trim()),
    });
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

  // Lock sender to configured default to avoid invalid/unverified From headers.
  const fromHeader = String(env.DEFAULT_FROM || body?.from || "").trim();
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
      await env.TASTERIST_SEND.send({
        from: fromAddr,
        to: toAddr,
        subject,
        text: text || undefined,
        html: html || undefined,
      });
      return json({ ok: true, to: toAddr, attempt });
    } catch (err) {
      lastErr = err;
      if (attempt < maxAttempts) {
        await sleep(attempt * 250);
      }
    }
  }
  console.log(`send_email failed after ${maxAttempts} attempts: ${formatError(lastErr)}`);
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
