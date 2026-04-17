/**
 * Cloudflare Worker — public HTTP bridge to RunPod serverless endpoint.
 *
 * Map: api.studio.nyptidindustries.com/* → RunPod /runsync (or /run for async)
 *
 * Secrets (set via `wrangler secret put`):
 *   RUNPOD_API_KEY    — your RunPod account API key
 *   RUNPOD_ENDPOINT_ID — endpoint ID from deploy.sh output
 *
 * Long-running jobs (>30s, e.g. full video render) automatically use /run (async)
 * and return a job_id that the frontend polls via /status/:job_id.
 *
 * Usage:
 *   wrangler deploy
 *   wrangler secret put RUNPOD_API_KEY
 *   wrangler secret put RUNPOD_ENDPOINT_ID
 */

const ASYNC_PATHS = [
  "/api/render/",        // all video renders
  "/api/generate/",      // AI gen jobs
  "/api/longform/",      // multi-minute processing
];

// Allowed origins — must be exact matches because we also send Allow-Credentials.
// Wildcard + credentials is forbidden by the CORS spec. Add any other frontend
// origin that needs direct API access here.
const ALLOWED_ORIGINS = new Set([
  "https://studio.nyptidindustries.com",
  "https://billing.nyptidindustries.com",
  "https://invoicer.nyptidindustries.com",
  "http://localhost:8080",
  "http://localhost:5173",
  "http://127.0.0.1:8080",
  "http://127.0.0.1:5173",
]);

function corsHeadersFor(request) {
  // Echo the requested origin back when it's in the allowlist; otherwise fall back
  // to the canonical studio origin so responses still carry CORS headers (useful
  // for error pages viewed directly in a browser).
  const origin = request.headers.get("Origin") || "";
  const allowOrigin = ALLOWED_ORIGINS.has(origin)
    ? origin
    : "https://studio.nyptidindustries.com";
  const reqHeaders = request.headers.get("Access-Control-Request-Headers") || "authorization,content-type";
  return {
    "Access-Control-Allow-Origin": allowOrigin,
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": reqHeaders,
    "Access-Control-Max-Age": "86400",
    "Vary": "Origin",
  };
}

function jsonResponse(obj, { status = 200, cors = {} } = {}) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json", ...cors },
  });
}

export default {
  async fetch(request, env, ctx) {
    const cors = corsHeadersFor(request);

    // Preflight — the browser fires OPTIONS before any fetch with an Authorization
    // header (non-simple request). We MUST respond with 2xx + CORS headers or the
    // real request never goes out.
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: cors });
    }

    const url = new URL(request.url);
    const apiKey = env.RUNPOD_API_KEY;
    const endpointId = env.RUNPOD_ENDPOINT_ID;

    if (!apiKey || !endpointId) {
      return jsonResponse({ error: "RunPod not configured" }, { status: 500, cors });
    }

    // ───────────────────────────────────────────────────────────
    // Status endpoint: GET /status/:job_id → RunPod status
    // ───────────────────────────────────────────────────────────
    if (url.pathname.startsWith("/status/")) {
      const jobId = url.pathname.split("/")[2];
      const statusResp = await fetch(
        `https://api.runpod.ai/v2/${endpointId}/status/${jobId}`,
        { headers: { Authorization: `Bearer ${apiKey}` } },
      );
      return new Response(await statusResp.text(), {
        status: statusResp.status,
        headers: { "content-type": "application/json", ...cors },
      });
    }

    // ───────────────────────────────────────────────────────────
    // Decide sync vs async based on path
    // ───────────────────────────────────────────────────────────
    const runpodPath = ASYNC_PATHS.some((p) => url.pathname.startsWith(p))
      ? "run"      // returns job_id immediately, client polls
      : "runsync"; // waits up to 30s for result

    // ───────────────────────────────────────────────────────────
    // Build RunPod event payload from HTTP request
    // ───────────────────────────────────────────────────────────
    const headers = {};
    for (const [k, v] of request.headers) {
      // Drop CF-internal headers
      if (k.toLowerCase().startsWith("cf-") || k.toLowerCase() === "host") continue;
      headers[k] = v;
    }

    const query = {};
    for (const [k, v] of url.searchParams) query[k] = v;

    let body = null;
    const ct = request.headers.get("content-type") || "";
    if (["GET", "HEAD"].indexOf(request.method) === -1) {
      if (ct.includes("application/json")) {
        body = await request.json().catch(() => null);
      } else if (ct.startsWith("text/")) {
        body = await request.text();
      } else {
        // Binary or form → base64
        const buf = await request.arrayBuffer();
        body = "b64:" + btoa(String.fromCharCode(...new Uint8Array(buf)));
      }
    }

    const runpodPayload = {
      input: {
        method: request.method,
        path: url.pathname,
        query,
        headers,
        body,
      },
    };

    // ───────────────────────────────────────────────────────────
    // Forward to RunPod
    // ───────────────────────────────────────────────────────────
    let rpResp;
    try {
      rpResp = await fetch(
        `https://api.runpod.ai/v2/${endpointId}/${runpodPath}`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${apiKey}`,
            "content-type": "application/json",
          },
          body: JSON.stringify(runpodPayload),
        },
      );
    } catch (fetchErr) {
      return jsonResponse(
        { error: "runpod_fetch_failed", detail: String(fetchErr).slice(0, 300) },
        { status: 502, cors },
      );
    }

    let rpData;
    try {
      rpData = await rpResp.json();
    } catch (parseErr) {
      return jsonResponse(
        { error: "runpod_response_not_json", status: rpResp.status },
        { status: 502, cors },
      );
    }

    // Propagate RunPod-level errors (429 queue-full, 401 auth, etc.) with CORS
    // headers so the frontend can actually read the status + message.
    if (!rpResp.ok) {
      return jsonResponse(rpData, { status: rpResp.status, cors });
    }

    // Async → return job_id for polling
    if (runpodPath === "run") {
      return jsonResponse(
        {
          job_id: rpData.id,
          status: rpData.status || "IN_QUEUE",
          poll_url: `/status/${rpData.id}`,
        },
        { status: 202, cors },
      );
    }

    // Sync → unwrap RunPod output into native HTTP response
    const output = rpData.output || {};
    const outStatus = output.status_code || 502;
    const outHeaders = output.headers || {};
    const respHeaders = new Headers();
    for (const [k, v] of Object.entries(outHeaders)) {
      // Strip hop-by-hop headers
      if (["content-length", "transfer-encoding", "connection"].indexOf(k.toLowerCase()) !== -1) continue;
      respHeaders.set(k, v);
    }
    // Always layer our CORS headers AFTER copying backend headers so they win.
    // FastAPI's CORSMiddleware also sets these, but when the backend never
    // responded (502) we still need them here.
    for (const [k, v] of Object.entries(cors)) {
      respHeaders.set(k, v);
    }

    // Body can be: `body` (dict/str) or `body_b64` (binary)
    let respBody;
    if (output.body_b64) {
      respBody = Uint8Array.from(atob(output.body_b64), (c) => c.charCodeAt(0));
    } else if (typeof output.body === "string") {
      respBody = output.body;
    } else {
      respBody = JSON.stringify(output.body ?? rpData);
      if (!respHeaders.has("content-type")) respHeaders.set("content-type", "application/json");
    }

    return new Response(respBody, { status: outStatus, headers: respHeaders });
  },
};
