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

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const apiKey = env.RUNPOD_API_KEY;
    const endpointId = env.RUNPOD_ENDPOINT_ID;

    if (!apiKey || !endpointId) {
      return new Response(JSON.stringify({ error: "RunPod not configured" }), {
        status: 500, headers: { "content-type": "application/json" },
      });
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
        headers: { "content-type": "application/json" },
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
    const rpResp = await fetch(
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

    const rpData = await rpResp.json();

    // Async → return job_id for polling
    if (runpodPath === "run") {
      return new Response(JSON.stringify({
        job_id: rpData.id,
        status: rpData.status || "IN_QUEUE",
        poll_url: `/status/${rpData.id}`,
      }), {
        status: 202,
        headers: { "content-type": "application/json" },
      });
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
