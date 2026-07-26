/**
 * Cloudflare ingress for the canonical NYPTID Studio API.
 *
 * Every request is streamed unchanged to the single Contabo backend. The
 * backend owns routing, CORS, authentication, command execution, queues, and
 * status semantics; this Worker is only the public TLS/ingress pass-through.
 *
 * The sole credential here is STUDIO_ORIGIN_TOKEN, stored as a Cloudflare
 * Worker secret. Caddy requires the corresponding private header on every
 * non-upload origin request and removes it before proxying to the API.
 */

const DEFAULT_CONTABO_STUDIO_ORIGIN = "https://studio.82.197.67.155.sslip.io";
const INGRESS_HEADER = "X-NYPTID-Studio-Ingress";
const INGRESS_IDENTITY = "cloudflare-contabo-v1";
const ORIGIN_AUTH_HEADER = "X-NYPTID-Studio-Origin-Token";
const ORIGIN_AUTH_SCHEME = "v1.";
const ORIGIN_TOKEN_PATTERN = /^[A-Za-z0-9_-]{43,128}$/;

const HOP_BY_HOP_HEADERS = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

const METHODS_WITHOUT_BODY = new Set(["GET", "HEAD"]);

function jsonErrorResponse() {
  return new Response(
    JSON.stringify({
      error: "studio_origin_unavailable",
      message: "Studio backend is temporarily unavailable.",
    }),
    {
      status: 502,
      headers: {
        "content-type": "application/json; charset=utf-8",
        "cache-control": "no-store",
        "access-control-allow-origin": "https://studio.nyptidindustries.com",
        "access-control-allow-credentials": "true",
        "vary": "Origin",
        [INGRESS_HEADER]: INGRESS_IDENTITY,
      },
    },
  );
}

function configuredOrigin(env) {
  const configured = String(
    (env && env.CONTABO_STUDIO_ORIGIN) || DEFAULT_CONTABO_STUDIO_ORIGIN,
  ).trim();
  const origin = new URL(configured);
  if (origin.protocol !== "https:") {
    throw new Error("Studio origin must use HTTPS");
  }
  if (origin.username || origin.password || origin.search || origin.hash) {
    throw new Error("invalid Studio origin");
  }
  if (origin.pathname && origin.pathname !== "/") {
    throw new Error("Studio origin must not contain a path");
  }
  return origin;
}

function configuredOriginAuthorization(env) {
  const token = String((env && env.STUDIO_ORIGIN_TOKEN) || "").trim();
  if (!ORIGIN_TOKEN_PATTERN.test(token)) {
    throw new Error("Studio origin token is missing or invalid");
  }
  return `${ORIGIN_AUTH_SCHEME}${token}`;
}

function upstreamUrlFor(requestUrl, env) {
  const publicUrl = new URL(requestUrl);
  const origin = configuredOrigin(env);
  if (publicUrl.host.toLowerCase() === origin.host.toLowerCase()) {
    throw new Error("Studio origin cannot point back to public ingress");
  }
  publicUrl.protocol = origin.protocol;
  publicUrl.hostname = origin.hostname;
  publicUrl.port = origin.port;
  publicUrl.username = "";
  publicUrl.password = "";
  return publicUrl;
}

function sanitizedForwardHeaders(request, originAuthorization) {
  const headers = new Headers(request.headers);
  const upgrade = String(headers.get("upgrade") || "").trim().toLowerCase();
  const websocket = upgrade === "websocket";
  const rawClientIp = String(headers.get("cf-connecting-ip") || "").trim();
  const clientIp = /^[0-9A-Fa-f:.]{2,64}$/.test(rawClientIp) ? rawClientIp : "";
  const publicUrl = new URL(request.url);

  const connectionTokens = String(headers.get("connection") || "")
    .split(",")
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
  for (const headerName of connectionTokens) {
    headers.delete(headerName);
  }
  for (const headerName of HOP_BY_HOP_HEADERS) {
    headers.delete(headerName);
  }
  for (const headerName of Array.from(headers.keys())) {
    const normalized = headerName.toLowerCase();
    if (normalized === "host" || normalized.startsWith("cf-")) {
      headers.delete(headerName);
    }
  }

  // Never trust client-supplied forwarding metadata. Cloudflare's
  // CF-Connecting-IP is the sole client-IP source at this ingress.
  headers.delete("forwarded");
  headers.delete("x-forwarded-for");
  headers.delete("x-forwarded-host");
  headers.delete("x-forwarded-port");
  headers.delete("x-forwarded-proto");
  headers.delete("x-real-ip");
  headers.delete(ORIGIN_AUTH_HEADER);
  if (clientIp) {
    headers.set("x-forwarded-for", clientIp);
    headers.set("x-real-ip", clientIp);
  }
  headers.set("x-forwarded-host", publicUrl.host);
  headers.set("x-forwarded-proto", publicUrl.protocol.replace(":", ""));
  headers.set(
    "x-forwarded-port",
    publicUrl.port || (publicUrl.protocol === "https:" ? "443" : "80"),
  );
  headers.delete(INGRESS_HEADER);
  headers.set(ORIGIN_AUTH_HEADER, originAuthorization);

  // Upgrade is hop-by-hop for ordinary HTTP, but is required for an upstream
  // WebSocket handshake. Cloudflare manages the Connection header itself.
  if (websocket) {
    headers.set("upgrade", "websocket");
  }
  return headers;
}

function downstreamResponse(upstreamResponse) {
  const headers = new Headers(upstreamResponse.headers);
  headers.set(INGRESS_HEADER, INGRESS_IDENTITY);

  if (upstreamResponse.webSocket) {
    return new Response(null, {
      status: upstreamResponse.status,
      statusText: upstreamResponse.statusText,
      headers,
      webSocket: upstreamResponse.webSocket,
    });
  }

  return new Response(upstreamResponse.body, {
    status: upstreamResponse.status,
    statusText: upstreamResponse.statusText,
    headers,
  });
}

export default {
  async fetch(request, env) {
    try {
      const upstreamUrl = upstreamUrlFor(request.url, env);
      const originAuthorization = configuredOriginAuthorization(env);
      const method = String(request.method || "GET").toUpperCase();
      const init = {
        method,
        headers: sanitizedForwardHeaders(request, originAuthorization),
        redirect: "manual",
        signal: request.signal,
      };
      if (!METHODS_WITHOUT_BODY.has(method)) {
        init.body = request.body;
        // Required by Node's standards-compatible fetch in the provider-free
        // contract test; ignored harmlessly by the Workers runtime.
        init.duplex = "half";
      }

      const upstreamResponse = await fetch(upstreamUrl.toString(), init);
      return downstreamResponse(upstreamResponse);
    } catch {
      return jsonErrorResponse();
    }
  },
};
