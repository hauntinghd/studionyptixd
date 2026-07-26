import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const source = await readFile(new URL("./worker-proxy.js", import.meta.url), "utf8");
const moduleUrl = `data:text/javascript;base64,${Buffer.from(source).toString("base64")}`;
const worker = (await import(moduleUrl)).default;
const TEST_ORIGIN_TOKEN = "test_only_0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef";
const TEST_ENV = {
  CONTABO_STUDIO_ORIGIN: "https://studio.internal.example",
  STUDIO_ORIGIN_TOKEN: TEST_ORIGIN_TOKEN,
};

async function withFetchStub(stub, operation) {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = stub;
  try {
    return await operation();
  } finally {
    globalThis.fetch = originalFetch;
  }
}

test("streams arbitrary HTTP requests to Contabo with safe forwarding identity", async () => {
  let captured;
  const originResponse = new Response(
    new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("origin-stream"));
        controller.close();
      },
    }),
    {
      status: 207,
      headers: {
        "content-type": "text/plain",
        "x-origin-header": "preserved",
        "x-nyptid-studio-ingress": "spoofed-origin-value",
      },
    },
  );

  const response = await withFetchStub(
    async (url, init) => {
      captured = {
        url,
        init,
        body: init.body ? await new Response(init.body).text() : "",
      };
      return originResponse;
    },
    () => worker.fetch(
      new Request(
        "https://api-studio.nyptidindustries.com/api/studio-agent/run?stream=1",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "cf-connecting-ip": "203.0.113.42",
            "cf-ray": "private-edge-metadata",
            "x-forwarded-for": "198.51.100.99",
            "x-real-ip": "198.51.100.99",
            connection: "keep-alive, x-remove-me",
            "x-remove-me": "hop-value",
            "x-nyptid-studio-origin-token": "v1.client-spoof-must-not-pass",
          },
          body: JSON.stringify({ command: "continue" }),
        },
      ),
      TEST_ENV,
    ),
  );

  assert.equal(
    captured.url,
    "https://studio.internal.example/api/studio-agent/run?stream=1",
  );
  assert.equal(captured.init.method, "POST");
  assert.equal(captured.body, JSON.stringify({ command: "continue" }));
  assert.equal(captured.init.headers.get("host"), null);
  assert.equal(captured.init.headers.get("cf-connecting-ip"), null);
  assert.equal(captured.init.headers.get("cf-ray"), null);
  assert.equal(captured.init.headers.get("connection"), null);
  assert.equal(captured.init.headers.get("x-remove-me"), null);
  assert.equal(captured.init.headers.get("x-forwarded-for"), "203.0.113.42");
  assert.equal(captured.init.headers.get("x-real-ip"), "203.0.113.42");
  assert.equal(
    captured.init.headers.get("x-forwarded-host"),
    "api-studio.nyptidindustries.com",
  );
  assert.equal(captured.init.headers.get("x-forwarded-proto"), "https");
  assert.equal(captured.init.headers.get("x-forwarded-port"), "443");
  assert.equal(
    captured.init.headers.get("x-nyptid-studio-origin-token"),
    `v1.${TEST_ORIGIN_TOKEN}`,
  );

  assert.equal(response.status, 207);
  assert.equal(response.headers.get("x-origin-header"), "preserved");
  assert.equal(
    response.headers.get("x-nyptid-studio-ingress"),
    "cloudflare-contabo-v1",
  );
  assert.ok(response.body instanceof ReadableStream);
  assert.equal(await response.text(), "origin-stream");
});

test("passes OPTIONS and WebSocket upgrades upstream instead of routing locally", async () => {
  const seen = [];
  await withFetchStub(
    async (url, init) => {
      seen.push({ url, method: init.method, headers: init.headers });
      return new Response(null, { status: 204 });
    },
    async () => {
      await worker.fetch(
        new Request("https://api-studio.nyptidindustries.com/any/preflight", {
          method: "OPTIONS",
        }),
        {
          STUDIO_ORIGIN_TOKEN: TEST_ORIGIN_TOKEN,
        },
      );
      await worker.fetch(
        new Request("https://api-studio.nyptidindustries.com/socket/live", {
          headers: {
            connection: "Upgrade",
            upgrade: "websocket",
            "cf-connecting-ip": "2001:db8::1",
          },
        }),
        {
          STUDIO_ORIGIN_TOKEN: TEST_ORIGIN_TOKEN,
        },
      );
    },
  );

  assert.equal(seen[0].method, "OPTIONS");
  assert.equal(
    seen[0].url,
    "https://studio.82.197.67.155.sslip.io/any/preflight",
  );
  assert.equal(seen[1].method, "GET");
  assert.equal(seen[1].headers.get("connection"), null);
  assert.equal(seen[1].headers.get("upgrade"), "websocket");
  assert.equal(seen[1].headers.get("x-forwarded-for"), "2001:db8::1");
});

test("returns safe noncached JSON when the configured origin fails", async () => {
  const response = await withFetchStub(
    async () => {
      throw new Error("secret upstream detail must never escape");
    },
    () => worker.fetch(
      new Request("https://api-studio.nyptidindustries.com/api/health"),
      TEST_ENV,
    ),
  );

  assert.equal(response.status, 502);
  assert.equal(response.headers.get("cache-control"), "no-store");
  assert.equal(
    response.headers.get("access-control-allow-origin"),
    "https://studio.nyptidindustries.com",
  );
  assert.equal(
    response.headers.get("x-nyptid-studio-ingress"),
    "cloudflare-contabo-v1",
  );
  const payload = await response.json();
  assert.deepEqual(payload, {
    error: "studio_origin_unavailable",
    message: "Studio backend is temporarily unavailable.",
  });
  assert.doesNotMatch(JSON.stringify(payload), /secret|internal\.example/i);
});

test("fails closed when an origin override is not HTTPS", async () => {
  const response = await worker.fetch(
    new Request("https://api-studio.nyptidindustries.com/api/health"),
    {
      CONTABO_STUDIO_ORIGIN: "http://82.197.67.155:10000",
      STUDIO_ORIGIN_TOKEN: TEST_ORIGIN_TOKEN,
    },
  );

  assert.equal(response.status, 502);
  assert.equal(response.headers.get("cache-control"), "no-store");
});

test("fails closed without a strong origin token and never reaches the origin", async () => {
  for (const token of [undefined, "", "too-short", "contains spaces but is long enough"]) {
    let fetchCalled = false;
    const response = await withFetchStub(
      async () => {
        fetchCalled = true;
        return new Response("must not happen");
      },
      () => worker.fetch(
        new Request("https://api-studio.nyptidindustries.com/api/health", {
          headers: {
            "x-nyptid-studio-origin-token": `v1.${TEST_ORIGIN_TOKEN}`,
          },
        }),
        {
          CONTABO_STUDIO_ORIGIN: "https://studio.internal.example",
          STUDIO_ORIGIN_TOKEN: token,
        },
      ),
    );

    assert.equal(response.status, 502);
    assert.equal(fetchCalled, false);
    assert.equal(response.headers.get("cache-control"), "no-store");
  }
});
