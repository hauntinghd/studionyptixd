# Studio RunPod backend: retired

The Studio production backend, durable queue consumer, persistent data, and
ClipLab execution are owned by the Contabo release contract.

The historical RunPod deployment and endpoint-upsert entrypoints in this
directory are retained only as fail-closed tombstones. They exit before reading
credentials, importing an HTTP client, or attempting a network mutation. Do not
restore them as a second production owner.

The Cloudflare Worker files in this directory are not a RunPod deployment.
They remain the canonical streaming ingress layer in front of the
Contabo-hosted API.

Canonical endpoints:

- API: `https://api-studio.nyptidindustries.com`
- Google callback:
  `https://api-studio.nyptidindustries.com/api/oauth/google/youtube/callback`
- PayPal webhook:
  `https://api-studio.nyptidindustries.com/api/paypal/webhook`

Fly is rollback-only. RunPod is not a Studio backend rollback target.
