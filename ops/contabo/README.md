# NYPTID Studio on Contabo

This stack runs the complete Studio backend as one API process with one
embedded production consumer, one persistent local Redis, and one persistent
`/var/data` bind mount. It is designed to join the network of the Caddy
container already serving ClipLab without replacing or restarting ClipLab.

## Ownership contract

- `studio-api` is the only API service and has `WEB_CONCURRENCY=1`.
- That same process is the only production consumer:
  `RUN_EMBEDDED_WORKER=true`, `JOB_QUEUE_WORKERS=1`.
- Redis uses AOF (`appendfsync everysec`), periodic RDB snapshots, and
  `maxmemory-policy noeviction`.
- Redis has no host port. The API has only a loopback diagnostic port; public
  traffic reaches it through the external Caddy network.
- Every API health check must match the full Git SHA and frontend build ID
  baked into the image.
- The first activation is impossible without fresh, raw Fly JSON evidence:
  before/after stopped-machine status, application config, repeated failed
  origin requests, repeated zero Redis queue/inflight/lease samples, and a
  zero resumable-file-workflow inventory. The fence hashes those exact files,
  binds the final data manifest, and re-runs the verifier immediately before
  the stopped candidate container is started.

## Host layout

```text
/opt/studio/
  backups/                 mode 0700
  current -> release root
  data/                    mounted at /var/data
  redis/                   Redis AOF/RDB
  releases/                immutable source checkouts
  shared/
    active.env -> candidates/<active>.env
    previous.env -> candidates/<rollback>.env
    staged.env -> candidates/<staged>.env
    base.env                generated Redis password/runtime paths, mode 0600
    studio.env              provider and billing secrets, mode 0600
    caddy.env               Worker-origin token for Caddy only, mode 0600
    data-manifests/         final Fly source manifests
    data-ready.attestation  verified destination count/bytes/hash, mode 0600
    fly-evidence/            raw, root-owned Fly status/config/probe evidence
    fences/                 one-use initial-cutover attestations
    reverse-manifests/      stopped Contabo source manifests for rollback
    queue-drained.attestation
    legacy-start.ready      exists only after a verified reverse sync
```

Keep each source checkout under `/opt/studio/releases/<full-git-sha>` and
read-only after staging. The deploy script refuses a dirty checkout.

## One-time preparation

Docker Engine, Docker Compose, `rsync`, `curl`, `python3`, and `git` must be
installed. The existing Caddy Docker network must already exist; its current
name is `deploy_default`.

From the exact release checkout:

```bash
sudo CADDY_NETWORK=deploy_default \
  bash ops/contabo/prepare_host.sh --install-systemd-timers
sudoedit /opt/studio/shared/studio.env
sudo chmod 600 /opt/studio/shared/studio.env
```

Copy secret values from the current production secret store without printing
them into terminal logs. Do not put `REDIS_URL`, worker counts, provider policy,
or filesystem paths in `studio.env`; Compose owns those invariants. Confirm the
Google redirect URI and payment webhooks are registered for
`https://api-studio.nyptidindustries.com` before public cutover.

`prepare_host.sh` does not alter Caddy, ClipLab, DNS, or start Studio.

The VPS has finite shared memory. The current Studio default is 4 GB plus
768 MB for Redis; do not activate it beside a 10 GB ClipLab ceiling on an
11 GiB host. Lower ClipLab's ceiling in its own reviewed deployment, leave
headroom for Caddy/the kernel, and configure swap before production cutover.
This stack deliberately does not mutate ClipLab's Compose project.

## Caddy and private Worker origin

The canonical hostname terminates at Cloudflare. Its Worker streams to
`https://studio.82.197.67.155.sslip.io` using a private
`X-NYPTID-Studio-Origin-Token` header. The final `Caddyfile.studio` accepts all
routes from that authenticated Worker, removes the private header before the
API sees it, and permits unauthenticated direct-origin traffic only for
`POST`/`OPTIONS` on these four large-upload routes:

- `/api/cliplab/ingest/upload`
- `/api/catalyst/hub/reference-video-analysis/manual`
- `/api/studio-agent/sessions/*/attachments/video`
- `/api/thumbnails/upload-video`

Every other direct-origin path or method receives a noncached HTTP 403. CORS
and bearer authorization remain backend-owned. Both the bootstrap and final
Caddy blocks delete the private header from upstream requests and access logs.
Do not add
`api-studio.nyptidindustries.com` to Caddy while its custom domain still points
to the Worker.

The Caddy container and `studio-api` must both be attached to the external
network named by `CADDY_NETWORK`. The snippet permits 30 GB request bodies,
keeps SSE/token streams unbuffered, and supports WebSockets through Caddy's
native reverse proxy handling.

Generate the initial token on the VPS without writing it to stdout:

```bash
sudo install -d -m 0700 /opt/studio/shared
sudo python3 - <<'PY'
import os
import secrets

path = "/opt/studio/shared/caddy.env"
flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
fd = os.open(path, flags, 0o600)
with os.fdopen(fd, "w", encoding="utf-8") as handle:
    handle.write(f"STUDIO_ORIGIN_TOKEN={secrets.token_urlsafe(48)}\n")
    handle.write("STUDIO_ORIGIN_TOKEN_PREVIOUS=\n")
PY
```

Apply `caddy-compose.override.yml` to the existing ClipLab/Caddy Compose
project so only Caddy receives the secret. Never add this value to
`studio.env`, the Studio API Compose service, Wrangler vars, or Git:

```bash
cd /opt/cliplab/deploy
docker compose \
  --env-file /opt/studio/shared/caddy.env \
  -f docker-compose.yml \
  -f /opt/studio/current/ops/contabo/caddy-compose.override.yml \
  config >/dev/null
docker compose \
  --env-file /opt/studio/shared/caddy.env \
  -f docker-compose.yml \
  -f /opt/studio/current/ops/contabo/caddy-compose.override.yml \
  up -d --no-deps caddy
```

Use this ordering to avoid a Worker/Caddy token-mismatch outage:

1. Merge `Caddyfile.studio.bootstrap` as the Studio site block without
   replacing the ClipLab site. Validate and reload Caddy. This temporary block
   preserves the existing open origin while stripping the new header from API
   requests.
2. From the release checkout on the authenticated operator workstation, pipe
   the VPS token directly into Wrangler and deploy the Worker. The value is
   consumed from the pipe and is not printed:

   ```powershell
   ssh.exe -F C:\Users\casey\.ssh\cliplab_vps_config cliplab-vps `
     "sed -n 's/^STUDIO_ORIGIN_TOKEN=//p' /opt/studio/shared/caddy.env" |
     npx wrangler secret put STUDIO_ORIGIN_TOKEN `
       --config runpod-serverless/wrangler.toml
   npx wrangler deploy --config runpod-serverless/wrangler.toml
   curl.exe --fail --silent --show-error `
     https://api-studio.nyptidindustries.com/api/health > $null
   ```

3. Replace only the temporary Studio site block with `Caddyfile.studio`, then
   validate and reload Caddy:

   ```bash
   docker exec cliplab-caddy caddy validate \
     --config /etc/caddy/Caddyfile --adapter caddyfile
   docker exec cliplab-caddy caddy reload \
     --config /etc/caddy/Caddyfile --adapter caddyfile
   ```

4. Prove the boundary without browser control:

   ```bash
   test "$(curl --silent --output /dev/null --write-out '%{http_code}' \
     https://studio.82.197.67.155.sslip.io/api/health)" = 403
   test "$(curl --silent --output /dev/null --write-out '%{http_code}' \
     --header 'X-NYPTID-Studio-Origin-Token: v1.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' \
     https://studio.82.197.67.155.sslip.io/api/health)" = 403
   test "$(curl --silent --output /dev/null --write-out '%{http_code}' \
     https://studio.82.197.67.155.sslip.io/api/cliplab/ingest/upload)" = 403
   for path in \
     /api/cliplab/ingest/upload \
     /api/catalyst/hub/reference-video-analysis/manual \
     /api/studio-agent/sessions/origin-boundary-check/attachments/video \
     /api/thumbnails/upload-video
   do
     code="$(curl --silent --output /dev/null --write-out '%{http_code}' \
       --request OPTIONS \
       --header 'Origin: https://studio.nyptidindustries.com' \
       --header 'Access-Control-Request-Method: POST' \
       "https://studio.82.197.67.155.sslip.io${path}")"
     test "${code}" = 200 || test "${code}" = 204
   done
   curl --fail --silent --show-error \
     https://api-studio.nyptidindustries.com/api/health >/dev/null
   ```

The optional `STUDIO_ORIGIN_TOKEN_PREVIOUS` matcher exists only for
zero-downtime rotation: load Caddy with `current=new, previous=old`, switch the
Wrangler secret to `new`, verify canonical health, then recreate Caddy with the
previous value empty. Both token slots enforce the same base64url length
contract, so an absent environment value cannot accidentally authorize a
request.

## Stage without starting a consumer

Run release tests before copying the clean checkout to the VPS. On the VPS:

```bash
cd /opt/studio/releases/<full-git-sha>
sudo bash ops/contabo/prepare_host.sh
sudo bash ops/contabo/deploy.sh stage \
  --build-id <studio-build-id> \
  --image-ref docker.io/nyptid/nyptid-studio-api@sha256:<64hex-ci-digest>
```

The normal release path pulls the exact manifest digest emitted by
`publish-api-image.yml`, verifies its Git/build labels against the checked-out
release, binds it to a local immutable image ID, writes a mode-0600 candidate
descriptor, validates the resolved Compose model, and prepares Redis. It
intentionally does **not** start `studio-api`. Omitting `--image-ref` performs
an explicit local source build and is reserved for operator-controlled recovery,
not the production CI release path.

## Data and queue cutover

The commands below are intentionally split by execution context. Use a Linux
or WSL shell on the operator workstation; do not translate the binary/data
transfer through a PowerShell object pipeline.

### 1. Operator workstation: establish variables and pre-drain

```bash
export STUDIO_APP=nyptid-studio
export FLY_MACHINE_ID='<machine-id>'
export LEGACY_ORIGIN=https://nyptid-studio.fly.dev
export VPS_SSH=cliplab-vps
export RELEASE_SHA='<full-git-sha>'
export CUTOVER_ID="$(date -u +%Y%m%dT%H%M%SZ)-${FLY_MACHINE_ID}"
export LOCAL_CUTOVER_ROOT="$(mktemp -d -t studio-forward.XXXXXXXX)"

fly ssh sftp put ops/contabo/legacy_queue_probe.py /tmp/legacy_queue_probe.py \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --mode 0700
fly ssh sftp put ops/contabo/file_quiescence.py /tmp/file_quiescence.py \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --mode 0700
fly ssh console --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --command \
  "python /tmp/legacy_queue_probe.py --app $STUDIO_APP \
  --machine-id $FLY_MACHINE_ID --samples 3 --interval 2 --require-drained"
fly ssh console --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --command \
  "python /tmp/file_quiescence.py \
  --sessions-dir /var/data/studio_agent_sessions --require-drained"
```

Do not proceed unless both commands exit zero. This is a precheck, not the
cutover proof: traffic can still arrive.

### 2. Operator workstation: remove admission and enter copy-only mode

Save the current raw config for rollback, then update the existing
volume-attached machine with the reviewed profile that defines no public
service or health check:

```bash
fly machine list --app "$STUDIO_APP" --json \
  >"$LOCAL_CUTOVER_ROOT/machine-before-copy-only.json"
fly machine update "$FLY_MACHINE_ID" \
  --app "$STUDIO_APP" \
  --config ops/contabo/fly-copy-only.toml \
  --command "sleep infinity" \
  --autostart=false \
  --restart=no \
  --skip-health-checks \
  --yes
```

`--skip-start` is absent so SSH can still reach `/var/data`. If Fly preserves a
service, command, entrypoint, health check, restart policy, or mount that does
not satisfy `fly_cutover_evidence.py`, the later fence fails closed.

The machine update recreates the ephemeral root filesystem. Re-upload the
read-only queue probe before taking the post-block proof:

```bash
fly ssh sftp put ops/contabo/legacy_queue_probe.py /tmp/legacy_queue_probe.py \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --mode 0700
```

### 3. Operator workstation: post-block drain and exact final sync

This post-block proof closes the admission-to-copy-only race. A request that
entered between the precheck and machine update either finished before the
final manifest or leaves a queue, lease, command, job, or resumable workflow
that makes these commands fail.

```bash
fly ssh console --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --command \
  "python /tmp/legacy_queue_probe.py --app $STUDIO_APP \
  --machine-id $FLY_MACHINE_ID --samples 3 --interval 2 --require-drained"

fly ssh sftp put ops/contabo/data_manifest.py /tmp/data_manifest.py \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --mode 0700
fly ssh console --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --command \
  "python /tmp/data_manifest.py create \
  --data-dir /var/data --manifest /tmp/fly-final.manifest"
```

Use a one-cutover SSH key authorized only for `/opt/studio/data`, plus a pinned
Contabo host-key file. The VPS `authorized_keys` entry must use OpenSSH's
`restrict` option and Debian's `rrsync` wrapper; a plain root key is forbidden:

```text
restrict,command="/usr/bin/rrsync -wo /opt/studio/data" ssh-ed25519 <cutover-public-key> <cutover-comment>
```

`rrsync` changes into `/opt/studio/data`, so the client destination below is
the restricted root (`:/`), not the host's literal filesystem path. Install
`rsync` and OpenSSH in the copy-only machine's ephemeral root, upload the
private key and pinned host file, and run the final sync directly from the
mounted Fly volume. This is intentionally a second, deletion-capable pass
after the earlier seed, so it transfers only the final delta:

```bash
fly ssh console --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --command \
  "apt-get update && apt-get install -y --no-install-recommends openssh-client rsync"
fly ssh sftp put '<cutover-private-key>' /tmp/studio-migration-key \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --mode 0600
fly ssh sftp put '<pinned-known-hosts>' /tmp/contabo-known-hosts \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --mode 0600
fly ssh console --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --command \
  "rsync --archive --delete --partial --human-readable --info=progress2 \
  -e 'ssh -i /tmp/studio-migration-key \
  -o UserKnownHostsFile=/tmp/contabo-known-hosts \
  -o StrictHostKeyChecking=yes -o IdentitiesOnly=yes' \
  /var/data/ root@82.197.67.155:/"

fly ssh sftp get /tmp/fly-final.manifest \
  "$LOCAL_CUTOVER_ROOT/fly-final.manifest" \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP"
scp "$LOCAL_CUTOVER_ROOT/fly-final.manifest" \
  "$VPS_SSH:/opt/studio/shared/data-manifests/fly-final.manifest"
ssh "$VPS_SSH" \
  "chmod 600 /opt/studio/shared/data-manifests/fly-final.manifest && \
  python /opt/studio/releases/$RELEASE_SHA/ops/contabo/data_manifest.py attest \
  --data-dir /opt/studio/data \
  --manifest /opt/studio/shared/data-manifests/fly-final.manifest \
  --attestation /opt/studio/shared/data-ready.attestation \
  --role migrated-data-ready --minimum-files 1 --minimum-bytes 1"
```

The root-only destination attestation hashes `/opt/studio/data` against Fly's
exact manifest and rejects a partial transfer, extra files, changed bytes, or
an extra directory layer. Do not stop Fly yet. Refresh both quiescence proofs
*after* the sync and attestation so the 30-minute evidence clock cannot expire
during a large transfer:

```bash
fly ssh sftp put ops/contabo/file_quiescence.py /tmp/file_quiescence.py \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --mode 0700
fly ssh console --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --command \
  "python /tmp/legacy_queue_probe.py --app $STUDIO_APP \
  --machine-id $FLY_MACHINE_ID --samples 3 --interval 2 \
  --output /tmp/legacy-queue.json --require-drained"
fly ssh console --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --command \
  "python /tmp/file_quiescence.py \
  --sessions-dir /var/data/studio_agent_sessions \
  --app $STUDIO_APP --machine-id $FLY_MACHINE_ID \
  --output /tmp/file-quiescence.json --require-drained"
fly ssh sftp get /tmp/legacy-queue.json \
  "$LOCAL_CUTOVER_ROOT/legacy-queue.json" \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP"
fly ssh sftp get /tmp/file-quiescence.json \
  "$LOCAL_CUTOVER_ROOT/file-quiescence.json" \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP"
```

Remove the cutover key from Fly and its matching authorization from Contabo
immediately after the stopped-machine evidence is safely stored.

### 4. Operator workstation: stop, probe, and capture raw evidence

```bash
fly machine stop "$FLY_MACHINE_ID" --app "$STUDIO_APP" --timeout 120
bash ops/contabo/capture_fly_cutover_evidence.sh \
  --app "$STUDIO_APP" \
  --machine-id "$FLY_MACHINE_ID" \
  --origin "$LEGACY_ORIGIN" \
  --queue-evidence "$LOCAL_CUTOVER_ROOT/legacy-queue.json" \
  --file-evidence "$LOCAL_CUTOVER_ROOT/file-quiescence.json" \
  --output-dir "$LOCAL_CUTOVER_ROOT/fly-evidence"

ssh "$VPS_SSH" \
  "install -d -m 700 /opt/studio/shared/fly-evidence/$CUTOVER_ID"
rsync --archive --delete -- \
  "$LOCAL_CUTOVER_ROOT/fly-evidence/" \
  "$VPS_SSH:/opt/studio/shared/fly-evidence/$CUTOVER_ID/"
ssh "$VPS_SSH" \
  "chown -R root:root /opt/studio/shared/fly-evidence/$CUTOVER_ID && \
  chmod 700 /opt/studio/shared/fly-evidence/$CUTOVER_ID && \
  chmod 600 /opt/studio/shared/fly-evidence/$CUTOVER_ID/*.json"
```

The capture performs three failed public-origin requests between two raw
`fly machine list --json` captures. It extracts the expected machine from each
list into the before/after status documents. Both lists must contain exactly
the one expected stopped machine, and both extracted status documents must
contain the exact inert command, no services/checks, restart `no`, and
`/var/data`.

If Fly billing prevents the reviewed copy-only machine update, the verifier
also accepts Fly's cordon as a narrow fail-closed fallback. All four raw
machine snapshots must independently report the retained machine as stopped,
and the uniquely newest timestamped event in every snapshot must be exactly a
user-sourced `cordon` with `stopped` status. The failed origin probes, drained
queue, file quiescence, single-machine identity, and `/var/data` checks remain
mandatory. A missing, superseded, malformed, or mixed cordon proof fails.
Only this unanimous `cordoned_stopped` evidence mode permits the unchanged app
config to retain services/autostart; normal `copy_only` evidence still requires
the strict service and autostart constraints. The verifier summary emits the
selected `EVIDENCE_MODE`.

### 5. VPS: issue and consume the hash-bound fence

```bash
cd "/opt/studio/releases/$RELEASE_SHA"
sudo bash ops/contabo/write_cutover_fence.sh \
  --legacy-app "$STUDIO_APP" \
  --legacy-machine-id "$FLY_MACHINE_ID" \
  --legacy-origin "$LEGACY_ORIGIN" \
  --evidence-dir "/opt/studio/shared/fly-evidence/$CUTOVER_ID" \
  --data-attestation /opt/studio/shared/data-ready.attestation
```

The fence expires after 30 minutes. `deploy.sh activate` rehashes the data
attestation and reruns the raw-evidence verifier immediately before it starts
the already-created, stopped, immutable-ID-verified container.

Activate the staged candidate with the fence path printed by that command:

```bash
sudo bash ops/contabo/deploy.sh activate \
  --fence /opt/studio/shared/fences/<candidate>.fence
```

Activation starts exactly one API/consumer and refuses success until health
proves:

- the full expected Git SHA and frontend build ID;
- Redis queue mode;
- exactly one ready/running embedded worker;
- FAL-only Studio image routing;
- xAI and unverified RunPod production routes disabled;
- exact frontend-origin CORS.

If first activation fails, the Contabo API is stopped again. If a later
candidate fails, the script automatically restores the previously active
Contabo image.

Normal activation is monotonic: a different candidate must have a strictly
newer UTC build timestamp than the active candidate. Intentional downgrade to
an older verified image is available only through `rollback.sh`.

Before canonical DNS cutover, verify the Caddy/TLS path without browser control:

```bash
sudo bash ops/contabo/smoke.sh \
  --url https://studio.82.197.67.155.sslip.io \
  --expected-sha <full-git-sha> \
  --expected-build <studio-build-id> \
  --origin-token-file /opt/studio/shared/caddy.env \
  --attempts 10
```

The curl credential is written to a mode-0600 temporary config, so it does not
appear in the process list. After the Worker and final Caddy policy are active,
run the same smoke against `https://api-studio.nyptidindustries.com` without
`--origin-token-file`.

## Smoke tests

Local, including the one-container assertion:

```bash
sudo bash /opt/studio/current/ops/contabo/smoke.sh \
  --candidate /opt/studio/shared/active.env \
  --attempts 10 \
  --check-container-count
```

Public TLS:

```bash
bash ops/contabo/smoke.sh \
  --url https://api-studio.nyptidindustries.com \
  --expected-sha <full-git-sha> \
  --expected-build <studio-build-id> \
  --attempts 10
```

## Rollback

Replace the active Contabo candidate with the previous verified image:

```bash
sudo bash /opt/studio/current/ops/contabo/rollback.sh --to-previous
```

Returning to Fly is a two-phase, fail-closed data migration. First block new
Contabo mutations and prepare a stopped reverse-sync source:

```bash
sudo bash /opt/studio/current/ops/contabo/rollback.sh \
  --stop-for-legacy \
  --confirm prepare-legacy-after-ingress-is-blocked
```

This refuses any backend queue, in-flight job, admission receipt, Studio Agent
lease/waiter, or production-slot lease. It disables the watchdog, stops the
only Contabo API/consumer, rechecks every Redis count at zero, and creates:

- `/opt/studio/shared/queue-drained.attestation`;
- a stopped-data source manifest in `reverse-manifests/`.

It does **not** authorize or start Fly. Keep Fly in benign copy-only mode,
and start only that benign maintenance command so SSH can reach the attached
volume. Re-run the same process/health proof before writing any data:

```bash
fly machine start <machine-id> --app nyptid-studio
fly machine status <machine-id> --app nyptid-studio --display-config
fly ssh console --machine <machine-id> --app nyptid-studio --command \
  "sh -lc 'test -d /var/data; \
  ! pgrep -af \"[u]vicorn|backend:[a]pp|backend_[w]orker.py\"; \
  ! python -c \"import urllib.request; urllib.request.urlopen(\\\"http://127.0.0.1:10000/api/health\\\", timeout=2)\" 2>/dev/null'"
```

Reverse-sync the stopped Contabo tree into Fly `/var/data` with deletion
semantics. Run these from the Linux/WSL operator workstation:

```bash
export VPS_SSH=cliplab-vps
export STUDIO_APP=nyptid-studio
export FLY_MACHINE_ID='<machine-id>'
export REVERSE_MANIFEST='<path printed by rollback.sh --stop-for-legacy>'
export LOCAL_REVERSE_ROOT="$(mktemp -d -t studio-reverse.XXXXXXXX)"
mkdir -p "$LOCAL_REVERSE_ROOT/data"

rsync --archive --delete --human-readable --info=progress2 -- \
  "$VPS_SSH:/opt/studio/data/" "$LOCAL_REVERSE_ROOT/data/"
scp "$VPS_SSH:$REVERSE_MANIFEST" "$LOCAL_REVERSE_ROOT/contabo-reverse.manifest"

fly machine start "$FLY_MACHINE_ID" --app "$STUDIO_APP"
fly ssh sftp put ops/contabo/data_manifest.py /tmp/data_manifest.py \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --mode 0700
fly ssh sftp put "$LOCAL_REVERSE_ROOT/contabo-reverse.manifest" \
  /tmp/contabo-reverse.manifest \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --mode 0600
fly ssh console --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --command \
  "python /tmp/data_manifest.py clear-reverse-destination \
  --data-dir /var/data \
  --confirm replace-reverse-destination-from-verified-staging"
fly ssh sftp put -R "$LOCAL_REVERSE_ROOT/data" /var \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP"
fly ssh console --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP" --command \
  "python /tmp/data_manifest.py attest \
  --data-dir /var/data \
  --manifest /tmp/contabo-reverse.manifest \
  --attestation /tmp/fly-reverse-data.attestation \
  --role reverse-destination --minimum-files 1 --minimum-bytes 1"
fly ssh sftp get /tmp/fly-reverse-data.attestation \
  "$LOCAL_REVERSE_ROOT/fly-reverse-data.attestation" \
  --machine "$FLY_MACHINE_ID" --app "$STUDIO_APP"
scp "$LOCAL_REVERSE_ROOT/fly-reverse-data.attestation" \
  "$VPS_SSH:/opt/studio/shared/fly-reverse-data.attestation"
```

`clear-reverse-destination` refuses any target other than a real mounted
`/var/data` and requires the exact confirmation. Uploading `data` to `/var`
places its contents directly under `/var/data`; the following attestation
rejects missing, extra, nested, or content-mismatched files.

Transfer that attestation back and install it root-owned/mode 0600. Then issue
the only legacy-start authorization:

```bash
sudo chown root:root /opt/studio/shared/fly-reverse-data.attestation
sudo chmod 600 /opt/studio/shared/fly-reverse-data.attestation
sudo bash /opt/studio/current/ops/contabo/rollback.sh \
  --authorize-legacy \
  --reverse-data-attestation /opt/studio/shared/fly-reverse-data.attestation \
  --confirm reverse-data-sync-is-verified-and-fly-is-still-copy-only
```

Authorization fails unless the Contabo API count remains zero, all live Redis
queue/in-flight/lease counts remain zero, the queue attestation is unchanged,
the stopped Contabo tree still rehashes to its reverse-source manifest, and Fly
attests that exact same full-file manifest hash/count/bytes. Only after
`/opt/studio/shared/legacy-start.ready` exists may an operator deliberately
restore the saved Fly API command/config and change traffic.

There is no force flag. An emergency rollback that starts Fly with stale data,
skips reverse sync, ignores queued/in-flight work, or accepts data loss is
unsupported.

Returning from Fly to Contabo requires putting Fly back into copy-only mode,
performing and attesting a fresh forward sync, stopping/request-testing Fly,
writing a new fence, and activating Contabo.

## Backups

The daily timer runs `backup.sh`, which:

- makes a two-pass snapshot of `/opt/studio/data`;
- runs synchronous Redis `SAVE`, copies only one `dump.rdb`, and requires its
  copied SHA-256 to match the source SHA-256;
- records release provenance without copying provider secrets;
- writes a SHA-256 sidecar;
- retains the newest `BACKUP_RETENTION_COUNT` archives (default seven).

List and verify backups:

```bash
sudo systemctl list-timers studio-backup.timer
sudo systemctl start studio-backup.service
cd /opt/studio/backups
sudo sha256sum -c studio-<timestamp>.tar.gz.sha256
```

Copy verified archives off the VPS. The local retention set is not protection
against full-host loss. Restoration should be rehearsed on an isolated Docker
project: verify the checksum, extract `data/` and `redis/dump.rdb` with the API
and Redis stopped, preserve the current directories as a rollback copy, restore
ownership (`root` for data and UID/GID 999 for Redis), then run the same
provenance smoke before admitting traffic.

The backup intentionally excludes the live multipart AOF directory because its
segments can rotate during a recursive copy. `dump.rdb` is internally
consistent, but its SAVE instant is not transactionally aligned with the
two-pass `/var/data` snapshot. A restored RDB can contain queue/admission/lease
state from a different instant; restore only in an isolated recovery project,
inspect/reconcile queued and in-flight billable work, and never enable the
consumer merely because the RDB loaded.

The minute watchdog restarts only the recorded single API owner when Docker
reports it unhealthy. It takes the same lifecycle lock as deploy/backup and
does nothing while `consumer.disabled` exists, so it cannot undo a deliberate
legacy rollback or race a release.

## Operational checks

```bash
docker compose \
  --project-name nyptid-studio \
  --env-file /opt/studio/shared/active.env \
  --file /opt/studio/current/ops/contabo/docker-compose.yml \
  ps

docker logs --since 30m nyptid-studio-api
docker logs --since 30m nyptid-studio-redis
curl -fsS -H 'Host: api-studio.nyptidindustries.com' \
  http://127.0.0.1:10000/api/health
```

Keep Fly and its volume intact as a rollback target until several Contabo
backups have been verified off-host. Do not run both platform consumers at the
same time.
