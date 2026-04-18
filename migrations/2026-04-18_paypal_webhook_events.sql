-- Phase 3 migration: persist PayPal webhook event IDs for idempotency.
--
-- Why: PayPal retries webhooks up to 25 times over 3 days when our endpoint
-- returns 5xx or times out. The existing in-memory / on-disk dedup lived in
-- each RunPod worker's ephemeral filesystem. A template cycle between
-- retries = duplicate event application = double-credit / double-cancel.
--
-- This table is the authoritative idempotency store. Webhook handler reads
-- before processing; upserts on success. Local file-cache remains as a
-- per-worker hot layer.

create table if not exists public.paypal_webhook_events (
    event_id        text        primary key,
    event_type      text,
    payload_excerpt jsonb,
    processed_at    timestamptz not null default now()
);

create index if not exists paypal_webhook_events_processed_at_idx
    on public.paypal_webhook_events (processed_at);

alter table public.paypal_webhook_events enable row level security;

drop policy if exists "paypal_webhook_events_service_all" on public.paypal_webhook_events;
create policy "paypal_webhook_events_service_all"
    on public.paypal_webhook_events
    for all
    using (auth.role() = 'service_role')
    with check (auth.role() = 'service_role');
