-- Phase 1.3 migration: persist YouTube OAuth tokens across RunPod worker cycles.
--
-- Problem: pre-2026-04-18 Studio stored YT OAuth tokens at
--   $TEMP_DIR/youtube_connections.json
-- on each RunPod worker's ephemeral filesystem. Every template cycle /
-- worker respawn wiped the token, forcing users to reconnect YouTube.
--
-- Fix: Supabase is the authoritative store; disk file is a per-worker
-- hot-cache that's rebuilt from Supabase on first access after cold boot.

create table if not exists public.youtube_channel_connections (
    user_id             text        not null,
    channel_id          text        not null,
    access_token        text,
    refresh_token       text,
    token_expires_at    double precision,
    token_scope         text,
    oauth_mode          text,
    oauth_source        text,
    linked_at           double precision,
    last_synced_at      double precision,
    is_default          boolean     not null default false,
    extra               jsonb,
    updated_at          timestamptz not null default now(),
    primary key (user_id, channel_id)
);

create index if not exists youtube_channel_connections_user_idx
    on public.youtube_channel_connections (user_id);

-- Only the service role should read/write this table (OAuth tokens are
-- sensitive). No RLS policies for authenticated role — access is via the
-- Studio backend using SUPABASE_SERVICE_KEY only.
alter table public.youtube_channel_connections enable row level security;

-- Explicit service-role bypass policy (service role bypasses RLS by default
-- but this makes the intent explicit if future auditing compares it).
drop policy if exists "yt_channel_connections_service_all" on public.youtube_channel_connections;
create policy "yt_channel_connections_service_all"
    on public.youtube_channel_connections
    for all
    using (auth.role() = 'service_role')
    with check (auth.role() = 'service_role');
