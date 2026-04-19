-- Supabase migration: creative_sessions (shared state across RunPod workers).
-- Run via Supabase dashboard → SQL editor, or supabase CLI migrate.
--
-- Context: Studio's creative session dict was in-memory + per-worker disk.
-- Multi-worker routing or worker rotation (cold boot, template bump, idle
-- timeout) dropped the session, surfacing as "Creative session not found"
-- mid-render. This table makes the session shared state.

create table if not exists public.creative_sessions (
    id text primary key,
    user_id text not null,
    data jsonb not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists creative_sessions_user_id_idx on public.creative_sessions (user_id);
create index if not exists creative_sessions_updated_at_idx on public.creative_sessions (updated_at desc);

alter table public.creative_sessions enable row level security;

-- Only the server (service key) reads/writes these rows. Users never
-- touch the table directly; they go through the Studio API which already
-- authorizes per-user.
drop policy if exists "service role only on creative_sessions" on public.creative_sessions;
-- (no policy = no public access once RLS is on, which is what we want)

-- updated_at trigger mirrors the refund-requests pattern.
create or replace function public.creative_sessions_set_updated_at()
returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

drop trigger if exists creative_sessions_updated_at on public.creative_sessions;
create trigger creative_sessions_updated_at
    before update on public.creative_sessions
    for each row execute function public.creative_sessions_set_updated_at();
