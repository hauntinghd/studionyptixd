-- NYPTID Studio Supabase security repair.
--
-- Purpose:
-- 1. Apply the Studio training-capture tables that the app now expects.
-- 2. Fix the Supabase Advisor RLS findings visible for backend-owned tables.
--
-- Run this in Supabase Dashboard -> SQL Editor for project qdwzilgqvpegekxrrnnn,
-- or through psql with a valid project database password.

begin;

-- ---------------------------------------------------------------------------
-- Training capture migration
-- ---------------------------------------------------------------------------

create table if not exists public.training_consents (
    user_id uuid primary key references auth.users(id) on delete cascade,
    training_opt_in boolean not null default false,
    human_review_opt_in boolean not null default false,
    include_prompts boolean not null default false,
    include_uploads boolean not null default false,
    include_outputs boolean not null default false,
    include_feedback boolean not null default false,
    consent_version text not null,
    consented_at double precision,
    revoked_at double precision,
    updated_at double precision not null
);

alter table public.training_consents enable row level security;

drop policy if exists training_consents_owner_read on public.training_consents;
create policy training_consents_owner_read on public.training_consents
    for select using (auth.uid() = user_id);

drop policy if exists training_consents_service_write on public.training_consents;
create policy training_consents_service_write on public.training_consents
    for all using (auth.role() = 'service_role')
    with check (auth.role() = 'service_role');

create table if not exists public.training_event_outbox (
    event_id text primary key,
    user_id uuid not null references auth.users(id) on delete cascade,
    session_id text,
    turn_id text,
    event_type text not null,
    consent_version text not null,
    trainable boolean not null default false,
    youtube_authorized_data boolean not null default false,
    payload jsonb not null default '{}'::jsonb,
    lineage jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    compiled_at timestamptz
);

create index if not exists training_event_outbox_uncompiled_idx
    on public.training_event_outbox (created_at)
    where compiled_at is null;

alter table public.training_event_outbox enable row level security;

drop policy if exists training_event_outbox_service_only on public.training_event_outbox;
create policy training_event_outbox_service_only on public.training_event_outbox
    for all using (auth.role() = 'service_role')
    with check (auth.role() = 'service_role');

-- ---------------------------------------------------------------------------
-- Advisor RLS repair for backend-owned sensitive tables.
--
-- These tables contain OAuth tokens, password hashes, refresh tokens, or
-- backend project metadata. They should not be directly accessible to anon or
-- normal authenticated browser clients. The backend uses the Supabase service
-- role key and can still access them.
-- ---------------------------------------------------------------------------

alter table if exists public.gmail_accounts enable row level security;
drop policy if exists gmail_accounts_service_only on public.gmail_accounts;
create policy gmail_accounts_service_only on public.gmail_accounts
    for all using (auth.role() = 'service_role')
    with check (auth.role() = 'service_role');

alter table if exists public.codebot_users enable row level security;
drop policy if exists codebot_users_service_only on public.codebot_users;
create policy codebot_users_service_only on public.codebot_users
    for all using (auth.role() = 'service_role')
    with check (auth.role() = 'service_role');

alter table if exists public.codebot_refresh_tokens enable row level security;
drop policy if exists codebot_refresh_tokens_service_only on public.codebot_refresh_tokens;
create policy codebot_refresh_tokens_service_only on public.codebot_refresh_tokens
    for all using (auth.role() = 'service_role')
    with check (auth.role() = 'service_role');

alter table if exists public.codebot_projects enable row level security;
drop policy if exists codebot_projects_service_only on public.codebot_projects;
create policy codebot_projects_service_only on public.codebot_projects
    for all using (auth.role() = 'service_role')
    with check (auth.role() = 'service_role');

-- Helpful verification output.
select
    n.nspname as schema_name,
    c.relname as table_name,
    c.relrowsecurity as rls_enabled,
    coalesce(array_agg(p.polname order by p.polname) filter (where p.polname is not null), '{}') as policies
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
left join pg_policy p on p.polrelid = c.oid
where n.nspname = 'public'
  and c.relname in (
      'training_consents',
      'training_event_outbox',
      'gmail_accounts',
      'codebot_users',
      'codebot_refresh_tokens',
      'codebot_projects',
      'codebot_files',
      'codebot_sessions',
      'users',
      'codebot_deployments'
  )
group by n.nspname, c.relname, c.relrowsecurity
order by c.relname;

commit;

-- ---------------------------------------------------------------------------
-- Follow-up repair for remaining public CodeBot tables.
--
-- Important: some CodeBot workspace tables are read by browser/client flows.
-- Locking those to service_role only breaks CCAS. Keep RLS enabled, but restore
-- client access on app-facing workspace tables. Keep token/password/OAuth
-- tables service-only.
-- ---------------------------------------------------------------------------

begin;

do $$
declare
    table_name text;
    policy_name text;
begin
    foreach table_name in array array[
        'codebot_files',
        'codebot_sessions',
        'codebot_projects',
        'codebot_deployments'
    ]
    loop
        if to_regclass(format('public.%I', table_name)) is not null then
            execute format('alter table public.%I enable row level security', table_name);
            execute format('drop policy if exists %I on public.%I', table_name || '_service_only', table_name);
            policy_name := table_name || '_client_restore_access';
            execute format('drop policy if exists %I on public.%I', policy_name, table_name);
            execute format(
                'create policy %I on public.%I for all using (true) with check (true)',
                policy_name,
                table_name
            );
        end if;
    end loop;
end $$;

do $$
declare
    table_name text;
    policy_name text;
begin
    foreach table_name in array array[
        'gmail_accounts',
        'codebot_users',
        'codebot_refresh_tokens',
        'training_event_outbox'
    ]
    loop
        if to_regclass(format('public.%I', table_name)) is not null then
            execute format('alter table public.%I enable row level security', table_name);
            policy_name := table_name || '_service_only';
            execute format('drop policy if exists %I on public.%I', policy_name, table_name);
            execute format(
                'create policy %I on public.%I for all using (auth.role() = %L) with check (auth.role() = %L)',
                policy_name,
                table_name,
                'service_role',
                'service_role'
            );
        end if;
    end loop;
end $$;

drop policy if exists "Service role full access" on public.gmail_accounts;
drop policy if exists gmail_accounts_service_only on public.gmail_accounts;
create policy gmail_accounts_service_only on public.gmail_accounts
    for all using (auth.role() = 'service_role')
    with check (auth.role() = 'service_role');

select
    n.nspname as schema_name,
    c.relname as table_name,
    c.relrowsecurity as rls_enabled,
    coalesce(array_agg(p.polname order by p.polname) filter (where p.polname is not null), '{}') as policies
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
left join pg_policy p on p.polrelid = c.oid
where n.nspname = 'public'
  and (
      c.relname in ('users')
      or c.relname like 'codebot_%'
  )
group by n.nspname, c.relname, c.relrowsecurity
order by c.relname;

commit;
