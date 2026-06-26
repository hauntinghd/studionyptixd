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
