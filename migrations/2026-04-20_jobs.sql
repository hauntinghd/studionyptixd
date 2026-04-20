-- Supabase migration: jobs (cross-worker job state for Studio renders).
-- Run via Supabase dashboard -> SQL editor, or supabase CLI migrate.
--
-- Context: Studio's render queue was keyed by local disk files per worker.
-- When RunPod routes /api/status/{job_id} polls across workers
-- (workersMax > 1), the non-creator worker has no local file -> 404. This
-- table mirrors in-memory job state so any worker can answer a status poll
-- for any job. Writes happen from backend_queue.py::_persist_job_state_supabase.
--
-- Also remediates: prior manual dashboard creation of `jobs` with user_id as
-- INTEGER. Production code writes user_id as text (a UUID string or the
-- literal "unknown" fallback). Int-typed column silently 400s every insert
-- ("invalid input syntax for type integer"), which the code logged as a
-- warning and swallowed -- cross-worker polling was broken for ~24h.

create table if not exists public.jobs (
    id           text        primary key,
    user_id      text        not null,
    status       text,
    payload      jsonb       not null,
    total_chunks int         not null default 0,
    done_chunks  int         not null default 0,
    error        text,
    created_at   timestamptz not null default now(),
    updated_at   timestamptz not null default now()
);

-- Remediation step 1: drop any foreign key constraint on user_id. The prior
-- manual table definition pointed user_id at an integer-id table (evidenced
-- by error 42804 "Key columns user_id and id are of incompatible types:
-- text and integer"). Studio authorizes per-user at the API layer + uses
-- service-role-only RLS, so DB-level FK integrity on user_id adds no value
-- and actively blocks the correct text typing.
do $$
declare
    r record;
begin
    for r in
        select tc.constraint_name
        from information_schema.table_constraints tc
        join information_schema.key_column_usage  kcu
          on tc.constraint_name = kcu.constraint_name
         and tc.table_schema    = kcu.table_schema
         and tc.table_name      = kcu.table_name
        where tc.table_schema   = 'public'
          and tc.table_name     = 'jobs'
          and tc.constraint_type = 'FOREIGN KEY'
          and kcu.column_name    = 'user_id'
    loop
        execute format('alter table public.jobs drop constraint %I', r.constraint_name);
    end loop;
end $$;

-- Remediation step 2: fix the column type. Idempotent: no-op if already text.
do $$
begin
    if exists (
        select 1 from information_schema.columns
        where table_schema = 'public'
          and table_name   = 'jobs'
          and column_name  = 'user_id'
          and data_type   <> 'text'
    ) then
        alter table public.jobs
            alter column user_id type text using user_id::text;
    end if;
end $$;

create index if not exists jobs_user_id_idx    on public.jobs (user_id);
create index if not exists jobs_updated_at_idx on public.jobs (updated_at desc);

alter table public.jobs enable row level security;

-- Service-role only. Users never touch this table directly; they poll
-- /api/studio/status/{job_id} and the backend authorizes per-user.
drop policy if exists "service role only on jobs" on public.jobs;
-- (no policy = no public access once RLS is on, which is what we want)

create or replace function public.jobs_set_updated_at()
returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

drop trigger if exists jobs_updated_at on public.jobs;
create trigger jobs_updated_at
    before update on public.jobs
    for each row execute function public.jobs_set_updated_at();
