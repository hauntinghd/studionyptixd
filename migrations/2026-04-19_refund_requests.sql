-- Supabase migration: user-submittable refund requests + admin review.
-- Run via Supabase dashboard → SQL editor, or supabase CLI migrate.

create table if not exists public.refund_requests (
    id uuid primary key default gen_random_uuid(),
    user_id text not null,
    email text not null,
    reason text not null,
    amount_usd numeric(10, 2),
    payment_reference text,                    -- PayPal order id, invoice id, etc.
    status text not null default 'pending',    -- pending | approved | denied | refunded
    admin_note text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists refund_requests_user_id_idx on public.refund_requests (user_id);
create index if not exists refund_requests_status_idx on public.refund_requests (status);
create index if not exists refund_requests_created_at_idx on public.refund_requests (created_at desc);

-- RLS: users can insert + read only their own. Admins (server-side with
-- service key) bypass RLS.
alter table public.refund_requests enable row level security;

drop policy if exists "users can insert own refund request" on public.refund_requests;
create policy "users can insert own refund request" on public.refund_requests
    for insert with check (auth.uid()::text = user_id);

drop policy if exists "users can read own refund requests" on public.refund_requests;
create policy "users can read own refund requests" on public.refund_requests
    for select using (auth.uid()::text = user_id);

-- Trigger to keep updated_at in sync on every update.
create or replace function public.refund_requests_set_updated_at()
returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

drop trigger if exists refund_requests_updated_at on public.refund_requests;
create trigger refund_requests_updated_at
    before update on public.refund_requests
    for each row execute function public.refund_requests_set_updated_at();
