-- Supabase migration: require image proof on refund requests.
-- 2026-04-21 — paired with client+server changes making amount paid,
-- PayPal order id, reason, and image proof all required on submission.
-- image_proof holds either a data URL (base64 image upload, max ~3 MB)
-- or an https URL pointing to a hosted screenshot.

alter table public.refund_requests
    add column if not exists image_proof text;

-- Backfill note: existing rows (pre-2026-04-21) have image_proof = NULL.
-- The admin panel surfaces "no proof" for these legacy rows — no
-- retroactive email ask required because every pre-existing request
-- either already paired with a Supabase join record (email-verified)
-- or was admin-created for manual credits.
