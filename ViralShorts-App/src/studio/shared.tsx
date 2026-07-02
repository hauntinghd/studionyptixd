import { useState, useEffect, createContext, useCallback, useRef } from 'react';
import { createClient, Session, SupabaseClient } from '@supabase/supabase-js';
import { UNIFIED_TOPUP_PACKS } from './lib/studioProduct';
import { trackAuthCompletion } from './lib/googleAds';

const viteEnv = ((import.meta as any).env || {}) as Record<string, string>;
const hostLower = window.location.hostname.toLowerCase();
export const isLocalDevHost = hostLower === "localhost" || hostLower === "127.0.0.1";
const billingHostAliases = new Set([
    "billing.nyptidindustries.com",
    "billing.niptidindustries.com",
    "invoicer.nyptidindustries.com",
    "invoicer.niptidindustries.com",
]);
export const isBillingHost = billingHostAliases.has(hostLower) || hostLower.startsWith("billing.") || hostLower.startsWith("invoicer.");
export const STUDIO_SITE_URL = "https://studio.nyptidindustries.com";
export const BILLING_SITE_URL = STUDIO_SITE_URL;
export const INVOICER_API_BASE_URL = "https://invoicer.nyptidindustries.com";
export const PROD_API_BASE_URL = "https://api-studio.nyptidindustries.com";
const resolveSafeApiBase = (rawBase: string): string => {
    const cleaned = (rawBase || "").trim().replace(/\/+$/, "");
    if (!cleaned) return "";
    if (isLocalDevHost) return cleaned;
    try {
        const parsed = new URL(cleaned, window.location.origin);
        const isMixedContent = window.location.protocol === "https:" && parsed.protocol === "http:";
        const isLocalTarget = parsed.hostname === "localhost" || parsed.hostname === "127.0.0.1";
        const sameHost = parsed.hostname === window.location.hostname;
        const hasCustomPort = parsed.port !== "" && parsed.port !== window.location.port;
        const hasNonStandardPort = parsed.port !== "" && parsed.port !== "443" && parsed.port !== "80";
        if (isMixedContent || isLocalTarget || (sameHost && hasCustomPort) || hasNonStandardPort) return "";
        return cleaned;
    } catch {
        return "";
    }
};

// API routing:
// - local dev: use VITE_API_BASE_URL / VITE_GENERATION_API_BASE_URL
// - hosted UI: default to the direct production API because the Studio proxy path can time out on larger authenticated requests
const rawLocalApi = resolveSafeApiBase(viteEnv.VITE_API_BASE_URL || "");
const rawProdApi = resolveSafeApiBase(viteEnv.VITE_PROD_API_BASE_URL || "");
const hostedOrigin = typeof window !== "undefined" ? window.location.origin : "";
export const API = isLocalDevHost ? rawLocalApi : (rawProdApi || PROD_API_BASE_URL || hostedOrigin);
export const DIRECT_API = isLocalDevHost ? (rawLocalApi || API) : (rawProdApi || PROD_API_BASE_URL || API);
/** Studio Agent hits Fly directly (persistent sessions); api-studio RunPod can lag behind. */
export const STUDIO_AGENT_API = isLocalDevHost
    ? (rawLocalApi || API)
    : (resolveSafeApiBase(viteEnv.VITE_STUDIO_AGENT_API || "") || "https://nyptid-studio.fly.dev");

export const FLY_DIRECT_API_PREFIXES = [
    '/api/studio-agent',
    '/api/studio-hub',
    '/api/youtube',
    '/api/studio/analytics',
    '/api/checkout',
    '/api/billing-portal',
    '/api/paypal',
];

/** Routes that must not go through RunPod (429 queue, cold sync, session loss). */
export function resolveStudioBackendUrl(path: string): string {
    const normalized = path.startsWith('/') ? path : `/${path}`;
    if (isLocalDevHost) return `${API}${normalized}`;
    if (FLY_DIRECT_API_PREFIXES.some((prefix: string) => normalized.startsWith(prefix))) {
        return `${STUDIO_AGENT_API}${normalized}`;
    }
    return `${API}${normalized}`;
}

/** OAuth return URL that lands back on Studio Agent after Google connect. */
export const studioAgentOAuthReturnUrl = (): string => {
    if (typeof window === "undefined") {
        return `${STUDIO_SITE_URL}?page=dashboard&tab=agent`;
    }
    const u = new URL(window.location.href);
    u.searchParams.set("page", "dashboard");
    u.searchParams.set("tab", "agent");
    u.searchParams.delete("youtube");
    u.searchParams.delete("youtube_message");
    return u.toString();
};

/** Authenticated fetch to Studio/Fly-backed routes (agent, YouTube, analytics). */
export async function studioAgentFetch(
    path: string,
    accessToken: string,
    init?: RequestInit,
): Promise<Response> {
    const url = resolveStudioBackendUrl(path);
    return fetch(url, {
        ...init,
        headers: {
            ...(init?.headers || {}),
            Authorization: `Bearer ${accessToken}`,
        },
    });
}
const rawGenerationApi = resolveSafeApiBase(
    (isLocalDevHost ? viteEnv.VITE_GENERATION_API_BASE_URL : viteEnv.VITE_PROD_GENERATION_API_BASE_URL) || ""
);
const FIREFOX_HOTFIX_TAG = "ff-hotfix-1";
const BOOT_CONFIG_TIMEOUT_MS = 12000;
const SUPABASE_SESSION_TIMEOUT_MS = 8000;
const HEALTH_PROBE_TIMEOUT_MS = 8000;
const HEALTH_PROBE_INTERVAL_MS = 6000;
const HEALTH_FAILURE_THRESHOLD = 8;
const HEALTH_RECENT_SUCCESS_GRACE_MS = 45000;
const OWNER_EMAILS = new Set(
    String(viteEnv.VITE_OWNER_EMAILS || "omatic657@gmail.com")
        .split(",")
        .map((email) => email.trim().toLowerCase())
        .filter(Boolean)
);
export const isOwnerEmail = (email?: string | null): boolean => {
    return Boolean(email && OWNER_EMAILS.has(String(email).trim().toLowerCase()));
};
export const GENERATION_API = (() => {
    if (!rawGenerationApi) {
        return API || (isLocalDevHost ? `${window.location.protocol}//${window.location.hostname}:8091` : hostedOrigin || PROD_API_BASE_URL);
    }
    return rawGenerationApi;
})();
if (typeof window !== "undefined" && /firefox/i.test(window.navigator.userAgent)) {
    (window as any).__NYPTID_FIREFOX_HOTFIX__ = FIREFOX_HOTFIX_TAG;
}
export const CREATE_WORKFLOW_PERSISTENCE_ENABLED = true;

export const startYouTubeBrowserConnect = (accessToken: string, nextUrl?: string): void => {
    const token = String(accessToken || "").trim();
    if (!token || typeof document === "undefined") return;
    const form = document.createElement("form");
    form.method = "POST";
    form.action = `${DIRECT_API || API}/api/oauth/google/youtube/browser-start`;
    form.style.display = "none";

    const appendField = (name: string, value: string) => {
        const input = document.createElement("input");
        input.type = "hidden";
        input.name = name;
        input.value = value;
        form.appendChild(input);
    };

    appendField("access_token", token);
    appendField("next_url", String(nextUrl || window.location.href || "").trim());
    document.body.appendChild(form);
    form.submit();
    window.setTimeout(() => {
        try {
            form.remove();
        } catch {
            // no-op
        }
    }, 0);
};
export const PUBLIC_TEMPLATE_IDS = new Set([
    'story',
    'motivation',
    'skeleton',
    'chatstory',
]);
export const CHAT_STORY_MONTHLY_PLAN_IDS = new Set([
    'starter',
    'creator',
    'pro',
    'studio',
    'studio_pro_1k',
    'studio_pro_2500',
    'studio_pro_5k',
    'studio_pro_11k',
    'studio_pro_17k',
    'studio_pro_24k',
    'studio_pro_32k',
    'studio_pro_2k',
    'studio_pro_8k',
    'studio_pro_15k',
]);
export const hasChatStoryTemplateAccess = (
    planName: string | null | undefined,
    billingActive: boolean,
    role?: string | null
): boolean => {
    const normalizedPlan = String(planName || "").trim().toLowerCase();
    const normalizedRole = String(role || "").trim().toLowerCase();
    if (normalizedRole === "admin") return true;
    return Boolean(billingActive && CHAT_STORY_MONTHLY_PLAN_IDS.has(normalizedPlan));
};
export const CLONE_COMING_SOON = true;
export const Logo = ({ size = 24 }: { size?: number }) => (
    <img
        src="/logo.png"
        alt="NYPTID Studio"
        width={size}
        height={size}
        className="rounded-md object-contain"
        style={{ maxHeight: size, maxWidth: size }}
    />
);

// ── Supabase fallback credentials (used when /api/config is down) ───────────
const FALLBACK_SUPABASE_URL = viteEnv.VITE_SUPABASE_URL || "https://qdwzilgqvpegekxrrnnn.supabase.co";
const FALLBACK_SUPABASE_ANON_KEY = viteEnv.VITE_SUPABASE_ANON_KEY || "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFkd3ppbGdxdnBlZ2VreHJybm5uIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwMjQ3NzYsImV4cCI6MjA4MTYwMDc3Nn0.89jrswXUwk1Th_e2y7QEq_vLf3M2XhQJjIfByWOD7EE";

export const WAITLIST_TABLE = "waiting_list";
export const WAITLIST_FALLBACK_TABLE = "app_settings";
export const WAITLIST_FALLBACK_KEY_PREFIX = "studio_waitlist_reservation:";
export const FRIENDLY_WAITLIST_SETUP_ERROR =
    "Waiting list is not initialized yet. Run the waitlist bootstrap SQL in Supabase SQL Editor.";
export const WAITLIST_BOOTSTRAP_SQL = `-- Run once in Supabase SQL Editor
create extension if not exists pgcrypto;

create table if not exists public.waiting_list (
  id uuid primary key default gen_random_uuid(),
  email text not null unique,
  plan text not null check (plan in ('starter','creator','pro')),
  price_usd numeric(10,2) not null default 0,
  paid boolean not null default false,
  stripe_session_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists waiting_list_created_at_idx on public.waiting_list (created_at desc);
create index if not exists waiting_list_paid_idx on public.waiting_list (paid);

create or replace function public.touch_waiting_list_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists trg_waiting_list_updated_at on public.waiting_list;
create trigger trg_waiting_list_updated_at
before update on public.waiting_list
for each row execute function public.touch_waiting_list_updated_at();

alter table public.waiting_list enable row level security;

drop policy if exists "Service role full access waiting_list" on public.waiting_list;
create policy "Service role full access waiting_list"
on public.waiting_list
for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists "Users can read own waiting_list row" on public.waiting_list;
create policy "Users can read own waiting_list row"
on public.waiting_list
for select
using (lower(email) = lower(coalesce(auth.jwt() ->> 'email', '')));

drop policy if exists "Users can insert own waiting_list row" on public.waiting_list;
create policy "Users can insert own waiting_list row"
on public.waiting_list
for insert
with check (lower(email) = lower(coalesce(auth.jwt() ->> 'email', '')));

drop policy if exists "Users can update own waiting_list row" on public.waiting_list;
create policy "Users can update own waiting_list row"
on public.waiting_list
for update
using (lower(email) = lower(coalesce(auth.jwt() ->> 'email', '')))
with check (lower(email) = lower(coalesce(auth.jwt() ->> 'email', '')));`;
export const isWaitlistTableMissingError = (err: any): boolean => {
    const code = String(err?.code || "").toUpperCase();
    const message = String(err?.message || "").toLowerCase();
    const details = String(err?.details || "").toLowerCase();
    return (
        code === "PGRST205"
        || (message.includes("could not find the table") && message.includes(WAITLIST_TABLE))
        || (details.includes("could not find the table") && details.includes(WAITLIST_TABLE))
    );
};


// ── Waiting List Types ──────────────────────────────────────────────────────
export type WaitingListEntry = {
    id?: string;
    email: string;
    plan: string;
    price_usd: number;
    paid: boolean;
    stripe_session_id?: string;
    created_at?: string;
};
const readJsonResponse = async <T = any>(res: Response): Promise<{ data: T | null; raw: string }> => {
    const raw = await res.text().catch(() => "");
    if (!raw) return { data: null, raw: "" };
    try {
        return { data: JSON.parse(raw) as T, raw };
    } catch {
        return { data: null, raw };
    }
};

const fetchWithTimeout = async (url: string, init: RequestInit, timeoutMs = 20000): Promise<Response> => {
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
    try {
        return await fetch(url, { ...init, signal: controller.signal });
    } finally {
        window.clearTimeout(timeoutId);
    }
};

const normalizeWaitlistPlan = (planName: string): string => {
    const normalized = String(planName || "").trim().toLowerCase();
    if (["starter", "creator", "pro", "studio", "studio_pro_1k", "studio_pro_2500", "studio_pro_5k", "studio_pro_11k", "studio_pro_17k", "studio_pro_24k", "studio_pro_32k", "studio_pro_2k", "studio_pro_8k", "studio_pro_15k"].includes(normalized)) return normalized;
    return "starter";
};

const waitlistFallbackKeyForEmail = (email: string): string => {
    return `${WAITLIST_FALLBACK_KEY_PREFIX}${String(email || "").trim().toLowerCase()}`;
};

const parseWaitlistFallbackValue = (value: any): Record<string, any> => {
    if (value && typeof value === "object" && !Array.isArray(value)) {
        return value as Record<string, any>;
    }
    if (typeof value === "string") {
        try {
            const parsed = JSON.parse(value);
            if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
                return parsed as Record<string, any>;
            }
        } catch {
            // ignore malformed fallback payload
        }
    }
    return {};
};

const coerceWaitlistFallbackRow = (row: any): WaitingListEntry | null => {
    const parsed = parseWaitlistFallbackValue(row?.value);
    const keyEmail = String(row?.key || "").startsWith(WAITLIST_FALLBACK_KEY_PREFIX)
        ? String(row.key).slice(WAITLIST_FALLBACK_KEY_PREFIX.length)
        : "";
    const email = String(parsed.email || keyEmail || "").trim().toLowerCase();
    if (!email) return null;
    return {
        id: String(row?.id || ""),
        email,
        plan: normalizeWaitlistPlan(String(parsed.plan || "starter")),
        price_usd: Number(parsed.price_usd || 0),
        paid: Boolean(parsed.paid),
        stripe_session_id: parsed.stripe_session_id ? String(parsed.stripe_session_id) : undefined,
        created_at: String(parsed.created_at || row?.updated_at || ""),
    };
};

const withTimeout = async <T,>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> => {
    let timeoutId: ReturnType<typeof setTimeout> | null = null;
    try {
        return await Promise.race([
            promise,
            new Promise<T>((_, reject) => {
                timeoutId = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
            }),
        ]);
    } finally {
        if (timeoutId) clearTimeout(timeoutId);
    }
};

const decodeAuthParam = (value: string): string => {
    const normalized = String(value || '').replace(/\+/g, ' ').trim();
    if (!normalized) return '';
    try {
        return decodeURIComponent(normalized);
    } catch {
        return normalized;
    }
};

const clearSupabaseAuthRedirectArtifacts = (): void => {
    if (typeof window === 'undefined') return;
    try {
        const url = new URL(window.location.href);
        let changed = false;
        const hashBody = String(url.hash || '').replace(/^#/, '');
        if (/(^|&)(access_token|refresh_token|expires_in|expires_at|token_type|type|provider_token|provider_refresh_token|error|error_description|error_code)=/i.test(hashBody)) {
            url.hash = '';
            changed = true;
        }
        for (const key of ['code', 'error', 'error_code', 'error_description']) {
            if (url.searchParams.has(key)) {
                url.searchParams.delete(key);
                changed = true;
            }
        }
        if (!changed) return;
        window.history.replaceState({}, document.title, `${url.pathname}${url.search}${url.hash}`);
        window.dispatchEvent(new Event('nyptid:navigation'));
    } catch {
        // ignore URL cleanup failures
    }
};

/** Avoid re-rendering the whole Studio tree when Supabase emits the same tokens on a new object. */
const sameAuthSession = (prev: Session | null, next: Session | null): boolean => {
    if (prev === next) return true;
    if (!prev || !next) return !prev && !next;
    return (
        prev.access_token === next.access_token
        && prev.refresh_token === next.refresh_token
        && (prev.user?.id ?? '') === (next.user?.id ?? '')
    );
};

const recoverSupabaseSessionFromUrl = async (sb: SupabaseClient): Promise<Session | null> => {
    if (typeof window === 'undefined') return null;
    const url = new URL(window.location.href);
    const query = url.searchParams;
    const hash = new URLSearchParams(String(url.hash || '').replace(/^#/, ''));
    const authError =
        decodeAuthParam(String(hash.get('error_description') || query.get('error_description') || hash.get('error') || query.get('error') || ''));
    if (authError) {
        clearSupabaseAuthRedirectArtifacts();
        throw new Error(authError);
    }

    const authCode = String(query.get('code') || '').trim();
    if (authCode && typeof (sb.auth as any).exchangeCodeForSession === 'function') {
        const result = await withTimeout(
            (sb.auth as any).exchangeCodeForSession(authCode),
            SUPABASE_SESSION_TIMEOUT_MS,
            'Supabase code exchange'
        );
        clearSupabaseAuthRedirectArtifacts();
        const error = (result as any)?.error;
        if (error) throw error;
        return ((result as any)?.data?.session || null) as Session | null;
    }

    const accessToken = String(hash.get('access_token') || '').trim();
    const refreshToken = String(hash.get('refresh_token') || '').trim();
    if (accessToken && refreshToken && typeof (sb.auth as any).setSession === 'function') {
        const result = await withTimeout(
            sb.auth.setSession({ access_token: accessToken, refresh_token: refreshToken }),
            SUPABASE_SESSION_TIMEOUT_MS,
            'Supabase session recovery'
        );
        clearSupabaseAuthRedirectArtifacts();
        if (result.error) throw result.error;
        return (result.data.session || null) as Session | null;
    }

    if (accessToken || refreshToken) clearSupabaseAuthRedirectArtifacts();
    return null;
};

export const readWaitlistFallbackRows = async (supabase: SupabaseClient): Promise<WaitingListEntry[]> => {
    const { data, error } = await supabase
        .from(WAITLIST_FALLBACK_TABLE)
        .select("id,key,value,updated_at")
        .like("key", `${WAITLIST_FALLBACK_KEY_PREFIX}%`)
        .order("updated_at", { ascending: false });
    if (error) throw error;
    const rows = (Array.isArray(data) ? data : [])
        .map((row) => coerceWaitlistFallbackRow(row))
        .filter((row): row is WaitingListEntry => Boolean(row));
    rows.sort((a, b) => {
        const ta = Date.parse(String(a.created_at || "")) || 0;
        const tb = Date.parse(String(b.created_at || "")) || 0;
        return tb - ta;
    });
    return rows;
};

export const upsertWaitlistFallbackRow = async (
    supabase: SupabaseClient,
    {
        email,
        plan,
        priceUsd,
        paid,
        stripeSessionId,
    }: {
        email: string;
        plan: string;
        priceUsd: number;
        paid: boolean;
        stripeSessionId?: string;
    }
): Promise<string | null> => {
    const normalizedEmail = String(email || "").trim().toLowerCase();
    if (!normalizedEmail) return "No email found";
    const key = waitlistFallbackKeyForEmail(normalizedEmail);
    const basePayload = {
        email: normalizedEmail,
        plan: normalizeWaitlistPlan(plan),
        price_usd: Number(priceUsd || 0),
        paid: Boolean(paid),
        stripe_session_id: stripeSessionId ? String(stripeSessionId) : null,
        created_at: new Date().toISOString(),
    };
    const { data: existing, error: existingErr } = await supabase
        .from(WAITLIST_FALLBACK_TABLE)
        .select("id,value")
        .eq("key", key)
        .limit(1);
    if (existingErr) return existingErr.message || "Failed to reserve waitlist entry";
    if (Array.isArray(existing) && existing.length > 0) {
        const prev = parseWaitlistFallbackValue(existing[0]?.value);
        const payload = {
            ...basePayload,
            created_at: String(prev.created_at || basePayload.created_at),
            paid: Boolean(prev.paid) || basePayload.paid,
            stripe_session_id: prev.stripe_session_id || basePayload.stripe_session_id,
        };
        const { error: updateErr } = await supabase
            .from(WAITLIST_FALLBACK_TABLE)
            .update({ value: payload })
            .eq("id", existing[0]?.id);
        if (updateErr) return updateErr.message || "Failed to update waitlist reservation";
        return null;
    }
    const { error: insertErr } = await supabase
        .from(WAITLIST_FALLBACK_TABLE)
        .insert({ key, value: basePayload });
    if (insertErr) return insertErr.message || "Failed to create waitlist reservation";
    return null;
};

// ── Waiting List Plan Config ────────────────────────────────────────────────
export const WAITLIST_PLANS: { name: string; label: string; price: number }[] = [];

export type Plan =
    | 'none'
    | 'free'
    | 'starter'
    | 'creator'
    | 'pro'
    | 'studio'
    | 'studio_pro_1k'
    | 'studio_pro_2500'
    | 'studio_pro_5k'
    | 'studio_pro_11k'
    | 'studio_pro_17k'
    | 'studio_pro_24k'
    | 'studio_pro_32k'
    | 'studio_pro_2k'
    | 'studio_pro_8k'
    | 'studio_pro_15k';
export type TopupPack = { price_id: string; pack: string; credits: number; price_usd: number };
export type PlanLimit = {
    videos_per_month?: number;
    animated_renders_per_month?: number;
    non_animated_ops_per_month?: number;
    monthly_credits?: number;
    price_usd?: number;
    max_duration_sec?: number;
    max_resolution?: string;
    can_clone?: boolean;
    priority?: boolean;
    demo_access?: boolean;
};
export type PlanLimitMap = Record<string, PlanLimit>;
export type PlanFeatureMap = Record<string, string[]>;
export type PlanPriceMap = Record<string, number>;
export type LaneAccessMap = Record<string, boolean>;

// Public-plan fallbacks. Kept in sync with backend_catalog.py PLAN_LIMITS/PLAN_FEATURES
// and backend_settings.py PLAN_PRICE_USD/TOPUP_PACK_SPECS. Used as initial state so
// Billing/Dashboard render correct prices/limits even while /api/config is loading or
// temporarily unreachable (e.g. serverless cold-start on RunPod).
export const PUBLIC_PLAN_LIMITS_FALLBACK: PlanLimitMap = {
    creator: { monthly_credits: 2000, price_usd: 60 },
    studio: { monthly_credits: 8000, price_usd: 200 },
    studio_pro_1k: { monthly_credits: 1000, price_usd: 25 },
    studio_pro_2500: { monthly_credits: 2500, price_usd: 50 },
    studio_pro_5k: { monthly_credits: 5000, price_usd: 100 },
    studio_pro_11k: { monthly_credits: 11000, price_usd: 200 },
    studio_pro_17k: { monthly_credits: 17000, price_usd: 300 },
    studio_pro_24k: { monthly_credits: 24000, price_usd: 400 },
    studio_pro_32k: { monthly_credits: 32000, price_usd: 500 },
    studio_pro_2k: { monthly_credits: 2000, price_usd: 60 },
    studio_pro_8k: { monthly_credits: 8000, price_usd: 200 },
    studio_pro_15k: { monthly_credits: 15000, price_usd: 350 },
};
export const PUBLIC_PLAN_PRICES_FALLBACK: PlanPriceMap = {
    creator: 60,
    studio: 200,
    studio_pro_1k: 25,
    studio_pro_2500: 50,
    studio_pro_5k: 100,
    studio_pro_11k: 200,
    studio_pro_17k: 300,
    studio_pro_24k: 400,
    studio_pro_32k: 500,
    studio_pro_2k: 60,
    studio_pro_8k: 200,
    studio_pro_15k: 350,
};
export const PUBLIC_PLAN_FEATURES_FALLBACK: PlanFeatureMap = {
    creator: ['studio_agent', 'openrouter', 'fal_render', 'elevenlabs'],
    studio: ['studio_agent', 'openrouter', 'fal_render', 'elevenlabs', 'priority_queue'],
    studio_pro_1k: ['studio_agent', 'openrouter', 'fal_render', 'elevenlabs'],
    studio_pro_2500: ['studio_agent', 'openrouter', 'fal_render', 'elevenlabs'],
    studio_pro_5k: ['studio_agent', 'openrouter', 'fal_render', 'elevenlabs', 'priority_queue'],
    studio_pro_11k: ['studio_agent', 'openrouter', 'fal_render', 'elevenlabs', 'priority_queue'],
    studio_pro_17k: ['studio_agent', 'openrouter', 'fal_render', 'elevenlabs', 'priority_queue'],
    studio_pro_24k: ['studio_agent', 'openrouter', 'fal_render', 'elevenlabs', 'priority_queue'],
    studio_pro_32k: ['studio_agent', 'openrouter', 'fal_render', 'elevenlabs', 'priority_queue'],
    studio_pro_2k: ['studio_agent', 'openrouter', 'fal_render', 'elevenlabs'],
    studio_pro_8k: ['studio_agent', 'openrouter', 'fal_render', 'elevenlabs', 'priority_queue'],
    studio_pro_15k: ['studio_agent', 'openrouter', 'fal_render', 'elevenlabs', 'priority_queue'],
};
export const PUBLIC_TOPUP_PACKS_FALLBACK: TopupPack[] = [
    { price_id: 'uc_reload', pack: 'reload', credits: 1000, price_usd: 25 },
];

export interface AuthContextType {
    session: Session | null;
    supabase: SupabaseClient | null;
    plan: Plan;
    role: string;
    ownerOverride: boolean;
    loading: boolean;
    billingActive: boolean;
    membershipActive: boolean;
    membershipPlanId: string;
    membershipSource: string;
    backendOffline: boolean;
    nextRenewalUnix: number;
    nextRenewalSource: string;
    billingAnchorUnix: number;
    monthlyCreditsRemaining: number;
    topupCreditsRemaining: number;
    creditsTotalRemaining: number;
    requiresTopup: boolean;
    topupPacks: TopupPack[];
    demoAccess: boolean;
    demoPriceId: string;
    demoComingSoon: boolean;
    maintenanceBannerEnabled: boolean;
    maintenanceBannerMessage: string;
    longformOwnerBeta: boolean;
    waitlistOnlyMode: boolean;
    waitlistRequiresStripePayment: boolean;
    publicPlanLimits: PlanLimitMap;
    publicPlanFeatures: PlanFeatureMap;
    publicPlanPrices: PlanPriceMap;
    studioLaneAccess: LaneAccessMap;
    defaultMembershipPlanId: string;
    signIn: (email: string, password: string) => Promise<string | null>;
    signInWithGoogle: () => Promise<string | null>;
    signUp: (email: string, password: string) => Promise<string | null>;
    signOut: () => Promise<void>;
    checkout: (plan: string) => Promise<string | null>;
    checkoutTopup: (priceId: string, preferredMethod?: 'card' | 'paypal') => Promise<string | null>;
    checkoutDemo: () => Promise<void>;
    manageBilling: () => Promise<string | null>;
    joinWaitingList: (plan: string, priceUsd: number) => Promise<string | null>;
    verifyPayPalOrder: (orderId: string) => Promise<PayPalVerifyResult>;
}

export interface PayPalVerifyResult {
    ok: boolean;
    captured: boolean;
    revoked: boolean;
    kind: string;
    plan?: string;
    credits?: number;
    error?: string;
}

export const AuthContext = createContext<AuthContextType>({
    session: null, supabase: null, plan: 'none', role: 'user', ownerOverride: false, loading: true, billingActive: false, membershipActive: false, membershipPlanId: 'none',
    membershipSource: '',
    backendOffline: false,
    nextRenewalUnix: 0, nextRenewalSource: '',
    billingAnchorUnix: 0,
    monthlyCreditsRemaining: 0, topupCreditsRemaining: 0, creditsTotalRemaining: 0, requiresTopup: false, topupPacks: PUBLIC_TOPUP_PACKS_FALLBACK,
    demoAccess: false, demoPriceId: '', demoComingSoon: true, publicPlanLimits: PUBLIC_PLAN_LIMITS_FALLBACK, publicPlanFeatures: PUBLIC_PLAN_FEATURES_FALLBACK, publicPlanPrices: PUBLIC_PLAN_PRICES_FALLBACK, studioLaneAccess: {}, defaultMembershipPlanId: 'studio_pro_1k',
    maintenanceBannerEnabled: false, maintenanceBannerMessage: '',
    longformOwnerBeta: false,
    waitlistOnlyMode: false,
    waitlistRequiresStripePayment: false,
    signIn: async () => null, signInWithGoogle: async () => null, signUp: async () => null, signOut: async () => {},
    checkout: async () => null, checkoutTopup: async () => null, checkoutDemo: async () => {}, manageBilling: async () => null,
    joinWaitingList: async () => null,
    verifyPayPalOrder: async () => ({ ok: false, captured: false, revoked: false, kind: '', error: 'Not initialized' }),
});

export function AuthProvider({ children }: { children: React.ReactNode }) {
    const [supabase, setSupabase] = useState<SupabaseClient | null>(null);
    const [session, setSession] = useState<Session | null>(null);
    const [plan, setPlan] = useState<Plan>('none');
    const [role, setRole] = useState<string>('user');
    const [ownerOverride, setOwnerOverride] = useState(false);
    const [loading, setLoading] = useState(true);
    const [billingActive, setBillingActive] = useState(false);
    const [membershipActive, setMembershipActive] = useState(false);
    const [membershipPlanId, setMembershipPlanId] = useState('none');
    const [membershipSource, setMembershipSource] = useState('');
    const [backendOffline, setBackendOffline] = useState(false);
    const [nextRenewalUnix, setNextRenewalUnix] = useState(0);
    const [nextRenewalSource, setNextRenewalSource] = useState('');
    const [billingAnchorUnix, setBillingAnchorUnix] = useState(0);
    const [monthlyCreditsRemaining, setMonthlyCreditsRemaining] = useState(0);
    const [topupCreditsRemaining, setTopupCreditsRemaining] = useState(0);
    const [creditsTotalRemaining, setCreditsTotalRemaining] = useState(0);
    const [requiresTopup, setRequiresTopup] = useState(false);
    const [topupPacks, setTopupPacks] = useState<TopupPack[]>(PUBLIC_TOPUP_PACKS_FALLBACK);
    const [demoAccess, setDemoAccess] = useState(false);
    const [demoPriceId, setDemoPriceId] = useState('');
    const [demoComingSoon, setDemoComingSoon] = useState(true);
    const [publicPlanLimits, setPublicPlanLimits] = useState<PlanLimitMap>(PUBLIC_PLAN_LIMITS_FALLBACK);
    const [publicPlanFeatures, setPublicPlanFeatures] = useState<PlanFeatureMap>(PUBLIC_PLAN_FEATURES_FALLBACK);
    const [publicPlanPrices, setPublicPlanPrices] = useState<PlanPriceMap>(PUBLIC_PLAN_PRICES_FALLBACK);
    const [studioLaneAccess, setStudioLaneAccess] = useState<LaneAccessMap>({});
    const [defaultMembershipPlanId, setDefaultMembershipPlanId] = useState('studio_pro_1k');
    const [maintenanceBannerEnabled, setMaintenanceBannerEnabled] = useState(false);
    const [maintenanceBannerMessage, setMaintenanceBannerMessage] = useState('');
    const [longformOwnerBeta, setLongformOwnerBeta] = useState(false);
    const [waitlistOnlyMode, setWaitlistOnlyMode] = useState(false);
    const [waitlistRequiresStripePayment, setWaitlistRequiresStripePayment] = useState(false);
    const pendingAuthIntentRef = useRef<'signup' | 'signin' | 'google' | ''>('');
    const lastTrackedSessionUserRef = useRef('');
    const healthFailureCountRef = useRef(0);
    const lastHealthSuccessAtRef = useRef(0);
    const supabaseRef = useRef<SupabaseClient | null>(null);
    const authSubscriptionRef = useRef<{ unsubscribe: () => void } | null>(null);
    const ensureSupabasePromiseRef = useRef<Promise<SupabaseClient | null> | null>(null);
    const ownerLaneAccess: LaneAccessMap = {
        create: true,
        thumbnails: true,
        cliplab: true,
        clone: true,
        longform: true,
        chatstory: true,
        autoclipper: true,
        demo: true,
        analytics: true,
        membership: true,
        wallet: true,
    };
    const applyOwnerAccess = useCallback(() => {
        setRole('admin');
        setPlan('pro');
        setBillingActive(true);
        setMembershipActive(true);
        setMembershipPlanId('pro');
        setMembershipSource('admin');
        setOwnerOverride(true);
        setStudioLaneAccess(ownerLaneAccess);
        setLongformOwnerBeta(true);
    }, []);
    const normalizeViewerPlanId = useCallback((rawValue: unknown, fallback: Plan = 'free'): Plan => {
        const normalized = String(rawValue || '').trim().toLowerCase();
        if (normalized === 'demo_pro' || normalized === 'elite') return 'pro';
        if (normalized === 'free' || normalized === 'none') return 'free';
        if (normalized === 'starter' || normalized === 'creator' || normalized === 'pro' || normalized === 'studio' || normalized === 'studio_pro_1k' || normalized === 'studio_pro_2500' || normalized === 'studio_pro_5k' || normalized === 'studio_pro_11k' || normalized === 'studio_pro_17k' || normalized === 'studio_pro_24k' || normalized === 'studio_pro_32k' || normalized === 'studio_pro_2k' || normalized === 'studio_pro_8k' || normalized === 'studio_pro_15k') return normalized as Plan;
        return fallback;
    }, []);
    useEffect(() => {
        supabaseRef.current = supabase;
    }, [supabase]);
    const attachSupabaseClient = useCallback(async (sb: SupabaseClient): Promise<SupabaseClient> => {
        supabaseRef.current = sb;
        setSupabase((prev) => prev || sb);
        let recoveredSession: Session | null = null;
        try {
            recoveredSession = await recoverSupabaseSessionFromUrl(sb);
        } catch (error) {
            console.warn('Supabase redirect recovery failed', error);
        }
        const sessionResult = recoveredSession
            ? { data: { session: recoveredSession } }
            : await withTimeout(sb.auth.getSession(), SUPABASE_SESSION_TIMEOUT_MS, 'Supabase session bootstrap');
        setSession((prev) => {
            const next = sessionResult.data.session || null;
            return sameAuthSession(prev, next) ? prev : next;
        });
        if (!authSubscriptionRef.current) {
            const { data } = sb.auth.onAuthStateChange((_e, nextSession) => {
                clearSupabaseAuthRedirectArtifacts();
                setSession((prev) => (sameAuthSession(prev, nextSession) ? prev : nextSession));
            });
            authSubscriptionRef.current = data.subscription;
        }
        return sb;
    }, []);
    const ensureSupabaseClient = useCallback(async (): Promise<SupabaseClient | null> => {
        if (supabaseRef.current) return supabaseRef.current;
        if (ensureSupabasePromiseRef.current) return await ensureSupabasePromiseRef.current;
        ensureSupabasePromiseRef.current = (async () => {
            try {
                const fallbackUrl = String(FALLBACK_SUPABASE_URL || '').trim();
                const fallbackKey = String(FALLBACK_SUPABASE_ANON_KEY || '').trim();
                if (!fallbackUrl || !fallbackKey) return null;
                const sb = createClient(fallbackUrl, fallbackKey);
                await attachSupabaseClient(sb);
                return sb;
            } catch {
                return null;
            } finally {
                ensureSupabasePromiseRef.current = null;
            }
        })();
        return await ensureSupabasePromiseRef.current;
    }, [attachSupabaseClient]);
    const refreshViewerState = useCallback(async () => {
        if (!session) {
            setPlan('none');
            setRole('user');
            setOwnerOverride(false);
            setBillingActive(false);
            setMembershipActive(false);
            setMembershipPlanId('none');
            setMembershipSource('');
            setNextRenewalUnix(0);
            setNextRenewalSource('');
            setBillingAnchorUnix(0);
            setLongformOwnerBeta(false);
            setWaitlistOnlyMode(false);
            setWaitlistRequiresStripePayment(false);
            setMonthlyCreditsRemaining(0);
            setTopupCreditsRemaining(0);
            setCreditsTotalRemaining(0);
            setRequiresTopup(false);
            setStudioLaneAccess({});
            setDemoAccess(false);
            setDemoComingSoon(true);
            return;
        }
        const userEmail = String(session.user?.email || '').trim().toLowerCase();
        const isOwner = isOwnerEmail(userEmail);
        if (backendOffline) {
            if (isOwner) {
                applyOwnerAccess();
            } else {
                setPlan('none');
                setRole('user');
                setOwnerOverride(false);
                setBillingActive(false);
                setMembershipActive(false);
                setMembershipPlanId('none');
                setStudioLaneAccess({});
            }
            return;
        }
        try {
            const res = await fetch(`${API}/api/me`, {
                headers: { Authorization: `Bearer ${session.access_token}` },
            });
            if (!res.ok) throw new Error('Unable to refresh account state');
            const { data } = await readJsonResponse<any>(res);
            if (!data || typeof data !== "object") throw new Error("Invalid /api/me payload");
            const incomingPlan = normalizeViewerPlanId(data.plan, 'free');
            const incomingMembershipPlanId = normalizeViewerPlanId(data.membership_plan_id || incomingPlan, incomingPlan);
            setPlan(incomingPlan);
            setRole(isOwner ? 'admin' : 'user');
            const incomingMembershipActive = Boolean(data.membership_active ?? data.billing_active);
            setBillingActive(incomingMembershipActive);
            setMembershipActive(incomingMembershipActive);
            setMembershipPlanId(incomingMembershipPlanId);
            setMembershipSource(String(data.membership_source || data.next_renewal_source || ''));
            setOwnerOverride(Boolean(data.owner_override || isOwner));
            setNextRenewalUnix(Number(data.next_renewal_unix || 0));
            setNextRenewalSource(String(data.next_renewal_source || ''));
            setBillingAnchorUnix(Number(data.billing_anchor_unix || 0));
            const laneAccess = (data.lane_access && typeof data.lane_access === 'object') ? (data.lane_access as LaneAccessMap) : {};
            setStudioLaneAccess(laneAccess);
            setLongformOwnerBeta(Boolean(data.longform_owner_beta));
            setMonthlyCreditsRemaining(Number(data.included_credits_remaining ?? data.animated_credits_remaining ?? data.monthly_credits_remaining ?? 0));
            setTopupCreditsRemaining(Number(data.credit_wallet_balance ?? data.animated_topup_credits_remaining ?? data.topup_credits_remaining ?? 0));
            setCreditsTotalRemaining(Number(data.animated_credits_total_remaining ?? data.credits_total_remaining ?? 0));
            setRequiresTopup(Boolean(data.requires_topup));
            setDemoAccess(data.demo_access || false);
            if (data.demo_price_id) setDemoPriceId(data.demo_price_id);
            setDemoComingSoon(data.demo_coming_soon !== false);
            if (isOwner) {
                applyOwnerAccess();
            }
        } catch {
            if (isOwner) {
                applyOwnerAccess();
            } else {
                setPlan('none');
                setRole('user');
                setOwnerOverride(false);
                setBillingActive(false);
                setMembershipActive(false);
                setMembershipPlanId('none');
                setMembershipSource('');
            }
            setNextRenewalUnix(0);
            setNextRenewalSource('');
            setBillingAnchorUnix(0);
            if (!isOwner) setLongformOwnerBeta(false);
            setMonthlyCreditsRemaining(0);
            setTopupCreditsRemaining(0);
            setCreditsTotalRemaining(0);
            setRequiresTopup(false);
            if (!isOwner) setStudioLaneAccess({});
            setDemoAccess(false);
            setDemoComingSoon(true);
        }
    }, [session?.access_token, session?.user?.id, backendOffline, applyOwnerAccess, normalizeViewerPlanId]);

    useEffect(() => {
        let cancelled = false;
        (async () => {
            let timeout: ReturnType<typeof setTimeout> | null = null;
            const controller = new AbortController();
            let configLoaded = false;
            let sbCreated = false;
            try {
                timeout = setTimeout(() => controller.abort(), BOOT_CONFIG_TIMEOUT_MS);
                const res = await fetch(`${API}/api/config`, { signal: controller.signal });
                const { data: cfg } = await readJsonResponse<any>(res);
                if (!cfg || typeof cfg !== "object") throw new Error("Invalid config payload");
                if (cancelled) return;
                configLoaded = true;
                if (cfg && typeof cfg === 'object') {
                    // Only overwrite the fallback if the backend returned a non-empty value.
                    // If the backend is unreachable/slow/misconfigured, keep showing the bundled fallback prices + limits.
                    if (cfg.plans && typeof cfg.plans === 'object' && Object.keys(cfg.plans).length > 0) setPublicPlanLimits(cfg.plans as PlanLimitMap);
                    if (cfg.plan_features && typeof cfg.plan_features === 'object' && Object.keys(cfg.plan_features).length > 0) setPublicPlanFeatures(cfg.plan_features as PlanFeatureMap);
                    if (cfg.plan_prices_usd && typeof cfg.plan_prices_usd === 'object' && Object.keys(cfg.plan_prices_usd).length > 0) setPublicPlanPrices(cfg.plan_prices_usd as PlanPriceMap);
                    if (cfg.billing_model && typeof cfg.billing_model === 'object') {
                        const incomingDefaultMembershipPlanId = String((cfg.billing_model as any).default_membership_plan_id || '').trim().toLowerCase();
                        if (incomingDefaultMembershipPlanId) {
                            setDefaultMembershipPlanId(incomingDefaultMembershipPlanId);
                        }
                    }
                    setWaitlistOnlyMode(false);
                    setWaitlistRequiresStripePayment(false);
                }
                setMaintenanceBannerEnabled(Boolean(cfg.maintenance_banner_enabled));
                setMaintenanceBannerMessage((cfg.maintenance_banner_message || "").trim());
                if (Array.isArray(cfg.topup_packs) && cfg.topup_packs.length > 0) {
                    const packs = cfg.topup_packs
                        .filter((p: any) => p && typeof p.price_id === 'string')
                        .map((p: any) => ({
                            price_id: p.price_id,
                            pack: String(p.pack || ''),
                            credits: Number(p.credits || 0),
                            price_usd: Number(p.price_usd || 0),
                        }))
                        .sort((a: TopupPack, b: TopupPack) => a.credits - b.credits);
                    const unifiedModel = String((cfg.billing_model as any)?.model || '').trim() === 'unified_credits';
                    const unifiedPacks = packs.filter((p: TopupPack) => p.price_id.startsWith('uc_'));
                    if (unifiedModel && unifiedPacks.length > 0) {
                        setTopupPacks(unifiedPacks);
                    } else if (unifiedModel) {
                        setTopupPacks(UNIFIED_TOPUP_PACKS);
                    } else if (packs.length > 0) {
                        setTopupPacks(packs);
                    }
                }
                if (cfg.supabase_url && cfg.supabase_anon_key) {
                    const sb = createClient(cfg.supabase_url, cfg.supabase_anon_key);
                    await attachSupabaseClient(sb);
                    sbCreated = true;
                }
            } catch {
                // Backend is offline
                if (!cancelled) setBackendOffline(true);
            }
            // Fallback: if backend was offline or didn't provide supabase creds, use hardcoded fallback
            if (!cancelled && !sbCreated) {
                try {
                    const sb = createClient(FALLBACK_SUPABASE_URL, FALLBACK_SUPABASE_ANON_KEY);
                    await attachSupabaseClient(sb);
                    if (!cancelled) {
                        if (!configLoaded) setBackendOffline(true);
                    }
                } catch {
                    // Supabase also unavailable
                }
            }
            if (timeout) clearTimeout(timeout);
            if (!cancelled) setLoading(false);
        })();
        return () => { cancelled = true; };
    }, [attachSupabaseClient]);
    useEffect(() => {
        return () => {
            try {
                authSubscriptionRef.current?.unsubscribe();
            } catch {
                // ignore auth listener cleanup issues
            }
            authSubscriptionRef.current = null;
        };
    }, []);

    useEffect(() => {
        let cancelled = false;
        const markProbeFailure = () => {
            const now = Date.now();
            healthFailureCountRef.current += 1;
            const hasRecentSuccess =
                lastHealthSuccessAtRef.current > 0 &&
                (now - lastHealthSuccessAtRef.current) <= HEALTH_RECENT_SUCCESS_GRACE_MS;
            if (
                !cancelled &&
                healthFailureCountRef.current >= HEALTH_FAILURE_THRESHOLD &&
                !hasRecentSuccess
            ) {
                setBackendOffline(true);
            }
        };
        const probe = async () => {
            if (isLocalDevHost && !API) return;
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), HEALTH_PROBE_TIMEOUT_MS);
            try {
                const res = await fetch(`${API}/api/health`, { signal: controller.signal });
                if (!res.ok) {
                    markProbeFailure();
                    return;
                }
                const { data } = await readJsonResponse<any>(res);
                if (!data || typeof data !== "object") {
                    markProbeFailure();
                    return;
                }
                const healthStatus = String((data as any).status || "").trim().toLowerCase();
                if (healthStatus && !["online", "ok", "healthy"].includes(healthStatus)) {
                    markProbeFailure();
                    return;
                }
                const skeletonRequiresWan = Boolean((data as any).skeleton_require_wan22);
                const wanReadyField = (data as any).wan22_ready;
                const wanT2IReady = Boolean((data as any).wan22_t2i_ready);
                // WAN T2I is the actual generation path; don't flap offline if generic WAN probe is noisy.
                const wanReady = wanT2IReady || (typeof wanReadyField === "boolean" ? wanReadyField : false);
                const wanLastOkAgoSec = Number((data as any).wan22_t2i_last_ok_ago_sec ?? -1);
                const wanLastError = String((data as any).wan22_t2i_last_error || "").toLowerCase();
                const wanLikelyTransient = wanLastOkAgoSec >= 0 && wanLastOkAgoSec <= 1800;
                const wanLikelyBusy =
                    wanLastError.includes("busy")
                    || wanLastError.includes("queue")
                    || wanLastError.includes("timeout")
                    || wanLastError.includes("resource")
                    || wanLastError.includes("concurrent");
                if (skeletonRequiresWan && !wanReady && !wanLikelyTransient && !wanLikelyBusy) {
                    markProbeFailure();
                    return;
                }
                healthFailureCountRef.current = 0;
                lastHealthSuccessAtRef.current = Date.now();
                if (!cancelled) {
                    setBackendOffline(false);
                }
            } catch {
                markProbeFailure();
            } finally {
                clearTimeout(timeout);
            }
        };
        probe();
        const id = setInterval(probe, HEALTH_PROBE_INTERVAL_MS);
        return () => {
            cancelled = true;
            clearInterval(id);
        };
    }, []);

    useEffect(() => {
        void refreshViewerState();
    }, [refreshViewerState]);

    useEffect(() => {
        if (!session) return;
        const handleFocusRefresh = () => {
            void refreshViewerState();
        };
        const handleVisibilityRefresh = () => {
            if (document.visibilityState === 'visible') {
                void refreshViewerState();
            }
        };
        window.addEventListener('focus', handleFocusRefresh);
        document.addEventListener('visibilitychange', handleVisibilityRefresh);
        return () => {
            window.removeEventListener('focus', handleFocusRefresh);
            document.removeEventListener('visibilitychange', handleVisibilityRefresh);
        };
    }, [session?.access_token, refreshViewerState]);

    const signIn = useCallback(async (email: string, password: string): Promise<string | null> => {
        const sb = supabase || await ensureSupabaseClient();
        if (!sb) return "Auth is still connecting. Try again in a second.";
        pendingAuthIntentRef.current = 'signin';
        const { error } = await sb.auth.signInWithPassword({ email, password });
        if (error) pendingAuthIntentRef.current = '';
        return error ? error.message : null;
    }, [supabase, ensureSupabaseClient]);

    const signInWithGoogle = useCallback(async (): Promise<string | null> => {
        const sb = supabase || await ensureSupabaseClient();
        if (!sb) return "Auth is still connecting. Try again in a second.";
        pendingAuthIntentRef.current = 'google';
        const redirectTo = isLocalDevHost
            ? `${window.location.origin}?page=dashboard&tab=agent`
            : `${STUDIO_SITE_URL}?page=dashboard&tab=agent`;
        const { error } = await sb.auth.signInWithOAuth({
            provider: 'google',
            options: {
                redirectTo,
            },
        });
        if (error) pendingAuthIntentRef.current = '';
        if (!error) return null;
        const message = String(error.message || '').trim();
        if (message.toLowerCase().includes('provider is not enabled')) {
            return 'Google sign-in is unavailable right now. Use email + password on the sign-in page until the Supabase Google provider is turned back on.';
        }
        return message || 'Google sign-in failed';
    }, [supabase, ensureSupabaseClient]);

    const signUp = useCallback(async (email: string, password: string): Promise<string | null> => {
        const sb = supabase || await ensureSupabaseClient();
        if (!sb) return "Auth is still connecting. Try again in a second.";
        pendingAuthIntentRef.current = 'signup';
        const { error } = await sb.auth.signUp({
            email,
            password,
            options: { emailRedirectTo: window.location.origin },
        });
        if (error) pendingAuthIntentRef.current = '';
        return error ? error.message : null;
    }, [supabase, ensureSupabaseClient]);

    const signOut = useCallback(async () => {
        if (supabase) await supabase.auth.signOut();
        pendingAuthIntentRef.current = '';
        lastTrackedSessionUserRef.current = '';
        setSession(null);
        setPlan('none');
        setRole('user');
        setOwnerOverride(false);
        setBillingActive(false);
        setMembershipActive(false);
        setMembershipPlanId('none');
        setMembershipSource('');
        setStudioLaneAccess({});
        setLongformOwnerBeta(false);
        setMonthlyCreditsRemaining(0);
        setTopupCreditsRemaining(0);
        setCreditsTotalRemaining(0);
        setRequiresTopup(false);
    }, [supabase]);

    useEffect(() => {
        const userId = String(session?.user?.id || '').trim();
        if (!userId) {
            lastTrackedSessionUserRef.current = '';
            return;
        }
        if (lastTrackedSessionUserRef.current === userId) return;
        lastTrackedSessionUserRef.current = userId;
        const intent = pendingAuthIntentRef.current;
        if (!intent) return;
        const createdAtMs = Date.parse(String((session as any)?.user?.created_at || ''));
        const lastSignInAtMs = Date.parse(String((session as any)?.user?.last_sign_in_at || ''));
        const isLikelyNewUser =
            Number.isFinite(createdAtMs)
            && Number.isFinite(lastSignInAtMs)
            && Math.abs(lastSignInAtMs - createdAtMs) <= (10 * 60 * 1000);
        trackAuthCompletion(intent, isLikelyNewUser);
        pendingAuthIntentRef.current = '';
    }, [session]);

    const checkout = useCallback(async (planName: string): Promise<string | null> => {
        if (!session) return "Missing membership checkout details";
        const normalizedPlanName = String(planName || '').trim().toLowerCase();
        const isMembershipCheckout = normalizedPlanName === 'membership';
        const targetPlanId = isMembershipCheckout ? (defaultMembershipPlanId || 'studio_pro_1k') : normalizedPlanName;
        const validPlan = [
            'creator',
            'studio',
            'studio_pro_1k',
            'studio_pro_2500',
            'studio_pro_5k',
            'studio_pro_11k',
            'studio_pro_17k',
            'studio_pro_24k',
            'studio_pro_32k',
            'studio_pro_2k',
            'studio_pro_8k',
            'studio_pro_15k',
        ].includes(targetPlanId);
        if (!validPlan) return "Missing membership checkout details";
        try {
            const res = await fetchWithTimeout(resolveStudioBackendUrl('/api/checkout'), {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Authorization: `Bearer ${session.access_token}`,
                },
                body: JSON.stringify(
                    { product: 'membership', plan: targetPlanId }
                ),
            });
            const { data } = await readJsonResponse<any>(res);
            const payload = data || {};
            if (!res.ok) return (payload as any).detail || "Could not start membership checkout";
            if ((payload as any).checkout_url) {
                window.location.href = (payload as any).checkout_url;
                return null;
            }
            return "Checkout URL missing";
        } catch (e) {
            console.error("Checkout failed", e);
            return e instanceof DOMException && e.name === 'AbortError'
                ? "Membership checkout timed out. Try again in a moment."
                : "Membership checkout failed";
        }
    }, [defaultMembershipPlanId, session]);

    const checkoutDemo = useCallback(async () => {
        if (!demoPriceId || !session) return;
        try {
            const res = await fetch(`${API}/api/checkout`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Authorization: `Bearer ${session.access_token}`,
                },
                body: JSON.stringify({ price_id: demoPriceId }),
            });
            const { data } = await readJsonResponse<any>(res);
            if (data?.checkout_url) window.location.href = data.checkout_url;
        } catch (e) { console.error("Demo checkout failed", e); }
    }, [session, demoPriceId]);

    const checkoutTopup = useCallback(async (priceId: string, preferredMethod: 'card' | 'paypal' = 'card'): Promise<string | null> => {
        if (!priceId || !session) return "Missing top-up price";
        try {
            const res = await fetchWithTimeout(resolveStudioBackendUrl('/api/checkout/topup'), {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Authorization: `Bearer ${session.access_token}`,
                },
                body: JSON.stringify({ price_id: priceId, preferred_method: preferredMethod }),
            });
            const { data } = await readJsonResponse<any>(res);
            const payload = data || {};
            if (!res.ok) return (payload as any).detail || "Could not start top-up checkout";
            if ((payload as any).checkout_url) {
                window.location.href = (payload as any).checkout_url;
                return null;
            }
            return "Checkout URL missing";
        } catch (e) {
            console.error("Top-up checkout failed", e);
            return e instanceof DOMException && e.name === 'AbortError'
                ? "Top-up checkout timed out. Try again in a moment."
                : "Top-up checkout failed";
        }
    }, [session]);

    const manageBilling = useCallback(async (): Promise<string | null> => {
        if (!session) return "Not signed in";
        try {
            const res = await fetchWithTimeout(resolveStudioBackendUrl('/api/billing-portal'), {
                method: "POST",
                headers: {
                    Authorization: `Bearer ${session.access_token}`,
                },
            });
            const { data } = await readJsonResponse<any>(res);
            const payload = data || {};
            if (!res.ok) {
                return (payload as any).detail || "Could not open billing portal";
            }
            if ((payload as any).portal_url) {
                window.location.href = (payload as any).portal_url;
                return null;
            }
            return "Billing portal URL missing";
        } catch (e) {
            console.error("Billing portal failed", e);
            return "Billing portal request failed";
        }
    }, [session]);

    const joinWaitingList = useCallback(async (planName: string, priceUsd: number): Promise<string | null> => {
        void planName;
        void priceUsd;
        return "Waiting list has been removed from Studio.";
    }, []);

    const verifyPayPalOrder = useCallback(async (orderId: string): Promise<PayPalVerifyResult> => {
        const trimmed = String(orderId || '').trim();
        if (!trimmed) return { ok: false, captured: false, revoked: false, kind: '', error: 'Missing order id' };
        if (!session) return { ok: false, captured: false, revoked: false, kind: '', error: 'Not signed in' };
        try {
            const res = await fetch(`${API}/api/paypal/verify/${encodeURIComponent(trimmed)}`, {
                headers: { Authorization: `Bearer ${session.access_token}` },
            });
            const { data } = await readJsonResponse<any>(res);
            const payload = data || {};
            if (!res.ok) {
                return { ok: false, captured: false, revoked: false, kind: '', error: String(payload.detail || 'Verify failed') };
            }
            return {
                ok: true,
                captured: Boolean(payload.captured),
                revoked: Boolean(payload.revoked),
                kind: String(payload.kind || ''),
                plan: String(payload.plan || ''),
                credits: Number(payload.credits || 0),
            };
        } catch (e: any) {
            return { ok: false, captured: false, revoked: false, kind: '', error: String(e?.message || 'Verify failed') };
        }
    }, [session]);

    return (
        <AuthContext.Provider value={{
            session, supabase, plan, role, ownerOverride, loading, billingActive, membershipActive, membershipPlanId,
            membershipSource,
            backendOffline,
            nextRenewalUnix, nextRenewalSource,
            billingAnchorUnix,
            longformOwnerBeta,
            monthlyCreditsRemaining, topupCreditsRemaining, creditsTotalRemaining, requiresTopup, topupPacks,
            demoAccess, demoPriceId, demoComingSoon,
            publicPlanLimits, publicPlanFeatures, publicPlanPrices, studioLaneAccess, defaultMembershipPlanId,
            maintenanceBannerEnabled, maintenanceBannerMessage,
            waitlistOnlyMode, waitlistRequiresStripePayment,
            signIn, signInWithGoogle, signUp, signOut, checkout, checkoutTopup, checkoutDemo, manageBilling,
            joinWaitingList, verifyPayPalOrder,
        }}>
            {children}
        </AuthContext.Provider>
    );
}
