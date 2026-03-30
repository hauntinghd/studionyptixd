import { useState, useEffect, createContext, useCallback, useRef } from 'react';
import { createClient, Session, SupabaseClient } from '@supabase/supabase-js';

const viteEnv = ((import.meta as any).env || {}) as Record<string, string>;
const isLocalDevHost = window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1";
const billingHostAliases = new Set(["billing.nyptidindustries.com", "billing.niptidindustries.com"]);
export const isBillingHost = billingHostAliases.has(window.location.hostname.toLowerCase()) || window.location.hostname.toLowerCase().startsWith("billing.");
export const BILLING_SITE_URL = "https://billing.nyptidindustries.com";
export const STUDIO_SITE_URL = "https://studio.nyptidindustries.com";
export const PROD_API_BASE_URL = "https://api.nyptidindustries.com";
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
// - hosted UI (Vercel): optionally point to a separate backend domain via VITE_PROD_API_BASE_URL
const rawLocalApi = resolveSafeApiBase(viteEnv.VITE_API_BASE_URL || "");
const rawProdApi = resolveSafeApiBase(viteEnv.VITE_PROD_API_BASE_URL || "");
export const API = isLocalDevHost ? rawLocalApi : (rawProdApi || PROD_API_BASE_URL);
const rawGenerationApi = resolveSafeApiBase(
    (isLocalDevHost ? viteEnv.VITE_GENERATION_API_BASE_URL : viteEnv.VITE_PROD_GENERATION_API_BASE_URL) || ""
);
const FIREFOX_HOTFIX_TAG = "ff-hotfix-1";
const BOOT_CONFIG_TIMEOUT_MS = 12000;
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
        return API || (isLocalDevHost ? `${window.location.protocol}//${window.location.hostname}:8091` : PROD_API_BASE_URL);
    }
    return rawGenerationApi;
})();
if (typeof window !== "undefined" && /firefox/i.test(window.navigator.userAgent)) {
    (window as any).__NYPTID_FIREFOX_HOTFIX__ = FIREFOX_HOTFIX_TAG;
}
export const CREATE_WORKFLOW_PERSISTENCE_ENABLED = true;
export const PUBLIC_TEMPLATE_IDS = new Set([
    'story',
    'motivation',
    'skeleton',
    'chatstory',
]);
export const CHAT_STORY_MONTHLY_PLAN_IDS = new Set(['starter', 'creator', 'pro']);
export const hasChatStoryTemplateAccess = (
    planName: string | null | undefined,
    billingActive: boolean,
    role?: string | null
): boolean => {
    void planName;
    void billingActive;
    void role;
    return true;
};
export const CLONE_COMING_SOON = true;
export const Logo = ({ size = 24 }: { size?: number }) => (
    <img src="/logo.png" alt="NYPTID" width={size} height={size} className="rounded-full" />
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
  plan text not null check (plan in ('starter','creator','pro','elite')),
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

const normalizeWaitlistPlan = (planName: string): string => {
    const normalized = String(planName || "").trim().toLowerCase();
    if (["starter", "creator", "pro", "elite"].includes(normalized)) return normalized;
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

export type Plan = 'none' | 'starter' | 'creator' | 'pro' | 'elite';
export type TopupPack = { price_id: string; pack: string; credits: number; price_usd: number };
export type PlanLimit = {
    videos_per_month?: number;
    animated_renders_per_month?: number;
    non_animated_ops_per_month?: number;
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
const PRICE_IDS: Record<string, string> = {
    starter: "price_1T4eT7BL8lRmwao2hHcUbcny",
    creator: "price_1T4eTUBL8lRmwao2EK3JDOpy",
    pro: "price_1T4eTjBL8lRmwao2q6WkoZLH",
    elite: "price_1T9uMwBL8lRmwao2Lk89pxiz",
};

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
    signUp: (email: string, password: string) => Promise<string | null>;
    signOut: () => Promise<void>;
    checkout: (plan: string) => Promise<string | null>;
    checkoutTopup: (priceId: string, preferredMethod?: 'card' | 'paypal') => Promise<string | null>;
    checkoutDemo: () => Promise<void>;
    manageBilling: () => Promise<string | null>;
    joinWaitingList: (plan: string, priceUsd: number) => Promise<string | null>;
}

export const AuthContext = createContext<AuthContextType>({
    session: null, supabase: null, plan: 'none', role: 'user', ownerOverride: false, loading: true, billingActive: false, membershipActive: false, membershipPlanId: 'none',
    membershipSource: '',
    backendOffline: false,
    nextRenewalUnix: 0, nextRenewalSource: '',
    billingAnchorUnix: 0,
    monthlyCreditsRemaining: 0, topupCreditsRemaining: 0, creditsTotalRemaining: 0, requiresTopup: false, topupPacks: [],
    demoAccess: false, demoPriceId: '', demoComingSoon: true, publicPlanLimits: {}, publicPlanFeatures: {}, publicPlanPrices: {}, studioLaneAccess: {}, defaultMembershipPlanId: 'starter',
    maintenanceBannerEnabled: false, maintenanceBannerMessage: '',
    longformOwnerBeta: false,
    waitlistOnlyMode: false,
    waitlistRequiresStripePayment: false,
    signIn: async () => null, signUp: async () => null, signOut: async () => {},
    checkout: async () => null, checkoutTopup: async () => null, checkoutDemo: async () => {}, manageBilling: async () => null,
    joinWaitingList: async () => null,
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
    const [topupPacks, setTopupPacks] = useState<TopupPack[]>([]);
    const [demoAccess, setDemoAccess] = useState(false);
    const [demoPriceId, setDemoPriceId] = useState('');
    const [demoComingSoon, setDemoComingSoon] = useState(true);
    const [publicPlanLimits, setPublicPlanLimits] = useState<PlanLimitMap>({});
    const [publicPlanFeatures, setPublicPlanFeatures] = useState<PlanFeatureMap>({});
    const [publicPlanPrices, setPublicPlanPrices] = useState<PlanPriceMap>({});
    const [studioLaneAccess, setStudioLaneAccess] = useState<LaneAccessMap>({});
    const [defaultMembershipPlanId, setDefaultMembershipPlanId] = useState('starter');
    const [maintenanceBannerEnabled, setMaintenanceBannerEnabled] = useState(false);
    const [maintenanceBannerMessage, setMaintenanceBannerMessage] = useState('');
    const [longformOwnerBeta, setLongformOwnerBeta] = useState(false);
    const [waitlistOnlyMode, setWaitlistOnlyMode] = useState(false);
    const [waitlistRequiresStripePayment, setWaitlistRequiresStripePayment] = useState(false);
    const healthFailureCountRef = useRef(0);
    const lastHealthSuccessAtRef = useRef(0);
    const ownerLaneAccess: LaneAccessMap = {
        create: true,
        thumbnails: true,
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
        setPlan('elite');
        setBillingActive(true);
        setMembershipActive(true);
        setMembershipPlanId(defaultMembershipPlanId || 'starter');
        setMembershipSource('admin');
        setOwnerOverride(true);
        setStudioLaneAccess(ownerLaneAccess);
        setLongformOwnerBeta(true);
    }, [defaultMembershipPlanId]);
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
            const incomingPlan = (data.plan === 'free' ? 'none' : data.plan) || 'none';
            setPlan((['none', 'starter', 'creator', 'pro', 'elite'].includes(incomingPlan) ? incomingPlan : 'none') as Plan);
            setRole(isOwner ? 'admin' : 'user');
            const incomingMembershipActive = Boolean(data.membership_active ?? data.billing_active);
            setBillingActive(incomingMembershipActive);
            setMembershipActive(incomingMembershipActive);
            setMembershipPlanId(String(data.membership_plan_id || incomingPlan || 'none'));
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
    }, [session, backendOffline, applyOwnerAccess]);

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
                    if (cfg.plans && typeof cfg.plans === 'object') setPublicPlanLimits(cfg.plans as PlanLimitMap);
                    if (cfg.plan_features && typeof cfg.plan_features === 'object') setPublicPlanFeatures(cfg.plan_features as PlanFeatureMap);
                    if (cfg.plan_prices_usd && typeof cfg.plan_prices_usd === 'object') setPublicPlanPrices(cfg.plan_prices_usd as PlanPriceMap);
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
                if (Array.isArray(cfg.topup_packs)) {
                    const packs = cfg.topup_packs
                        .filter((p: any) => p && typeof p.price_id === 'string')
                        .map((p: any) => ({
                            price_id: p.price_id,
                            pack: String(p.pack || ''),
                            credits: Number(p.credits || 0),
                            price_usd: Number(p.price_usd || 0),
                        }))
                        .sort((a: TopupPack, b: TopupPack) => a.credits - b.credits);
                    setTopupPacks(packs);
                }
                if (cfg.supabase_url && cfg.supabase_anon_key) {
                    const sb = createClient(cfg.supabase_url, cfg.supabase_anon_key);
                    setSupabase(sb);
                    sbCreated = true;
                    const { data: { session: s } } = await sb.auth.getSession();
                    if (!cancelled) {
                        setSession(s);
                        sb.auth.onAuthStateChange((_e, s) => setSession(s));
                    }
                }
            } catch {
                // Backend is offline
                if (!cancelled) setBackendOffline(true);
            }
            // Fallback: if backend was offline or didn't provide supabase creds, use hardcoded fallback
            if (!cancelled && !sbCreated) {
                try {
                    const sb = createClient(FALLBACK_SUPABASE_URL, FALLBACK_SUPABASE_ANON_KEY);
                    setSupabase(sb);
                    const { data: { session: s } } = await sb.auth.getSession();
                    if (!cancelled) {
                        setSession(s);
                        sb.auth.onAuthStateChange((_e, s) => setSession(s));
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
    }, [session, refreshViewerState]);

    const signIn = useCallback(async (email: string, password: string): Promise<string | null> => {
        if (!supabase) return "Auth not configured yet";
        const { error } = await supabase.auth.signInWithPassword({ email, password });
        return error ? error.message : null;
    }, [supabase]);

    const signUp = useCallback(async (email: string, password: string): Promise<string | null> => {
        if (!supabase) return "Auth not configured yet";
        const { error } = await supabase.auth.signUp({
            email,
            password,
            options: { emailRedirectTo: window.location.origin },
        });
        return error ? error.message : null;
    }, [supabase]);

    const signOut = useCallback(async () => {
        if (supabase) await supabase.auth.signOut();
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

    const checkout = useCallback(async (planName: string): Promise<string | null> => {
        if (!session) return "Missing membership checkout details";
        const normalizedPlanName = String(planName || '').trim().toLowerCase();
        const isMembershipCheckout = normalizedPlanName === 'membership';
        const targetPlanId = isMembershipCheckout ? (defaultMembershipPlanId || 'starter') : normalizedPlanName;
        const priceId = PRICE_IDS[targetPlanId];
        if (!isMembershipCheckout && !priceId) return "Missing membership checkout details";
        try {
            const res = await fetch(`${API}/api/checkout`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Authorization: `Bearer ${session.access_token}`,
                },
                body: JSON.stringify(
                    isMembershipCheckout
                        ? { product: 'membership', plan: targetPlanId }
                        : { price_id: priceId, plan: targetPlanId }
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
            return "Membership checkout failed";
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

    const checkoutTopup = useCallback(async (priceId: string, preferredMethod: 'card' | 'paypal' = 'paypal'): Promise<string | null> => {
        if (!priceId || !session) return "Missing top-up price";
        try {
            const res = await fetch(`${API}/api/checkout/topup`, {
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
            return "Top-up checkout failed";
        }
    }, [session]);

    const manageBilling = useCallback(async (): Promise<string | null> => {
        if (!session) return "Not signed in";
        try {
            const res = await fetch(`${API}/api/billing-portal`, {
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
            signIn, signUp, signOut, checkout, checkoutTopup, checkoutDemo, manageBilling,
            joinWaitingList,
        }}>
            {children}
        </AuthContext.Provider>
    );
}
