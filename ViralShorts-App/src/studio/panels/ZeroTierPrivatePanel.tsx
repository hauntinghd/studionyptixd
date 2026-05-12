/**
 * ZeroTierPrivatePanel — Catalyst-powered ZeroTier (Private) niche surface.
 *
 * Owner-only panel that wraps the existing Catalyst hub endpoints filtered
 * to Casey's ZeroTier channel (UC9Gth_4MVet6rdPH7MHJf-g). Rendered from
 * DashboardPage when selectedNiche === 'zerotier_private'.
 *
 * Backend reuse:
 *   GET  /api/catalyst/hub?channel_id=...    → channel snapshot + audit
 *   POST /api/catalyst/hub/refresh           → re-sync
 *
 * The panel surfaces:
 *   - Channel header + total stats
 *   - Recent uploads sorted by like-rate (highest first)
 *   - Catalyst's `channel_audit.next_video_candidates` (recommended topics)
 *   - "Build This Short" buttons that route to the build flow with the
 *     candidate title prefilled (Phase 2 wires the zerotier_private template).
 */
import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { AlertTriangle, Download, Film, Image as ImageIcon, Lightbulb, Loader2, RefreshCw, Sparkles, TrendingUp, X, Youtube, Zap } from 'lucide-react';
import { API, AuthContext, startYouTubeBrowserConnect } from '../shared';

const ZEROTIER_CHANNEL_ID = 'UC9Gth_4MVet6rdPH7MHJf-g';

// PR #143 — bundle title + description + tags into a single plain-text
// blob and trigger a browser download to Casey's Downloads folder.
// Casey's flow: render finishes → metadata auto-generates → this fires
// → a `.txt` lands in Downloads alongside the MP4 he's about to upload.
// The clipboard copy buttons remain as the per-field fallback.
function _slugifyForFilename(s: string, max = 60): string {
    const cleaned = (s || '')
        .normalize('NFKD')
        .replace(/[̀-ͯ]/g, '')   // strip combining marks
        .replace(/[^A-Za-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '')
        .slice(0, max);
    return cleaned || 'untitled';
}
function downloadMetadataAsTxt(
    title: string,
    description: string,
    tags: string[],
    jobId: string,
): void {
    if (typeof document === 'undefined' || typeof window === 'undefined') return;
    const cleanTitle = (title || '').trim();
    const cleanDesc = (description || '').trim();
    const cleanTags = (tags || []).map((t) => String(t).trim()).filter(Boolean);

    const lines: string[] = [];
    lines.push('=== TITLE ===');
    lines.push(cleanTitle);
    lines.push('');
    lines.push('=== DESCRIPTION ===');
    lines.push(cleanDesc || '(none)');
    lines.push('');
    lines.push('=== TAGS (comma-separated, paste into YouTube Studio) ===');
    lines.push(cleanTags.join(', '));
    lines.push('');
    lines.push(`# ZeroTier short — job ${jobId} — generated ${new Date().toISOString()}`);
    const text = lines.join('\n');

    const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    const slug = _slugifyForFilename(cleanTitle || `ZeroTier_${jobId}`);
    a.href = url;
    a.download = `ZeroTier_${slug}_${jobId}.txt`;
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    // Defer revoke + remove so the click event has fully dispatched.
    setTimeout(() => {
        try { document.body.removeChild(a); } catch { /* ignored */ }
        try { URL.revokeObjectURL(url); } catch { /* ignored */ }
    }, 1500);
}

type UploadedVideo = {
    video_id?: string;
    title?: string;
    published_at?: string;
    thumbnail_url?: string;
    views?: number;
    likes?: number;
    comments?: number;
    duration_sec?: number;
    privacy_status?: string;
    average_view_percentage?: number;
};

type ChannelAudit = {
    summary?: string;
    honesty_note?: string;
    measured_facts?: string[];
    inferred_notes?: string[];
    limitations?: string[];
    strengths?: string[];
    warnings?: string[];
    next_moves?: string[];
    next_video_candidates?: string[];
    strongest_arc?: string;
    weakest_arc?: string;
    best_recent_title?: string;
    worst_recent_title?: string;
};

type CatalystChannel = {
    channel_id: string;
    title: string;
    channel_handle?: string;
    subscriber_count?: number;
    video_count?: number;
    view_count?: number;
    last_sync_error?: string;
    needs_reconnect?: boolean;
    analytics_snapshot?: {
        channel_summary?: string;
        recent_upload_titles?: string[];
        uploaded_videos?: UploadedVideo[];
        top_video_titles?: string[];
        channel_audit?: ChannelAudit;
    };
};

type CatalystHubPayload = {
    ok?: boolean;
    selected_channel_id?: string;
    selected_channel?: CatalystChannel;
    channels?: CatalystChannel[];
};

function formatNum(n: number | undefined): string {
    if (typeof n !== 'number' || !isFinite(n)) return '—';
    if (n >= 1000000) return `${(n / 1000000).toFixed(1)}M`;
    if (n >= 1000) return `${(n / 1000).toFixed(1)}K`;
    return String(n);
}

function likeRate(v: UploadedVideo): number | null {
    if (!v.views || !v.likes) return null;
    if (v.views < 50) return null;
    return (v.likes / v.views) * 100;
}

// ────────────────────────────────────────────────────────────────────
// Phase 2c: client-side virality scorer.
//
// Heuristic v1 — runs in the browser, zero round-trip cost. Encodes the
// patterns decoded from this channel's actual upload performance:
//   - "Stole Speed Force From Every Speedster" (3.54% LR — channel best)
//   - "Outran A Black Hole" (3.07%)
//   - "Made Superman Admit He Was Slower" (2.74%)
// vs the underperformers (Speed Force Erasure 1.59%, Saved 5,000 1.92%).
//
// Signals weighted by observed correlation with high like-rate:
//   + past-tense "The Time Wally West [verb]" formula
//   + cosmic / identity / mortality stakes
//   + iconic-character confrontation (Superman, Reverse Flash, Crisis)
//   - "every / ranked / vs every" power-scaling rankers
//   - already-uploaded topics
//
// Future Phase 3 replaces this with a Grok-scored predictor that learns
// from logged predictions vs actual outcomes harvested by Catalyst.
// ────────────────────────────────────────────────────────────────────

const TITLE_FORMULA_RE = /\bthe\s+time\s+wally\s+west\b/i;

const PAST_TENSE_VERBS = [
    'outran', 'stole', 'saved', 'made', 'refused', 'forgave', 'beat', 'broke',
    'caught', 'crossed', 'died', 'erased', 'escaped', 'faced', 'fought', 'gave',
    'gained', 'grew', 'held', 'killed', 'lost', 'lived', 'met', 'moved', 'opened',
    'overpowered', 'overtook', 'ran', 'reached', 'returned', 'rewrote', 'rewound',
    'saved', 'sealed', 'shattered', 'silenced', 'sprinted', 'survived',
    'time-traveled', 'time traveled', 'took', 'traveled', 'unmade', 'unwound',
    'walked', 'won', 'broke', 'remade', 'admitted', 'denied', 'protected',
];

const COSMIC_STAKES = [
    'black hole', 'multiverse', 'crisis', 'speed force', 'anti-monitor',
    'big bang', 'spectre', 'infinity', 'universe', 'reality', 'time',
    'event horizon', 'singularity', 'cosmos', 'paradox', 'timeline',
];

const IDENTITY_MORTALITY = [
    'death', 'daughter', 'son', 'wife', 'linda', 'funeral', 'born', 'age',
    'mortality', 'forgiveness', 'memory', 'forgot', 'remembered', 'sacrifice',
    'identity', 'name', 'family',
];

const ICONIC_CHARACTERS = [
    'superman', 'batman', 'flash', 'wonder woman', 'green lantern', 'aquaman',
    'reverse flash', 'eobard', 'thawne', 'zoom', 'black flash', 'savitar',
    'godspeed', 'barry', 'jay', 'bart', 'kid flash', 'professor zoom',
    'darkseid', 'doomsday', 'lex luthor', 'lex',
];

const RANKER_PENALTY = [
    'every speedster', 'ranking', 'vs every', 'tier list', 'ranked',
    'every time', 'all of', 'every villain',
];

interface ViralityScore {
    score: number;          // 0-100
    predicted_lr: number;   // predicted like-rate %
    reasons: string[];      // up to 3 short bullets explaining the score
    novelty_ok: boolean;    // false if title duplicates an already-uploaded topic
}

function scoreVirality(title: string, existingTitles: string[]): ViralityScore {
    const t = (title || '').trim();
    const tLow = t.toLowerCase();
    const reasons: string[] = [];
    let score = 30; // neutral baseline

    // Title formula
    if (TITLE_FORMULA_RE.test(t)) {
        score += 25;
        reasons.push('"The Time Wally West" formula matches');
    } else {
        score -= 10;
        reasons.push('Title format misses past-tense story formula');
    }

    // Past-tense verb in opening clause
    const hasPastTense = PAST_TENSE_VERBS.some((v) => new RegExp(`\\b${v.replace(/\s+/g, '\\s+')}\\b`, 'i').test(tLow));
    if (hasPastTense) {
        score += 12;
        reasons.push('Past-tense verb signals story (not tier list)');
    }

    // Cosmic stakes
    const cosmicMatch = COSMIC_STAKES.find((kw) => tLow.includes(kw));
    if (cosmicMatch) {
        score += 15;
        reasons.push(`Cosmic stakes ("${cosmicMatch}")`);
    }

    // Identity / mortality
    const identityMatch = IDENTITY_MORTALITY.find((kw) => tLow.includes(kw));
    if (identityMatch) {
        score += 10;
        reasons.push(`Identity/mortality hook ("${identityMatch}")`);
    }

    // Iconic confrontation
    const iconicMatch = ICONIC_CHARACTERS.find((kw) => tLow.includes(kw));
    if (iconicMatch) {
        score += 5;
        reasons.push(`Iconic confrontation ("${iconicMatch}")`);
    }

    // Ranker penalty
    const rankerMatch = RANKER_PENALTY.find((kw) => tLow.includes(kw));
    if (rankerMatch) {
        score -= 18;
        reasons.push(`Ranker format underperforms ("${rankerMatch}")`);
    }

    // Novelty — if title overlaps with an existing upload (>=4 word match)
    const tWords = new Set(tLow.split(/\W+/).filter((w) => w.length >= 5));
    let overlapMax = 0;
    for (const existing of existingTitles) {
        const eWords = new Set(String(existing || '').toLowerCase().split(/\W+/).filter((w) => w.length >= 5));
        let common = 0;
        for (const w of tWords) if (eWords.has(w)) common++;
        if (common > overlapMax) overlapMax = common;
    }
    const novelty_ok = overlapMax < 4;
    if (novelty_ok) {
        score += 12;
        reasons.push('Topic not already covered');
    } else {
        score -= 18;
        reasons.push(`Overlaps existing upload (${overlapMax} word matches)`);
    }

    // Clamp + estimate predicted like-rate. Channel baseline ~2.2%, top
    // performer 3.54%. Linear-ish mapping: score 30→1.5%, 60→2.5%, 80→3.2%, 95→3.8%.
    const clamped = Math.max(0, Math.min(100, score));
    const predicted_lr = +(1.0 + (clamped / 100) * 3.0).toFixed(2);

    // Keep the top 3 most-impactful reasons.
    return { score: clamped, predicted_lr, reasons: reasons.slice(0, 3), novelty_ok };
}

export default function ZeroTierPrivatePanel() {
    const { session, supabase } = useContext(AuthContext);
    const accessToken = session?.access_token || '';

    // Always-fresh token getter for long-running polls. Supabase auto-refreshes
    // on getSession() if the token is near expiry, so we can survive a 5-12min
    // background-job poll even after the original closure-captured token has
    // expired (was the cause of the "Authentication required" timeout Casey
    // hit on /render-finalize after 720s).
    const getFreshToken = useCallback(async (): Promise<string> => {
        if (!supabase) return accessToken;
        try {
            const { data } = await supabase.auth.getSession();
            return data?.session?.access_token || accessToken;
        } catch {
            return accessToken;
        }
    }, [supabase, accessToken]);

    // Resilient fetch+JSON helper. Fly cold-starts return 502 with an HTML
    // body which throws on r.json(); retry up to N times with backoff before
    // surfacing the error. Used for all single-shot calls (initial POSTs,
    // /state lookups, etc.) — long-running polls have their own retry logic.
    const fetchJsonResilient = useCallback(async (
        url: string,
        init: RequestInit,
        { retries = 3, retryDelayMs = 2500 }: { retries?: number; retryDelayMs?: number } = {},
    ): Promise<{ ok: boolean; status: number; data: any }> => {
        let lastErr: string = '';
        for (let attempt = 0; attempt <= retries; attempt++) {
            try {
                const r = await fetch(url, init);
                // 502/504 → likely Fly cold-start; backoff + retry
                if ((r.status === 502 || r.status === 504) && attempt < retries) {
                    lastErr = `cold-start ${r.status}`;
                    await new Promise((res) => setTimeout(res, retryDelayMs * Math.pow(1.5, attempt)));
                    continue;
                }
                const text = await r.text();
                let data: any = null;
                try {
                    data = text ? JSON.parse(text) : null;
                } catch {
                    // Non-JSON body (HTML error page, etc.). Treat as transient
                    // if we have retries left, hard-fail otherwise.
                    if (attempt < retries) {
                        lastErr = `non-JSON response (${r.status}): ${text.slice(0, 80)}`;
                        await new Promise((res) => setTimeout(res, retryDelayMs * Math.pow(1.5, attempt)));
                        continue;
                    }
                    return { ok: false, status: r.status, data: { detail: lastErr || `Server returned non-JSON (${r.status})` } };
                }
                return { ok: r.ok, status: r.status, data };
            } catch (e: any) {
                lastErr = String(e?.message || e || 'Network error');
                if (attempt < retries) {
                    await new Promise((res) => setTimeout(res, retryDelayMs * Math.pow(1.5, attempt)));
                    continue;
                }
                throw e;
            }
        }
        throw new Error(lastErr || 'Request failed after retries');
    }, []);

    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [connecting, setConnecting] = useState(false);
    const [error, setError] = useState('');
    const [payload, setPayload] = useState<CatalystHubPayload | null>(null);
    const retriedRef = useRef(false);

    // Build-This-Short modal state
    const [buildModalOpen, setBuildModalOpen] = useState(false);
    const [buildTopic, setBuildTopic] = useState('');
    const [buildTopicScore, setBuildTopicScore] = useState<ViralityScore | null>(null);
    const [buildScript, setBuildScript] = useState('');
    const [buildError, setBuildError] = useState('');
    const [buildLoading, setBuildLoading] = useState(false);
    // Manual topic input (always available, doesn't require Catalyst candidates)
    const [manualTopic, setManualTopic] = useState('');

    // Phase 4: autonomous topic generation — Grok-generated topics from your
    // own channel data + competitor patterns, no manual input required.
    const [genLoading, setGenLoading] = useState(false);
    const [genTopics, setGenTopics] = useState<string[]>([]);
    const [genError, setGenError] = useState('');
    const [genBaseline, setGenBaseline] = useState<{ channel_top_lr?: number; channel_avg_lr?: number; uploads_considered?: number } | null>(null);
    // PR #144 — Catalyst Learning Loop visibility. Surfaces the HIT/MISS
    // pattern buckets the backend extracted from THIS channel's actual
    // uploads. So Casey can SEE what Catalyst has learned, not just trust it.
    interface PatternBucket {
        name: string;
        n: number;
        avg_lr: number;
        delta_vs_baseline: number;
        examples: string[];
    }
    const [genCalibration, setGenCalibration] = useState<null | {
        channel_baseline_lr?: number;
        buckets?: PatternBucket[];
    }>(null);
    // Phase 2b: kept for the legacy 'monolithic render' path. Phase 4.5
    // splits this into stills + finalize stages, but the renderResult shape
    // is reused for both.
    const [renderLoading] = useState(false);  // legacy flag, always false
    const [renderResult, setRenderResult] = useState<null | {
        job_id: string;
        title?: string;
        mp4_url?: string;
        scene_count?: number;
        duration_total_sec?: number;
        fal_cost_estimate_usd?: number;
        // PR #142 — YouTube metadata. Populated by /generate-metadata
        // after a successful finalize, or hydrated from result.json
        // when resuming a finished job.
        description?: string;
        tags?: string[];
    }>(null);
    // PR #142 — YouTube metadata generator state
    const [metadataLoading, setMetadataLoading] = useState(false);
    const [metadataError, setMetadataError] = useState<string>('');
    // Toast-y "Copied!" indicator keyed by which field was copied
    const [copiedField, setCopiedField] = useState<'title' | 'description' | 'tags' | ''>('');

    // Phase 4.5: per-scene approval flow
    interface ScenePreview {
        scene_index: number;
        scene_id: string;
        caption: string;
        narration: string;
        duration: number;
        still_url: string;
        visual_prompt_preview?: string;
        regenerating?: boolean;
    }
    const [stillsLoading, setStillsLoading] = useState(false);
    const [scenePreviews, setScenePreviews] = useState<ScenePreview[]>([]);
    const [stillsJobId, setStillsJobId] = useState<string>('');
    const [finalizeLoading, setFinalizeLoading] = useState(false);

    // Phase 3: predictions calibration data
    interface PredictionRow {
        job_id?: string;
        ts?: number;
        title?: string;
        topic?: string;
        predicted_score?: number;
        predicted_like_rate?: number;
        matched?: boolean;
        video_id?: string | null;
        actual_views?: number | null;
        actual_likes?: number | null;
        actual_like_rate?: number | null;
        delta_lr?: number | null;
    }
    const [predictions, setPredictions] = useState<PredictionRow[]>([]);

    // Phase 4.5c: list of past jobs the user can resume
    interface RecentJob {
        job_id: string;
        title?: string;
        topic?: string;
        stage?: string;
        scene_count?: number;
        predicted_score?: number;
        predicted_like_rate?: number;
        duration_total_sec?: number;
        fal_cost_estimate_usd?: number;
        thumbnail_url?: string | null;
        mp4_url?: string | null;
        updated_at?: number;
    }
    const [recentJobs, setRecentJobs] = useState<RecentJob[]>([]);

    const channel = useMemo(() => {
        const channels = payload?.channels || [];
        return channels.find((c) => c.channel_id === ZEROTIER_CHANNEL_ID) || payload?.selected_channel || channels[0];
    }, [payload]);

    const snapshot = channel?.analytics_snapshot;
    const audit = snapshot?.channel_audit;
    const candidates = audit?.next_video_candidates || [];

    const existingTitles = useMemo(
        () => (snapshot?.uploaded_videos || []).map((v) => v.title || '').filter(Boolean),
        [snapshot],
    );

    // Phase 2c: score every Catalyst-recommended candidate, sort descending.
    const scoredCandidates = useMemo(
        () => candidates
            .map((title) => ({ title, ...scoreVirality(title, existingTitles) }))
            .sort((a, b) => b.score - a.score),
        [candidates, existingTitles],
    );

    // Live score for the manual-topic input (so user sees the score as they type)
    const manualScore = useMemo(
        () => manualTopic.trim() ? scoreVirality(manualTopic.trim(), existingTitles) : null,
        [manualTopic, existingTitles],
    );

    // Score every Grok-generated topic on the client (heuristic v1)
    const scoredGenTopics = useMemo(
        () => genTopics
            .map((title) => ({ title, ...scoreVirality(title, existingTitles) }))
            .sort((a, b) => b.score - a.score),
        [genTopics, existingTitles],
    );

    const sortedUploads = useMemo<UploadedVideo[]>(() => {
        const list = (snapshot?.uploaded_videos || []).slice();
        list.sort((a, b) => {
            const ra = likeRate(a);
            const rb = likeRate(b);
            if (ra === null && rb === null) return (b.views || 0) - (a.views || 0);
            if (ra === null) return 1;
            if (rb === null) return -1;
            return rb - ra;
        });
        return list;
    }, [snapshot]);

    const fetchHub = useCallback(async (): Promise<void> => {
        if (!accessToken) return;
        setError('');
        setLoading(true);
        try {
            const url = `${API}/api/catalyst/hub?channel_id=${encodeURIComponent(ZEROTIER_CHANNEL_ID)}`;
            const r = await fetch(url, {
                headers: { Authorization: `Bearer ${accessToken}` },
            });
            const data = await r.json();
            if (!r.ok) throw new Error(String(data?.detail || data?.error || `Failed (${r.status})`));
            setPayload(data as CatalystHubPayload);
            retriedRef.current = false;
        } catch (e: any) {
            const msg = String(e?.message || e || 'Failed to load');
            if (!retriedRef.current) {
                retriedRef.current = true;
                setTimeout(() => void fetchHub(), 1500);
            } else {
                setError(msg);
            }
        } finally {
            setLoading(false);
        }
    }, [accessToken]);

    const handleRefresh = useCallback(async (): Promise<void> => {
        if (!accessToken) return;
        setError('');
        setRefreshing(true);
        try {
            // Step 1: force a fresh YouTube fetch + persist for ALL connected
            // channels (this is where the OAuth-authenticated path actually
            // pulls likes/comments/CTR and writes them into the connection
            // store). /api/catalyst/hub/refresh alone does NOT trigger this —
            // it only rebuilds the hub payload from already-cached state.
            const ytRes = await fetch(`${API}/api/youtube/channels?sync=true`, {
                headers: { Authorization: `Bearer ${accessToken}` },
            });
            if (!ytRes.ok) {
                console.warn(`[ZT] /api/youtube/channels?sync=true returned ${ytRes.status} — continuing to hub refresh anyway`);
            }
            // Step 2: rebuild the Catalyst hub payload from the freshly-synced
            // connection-store data.
            const r = await fetch(`${API}/api/catalyst/hub/refresh`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${accessToken}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    channel_id: ZEROTIER_CHANNEL_ID,
                    include_public_benchmarks: true,
                    refresh_outcomes: true,
                }),
            });
            const data = await r.json();
            if (!r.ok) throw new Error(String(data?.detail || data?.error || `Refresh failed (${r.status})`));
            setPayload(data as CatalystHubPayload);
        } catch (e: any) {
            setError(String(e?.message || e || 'Refresh failed'));
        } finally {
            setRefreshing(false);
        }
    }, [accessToken]);

    useEffect(() => {
        void fetchHub();
    }, [fetchHub]);

    // Phase 3: fetch predictions log on mount + after every Sync
    const fetchPredictions = useCallback(async () => {
        if (!accessToken) return;
        try {
            const r = await fetch(`${API}/api/zerotier-private/predictions?limit=20`, {
                headers: { Authorization: `Bearer ${accessToken}` },
            });
            const data = await r.json();
            if (r.ok) {
                setPredictions(Array.isArray(data?.predictions) ? data.predictions : []);
            }
        } catch {
            // non-blocking — predictions are nice-to-have
        }
    }, [accessToken]);

    // Phase 4.5c: fetch list of past renders (for resume + history view)
    const fetchRecentJobs = useCallback(async () => {
        if (!accessToken) return;
        try {
            const r = await fetch(`${API}/api/zerotier-private/jobs?limit=12`, {
                headers: { Authorization: `Bearer ${accessToken}` },
            });
            const data = await r.json();
            if (r.ok) {
                setRecentJobs(Array.isArray(data?.jobs) ? data.jobs : []);
            }
        } catch {
            // non-blocking
        }
    }, [accessToken]);

    useEffect(() => {
        void fetchPredictions();
        void fetchRecentJobs();
    }, [fetchPredictions, fetchRecentJobs, payload]);

    // Phase 4.5c: resume a past job — re-hydrate the modal with its persisted
    // scenes + stills + script. User can regenerate any still or jump straight
    // to finalize without re-rendering anything.
    const resumeJob = useCallback(async (jobId: string) => {
        if (!accessToken) return;
        setBuildError('');
        setBuildModalOpen(true);
        setBuildLoading(false);
        setStillsLoading(false);
        setFinalizeLoading(false);
        try {
            const tok = await getFreshToken();
            const { ok, status, data } = await fetchJsonResilient(`${API}/api/zerotier-private/jobs/${jobId}/state`, {
                headers: { Authorization: `Bearer ${tok}` },
            });
            if (!ok) throw new Error(String(data?.detail || data?.error || `Resume failed (${status})`));
            setBuildTopic(data?.topic || data?.title || '');
            setBuildScript(String(data?.script_json || '').trim());
            setStillsJobId(jobId);
            const scenes = (data?.scenes || []) as ScenePreview[];
            setScenePreviews(scenes.map((s) => ({ ...s, still_url: `${API}${s.still_url}` })));
            if (data?.mp4_url) {
                // PR #142 — hydrate previously-saved YouTube metadata
                // from disk so a resumed finished job shows the same
                // description + tags Casey saw at finalize time, instead
                // of re-firing Grok (which would burn fal-budget +
                // produce a different output every time).
                const meta = (data?.youtube_metadata || data?.result?.youtube_metadata) as
                    { description?: string; tags?: string[] } | undefined;
                setRenderResult({
                    job_id: jobId,
                    title: data?.title,
                    mp4_url: `${API}${data.mp4_url}`,
                    scene_count: data?.scene_count,
                    duration_total_sec: data?.duration_total_sec,
                    fal_cost_estimate_usd: data?.fal_cost_estimate_usd,
                    description: meta?.description,
                    tags: Array.isArray(meta?.tags) ? meta.tags : undefined,
                });
            } else {
                setRenderResult(null);
            }
        } catch (e: any) {
            setBuildError(String(e?.message || e || 'Resume failed'));
        }
    }, [accessToken, getFreshToken, fetchJsonResilient]);

    // OAuth-return flow: when the URL contains ?youtube_channel_id=... the
    // user just came back from Google OAuth. Force a Catalyst refresh (not
    // just a hub fetch) so the new refresh token is exercised and the panel
    // populates with full likes/comments/CTR data. Then strip the query
    // params from the URL so refreshing the page doesn't re-trigger.
    const oauthReturnHandledRef = useRef(false);
    useEffect(() => {
        if (oauthReturnHandledRef.current) return;
        if (typeof window === 'undefined') return;
        try {
            const url = new URL(window.location.href);
            const channelParam = url.searchParams.get('youtube_channel_id');
            if (!channelParam || channelParam !== ZEROTIER_CHANNEL_ID) return;
            oauthReturnHandledRef.current = true;
            // Strip the OAuth-return params so a manual reload doesn't loop.
            url.searchParams.delete('youtube_channel_id');
            url.searchParams.delete('niche');
            window.history.replaceState({}, '', url.toString());
            // Wait until accessToken is available, then trigger refresh.
            if (accessToken) {
                void handleRefresh();
            }
        } catch {
            // no-op
        }
    }, [accessToken, handleRefresh]);

    const generateTopicIdeas = useCallback(async () => {
        if (!accessToken || genLoading) return;
        setGenError('');
        setGenLoading(true);
        try {
            const r = await fetch(`${API}/api/zerotier-private/generate-topics`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${accessToken}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ count: 8 }),
            });
            const data = await r.json();
            if (!r.ok) throw new Error(String(data?.detail || data?.error || `Failed (${r.status})`));
            setGenTopics(Array.isArray(data?.topics) ? data.topics : []);
            setGenBaseline(data?.baseline || null);
            setGenCalibration(data?.calibration || null);
        } catch (e: any) {
            setGenError(String(e?.message || e || 'Topic generation failed'));
        } finally {
            setGenLoading(false);
        }
    }, [accessToken, genLoading]);

    const onConnectZeroTier = useCallback(() => {
        if (!accessToken || connecting) return;
        setConnecting(true);
        setError('');
        try {
            // Pre-tag the channel id so the OAuth callback knows which channel
            // the resulting refresh token should be linked to.
            const nextUrl = new URL(window.location.href);
            nextUrl.searchParams.set('youtube_channel_id', ZEROTIER_CHANNEL_ID);
            nextUrl.searchParams.set('niche', 'zerotier_private');
            startYouTubeBrowserConnect(accessToken, nextUrl.toString());
            // The browser is navigating to Google; setConnecting(false) is
            // not really needed but keeps the UI honest if the form fails.
        } catch (e: any) {
            setError(String(e?.message || e || 'Failed to start YouTube OAuth'));
            setConnecting(false);
        }
    }, [accessToken, connecting]);

    // Detect "no likes anywhere" → suggests channel is public-API only.
    const channelLacksOAuthData = useMemo(() => {
        if (!sortedUploads.length) return false;
        return sortedUploads.every((v) => !v.likes);
    }, [sortedUploads]);
    const channelNeedsReconnect = !!channel?.needs_reconnect || (
        typeof channel?.last_sync_error === 'string' &&
        /refresh token|reconnect|missing google/i.test(channel.last_sync_error || '')
    );

    const onBuildShort = (title: string, score?: ViralityScore) => {
        // Open the build modal pre-loaded with the candidate topic + score.
        setBuildTopic(title);
        setBuildTopicScore(score || null);
        setBuildScript('');
        setBuildError('');
        setRenderResult(null);
        setScenePreviews([]);
        setStillsJobId('');
        setBuildModalOpen(true);
    };

    // Phase 4.5b: poll a job_id until status=ready or failed. Backend returns
    // {job_id, status} immediately; we poll every 4s. Avoids proxy timeouts on
    // long render jobs (proxies 504 a synchronous 60-130s call).
    //
    // Phase 4.5d: getFreshToken() on every poll iteration so a Supabase token
    // expiry mid-poll (the 720s timeout Casey hit on finalize) auto-refreshes
    // instead of leaving the loop stuck with a dead token.
    const pollJobStatus = useCallback(async (jobId: string, maxSeconds = 600): Promise<any> => {
        const deadline = Date.now() + maxSeconds * 1000;
        let lastErr: string = '';
        let consecutive401 = 0;
        while (Date.now() < deadline) {
            try {
                const tok = await getFreshToken();
                const r = await fetch(`${API}/api/zerotier-private/jobs/${jobId}/status`, {
                    headers: { Authorization: `Bearer ${tok}` },
                });
                // 502/504 happens on Fly cold-start during long polls — keep polling
                if (r.status === 502 || r.status === 504) {
                    await new Promise((res) => setTimeout(res, 4000));
                    continue;
                }
                if (r.status === 401) {
                    // Token expired mid-poll. getFreshToken() should auto-refresh
                    // on the next iteration; bail after 5 consecutive 401s.
                    consecutive401 += 1;
                    lastErr = 'Authentication required';
                    if (consecutive401 >= 5) {
                        throw new Error('Auth refresh failed 5 times — please refresh the page and try Resume from Recent renders');
                    }
                    await new Promise((res) => setTimeout(res, 4000));
                    continue;
                }
                consecutive401 = 0;
                const text = await r.text();
                let data: any = null;
                try { data = JSON.parse(text); } catch {
                    // proxy returned HTML — treat as transient and keep polling
                    lastErr = `non-JSON response (${r.status}): ${text.slice(0, 100)}`;
                    await new Promise((res) => setTimeout(res, 4000));
                    continue;
                }
                if (!r.ok) {
                    if (r.status === 404) {
                        await new Promise((res) => setTimeout(res, 2000));
                        continue;
                    }
                    throw new Error(String(data?.detail || data?.error || `Poll failed (${r.status})`));
                }
                const status = String(data?.status || '');
                if (status === 'ready') return data;
                if (status === 'failed') throw new Error(String(data?.error || 'Background job failed'));
                await new Promise((res) => setTimeout(res, 4000));
            } catch (e: any) {
                lastErr = String(e?.message || e || 'Poll error');
                if (lastErr.includes('Auth refresh failed')) throw e;
                await new Promise((res) => setTimeout(res, 4000));
            }
        }
        throw new Error(`Timed out waiting for job after ${maxSeconds}s. The render may still be running on the backend — check 'Recent renders' for the result.${lastErr ? ` Last error: ${lastErr}` : ''}`);
    }, [getFreshToken]);

    // Phase 4.5: stage 1 — generate stills only (background job + polling)
    const generateStills = useCallback(async () => {
        if (!accessToken || !buildScript.trim() || stillsLoading) return;
        setBuildError('');
        setScenePreviews([]);
        setStillsJobId('');
        setStillsLoading(true);
        try {
            let scriptForBackend = buildScript.trim();
            try { scriptForBackend = JSON.stringify(JSON.parse(scriptForBackend)); } catch {}
            const tok = await getFreshToken();
            const { ok, status, data } = await fetchJsonResilient(`${API}/api/zerotier-private/render-stills`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${tok}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    script_json: scriptForBackend,
                    topic: buildTopic.trim(),
                    predicted_score: buildTopicScore?.score ?? null,
                    predicted_like_rate: buildTopicScore?.predicted_lr ?? null,
                }),
            });
            if (!ok) throw new Error(String(data?.detail || data?.error || `Stills submit failed (${status})`));
            const jobId = String(data?.job_id || '');
            if (!jobId) throw new Error('Backend returned no job_id');
            setStillsJobId(jobId);
            const ready = await pollJobStatus(jobId, 300);
            const result = ready?.result || {};
            const scenes = (result?.scenes || []) as ScenePreview[];
            setScenePreviews(scenes.map((s) => ({ ...s, still_url: `${API}${s.still_url}` })));
        } catch (e: any) {
            setBuildError(String(e?.message || e || 'Stills generation failed'));
        } finally {
            setStillsLoading(false);
        }
    }, [accessToken, buildScript, buildTopic, buildTopicScore, stillsLoading, pollJobStatus, getFreshToken, fetchJsonResilient]);

    // Phase 4.5: regen one specific still
    const regenerateStill = useCallback(async (sceneIndex: number, customPrompt?: string) => {
        if (!accessToken || !stillsJobId) return;
        setScenePreviews((prev) => prev.map((s) => s.scene_index === sceneIndex ? { ...s, regenerating: true } : s));
        try {
            const r = await fetch(`${API}/api/zerotier-private/render-still-one`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${accessToken}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    job_id: stillsJobId,
                    scene_index: sceneIndex,
                    custom_prompt: customPrompt || null,
                }),
            });
            const data = await r.json();
            if (!r.ok) throw new Error(String(data?.detail || data?.error || `Regen failed (${r.status})`));
            // Cache-bust the URL so browser fetches the new image
            const cacheBustedUrl = `${API}${data.still_url}?t=${Date.now()}`;
            setScenePreviews((prev) => prev.map((s) =>
                s.scene_index === sceneIndex
                    ? { ...s, still_url: cacheBustedUrl, regenerating: false }
                    : s
            ));
        } catch (e: any) {
            setBuildError(String(e?.message || e || 'Still regen failed'));
            setScenePreviews((prev) => prev.map((s) => s.scene_index === sceneIndex ? { ...s, regenerating: false } : s));
        }
    }, [accessToken, stillsJobId]);

    // Phase 4.5: stage 3 — finalize (Pixverse + TTS + compose, background job + polling)
    const finalizeRender = useCallback(async () => {
        if (!accessToken || !stillsJobId || finalizeLoading) return;
        setBuildError('');
        setRenderResult(null);
        setFinalizeLoading(true);
        try {
            const tok = await getFreshToken();
            const { ok, status, data } = await fetchJsonResilient(`${API}/api/zerotier-private/render-finalize`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${tok}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ job_id: stillsJobId }),
            });
            if (!ok) throw new Error(String(data?.detail || data?.error || `Finalize submit failed (${status})`));
            // PR #148 — bump poll timeout 720s → 1200s. Pixverse i2v at
            // 8 scenes × ~30-90s each + mmaudio SFX + minimax narration +
            // compose can legitimately take 10-14 min. The prior 12-min
            // ceiling fired before the backend finished, leaving Casey
            // with "Timed out" while the job was still running.
            const ready = await pollJobStatus(stillsJobId, 1200);
            const result = ready?.result || {};
            const finishedJobId = String(result?.job_id || stillsJobId);
            setRenderResult({
                job_id: finishedJobId,
                title: result?.title,
                mp4_url: result?.mp4_url ? `${API}${result.mp4_url}` : undefined,
                scene_count: result?.scene_count,
                duration_total_sec: result?.duration_total_sec,
                fal_cost_estimate_usd: result?.fal_cost_estimate_usd,
                description: result?.youtube_metadata?.description,
                tags: result?.youtube_metadata?.tags,
            });
            // PR #142 — auto-fire metadata generation right after finalize.
            // Render is done, MP4 is on disk — no reason to make Casey click
            // a separate button before he can copy/paste into YouTube Studio.
            // If the job already had youtube_metadata baked into result.json
            // (e.g. on a resume), skip the regeneration to save a Grok call.
            if (!result?.youtube_metadata?.description) {
                generateMetadataRef.current?.(finishedJobId).catch(() => {});
            }
        } catch (e: any) {
            setBuildError(String(e?.message || e || 'Finalize failed'));
        } finally {
            setFinalizeLoading(false);
        }
    }, [accessToken, stillsJobId, finalizeLoading, pollJobStatus, getFreshToken, fetchJsonResilient]);

    // PR #142 — YouTube metadata generator. Calls
    // POST /api/zerotier-private/generate-metadata which uses Grok with
    // ZeroTier-locked metadata system prompt to produce description + tags
    // from the title + scene narration. Result is also persisted into
    // result.json on disk so it survives a page reload.
    //
    // Wrapped in a ref so finalizeRender (defined above) can call this
    // before generateMetadata is defined as a const. Same pattern used by
    // the long-form panel for resume hooks.
    const generateMetadataRef = useRef<((jobId: string) => Promise<void>) | null>(null);
    const generateMetadata = useCallback(async (jobId: string) => {
        if (!accessToken || !jobId || metadataLoading) return;
        setMetadataLoading(true);
        setMetadataError('');
        try {
            const tok = await getFreshToken();
            const r = await fetch(`${API}/api/zerotier-private/generate-metadata`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${tok}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ job_id: jobId }),
            });
            if (!r.ok) {
                const txt = await r.text().catch(() => '');
                throw new Error(`generate-metadata failed: ${r.status} ${txt.slice(0, 200)}`);
            }
            const d = await r.json();
            const tagsArr: string[] = Array.isArray(d.tags) ? d.tags : [];
            const titleStr: string = String(d.title || '').trim();
            const descStr: string = String(d.description || '').trim();
            setRenderResult((prev) => prev ? {
                ...prev,
                title: titleStr || prev.title,
                description: descStr,
                tags: tagsArr,
            } : prev);
            // PR #143 — auto-save title + description + tags as a single
            // .txt to Casey's Downloads folder so he doesn't need to copy
            // each field individually before YouTube uploading.
            try {
                downloadMetadataAsTxt(titleStr || (renderResult?.title || ''), descStr, tagsArr, jobId);
            } catch { /* best-effort; clipboard buttons remain as fallback */ }
        } catch (e: any) {
            setMetadataError(String(e?.message || e || 'Failed to generate metadata'));
        } finally {
            setMetadataLoading(false);
        }
    }, [accessToken, getFreshToken, metadataLoading, renderResult?.title]);
    // Bind the ref every render so finalizeRender's closure reads the
    // latest generateMetadata (which itself depends on accessToken etc.).
    generateMetadataRef.current = generateMetadata;

    // Copy helper — writes to clipboard + flashes "Copied!" badge for the
    // field. Tags are joined with ", " for YouTube Studio's tag input.
    const copyToClipboard = useCallback(async (field: 'title' | 'description' | 'tags', text: string) => {
        try {
            await navigator.clipboard.writeText(text);
            setCopiedField(field);
            setTimeout(() => setCopiedField(''), 1800);
        } catch {
            // Fallback: select + execCommand (deprecated but still works in
            // older browsers). If even this fails, alert the user.
            try {
                const ta = document.createElement('textarea');
                ta.value = text;
                ta.style.position = 'fixed';
                ta.style.opacity = '0';
                document.body.appendChild(ta);
                ta.select();
                document.execCommand('copy');
                document.body.removeChild(ta);
                setCopiedField(field);
                setTimeout(() => setCopiedField(''), 1800);
            } catch {
                alert('Could not copy to clipboard. Select and copy manually.');
            }
        }
    }, []);

    const generateBuildScript = useCallback(async () => {
        if (!accessToken || !buildTopic.trim() || buildLoading) return;
        setBuildError('');
        setBuildScript('');
        setBuildLoading(true);
        try {
            const r = await fetch(`${API}/api/zerotier-private/script`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${accessToken}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ topic: buildTopic.trim(), stream: false }),
            });
            const data = await r.json();
            if (!r.ok) throw new Error(String(data?.detail || data?.error || `Failed (${r.status})`));
            // Backend returns { script_json: "<JSON-as-text>", topic }. Try to
            // pretty-print it; fall back to raw text if parse fails.
            const raw = String(data?.script_json || '').trim();
            try {
                const parsed = JSON.parse(raw);
                setBuildScript(JSON.stringify(parsed, null, 2));
            } catch {
                setBuildScript(raw);
            }
        } catch (e: any) {
            setBuildError(String(e?.message || e || 'Script generation failed'));
        } finally {
            setBuildLoading(false);
        }
    }, [accessToken, buildTopic, buildLoading]);

    const closeBuildModal = useCallback(() => {
        if (buildLoading || renderLoading || stillsLoading || finalizeLoading) return; // don't close mid-flight
        setBuildModalOpen(false);
    }, [buildLoading, renderLoading, stillsLoading, finalizeLoading]);

    // (Phase 2b's monolithic renderBuildShort retired — Phase 4.5 splits it
    // into render-stills + render-finalize so the user can preview + approve
    // stills before paying for animation.)

    return (
        <div className="flex flex-col gap-6 px-6 py-8 max-w-6xl mx-auto">
            <header className="flex items-start justify-between gap-3">
                <div>
                    <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.18em] text-amber-300">
                        <Zap className="h-3.5 w-3.5" />
                        Private Niche
                    </div>
                    <h1 className="mt-1 text-2xl font-bold text-white">ZeroTier — Catalyst Insights</h1>
                    <p className="mt-1 text-sm text-zinc-400">
                        Catalyst pulls live YouTube data for your ZeroTier channel and recommends the next short to ship.
                    </p>
                </div>
                <button
                    type="button"
                    onClick={handleRefresh}
                    disabled={loading || refreshing}
                    className="inline-flex items-center gap-2 rounded-lg border border-violet-500/30 bg-violet-500/10 px-4 py-2 text-sm font-semibold text-violet-200 transition hover:border-violet-400 hover:bg-violet-500/20 disabled:opacity-50"
                >
                    {refreshing ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                    {refreshing ? 'Syncing…' : 'Sync Channel'}
                </button>
            </header>

            {error && (
                <div className="flex items-start gap-2 rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-200">
                    <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" />
                    <span>{error}</span>
                </div>
            )}

            {loading && !payload && (
                <div className="flex items-center justify-center gap-3 rounded-2xl border border-white/[0.06] bg-white/[0.02] py-16 text-sm text-zinc-400">
                    <Loader2 className="h-5 w-5 animate-spin" />
                    Loading Catalyst data for ZeroTier…
                </div>
            )}

            {!loading && !channel && (
                <div className="rounded-2xl border border-amber-500/30 bg-amber-500/5 p-6 text-sm text-amber-200">
                    <div className="font-semibold mb-1">ZeroTier channel not connected to Catalyst.</div>
                    <p className="text-amber-100/80 mb-4">
                        Connect ZeroTier ({ZEROTIER_CHANNEL_ID}) via Google OAuth to enable rich analytics
                        (per-video likes, comments, average view duration, impression CTR) and unlock
                        Catalyst's full topic-recommendation pipeline.
                    </p>
                    <button
                        type="button"
                        onClick={onConnectZeroTier}
                        disabled={connecting}
                        className="inline-flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-red-500 disabled:opacity-60"
                    >
                        {connecting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Youtube className="h-4 w-4" />}
                        {connecting ? 'Opening Google…' : 'Connect ZeroTier via Google'}
                    </button>
                </div>
            )}

            {channel && (
                <>
                    {/* Channel header card */}
                    <section className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
                        <div className="flex flex-wrap items-baseline justify-between gap-3">
                            <div>
                                <h2 className="text-lg font-bold text-white">{channel.title || 'ZeroTier'}</h2>
                                <div className="text-xs text-zinc-500">
                                    {channel.channel_handle || `@${ZEROTIER_CHANNEL_ID}`}
                                </div>
                            </div>
                            <div className="grid grid-cols-3 gap-4 text-right">
                                <div>
                                    <div className="text-xs uppercase tracking-[0.18em] text-zinc-500">Subs</div>
                                    <div className="text-lg font-bold text-white">{formatNum(channel.subscriber_count)}</div>
                                </div>
                                <div>
                                    <div className="text-xs uppercase tracking-[0.18em] text-zinc-500">Videos</div>
                                    <div className="text-lg font-bold text-white">{formatNum(channel.video_count)}</div>
                                </div>
                                <div>
                                    <div className="text-xs uppercase tracking-[0.18em] text-zinc-500">Total views</div>
                                    <div className="text-lg font-bold text-white">{formatNum(channel.view_count)}</div>
                                </div>
                            </div>
                        </div>
                        {snapshot?.channel_summary && (
                            <p className="mt-3 text-sm text-zinc-300">{snapshot.channel_summary}</p>
                        )}
                        {channel.last_sync_error && (
                            <div className="mt-3 flex items-start gap-2 rounded-lg border border-amber-500/30 bg-amber-500/10 p-2 text-xs text-amber-200">
                                <AlertTriangle className="h-3.5 w-3.5 shrink-0 mt-0.5" />
                                <span>Last sync error: {channel.last_sync_error}</span>
                            </div>
                        )}
                        {(channelLacksOAuthData || channelNeedsReconnect) && (
                            <div className="mt-3 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-red-500/30 bg-red-500/5 p-3 text-xs text-red-200">
                                <div className="flex-1 min-w-0">
                                    <div className="font-semibold mb-0.5">
                                        {channelNeedsReconnect
                                            ? 'ZeroTier needs reconnection — old OAuth token expired.'
                                            : 'Per-video likes / comments / CTR are missing.'}
                                    </div>
                                    <div className="text-red-100/80">
                                        Catalyst is currently using the public YouTube API path which only returns view
                                        counts. Connect via Google OAuth to unlock full analytics + Catalyst's learning loop.
                                    </div>
                                </div>
                                <button
                                    type="button"
                                    onClick={onConnectZeroTier}
                                    disabled={connecting}
                                    className="inline-flex items-center gap-2 rounded-md bg-red-600 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-red-500 disabled:opacity-60 shrink-0"
                                >
                                    {connecting ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Youtube className="h-3.5 w-3.5" />}
                                    {connecting ? 'Opening Google…' : 'Connect ZeroTier'}
                                </button>
                            </div>
                        )}
                    </section>

                    {/* Phase 4.5c: Recent renders — resume a project you started earlier */}
                    {recentJobs.length > 0 && (
                        <section className="rounded-2xl border border-emerald-500/30 bg-emerald-500/[0.04] p-5">
                            <div className="flex items-center gap-2 mb-3">
                                <Film className="h-5 w-5 text-emerald-300" />
                                <h2 className="text-lg font-bold text-white">Recent renders</h2>
                                <span className="text-xs text-zinc-500">— click any to resume</span>
                            </div>
                            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
                                {recentJobs.map((j) => {
                                    const stageLabel = j.stage === 'done' || j.mp4_url ? 'Final MP4 ready'
                                        : j.stage === 'stills_done' ? 'Stills ready (resume to animate)'
                                        : 'In progress';
                                    const stageColor = j.stage === 'done' || j.mp4_url ? 'text-emerald-300 bg-emerald-500/10'
                                        : j.stage === 'stills_done' ? 'text-amber-300 bg-amber-500/10'
                                        : 'text-zinc-400 bg-zinc-500/10';
                                    return (
                                        <button
                                            key={j.job_id}
                                            type="button"
                                            onClick={() => resumeJob(j.job_id)}
                                            className="text-left rounded-xl border border-white/[0.08] bg-black/30 overflow-hidden transition hover:border-emerald-500/50 hover:shadow-lg hover:shadow-emerald-900/20"
                                        >
                                            <div className="relative aspect-[9/16] bg-black">
                                                {j.thumbnail_url ? (
                                                    <img
                                                        src={`${API}${j.thumbnail_url}`}
                                                        alt={j.title || j.job_id}
                                                        loading="lazy"
                                                        className="absolute inset-0 w-full h-full object-cover"
                                                    />
                                                ) : (
                                                    <div className="absolute inset-0 flex items-center justify-center text-xs text-zinc-600">
                                                        no thumbnail
                                                    </div>
                                                )}
                                                <div className={`absolute top-1 left-1 rounded px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-[0.18em] ${stageColor}`}>
                                                    {stageLabel}
                                                </div>
                                                {j.mp4_url && (
                                                    <div className="absolute top-1 right-1 rounded bg-emerald-500 px-1.5 py-0.5 text-[9px] font-bold text-white">
                                                        MP4
                                                    </div>
                                                )}
                                            </div>
                                            <div className="p-2.5">
                                                <h3 className="text-xs font-semibold text-white leading-snug line-clamp-2 mb-1">
                                                    {j.title || j.topic || j.job_id}
                                                </h3>
                                                <div className="flex items-center justify-between gap-2 text-[10px] text-zinc-500">
                                                    <span>{j.scene_count || 0} scenes</span>
                                                    {typeof j.predicted_score === 'number' && (
                                                        <span className="text-zinc-300">score {Math.round(j.predicted_score)}</span>
                                                    )}
                                                    {typeof j.fal_cost_estimate_usd === 'number' && (
                                                        <span>${j.fal_cost_estimate_usd.toFixed(2)}</span>
                                                    )}
                                                </div>
                                            </div>
                                        </button>
                                    );
                                })}
                            </div>
                        </section>
                    )}

                    {/* Phase 4: AI-generated topic ideas — fully autonomous, learns from your channel */}
                    <section className="rounded-2xl border border-violet-500/40 bg-violet-500/[0.06] p-5">
                        <div className="flex items-start justify-between gap-3 mb-3 flex-wrap">
                            <div>
                                <div className="flex items-center gap-2">
                                    <Sparkles className="h-5 w-5 text-violet-300" />
                                    <h2 className="text-lg font-bold text-white">AI topic generator</h2>
                                </div>
                                <p className="text-xs text-zinc-400 mt-1">
                                    Grok reads your channel's actual top performers, under-performers,
                                    + decoded competitor patterns (CreationsComic, TheManDeeDubs)
                                    and proposes fresh "The Time Wally West" topics. No typing required.
                                </p>
                            </div>
                            <button
                                type="button"
                                onClick={generateTopicIdeas}
                                disabled={genLoading}
                                className="inline-flex items-center gap-2 rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-violet-500 disabled:opacity-50 shrink-0"
                            >
                                {genLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
                                {genLoading ? 'Generating ideas…' : (genTopics.length ? 'Regenerate Ideas' : 'Generate Topic Ideas')}
                            </button>
                        </div>

                        {genError && (
                            <div className="mb-3 flex items-start gap-2 rounded-lg border border-red-500/30 bg-red-500/10 p-2 text-xs text-red-200">
                                <AlertTriangle className="h-3.5 w-3.5 shrink-0 mt-0.5" />
                                <span>{genError}</span>
                            </div>
                        )}

                        {genBaseline && (
                            <div className="mb-3 text-xs text-zinc-400">
                                Channel baseline: <span className="text-zinc-200 font-semibold">{(genBaseline.channel_top_lr || 0).toFixed(2)}%</span> top LR ·
                                <span className="text-zinc-200 font-semibold"> {(genBaseline.channel_avg_lr || 0).toFixed(2)}%</span> avg
                                <span className="text-zinc-600"> · {genBaseline.uploads_considered || 0} uploads considered</span>
                            </div>
                        )}

                        {/* PR #144 — Catalyst Learning Loop visibility.
                            Renders the HIT/MISSED pattern buckets the
                            backend extracted from THIS channel's actual
                            uploads. So Casey can SEE what Catalyst has
                            learned before he hits Generate. */}
                        {genCalibration && Array.isArray(genCalibration.buckets) && genCalibration.buckets.length > 0 && (
                            <div className="mb-3 rounded-lg border border-violet-500/30 bg-black/30 p-3">
                                <div className="flex items-center justify-between mb-2">
                                    <div className="text-[10px] uppercase tracking-[0.18em] text-violet-300 font-semibold">
                                        🧠 Catalyst learning signal — patterns from your uploads
                                    </div>
                                    <div className="text-[10px] text-zinc-500">
                                        baseline {(genCalibration.channel_baseline_lr || 0).toFixed(2)}% LR
                                    </div>
                                </div>
                                {(() => {
                                    const buckets = genCalibration.buckets || [];
                                    const hits = buckets.filter((b) => b.delta_vs_baseline > 0 && b.n >= 2);
                                    const misses = buckets.filter((b) => b.delta_vs_baseline < 0 && b.n >= 2);
                                    const weak = buckets.filter((b) => b.n < 2);
                                    return (
                                        <div className="grid gap-2 sm:grid-cols-2">
                                            <div>
                                                <div className="text-[10px] uppercase tracking-[0.15em] text-emerald-300/70 mb-1">
                                                    ✓ Patterns that HIT
                                                </div>
                                                {hits.length === 0 ? (
                                                    <div className="text-[11px] text-zinc-500 italic">(not enough data — need more uploads)</div>
                                                ) : (
                                                    <ul className="space-y-1">
                                                        {hits.map((b) => (
                                                            <li key={`hit-${b.name}`} className="text-[11px] text-zinc-200">
                                                                <span className="font-semibold text-emerald-300">{b.name.replace(/_/g, ' ')}</span>
                                                                <span className="text-zinc-500"> · {b.avg_lr.toFixed(2)}% LR (+{b.delta_vs_baseline.toFixed(2)}, n={b.n})</span>
                                                            </li>
                                                        ))}
                                                    </ul>
                                                )}
                                            </div>
                                            <div>
                                                <div className="text-[10px] uppercase tracking-[0.15em] text-rose-300/70 mb-1">
                                                    ✗ Patterns that MISSED
                                                </div>
                                                {misses.length === 0 ? (
                                                    <div className="text-[11px] text-zinc-500 italic">(no clear losers yet)</div>
                                                ) : (
                                                    <ul className="space-y-1">
                                                        {misses.map((b) => (
                                                            <li key={`miss-${b.name}`} className="text-[11px] text-zinc-200">
                                                                <span className="font-semibold text-rose-300">{b.name.replace(/_/g, ' ')}</span>
                                                                <span className="text-zinc-500"> · {b.avg_lr.toFixed(2)}% LR ({b.delta_vs_baseline.toFixed(2)}, n={b.n})</span>
                                                            </li>
                                                        ))}
                                                    </ul>
                                                )}
                                                {weak.length > 0 && (
                                                    <div className="mt-2 text-[10px] text-zinc-500">
                                                        Weak signal (n=1): {weak.map((b) => b.name.replace(/_/g, ' ')).join(', ')}
                                                    </div>
                                                )}
                                            </div>
                                        </div>
                                    );
                                })()}
                                <div className="mt-2 text-[10px] text-zinc-500 italic">
                                    Grok sees this calibration on every topic-gen call — biases toward HIT patterns, avoids MISSED.
                                </div>
                            </div>
                        )}

                        {scoredGenTopics.length > 0 && (
                            <div className="grid gap-3 sm:grid-cols-1 lg:grid-cols-2">
                                {scoredGenTopics.map((c, i) => {
                                    const sColor = c.score >= 70 ? 'text-emerald-300 bg-emerald-500/10 border-emerald-500/40'
                                        : c.score >= 40 ? 'text-amber-300 bg-amber-500/10 border-amber-500/40'
                                        : 'text-zinc-400 bg-zinc-500/10 border-zinc-500/40';
                                    return (
                                        <div key={`gen-${i}`} className="rounded-xl border border-white/[0.08] bg-black/30 p-4">
                                            <div className="flex items-start gap-2 mb-2">
                                                <Sparkles className="h-4 w-4 shrink-0 text-violet-300 mt-0.5" />
                                                <div className="flex-1 min-w-0">
                                                    <h3 className="text-sm font-semibold text-white leading-snug">{c.title}</h3>
                                                </div>
                                                <div className={`rounded-md border px-2 py-0.5 text-xs font-bold tabular-nums ${sColor}`}>
                                                    {c.score}
                                                </div>
                                            </div>
                                            <div className="text-[11px] text-zinc-400 mb-2">
                                                Predicted LR: <span className="text-zinc-200 font-semibold">{c.predicted_lr.toFixed(2)}%</span>
                                            </div>
                                            {c.reasons.length > 0 && (
                                                <ul className="text-[11px] text-zinc-400 space-y-0.5 mb-2">
                                                    {c.reasons.slice(0, 2).map((r, ri) => (
                                                        <li key={`gr-${i}-${ri}`} className="flex items-start gap-1.5">
                                                            <span className="text-zinc-600 mt-0.5">·</span>
                                                            <span>{r}</span>
                                                        </li>
                                                    ))}
                                                </ul>
                                            )}
                                            <button
                                                type="button"
                                                onClick={() => onBuildShort(c.title, c)}
                                                className="inline-flex items-center gap-1.5 rounded-md border border-violet-500/40 bg-violet-500/10 px-3 py-1.5 text-xs font-semibold text-violet-200 transition hover:border-violet-400 hover:bg-violet-500/20"
                                            >
                                                <Sparkles className="h-3.5 w-3.5" />
                                                Build This Short
                                            </button>
                                        </div>
                                    );
                                })}
                            </div>
                        )}
                    </section>

                    {/* Manual topic input — fallback for when you have your own idea */}
                    <section className="rounded-2xl border border-amber-500/30 bg-amber-500/[0.04] p-5">
                        <div className="flex items-center gap-2 mb-3">
                            <Sparkles className="h-5 w-5 text-amber-300" />
                            <h2 className="text-lg font-bold text-white">Build a short from your own topic</h2>
                            <span className="text-xs text-zinc-500">
                                — heuristic-v1 scores live as you type
                            </span>
                        </div>
                        <div className="flex flex-col gap-3 sm:flex-row sm:items-start">
                            <div className="flex-1">
                                <input
                                    type="text"
                                    value={manualTopic}
                                    onChange={(e) => setManualTopic(e.target.value)}
                                    placeholder="The Time Wally West Outran the Spectre"
                                    className="w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white placeholder-zinc-500 focus:outline-none focus:border-amber-500/50"
                                />
                                {manualScore && (
                                    <div className="mt-2 flex flex-wrap items-center gap-3 text-xs">
                                        <div className={`rounded-md border px-2 py-0.5 font-bold tabular-nums ${
                                            manualScore.score >= 70 ? 'text-emerald-300 bg-emerald-500/10 border-emerald-500/40'
                                            : manualScore.score >= 40 ? 'text-amber-300 bg-amber-500/10 border-amber-500/40'
                                            : 'text-zinc-400 bg-zinc-500/10 border-zinc-500/40'
                                        }`}>
                                            Score {manualScore.score}/100
                                        </div>
                                        <span className="text-zinc-300">
                                            Predicted LR: <span className="font-semibold">{manualScore.predicted_lr.toFixed(2)}%</span>
                                        </span>
                                        {manualScore.reasons[0] && (
                                            <span className="text-zinc-500 truncate">— {manualScore.reasons[0]}</span>
                                        )}
                                    </div>
                                )}
                            </div>
                            <button
                                type="button"
                                onClick={() => onBuildShort(manualTopic.trim(), manualScore || undefined)}
                                disabled={!manualTopic.trim()}
                                className="inline-flex items-center gap-2 rounded-lg bg-amber-500 px-4 py-2 text-sm font-semibold text-zinc-950 transition hover:bg-amber-400 disabled:opacity-50"
                            >
                                <Sparkles className="h-4 w-4" />
                                Build This Short
                            </button>
                        </div>
                    </section>

                    {/* Recommended next-shorts — scored by virality heuristic v1 */}
                    {scoredCandidates.length > 0 && (
                        <section className="rounded-2xl border border-violet-500/30 bg-violet-500/[0.04] p-5">
                            <div className="flex items-center gap-2 mb-3">
                                <Sparkles className="h-5 w-5 text-violet-300" />
                                <h2 className="text-lg font-bold text-white">Catalyst recommends</h2>
                                <span className="text-xs text-zinc-500">— ranked by predicted virality (heuristic v1)</span>
                            </div>
                            <div className="grid gap-3 sm:grid-cols-1 lg:grid-cols-2">
                                {scoredCandidates.slice(0, 6).map((c, i) => {
                                    const scoreColor = c.score >= 70 ? 'text-emerald-300 bg-emerald-500/10 border-emerald-500/40'
                                        : c.score >= 40 ? 'text-amber-300 bg-amber-500/10 border-amber-500/40'
                                        : 'text-zinc-400 bg-zinc-500/10 border-zinc-500/40';
                                    return (
                                        <div
                                            key={`cand-${i}`}
                                            className="rounded-xl border border-white/[0.08] bg-black/20 p-4"
                                        >
                                            <div className="flex items-start gap-2 mb-2">
                                                <Lightbulb className="h-4 w-4 shrink-0 text-amber-300 mt-0.5" />
                                                <div className="flex-1 min-w-0">
                                                    <h3 className="text-sm font-semibold text-white leading-snug">{c.title}</h3>
                                                </div>
                                                <div className={`rounded-md border px-2 py-0.5 text-xs font-bold tabular-nums ${scoreColor}`}>
                                                    {c.score}
                                                </div>
                                            </div>
                                            <div className="text-[11px] text-zinc-400 mb-2">
                                                Predicted like-rate: <span className="text-zinc-200 font-semibold">{c.predicted_lr.toFixed(2)}%</span>
                                                <span className="text-zinc-600"> · channel baseline ~2.2%</span>
                                            </div>
                                            {c.reasons.length > 0 && (
                                                <ul className="text-[11px] text-zinc-400 space-y-0.5 mb-2">
                                                    {c.reasons.map((r, ri) => (
                                                        <li key={`r-${i}-${ri}`} className="flex items-start gap-1.5">
                                                            <span className="text-zinc-600 mt-0.5">·</span>
                                                            <span>{r}</span>
                                                        </li>
                                                    ))}
                                                </ul>
                                            )}
                                            <button
                                                type="button"
                                                onClick={() => onBuildShort(c.title, c)}
                                                className="inline-flex items-center gap-1.5 rounded-md border border-violet-500/40 bg-violet-500/10 px-3 py-1.5 text-xs font-semibold text-violet-200 transition hover:border-violet-400 hover:bg-violet-500/20"
                                            >
                                                <Sparkles className="h-3.5 w-3.5" />
                                                Build This Short
                                            </button>
                                        </div>
                                    );
                                })}
                            </div>
                            {audit?.next_moves && audit.next_moves.length > 0 && (
                                <div className="mt-4 rounded-lg border border-white/[0.06] bg-black/20 p-3 text-xs text-zinc-300">
                                    <div className="font-semibold uppercase tracking-[0.18em] text-zinc-400 mb-1">Catalyst suggests</div>
                                    <ul className="list-disc pl-4 space-y-1">
                                        {audit.next_moves.slice(0, 5).map((m, i) => (
                                            <li key={`move-${i}`}>{m}</li>
                                        ))}
                                    </ul>
                                </div>
                            )}
                        </section>
                    )}

                    {/* Phase 3: Predictions calibration — predicted vs actual */}
                    {predictions.length > 0 && (
                        <section className="rounded-2xl border border-cyan-500/30 bg-cyan-500/[0.04] p-5">
                            <div className="flex items-center gap-2 mb-3">
                                <TrendingUp className="h-5 w-5 text-cyan-300" />
                                <h2 className="text-lg font-bold text-white">Catalyst learning</h2>
                                <span className="text-xs text-zinc-500">
                                    — heuristic v1 predictions vs actual YouTube outcomes
                                </span>
                            </div>
                            {(() => {
                                const matched = predictions.filter((p) => p.matched && typeof p.delta_lr === 'number');
                                if (!matched.length) {
                                    return (
                                        <div className="text-xs text-zinc-400">
                                            {predictions.length} prediction{predictions.length === 1 ? '' : 's'} logged.
                                            Once Catalyst harvests the actual YouTube outcome (24-48h after upload),
                                            calibration deltas will appear here.
                                        </div>
                                    );
                                }
                                const avgDelta = matched.reduce((s, p) => s + (p.delta_lr || 0), 0) / matched.length;
                                const direction = avgDelta > 0.2 ? 'under-predicting' : avgDelta < -0.2 ? 'over-predicting' : 'well-calibrated';
                                const dirColor = direction === 'well-calibrated' ? 'text-emerald-300'
                                    : direction === 'under-predicting' ? 'text-amber-300' : 'text-red-300';
                                return (
                                    <div className="text-xs text-zinc-400 mb-3">
                                        <span className="text-zinc-300">{matched.length}</span> matched against actual outcomes ·
                                        avg Δ <span className={`font-semibold ${dirColor}`}>{avgDelta >= 0 ? '+' : ''}{avgDelta.toFixed(2)}%</span> ·
                                        <span className={`font-semibold ${dirColor}`}> {direction}</span>
                                    </div>
                                );
                            })()}
                            <div className="overflow-x-auto">
                                <table className="w-full text-xs">
                                    <thead>
                                        <tr className="text-left text-[10px] uppercase tracking-[0.18em] text-zinc-500">
                                            <th className="px-2 py-1.5">Title</th>
                                            <th className="px-2 py-1.5 text-right">Score</th>
                                            <th className="px-2 py-1.5 text-right">Predicted LR</th>
                                            <th className="px-2 py-1.5 text-right">Actual LR</th>
                                            <th className="px-2 py-1.5 text-right">Δ</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {predictions.slice(0, 12).map((p, i) => {
                                            const delta = typeof p.delta_lr === 'number' ? p.delta_lr : null;
                                            const dColor = delta === null ? 'text-zinc-500'
                                                : Math.abs(delta) <= 0.5 ? 'text-emerald-300'
                                                : Math.abs(delta) <= 1.0 ? 'text-amber-300' : 'text-red-300';
                                            return (
                                                <tr key={p.job_id || `pred-${i}`} className="border-t border-white/[0.06]">
                                                    <td className="px-2 py-1.5 text-white truncate max-w-md">{p.title || p.topic || '—'}</td>
                                                    <td className="px-2 py-1.5 text-right text-zinc-300 tabular-nums">
                                                        {typeof p.predicted_score === 'number' ? Math.round(p.predicted_score) : '—'}
                                                    </td>
                                                    <td className="px-2 py-1.5 text-right text-zinc-300 tabular-nums">
                                                        {typeof p.predicted_like_rate === 'number' ? `${p.predicted_like_rate.toFixed(2)}%` : '—'}
                                                    </td>
                                                    <td className="px-2 py-1.5 text-right text-zinc-300 tabular-nums">
                                                        {typeof p.actual_like_rate === 'number' ? `${p.actual_like_rate.toFixed(2)}%` : (p.matched ? '—' : <span className="text-zinc-500 italic">unmatched</span>)}
                                                    </td>
                                                    <td className={`px-2 py-1.5 text-right font-semibold tabular-nums ${dColor}`}>
                                                        {delta === null ? '—' : `${delta >= 0 ? '+' : ''}${delta.toFixed(2)}%`}
                                                    </td>
                                                </tr>
                                            );
                                        })}
                                    </tbody>
                                </table>
                            </div>
                        </section>
                    )}

                    {/* Channel audit + best/worst */}
                    {audit && (
                        <section className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
                            <div className="flex items-center gap-2 mb-3">
                                <TrendingUp className="h-5 w-5 text-emerald-300" />
                                <h2 className="text-lg font-bold text-white">Channel audit</h2>
                            </div>
                            {audit.summary && <p className="text-sm text-zinc-300 mb-3">{audit.summary}</p>}
                            <div className="grid gap-3 sm:grid-cols-2">
                                {audit.best_recent_title && (
                                    <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/5 p-3 text-xs">
                                        <div className="font-semibold uppercase tracking-[0.18em] text-emerald-300 mb-1">Best recent</div>
                                        <div className="text-white">{audit.best_recent_title}</div>
                                    </div>
                                )}
                                {audit.worst_recent_title && (
                                    <div className="rounded-lg border border-red-500/30 bg-red-500/5 p-3 text-xs">
                                        <div className="font-semibold uppercase tracking-[0.18em] text-red-300 mb-1">Weakest recent</div>
                                        <div className="text-white">{audit.worst_recent_title}</div>
                                    </div>
                                )}
                                {audit.strongest_arc && (
                                    <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3 text-xs">
                                        <div className="font-semibold uppercase tracking-[0.18em] text-zinc-400 mb-1">Strongest arc</div>
                                        <div className="text-white">{audit.strongest_arc}</div>
                                    </div>
                                )}
                                {audit.weakest_arc && (
                                    <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3 text-xs">
                                        <div className="font-semibold uppercase tracking-[0.18em] text-zinc-400 mb-1">Weakest arc</div>
                                        <div className="text-white">{audit.weakest_arc}</div>
                                    </div>
                                )}
                            </div>
                            {audit.measured_facts && audit.measured_facts.length > 0 && (
                                <div className="mt-3 text-xs text-zinc-400">
                                    <div className="font-semibold uppercase tracking-[0.18em] text-zinc-500 mb-1">Measured facts</div>
                                    <ul className="list-disc pl-4 space-y-1">
                                        {audit.measured_facts.slice(0, 4).map((f, i) => (
                                            <li key={`fact-${i}`}>{f}</li>
                                        ))}
                                    </ul>
                                </div>
                            )}
                        </section>
                    )}

                    {/* Uploads sorted by like-rate */}
                    {sortedUploads.length > 0 && (
                        <section className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
                            <h2 className="mb-3 text-lg font-bold text-white">
                                Your uploads — sorted by like-rate
                            </h2>
                            <div className="overflow-x-auto">
                                <table className="w-full text-sm">
                                    <thead>
                                        <tr className="text-left text-xs uppercase tracking-[0.18em] text-zinc-500">
                                            <th className="px-2 py-2">Title</th>
                                            <th className="px-2 py-2 text-right">Views</th>
                                            <th className="px-2 py-2 text-right">Likes</th>
                                            <th className="px-2 py-2 text-right">Like-rate</th>
                                            <th className="px-2 py-2 text-right">Pub</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {sortedUploads.map((v, i) => {
                                            const lr = likeRate(v);
                                            const lrColor = lr === null ? 'text-zinc-500' : lr >= 3.0 ? 'text-emerald-300' : lr >= 2.0 ? 'text-amber-300' : 'text-zinc-400';
                                            return (
                                                <tr key={v.video_id || i} className="border-t border-white/[0.06]">
                                                    <td className="px-2 py-2 text-white">{v.title || '—'}</td>
                                                    <td className="px-2 py-2 text-right text-zinc-300">{formatNum(v.views)}</td>
                                                    <td className="px-2 py-2 text-right text-zinc-300">{formatNum(v.likes)}</td>
                                                    <td className={`px-2 py-2 text-right font-semibold ${lrColor}`}>
                                                        {lr === null ? '—' : `${lr.toFixed(2)}%`}
                                                    </td>
                                                    <td className="px-2 py-2 text-right text-zinc-500 text-xs">
                                                        {v.published_at ? v.published_at.slice(0, 10) : '—'}
                                                    </td>
                                                </tr>
                                            );
                                        })}
                                    </tbody>
                                </table>
                            </div>
                        </section>
                    )}
                </>
            )}

            {/* Build-This-Short modal — Phase 2a: script generation */}
            {buildModalOpen && (
                <div
                    className="fixed inset-0 z-40 flex items-center justify-center bg-black/70 p-4"
                    onClick={closeBuildModal}
                >
                    <div
                        className="bg-zinc-950 border border-zinc-800 rounded-lg p-6 max-w-3xl w-full max-h-[85vh] overflow-y-auto"
                        onClick={(e) => e.stopPropagation()}
                    >
                        <div className="flex items-start justify-between mb-3 gap-3">
                            <div>
                                <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.18em] text-amber-300">
                                    <Sparkles className="h-3.5 w-3.5" />
                                    Build This Short
                                </div>
                                <h3 className="mt-1 text-lg font-bold text-white leading-snug">{buildTopic}</h3>
                                <p className="mt-1 text-xs text-zinc-400">
                                    Generates an 8-scene Conflict Arc script using the locked
                                    ZeroTier (Private) template — past-tense title, cosmic stakes,
                                    cel-shaded comic visuals, MiniMax narration. Render pipeline
                                    wires in next phase.
                                </p>
                            </div>
                            <button
                                type="button"
                                onClick={closeBuildModal}
                                disabled={buildLoading}
                                className="text-zinc-400 hover:text-white disabled:opacity-50"
                            >
                                <X className="h-5 w-5" />
                            </button>
                        </div>

                        <div className="mt-3 flex items-center gap-2 flex-wrap">
                            <button
                                type="button"
                                onClick={generateBuildScript}
                                disabled={buildLoading || stillsLoading || finalizeLoading || !buildTopic.trim()}
                                className="inline-flex items-center gap-2 rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-violet-500 disabled:opacity-50"
                            >
                                {buildLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
                                {buildLoading ? 'Generating with Grok…' : (buildScript ? 'Regenerate Script' : 'Generate Script')}
                            </button>
                            <button
                                type="button"
                                onClick={generateStills}
                                disabled={!buildScript.trim() || stillsLoading || buildLoading || finalizeLoading}
                                title="Stage 1 of 2: render 8 stills only (~$0.32, ~60s). You preview + approve before animation."
                                className="inline-flex items-center gap-2 rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-cyan-500 disabled:opacity-50"
                            >
                                {stillsLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <ImageIcon className="h-4 w-4" />}
                                {stillsLoading ? 'Rendering 8 stills…' : (scenePreviews.length ? 'Re-render All Stills' : 'Generate Stills (~$0.32)')}
                            </button>
                        </div>

                        {/* Phase 4.5: per-scene still gallery + regenerate buttons */}
                        {scenePreviews.length > 0 && (
                            <div className="mt-4 rounded-lg border border-cyan-500/30 bg-cyan-500/[0.04] p-3">
                                <div className="flex items-center justify-between mb-3">
                                    <div className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300">
                                        Stage 1 — review {scenePreviews.length} stills before animating
                                    </div>
                                    <span className="text-[10px] text-zinc-500">
                                        Job: {stillsJobId.slice(0, 8)}…
                                    </span>
                                </div>
                                <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                                    {scenePreviews.map((s) => (
                                        <div key={s.scene_id} className="rounded-lg border border-white/[0.08] bg-black/30 p-2">
                                            <div className="relative aspect-[9/16] rounded overflow-hidden bg-black mb-2">
                                                <img
                                                    src={s.still_url}
                                                    alt={`Scene ${s.scene_index + 1}: ${s.caption}`}
                                                    className="absolute inset-0 w-full h-full object-cover"
                                                    loading="lazy"
                                                />
                                                {s.regenerating && (
                                                    <div className="absolute inset-0 flex items-center justify-center bg-black/70">
                                                        <Loader2 className="h-6 w-6 animate-spin text-cyan-300" />
                                                    </div>
                                                )}
                                                <div className="absolute top-1 left-1 rounded bg-black/70 px-1.5 py-0.5 text-[10px] font-mono text-zinc-300">
                                                    #{s.scene_index + 1}
                                                </div>
                                            </div>
                                            <div className="text-[11px] font-semibold text-zinc-200 mb-1 leading-snug">
                                                "{s.caption}"
                                            </div>
                                            <div className="text-[10px] text-zinc-500 mb-2 line-clamp-2 leading-snug">
                                                {s.narration}
                                            </div>
                                            <button
                                                type="button"
                                                onClick={() => regenerateStill(s.scene_index)}
                                                disabled={s.regenerating || finalizeLoading}
                                                className="w-full inline-flex items-center justify-center gap-1.5 rounded border border-cyan-500/40 bg-cyan-500/10 px-2 py-1 text-[10px] font-semibold text-cyan-200 transition hover:bg-cyan-500/20 disabled:opacity-50"
                                            >
                                                {s.regenerating ? <Loader2 className="h-3 w-3 animate-spin" /> : <RefreshCw className="h-3 w-3" />}
                                                {s.regenerating ? 'Regenerating…' : 'Regenerate'}
                                            </button>
                                        </div>
                                    ))}
                                </div>
                                <div className="mt-3 flex items-center justify-between gap-2">
                                    <div className="text-xs text-zinc-400">
                                        Stage 2: animate via Pixverse (~$1.86, 5-7 min) → MiniMax narration → final MP4.
                                    </div>
                                    <button
                                        type="button"
                                        onClick={finalizeRender}
                                        disabled={finalizeLoading || stillsLoading || scenePreviews.some((s) => s.regenerating)}
                                        className="inline-flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-emerald-500 disabled:opacity-50 shrink-0"
                                    >
                                        {finalizeLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
                                        {finalizeLoading ? 'Animating + composing (5-7 min)…' : 'Approve + Animate + Compose Final MP4'}
                                    </button>
                                </div>
                            </div>
                        )}

                        {renderResult?.mp4_url && (
                            <div className="mt-3 rounded-lg border border-emerald-500/30 bg-emerald-500/5 p-3 text-xs text-emerald-100">
                                <div className="font-semibold uppercase tracking-[0.18em] text-emerald-300 mb-1">
                                    ✓ Render complete
                                </div>
                                <div className="space-y-1">
                                    {renderResult.title && <div><span className="text-emerald-300/80">Title:</span> {renderResult.title}</div>}
                                    {typeof renderResult.scene_count === 'number' && <div><span className="text-emerald-300/80">Scenes:</span> {renderResult.scene_count}</div>}
                                    {typeof renderResult.duration_total_sec === 'number' && <div><span className="text-emerald-300/80">Duration:</span> {renderResult.duration_total_sec.toFixed(1)}s</div>}
                                    {typeof renderResult.fal_cost_estimate_usd === 'number' && <div><span className="text-emerald-300/80">fal cost (est):</span> ${renderResult.fal_cost_estimate_usd.toFixed(2)}</div>}
                                    <div className="pt-2">
                                        <a
                                            href={renderResult.mp4_url}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            className="inline-flex items-center gap-2 rounded-md bg-emerald-600 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-emerald-500"
                                        >
                                            Open MP4 in new tab
                                        </a>
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* PR #142 — YouTube upload metadata card. Auto-
                            generates after finalize completes (or on click of
                            Regenerate). Casey copy-pastes title +
                            description + tags into YouTube Studio. */}
                        {renderResult?.mp4_url && (
                            <div className="mt-3 rounded-lg border border-violet-500/30 bg-violet-500/5 p-3 text-xs text-zinc-100">
                                <div className="flex items-center justify-between mb-2">
                                    <div className="font-semibold uppercase tracking-[0.18em] text-violet-300">
                                        YouTube upload metadata
                                    </div>
                                    <div className="flex items-center gap-1.5">
                                        {/* PR #143 — manual re-download. Auto-fires
                                            once on Generate/Regenerate, but Casey
                                            can re-trigger here if the original
                                            download was lost or skipped by the
                                            browser's per-site download prompt. */}
                                        <button
                                            type="button"
                                            onClick={() => renderResult?.description && downloadMetadataAsTxt(
                                                renderResult.title || '',
                                                renderResult.description || '',
                                                renderResult.tags || [],
                                                renderResult.job_id,
                                            )}
                                            disabled={!renderResult.description}
                                            title="Download title + description + tags as a .txt file to your Downloads folder"
                                            className="inline-flex items-center gap-1.5 rounded-md bg-zinc-900 hover:bg-violet-500/20 border border-violet-500/30 px-2.5 py-1 text-[10px] font-semibold text-violet-200 transition disabled:opacity-50"
                                        >
                                            <Download className="h-3 w-3" />
                                            Download .txt
                                        </button>
                                        <button
                                            type="button"
                                            onClick={() => renderResult.job_id && generateMetadata(renderResult.job_id)}
                                            disabled={metadataLoading || !renderResult.job_id}
                                            className="inline-flex items-center gap-1.5 rounded-md bg-zinc-900 hover:bg-violet-500/20 border border-violet-500/30 px-2.5 py-1 text-[10px] font-semibold text-violet-200 transition disabled:opacity-50"
                                        >
                                            {metadataLoading ? <Loader2 className="h-3 w-3 animate-spin" /> : <RefreshCw className="h-3 w-3" />}
                                            {metadataLoading ? 'Generating…' : (renderResult.description ? 'Regenerate' : 'Generate')}
                                        </button>
                                    </div>
                                </div>

                                {metadataError && (
                                    <div className="mb-2 flex items-start gap-1.5 rounded border border-red-500/30 bg-red-500/10 p-2 text-[11px] text-red-200">
                                        <AlertTriangle className="h-3 w-3 shrink-0 mt-0.5" />
                                        <span>{metadataError}</span>
                                    </div>
                                )}

                                {!renderResult.description && !metadataLoading && !metadataError && (
                                    <div className="text-[11px] text-violet-200/70 italic">
                                        No metadata yet. Click "Generate" to ask Grok for a tuned description + tag list.
                                    </div>
                                )}

                                {(renderResult.description || metadataLoading) && (
                                    <div className="space-y-2.5">
                                        {/* Title row — read-only, locked by render */}
                                        <div>
                                            <div className="flex items-center justify-between mb-0.5">
                                                <label className="text-[10px] uppercase tracking-[0.15em] text-violet-300/70">Title</label>
                                                <button
                                                    type="button"
                                                    onClick={() => renderResult.title && copyToClipboard('title', renderResult.title)}
                                                    disabled={!renderResult.title}
                                                    className="text-[10px] font-semibold text-violet-200 hover:text-white transition disabled:opacity-40"
                                                >
                                                    {copiedField === 'title' ? '✓ Copied' : 'Copy'}
                                                </button>
                                            </div>
                                            <input
                                                type="text"
                                                readOnly
                                                value={renderResult.title || ''}
                                                className="w-full rounded border border-violet-500/20 bg-black/40 px-2 py-1 text-[11px] text-zinc-100 font-mono"
                                            />
                                        </div>

                                        {/* Description row — multi-line textarea */}
                                        <div>
                                            <div className="flex items-center justify-between mb-0.5">
                                                <label className="text-[10px] uppercase tracking-[0.15em] text-violet-300/70">
                                                    Description {renderResult.description && (
                                                        <span className="ml-1 text-zinc-500 normal-case tracking-normal">({renderResult.description.length} chars)</span>
                                                    )}
                                                </label>
                                                <button
                                                    type="button"
                                                    onClick={() => renderResult.description && copyToClipboard('description', renderResult.description)}
                                                    disabled={!renderResult.description}
                                                    className="text-[10px] font-semibold text-violet-200 hover:text-white transition disabled:opacity-40"
                                                >
                                                    {copiedField === 'description' ? '✓ Copied' : 'Copy'}
                                                </button>
                                            </div>
                                            <textarea
                                                readOnly
                                                value={metadataLoading && !renderResult.description ? 'Generating…' : (renderResult.description || '')}
                                                rows={5}
                                                className="w-full resize-none rounded border border-violet-500/20 bg-black/40 px-2 py-1 text-[11px] text-zinc-100 font-mono leading-snug"
                                            />
                                        </div>

                                        {/* Tags row — comma-joined for clipboard, chip display for visual */}
                                        <div>
                                            <div className="flex items-center justify-between mb-0.5">
                                                <label className="text-[10px] uppercase tracking-[0.15em] text-violet-300/70">
                                                    Tags {renderResult.tags && (
                                                        <span className="ml-1 text-zinc-500 normal-case tracking-normal">({renderResult.tags.length})</span>
                                                    )}
                                                </label>
                                                <button
                                                    type="button"
                                                    onClick={() => renderResult.tags?.length && copyToClipboard('tags', renderResult.tags.join(', '))}
                                                    disabled={!renderResult.tags?.length}
                                                    className="text-[10px] font-semibold text-violet-200 hover:text-white transition disabled:opacity-40"
                                                >
                                                    {copiedField === 'tags' ? '✓ Copied (comma-separated)' : 'Copy'}
                                                </button>
                                            </div>
                                            {renderResult.tags?.length ? (
                                                <div className="flex flex-wrap gap-1 rounded border border-violet-500/20 bg-black/40 px-2 py-1.5">
                                                    {renderResult.tags.map((t, i) => (
                                                        <span
                                                            key={`${t}-${i}`}
                                                            className="rounded-full bg-violet-500/15 border border-violet-500/30 px-2 py-0.5 text-[10px] text-violet-100"
                                                        >
                                                            {t}
                                                        </span>
                                                    ))}
                                                </div>
                                            ) : (
                                                <div className="rounded border border-violet-500/20 bg-black/40 px-2 py-1 text-[11px] text-zinc-500 italic">
                                                    {metadataLoading ? 'Generating…' : '(no tags yet)'}
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                )}
                            </div>
                        )}

                        {buildError && (
                            <div className="mt-3 flex items-start gap-2 rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-xs text-red-200">
                                <AlertTriangle className="h-3.5 w-3.5 shrink-0 mt-0.5" />
                                <span>{buildError}</span>
                            </div>
                        )}

                        {buildScript && (
                            <div className="mt-3 rounded-lg border border-white/[0.08] bg-black/30 p-3">
                                <div className="flex items-center justify-between mb-2">
                                    <div className="text-xs font-semibold uppercase tracking-[0.18em] text-zinc-400">
                                        Grok output (zerotier_private template) — editable
                                    </div>
                                    <span className="text-[10px] text-zinc-500">{buildScript.length} chars</span>
                                </div>
                                <textarea
                                    value={buildScript}
                                    onChange={(e) => setBuildScript(e.target.value)}
                                    disabled={renderLoading}
                                    rows={18}
                                    className="w-full overflow-x-auto whitespace-pre font-mono text-[11px] leading-snug text-zinc-200 bg-black/50 border border-white/[0.06] rounded px-2 py-2 disabled:opacity-60"
                                    spellCheck={false}
                                />
                            </div>
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}
/* force vercel rebuild 20260508T061435Z */
