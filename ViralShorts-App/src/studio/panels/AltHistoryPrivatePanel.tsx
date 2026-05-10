/**
 * AltHistoryPrivatePanel — admin-only Catalyst-fed surface for the
 * Cryptic Science channel (where Casey's alt-history battles uploads
 * land). Mirrors ZeroTierPrivatePanel's analytics + topic-generator
 * UX but delegates the actual render to the existing CreatePanel
 * (alt_battles niche), which already handles the
 * /api/skeleton-ai/* alt-battles pipeline.
 *
 * Why a separate panel vs. just using the public alt_battles niche:
 *   - Catalyst hub data filtered to the Cryptic Science channel so
 *     topic suggestions are tuned to actual top performers.
 *   - Topic generator that learns from the channel's actual analytics
 *     (winners + losers + already-uploaded) to surface ideas this
 *     channel will reward.
 *   - Admin gating — keeps this from non-admin users while testing
 *     continues. Casey 2026-05-08: "keep the main version public,
 *     my version private."
 *
 * Backend reuse:
 *   GET  /api/catalyst/hub?channel_id=...    → channel snapshot + audit
 *   POST /api/catalyst/hub/refresh           → re-sync
 *   POST /api/alt-history-private/generate-topics → Grok topic gen
 */
import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import {
    AlertTriangle, ArrowRight, Lightbulb, Link2, Loader2, RefreshCw,
    Sparkles, TrendingUp, Youtube,
} from 'lucide-react';
import { API, AuthContext, startYouTubeBrowserConnect } from '../shared';

const CRYPTIC_SCIENCE_CHANNEL_ID = 'UCOHnksm14B-9AqGhlpRxG5A';

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
};

type ChannelAudit = {
    summary?: string;
    next_video_candidates?: string[] | { title: string }[];
    [k: string]: any;
};

type CatalystHubPayload = {
    selected_channel?: {
        channel_id?: string;
        channel_title?: string;
        subscriber_count?: number;
        view_count?: number;
        video_count?: number;
        last_synced_at?: number | null;
        analytics_snapshot?: {
            uploaded_videos?: UploadedVideo[];
            channel_audit?: ChannelAudit;
            channel_summary?: any;
        };
    };
    channels?: Array<{
        channel_id?: string;
        channel_title?: string;
        analytics_snapshot?: any;
        [k: string]: any;
    }>;
};

interface ViralityScore {
    score: number;     // 0-100
    predicted_lr: number;
    flags: string[];
}

interface GeneratedTopic {
    topic: string;
    score: ViralityScore;
}

// Heuristic v1 scoring for alt-battles topics. Lifted concept from
// ZeroTierPrivatePanel but rule set adapted for what works on
// counterfactual military matchups (Cryptic Science / alt-battles).
const ERA_DIVERSITY_BONUS = 8;            // distinct era pairs are good
const NAMED_LEADER_BONUS = 12;            // "Napoleon" > "French army"
const TECH_GAP_BONUS = 10;                // gunpowder vs spears, drones vs cavalry
const TERRAIN_HOOK_BONUS = 6;             // mountain pass, river crossing, urban siege
const SPECIFIC_DATE_BONUS = 5;            // year mention
const VAGUE_PENALTY = -15;                // "who would win" energy
const RANKER_PENALTY = -18;               // "every", "ranked", "top 10"
const ALREADY_UPLOADED_PENALTY = -25;     // close to existing upload

const NAMED_LEADERS = new Set([
    'napoleon', 'caesar', 'genghis', 'khan', 'alexander', 'hannibal',
    'patton', 'rommel', 'zhukov', 'lee', 'grant', 'sherman', 'wellington',
    'saladin', 'tamerlane', 'leonidas', 'attila', 'pyrrhus', 'scipio',
    'sun tzu', 'cyrus', 'darius', 'belisarius', 'subutai', 'frederick',
]);
const TECH_GAP_MARKERS = new Set([
    'gunpowder', 'cavalry', 'phalanx', 'longbow', 'crossbow', 'musket',
    'cannon', 'tank', 'drone', 'rifle', 'sword', 'spear', 'archer',
    'infantry', 'artillery', 'chariot', 'elephant', 'siege',
]);
const TERRAIN_MARKERS = new Set([
    'mountain', 'pass', 'river', 'desert', 'forest', 'urban', 'siege',
    'bridge', 'beach', 'plain', 'valley', 'jungle', 'tundra', 'steppe',
    'crossing',
]);
const VAGUE_MARKERS = new Set([
    'who would win', 'best', 'ultimate', 'craziest', 'greatest of all',
    'most insane',
]);
const RANKER_MARKERS = new Set([
    'every', 'ranked', 'top 10', 'top 5', 'all time',
]);

function scoreAltTopic(topic: string, alreadyUploaded: string[]): ViralityScore {
    const t = topic.toLowerCase();
    const flags: string[] = [];
    let score = 50;

    let leaderHits = 0;
    for (const w of NAMED_LEADERS) {
        if (t.includes(w)) leaderHits++;
    }
    if (leaderHits >= 2) {
        score += NAMED_LEADER_BONUS;
        flags.push('named-leader-clash');
    } else if (leaderHits === 1) {
        score += NAMED_LEADER_BONUS / 2;
    }

    let techHits = 0;
    for (const w of TECH_GAP_MARKERS) {
        if (t.includes(w)) techHits++;
    }
    if (techHits >= 2) {
        score += TECH_GAP_BONUS;
        flags.push('tech-gap');
    }

    for (const w of TERRAIN_MARKERS) {
        if (t.includes(w)) {
            score += TERRAIN_HOOK_BONUS;
            flags.push('terrain-hook');
            break;
        }
    }

    if (/\b(1[0-9]{3}|20[0-9]{2}|[1-9][0-9]{0,2}\s*(BC|AD))\b/i.test(topic)) {
        score += SPECIFIC_DATE_BONUS;
        flags.push('specific-date');
    }

    for (const w of VAGUE_MARKERS) {
        if (t.includes(w)) {
            score += VAGUE_PENALTY;
            flags.push('vague-framing');
            break;
        }
    }
    for (const w of RANKER_MARKERS) {
        if (t.includes(w)) {
            score += RANKER_PENALTY;
            flags.push('ranker-format');
            break;
        }
    }

    // Era pair detection — if we mention 2+ era markers, bonus
    const eraMarkers = ['roman', 'medieval', 'mongol', 'viking', 'samurai',
        'napoleonic', 'civil war', 'wwi', 'wwii', 'vietnam', 'modern',
        'cold war', 'crusader', 'persian', 'greek', 'spartan', 'aztec',
        'inca', 'zulu', 'colonial'];
    let eraHits = 0;
    for (const e of eraMarkers) if (t.includes(e)) eraHits++;
    if (eraHits >= 2) {
        score += ERA_DIVERSITY_BONUS;
        flags.push('era-pair');
    }

    // Already-uploaded penalty
    const tWords = new Set(t.split(/\W+/).filter((w) => w.length >= 5));
    for (const existing of alreadyUploaded) {
        const eWords = new Set(existing.toLowerCase().split(/\W+/).filter((w) => w.length >= 5));
        let overlap = 0;
        for (const w of tWords) if (eWords.has(w)) overlap++;
        if (overlap >= 4) {
            score += ALREADY_UPLOADED_PENALTY;
            flags.push('overlaps-existing');
            break;
        }
    }

    score = Math.max(0, Math.min(100, score));
    // Predicted like-rate map (calibrated from ZT Private observations,
    // adjusted slightly for shorter alt-battles videos)
    let predicted_lr = 1.5;
    if (score >= 95) predicted_lr = 4.2;
    else if (score >= 80) predicted_lr = 3.4;
    else if (score >= 60) predicted_lr = 2.6;
    else if (score >= 30) predicted_lr = 1.5;
    else predicted_lr = 0.8;

    return { score: Math.round(score), predicted_lr, flags };
}

interface AltHistoryPrivatePanelProps {
    onPickNiche?: (niche: string, options?: { topic?: string }) => void;
}

export default function AltHistoryPrivatePanel({ onPickNiche }: AltHistoryPrivatePanelProps = {}) {
    const { session } = useContext(AuthContext);
    const accessToken = session?.access_token || '';

    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [connecting, setConnecting] = useState(false);
    const [error, setError] = useState('');
    const [payload, setPayload] = useState<CatalystHubPayload | null>(null);

    const fetchHub = useCallback(async () => {
        if (!accessToken) return;
        setError('');
        try {
            const url = `${API}/api/catalyst/hub?channel_id=${encodeURIComponent(CRYPTIC_SCIENCE_CHANNEL_ID)}`;
            const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
            if (!r.ok) throw new Error(`Catalyst hub failed: ${r.status}`);
            const d = await r.json();
            setPayload(d);
        } catch (e) {
            setError((e as Error).message);
        } finally {
            setLoading(false);
        }
    }, [accessToken]);

    useEffect(() => {
        if (accessToken) fetchHub();
    }, [accessToken, fetchHub]);

    const refresh = useCallback(async () => {
        if (!accessToken) return;
        setRefreshing(true);
        setError('');
        try {
            await fetch(`${API}/api/catalyst/hub/refresh`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${accessToken}`,
                },
                body: JSON.stringify({ channel_id: CRYPTIC_SCIENCE_CHANNEL_ID, sync: true }),
            });
            await fetchHub();
        } catch (e) {
            setError((e as Error).message);
        } finally {
            setRefreshing(false);
        }
    }, [accessToken, fetchHub]);

    const channel = useMemo(() => {
        const channels = payload?.channels || [];
        return channels.find((c) => c.channel_id === CRYPTIC_SCIENCE_CHANNEL_ID) || payload?.selected_channel || channels[0];
    }, [payload]);

    const uploads: UploadedVideo[] = useMemo(() => {
        const list = (channel as any)?.analytics_snapshot?.uploaded_videos || [];
        return [...list].sort((a, b) => {
            const aRate = a.views ? ((a.likes || 0) / a.views) * 100 : 0;
            const bRate = b.views ? ((b.likes || 0) / b.views) * 100 : 0;
            return bRate - aRate;
        });
    }, [channel]);

    const alreadyUploadedTitles = useMemo(
        () => uploads.map((u) => u.title || '').filter(Boolean),
        [uploads],
    );

    // Topic generator
    const [genLoading, setGenLoading] = useState(false);
    const [topics, setTopics] = useState<GeneratedTopic[]>([]);
    const [genError, setGenError] = useState('');
    const [genBaseline, setGenBaseline] = useState<{ channel_top_lr?: number; channel_avg_lr?: number; uploads_considered?: number } | null>(null);
    // PR #144 — Catalyst Learning Loop visibility for Cryptic Science.
    // Shows which alt-battles era/commander/terrain/tech-gap patterns are
    // hitting on this channel based on real upload like-rates.
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

    const generateTopics = useCallback(async () => {
        if (!accessToken) {
            alert('Sign in first.');
            return;
        }
        setGenLoading(true);
        setGenError('');
        setTopics([]);
        try {
            const r = await fetch(`${API}/api/alt-history-private/generate-topics`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${accessToken}`,
                },
                body: JSON.stringify({ count: 8 }),
            });
            if (!r.ok) {
                const txt = await r.text().catch(() => '');
                throw new Error(`generate-topics failed: ${r.status} ${txt.slice(0, 200)}`);
            }
            const d = await r.json();
            const raw: string[] = Array.isArray(d.topics) ? d.topics : [];
            const scored = raw.map((t) => ({ topic: t, score: scoreAltTopic(t, alreadyUploadedTitles) }));
            scored.sort((a, b) => b.score.score - a.score.score);
            setTopics(scored);
            setGenBaseline(d.channel_baseline || null);
            setGenCalibration(d.calibration || null);
        } catch (e) {
            setGenError((e as Error).message);
        } finally {
            setGenLoading(false);
        }
    }, [accessToken, alreadyUploadedTitles]);

    const buildWithTopic = useCallback((topic: string) => {
        // Hand off to the public alt_battles niche (CreatePanel) with the
        // topic prefilled. The Create tab handles the actual render via
        // the existing /api/skeleton-ai/* pipeline.
        if (onPickNiche) {
            onPickNiche('alt_battles', { topic });
        } else {
            // Fallback: navigate via URL param so DashboardPage's URL-driven
            // selectedNiche hydration picks it up.
            const u = new URL(window.location.href);
            u.searchParams.set('niche', 'alt_battles');
            u.searchParams.set('topic', topic);
            window.location.href = u.toString();
        }
    }, [onPickNiche]);

    if (loading) {
        return (
            <div className="flex items-center justify-center py-20 text-zinc-500">
                <Loader2 className="h-6 w-6 animate-spin mr-2" /> Loading Cryptic Science analytics…
            </div>
        );
    }

    const connectChannel = () => {
        setConnecting(true);
        try {
            const nextUrl = new URL(window.location.href);
            nextUrl.searchParams.set('niche', 'alt_history_private');
            nextUrl.searchParams.set('youtube_channel_id', CRYPTIC_SCIENCE_CHANNEL_ID);
            startYouTubeBrowserConnect(accessToken, nextUrl.toString());
        } finally {
            setConnecting(false);
        }
    };

    const subs = (channel as any)?.subscriber_count || 0;
    const views = (channel as any)?.view_count || 0;
    const videos = (channel as any)?.video_count || 0;
    const channelTitle = (channel as any)?.channel_title || 'Cryptic Science';
    const isConnected = !!channel?.channel_id;

    return (
        <div className="flex flex-col gap-6 px-6 py-8 max-w-5xl mx-auto">
            <header className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-white flex items-center gap-2">
                        <Sparkles className="h-5 w-5 text-violet-300" />
                        Alt-History (Private)
                    </h1>
                    <p className="text-xs text-zinc-500 mt-1">
                        Catalyst-fed alt-battles surface for {channelTitle} — admin only.
                    </p>
                </div>
                <div className="flex items-center gap-2">
                    {/* Always-visible re-OAuth button. When the snapshot is
                        stale (e.g. Casey's Cryptic Science showed 1 of 19
                        uploads after he deleted videos and the bucket
                        didn't refresh), clicking this re-runs the Google
                        OAuth flow and rehydrates the bucket on return. */}
                    <button
                        onClick={connectChannel}
                        disabled={connecting}
                        title="Re-run Google OAuth to refresh this channel's analytics snapshot"
                        className="rounded-md bg-violet-500/10 hover:bg-violet-500/20 border border-violet-500/30 px-3 py-1.5 text-xs font-semibold text-violet-200 flex items-center gap-1.5 disabled:opacity-50"
                    >
                        <Link2 className="h-3 w-3" />
                        {connecting ? 'Opening Google…' : 'Re-auth channel'}
                    </button>
                    <button
                        onClick={refresh}
                        disabled={refreshing}
                        className="rounded-md bg-zinc-900 hover:bg-zinc-800 border border-zinc-800 px-3 py-1.5 text-xs font-semibold text-zinc-300 flex items-center gap-1.5 disabled:opacity-50"
                    >
                        <RefreshCw className={`h-3 w-3 ${refreshing ? 'animate-spin' : ''}`} />
                        {refreshing ? 'Syncing…' : 'Sync channel'}
                    </button>
                </div>
            </header>

            {error && (
                <div className="rounded-md bg-rose-500/10 border border-rose-500/30 px-3 py-2 text-sm text-rose-200 flex items-start gap-2">
                    <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                    {error}
                </div>
            )}

            {/* Channel header */}
            <section className="rounded-md border border-zinc-800 bg-zinc-950 p-4 flex items-center justify-between">
                <div className="flex items-center gap-4">
                    <div className="rounded-full bg-violet-500/10 border border-violet-500/30 p-3">
                        <Youtube className="h-5 w-5 text-violet-300" />
                    </div>
                    <div>
                        <div className="text-sm font-semibold text-white">{channelTitle}</div>
                        <div className="text-xs text-zinc-400">
                            {subs.toLocaleString()} subs · {views.toLocaleString()} total views · {videos} uploads
                        </div>
                        {!isConnected && (
                            <button
                                onClick={connectChannel}
                                disabled={connecting}
                                className="mt-2 rounded-md bg-violet-500 hover:bg-violet-600 disabled:bg-zinc-800 px-3 py-1 text-[11px] font-semibold text-white"
                            >
                                {connecting ? 'Opening…' : 'Connect Cryptic Science via OAuth'}
                            </button>
                        )}
                    </div>
                </div>
            </section>

            {/* Recent uploads sorted by like-rate */}
            {uploads.length > 0 && (
                <section className="flex flex-col gap-2">
                    <div className="flex items-center justify-between">
                        <h2 className="text-sm font-semibold text-white flex items-center gap-1.5">
                            <TrendingUp className="h-4 w-4 text-emerald-400" />
                            Recent uploads — sorted by like-rate
                        </h2>
                        <span className="text-[10px] text-zinc-500">
                            top of this list = template for new topic ideas
                        </span>
                    </div>
                    <div className="rounded-md border border-zinc-800 bg-zinc-950 divide-y divide-zinc-800">
                        {uploads.slice(0, 10).map((u, i) => {
                            const lr = u.views ? ((u.likes || 0) / u.views) * 100 : 0;
                            return (
                                <div key={u.video_id || i} className="px-3 py-2 flex items-center gap-3">
                                    <div className="text-zinc-600 text-[10px] font-mono w-4">{i + 1}</div>
                                    <div className="flex-1 min-w-0">
                                        <div className="text-xs text-white truncate" title={u.title}>
                                            {u.title || '(no title)'}
                                        </div>
                                        <div className="text-[10px] text-zinc-500">
                                            {(u.views || 0).toLocaleString()} views · {u.likes || 0} likes
                                            · {(u.comments || 0).toLocaleString()} comments
                                        </div>
                                    </div>
                                    <div className={`text-[11px] font-mono font-semibold ${
                                        lr >= 3.0 ? 'text-emerald-300'
                                            : lr >= 2.0 ? 'text-violet-300'
                                            : lr >= 1.0 ? 'text-zinc-300'
                                            : 'text-zinc-500'
                                    }`}>
                                        {lr.toFixed(2)}%
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                </section>
            )}

            {/* Topic generator */}
            <section className="flex flex-col gap-3">
                <div className="flex items-center justify-between">
                    <h2 className="text-sm font-semibold text-white flex items-center gap-1.5">
                        <Lightbulb className="h-4 w-4 text-amber-300" />
                        Generate alt-battles topic ideas
                    </h2>
                    {genBaseline && (
                        <span className="text-[10px] text-zinc-500">
                            channel baseline: top {(genBaseline.channel_top_lr || 0).toFixed(2)}% · avg {(genBaseline.channel_avg_lr || 0).toFixed(2)}%
                            {genBaseline.uploads_considered != null && ` · ${genBaseline.uploads_considered} uploads`}
                        </span>
                    )}
                </div>
                <button
                    onClick={generateTopics}
                    disabled={genLoading}
                    className="rounded-md bg-violet-500 hover:bg-violet-600 disabled:bg-zinc-800 disabled:text-zinc-500 px-4 py-2.5 text-sm font-semibold text-white flex items-center justify-center gap-2"
                >
                    {genLoading ? (
                        <><Loader2 className="h-4 w-4 animate-spin" /> Grok generating 8 topics…</>
                    ) : (
                        <><Sparkles className="h-4 w-4" /> Generate 8 topic ideas (channel-tuned)</>
                    )}
                </button>
                {genError && (
                    <div className="rounded-md bg-rose-500/10 border border-rose-500/30 px-3 py-2 text-xs text-rose-200">
                        {genError}
                    </div>
                )}
                {/* PR #144 — Catalyst Learning Loop visibility for Cryptic
                    Science. Shows which alt-battles patterns hit/missed
                    based on real upload like-rates from this channel. */}
                {genCalibration && Array.isArray(genCalibration.buckets) && genCalibration.buckets.length > 0 && (
                    <div className="rounded-md border border-violet-500/30 bg-black/30 p-3">
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
                {topics.length > 0 && (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                        {topics.map((t, i) => {
                            const tier = t.score.score >= 80 ? 'green' : t.score.score >= 60 ? 'violet' : t.score.score >= 30 ? 'zinc' : 'rose';
                            const tierBorder = {
                                green: 'border-emerald-500/40 bg-emerald-500/5',
                                violet: 'border-violet-500/40 bg-violet-500/5',
                                zinc: 'border-zinc-800 bg-zinc-950',
                                rose: 'border-rose-500/30 bg-rose-500/5',
                            }[tier];
                            const tierText = {
                                green: 'text-emerald-300',
                                violet: 'text-violet-300',
                                zinc: 'text-zinc-300',
                                rose: 'text-rose-300',
                            }[tier];
                            return (
                                <div
                                    key={i}
                                    className={`rounded-md border ${tierBorder} p-3 flex flex-col gap-2`}
                                >
                                    <div className="flex items-start justify-between gap-2">
                                        <div className="text-sm text-white font-medium flex-1">
                                            {t.topic}
                                        </div>
                                        <div className={`text-xs font-mono font-bold ${tierText}`}>
                                            {t.score.score}
                                        </div>
                                    </div>
                                    <div className="text-[10px] text-zinc-500">
                                        predicted LR ~{t.score.predicted_lr.toFixed(1)}%
                                        {t.score.flags.length > 0 && (
                                            <span className="ml-2">· {t.score.flags.join(' · ')}</span>
                                        )}
                                    </div>
                                    <button
                                        onClick={() => buildWithTopic(t.topic)}
                                        className="rounded-md bg-zinc-800 hover:bg-violet-500 px-2.5 py-1 text-[11px] font-semibold text-zinc-200 hover:text-white flex items-center justify-center gap-1 transition-colors"
                                    >
                                        Build with this topic <ArrowRight className="h-3 w-3" />
                                    </button>
                                </div>
                            );
                        })}
                    </div>
                )}
            </section>

            <div className="rounded-md bg-zinc-900/50 border border-zinc-800 px-4 py-3 text-xs text-zinc-400">
                <strong className="text-zinc-300">Build path:</strong> clicking "Build with this topic" hands the
                topic off to the public Alt-History Battles niche, which uses the existing skeleton-ai render
                pipeline (Grok script → seedream stills → Seedance/LTX i2v → ElevenLabs VO → ffmpeg compose).
                Same render flow as the public version — this panel is just the channel-aware topic discovery on top.
            </div>
        </div>
    );
}
