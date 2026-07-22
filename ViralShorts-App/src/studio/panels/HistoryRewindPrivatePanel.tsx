/**
 * HistoryRewindPrivatePanel — admin-only Catalyst-fed surface for the
 * History Rewind channel (Casey's 9-hour sleep documentary channel).
 * Mirrors AltHistoryPrivatePanel + ZeroTierPrivatePanel UX, but:
 *
 *   - Scores topics by VIEW-VELOCITY (views/day) instead of like-rate.
 *     HR is sleep-doc style — long-tail watch traffic on
 *     ambient-listen / overnight playback. Like-rate isn't a useful
 *     proxy for long-form sleep audiences.
 *   - Locks topic format to the decoded winner pattern:
 *     "[Empire/Topic] — [Hook noun phrase] | History for Sleep | 9 Hours"
 *     (per `D:/recaps/history_rewind/competitor_decode_2026-05-07.md`:
 *     n=93 hits at 543 v/d for 'History for Sleep' suffix vs n=2 hits
 *     at 16 v/d for 'Rise and Fall ... Full Documentary' — strictly
 *     avoid the latter).
 *   - "Build with this topic" hands off to the existing Long-Form
 *     panel (the sleep_doc pipeline already powers HR 9hr renders via
 *     /api/long-form/*). Topic + channel are stashed in sessionStorage
 *     so LongFormPanel can hydrate them on mount.
 *
 * Why a separate panel vs. just using public Long Form:
 *   - Catalyst hub data filtered to History Rewind so topic suggestions
 *     are tuned to the channel's actual top-velocity uploads.
 *   - Topic generator that lists competitor whitespace empires (Khmer,
 *     Phoenicians, Inca, Mughal, Persian, Hittite — zero hits in the
 *     264-vid corpus) and feeds them as preferred picks to Claude.
 *   - Admin gating — keeps this from non-admin users while testing.
 *
 * Backend reuse:
 *   GET  /api/catalyst/hub?channel_id=...               → snapshot
 *   POST /api/catalyst/hub/refresh                      → re-sync
 *   POST /api/history-rewind-private/generate-topics    → direct Anthropic topic gen
 */
import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import {
    AlertTriangle, ArrowRight, Lightbulb, Link2, Loader2, RefreshCw,
    Sparkles, TrendingUp, Youtube,
} from 'lucide-react';
import { API, AuthContext, startYouTubeBrowserConnect } from '../shared';

const HISTORY_REWIND_CHANNEL_ID = 'UCHmwsIGud6CeZ3CIs5cuaUA';
// LongFormPanel reads these sessionStorage keys on mount to pre-fill
// its Outline tab when the user clicks "Build with this topic" here.
const LF_PENDING_TOPIC_KEY = 'longform_pending_topic';
const LF_PENDING_CHANNEL_KEY = 'longform_pending_channel';

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
    predicted_vps: number;  // predicted views/day
    flags: string[];
}

interface GeneratedTopic {
    topic: string;
    score: ViralityScore;
}

// HR sleep-doc heuristic v1. Calibrated from
// `D:/recaps/history_rewind/competitor_decode_2026-05-07.md` (n=264).
// Winner pattern: "[Empire/Topic] — [Hook] | History for Sleep | 9 Hours"
// with rare empire (no zero-hits in 264-vid corpus) + specific hook.
const SUFFIX_LOCK_BONUS = 14;             // "| History for Sleep | 9 Hours"
const RARE_EMPIRE_BONUS = 16;             // Khmer, Phoenicians, Inca, Mughal etc.
const NAMED_HOOK_BONUS = 10;              // "Angkor's Dark Secret", "Sun Kings"
const SPECIFIC_RULER_BONUS = 8;           // "Jayavarman VII", "Cyrus", "Akbar"
const SUFFIX_MISSING_PENALTY = -20;       // no "History for Sleep | 9 Hours"
const RISE_AND_FALL_PENALTY = -25;        // anti-pattern (16 v/d competitor)
const SATURATED_EMPIRE_PENALTY = -12;     // Roman, Greek, Egypt — already saturated
const RANKER_PENALTY = -18;               // "every", "ranked", "top 10"
const ALREADY_UPLOADED_PENALTY = -25;

const RARE_EMPIRES = new Set([
    'khmer', 'angkor', 'jayavarman', 'phoenician', 'phoenicia', 'carthag',
    'inca', 'cusco', 'aztec', 'mexica', 'tenochtitlan', 'maya', 'mughal',
    'akbar', 'aurangzeb', 'achaemenid', 'cyrus', 'darius', 'hittite',
    'olmec', 'etruscan', 'mycenaean', 'minoan', 'sasanian', 'parthian',
    'kushan', 'gupta', 'maurya', 'tang', 'song', 'goguryeo', 'silla',
    'baekje', 'srivijaya', 'majapahit', 'mali', 'songhai', 'ghana empire',
    'aksum', 'nubia', 'kush', 'meroe', 'zimbabwe', 'taino', 'olmec',
]);
const SATURATED_EMPIRES = new Set([
    'roman empire', 'ancient rome', 'ancient greece', 'ancient egypt',
    'mongol empire', 'ottoman empire', 'byzantine',
]);
const SPECIFIC_RULERS = /\b(jayavarman|akbar|aurangzeb|cyrus|darius|xerxes|alexander|hannibal|caesar|augustus|justinian|saladin|tamerlane|genghis|kublai|atahualpa|pachacuti|moctezuma|montezuma|hammurabi|nebuchadnezzar|ashurbanipal|cleopatra|ramses|tutankhamun|qin shi|wu zetian|hatshepsut)\b/i;
const RANKER_MARKERS = new Set(['every ', 'ranked', 'top 10', 'top 5', 'all time']);
const RISE_AND_FALL_PATTERNS = [
    'rise and fall',
    'full documentary',
    'complete documentary',
    'whole documentary',
];

function scoreHRTopic(topic: string, alreadyUploaded: string[]): ViralityScore {
    const t = topic.toLowerCase();
    const flags: string[] = [];
    let score = 50;

    // Suffix lock — the winner pattern from the 264-video decode.
    const hasSuffix = /history for sleep\s*\|\s*9\s*hours?/i.test(topic);
    if (hasSuffix) {
        score += SUFFIX_LOCK_BONUS;
        flags.push('history-for-sleep-suffix');
    } else {
        score += SUFFIX_MISSING_PENALTY;
        flags.push('missing-suffix');
    }

    // Anti-pattern: Casey's old "Rise and Fall ... Full Documentary"
    // format averaged 16 v/d in the corpus — strict no-go.
    for (const p of RISE_AND_FALL_PATTERNS) {
        if (t.includes(p)) {
            score += RISE_AND_FALL_PENALTY;
            flags.push('rise-and-fall-anti-pattern');
            break;
        }
    }

    // Rare-empire bonus — competitor whitespace.
    let rareHit = false;
    for (const w of RARE_EMPIRES) {
        if (t.includes(w)) { rareHit = true; break; }
    }
    if (rareHit) {
        score += RARE_EMPIRE_BONUS;
        flags.push('rare-empire');
    }

    // Saturated-empire penalty — Roman/Greek/Egypt are already covered
    // by every history-for-sleep competitor.
    for (const w of SATURATED_EMPIRES) {
        if (t.includes(w)) {
            score += SATURATED_EMPIRE_PENALTY;
            flags.push('saturated-empire');
            break;
        }
    }

    // Specific ruler / capital city / dynasty by name (vs. generic).
    if (SPECIFIC_RULERS.test(topic)) {
        score += SPECIFIC_RULER_BONUS;
        flags.push('named-ruler');
    }

    // Hook noun phrase — em-dash + descriptor pattern (the
    // competitor_decode shows winners almost always have a punchy
    // hook noun phrase between the topic and the suffix).
    if (/—|–|:\s*[A-Z]/.test(topic)) {
        score += NAMED_HOOK_BONUS;
        flags.push('hook-phrase');
    }

    for (const w of RANKER_MARKERS) {
        if (t.includes(w)) {
            score += RANKER_PENALTY;
            flags.push('ranker-format');
            break;
        }
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
    // Predicted views/day — calibrated against competitor_decode
    // (suffix winners avg 543 v/d; outliers reach 1,500-3,000+ v/d).
    let predicted_vps = 60;
    if (score >= 95) predicted_vps = 1800;
    else if (score >= 80) predicted_vps = 900;
    else if (score >= 60) predicted_vps = 400;
    else if (score >= 30) predicted_vps = 120;
    else predicted_vps = 30;

    return { score: Math.round(score), predicted_vps, flags };
}

interface HistoryRewindPrivatePanelProps {
    onLongformHandoff?: (topic: string) => void;
}

export default function HistoryRewindPrivatePanel({ onLongformHandoff }: HistoryRewindPrivatePanelProps = {}) {
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
            const url = `${API}/api/catalyst/hub?channel_id=${encodeURIComponent(HISTORY_REWIND_CHANNEL_ID)}`;
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
                body: JSON.stringify({ channel_id: HISTORY_REWIND_CHANNEL_ID, sync: true }),
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
        return channels.find((c) => c.channel_id === HISTORY_REWIND_CHANNEL_ID) || payload?.selected_channel || channels[0];
    }, [payload]);

    // Sort by view-velocity (views/day since publish), not like-rate.
    // HR is sleep-doc — long-tail watch is the only metric that
    // matters; like-rate clusters tightly across all uploads.
    const uploads: Array<UploadedVideo & { vps: number }> = useMemo(() => {
        const list: UploadedVideo[] = (channel as any)?.analytics_snapshot?.uploaded_videos || [];
        const now = Date.now();
        return list.map((u) => {
            const views = u.views || 0;
            let days = 1;
            try {
                if (u.published_at) {
                    const pub = new Date(u.published_at).getTime();
                    days = Math.max(1, Math.round((now - pub) / 86_400_000));
                }
            } catch { days = 30; }
            return { ...u, vps: views / days };
        }).sort((a, b) => (b.vps || 0) - (a.vps || 0));
    }, [channel]);

    const alreadyUploadedTitles = useMemo(
        () => uploads.map((u) => u.title || '').filter(Boolean),
        [uploads],
    );

    // Topic generator
    const [genLoading, setGenLoading] = useState(false);
    const [topics, setTopics] = useState<GeneratedTopic[]>([]);
    const [genError, setGenError] = useState('');
    const [genBaseline, setGenBaseline] = useState<{ channel_top_vps?: number; channel_avg_vps?: number; uploads_considered?: number } | null>(null);

    const generateTopics = useCallback(async () => {
        if (!accessToken) {
            alert('Sign in first.');
            return;
        }
        setGenLoading(true);
        setGenError('');
        setTopics([]);
        try {
            const r = await fetch(`${API}/api/history-rewind-private/generate-topics`, {
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
            const scored = raw.map((t) => ({ topic: t, score: scoreHRTopic(t, alreadyUploadedTitles) }));
            scored.sort((a, b) => b.score.score - a.score.score);
            setTopics(scored);
            setGenBaseline(d.channel_baseline || null);
        } catch (e) {
            setGenError((e as Error).message);
        } finally {
            setGenLoading(false);
        }
    }, [accessToken, alreadyUploadedTitles]);

    const buildWithTopic = useCallback((topic: string) => {
        // Stash topic + channel-key in sessionStorage so LongFormPanel
        // hydrates on mount. Then call the parent handoff so the
        // Dashboard switches to the longform tab.
        try {
            sessionStorage.setItem(LF_PENDING_TOPIC_KEY, topic);
            // LongFormPanel's channel registry uses the key
            // 'history_rewind' (not the YouTube channel ID).
            sessionStorage.setItem(LF_PENDING_CHANNEL_KEY, 'history_rewind');
        } catch { /* sessionStorage might be blocked */ }
        if (onLongformHandoff) {
            onLongformHandoff(topic);
        } else {
            // Fallback: navigate via URL param.
            const u = new URL(window.location.href);
            u.searchParams.set('tab', 'longform');
            u.searchParams.set('lf_topic', topic);
            window.location.href = u.toString();
        }
    }, [onLongformHandoff]);

    if (loading) {
        return (
            <div className="flex items-center justify-center py-20 text-zinc-500">
                <Loader2 className="h-6 w-6 animate-spin mr-2" /> Loading History Rewind analytics…
            </div>
        );
    }

    const connectChannel = () => {
        setConnecting(true);
        try {
            const nextUrl = new URL(window.location.href);
            nextUrl.searchParams.set('niche', 'history_rewind_private');
            nextUrl.searchParams.set('youtube_channel_id', HISTORY_REWIND_CHANNEL_ID);
            startYouTubeBrowserConnect(accessToken, nextUrl.toString());
        } finally {
            setConnecting(false);
        }
    };

    const subs = (channel as any)?.subscriber_count || 0;
    const views = (channel as any)?.view_count || 0;
    const videos = (channel as any)?.video_count || 0;
    const channelTitle = (channel as any)?.channel_title || 'History Rewind';
    const isConnected = !!channel?.channel_id;

    return (
        <div className="flex flex-col gap-6 px-6 py-8 max-w-5xl mx-auto">
            <header className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-white flex items-center gap-2">
                        <Sparkles className="h-5 w-5 text-amber-300" />
                        History Rewind (Private)
                    </h1>
                    <p className="text-xs text-zinc-500 mt-1">
                        Catalyst-fed sleep-doc topic surface for {channelTitle} — admin only.
                        Format locked: <span className="text-amber-300/80 font-mono text-[10px]">[Empire] — [Hook] | History for Sleep | 9 Hours</span>
                    </p>
                </div>
                <div className="flex items-center gap-2">
                    {/* Always-visible re-OAuth button. Mirrors the
                        Alt-History (Private) pattern — when the snapshot
                        is stale or OAuth's refresh token is revoked, this
                        re-runs the Google OAuth flow and rehydrates the
                        bucket on return. */}
                    <button
                        onClick={connectChannel}
                        disabled={connecting}
                        title="Re-run Google OAuth to refresh this channel's analytics snapshot"
                        className="rounded-md bg-amber-500/10 hover:bg-amber-500/20 border border-amber-500/30 px-3 py-1.5 text-xs font-semibold text-amber-200 flex items-center gap-1.5 disabled:opacity-50"
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
                    <div className="rounded-full bg-amber-500/10 border border-amber-500/30 p-3">
                        <Youtube className="h-5 w-5 text-amber-300" />
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
                                className="mt-2 rounded-md bg-amber-500 hover:bg-amber-600 disabled:bg-zinc-800 px-3 py-1 text-[11px] font-semibold text-white"
                            >
                                {connecting ? 'Opening…' : 'Connect History Rewind via OAuth'}
                            </button>
                        )}
                    </div>
                </div>
            </section>

            {/* Recent uploads sorted by view-velocity */}
            {uploads.length > 0 && (
                <section className="flex flex-col gap-2">
                    <div className="flex items-center justify-between">
                        <h2 className="text-sm font-semibold text-white flex items-center gap-1.5">
                            <TrendingUp className="h-4 w-4 text-emerald-400" />
                            Recent uploads — sorted by views/day
                        </h2>
                        <span className="text-[10px] text-zinc-500">
                            top of this list = template for new topic ideas
                        </span>
                    </div>
                    <div className="rounded-md border border-zinc-800 bg-zinc-950 divide-y divide-zinc-800">
                        {uploads.slice(0, 10).map((u, i) => {
                            const vps = u.vps || 0;
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
                                        vps >= 500 ? 'text-emerald-300'
                                            : vps >= 200 ? 'text-amber-300'
                                            : vps >= 50 ? 'text-zinc-300'
                                            : 'text-zinc-500'
                                    }`}>
                                        {vps >= 1000 ? `${(vps / 1000).toFixed(1)}k` : vps.toFixed(0)}/day
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
                        Generate sleep-doc topic ideas
                    </h2>
                    {genBaseline && (
                        <span className="text-[10px] text-zinc-500">
                            channel baseline: top {(genBaseline.channel_top_vps || 0).toFixed(0)}/day · avg {(genBaseline.channel_avg_vps || 0).toFixed(0)}/day
                            {genBaseline.uploads_considered != null && ` · ${genBaseline.uploads_considered} uploads`}
                        </span>
                    )}
                </div>
                <button
                    onClick={generateTopics}
                    disabled={genLoading}
                    className="rounded-md bg-amber-500 hover:bg-amber-600 disabled:bg-zinc-800 disabled:text-zinc-500 px-4 py-2.5 text-sm font-semibold text-white flex items-center justify-center gap-2"
                >
                    {genLoading ? (
                        <><Loader2 className="h-4 w-4 animate-spin" /> Claude generating 8 topics…</>
                    ) : (
                        <><Sparkles className="h-4 w-4" /> Generate 8 topic ideas (channel-tuned)</>
                    )}
                </button>
                {genError && (
                    <div className="rounded-md bg-rose-500/10 border border-rose-500/30 px-3 py-2 text-xs text-rose-200">
                        {genError}
                    </div>
                )}
                {topics.length > 0 && (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                        {topics.map((t, i) => {
                            const tier = t.score.score >= 80 ? 'green' : t.score.score >= 60 ? 'amber' : t.score.score >= 30 ? 'zinc' : 'rose';
                            const tierBorder = {
                                green: 'border-emerald-500/40 bg-emerald-500/5',
                                amber: 'border-amber-500/40 bg-amber-500/5',
                                zinc: 'border-zinc-800 bg-zinc-950',
                                rose: 'border-rose-500/30 bg-rose-500/5',
                            }[tier];
                            const tierText = {
                                green: 'text-emerald-300',
                                amber: 'text-amber-300',
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
                                        predicted ~{t.score.predicted_vps >= 1000 ? `${(t.score.predicted_vps / 1000).toFixed(1)}k` : t.score.predicted_vps}/day
                                        {t.score.flags.length > 0 && (
                                            <span className="ml-2">· {t.score.flags.join(' · ')}</span>
                                        )}
                                    </div>
                                    <button
                                        onClick={() => buildWithTopic(t.topic)}
                                        className="rounded-md bg-zinc-800 hover:bg-amber-500 px-2.5 py-1 text-[11px] font-semibold text-zinc-200 hover:text-white flex items-center justify-center gap-1 transition-colors"
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
                topic off to the existing Long-Form panel (pre-fills topic + History Rewind channel via
                sessionStorage), which uses the existing sleep_doc render pipeline (Claude outline → FAL ERNIE
                stills → fal MiniMax 9hr narration → ffmpeg compose). Same render flow as the public
                Long-Form tab — this panel is just the channel-aware topic discovery on top.
            </div>
        </div>
    );
}
