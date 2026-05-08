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
import { AlertTriangle, Lightbulb, Loader2, RefreshCw, Sparkles, TrendingUp, X, Youtube, Zap } from 'lucide-react';
import { API, AuthContext, startYouTubeBrowserConnect } from '../shared';

const ZEROTIER_CHANNEL_ID = 'UC9Gth_4MVet6rdPH7MHJf-g';

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

export default function ZeroTierPrivatePanel() {
    const { session } = useContext(AuthContext);
    const accessToken = session?.access_token || '';

    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [connecting, setConnecting] = useState(false);
    const [error, setError] = useState('');
    const [payload, setPayload] = useState<CatalystHubPayload | null>(null);
    const retriedRef = useRef(false);

    // Build-This-Short modal state
    const [buildModalOpen, setBuildModalOpen] = useState(false);
    const [buildTopic, setBuildTopic] = useState('');
    const [buildScript, setBuildScript] = useState('');
    const [buildError, setBuildError] = useState('');
    const [buildLoading, setBuildLoading] = useState(false);
    // Phase 2b: render state
    const [renderLoading, setRenderLoading] = useState(false);
    const [renderResult, setRenderResult] = useState<null | {
        job_id: string;
        title?: string;
        mp4_url?: string;
        scene_count?: number;
        duration_total_sec?: number;
        fal_cost_estimate_usd?: number;
    }>(null);

    const channel = useMemo(() => {
        const channels = payload?.channels || [];
        return channels.find((c) => c.channel_id === ZEROTIER_CHANNEL_ID) || payload?.selected_channel || channels[0];
    }, [payload]);

    const snapshot = channel?.analytics_snapshot;
    const audit = snapshot?.channel_audit;
    const candidates = audit?.next_video_candidates || [];

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

    const onBuildShort = (title: string) => {
        // Open the build modal pre-loaded with the candidate topic.
        setBuildTopic(title);
        setBuildScript('');
        setBuildError('');
        setRenderResult(null);
        setBuildModalOpen(true);
    };

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
        if (buildLoading || renderLoading) return; // don't close mid-stream/mid-render
        setBuildModalOpen(false);
    }, [buildLoading, renderLoading]);

    const renderBuildShort = useCallback(async () => {
        if (!accessToken || !buildScript.trim() || renderLoading) return;
        setBuildError('');
        setRenderResult(null);
        setRenderLoading(true);
        try {
            // The script content the user is looking at is already pretty-printed
            // JSON. Re-stringify it (round-trips through JSON.parse to catch
            // edits the user may have made + normalize formatting).
            let scriptForBackend = buildScript.trim();
            try {
                const parsed = JSON.parse(scriptForBackend);
                scriptForBackend = JSON.stringify(parsed);
            } catch {
                // not valid JSON — let the backend reject it
            }
            const r = await fetch(`${API}/api/zerotier-private/render`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${accessToken}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ script_json: scriptForBackend }),
                // Render takes 5-10 min. Browser fetch has no built-in timeout
                // so this just waits. AbortController could be added for a Stop
                // button later.
            });
            const data = await r.json();
            if (!r.ok) throw new Error(String(data?.detail || data?.error || `Render failed (${r.status})`));
            setRenderResult({
                job_id: String(data?.job_id || ''),
                title: data?.title,
                mp4_url: data?.mp4_url ? `${API}${data.mp4_url}` : undefined,
                scene_count: data?.scene_count,
                duration_total_sec: data?.duration_total_sec,
                fal_cost_estimate_usd: data?.fal_cost_estimate_usd,
            });
        } catch (e: any) {
            setBuildError(String(e?.message || e || 'Render failed'));
        } finally {
            setRenderLoading(false);
        }
    }, [accessToken, buildScript, renderLoading]);

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

                    {/* Recommended next-shorts */}
                    {candidates.length > 0 && (
                        <section className="rounded-2xl border border-violet-500/30 bg-violet-500/[0.04] p-5">
                            <div className="flex items-center gap-2 mb-3">
                                <Sparkles className="h-5 w-5 text-violet-300" />
                                <h2 className="text-lg font-bold text-white">Catalyst recommends</h2>
                                <span className="text-xs text-zinc-500">— scored by virality potential vs your channel baseline</span>
                            </div>
                            <div className="grid gap-3 sm:grid-cols-1 lg:grid-cols-2">
                                {candidates.slice(0, 6).map((title, i) => (
                                    <div
                                        key={`cand-${i}`}
                                        className="rounded-xl border border-white/[0.08] bg-black/20 p-4"
                                    >
                                        <div className="flex items-start gap-2">
                                            <Lightbulb className="h-4 w-4 shrink-0 text-amber-300 mt-0.5" />
                                            <div className="flex-1 min-w-0">
                                                <h3 className="text-sm font-semibold text-white leading-snug">{title}</h3>
                                            </div>
                                        </div>
                                        <button
                                            type="button"
                                            onClick={() => onBuildShort(title)}
                                            className="mt-3 inline-flex items-center gap-1.5 rounded-md border border-violet-500/40 bg-violet-500/10 px-3 py-1.5 text-xs font-semibold text-violet-200 transition hover:border-violet-400 hover:bg-violet-500/20"
                                        >
                                            <Sparkles className="h-3.5 w-3.5" />
                                            Build This Short
                                        </button>
                                    </div>
                                ))}
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
                                disabled={buildLoading || renderLoading || !buildTopic.trim()}
                                className="inline-flex items-center gap-2 rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-violet-500 disabled:opacity-50"
                            >
                                {buildLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
                                {buildLoading ? 'Generating with Grok…' : (buildScript ? 'Regenerate Script' : 'Generate Script')}
                            </button>
                            <button
                                type="button"
                                onClick={renderBuildShort}
                                disabled={!buildScript.trim() || renderLoading || buildLoading}
                                title="Renders the full short via canonical pipeline. Takes 5-10 minutes."
                                className="inline-flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-emerald-500 disabled:opacity-50"
                            >
                                {renderLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
                                {renderLoading ? 'Rendering (5-10 min)…' : 'Render This Short (~$2)'}
                            </button>
                        </div>

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
