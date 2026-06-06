import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import {
    BarChart3, Eye, Loader2, RefreshCw, Search, TrendingUp, ChevronDown, Sparkles,
} from 'lucide-react';
import { API, AuthContext, resolveStudioBackendUrl } from '../shared';

interface ConnectedChannel {
    channel_id: string;
    channel_title: string;
    subscriber_count: number;
    view_count: number;
    video_count: number;
    harvest_present: boolean;
    registry_key: string;
    registry_label: string;
}

interface ChannelInsights {
    top_titles?: { title: string; views: number }[];
    breakout_titles?: { title: string; lift_vs_baseline: number }[];
    hook_patterns?: string[];
    thumbnail_signals?: string[];
    subscribers?: number;
    channel_views?: number;
    harvest_present?: boolean;
}

interface SearchVideo {
    title: string;
    channel: string;
    published_at?: string;
    watch_url?: string;
    query?: string;
}

interface PredictedTopic {
    topic: string;
    composite_score: number;
    niche_score: number;
    gap_score: number;
    trend_score: number;
}

export default function AdminAnalyticsPanel() {
    const { session, backendOffline } = useContext(AuthContext);
    const [channels, setChannels] = useState<ConnectedChannel[]>([]);
    const [selectedChannelId, setSelectedChannelId] = useState('');
    const [insights, setInsights] = useState<ChannelInsights | null>(null);
    const [velocity, setVelocity] = useState<Record<string, unknown> | null>(null);
    const [searchTrends, setSearchTrends] = useState<SearchVideo[]>([]);
    const [predictions, setPredictions] = useState<PredictedTopic[]>([]);
    const [productData, setProductData] = useState<any>(null);
    const [billingAuditRows, setBillingAuditRows] = useState<any[]>([]);
    const [loading, setLoading] = useState(true);
    const [channelLoading, setChannelLoading] = useState(false);
    const [error, setError] = useState('');
    const [productOpen, setProductOpen] = useState(false);
    const [bannerEnabled, setBannerEnabled] = useState(false);
    const [bannerMessage, setBannerMessage] = useState(
        'Studio is under high load. Queue times may be longer than usual while we scale capacity.',
    );
    const [savingBanner, setSavingBanner] = useState(false);

    const selectedChannel = useMemo(
        () => channels.find((c) => c.channel_id === selectedChannelId) || null,
        [channels, selectedChannelId],
    );

    const authHeaders = useCallback(() => ({
        Authorization: `Bearer ${session?.access_token}`,
    }), [session]);

    const loadChannels = useCallback(async () => {
        if (!session || backendOffline) return;
        const res = await fetch(resolveStudioBackendUrl('/api/studio/analytics/channels'), { headers: authHeaders() });
        if (!res.ok) throw new Error(`Channels failed (${res.status})`);
        const data = await res.json();
        const list = Array.isArray(data?.channels) ? data.channels : [];
        setChannels(list);
        setSelectedChannelId((prev) => prev || (list[0]?.channel_id ?? ''));
    }, [session, backendOffline, authHeaders]);

    const loadChannelDetail = useCallback(async (channelId: string, registryKey: string) => {
        if (!session || !channelId) return;
        setChannelLoading(true);
        try {
            const params = new URLSearchParams({ channel_id: channelId });
            if (registryKey) params.set('registry_key', registryKey);
            const [chRes, trendRes] = await Promise.all([
                fetch(resolveStudioBackendUrl(`/api/studio/analytics/channel?${params}`), { headers: authHeaders() }),
                fetch(
                    resolveStudioBackendUrl(
                        `/api/studio/analytics/search-trends?days=30&registry_key=${encodeURIComponent(registryKey)}`,
                    ),
                    { headers: authHeaders() },
                ),
            ]);
            if (chRes.ok) {
                const chData = await chRes.json();
                setInsights(chData?.insights || null);
                setVelocity(chData?.velocity || null);
            }
            if (trendRes.ok) {
                const tData = await trendRes.json();
                const flat: SearchVideo[] = [];
                const byQuery = tData?.results_by_query || {};
                Object.values(byQuery).forEach((rows: unknown) => {
                    if (Array.isArray(rows)) flat.push(...rows);
                });
                setSearchTrends(flat.slice(0, 24));
                setPredictions(Array.isArray(tData?.predicted_topics) ? tData.predicted_topics : []);
            }
        } catch (e: unknown) {
            setError((e as Error)?.message || 'Failed to load channel analytics');
        } finally {
            setChannelLoading(false);
        }
    }, [session, authHeaders]);

    const loadProduct = useCallback(async () => {
        if (!session || backendOffline) return;
        const res = await fetch(resolveStudioBackendUrl('/api/studio/analytics/product'), { headers: authHeaders() });
        if (!res.ok) return;
        const payload = await res.json();
        setProductData(payload);
        const auditRes = await fetch(`${API}/api/admin/billing-audit`, { headers: authHeaders() });
        if (auditRes.ok) {
            const audit = await auditRes.json();
            setBillingAuditRows(Array.isArray(audit?.rows) ? audit.rows : []);
        }
        setBannerEnabled(Boolean(payload.maintenance_banner_enabled));
        setBannerMessage(
            (payload.maintenance_banner_message || '').trim()
            || 'Studio is under high load. Queue times may be longer than usual while we scale capacity.',
        );
    }, [session, backendOffline, authHeaders]);

    const refreshAll = useCallback(async () => {
        if (!session) return;
        setLoading(true);
        setError('');
        try {
            await Promise.all([loadChannels(), loadProduct()]);
        } catch (e: unknown) {
            setError((e as Error)?.message || 'Failed to load analytics');
        } finally {
            setLoading(false);
        }
    }, [session, loadChannels, loadProduct]);

    useEffect(() => { refreshAll(); }, [refreshAll]);

    useEffect(() => {
        if (!selectedChannelId) return;
        loadChannelDetail(selectedChannelId, selectedChannel?.registry_key || '');
    }, [selectedChannelId, selectedChannel?.registry_key, loadChannelDetail]);

    const saveBanner = useCallback(async () => {
        if (!session) return;
        setSavingBanner(true);
        try {
            const res = await fetch(`${API}/api/admin/maintenance-banner`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', ...authHeaders() },
                body: JSON.stringify({ enabled: bannerEnabled, message: bannerMessage }),
            });
            if (!res.ok) throw new Error('Failed to save banner');
            await loadProduct();
        } catch (e: unknown) {
            setError((e as Error)?.message || 'Failed to save banner');
        } finally {
            setSavingBanner(false);
        }
    }, [session, bannerEnabled, bannerMessage, authHeaders, loadProduct]);

    const formatUsd = (v: number) =>
        `$${Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

    const subscribers = productData?.subscribers_by_tier || {};

    return (
        <div className="mx-auto max-w-5xl space-y-6 px-2 pb-10 sm:px-6">
            <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                    <h2 className="text-xl font-bold text-white">Analytics & Insights</h2>
                    <p className="text-sm text-gray-500">
                        Channel performance, public search demand (30 days), and predicted topics.
                    </p>
                </div>
                <button
                    type="button"
                    onClick={refreshAll}
                    className="inline-flex items-center gap-2 rounded-lg bg-white/5 px-4 py-2 text-sm text-gray-300 transition hover:bg-white/10"
                >
                    <RefreshCw className="h-4 w-4" /> Refresh
                </button>
            </div>

            {loading && (
                <p className="flex items-center gap-2 text-sm text-gray-500">
                    <Loader2 className="h-4 w-4 animate-spin" /> Loading…
                </p>
            )}
            {error && <p className="text-sm text-red-400">{error}</p>}

            {/* Channel picker */}
            <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4">
                <label className="block text-xs font-semibold uppercase tracking-wider text-gray-500">
                    YouTube channel
                    <select
                        className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2.5 text-sm text-white"
                        value={selectedChannelId}
                        onChange={(e) => setSelectedChannelId(e.target.value)}
                    >
                        {channels.length === 0 && (
                            <option value="">No connected channels — link in Settings</option>
                        )}
                        {channels.map((c) => (
                            <option key={c.channel_id} value={c.channel_id}>
                                {c.channel_title || c.channel_id}
                                {c.registry_label ? ` · ${c.registry_label}` : ''}
                                {c.harvest_present ? '' : ' · pending harvest'}
                            </option>
                        ))}
                    </select>
                </label>
                {selectedChannel && (
                    <p className="mt-2 text-xs text-gray-500">
                        {selectedChannel.subscriber_count.toLocaleString()} subs ·{' '}
                        {selectedChannel.view_count.toLocaleString()} views ·{' '}
                        {selectedChannel.video_count} videos
                        {selectedChannel.registry_key ? ` · registry: ${selectedChannel.registry_key}` : ''}
                    </p>
                )}
            </div>

            {channelLoading && (
                <p className="flex items-center gap-2 text-sm text-gray-500">
                    <Loader2 className="h-4 w-4 animate-spin" /> Loading channel insights…
                </p>
            )}

            {/* Channel insights */}
            {insights && !channelLoading && (
                <section className="space-y-3">
                    <h3 className="flex items-center gap-2 text-sm font-semibold text-white">
                        <BarChart3 className="h-4 w-4 text-violet-400" /> Channel insights
                    </h3>
                    {!insights.harvest_present ? (
                        <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                            Catalyst harvest pending for this channel. Connect OAuth in Settings and wait for the first sync — or open Catalyst to force refresh.
                        </div>
                    ) : (
                        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                            {(insights.top_titles?.length || 0) > 0 && (
                                <InsightCard
                                    title="Top performing titles"
                                    icon={<Eye className="h-4 w-4" />}
                                    items={(insights.top_titles || []).slice(0, 5).map((t) => ({
                                        primary: t.title,
                                        secondary: `${t.views.toLocaleString()} views`,
                                    }))}
                                />
                            )}
                            {(insights.breakout_titles?.length || 0) > 0 && (
                                <InsightCard
                                    title="Breakouts"
                                    icon={<TrendingUp className="h-4 w-4" />}
                                    items={(insights.breakout_titles || []).slice(0, 5).map((t) => ({
                                        primary: t.title,
                                        secondary: `${t.lift_vs_baseline.toFixed(1)}× baseline`,
                                    }))}
                                />
                            )}
                            {(insights.hook_patterns?.length || 0) > 0 && (
                                <InsightCard
                                    title="Hook patterns"
                                    icon={<Sparkles className="h-4 w-4" />}
                                    items={(insights.hook_patterns || []).slice(0, 6).map((h) => ({
                                        primary: h, secondary: '',
                                    }))}
                                />
                            )}
                            {velocity && Boolean(velocity.velocity_vph) && (
                                <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4">
                                    <p className="text-xs font-semibold uppercase tracking-wider text-gray-500">Latest upload velocity</p>
                                    <p className="mt-2 text-2xl font-bold text-white">
                                        {Number(velocity.velocity_vph || 0).toFixed(0)} views/hr
                                    </p>
                                    <p className="mt-1 text-xs text-gray-500">
                                        {String(velocity.title || 'Latest video')}
                                        {velocity.is_decaying ? ' · decaying' : ' · healthy'}
                                    </p>
                                </div>
                            )}
                        </div>
                    )}
                </section>
            )}

            {/* Public search — 30 days */}
            <section className="space-y-3">
                <h3 className="flex items-center gap-2 text-sm font-semibold text-white">
                    <Search className="h-4 w-4 text-emerald-400" /> Public search demand (30 days)
                </h3>
                <p className="text-xs text-gray-500">
                    Recent + high-view public videos matching your channel niche on YouTube (Data API).
                </p>
                {searchTrends.length === 0 ? (
                    <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-6 text-sm text-gray-500">
                        No search trend data yet. Select a channel with a registry key, or check YouTube API key quota.
                    </div>
                ) : (
                    <ul className="divide-y divide-white/[0.06] rounded-xl border border-white/[0.08] bg-white/[0.02]">
                        {searchTrends.map((v, i) => (
                            <li key={`${v.title}-${i}`} className="px-4 py-3">
                                <p className="text-sm text-white">{v.title}</p>
                                <p className="mt-0.5 text-xs text-gray-500">
                                    {v.channel}
                                    {v.published_at ? ` · ${new Date(v.published_at).toLocaleDateString()}` : ''}
                                </p>
                                {v.watch_url && (
                                    <a
                                        href={v.watch_url}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="mt-1 inline-block text-[10px] text-violet-400 hover:underline"
                                    >
                                        Watch on YouTube
                                    </a>
                                )}
                            </li>
                        ))}
                    </ul>
                )}
            </section>

            {/* Predictions */}
            {predictions.length > 0 && (
                <section className="space-y-3">
                    <h3 className="text-sm font-semibold text-white">Predicted topics</h3>
                    <p className="text-xs text-gray-500">
                        Scored by niche fit, gap vs your catalog, and trend momentum from public search.
                    </p>
                    <div className="grid gap-2 sm:grid-cols-2">
                        {predictions.slice(0, 8).map((p) => (
                            <div
                                key={p.topic}
                                className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-3"
                            >
                                <p className="text-sm text-white line-clamp-2">{p.topic}</p>
                                <p className="mt-1 text-xs text-emerald-400">
                                    Score {(p.composite_score * 100).toFixed(0)}%
                                    <span className="text-gray-500">
                                        {' '}· gap {(p.gap_score * 100).toFixed(0)}% · trend {(p.trend_score * 100).toFixed(0)}%
                                    </span>
                                </p>
                            </div>
                        ))}
                    </div>
                </section>
            )}

            {/* Product metrics (collapsed) */}
            <section className="rounded-xl border border-white/[0.08] bg-white/[0.02]">
                <button
                    type="button"
                    onClick={() => setProductOpen((v) => !v)}
                    className="flex w-full items-center justify-between px-4 py-3 text-left"
                >
                    <span className="text-sm font-semibold text-gray-300">Studio product metrics</span>
                    <ChevronDown className={`h-4 w-4 text-gray-500 transition ${productOpen ? 'rotate-180' : ''}`} />
                </button>
                {productOpen && productData && !backendOffline && (
                    <div className="space-y-4 border-t border-white/[0.06] px-4 pb-4 pt-3">
                        <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
                            <MetricCard label="Active users (est.)" value={productData.active_users_estimate || 0} />
                            <MetricCard
                                label="Active generations"
                                value={productData.active_generations || 0}
                                sub={`Queue ${productData.queue_depth || 0}/${productData.queue_max_depth || 0}`}
                            />
                            <MetricCard
                                label="Monthly profit (proxy)"
                                value={formatUsd(productData.monthly_profit_usd || 0)}
                                sub={productData.revenue_source || 'none'}
                                accent
                            />
                        </div>
                        <div className="rounded-lg border border-white/[0.06] bg-black/20 p-3 text-xs text-gray-400">
                            Paid tiers: Starter {subscribers.starter || 0} · Creator {subscribers.creator || 0} · Pro {subscribers.pro} · Total paid {productData.total_paid_subscribers || 0}
                        </div>
                        <div className="space-y-2">
                            <p className="text-xs font-semibold text-gray-400">High load banner</p>
                            <div className="flex items-center gap-3">
                                <button
                                    type="button"
                                    onClick={() => setBannerEnabled((v) => !v)}
                                    className={`rounded-lg px-3 py-1.5 text-xs font-semibold ${bannerEnabled ? 'bg-emerald-600 text-white' : 'bg-white/10 text-gray-300'}`}
                                >
                                    {bannerEnabled ? 'ON' : 'OFF'}
                                </button>
                                <textarea
                                    value={bannerMessage}
                                    onChange={(e) => setBannerMessage(e.target.value)}
                                    rows={2}
                                    className="flex-1 rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-xs text-white"
                                />
                                <button
                                    type="button"
                                    onClick={saveBanner}
                                    disabled={savingBanner}
                                    className="rounded-lg bg-violet-600 px-3 py-2 text-xs text-white disabled:opacity-50"
                                >
                                    Save
                                </button>
                            </div>
                        </div>
                        {billingAuditRows.length > 0 && (
                            <p className="text-xs text-gray-500">{billingAuditRows.length} billing audit rows (full table in legacy admin view).</p>
                        )}
                    </div>
                )}
            </section>
        </div>
    );
}

function InsightCard({
    title, icon, items,
}: {
    title: string;
    icon: React.ReactNode;
    items: { primary: string; secondary: string }[];
}) {
    return (
        <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4">
            <div className="mb-2 flex items-center gap-2 text-xs font-semibold text-gray-300">
                {icon} {title}
            </div>
            <ul className="space-y-1.5">
                {items.map((it, i) => (
                    <li key={i} className="text-xs">
                        <div className="truncate text-gray-200" title={it.primary}>{it.primary}</div>
                        {it.secondary && <div className="text-[10px] text-gray-500">{it.secondary}</div>}
                    </li>
                ))}
            </ul>
        </div>
    );
}

function MetricCard({
    label, value, sub, accent,
}: {
    label: string;
    value: string | number;
    sub?: string;
    accent?: boolean;
}) {
    return (
        <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4">
            <p className="text-xs uppercase tracking-wider text-gray-500">{label}</p>
            <p className={`mt-1 text-2xl font-bold ${accent ? 'text-emerald-400' : 'text-white'}`}>{value}</p>
            {sub && <p className="mt-1 text-xs text-gray-500">{sub}</p>}
        </div>
    );
}
