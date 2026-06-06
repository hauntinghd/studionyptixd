import { useContext, useEffect, useState } from 'react';
import { BarChart3, Eye, Loader2, Sparkles, Users, Video, Wand2 } from 'lucide-react';
import { AuthContext, resolveStudioBackendUrl } from '../../shared';

type ChannelRow = {
    channel_id: string;
    channel_title: string;
    subscriber_count: number;
    view_count: number;
    video_count: number;
    harvest_present: boolean;
    registry_key?: string;
    registry_label?: string;
    registry_format?: string;
};

function compact(n: number): string {
    const v = Number(n || 0);
    if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(v % 1_000_000 === 0 ? 0 : 1)}M`;
    if (v >= 1_000) return `${(v / 1_000).toFixed(v % 1_000 === 0 ? 0 : 1)}K`;
    return v.toLocaleString();
}

function Stat({ icon: Icon, label, value }: { icon: typeof Users; label: string; value: string }) {
    return (
        <div className="flex items-center gap-2 rounded-lg border border-white/[0.06] bg-white/[0.02] px-3 py-2">
            <Icon className="h-4 w-4 shrink-0 text-cyan-300/80" />
            <div className="min-w-0">
                <p className="text-[10px] uppercase tracking-wide text-gray-500">{label}</p>
                <p className="text-sm font-bold tabular-nums text-white">{value}</p>
            </div>
        </div>
    );
}

export default function ChannelHomePanel({
    onOpenAgent,
}: {
    onOpenAgent?: () => void;
    isAdmin?: boolean;
}) {
    const { session } = useContext(AuthContext);
    const [channels, setChannels] = useState<ChannelRow[] | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');

    useEffect(() => {
        const tok = session?.access_token;
        if (!tok) {
            setLoading(false);
            return;
        }
        let cancelled = false;
        (async () => {
            setLoading(true);
            setError('');
            try {
                const res = await fetch(resolveStudioBackendUrl('/api/studio/analytics/channels'), {
                    headers: { Authorization: `Bearer ${tok}` },
                });
                if (!res.ok) {
                    if (!cancelled) {
                        if (res.status === 403) {
                            setError(
                                'Connect YouTube in Settings to see your channel stats here.',
                            );
                        } else if (res.status === 429) {
                            setError(
                                'Channel analytics is rate-limited on the API bridge — refresh in a minute. '
                                + 'Studio Agent chat still works on Fly.',
                            );
                        } else {
                            setError(`Couldn't load channel analytics (${res.status})`);
                        }
                    }
                    return;
                }
                const data = (await res.json()) as { channels?: ChannelRow[] };
                if (!cancelled) setChannels(Array.isArray(data.channels) ? data.channels : []);
            } catch {
                if (!cancelled) setError("Couldn't reach the analytics service");
            } finally {
                if (!cancelled) setLoading(false);
            }
        })();
        return () => {
            cancelled = true;
        };
    }, [session?.access_token]);

    return (
        <section className="space-y-4">
            <div className="flex flex-wrap items-end justify-between gap-3">
                <div>
                    <h2 className="text-lg font-bold text-white">Your channel</h2>
                    <p className="mt-1 max-w-2xl text-sm text-gray-500">
                        The Studio Agent now drives every video — no niche tiles to pick. It analyzes your live
                        analytics and competitors, then engineers content built to go viral.
                    </p>
                </div>
                {onOpenAgent && session && (
                    <button
                        type="button"
                        onClick={onOpenAgent}
                        className="inline-flex items-center gap-2 rounded-xl bg-gradient-to-r from-violet-600 to-cyan-600 px-4 py-2.5 text-sm font-semibold text-white shadow-lg shadow-violet-900/30 transition hover:from-violet-500 hover:to-cyan-500"
                    >
                        <Wand2 className="h-4 w-4" />
                        Open Studio Agent
                    </button>
                )}
            </div>

            {loading && (
                <div className="flex h-32 items-center justify-center gap-2 rounded-2xl border border-white/[0.06] bg-white/[0.02] text-sm text-gray-500">
                    <Loader2 className="h-4 w-4 animate-spin" /> Loading channel analytics…
                </div>
            )}

            {!loading && error && (
                <div className="rounded-2xl border border-amber-500/20 bg-amber-500/[0.06] px-4 py-3 text-sm text-amber-200/80">
                    {error}
                </div>
            )}

            {!loading && !error && channels && channels.length === 0 && (
                <div className="flex flex-col items-start gap-3 rounded-2xl border border-white/[0.08] bg-white/[0.02] px-5 py-6">
                    <div className="flex items-center gap-2 text-white">
                        <Sparkles className="h-5 w-5 text-cyan-300" />
                        <span className="font-semibold">No channel connected yet</span>
                    </div>
                    <p className="max-w-xl text-sm text-gray-500">
                        Connect a YouTube channel so the Studio Agent can read your real analytics — top videos,
                        retention, and packaging — and tailor every recommendation to your audience.
                    </p>
                </div>
            )}

            {!loading && !error && channels && channels.length > 0 && (
                <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                    {channels.map((c) => (
                        <div
                            key={c.channel_id}
                            className="rounded-2xl border border-white/[0.08] bg-gradient-to-br from-white/[0.03] to-transparent p-5"
                        >
                            <div className="flex items-start justify-between gap-3">
                                <div className="min-w-0">
                                    <p className="truncate text-base font-bold text-white">{c.channel_title}</p>
                                    {c.registry_label && (
                                        <p className="mt-0.5 text-xs text-gray-500">
                                            {c.registry_label}
                                            {c.registry_format ? ` · ${c.registry_format}` : ''}
                                        </p>
                                    )}
                                </div>
                                {c.harvest_present && (
                                    <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/20 bg-emerald-500/10 px-2 py-1 text-[10px] font-semibold text-emerald-200">
                                        <BarChart3 className="h-3 w-3" /> Analytics live
                                    </span>
                                )}
                            </div>
                            <div className="mt-4 grid grid-cols-3 gap-2">
                                <Stat icon={Users} label="Subscribers" value={compact(c.subscriber_count)} />
                                <Stat icon={Eye} label="Views" value={compact(c.view_count)} />
                                <Stat icon={Video} label="Videos" value={compact(c.video_count)} />
                            </div>
                        </div>
                    ))}
                </div>
            )}
        </section>
    );
}
