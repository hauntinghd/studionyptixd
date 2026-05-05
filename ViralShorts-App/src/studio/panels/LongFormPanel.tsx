/**
 * LongFormPanel — Long-form (10-540 minute) generator (built 2026-05-05).
 *
 * 3-tab UX mirroring Skeleton AI's CreatePanel:
 *   1. Channel — pick from 6 channels; each shows canonical signature (style,
 *                duration, voice, cost). Catalyst-derived signals (top titles,
 *                breakouts, hook formulas) surface immediately for the picked
 *                channel so the user sees what works on THIS channel before
 *                writing a topic.
 *   2. Outline — topic input + Grok chapter outline (uses Catalyst signals as
 *                system-prompt context so framing matches channel performance).
 *                Editable.
 *   3. Render  — kicks off the v5 pipeline (phase 2 — wires once fal balance
 *                refills). For now shows a clear "render coming soon" state.
 *
 * Backend: /api/long-form/{channels,catalyst-insights,outline,render}.
 *          Catalyst Hub data threads through /api/long-form/catalyst-insights.
 */
import { useCallback, useContext, useEffect, useState } from 'react';
import {
    BookOpen, BrainCircuit, Briefcase, Building2, Clock, Eye, FileText,
    Film, Headphones, Loader2, Music, Search, Sparkles, TrendingUp, Wand2,
} from 'lucide-react';
import { AuthContext } from '../shared';

type LongFormTab = 'channel' | 'outline' | 'render';

interface ChannelInfo {
    key: string;
    label: string;
    tagline: string;
    icon: string;
    default_minutes: number;
    fps: number;
    image_model_default: string;
    i2v_model_default: string;
    voice_provider_default: string;
    cost_estimate_usd: number;
}

interface CatalystInsights {
    top_titles: { title: string; views: number; vps: number; video_id: string }[];
    breakout_titles: { title: string; views: number; lift_vs_baseline: number; video_id: string }[];
    hook_patterns: string[];
    thumbnail_signals: string[];
    subscribers?: number;
    median_vps?: number;
}

interface Chapter {
    index: number;
    title: string;
    minutes: number;
    synopsis: string;
}

interface Outline {
    title: string;
    hook: string;
    chapters: Chapter[];
    tags: string[];
    _parse_error?: boolean;
}

const CHANNEL_ICON: Record<string, typeof BookOpen> = {
    lacuna: Search,
    hidden_cortex: BrainCircuit,
    pb_live: Briefcase,
    lofi_radio: Headphones,
    empire_magnates: Building2,
    history_rewind: BookOpen,
};

export default function LongFormPanel() {
    const { session } = useContext(AuthContext);
    const accessToken = session?.access_token || '';

    const [tab, setTab] = useState<LongFormTab>('channel');
    const [channels, setChannels] = useState<ChannelInfo[]>([]);
    const [selectedChannel, setSelectedChannel] = useState<string>('');
    const [insights, setInsights] = useState<CatalystInsights | null>(null);
    const [insightsLoading, setInsightsLoading] = useState(false);
    const [catalystPresent, setCatalystPresent] = useState<boolean | null>(null);

    const [topic, setTopic] = useState('');
    const [targetMinutes, setTargetMinutes] = useState<number | ''>('');
    const [useCatalystContext, setUseCatalystContext] = useState(true);
    const [outline, setOutline] = useState<Outline | null>(null);
    const [outliningBusy, setOutliningBusy] = useState(false);
    const [outlineError, setOutlineError] = useState('');

    // Load channel registry once we have an auth token.
    useEffect(() => {
        if (!accessToken) return;
        fetch('/api/long-form/channels', {
            headers: { Authorization: `Bearer ${accessToken}` },
        })
            .then((r) => r.json())
            .then((d) => Array.isArray(d.channels) && setChannels(d.channels))
            .catch(() => setChannels([]));
    }, [accessToken]);

    // When a channel is picked, default targetMinutes + fetch Catalyst insights.
    const onPickChannel = useCallback((key: string) => {
        setSelectedChannel(key);
        const ch = channels.find((c) => c.key === key);
        if (ch) setTargetMinutes(ch.default_minutes);
        setOutline(null);
        setOutlineError('');
        if (!accessToken) return;
        setInsightsLoading(true);
        setInsights(null);
        setCatalystPresent(null);
        fetch('/api/long-form/catalyst-insights', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${accessToken}`,
            },
            body: JSON.stringify({ channel_key: key }),
        })
            .then((r) => r.json())
            .then((d) => {
                setInsights(d.insights || null);
                setCatalystPresent(Boolean(d.catalyst_present));
            })
            .catch(() => { setInsights(null); setCatalystPresent(false); })
            .finally(() => setInsightsLoading(false));
    }, [accessToken, channels]);

    const generateOutline = useCallback(async () => {
        if (!selectedChannel) { alert('Pick a channel first.'); return; }
        if (!topic.trim()) { alert('Enter a topic.'); return; }
        if (!accessToken) { alert('Sign in first.'); return; }
        setOutliningBusy(true);
        setOutlineError('');
        setOutline(null);
        try {
            const r = await fetch('/api/long-form/outline', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${accessToken}`,
                },
                body: JSON.stringify({
                    channel_key: selectedChannel,
                    topic: topic.trim(),
                    target_minutes: targetMinutes || undefined,
                    use_catalyst_context: useCatalystContext,
                }),
            });
            if (!r.ok) {
                const txt = await r.text().catch(() => '');
                throw new Error(`outline failed: ${r.status} ${txt.slice(0, 200)}`);
            }
            const d = await r.json();
            setOutline(d.outline as Outline);
        } catch (e) {
            setOutlineError((e as Error).message);
        } finally {
            setOutliningBusy(false);
        }
    }, [selectedChannel, topic, targetMinutes, useCatalystContext, accessToken]);

    return (
        <div className="flex flex-col gap-6 px-6 py-8 max-w-5xl mx-auto">
            <header className="flex items-center justify-between">
                <h1 className="text-2xl font-bold text-white">Long-Form</h1>
                <div className="text-xs text-zinc-400">
                    6 channels · Catalyst-fed outlines · v5 render pipeline
                </div>
            </header>

            <TabRow tab={tab} setTab={setTab} />

            {tab === 'channel' && (
                <ChannelTab
                    channels={channels}
                    selectedChannel={selectedChannel}
                    onPickChannel={onPickChannel}
                    insights={insights}
                    insightsLoading={insightsLoading}
                    catalystPresent={catalystPresent}
                    onContinue={() => setTab('outline')}
                />
            )}
            {tab === 'outline' && (
                <OutlineTab
                    selectedChannel={selectedChannel}
                    channels={channels}
                    topic={topic}
                    setTopic={setTopic}
                    targetMinutes={targetMinutes}
                    setTargetMinutes={setTargetMinutes}
                    useCatalystContext={useCatalystContext}
                    setUseCatalystContext={setUseCatalystContext}
                    insights={insights}
                    onGenerate={generateOutline}
                    outline={outline}
                    setOutline={setOutline}
                    busy={outliningBusy}
                    error={outlineError}
                    onContinue={() => setTab('render')}
                />
            )}
            {tab === 'render' && (
                <RenderTab
                    selectedChannel={selectedChannel}
                    channels={channels}
                    outline={outline}
                />
            )}
        </div>
    );
}

function TabRow({ tab, setTab }: { tab: LongFormTab; setTab: (t: LongFormTab) => void }) {
    const tabs: { id: LongFormTab; label: string; icon: typeof Wand2 }[] = [
        { id: 'channel', label: 'Channel', icon: Sparkles },
        { id: 'outline', label: 'Outline', icon: FileText },
        { id: 'render', label: 'Render', icon: Film },
    ];
    return (
        <div className="flex gap-1 border-b border-zinc-800">
            {tabs.map(({ id, label, icon: Icon }) => (
                <button
                    key={id}
                    onClick={() => setTab(id)}
                    className={`flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                        tab === id
                            ? 'border-violet-500 text-violet-300'
                            : 'border-transparent text-zinc-500 hover:text-zinc-300'
                    }`}
                >
                    <Icon className="h-4 w-4" />
                    {label}
                </button>
            ))}
        </div>
    );
}

function ChannelTab({
    channels, selectedChannel, onPickChannel, insights, insightsLoading,
    catalystPresent, onContinue,
}: {
    channels: ChannelInfo[];
    selectedChannel: string;
    onPickChannel: (key: string) => void;
    insights: CatalystInsights | null;
    insightsLoading: boolean;
    catalystPresent: boolean | null;
    onContinue: () => void;
}) {
    const channel = channels.find((c) => c.key === selectedChannel) || null;
    return (
        <section className="flex flex-col gap-6">
            <div>
                <h2 className="text-lg font-semibold text-white mb-2">Pick a channel</h2>
                <p className="text-sm text-zinc-400 mb-4">
                    Each channel has a locked grammar (visual style, voice, target length).
                    Catalyst Hub data for that channel feeds the outline generator on the next tab.
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                    {channels.map((c) => {
                        const Icon = CHANNEL_ICON[c.key] || Sparkles;
                        const isSelected = c.key === selectedChannel;
                        return (
                            <button
                                key={c.key}
                                onClick={() => onPickChannel(c.key)}
                                className={`text-left rounded-md border px-4 py-3 transition-colors ${
                                    isSelected
                                        ? 'border-violet-500/60 bg-violet-500/10'
                                        : 'border-zinc-800 bg-zinc-950 hover:border-zinc-700'
                                }`}
                            >
                                <div className="flex items-start justify-between gap-2 mb-1">
                                    <div className="flex items-center gap-2">
                                        <Icon className="h-5 w-5 text-violet-300" />
                                        <span className="text-sm font-semibold text-white">{c.label}</span>
                                    </div>
                                    <span className="text-xs text-zinc-500">{c.icon}</span>
                                </div>
                                <div className="text-xs text-zinc-400 mb-2">{c.tagline}</div>
                                <div className="flex items-center gap-3 text-[10px] text-zinc-500">
                                    <span className="flex items-center gap-1">
                                        <Clock className="h-3 w-3" />
                                        {c.default_minutes >= 60
                                            ? `${(c.default_minutes / 60).toFixed(1)}h`
                                            : `${c.default_minutes}m`}
                                    </span>
                                    <span>{c.fps}fps</span>
                                    <span>~${c.cost_estimate_usd}</span>
                                </div>
                            </button>
                        );
                    })}
                </div>
            </div>

            {selectedChannel && (
                <div>
                    <h3 className="text-sm font-semibold text-white mb-2">
                        Catalyst signals for {channel?.label}
                    </h3>
                    {insightsLoading ? (
                        <div className="rounded-md bg-zinc-950 border border-zinc-800 px-4 py-6 flex items-center justify-center gap-2 text-sm text-zinc-500">
                            <Loader2 className="h-4 w-4 animate-spin" /> Fetching Catalyst data…
                        </div>
                    ) : catalystPresent === false ? (
                        <div className="rounded-md bg-amber-500/10 border border-amber-500/30 px-4 py-3 text-sm text-amber-200">
                            No Catalyst harvest yet for this channel. Connect it on the
                            Catalyst tab and wait for the first refresh — outlines will
                            still generate, just without channel-specific bias.
                        </div>
                    ) : insights ? (
                        <CatalystSignalCards insights={insights} />
                    ) : null}
                </div>
            )}

            {selectedChannel && (
                <div className="flex justify-end">
                    <button
                        onClick={onContinue}
                        className="rounded-md bg-violet-500 hover:bg-violet-600 px-4 py-2 text-sm font-semibold text-white"
                    >
                        Continue to Outline →
                    </button>
                </div>
            )}
        </section>
    );
}

function CatalystSignalCards({ insights }: { insights: CatalystInsights }) {
    const totalSignals = (insights.top_titles?.length || 0)
        + (insights.breakout_titles?.length || 0)
        + (insights.hook_patterns?.length || 0)
        + (insights.thumbnail_signals?.length || 0);
    if (totalSignals === 0) {
        return (
            <div className="rounded-md bg-zinc-950 border border-zinc-800 px-4 py-3 text-sm text-zinc-500">
                Catalyst is connected but the harvest hasn't surfaced top performers,
                breakouts, or hook formulas yet. Run a refresh on the Catalyst tab.
            </div>
        );
    }
    return (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {insights.top_titles?.length > 0 && (
                <SignalCard
                    title="Top performing titles"
                    icon={<Eye className="h-4 w-4" />}
                    items={insights.top_titles.slice(0, 5).map((t) => ({
                        primary: t.title,
                        secondary: `${t.views.toLocaleString()} views`,
                    }))}
                />
            )}
            {insights.breakout_titles?.length > 0 && (
                <SignalCard
                    title="Breakouts (high lift)"
                    icon={<TrendingUp className="h-4 w-4" />}
                    items={insights.breakout_titles.slice(0, 5).map((t) => ({
                        primary: t.title,
                        secondary: `${t.lift_vs_baseline.toFixed(1)}× baseline`,
                    }))}
                />
            )}
            {insights.hook_patterns?.length > 0 && (
                <SignalCard
                    title="Hook formulas that work"
                    icon={<Sparkles className="h-4 w-4" />}
                    items={insights.hook_patterns.slice(0, 6).map((h) => ({
                        primary: h, secondary: '',
                    }))}
                />
            )}
            {insights.thumbnail_signals?.length > 0 && (
                <SignalCard
                    title="Thumbnail patterns"
                    icon={<Music className="h-4 w-4" />}
                    items={insights.thumbnail_signals.slice(0, 4).map((t) => ({
                        primary: t, secondary: '',
                    }))}
                />
            )}
        </div>
    );
}

function SignalCard({
    title, icon, items,
}: {
    title: string;
    icon: React.ReactNode;
    items: { primary: string; secondary: string }[];
}) {
    return (
        <div className="rounded-md border border-zinc-800 bg-zinc-950 p-3">
            <div className="flex items-center gap-2 text-xs font-semibold text-zinc-300 mb-2">
                {icon} {title}
            </div>
            <ul className="space-y-1.5">
                {items.map((it, i) => (
                    <li key={i} className="text-xs text-zinc-400">
                        <div className="text-zinc-200 truncate" title={it.primary}>{it.primary}</div>
                        {it.secondary && <div className="text-[10px] text-zinc-500">{it.secondary}</div>}
                    </li>
                ))}
            </ul>
        </div>
    );
}

function OutlineTab({
    selectedChannel, channels, topic, setTopic, targetMinutes, setTargetMinutes,
    useCatalystContext, setUseCatalystContext, insights, onGenerate, outline,
    setOutline, busy, error, onContinue,
}: {
    selectedChannel: string;
    channels: ChannelInfo[];
    topic: string;
    setTopic: (s: string) => void;
    targetMinutes: number | '';
    setTargetMinutes: (n: number | '') => void;
    useCatalystContext: boolean;
    setUseCatalystContext: (b: boolean) => void;
    insights: CatalystInsights | null;
    onGenerate: () => void;
    outline: Outline | null;
    setOutline: (o: Outline | null) => void;
    busy: boolean;
    error: string;
    onContinue: () => void;
}) {
    const channel = channels.find((c) => c.key === selectedChannel);
    if (!channel) {
        return (
            <div className="rounded-md bg-amber-500/10 border border-amber-500/30 px-3 py-2 text-sm text-amber-200">
                Pick a channel on the Channel tab first.
            </div>
        );
    }
    const suggestionTitles: { title: string }[] = [
        ...((insights?.top_titles || []).map((t) => ({ title: t.title }))),
        ...((insights?.breakout_titles || []).map((t) => ({ title: t.title }))),
    ].slice(0, 6);

    return (
        <section className="flex flex-col gap-4">
            <div>
                <h2 className="text-lg font-semibold text-white mb-1">
                    Topic for {channel.label}
                </h2>
                <p className="text-xs text-zinc-500">
                    {channel.tagline}. Default length: {channel.default_minutes >= 60
                        ? `${(channel.default_minutes / 60).toFixed(1)}h`
                        : `${channel.default_minutes}m`}
                </p>
            </div>

            <div className="flex flex-col gap-2">
                <label className="text-xs font-semibold text-zinc-300">Topic</label>
                <input
                    type="text"
                    value={topic}
                    onChange={(e) => setTopic(e.target.value)}
                    placeholder={`e.g. "${channel.label === 'We Are Lacuna' ? 'The Dyatlov Pass mystery' : channel.label === 'Empire Magnates' ? 'How Sanjay Shah legally stole $1.6B' : 'Pick a topic for this channel'}"`}
                    className="rounded-md bg-zinc-950 border border-zinc-800 px-3 py-2 text-sm text-white placeholder-zinc-600 focus:border-violet-500 outline-none"
                />
            </div>

            {suggestionTitles.length > 0 && (
                <div>
                    <div className="text-xs font-semibold text-zinc-300 mb-2">
                        Or seed from a top performer on this channel:
                    </div>
                    <div className="flex flex-wrap gap-2">
                        {suggestionTitles.map((t, i) => (
                            <button
                                key={i}
                                onClick={() => setTopic(t.title)}
                                className="rounded-full bg-violet-500/10 border border-violet-500/30 px-3 py-1 text-xs text-violet-200 hover:bg-violet-500/20 max-w-[300px] truncate"
                                title={t.title}
                            >
                                {t.title}
                            </button>
                        ))}
                    </div>
                </div>
            )}

            <div className="flex items-center gap-4">
                <div className="flex flex-col gap-1">
                    <label className="text-xs font-semibold text-zinc-300">Target minutes</label>
                    <input
                        type="number"
                        min={1}
                        max={600}
                        value={targetMinutes}
                        onChange={(e) => {
                            const v = e.target.value;
                            setTargetMinutes(v === '' ? '' : Number(v));
                        }}
                        className="rounded-md bg-zinc-950 border border-zinc-800 px-3 py-1.5 text-sm text-white w-24"
                    />
                </div>
                <label className="flex items-center gap-2 mt-5 text-xs text-zinc-300 cursor-pointer">
                    <input
                        type="checkbox"
                        checked={useCatalystContext}
                        onChange={(e) => setUseCatalystContext(e.target.checked)}
                        className="rounded border-zinc-700 bg-zinc-950 text-violet-500 focus:ring-violet-500"
                    />
                    Bias outline with Catalyst signals
                </label>
            </div>

            <button
                onClick={onGenerate}
                disabled={busy || !topic.trim()}
                className="rounded-md bg-violet-500 hover:bg-violet-600 disabled:bg-zinc-800 disabled:text-zinc-500 disabled:cursor-not-allowed px-4 py-2.5 text-sm font-semibold text-white flex items-center justify-center gap-2"
            >
                {busy ? (
                    <><Loader2 className="h-4 w-4 animate-spin" /> Generating outline…</>
                ) : (
                    <><Wand2 className="h-4 w-4" /> Generate outline w/ AI</>
                )}
            </button>

            {error && (
                <div className="rounded-md bg-rose-500/10 border border-rose-500/30 px-3 py-2 text-sm text-rose-200">
                    {error}
                </div>
            )}

            {outline && (
                <OutlineEditor outline={outline} setOutline={setOutline} onContinue={onContinue} />
            )}
        </section>
    );
}

function OutlineEditor({
    outline, setOutline, onContinue,
}: {
    outline: Outline;
    setOutline: (o: Outline | null) => void;
    onContinue: () => void;
}) {
    return (
        <div className="rounded-md border border-zinc-800 bg-zinc-950 p-4 flex flex-col gap-3 mt-2">
            <input
                type="text"
                value={outline.title}
                onChange={(e) => setOutline({ ...outline, title: e.target.value })}
                className="bg-transparent text-base font-bold text-white border-b border-zinc-800 pb-1 focus:outline-none focus:border-violet-500"
            />
            <textarea
                value={outline.hook}
                onChange={(e) => setOutline({ ...outline, hook: e.target.value })}
                placeholder="Cold-open hook…"
                rows={2}
                className="bg-zinc-900 rounded-md text-sm text-zinc-200 px-3 py-2 focus:outline-none focus:border-violet-500 border border-zinc-800"
            />
            <div className="flex flex-col gap-2">
                {outline.chapters.map((c, i) => (
                    <div key={i} className="rounded-md bg-zinc-900 border border-zinc-800 p-3">
                        <div className="flex items-center justify-between mb-1">
                            <input
                                type="text"
                                value={c.title}
                                onChange={(e) => {
                                    const next = [...outline.chapters];
                                    next[i] = { ...c, title: e.target.value };
                                    setOutline({ ...outline, chapters: next });
                                }}
                                className="bg-transparent text-sm font-semibold text-white flex-1 focus:outline-none"
                            />
                            <input
                                type="number"
                                value={c.minutes}
                                min={1}
                                onChange={(e) => {
                                    const next = [...outline.chapters];
                                    next[i] = { ...c, minutes: Number(e.target.value) };
                                    setOutline({ ...outline, chapters: next });
                                }}
                                className="w-16 bg-zinc-950 rounded-md border border-zinc-800 px-2 py-0.5 text-xs text-zinc-300 ml-2"
                            />
                            <span className="text-[10px] text-zinc-500 ml-1">min</span>
                        </div>
                        <textarea
                            value={c.synopsis}
                            onChange={(e) => {
                                const next = [...outline.chapters];
                                next[i] = { ...c, synopsis: e.target.value };
                                setOutline({ ...outline, chapters: next });
                            }}
                            rows={2}
                            className="w-full bg-transparent text-xs text-zinc-300 mt-1 focus:outline-none resize-none"
                        />
                    </div>
                ))}
            </div>
            <div className="flex justify-end">
                <button
                    onClick={onContinue}
                    className="rounded-md bg-violet-500 hover:bg-violet-600 px-4 py-2 text-sm font-semibold text-white"
                >
                    Continue to Render →
                </button>
            </div>
        </div>
    );
}

function RenderTab({
    selectedChannel, channels, outline,
}: {
    selectedChannel: string;
    channels: ChannelInfo[];
    outline: Outline | null;
}) {
    const channel = channels.find((c) => c.key === selectedChannel);
    if (!channel) {
        return (
            <div className="rounded-md bg-amber-500/10 border border-amber-500/30 px-3 py-2 text-sm text-amber-200">
                Pick a channel on the Channel tab first.
            </div>
        );
    }
    if (!outline) {
        return (
            <div className="rounded-md bg-amber-500/10 border border-amber-500/30 px-3 py-2 text-sm text-amber-200">
                Generate an outline on the Outline tab first.
            </div>
        );
    }
    const totalMinutes = outline.chapters.reduce((s, c) => s + c.minutes, 0);
    return (
        <section className="flex flex-col gap-4">
            <h2 className="text-lg font-semibold text-white">Confirm + Render</h2>
            <div className="rounded-md border border-zinc-800 bg-zinc-950 p-4 flex flex-col gap-2">
                <div className="text-sm text-zinc-300"><span className="text-zinc-500">Channel:</span> {channel.label}</div>
                <div className="text-sm text-zinc-300"><span className="text-zinc-500">Title:</span> {outline.title}</div>
                <div className="text-sm text-zinc-300"><span className="text-zinc-500">Chapters:</span> {outline.chapters.length}</div>
                <div className="text-sm text-zinc-300"><span className="text-zinc-500">Length:</span> {totalMinutes >= 60 ? `${(totalMinutes / 60).toFixed(1)}h` : `${totalMinutes}m`}</div>
                <div className="text-sm text-zinc-300"><span className="text-zinc-500">Image model:</span> {channel.image_model_default}</div>
                <div className="text-sm text-zinc-300"><span className="text-zinc-500">i2v model:</span> {channel.i2v_model_default}</div>
                <div className="text-sm text-zinc-300"><span className="text-zinc-500">Voice:</span> {channel.voice_provider_default}</div>
                <div className="text-sm text-zinc-300"><span className="text-zinc-500">Estimated fal cost:</span> ~${channel.cost_estimate_usd}</div>
            </div>
            <div className="rounded-md bg-violet-500/10 border border-violet-500/30 px-4 py-3 text-sm text-violet-200">
                <strong>Render pipeline ships next session.</strong> The outline + channel
                config is saved — when fal balance refills, this button kicks off the v5
                pipeline (per-channel image model → i2v → voice → silence-kill compose).
                The legacy /api/longform/session endpoints still work today and are what
                shipped Wirecard / Mongol 9H / Ottoman 9H.
            </div>
            <button
                disabled
                className="rounded-md bg-zinc-800 text-zinc-500 px-4 py-2.5 text-sm font-semibold cursor-not-allowed"
            >
                Render Long-Form (phase 2)
            </button>
        </section>
    );
}
