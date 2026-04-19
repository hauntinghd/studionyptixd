import { useEffect, useMemo, useState } from 'react';
import { Flame, Loader2, X, Zap } from 'lucide-react';
import { getIdeaStylesForTemplate, type IdeaStyle } from '../lib/shortFormIdeaPresets';
import {
    computeHeatScore,
    computeViralityTier,
    heatScoreColorClass,
    viralityBadge,
} from '../lib/heatScore';
import { GENERATION_API } from '../shared';

type TabKey = 'idea_list' | 'custom_topic';

interface Props {
    open: boolean;
    template: string;           // 'skeleton' | 'daytrading' | ...
    templateLabel: string;      // human-readable for modal title
    disabled?: boolean;         // disable Generate buttons while rendering
    onClose: () => void;
    onGenerate: (topic: string) => void;   // parent hooks this into existing script-gen path
}

// "Spark Script" modal. Idea List + Custom Topic (no Remix Script).
// Two-step Idea List flow: pick a style → click "Generate Ideas" → see
// a curated list tagged VIRAL / TRENDING / PREDICTED. Heat scores + niche
// personality (emoji, angle) differentiate from Korpi-style topic dumps.
export default function GenerateScriptWithAIModal({
    open,
    template,
    templateLabel,
    disabled = false,
    onClose,
    onGenerate,
}: Props) {
    const [tab, setTab] = useState<TabKey>('idea_list');
    const styles = useMemo<IdeaStyle[]>(() => getIdeaStylesForTemplate(template), [template]);
    const [selectedStyleId, setSelectedStyleId] = useState<string>(styles[0]?.id ?? '');
    const [customTopic, setCustomTopic] = useState('');

    // Two-step idea reveal: skeleton → Generate Ideas click → loading → list.
    const [ideasLoading, setIdeasLoading] = useState(false);
    const [ideasGenerated, setIdeasGenerated] = useState(false);
    // Bumped each Generate Ideas click so the hash-based virality tiers
    // rotate and the list feels freshly pulled.
    const [refreshKey, setRefreshKey] = useState(0);
    // Live ideas pulled from /api/studio/shorts/ideas (public YouTube
    // trend data through Catalyst infra). Empty array means fall back
    // to the hardcoded presets.
    const [liveIdeas, setLiveIdeas] = useState<string[]>([]);

    // Resetting ideas when the user changes style keeps the Generate Ideas
    // CTA discoverable for each style instead of silently reusing old output.
    useEffect(() => {
        setIdeasGenerated(false);
        setIdeasLoading(false);
        setLiveIdeas([]);
    }, [selectedStyleId, template]);

    // Compute heat scores once per render — deterministic per-day anyway.
    const heatScores = useMemo<Record<string, number>>(() => {
        const map: Record<string, number> = {};
        for (const s of styles) {
            map[s.id] = computeHeatScore(template, s.id);
        }
        return map;
    }, [styles, template]);

    if (!open) return null;

    const selectedStyle = styles.find((s) => s.id === selectedStyleId) ?? styles[0];

    const handlePickIdea = (idea: string) => {
        if (disabled) return;
        onGenerate(idea);
    };

    const handleGenerateCustom = () => {
        const topic = customTopic.trim();
        if (!topic || disabled) return;
        onGenerate(topic);
    };

    const handleGenerateIdeas = async () => {
        if (!selectedStyle || disabled) return;
        setIdeasLoading(true);
        setIdeasGenerated(false);
        setLiveIdeas([]);
        // Live trend query: combine niche + style label + "shorts" so
        // YouTube returns niche-specific shorts that actually match the
        // picked framing. Backend uses Catalyst's cached YouTube search.
        const query = `${templateLabel} ${selectedStyle.label} shorts`.trim();
        try {
            const res = await fetch(`${GENERATION_API}/api/studio/shorts/ideas?q=${encodeURIComponent(query)}&max_results=8`);
            if (res.ok) {
                const data = await res.json();
                const titles = Array.isArray(data?.ideas) ? data.ideas.filter((t: unknown): t is string => typeof t === 'string' && t.trim().length > 0) : [];
                setLiveIdeas(titles);
            }
        } catch {
            // Upstream failure → fall back to hardcoded presets silently
        }
        setRefreshKey((k) => k + 1);
        setIdeasLoading(false);
        setIdeasGenerated(true);
    };

    return (
        <div
            className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
            onClick={onClose}
        >
            <div
                className="w-full max-w-2xl rounded-2xl border border-white/10 bg-[#0b0b12] shadow-2xl"
                onClick={(e) => e.stopPropagation()}
            >
                {/* header */}
                <div className="flex items-start justify-between gap-4 border-b border-white/[0.06] p-5">
                    <div className="flex items-start gap-3">
                        <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-gradient-to-br from-violet-500/25 to-cyan-500/25 text-cyan-200">
                            <Zap className="h-5 w-5" />
                        </span>
                        <div>
                            <h3 className="text-lg font-semibold">
                                <span className="bg-gradient-to-r from-violet-200 via-fuchsia-200 to-cyan-200 bg-clip-text text-transparent">
                                    Spark a {templateLabel} Script
                                </span>
                            </h3>
                            <p className="mt-1 text-xs text-gray-400">
                                Pick a style and spark a list of trending ideas, or enter your own topic.
                            </p>
                        </div>
                    </div>
                    <button
                        type="button"
                        onClick={onClose}
                        className="rounded-lg p-1.5 text-gray-400 transition hover:bg-white/[0.06] hover:text-white"
                        aria-label="Close"
                    >
                        <X className="h-5 w-5" />
                    </button>
                </div>

                {/* tab switcher */}
                <div className="flex gap-1 border-b border-white/[0.06] p-3">
                    <button
                        type="button"
                        onClick={() => setTab('idea_list')}
                        className={`flex-1 rounded-lg px-4 py-2 text-sm font-semibold transition ${
                            tab === 'idea_list'
                                ? 'bg-white/[0.08] text-white'
                                : 'text-gray-400 hover:bg-white/[0.04] hover:text-white'
                        }`}
                    >
                        Idea List
                    </button>
                    <button
                        type="button"
                        onClick={() => setTab('custom_topic')}
                        className={`flex-1 rounded-lg px-4 py-2 text-sm font-semibold transition ${
                            tab === 'custom_topic'
                                ? 'bg-white/[0.08] text-white'
                                : 'text-gray-400 hover:bg-white/[0.04] hover:text-white'
                        }`}
                    >
                        Custom Topic
                    </button>
                </div>

                {/* content */}
                <div className="p-5">
                    {tab === 'idea_list' ? (
                        <div className="space-y-4">
                            {/* Idea Style picker */}
                            <div>
                                <p className="mb-2 text-xs font-semibold uppercase tracking-wider text-gray-500">Idea Style</p>
                                {styles.length === 0 ? (
                                    <p className="text-sm text-gray-400">
                                        No preset styles for this template yet. Use Custom Topic instead.
                                    </p>
                                ) : (
                                    <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
                                        {styles.map((style) => {
                                            const active = style.id === selectedStyle?.id;
                                            const score = heatScores[style.id] ?? 0;
                                            return (
                                                <button
                                                    key={style.id}
                                                    type="button"
                                                    onClick={() => setSelectedStyleId(style.id)}
                                                    className={`relative rounded-lg border p-3 text-left transition ${
                                                        active
                                                            ? 'border-violet-500 bg-violet-500/10'
                                                            : 'border-white/[0.06] bg-white/[0.02] hover:border-white/20'
                                                    }`}
                                                >
                                                    <div className={`absolute right-2 top-2 flex items-center gap-0.5 text-[10px] font-bold tabular-nums ${heatScoreColorClass(score)}`}>
                                                        <Flame className="h-3 w-3" />
                                                        {score}
                                                    </div>
                                                    <div className="mb-1 text-lg leading-none" aria-hidden="true">{style.emoji}</div>
                                                    <div className="text-sm font-semibold text-white">{style.label}</div>
                                                    <div className="mt-1 text-xs text-gray-400">{style.description}</div>
                                                    <div className="mt-2 border-t border-white/[0.05] pt-2 text-[11px] italic leading-snug text-violet-300/70">
                                                        {style.angle}
                                                    </div>
                                                </button>
                                            );
                                        })}
                                    </div>
                                )}
                            </div>

                            {/* Ideas area — skeleton until Generate Ideas is clicked */}
                            {selectedStyle && (
                                <div className="space-y-2">
                                    <div className="flex items-center justify-between">
                                        <p className="text-xs font-semibold uppercase tracking-wider text-gray-500">
                                            {ideasGenerated
                                                ? (liveIdeas.length > 0 ? 'Live from YouTube Shorts · click to spark' : 'Curated picks · click to spark')
                                                : 'Ideas'}
                                        </p>
                                        {ideasGenerated && !ideasLoading && (
                                            <button
                                                type="button"
                                                onClick={handleGenerateIdeas}
                                                className="text-[11px] font-semibold uppercase tracking-wider text-violet-300 transition hover:text-violet-200"
                                            >
                                                Refresh
                                            </button>
                                        )}
                                    </div>

                                    {!ideasGenerated || ideasLoading ? (
                                        <div className="space-y-2">
                                            {[0, 1, 2].map((i) => (
                                                <div
                                                    key={i}
                                                    className="h-[52px] w-full animate-pulse rounded-lg border border-white/[0.04] bg-white/[0.02]"
                                                />
                                            ))}
                                        </div>
                                    ) : (
                                        <div className="space-y-2">
                                            {(liveIdeas.length > 0 ? liveIdeas : selectedStyle.ideas).map((idea) => {
                                                const tier = computeViralityTier(
                                                    template,
                                                    selectedStyle.id,
                                                    `${idea}#${refreshKey}`,
                                                );
                                                const badge = viralityBadge(tier);
                                                return (
                                                    <button
                                                        key={idea}
                                                        type="button"
                                                        disabled={disabled}
                                                        onClick={() => handlePickIdea(idea)}
                                                        className="flex w-full items-center gap-3 rounded-lg border border-white/[0.06] bg-white/[0.02] px-4 py-3 text-left text-sm text-white transition hover:border-violet-400 hover:bg-violet-500/10 disabled:cursor-not-allowed disabled:opacity-50"
                                                    >
                                                        <span
                                                            className={`inline-flex shrink-0 items-center gap-1 rounded-full border px-2 py-0.5 text-[9px] font-bold uppercase tracking-wider ${badge.className}`}
                                                        >
                                                            <span className={`h-1.5 w-1.5 rounded-full ${badge.dotClassName}`} />
                                                            {badge.label}
                                                        </span>
                                                        <span className="flex-1">{idea}</span>
                                                    </button>
                                                );
                                            })}
                                        </div>
                                    )}
                                </div>
                            )}
                        </div>
                    ) : (
                        <div className="space-y-3">
                            <textarea
                                value={customTopic}
                                onChange={(e) => setCustomTopic(e.target.value)}
                                placeholder={`Example: ${styles[0]?.ideas[0] ?? 'How long can you stay awake?'}`}
                                className="w-full resize-y rounded-lg border border-white/[0.08] bg-black/40 p-3 text-sm text-white placeholder-gray-500 focus:border-violet-400 focus:outline-none"
                                rows={6}
                                disabled={disabled}
                            />
                            <div className="flex justify-end">
                                <button
                                    type="button"
                                    onClick={handleGenerateCustom}
                                    disabled={disabled || !customTopic.trim()}
                                    className="inline-flex items-center gap-1.5 rounded-lg bg-gradient-to-r from-violet-600 via-fuchsia-600 to-cyan-600 px-4 py-2 text-sm font-semibold text-white shadow-md shadow-violet-900/30 transition hover:opacity-95 disabled:cursor-not-allowed disabled:opacity-50"
                                >
                                    <Zap className="h-4 w-4" />
                                    Spark Script
                                </button>
                            </div>
                        </div>
                    )}
                </div>

                {/* sticky Generate Ideas CTA footer (Idea List only) */}
                {tab === 'idea_list' && selectedStyle && (
                    <div className="flex items-center justify-between gap-3 border-t border-white/[0.06] p-4">
                        <p className="text-[11px] text-gray-500">
                            {ideasGenerated
                                ? 'Rotated daily · VIRAL · TRENDING · PREDICTED'
                                : 'Click to spark a curated trend-scored idea list.'}
                        </p>
                        <button
                            type="button"
                            onClick={handleGenerateIdeas}
                            disabled={disabled || ideasLoading}
                            className="inline-flex items-center gap-1.5 rounded-lg bg-gradient-to-r from-violet-600 via-fuchsia-600 to-cyan-600 px-4 py-2 text-sm font-semibold text-white shadow-md shadow-violet-900/30 transition hover:opacity-95 disabled:cursor-not-allowed disabled:opacity-50"
                        >
                            {ideasLoading ? (
                                <>
                                    <Loader2 className="h-4 w-4 animate-spin" />
                                    Sparking...
                                </>
                            ) : (
                                <>
                                    <Zap className="h-4 w-4" />
                                    {ideasGenerated ? 'Spark New Ideas' : 'Generate Ideas'}
                                </>
                            )}
                        </button>
                    </div>
                )}
            </div>
        </div>
    );
}
