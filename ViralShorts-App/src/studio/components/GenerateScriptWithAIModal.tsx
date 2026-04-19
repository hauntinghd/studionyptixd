import { useMemo, useState } from 'react';
import { Flame, X, Zap } from 'lucide-react';
import { getIdeaStylesForTemplate, type IdeaStyle } from '../lib/shortFormIdeaPresets';
import { computeHeatScore, heatScoreColorClass } from '../lib/heatScore';

type TabKey = 'idea_list' | 'custom_topic';

interface Props {
    open: boolean;
    template: string;           // 'skeleton' | 'daytrading' | ...
    templateLabel: string;      // human-readable for modal title
    disabled?: boolean;         // disable Generate buttons while rendering
    onClose: () => void;
    onGenerate: (topic: string) => void;   // parent hooks this into existing script-gen path
}

// "Spark Script" modal — Idea List + Custom Topic tabs. Each style card
// carries a niche-emoji + "why this works" angle + a per-day heat score
// pulled from `computeHeatScore()`. That trio (personality, framing,
// heat) is what differentiates NYPTID from Korpi-style topic-only modals.
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

    // Compute heat scores once per render — they're deterministic per-day anyway.
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
                                Pick a trending angle or enter a custom topic. Heat scores refresh daily.
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
                            <div>
                                <p className="mb-2 text-xs font-semibold uppercase tracking-wider text-gray-500">Idea Style</p>
                                {styles.length === 0 ? (
                                    <p className="text-sm text-gray-400">
                                        No preset styles for this template yet. Use Custom Topic instead.
                                    </p>
                                ) : (
                                    <div className="grid grid-cols-1 gap-2 md:grid-cols-3">
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

                            {selectedStyle && selectedStyle.ideas.length > 0 && (
                                <div className="space-y-2">
                                    <p className="text-xs font-semibold uppercase tracking-wider text-gray-500">
                                        Ideas — click one to spark a script
                                    </p>
                                    <div className="space-y-2">
                                        {selectedStyle.ideas.map((idea) => (
                                            <button
                                                key={idea}
                                                type="button"
                                                disabled={disabled}
                                                onClick={() => handlePickIdea(idea)}
                                                className="w-full rounded-lg border border-white/[0.06] bg-white/[0.02] px-4 py-3 text-left text-sm text-white transition hover:border-violet-400 hover:bg-violet-500/10 disabled:cursor-not-allowed disabled:opacity-50"
                                            >
                                                {idea}
                                            </button>
                                        ))}
                                    </div>
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
            </div>
        </div>
    );
}
