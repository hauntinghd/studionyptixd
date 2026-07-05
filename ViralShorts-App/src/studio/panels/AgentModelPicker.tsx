import { Check, Search, Sparkles, Star, X } from 'lucide-react';
import { useMemo, useState } from 'react';

export interface AgentModelOption {
    id: string;
    name: string;
    provider: string;
    description?: string;
    context_length?: number;
    prompt_price_per_m?: number;
    completion_price_per_m?: number;
    recommended?: boolean;
    intelligence?: number;
    speed?: number;
}

function Stars({ n }: { n: number }) {
    return (
        <span className="text-amber-400">
            {'★'.repeat(Math.max(0, Math.min(5, n)))}
            <span className="text-gray-700">{'★'.repeat(Math.max(0, 5 - n))}</span>
        </span>
    );
}

function formatCtx(n?: number) {
    if (!n) return '—';
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(n % 1_000_000 === 0 ? 0 : 1)}M`;
    if (n >= 1000) return `${Math.round(n / 1000)}K`;
    return String(n);
}

function formatPrice(inP?: number, outP?: number) {
    if (inP == null && outP == null) return '—';
    const i = inP != null ? `${inP.toFixed(2)} in` : '? in';
    const o = outP != null ? `${outP.toFixed(2)} out` : '? out';
    return `${i} · ${o} $/Mtok`;
}

export default function AgentModelPicker({
    open,
    models,
    selectedId,
    onSelect,
    onClose,
    title = 'Choose a runner model',
    subtitle = 'Used for planning, tool calls, and production orchestration.',
    statusText,
    searchPlaceholder = 'Search Claude models and capabilities...',
}: {
    open: boolean;
    models: AgentModelOption[];
    selectedId: string;
    onSelect: (id: string) => void;
    onClose: () => void;
    title?: string;
    subtitle?: string;
    statusText?: string;
    searchPlaceholder?: string;
}) {
    const [query, setQuery] = useState('');
    const [tab, setTab] = useState<string>('all');
    const cleanModels = useMemo(
        () => models.filter((m) => !m.id.startsWith('~') && !/latest/i.test(m.name || '')),
        [models],
    );

    const providerTabs = useMemo(() => {
        const counts = new Map<string, number>();
        for (const m of cleanModels) {
            const key = String(m.provider || 'Other');
            counts.set(key, (counts.get(key) || 0) + 1);
        }
        return Array.from(counts.entries())
            .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
            .slice(0, 10)
            .map(([id, count]) => ({ id, label: id, count }));
    }, [cleanModels]);

    const filtered = useMemo(() => {
        const q = query.trim().toLowerCase();
        let list = cleanModels;
        if (q) {
            list = cleanModels;
        } else if (tab === 'recommended') {
            list = cleanModels.filter((m) => m.recommended);
        } else if (tab !== 'all') {
            list = cleanModels.filter((m) => m.provider === tab);
        }
        const searched = q ? list.filter(
            (m) =>
                m.id.toLowerCase().includes(q)
                || m.name.toLowerCase().includes(q)
                || m.provider.toLowerCase().includes(q)
                || (m.description || '').toLowerCase().includes(q),
        ) : list;
        return [...searched].sort((a, b) => {
            if (a.id === selectedId) return -1;
            if (b.id === selectedId) return 1;
            if (Boolean(a.recommended) !== Boolean(b.recommended)) return a.recommended ? -1 : 1;
            const ai = Number(a.intelligence || 0);
            const bi = Number(b.intelligence || 0);
            if (ai !== bi) return bi - ai;
            return String(a.name || a.id).localeCompare(String(b.name || b.id));
        });
    }, [cleanModels, query, selectedId, tab]);

    if (!open) return null;

    return (
        <div className="fixed inset-0 z-50 flex items-end justify-center bg-black/70 p-0 sm:items-center sm:p-4">
            <button type="button" className="absolute inset-0" aria-label="Close" onClick={onClose} />
            <div className="relative z-10 flex max-h-[88vh] w-full max-w-4xl flex-col overflow-hidden rounded-t-2xl border border-white/10 bg-[#0a0a0c] shadow-2xl sm:rounded-2xl">
                <div className="flex items-start justify-between gap-3 border-b border-white/[0.06] px-4 py-3">
                    <div>
                        <h2 className="text-lg font-semibold text-white">{title}</h2>
                        <p className="mt-1 text-xs leading-relaxed text-gray-500">
                            {subtitle}
                        </p>
                        <p className="mt-2 flex items-center gap-1.5 text-[10px] text-emerald-400/90">
                            <Sparkles className="h-3 w-3" />
                            {statusText || `${cleanModels.length} Claude models available through your Anthropic API`}
                        </p>
                    </div>
                    <button
                        type="button"
                        onClick={onClose}
                        className="rounded-lg p-1.5 text-gray-400 hover:bg-white/10 hover:text-white"
                    >
                        <X className="h-4 w-4" />
                    </button>
                </div>

                <div className="border-b border-white/[0.06] px-4 py-3">
                    <div className="relative">
                        <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-500" />
                        <input
                            value={query}
                            onChange={(e) => setQuery(e.target.value)}
                            placeholder={searchPlaceholder}
                            className="w-full rounded-xl border border-teal-500/25 bg-black/40 py-2 pl-10 pr-3 text-sm text-white placeholder:text-gray-600 focus:border-teal-400/60 focus:outline-none"
                        />
                    </div>
                    <div className="mt-3 flex gap-1 overflow-x-auto pb-1">
                        <button
                            type="button"
                            onClick={() => setTab('all')}
                            className={`shrink-0 rounded-lg px-3 py-1.5 text-[11px] font-medium transition ${
                                tab === 'all'
                                    ? 'border border-teal-500/40 bg-teal-500/10 text-teal-200'
                                    : 'border border-transparent text-gray-500 hover:text-gray-300'
                            }`}
                        >
                            All {cleanModels.length}
                        </button>
                        <button
                            type="button"
                            onClick={() => setTab('recommended')}
                            className={`inline-flex shrink-0 items-center gap-1 rounded-lg px-3 py-1.5 text-[11px] font-medium transition ${
                                tab === 'recommended'
                                    ? 'border border-teal-500/40 bg-teal-500/10 text-teal-200'
                                    : 'border border-transparent text-gray-500 hover:text-gray-300'
                            }`}
                        >
                            <Star className="h-3 w-3" />
                            Recommended
                        </button>
                        {providerTabs.map((t) => (
                            <button
                                key={t.id}
                                type="button"
                                onClick={() => setTab(t.id)}
                                className={`shrink-0 rounded-lg px-3 py-1.5 text-[11px] font-medium transition ${
                                    tab === t.id
                                        ? 'border border-teal-500/40 bg-teal-500/10 text-teal-200'
                                        : 'border border-transparent text-gray-500 hover:text-gray-300'
                                }`}
                            >
                                {t.label} <span className="text-gray-600">{t.count}</span>
                            </button>
                        ))}
                    </div>
                </div>

                <div className="min-h-0 flex-1 overflow-y-auto p-3">
                    <div className="grid gap-3 lg:grid-cols-2">
                        {filtered.map((m) => {
                            const active = m.id === selectedId;
                            return (
                                <button
                                    key={m.id}
                                    type="button"
                                    onClick={() => {
                                        onSelect(m.id);
                                        onClose();
                                    }}
                                    className={`rounded-xl border p-3 text-left transition ${
                                        active
                                            ? 'border-teal-500/50 bg-teal-500/[0.07] ring-1 ring-teal-500/30'
                                            : 'border-white/[0.08] bg-white/[0.02] hover:border-white/15'
                                    }`}
                                >
                                    <div className="flex items-start justify-between gap-2">
                                        <div>
                                            <p className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                                                {m.provider}
                                            </p>
                                            <p className="mt-0.5 text-sm font-semibold text-white">{m.name}</p>
                                        </div>
                                        {m.recommended && (
                                            <span className="rounded bg-teal-500/15 px-1.5 py-0.5 text-[9px] font-bold uppercase text-teal-300">
                                                Rec
                                            </span>
                                        )}
                                        {active && <Check className="h-4 w-4 text-teal-300" />}
                                    </div>
                                    {m.description && (
                                        <p className="mt-2 line-clamp-1 text-[11px] leading-relaxed text-gray-500">
                                            {m.description}
                                        </p>
                                    )}
                                    <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1 text-[10px] text-gray-500">
                                        <span>Context: {formatCtx(m.context_length)}</span>
                                        <span>{formatPrice(m.prompt_price_per_m, m.completion_price_per_m)}</span>
                                        {m.intelligence != null && (
                                            <span className="col-span-2">
                                                Intelligence <Stars n={m.intelligence} />
                                            </span>
                                        )}
                                        {m.speed != null && (
                                            <span className="col-span-2">
                                                Speed <Stars n={m.speed} />
                                            </span>
                                        )}
                                    </div>
                                    <p className="mt-2 truncate font-mono text-[10px] text-gray-600">{m.id}</p>
                                </button>
                            );
                        })}
                    </div>
                    {filtered.length === 0 && (
                        <p className="py-8 text-center text-sm text-gray-500">No models match your search.</p>
                    )}
                </div>
            </div>
        </div>
    );
}
