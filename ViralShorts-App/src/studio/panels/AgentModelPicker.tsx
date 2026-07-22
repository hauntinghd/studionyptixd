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
    /** Estimated USD for a sample turn (10k input + 2k output tokens). */
    est_cost_10k_2k?: number;
    /** Exact unit price for image/video generation profiles. */
    estimated_unit_usd?: number;
    billing_unit?: string;
    /** Provider pricing provenance for media-generation models. */
    pricing_source?: string;
    pricing_fetched_at?: number;
    pricing_live?: boolean;
    input_image_usd?: number;
    pricing_assumptions?: string;
    recommended?: boolean;
    intelligence?: number;
    speed?: number;
    /** Product/provider policy. Disabled models remain visible but cannot be selected. */
    selectable?: boolean;
    disabled?: boolean;
    disabled_reason?: string;
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

/** Precise $/M display from API/catalog pricing. */
function formatPricePerM(inP?: number, outP?: number) {
    if (inP == null && outP == null) return 'Pricing unavailable';
    const fmt = (v: number) => {
        if (v >= 10) return `$${v.toFixed(2)}`;
        if (v >= 1) return `$${v.toFixed(2)}`;
        if (v >= 0.01) return `$${v.toFixed(3)}`;
        return `$${v.toFixed(4)}`;
    };
    const i = inP != null ? fmt(inP) : '—';
    const o = outP != null ? fmt(outP) : '—';
    return `${i} in · ${o} out / 1M tok`;
}

/** Sample turn cost: 10k input + 2k completion (matches backend est_cost_10k_2k). */
function formatSampleCost(m: AgentModelOption) {
    let est = m.est_cost_10k_2k;
    if (est == null && (m.prompt_price_per_m != null || m.completion_price_per_m != null)) {
        const pin = (m.prompt_price_per_m || 0) * (10_000 / 1_000_000);
        const pout = (m.completion_price_per_m || 0) * (2_000 / 1_000_000);
        est = pin + pout;
    }
    if (est == null || Number.isNaN(est)) return null;
    if (est < 0.0001) return '< $0.0001 / sample turn';
    if (est < 0.01) return `~$${est.toFixed(4)} / sample turn`;
    if (est < 1) return `~$${est.toFixed(3)} / sample turn`;
    return `~$${est.toFixed(2)} / sample turn`;
}

function formatGenerationPrice(m: AgentModelOption) {
    if (typeof m.estimated_unit_usd !== 'number') return null;
    const value = m.estimated_unit_usd;
    const amount = value < 0.01 ? value.toFixed(4) : value.toFixed(3).replace(/0+$/, '').replace(/\.$/, '');
    const input = typeof m.input_image_usd === 'number'
        ? ` + $${m.input_image_usd < 0.01 ? m.input_image_usd.toFixed(3) : m.input_image_usd.toFixed(2)}/input image`
        : '';
    return `$${amount}/${m.billing_unit || 'unit'}${input}`;
}

function formatPricingSource(m: AgentModelOption) {
    const source = String(m.pricing_source || '').toLowerCase();
    if (!source) return null;
    if (m.pricing_live || source === 'fal_api') return 'Live provider rate';
    if (source.includes('disk_cache')) return 'Last-known provider rate';
    if (source === 'xai_published') return 'Retired legacy rate';
    if (source === 'fallback') return 'Fallback estimate';
    return null;
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
    searchPlaceholder = 'Search models, providers, or capabilities...',
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

    const providerSummary = useMemo(() => {
        if (!providerTabs.length) return `${cleanModels.length} models available`;
        return providerTabs.map((t) => `${t.count} ${t.label}`).join(' · ');
    }, [cleanModels.length, providerTabs]);

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
        const searched = q
            ? list.filter(
                  (m) =>
                      m.id.toLowerCase().includes(q)
                      || m.name.toLowerCase().includes(q)
                      || m.provider.toLowerCase().includes(q)
                      || (m.description || '').toLowerCase().includes(q)
                      || (m.disabled_reason || '').toLowerCase().includes(q),
              )
            : list;
        return [...searched].sort((a, b) => {
            if (a.id === selectedId) return -1;
            if (b.id === selectedId) return 1;
            const aDisabled = a.disabled === true || a.selectable === false;
            const bDisabled = b.disabled === true || b.selectable === false;
            if (aDisabled !== bDisabled) return aDisabled ? 1 : -1;
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
                        <p className="mt-1 text-xs leading-relaxed text-gray-500">{subtitle}</p>
                        <p className="mt-2 flex items-center gap-1.5 text-[10px] text-emerald-400/90">
                            <Sparkles className="h-3 w-3" />
                            {statusText || `${providerSummary} through your configured APIs`}
                        </p>
                    </div>
                    <button
                        type="button"
                        onClick={onClose}
                        className="rounded-lg p-1.5 text-gray-400 transition hover:bg-white/10 hover:text-white"
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
                            className="w-full rounded-xl border border-teal-500/25 bg-black/40 py-2 pl-10 pr-3 text-sm text-white placeholder:text-gray-600 transition focus:border-teal-400/60 focus:outline-none focus:ring-1 focus:ring-teal-500/30"
                        />
                    </div>
                    <div className="mt-3 flex gap-1 overflow-x-auto pb-1">
                        <button
                            type="button"
                            onClick={() => setTab('all')}
                            className={`shrink-0 rounded-lg px-3 py-1.5 text-[11px] font-medium transition ${
                                tab === 'all'
                                    ? 'border border-teal-500/40 bg-teal-500/10 text-teal-200'
                                    : 'border border-transparent text-gray-500 hover:bg-white/[0.04] hover:text-gray-300'
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
                                    : 'border border-transparent text-gray-500 hover:bg-white/[0.04] hover:text-gray-300'
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
                                        : 'border border-transparent text-gray-500 hover:bg-white/[0.04] hover:text-gray-300'
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
                            const disabled = m.disabled === true || m.selectable === false;
                            const sample = formatSampleCost(m);
                            const pricingSource = formatPricingSource(m);
                            return (
                                <button
                                    key={m.id}
                                    type="button"
                                    disabled={disabled}
                                    aria-disabled={disabled}
                                    title={disabled ? (m.disabled_reason || `${m.name} is unavailable`) : undefined}
                                    onClick={() => {
                                        if (disabled) return;
                                        onSelect(m.id);
                                        onClose();
                                    }}
                                    className={`group rounded-xl border p-3 text-left transition-all duration-200 ease-out will-change-transform ${
                                        disabled
                                            ? 'cursor-not-allowed border-white/[0.05] bg-white/[0.01] opacity-45 grayscale'
                                            : active
                                            ? 'border-teal-500/50 bg-teal-500/[0.07] ring-1 ring-teal-500/30 shadow-lg shadow-teal-500/10'
                                            : 'border-white/[0.08] bg-white/[0.02] hover:-translate-y-0.5 hover:scale-[1.01] hover:border-teal-500/40 hover:bg-white/[0.05] hover:shadow-lg hover:shadow-teal-500/15 active:scale-[0.995]'
                                    }`}
                                >
                                    <div className="flex items-start justify-between gap-2">
                                        <div>
                                            <p className="text-[10px] font-semibold uppercase tracking-wider text-gray-500 transition group-hover:text-gray-400">
                                                {m.provider}
                                            </p>
                                            <p className="mt-0.5 text-sm font-semibold text-white transition group-hover:text-teal-50">
                                                {m.name}
                                            </p>
                                        </div>
                                        <div className="flex shrink-0 items-center gap-1.5">
                                            {m.recommended && (
                                                <span className="rounded bg-teal-500/15 px-1.5 py-0.5 text-[9px] font-bold uppercase text-teal-300">
                                                    Rec
                                                </span>
                                            )}
                                            {disabled && (
                                                <span className="rounded bg-gray-700/40 px-1.5 py-0.5 text-[9px] font-bold uppercase text-gray-400">
                                                    Disabled
                                                </span>
                                            )}
                                            {active && <Check className="h-4 w-4 text-teal-300" />}
                                        </div>
                                    </div>
                                    {m.description && (
                                        <p className="mt-2 line-clamp-2 text-[11px] leading-relaxed text-gray-500 transition group-hover:text-gray-400">
                                            {m.description}
                                        </p>
                                    )}
                                    {disabled && m.disabled_reason && (
                                        <p className="mt-2 text-[11px] leading-relaxed text-gray-400">
                                            {m.disabled_reason}
                                        </p>
                                    )}
                                    <div className="mt-2.5 space-y-1 text-[10px] text-gray-500">
                                        <div className="flex flex-wrap items-center justify-between gap-x-2 gap-y-0.5">
                                            <span>Context {formatCtx(m.context_length)}</span>
                                            <span className="font-medium text-gray-400 group-hover:text-teal-200/90">
                                                {formatGenerationPrice(m) || formatPricePerM(m.prompt_price_per_m, m.completion_price_per_m)}
                                            </span>
                                        </div>
                                        {pricingSource && (
                                            <div className={m.pricing_live ? 'text-emerald-500/80' : 'text-gray-600'}>
                                                {pricingSource}
                                            </div>
                                        )}
                                        {m.pricing_assumptions && (
                                            <div className="text-gray-600">For {m.pricing_assumptions}</div>
                                        )}
                                        {sample && (
                                            <div className="text-[10px] text-emerald-500/80 group-hover:text-emerald-400/90">
                                                {sample}
                                                <span className="text-gray-600"> (10k in + 2k out)</span>
                                            </div>
                                        )}
                                        {m.intelligence != null && (
                                            <div>
                                                Intelligence <Stars n={m.intelligence} />
                                            </div>
                                        )}
                                        {m.speed != null && (
                                            <div>
                                                Speed <Stars n={m.speed} />
                                            </div>
                                        )}
                                    </div>
                                    <p className="mt-2 truncate font-mono text-[10px] text-gray-600 group-hover:text-gray-500">
                                        {m.id}
                                    </p>
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
