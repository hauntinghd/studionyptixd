/**
 * Concept plan card — shown before production Approve.
 * Shortform / longform / product ads all use this gate.
 */
import { Check, ChevronDown, ChevronUp, Clapperboard, Clock, Layers, Sparkles } from 'lucide-react';
import { useEffect, useState } from 'react';

export type ConceptBeat = {
    label?: string;
    seconds?: number;
    note?: string;
};

export type ConceptPlan = {
    id?: string;
    status?: string;
    format?: string;
    title?: string;
    hook?: string;
    duration_sec?: number;
    scene_count?: number;
    beats?: ConceptBeat[];
    improvements?: string[];
    visual_style?: string;
    reference_title?: string;
    user_request?: string;
    niche?: string;
    channel_title?: string;
};

const FORMAT_LABEL: Record<string, string> = {
    shortform: 'Short-form',
    longform: 'Long-form',
    product_ad: 'Product / SaaS ad',
};

function formatDuration(sec?: number) {
    const s = Math.max(0, Number(sec || 0));
    if (s >= 3600) {
        const h = Math.floor(s / 3600);
        const m = Math.floor((s % 3600) / 60);
        return m ? `${h}h ${m}m` : `${h}h`;
    }
    if (s >= 60) {
        const m = Math.floor(s / 60);
        const rem = s % 60;
        return rem ? `${m}m ${rem}s` : `${m}m`;
    }
    return `${s || 30}s`;
}

type Props = {
    plan: ConceptPlan;
    disabled?: boolean;
    onCommit: () => void;
    onDismiss?: () => void;
};

export default function AgentConceptCard({ plan, disabled, onCommit, onDismiss }: Props) {
    const fmt = String(plan.format || 'shortform');
    const fmtLabel = FORMAT_LABEL[fmt] || fmt;
    const status = String(plan.status || 'awaiting_confirm');
    const locked = status === 'confirmed' || status === 'started';
    const productionActive = Boolean(disabled);
    const autoCollapse = locked || productionActive;
    const beats = Array.isArray(plan.beats) ? plan.beats.filter((b) => b && typeof b === 'object') : [];
    const improvements = Array.isArray(plan.improvements)
        ? plan.improvements.map(String).filter(Boolean)
        : [];
    // A locked concept must never pin a large card above the composer. It
    // starts as a small dock and only expands when the user explicitly asks.
    const [collapsed, setCollapsed] = useState(autoCollapse);
    const [lockedExpanded, setLockedExpanded] = useState(false);
    useEffect(() => {
        setCollapsed(autoCollapse);
        setLockedExpanded(false);
    }, [autoCollapse, plan.id]);
    const isCollapsed = autoCollapse ? !lockedExpanded : collapsed;

    if (isCollapsed) {
        return (
            <div className="flex items-center justify-between gap-3 rounded-xl border border-cyan-500/25 bg-[#0a1218]/95 px-3 py-2 shadow-lg shadow-black/30">
                <div className="min-w-0">
                    <p className="truncate text-xs font-semibold text-cyan-100">{plan.title || 'Concept plan'}</p>
                    <p className="text-[10px] text-emerald-300/90">{locked ? 'Concept locked — production is ready below.' : 'Concept minimized'}</p>
                </div>
                <button type="button" onClick={() => autoCollapse ? setLockedExpanded(true) : setCollapsed(false)} className="inline-flex shrink-0 items-center gap-1 rounded-md border border-cyan-500/25 px-2 py-1 text-[10px] font-semibold text-cyan-100 hover:bg-cyan-500/10">
                    <ChevronUp className="h-3.5 w-3.5" /> Expand
                </button>
            </div>
        );
    }

    return (
        <div className="rounded-xl border border-cyan-500/30 bg-[#0a1218]/95 px-3 py-2.5 shadow-lg shadow-black/30">
            <div className="mb-2 flex items-start justify-between gap-2">
                <div>
                    <p className="flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-wide text-cyan-200/90">
                        <Clapperboard className="h-3.5 w-3.5 text-cyan-400" />
                        Concept plan
                        <span className="rounded-full border border-cyan-500/25 bg-cyan-500/10 px-1.5 py-0.5 text-[9px] font-bold normal-case tracking-normal text-cyan-100">
                            {fmtLabel}
                        </span>
                    </p>
                    <p className="mt-1 text-sm font-semibold leading-snug text-white">
                        {plan.title || 'Untitled concept'}
                    </p>
                </div>
                <div className="flex shrink-0 items-center gap-1">
                    <button type="button" onClick={() => autoCollapse ? setLockedExpanded(false) : setCollapsed(true)} className="rounded-md px-1.5 py-0.5 text-[10px] text-gray-500 hover:bg-white/5 hover:text-gray-300" title="Minimize concept plan">
                        <ChevronDown className="h-3.5 w-3.5" />
                    </button>
                {onDismiss && !locked && (
                    <button
                        type="button"
                        onClick={onDismiss}
                        className="shrink-0 rounded-md px-1.5 py-0.5 text-[10px] text-gray-500 hover:bg-white/5 hover:text-gray-300"
                    >
                        Hide
                    </button>
                )}
                </div>
            </div>

            {plan.hook ? (
                <p className="mb-2 text-[12px] leading-relaxed text-gray-300">
                    <span className="font-semibold text-cyan-100/90">Hook:</span> {plan.hook}
                </p>
            ) : null}

            <div className="mb-2 flex flex-wrap gap-2 text-[10px] text-gray-400">
                <span className="inline-flex items-center gap-1 rounded-md border border-white/10 bg-black/30 px-1.5 py-0.5">
                    <Clock className="h-3 w-3 text-cyan-400/80" />
                    {formatDuration(plan.duration_sec)}
                </span>
                {fmt === 'shortform' || fmt === 'product_ad' ? (
                    <span className="inline-flex items-center gap-1 rounded-md border border-white/10 bg-black/30 px-1.5 py-0.5">
                        <Layers className="h-3 w-3 text-cyan-400/80" />
                        {Number(plan.scene_count || 0) || beats.length || '—'} scenes
                    </span>
                ) : null}
                {plan.visual_style ? (
                    <span className="inline-flex items-center gap-1 rounded-md border border-white/10 bg-black/30 px-1.5 py-0.5">
                        <Sparkles className="h-3 w-3 text-cyan-400/80" />
                        {plan.visual_style}
                    </span>
                ) : null}
            </div>

            {beats.length > 0 && (
                <div className="mb-2 max-h-36 space-y-1 overflow-y-auto rounded-lg border border-white/[0.06] bg-black/25 p-2">
                    {beats.slice(0, 8).map((beat, i) => (
                        <div key={`${beat.label || i}-${i}`} className="flex gap-2 text-[11px]">
                            <span className="w-4 shrink-0 font-mono text-cyan-500/80">{i + 1}.</span>
                            <div className="min-w-0">
                                <span className="font-semibold text-gray-200">
                                    {beat.label || `Beat ${i + 1}`}
                                </span>
                                {typeof beat.seconds === 'number' && beat.seconds > 0 ? (
                                    <span className="ml-1 text-gray-500">({formatDuration(beat.seconds)})</span>
                                ) : null}
                                {beat.note ? (
                                    <span className="block truncate text-gray-500">{beat.note}</span>
                                ) : null}
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {improvements.length > 0 && (
                <ul className="mb-2 space-y-0.5 text-[11px] text-gray-400">
                    {improvements.slice(0, 4).map((tip) => (
                        <li key={tip} className="flex gap-1.5">
                            <span className="text-cyan-500/70">•</span>
                            <span>{tip}</span>
                        </li>
                    ))}
                </ul>
            )}

            {locked ? (
                <p className="text-[11px] font-medium text-emerald-300/90">
                    {status === 'started' ? 'Production started from this concept.' : 'Concept locked — approve production below.'}
                </p>
            ) : (
                <>
                    <button
                        type="button"
                        disabled={disabled}
                        onClick={onCommit}
                        className="inline-flex min-h-9 w-full items-center justify-center gap-1.5 rounded-lg bg-cyan-600 px-3 py-1.5 text-xs font-bold text-white transition hover:bg-cyan-500 disabled:opacity-50"
                    >
                        <Check className="h-4 w-4 stroke-[3]" />
                        Implement plan
                    </button>
                    <p className="mt-1.5 text-center text-[10px] text-gray-500">
                        Switches to Production. Spend still needs Approve &amp; run.
                    </p>
                </>
            )}
        </div>
    );
}
