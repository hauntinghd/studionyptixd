import { creatableShortNiches, nicheById, type NicheId } from '../../lib/studioProduct';

export default function NichePickerStrip({
    value,
    onChange,
    isOwner,
    compact,
}: {
    value: NicheId | null;
    onChange: (id: NicheId) => void;
    isOwner?: boolean;
    compact?: boolean;
}) {
    const niches = creatableShortNiches(Boolean(isOwner));
    return (
        <div className={compact ? 'space-y-2' : 'space-y-3'}>
            {!compact && (
                <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-violet-300">Choose niche</p>
                    <p className="mt-1 text-sm text-gray-500">Each lane has its own script voice, visuals, and hook shape.</p>
                </div>
            )}
            <div className={`flex flex-wrap gap-2 ${compact ? '' : 'sm:gap-3'}`}>
                {niches.map((n) => {
                    const active = value === n.id;
                    return (
                        <button
                            key={n.id}
                            type="button"
                            onClick={() => onChange(n.id)}
                            className={`rounded-xl border px-3 py-2 text-left transition sm:px-4 sm:py-2.5 ${
                                active
                                    ? 'border-violet-500/50 bg-violet-500/10 text-white shadow-lg shadow-violet-950/20'
                                    : 'border-white/[0.08] bg-white/[0.02] text-gray-300 hover:border-violet-500/30 hover:text-white'
                            }`}
                        >
                            <span className="block text-sm font-semibold">{n.title}</span>
                            {!compact && (
                                <span className="mt-0.5 block text-[11px] text-gray-500">{n.desc}</span>
                            )}
                        </button>
                    );
                })}
            </div>
            {value && !compact && (
                <p className="text-xs text-gray-500">
                    Selected: {nicheById(value)?.title}. Switch anytime — your script stays unless you change niche mid-build.
                </p>
            )}
        </div>
    );
}

export function NichePickerGrid({
    isOwner,
    onPick,
}: {
    isOwner?: boolean;
    onPick: (id: NicheId) => void;
}) {
    const niches = creatableShortNiches(Boolean(isOwner));
    return (
        <section className="mx-auto max-w-4xl space-y-6">
            <div>
                <p className="text-xs font-semibold uppercase tracking-[0.2em] text-violet-300">Create</p>
                <h1 className="mt-2 text-2xl font-bold text-white sm:text-3xl">Pick a niche to build</h1>
                <p className="mt-2 max-w-xl text-sm text-gray-400">
                    Alt-history battles, moral dilemmas, horror, and historical epic each ship with a locked visual lane.
                </p>
            </div>
            <div className="grid gap-3 sm:grid-cols-2">
                {niches.map((n) => (
                    <button
                        key={n.id}
                        type="button"
                        onClick={() => onPick(n.id)}
                        className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-5 text-left transition hover:border-violet-500/40 hover:bg-violet-500/5"
                    >
                        <div className="flex items-start justify-between gap-3">
                            <h3 className="text-base font-bold text-white">{n.title}</h3>
                            {n.badge && (
                                <span className="rounded-full border border-white/10 bg-black/40 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-gray-400">
                                    {n.badge}
                                </span>
                            )}
                        </div>
                        <p className="mt-2 text-sm text-gray-400">{n.desc}</p>
                    </button>
                ))}
            </div>
        </section>
    );
}
