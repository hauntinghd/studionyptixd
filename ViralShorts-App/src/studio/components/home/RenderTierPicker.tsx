import { RENDER_TIERS, type RenderTierId } from '../../lib/studioProduct';

export default function RenderTierPicker({
    value,
    onChange,
    disabled,
}: {
    value: RenderTierId;
    onChange: (tier: RenderTierId) => void;
    disabled?: boolean;
}) {
    return (
        <div className="grid gap-3 sm:grid-cols-3">
            {RENDER_TIERS.map((tier) => {
                const active = value === tier.id;
                return (
                    <button
                        key={tier.id}
                        type="button"
                        disabled={disabled}
                        onClick={() => onChange(tier.id)}
                        className={`rounded-2xl border p-4 text-left transition ${
                            active
                                ? 'border-violet-500/50 bg-violet-500/10 shadow-lg shadow-violet-900/20'
                                : 'border-white/[0.08] bg-black/20 hover:border-white/[0.14]'
                        } ${disabled ? 'opacity-60' : ''}`}
                    >
                        <div className="flex items-center justify-between gap-2">
                            <p className="text-sm font-bold text-white">{tier.label}</p>
                            {tier.badge && (
                                <span className="rounded-full border border-white/10 bg-black/40 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-gray-300">
                                    {tier.badge}
                                </span>
                            )}
                        </div>
                        <p className="mt-2 text-xs leading-relaxed text-gray-400">{tier.tagline}</p>
                        <div className="mt-3 flex flex-wrap gap-2 text-[10px] font-medium uppercase tracking-wider text-gray-500">
                            <span>{tier.provider}</span>
                            <span>·</span>
                            <span>{tier.creditHint}</span>
                            <span>·</span>
                            <span>{tier.eta}</span>
                        </div>
                    </button>
                );
            })}
        </div>
    );
}
