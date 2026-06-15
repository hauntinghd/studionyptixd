import { CheckCircle2, Clock, Cpu } from 'lucide-react';
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
        <div className="grid gap-3 lg:grid-cols-3">
            {RENDER_TIERS.map((tier) => {
                const active = value === tier.id;
                return (
                    <button
                        key={tier.id}
                        type="button"
                        disabled={disabled}
                        onClick={() => onChange(tier.id)}
                        className={`relative overflow-hidden rounded-2xl border p-4 text-left shadow-sm shadow-black/25 transition ${
                            active
                                ? 'border-violet-500/55 bg-violet-500/10 shadow-lg shadow-violet-900/20'
                                : 'border-white/[0.08] bg-gradient-to-br from-white/[0.04] to-transparent hover:border-white/[0.16]'
                        } ${disabled ? 'opacity-60' : ''}`}
                    >
                        <div className="flex items-start justify-between gap-3">
                            <div>
                                <p className="text-base font-bold text-white">{tier.label}</p>
                                <p className="mt-2 text-xs leading-relaxed text-gray-400">{tier.tagline}</p>
                            </div>
                            {active ? (
                                <CheckCircle2 className="h-5 w-5 shrink-0 text-emerald-400" />
                            ) : tier.badge ? (
                                <span className="rounded-full border border-white/10 bg-black/40 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-gray-300">
                                    {tier.badge}
                                </span>
                            ) : null}
                        </div>
                        <div className="mt-4 grid grid-cols-3 gap-2 text-[10px] uppercase tracking-wider text-gray-500">
                            <TierPill icon={Cpu} label={tier.provider} />
                            <TierPill icon={CheckCircle2} label={tier.creditHint} />
                            <TierPill icon={Clock} label={tier.eta} />
                        </div>
                    </button>
                );
            })}
        </div>
    );
}

function TierPill({ icon: Icon, label }: { icon: typeof Cpu; label: string }) {
    return (
        <div className="min-w-0 rounded-lg border border-white/[0.06] bg-black/25 px-2 py-2">
            <Icon className="mb-1 h-3 w-3 text-cyan-300/80" />
            <span className="block truncate">{label}</span>
        </div>
    );
}
