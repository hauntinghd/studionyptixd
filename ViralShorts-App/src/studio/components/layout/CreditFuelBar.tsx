import { estimateShortsRemaining, type RenderTierId } from '../../lib/studioProduct';

export default function CreditFuelBar({
    totalAc,
    tier = 'draft',
    onTopUp,
}: {
    totalAc: number;
    tier?: RenderTierId;
    onTopUp?: () => void;
}) {
    const shortsLeft = estimateShortsRemaining(totalAc, tier);
    return (
        <div className="flex items-center gap-2 rounded-xl border border-white/[0.08] bg-black/40 px-2 py-1.5">
            <div className="hidden sm:block rounded-lg bg-gradient-to-br from-violet-600/30 to-cyan-600/20 px-3 py-1.5">
                <p className="text-[9px] font-semibold uppercase tracking-[0.2em] text-gray-400">Render fuel</p>
                <p className="text-lg font-bold leading-tight text-white tabular-nums">{totalAc}</p>
            </div>
            <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/10 px-3 py-1.5">
                <p className="text-[9px] font-semibold uppercase tracking-[0.2em] text-emerald-200/70">Shorts left</p>
                <p className="text-lg font-bold leading-tight text-emerald-100 tabular-nums">~{shortsLeft}</p>
            </div>
            {onTopUp && (
                <button
                    type="button"
                    onClick={onTopUp}
                    className="ml-1 rounded-lg bg-cyan-600 px-3 py-2 text-xs font-semibold text-white transition hover:bg-cyan-500"
                >
                    Top up
                </button>
            )}
        </div>
    );
}
