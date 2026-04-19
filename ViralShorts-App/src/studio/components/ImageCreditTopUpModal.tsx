import { Image as ImageIcon, Sparkles, X, Zap } from 'lucide-react';

export interface IGPack {
    id: string;
    credits: number;
    price_usd: number;
    label: string;
    highlight?: boolean;
}

export const DEFAULT_IG_PACKS: IGPack[] = [
    { id: 'ig_100', credits: 100, price_usd: 5, label: '100 IGs', },
    { id: 'ig_500', credits: 500, price_usd: 19, label: '500 IGs', highlight: true },
    { id: 'ig_2000', credits: 2000, price_usd: 59, label: '2,000 IGs' },
];

interface Props {
    open: boolean;
    requiredCredits: number;
    availableCredits: number;
    packs?: IGPack[];
    onClose: () => void;
    onSelectPack: (pack: IGPack) => void;   // wire to /api/billing/image-credits/checkout
}

// Image Generation Credit (IG) top-up modal. Mirrors the animation
// credit prompt pattern but scoped to image-model credits. Usage-based
// per Casey's 2026-04-19 direction — no subscription gate.
//
// Checkout flow: deep-links into the existing BillingPage at
// /billing?pack=<id>&provider=paypal which runs the PayPal order flow
// (BillingPage already has verifyPayPalOrder + paypalOrderId wiring).
// Studio is PayPal-only for billing per Casey's direction.
export default function ImageCreditTopUpModal({
    open,
    requiredCredits,
    availableCredits,
    packs = DEFAULT_IG_PACKS,
    onClose,
    onSelectPack,
}: Props) {
    if (!open) return null;
    const shortfall = Math.max(0, requiredCredits - availableCredits);

    return (
        <div
            className="fixed inset-0 z-50 flex items-center justify-center bg-black/75 p-4"
            onClick={onClose}
        >
            <div
                className="w-full max-w-lg overflow-hidden rounded-2xl border border-white/10 bg-[#0b0b12] shadow-2xl"
                onClick={(e) => e.stopPropagation()}
            >
                <div className="flex items-start justify-between gap-4 border-b border-white/[0.06] p-5">
                    <div className="flex items-start gap-3">
                        <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-gradient-to-br from-cyan-500/25 to-violet-500/25 text-cyan-200">
                            <ImageIcon className="h-5 w-5" />
                        </span>
                        <div>
                            <h3 className="text-lg font-semibold text-white">Top up Image Credits</h3>
                            <p className="mt-1 text-xs text-gray-400">
                                {shortfall > 0 ? (
                                    <>You need <span className="font-semibold text-white">{requiredCredits}</span> IGs — you have <span className="font-semibold text-white">{availableCredits}</span>. Short <span className="font-semibold text-amber-300">{shortfall}</span>.</>
                                ) : (
                                    <>Pick a pack that gives you the exact amount you need. Usage-based, no subscription.</>
                                )}
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

                <div className="space-y-3 p-5">
                    {packs.map((pack) => {
                        const covers = pack.credits >= shortfall;
                        return (
                            <button
                                key={pack.id}
                                type="button"
                                onClick={() => onSelectPack(pack)}
                                className={`group flex w-full items-center justify-between gap-3 rounded-xl border p-4 text-left transition ${
                                    pack.highlight
                                        ? 'border-violet-500/60 bg-violet-500/[0.08] hover:bg-violet-500/[0.12]'
                                        : 'border-white/[0.08] bg-white/[0.02] hover:border-white/20 hover:bg-white/[0.04]'
                                }`}
                            >
                                <div className="flex items-center gap-3">
                                    <span className={`flex h-9 w-9 items-center justify-center rounded-lg ${pack.highlight ? 'bg-violet-500/20 text-violet-200' : 'bg-white/[0.05] text-gray-300'}`}>
                                        {pack.highlight ? <Sparkles className="h-5 w-5" /> : <Zap className="h-5 w-5" />}
                                    </span>
                                    <div>
                                        <div className="text-sm font-semibold text-white">{pack.label}</div>
                                        <div className="text-[11px] text-gray-400">
                                            ~{Math.round((pack.credits / pack.price_usd) * 100) / 100} IGs / $1 · {covers ? 'Covers your current render' : `Covers ${Math.floor(pack.credits / Math.max(1, shortfall || 1))}× your current need`}
                                        </div>
                                    </div>
                                </div>
                                <div className="text-right">
                                    <div className="text-sm font-bold text-white">${pack.price_usd}</div>
                                    {pack.highlight && (
                                        <div className="text-[10px] font-semibold uppercase tracking-wider text-violet-300">Popular</div>
                                    )}
                                </div>
                            </button>
                        );
                    })}
                </div>

                <div className="border-t border-white/[0.06] px-5 py-3 text-[11px] text-gray-500">
                    IGs never expire. Use them across any image model — premium lanes cost more IGs per image. Checkout runs through PayPal.
                </div>
            </div>
        </div>
    );
}
