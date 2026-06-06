import { CheckCircle2, Sparkles, WalletCards, Zap } from 'lucide-react';
import type { ReactNode, RefObject } from 'react';

type PlanCard = {
    id: string;
    title: string;
    priceLabel: string;
    description: string;
    features: string[];
    bestValue?: boolean;
};

type Pack = {
    price_id: string;
    pack?: string;
    credits: number;
    price_usd: number;
};

function creditLabel(count: number): string {
    return count === 1 ? '1 credit' : `${count.toLocaleString()} credits`;
}

export default function BillingPremiumView({
    publicPlans,
    normalizedCurrentPlan,
    billingActive,
    creditBalance,
    selectedPack,
    sortedPacks,
    onSelectPack,
    onPlanAction,
    onPackCheckout,
    planLoadingId,
    packCheckoutLoadingId,
    checkoutError,
    paypalBanner,
    topUpSectionRef,
    refundSection,
}: {
    publicPlans: PlanCard[];
    normalizedCurrentPlan: string;
    billingActive: boolean;
    creditBalance: number;
    selectedPack: Pack | null;
    sortedPacks: Pack[];
    onSelectPack: (id: string) => void;
    onPlanAction: (id: string) => void;
    onPackCheckout: () => void;
    planLoadingId: string;
    packCheckoutLoadingId: string;
    checkoutError: string;
    paypalBanner: ReactNode;
    topUpSectionRef: RefObject<HTMLElement>;
    refundSection: React.ReactNode;
}) {
    return (
        <div className="mx-auto max-w-6xl space-y-8">
            {paypalBanner}
            {checkoutError && (
                <div className="rounded-xl border border-rose-500/30 bg-rose-500/10 px-4 py-3 text-sm text-rose-100">
                    {checkoutError}
                </div>
            )}

            <section className="relative overflow-hidden rounded-2xl border border-white/[0.08] bg-gradient-to-br from-cyan-950/30 via-[#0c0c10] to-violet-950/40 p-6 sm:p-8">
                <div className="relative z-10 flex flex-wrap items-end justify-between gap-6">
                    <div>
                        <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.2em] text-cyan-300">
                            <WalletCards className="h-4 w-4" />
                            Billing
                        </div>
                        <h1 className="mt-3 text-3xl font-bold text-white sm:text-4xl">One wallet. Every model.</h1>
                        <p className="mt-3 max-w-xl text-sm leading-relaxed text-gray-400">
                            Credits cover OpenRouter, fal image/video, and ElevenLabs — debited from real usage, not guesswork.
                        </p>
                    </div>
                    <div className="rounded-2xl border border-violet-500/25 bg-violet-500/10 px-6 py-4 text-center">
                        <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-violet-200/80">Your balance</p>
                        <p className="mt-1 text-4xl font-bold tabular-nums text-white">
                            {creditBalance >= 999999 ? '∞' : creditBalance.toLocaleString()}
                        </p>
                        <p className="mt-1 text-xs text-violet-200/70">unified credits</p>
                    </div>
                </div>
            </section>

            <section>
                <h2 className="text-lg font-bold text-white">Plans</h2>
                <p className="mt-1 text-sm text-gray-500">Two tiers. Monthly credits refresh each billing cycle. Top-ups stack on top.</p>
                <div className="mt-4 grid gap-4 md:grid-cols-2">
                    {publicPlans.map((planCard) => {
                        const isCurrent = billingActive && normalizedCurrentPlan === planCard.id;
                        const actionLabel = isCurrent
                            ? 'Extend plan'
                            : billingActive
                                ? `Switch to ${planCard.title}`
                                : `Start ${planCard.title}`;
                        return (
                            <div
                                key={planCard.id}
                                className={`relative flex flex-col rounded-2xl border p-5 ${
                                    isCurrent ? 'border-violet-500/40 bg-violet-500/[0.08]' : 'border-white/[0.08] bg-white/[0.02]'
                                }`}
                            >
                                {planCard.bestValue && (
                                    <span className="absolute right-4 top-4 rounded-full border border-cyan-400/30 bg-cyan-500/10 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-cyan-200">
                                        Best value
                                    </span>
                                )}
                                <p className="text-xs uppercase tracking-wider text-gray-500">{planCard.title}</p>
                                <p className="mt-2 text-3xl font-bold text-white">{planCard.priceLabel}</p>
                                <p className="mt-2 flex-1 text-sm text-gray-400">{planCard.description}</p>
                                <ul className="mt-4 space-y-2">
                                    {planCard.features.map((f) => (
                                        <li key={f} className="flex gap-2 text-xs text-gray-300">
                                            <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0 text-emerald-400" />
                                            {f}
                                        </li>
                                    ))}
                                </ul>
                                <button
                                    type="button"
                                    disabled={planLoadingId === planCard.id}
                                    onClick={() => onPlanAction(planCard.id)}
                                    className={`mt-5 w-full rounded-xl py-2.5 text-sm font-semibold transition ${
                                        isCurrent ? 'bg-white/10 text-white hover:bg-white/15' : 'bg-violet-600 text-white hover:bg-violet-500'
                                    } disabled:opacity-50`}
                                >
                                    {planLoadingId === planCard.id ? 'Opening…' : actionLabel}
                                </button>
                            </div>
                        );
                    })}
                </div>
            </section>

            <section ref={topUpSectionRef} id="topup-packs" className="scroll-mt-24">
                <div className="flex items-center gap-2">
                    <Zap className="h-5 w-5 text-cyan-400" />
                    <h2 className="text-lg font-bold text-white">Top up</h2>
                </div>
                <p className="mt-1 text-sm text-gray-500">Pay-as-you-go credits — used after your monthly grant runs out.</p>
                <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                    {sortedPacks.map((pack) => {
                        const active = selectedPack?.price_id === pack.price_id;
                        return (
                            <button
                                key={pack.price_id}
                                type="button"
                                onClick={() => onSelectPack(pack.price_id)}
                                className={`rounded-2xl border p-4 text-left transition ${
                                    active ? 'border-cyan-500/50 bg-cyan-500/10' : 'border-white/[0.08] bg-black/20 hover:border-white/[0.14]'
                                }`}
                            >
                                <p className="text-xs uppercase tracking-wider text-gray-500">{String(pack.pack || 'Pack').toUpperCase()}</p>
                                <p className="mt-2 text-2xl font-bold text-white">{pack.credits.toLocaleString()}</p>
                                <p className="text-[10px] uppercase tracking-wider text-gray-500">credits</p>
                                <p className="mt-1 text-sm text-cyan-300">${Number(pack.price_usd || 0).toFixed(2)}</p>
                            </button>
                        );
                    })}
                </div>
                {selectedPack && (
                    <button
                        type="button"
                        onClick={onPackCheckout}
                        disabled={Boolean(packCheckoutLoadingId)}
                        className="mt-5 w-full max-w-md rounded-xl bg-gradient-to-r from-cyan-600 to-cyan-500 py-3 text-sm font-semibold text-white shadow-lg shadow-cyan-900/30 transition hover:from-cyan-500 hover:to-cyan-400 disabled:opacity-60 sm:w-auto sm:px-8"
                    >
                        {packCheckoutLoadingId
                            ? 'Opening Stripe…'
                            : `Buy ${creditLabel(selectedPack.credits)} with Stripe`}
                    </button>
                )}
            </section>

            <section className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
                <div className="flex items-start gap-3">
                    <Sparkles className="mt-0.5 h-5 w-5 text-violet-400" />
                    <div className="space-y-2 text-sm text-gray-400">
                        <p><strong className="text-gray-200">Studio Agent</strong> — OpenRouter models debited per token at live rates.</p>
                        <p><strong className="text-gray-200">fal</strong> — image, video, SFX, and motion graphics charged per render.</p>
                        <p><strong className="text-gray-200">ElevenLabs</strong> — TTS and voice cloning charged per character.</p>
                    </div>
                </div>
            </section>

            {refundSection}
        </div>
    );
}
