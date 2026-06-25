import { CheckCircle2, CreditCard, Gauge, Sparkles, WalletCards, Zap } from 'lucide-react';
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
        <div className="mx-auto max-w-7xl space-y-8">
            {paypalBanner}
            {checkoutError && (
                <div className="rounded-xl border border-rose-500/30 bg-rose-500/10 px-4 py-3 text-sm text-rose-100">
                    {checkoutError}
                </div>
            )}

            <section className="relative overflow-hidden rounded-2xl border border-white/[0.08] bg-[radial-gradient(circle_at_14%_0%,rgba(6,182,212,0.18),transparent_34%),linear-gradient(135deg,rgba(8,47,73,0.28),rgba(9,9,11,0.96)_48%,rgba(46,16,101,0.34))] p-6 shadow-2xl shadow-black/30 sm:p-8">
                <div className="relative z-10 flex flex-wrap items-end justify-between gap-6">
                    <div>
                        <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.2em] text-cyan-300">
                            <WalletCards className="h-4 w-4" />
                            Billing
                        </div>
                        <h1 className="mt-3 text-3xl font-bold text-white sm:text-4xl">One wallet. Every model.</h1>
                        <p className="mt-3 max-w-2xl text-sm leading-relaxed text-gray-400">
                            One credit wallet covers Studio Agent, image and video generation, voice, sound, and model usage.
                        </p>
                    </div>
                    <div className="grid min-w-[320px] grid-cols-2 gap-3">
                        <HeroStat icon={Gauge} label="Balance" value={creditBalance >= 999999 ? 'Unlimited' : creditBalance.toLocaleString()} sub="unified credits" />
                        <HeroStat
                            icon={CreditCard}
                            label="Plan"
                            value={
                                billingActive
                                    ? normalizedCurrentPlan === 'creator'
                                        ? 'Studio'
                                        : normalizedCurrentPlan === 'studio'
                                            ? 'Studio Pro'
                                            : 'Active'
                                    : 'No plan'
                            }
                            sub="membership"
                        />
                    </div>
                </div>
            </section>

            <section>
                <div className="flex flex-wrap items-end justify-between gap-4">
                    <div>
                        <h2 className="text-lg font-bold text-white">Plans</h2>
                        <p className="mt-1 text-sm text-gray-500">Monthly credits roll over once. Purchased reloads remain available until used.</p>
                    </div>
                    <span className="rounded-full border border-cyan-400/20 bg-cyan-500/10 px-3 py-1 text-[10px] font-semibold uppercase tracking-wider text-cyan-200">
                        Usage-priced
                    </span>
                </div>
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
                                className={`relative flex flex-col rounded-2xl border p-5 shadow-sm shadow-black/30 ${
                                    isCurrent ? 'border-violet-500/40 bg-violet-500/[0.08]' : 'border-white/[0.08] bg-gradient-to-br from-white/[0.04] to-white/[0.015]'
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
                                    {planLoadingId === planCard.id ? 'Opening...' : actionLabel}
                                </button>
                            </div>
                        );
                    })}
                </div>
            </section>

            <section ref={topUpSectionRef} id="topup-packs" className="scroll-mt-24 rounded-2xl border border-white/[0.08] bg-white/[0.025] p-5">
                <div className="flex items-center gap-2">
                    <Zap className="h-5 w-5 text-cyan-400" />
                    <h2 className="text-lg font-bold text-white">Top up</h2>
                </div>
                <p className="mt-1 text-sm text-gray-500">Add 1,000 credits for $25 whenever a larger production needs more capacity.</p>
                <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                    {sortedPacks.map((pack) => {
                        const active = selectedPack?.price_id === pack.price_id;
                        return (
                            <button
                                key={pack.price_id}
                                type="button"
                                onClick={() => onSelectPack(pack.price_id)}
                                className={`rounded-2xl border p-4 text-left transition ${
                                    active ? 'border-cyan-500/50 bg-cyan-500/10 shadow-lg shadow-cyan-950/20' : 'border-white/[0.08] bg-black/20 hover:border-white/[0.14]'
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
                            ? 'Opening Stripe...'
                            : `Buy ${creditLabel(selectedPack.credits)} with Stripe`}
                    </button>
                )}
            </section>

            <section className="rounded-2xl border border-white/[0.06] bg-gradient-to-br from-white/[0.04] to-white/[0.015] p-5">
                <div className="flex items-start gap-3">
                    <Sparkles className="mt-0.5 h-5 w-5 text-violet-400" />
                    <div className="space-y-2 text-sm text-gray-400">
                        <p><strong className="text-gray-200">Studio Agent</strong> - OpenRouter models debited per token at live rates.</p>
                        <p><strong className="text-gray-200">fal</strong> - image, video, SFX, and motion graphics charged per render.</p>
                        <p><strong className="text-gray-200">ElevenLabs</strong> - TTS and voice cloning charged per character.</p>
                        <p><strong className="text-gray-200">Protection</strong> - paid jobs reserve credits before starting; failed starts are refunded automatically.</p>
                    </div>
                </div>
            </section>

            {refundSection}
        </div>
    );
}

function HeroStat({ icon: Icon, label, value, sub }: { icon: typeof Gauge; label: string; value: string; sub: string }) {
    return (
        <div className="rounded-2xl border border-white/[0.08] bg-black/25 px-5 py-4">
            <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.2em] text-cyan-200/80">
                <Icon className="h-3.5 w-3.5 text-cyan-300" />
                {label}
            </div>
            <p className="mt-2 text-xl font-bold capitalize text-white">{value}</p>
            <p className="mt-1 text-xs text-gray-500">{sub}</p>
        </div>
    );
}
