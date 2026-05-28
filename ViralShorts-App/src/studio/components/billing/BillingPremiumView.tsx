import { CheckCircle2, Sparkles, WalletCards, Zap } from 'lucide-react';
import type { RefObject } from 'react';

type PlanCard = {
    id: string;
    title: string;
    priceLabel: string;
    description: string;
    features: string[];
};

type Pack = {
    price_id: string;
    pack?: string;
    credits: number;
    price_usd: number;
};

export default function BillingPremiumView({
    publicPlans,
    normalizedCurrentPlan,
    billingActive,
    totalAc,
    shortsEstimate,
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
    totalAc: number;
    shortsEstimate: number;
    selectedPack: Pack | null;
    sortedPacks: Pack[];
    onSelectPack: (id: string) => void;
    onPlanAction: (id: string) => void;
    onPackCheckout: () => void;
    planLoadingId: string;
    packCheckoutLoadingId: string;
    checkoutError: string;
    paypalBanner: React.ReactNode;
    topUpSectionRef: RefObject<HTMLElement>;
    refundSection: React.ReactNode;
}) {
    const featuredPacks = sortedPacks.filter((p) => {
        const name = String(p.pack || '').toLowerCase();
        return ['trial', 'starter', 'creator', 'scale'].includes(name);
    });
    const extraPacks = sortedPacks.filter((p) => !featuredPacks.includes(p));

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
                        <h1 className="mt-3 text-3xl font-bold text-white sm:text-4xl">Fuel your renders. Ship your channel.</h1>
                        <p className="mt-3 max-w-xl text-sm leading-relaxed text-gray-400">
                            Draft with fal. Ship with cinematic realism. Failed renders refund automatically — no ticket required.
                        </p>
                    </div>
                    <div className="rounded-2xl border border-emerald-500/25 bg-emerald-500/10 px-6 py-4 text-center">
                        <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-emerald-200/80">Total fuel</p>
                        <p className="mt-1 text-4xl font-bold tabular-nums text-white">{totalAc}</p>
                        <p className="mt-1 text-xs text-emerald-200/70">~{shortsEstimate} draft shorts left</p>
                    </div>
                </div>
            </section>

            <section>
                <h2 className="text-lg font-bold text-white">Membership</h2>
                <p className="mt-1 text-sm text-gray-500">Monthly included credits stack with wallet top-ups.</p>
                <div className="mt-4 grid gap-4 lg:grid-cols-4">
                    {publicPlans.map((planCard) => {
                        const isCurrent = normalizedCurrentPlan === planCard.id;
                        const isPaidCurrent = billingActive && isCurrent && planCard.id !== 'free';
                        const actionLabel = planCard.id === 'free'
                            ? (isCurrent && !billingActive ? 'Current plan' : 'Included')
                            : isPaidCurrent
                                ? 'Extend plan'
                                : billingActive
                                    ? `Switch to ${planCard.title}`
                                    : `Start ${planCard.title}`;
                        return (
                            <div
                                key={planCard.id}
                                className={`flex flex-col rounded-2xl border p-5 ${
                                    isCurrent ? 'border-violet-500/40 bg-violet-500/[0.08]' : 'border-white/[0.08] bg-white/[0.02]'
                                }`}
                            >
                                <p className="text-xs uppercase tracking-wider text-gray-500">{planCard.title}</p>
                                <p className="mt-2 text-3xl font-bold text-white">{planCard.priceLabel}</p>
                                <p className="mt-2 flex-1 text-sm text-gray-400">{planCard.description}</p>
                                <ul className="mt-4 space-y-2">
                                    {planCard.features.slice(0, 3).map((f) => (
                                        <li key={f} className="flex gap-2 text-xs text-gray-300">
                                            <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0 text-emerald-400" />
                                            {f}
                                        </li>
                                    ))}
                                </ul>
                                <button
                                    type="button"
                                    disabled={planLoadingId === planCard.id || (planCard.id === 'free' && isCurrent && !billingActive)}
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
                <p className="mt-1 text-sm text-gray-500">Pay-as-you-go credits — used after monthly included fuel.</p>
                <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                    {featuredPacks.map((pack) => {
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
                                <p className="mt-2 text-2xl font-bold text-white">{pack.credits} AC</p>
                                <p className="mt-1 text-sm text-cyan-300">${Number(pack.price_usd || 0).toFixed(2)}</p>
                            </button>
                        );
                    })}
                </div>
                {extraPacks.length > 0 && (
                    <details className="mt-4 rounded-xl border border-white/[0.06] bg-white/[0.02] p-4">
                        <summary className="cursor-pointer text-sm font-medium text-gray-300">More packs for agencies & teams</summary>
                        <div className="mt-3 grid gap-2 sm:grid-cols-2">
                            {extraPacks.map((pack) => (
                                <button
                                    key={pack.price_id}
                                    type="button"
                                    onClick={() => onSelectPack(pack.price_id)}
                                    className="rounded-lg border border-white/[0.08] px-3 py-2 text-left text-sm text-gray-300 hover:bg-white/[0.03]"
                                >
                                    {String(pack.pack || 'Pack').toUpperCase()} — {pack.credits} AC — ${Number(pack.price_usd || 0).toFixed(2)}
                                </button>
                            ))}
                        </div>
                    </details>
                )}
                {selectedPack && (
                    <button
                        type="button"
                        onClick={onPackCheckout}
                        disabled={Boolean(packCheckoutLoadingId)}
                        className="mt-5 w-full max-w-md rounded-xl bg-gradient-to-r from-cyan-600 to-cyan-500 py-3 text-sm font-semibold text-white shadow-lg shadow-cyan-900/30 transition hover:from-cyan-500 hover:to-cyan-400 disabled:opacity-60 sm:w-auto sm:px-8"
                    >
                        {packCheckoutLoadingId ? 'Opening PayPal…' : `Buy ${selectedPack.credits} credits with PayPal`}
                    </button>
                )}
            </section>

            <section className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
                <div className="flex items-start gap-3">
                    <Sparkles className="mt-0.5 h-5 w-5 text-violet-400" />
                    <div className="text-sm text-gray-400 space-y-2">
                        <p><strong className="text-gray-200">Draft</strong> — fast fal iteration for hooks and pacing.</p>
                        <p><strong className="text-gray-200">Ship</strong> — premium cinematic export (Higgsfield lane on Pro+).</p>
                        <p><strong className="text-gray-200">Documentary</strong> — long-form beta; still gallery approval before animate.</p>
                    </div>
                </div>
            </section>

            {refundSection}
        </div>
    );
}
