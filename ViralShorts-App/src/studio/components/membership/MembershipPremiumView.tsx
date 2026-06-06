import { ArrowLeft, BadgeCheck, Sparkles } from 'lucide-react';
import type { ReactNode } from 'react';

type PlanCard = {
    id: string;
    title: string;
    priceLabel: string;
    subtitle: string;
    bullets: string[];
    bestValue?: boolean;
    isCurrent: boolean;
    actionLabel: string;
    loading: boolean;
    disabled: boolean;
    onAction: () => void;
};

export default function MembershipPremiumView({
    currentStatus,
    plans,
    creditBalance,
    onBack,
    onOpenBilling,
    banners,
    actionError,
}: {
    currentStatus: string;
    plans: PlanCard[];
    creditBalance: number;
    onBack: () => void;
    onOpenBilling: () => void;
    banners: ReactNode;
    actionError: string;
}) {
    return (
        <div className="mx-auto max-w-6xl space-y-8">
            {banners}
            {actionError && (
                <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                    {actionError}
                </div>
            )}

            <section className="relative overflow-hidden rounded-2xl border border-white/[0.08] bg-gradient-to-br from-violet-950/40 via-[#0c0c10] to-cyan-950/20 p-6 sm:p-8">
                <div className="relative z-10 flex flex-wrap items-end justify-between gap-6">
                    <div>
                        <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.2em] text-violet-300">
                            <Sparkles className="h-4 w-4" />
                            Membership
                        </div>
                        <h1 className="mt-3 text-3xl font-bold text-white sm:text-4xl">Creator & Studio</h1>
                        <p className="mt-3 max-w-xl text-sm leading-relaxed text-gray-400">
                            One unified credit wallet for Studio Agent, OpenRouter, fal, and ElevenLabs. Top-ups stack on any plan.
                        </p>
                        <div className="mt-4 flex flex-wrap gap-3">
                            <button
                                type="button"
                                onClick={onBack}
                                className="inline-flex items-center gap-2 rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:border-white/[0.14] hover:bg-white/[0.06]"
                            >
                                <ArrowLeft className="h-4 w-4" />
                                Back to Studio
                            </button>
                            <button
                                type="button"
                                onClick={onOpenBilling}
                                className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500"
                            >
                                Open Billing
                            </button>
                        </div>
                    </div>
                    <div className="rounded-2xl border border-violet-500/25 bg-violet-500/10 px-6 py-4 text-center">
                        <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-violet-200/80">Current</p>
                        <p className="mt-1 text-xl font-bold text-white">{currentStatus}</p>
                        <p className="mt-1 text-xs text-violet-200/70">
                            {creditBalance >= 999999 ? '∞' : creditBalance.toLocaleString()} credits
                        </p>
                    </div>
                </div>
            </section>

            <section>
                <h2 className="text-lg font-bold text-white">Plans</h2>
                <p className="mt-1 text-sm text-gray-500">Monthly credits refresh each billing cycle.</p>
                <div className="mt-4 grid gap-4 md:grid-cols-2">
                    {plans.map((plan) => (
                        <div
                            key={plan.id}
                            className={`relative flex flex-col rounded-2xl border p-5 ${
                                plan.isCurrent
                                    ? 'border-violet-500/40 bg-violet-500/[0.08]'
                                    : 'border-white/[0.08] bg-white/[0.02]'
                            }`}
                        >
                            {plan.bestValue && (
                                <span className="absolute right-4 top-4 rounded-full border border-cyan-400/30 bg-cyan-500/10 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-cyan-200">
                                    Best value
                                </span>
                            )}
                            <div className="flex items-start justify-between gap-2">
                                <p className="text-xs uppercase tracking-wider text-gray-500">{plan.title}</p>
                                {plan.isCurrent && (
                                    <span className="rounded-full border border-violet-400/30 bg-violet-500/10 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-violet-200">
                                        Current
                                    </span>
                                )}
                            </div>
                            <p className="mt-2 text-3xl font-bold text-white">{plan.priceLabel}</p>
                            <p className="mt-2 flex-1 text-sm text-gray-400">{plan.subtitle}</p>
                            <ul className="mt-4 space-y-2">
                                {plan.bullets.map((bullet) => (
                                    <li key={bullet} className="flex gap-2 text-xs text-gray-300">
                                        <BadgeCheck className="mt-0.5 h-3.5 w-3.5 shrink-0 text-emerald-400" />
                                        {bullet}
                                    </li>
                                ))}
                            </ul>
                            <button
                                type="button"
                                onClick={plan.onAction}
                                disabled={plan.disabled || plan.loading}
                                className="mt-5 w-full rounded-xl bg-violet-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-violet-500 disabled:opacity-60"
                            >
                                {plan.loading ? 'Opening…' : plan.actionLabel}
                            </button>
                        </div>
                    ))}
                </div>
            </section>

            <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-6">
                <h2 className="text-lg font-semibold text-white">How billing works</h2>
                <ol className="mt-4 list-decimal space-y-2 pl-4 text-sm text-gray-400">
                    <li>Pick Creator ($60 / 5,000 credits) or Studio ($200 / 20,000 credits).</li>
                    <li>Credits debit from real usage — OpenRouter tokens, fal renders, ElevenLabs characters.</li>
                    <li>Top-up packs on Billing stack with your monthly grant.</li>
                    <li>Wallet credits stay on your account if membership lapses.</li>
                </ol>
            </section>
        </div>
    );
}
