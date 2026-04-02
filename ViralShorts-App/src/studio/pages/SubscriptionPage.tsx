import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { ArrowLeft, BadgeCheck, CreditCard, Sparkles } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, STUDIO_SITE_URL, isBillingHost } from '../shared';
import { trackMembershipPurchaseCompleted, trackOnce } from '../lib/googleAds';
import { startHostedStudioCheckout } from '../lib/invoicer';

type PublicPlanId = 'free' | 'starter' | 'creator' | 'pro';

const PUBLIC_PLAN_ORDER: PublicPlanId[] = ['free', 'starter', 'creator', 'pro'];

export default function SubscriptionPage({ onNavigate }: { onNavigate: PageNav }) {
    const {
        session,
        billingActive,
        membershipPlanId,
        membershipSource,
        monthlyCreditsRemaining,
        topupCreditsRemaining,
        creditsTotalRemaining,
        nextRenewalSource,
        checkout,
        manageBilling,
        publicPlanLimits,
        publicPlanPrices,
    } = useContext(AuthContext);
    const params = useMemo(() => {
        if (typeof window === 'undefined') return new URLSearchParams();
        return new URLSearchParams(window.location.search);
    }, []);
    const requestedPlanId = String(params.get('plan') || '').trim().toLowerCase();
    const subscriptionResult = String(params.get('subscription') || '').trim().toLowerCase();
    const subscriptionError = String(params.get('error') || '').trim();
    const [actionError, setActionError] = useState('');
    const [loadingPlanId, setLoadingPlanId] = useState('');
    const [hostedCheckoutLoading, setHostedCheckoutLoading] = useState(false);

    const normalizedMembershipSource = String(membershipSource || nextRenewalSource || '').trim().toLowerCase();
    const usesStripeMembership = billingActive && normalizedMembershipSource === 'stripe';
    const usesManualPayPalMembership = billingActive && normalizedMembershipSource === 'paypal_manual';
    const normalizedCurrentPlan = useMemo<PublicPlanId>(() => {
        const raw = String(membershipPlanId || 'free').trim().toLowerCase();
        if (raw === 'starter' || raw === 'creator' || raw === 'pro') return raw;
        return 'free';
    }, [membershipPlanId]);

    const plans = useMemo(() => {
        return PUBLIC_PLAN_ORDER.map((planId) => {
            const limits = (publicPlanLimits as Record<string, any>)[planId] || {};
            const price = Number((publicPlanPrices as Record<string, number>)[planId] || 0);
            const animatedCredits = Number(limits.animated_renders_per_month || 0);
            return {
                id: planId,
                title: capitalizePlan(planId),
                priceLabel: planId === 'free' ? '$0' : `$${price.toFixed(price % 1 === 0 ? 0 : 2)}/mo`,
                subtitle:
                    planId === 'free'
                        ? 'Enough included credits for two short-form animated renders.'
                    : planId === 'starter'
                        ? 'Best for solo operators getting started with Catalyst.'
                            : planId === 'creator'
                                ? 'More monthly headroom for active creators.'
                                : 'Highest public monthly headroom for teams and daily operators.',
                bullets: [
                    `${animatedCredits} included animation credits${planId === 'free' ? '' : ' per month'}`,
                    `${Math.max(1, Math.round(Number(limits.max_duration_sec || 0) / 60))} minute max jobs`,
                    `${String(limits.max_resolution || '720p').toUpperCase()} output`,
                    planId === 'free'
                        ? 'Short-form Create workflow included'
                        : 'Short-form Create workflow + Chat Story',
                ],
            };
        });
    }, [publicPlanLimits, publicPlanPrices]);

    useEffect(() => {
        if (subscriptionResult !== 'success') return;
        const planId = requestedPlanId || normalizedCurrentPlan;
        const value = Number((publicPlanPrices as Record<string, number>)[planId] || 0);
        const search = typeof window === 'undefined' ? '' : window.location.search;
        trackOnce(`subscription_membership_success:${search}`, () => {
            trackMembershipPurchaseCompleted(planId, value);
        });
    }, [normalizedCurrentPlan, publicPlanPrices, requestedPlanId, subscriptionResult]);

    const currentStatus = billingActive ? `Active on ${capitalizePlan(normalizedCurrentPlan)}` : 'Free plan active';

    const handleBack = () => {
        if (isBillingHost) {
            window.location.href = STUDIO_SITE_URL;
            return;
        }
        onNavigate('dashboard');
    };

    const handleOpenBilling = () => {
        if (isBillingHost) {
            window.location.href = `${BILLING_SITE_URL}?page=billing`;
            return;
        }
        onNavigate('billing');
    };

    const handlePlanAction = useCallback(async (planId: PublicPlanId) => {
        if (planId === 'free') {
            if (!session) onNavigate('auth');
            return;
        }
        if (!session) {
            onNavigate('auth');
            return;
        }
        setActionError('');
        setLoadingPlanId(planId);
        try {
            if (billingActive && normalizedCurrentPlan === planId) {
                if (usesStripeMembership) {
                    const err = await manageBilling();
                    if (err) setActionError(err);
                    return;
                }
                if (usesManualPayPalMembership) {
                    const err = await checkout(planId);
                    if (err) setActionError(err);
                    return;
                }
            }
            const err = await checkout(planId);
            if (err) setActionError(err);
        } finally {
            setLoadingPlanId('');
        }
    }, [billingActive, checkout, manageBilling, normalizedCurrentPlan, onNavigate, session, usesManualPayPalMembership, usesStripeMembership]);

    const handleHostedCheckout = useCallback(async () => {
        if (!session) {
            onNavigate('auth');
            return;
        }
        setActionError('');
        setHostedCheckoutLoading(true);
        try {
            const error = await startHostedStudioCheckout(session);
            if (error) setActionError(error);
        } finally {
            setHostedCheckoutLoading(false);
        }
    }, [onNavigate, session]);

    return (
        <>
            <NavBar onNavigate={onNavigate} active="subscription" />
            <div className="mx-auto max-w-6xl px-6 pt-24 pb-12">
                <div className="mb-8 flex flex-wrap items-center justify-between gap-4">
                    <div>
                        <div className="flex items-center gap-2 text-violet-300 text-sm font-semibold uppercase tracking-[0.18em]">
                            <Sparkles className="h-4 w-4" />
                            Plans
                        </div>
                        <h1 className="mt-3 text-3xl font-bold text-white">Free, Starter, Creator, and Pro</h1>
                        <p className="mt-2 max-w-3xl text-sm text-gray-400">
                            Free gets users into Catalyst short-form. The three monthly plans add more included credits and unlock Chat Story, while wallet top-ups stay separate on the billing page.
                        </p>
                        <div className="mt-5 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 p-4 text-sm text-cyan-100">
                            Hosted Studio business licensing now starts through Invoicer at a one-time $300 price. The monthly plan cards below are the legacy Studio billing path until the separate entitlement backend is migrated.
                        </div>
                    </div>
                    <div className="flex flex-wrap gap-3">
                        <button
                            type="button"
                            onClick={handleBack}
                            className="inline-flex items-center gap-2 rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:border-white/[0.14] hover:bg-white/[0.06]"
                        >
                            <ArrowLeft className="h-4 w-4" />
                            Back
                        </button>
                        <button
                            type="button"
                            onClick={handleOpenBilling}
                            className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500"
                        >
                            Open Billing
                        </button>
                        <button
                            type="button"
                            onClick={() => void handleHostedCheckout()}
                            disabled={hostedCheckoutLoading}
                            className="inline-flex items-center gap-2 rounded-xl border border-cyan-500/30 bg-cyan-500/10 px-4 py-2.5 text-sm font-semibold text-cyan-100 transition hover:border-cyan-400/50 hover:bg-cyan-500/15 disabled:opacity-60"
                        >
                            <CreditCard className="h-4 w-4" />
                            {hostedCheckoutLoading ? 'Opening Invoicer...' : 'Start $300 Hosted License'}
                        </button>
                    </div>
                </div>

                <div className="grid gap-6 lg:grid-cols-[1.1fr,0.9fr]">
                    <section className="rounded-3xl border border-violet-500/20 bg-violet-500/[0.06] p-6">
                        <p className="text-xs uppercase tracking-[0.18em] text-violet-200/70">Current Status</p>
                        <h2 className="mt-2 text-3xl font-bold text-white">{currentStatus}</h2>
                        <p className="mt-3 max-w-2xl text-sm text-gray-300">
                            Included credits burn before the wallet. If you stay on Free, you still keep enough included credits for two short-form animated renders every cycle.
                        </p>

                        <div className="mt-6 grid gap-4 md:grid-cols-2">
                            {plans.map((planCard) => {
                                const isCurrent = normalizedCurrentPlan === planCard.id;
                                const isPaidCurrent = billingActive && isCurrent && planCard.id !== 'free';
                                const actionLabel = planCard.id === 'free'
                                    ? (isCurrent && !billingActive ? 'Current plan' : 'Included with account')
                                    : isPaidCurrent
                                        ? (usesStripeMembership ? 'Manage plan' : 'Extend plan')
                                        : billingActive
                                            ? `Switch to ${planCard.title}`
                                            : `Start ${planCard.title}`;
                                return (
                                    <div
                                        key={planCard.id}
                                        className={`rounded-2xl border p-4 ${
                                            isCurrent
                                                ? 'border-violet-500/40 bg-black/20'
                                                : 'border-white/[0.08] bg-black/20'
                                        }`}
                                    >
                                        <div className="flex items-start justify-between gap-3">
                                            <div>
                                                <p className="text-sm font-bold text-white">{planCard.title}</p>
                                                <p className="mt-2 text-2xl font-bold text-white">{planCard.priceLabel}</p>
                                            </div>
                                            {isCurrent && (
                                                <span className="rounded-full border border-violet-400/30 bg-violet-500/10 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-violet-200">
                                                    Current
                                                </span>
                                            )}
                                        </div>
                                        <p className="mt-3 text-sm text-gray-300">{planCard.subtitle}</p>
                                        <div className="mt-4 space-y-2">
                                            {planCard.bullets.map((bullet) => (
                                                <div key={bullet} className="flex items-start gap-2 text-sm text-gray-300">
                                                    <BadgeCheck className="mt-0.5 h-4 w-4 shrink-0 text-emerald-300" />
                                                    <span>{bullet}</span>
                                                </div>
                                            ))}
                                        </div>
                                        <button
                                            type="button"
                                            onClick={() => void handlePlanAction(planCard.id)}
                                            disabled={loadingPlanId === planCard.id || (planCard.id === 'free' && isCurrent && !billingActive)}
                                            className="mt-5 w-full rounded-xl bg-white/[0.08] px-4 py-3 text-sm font-semibold text-white transition hover:bg-white/[0.14] disabled:opacity-60"
                                        >
                                            {loadingPlanId === planCard.id ? 'Opening...' : actionLabel}
                                        </button>
                                    </div>
                                );
                            })}
                        </div>
                    </section>

                    <aside className="space-y-6">
                        <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                            <h2 className="text-lg font-semibold text-white">Balance</h2>
                            <div className="mt-4 grid gap-3">
                                <MetricCard label="Included Credits" value={Number(monthlyCreditsRemaining || 0)} />
                                <MetricCard label="Credit Wallet" value={Number(topupCreditsRemaining || 0)} />
                                <MetricCard label="Total Available" value={Number(creditsTotalRemaining || 0)} />
                            </div>
                        </section>
                        <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                            <h2 className="text-lg font-semibold text-white">How billing works</h2>
                            <div className="mt-4 space-y-3 text-sm text-gray-300">
                                <p>1. Every signed-in account starts on Free.</p>
                                <p>2. Free and paid plans are currently for short-form only.</p>
                                <p>3. Paid plans add more included credits and unlock Chat Story.</p>
                                <p>4. Top-up packs live on the billing page and stack on top of any plan.</p>
                                <p>5. If a monthly plan expires, wallet credits stay on the account.</p>
                            </div>
                        </section>
                    </aside>
                </div>

                {actionError && (
                    <p className="mt-6 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-5 py-4 text-sm text-amber-100">
                        {actionError}
                    </p>
                )}
                {subscriptionResult === 'success' && (
                    <p className="mt-6 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-5 py-4 text-sm text-emerald-100">
                        Your monthly plan is active. Included credits now burn before the wallet.
                    </p>
                )}
                {subscriptionResult === 'manual' && (
                    <p className="mt-6 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-5 py-4 text-sm text-cyan-100">
                        This account is on manual PayPal renewal. Click the same plan again whenever you want to extend another month.
                    </p>
                )}
                {subscriptionResult === 'cancelled' && (
                    <p className="mt-6 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-5 py-4 text-sm text-amber-100">
                        Monthly checkout was cancelled.{subscriptionError ? ` ${subscriptionError}` : ''}
                    </p>
                )}
            </div>
        </>
    );
}

function capitalizePlan(planId: PublicPlanId) {
    return planId.charAt(0).toUpperCase() + planId.slice(1);
}

function MetricCard({ label, value }: { label: string; value: number }) {
    return (
        <div className="rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3">
            <p className="text-[10px] uppercase tracking-[0.18em] text-gray-500">{label}</p>
            <p className="mt-2 text-2xl font-bold text-white">{value}</p>
        </div>
    );
}
