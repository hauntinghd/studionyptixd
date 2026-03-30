import { useCallback, useContext, useMemo, useState } from 'react';
import { ArrowLeft, BadgeCheck, Sparkles } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, STUDIO_SITE_URL, isBillingHost } from '../shared';

type PlanCard = {
    id: string;
    title: string;
    price: string;
    subtitle: string;
    credits: string;
    features: string[];
    badge?: string;
};

const planMeta = [
    {
        id: 'starter',
        title: 'Starter',
        fallbackPrice: '$14/mo',
        subtitle: 'For creators who want Chat Story and starter AutoClipper access.',
        credits: 'Starter recurring access',
        features: [
            'Chat Story access',
            'AutoClipper beta access',
            'Recurring monthly access',
            'Free image generation stays enabled',
            'Standard queue priority',
        ],
    },
    {
        id: 'creator',
        title: 'Creator',
        fallbackPrice: '$24/mo',
        subtitle: 'For active creators who need more monthly headroom for Chat Story and clipping.',
        credits: 'Balanced recurring access',
        badge: 'Recommended',
        features: [
            'Chat Story access',
            'AutoClipper beta access',
            'More monthly headroom',
            'Priority queue handling',
            'More headroom for repeat clip jobs',
        ],
    },
    {
        id: 'pro',
        title: 'Pro',
        fallbackPrice: '$39/mo',
        subtitle: 'For operators and teams using Chat Story plus repeat clip workflows.',
        credits: 'Highest public monthly tier',
        features: [
            'Chat Story access',
            'AutoClipper beta access',
            'Highest public monthly headroom',
            'Top queue priority',
            'Best fit for daily volume',
            'Built to stack with one-time AC packs',
        ],
    },
];

export default function SubscriptionPage({ onNavigate }: { onNavigate: PageNav }) {
    const { session, plan, monthlyCreditsRemaining, creditsTotalRemaining, billingActive, nextRenewalSource, checkout, manageBilling, publicPlanPrices } = useContext(AuthContext);
    const params = useMemo(() => {
        if (typeof window === 'undefined') return new URLSearchParams();
        return new URLSearchParams(window.location.search);
    }, []);
    const subscriptionResult = String(params.get('subscription') || '').trim().toLowerCase();
    const subscriptionError = String(params.get('error') || '').trim();
    const requestedSubscriptionPlan = String(params.get('plan') || '').trim().toLowerCase();
    const [planCheckoutLoadingId, setPlanCheckoutLoadingId] = useState('');
    const [subscriptionActionError, setSubscriptionActionError] = useState('');
    const normalizedPlan = String(plan || '').trim().toLowerCase();
    const usesManualPayPalSubscription = billingActive && nextRenewalSource === 'paypal_manual';
    const currentPlan = useMemo(() => {
        const activePlanId = normalizedPlan || requestedSubscriptionPlan;
        const match = planMeta.find((item) => item.id === activePlanId);
        if ((billingActive || subscriptionResult === 'success') && match?.title) return `${match.title} active`;
        if (billingActive || subscriptionResult === 'success') return 'Active plan detected';
        return 'No recurring plan configured';
    }, [billingActive, normalizedPlan, requestedSubscriptionPlan, subscriptionResult]);
    const formatPlanPrice = useCallback((planId: string, fallbackPrice: string) => {
        const raw = Number((publicPlanPrices as Record<string, number>)[planId]);
        if (!Number.isFinite(raw) || raw <= 0) return fallbackPrice;
        return `$${raw.toFixed(raw % 1 === 0 ? 0 : 2)}/mo`;
    }, [publicPlanPrices]);
    const plans = useMemo<PlanCard[]>(() => {
        return planMeta.map((plan) => ({
            id: plan.id,
            title: plan.title,
            price: formatPlanPrice(plan.id, plan.fallbackPrice),
            subtitle: plan.subtitle,
            credits: plan.credits,
            features: plan.features,
            badge: plan.badge,
        }));
    }, [formatPlanPrice]);
    const activePlanLabel = useMemo(() => {
        const match = planMeta.find((item) => item.id === (normalizedPlan || requestedSubscriptionPlan));
        return match?.title || 'Your';
    }, [normalizedPlan, requestedSubscriptionPlan]);

    const handleBackToDashboard = () => {
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

    const startPlanCheckout = useCallback(async (planId: string) => {
        if (!session) {
            onNavigate('auth');
            return;
        }
        setSubscriptionActionError('');
        setPlanCheckoutLoadingId(planId);
        try {
            if (usesManualPayPalSubscription) {
                const err = await checkout(planId);
                if (err) setSubscriptionActionError(err);
                return;
            }
            if (billingActive) {
                const err = await manageBilling();
                if (err) setSubscriptionActionError(err);
                return;
            }
            const err = await checkout(planId);
            if (err) setSubscriptionActionError(err);
        } finally {
            setPlanCheckoutLoadingId('');
        }
    }, [session, onNavigate, billingActive, usesManualPayPalSubscription, manageBilling, checkout]);

    return (
        <>
            <NavBar onNavigate={onNavigate} active="subscription" />
            <div className="mx-auto max-w-7xl px-6 pt-24 pb-12">
                <div className="mb-8 flex flex-wrap items-center justify-between gap-4">
                    <div>
                        <div className="flex items-center gap-2 text-violet-300 text-sm font-semibold uppercase tracking-[0.18em]">
                            <Sparkles className="h-4 w-4" />
                            Manage Subscription
                        </div>
                        <h1 className="mt-3 text-3xl font-bold text-white">Subscription Plans</h1>
                        <p className="mt-2 text-sm text-gray-400">
                            Monthly plans unlock Chat Story, include clipper credits, and keep one-time AC packs separate for animation. PayPal monthly access renews manually for now.
                        </p>
                    </div>
                    <div className="flex flex-wrap gap-3">
                        <button
                            type="button"
                            onClick={handleBackToDashboard}
                            className="inline-flex items-center gap-2 rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:border-white/[0.14] hover:bg-white/[0.06]"
                        >
                            <ArrowLeft className="h-4 w-4" />
                            Back to Dashboard
                        </button>
                        <button
                            type="button"
                            onClick={handleOpenBilling}
                            className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500"
                        >
                            Open Billing
                        </button>
                    </div>
                </div>

                <div className="mb-6 rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                    <div className="flex flex-wrap items-center justify-between gap-4">
                        <div>
                            <p className="text-xs uppercase tracking-[0.18em] text-gray-500">Current State</p>
                            <h2 className="mt-2 text-2xl font-bold text-white">{currentPlan}</h2>
                            <p className="mt-2 text-sm text-gray-400">
                                {session ? 'Your account is ready for recurring Chat Story access, AutoClipper beta, and monthly clipper credits.' : 'Sign in before activating a recurring plan.'}
                            </p>
                        </div>
                        <div className="grid gap-3 sm:grid-cols-2">
                            <MetricCard label="Monthly Clipper Credits" value={Number(monthlyCreditsRemaining || 0)} />
                            <MetricCard label="Total AC Remaining" value={Number(creditsTotalRemaining || 0)} />
                        </div>
                    </div>
                </div>

                <div className="grid gap-5 lg:grid-cols-3">
                    {plans.map((plan) => (
                        <section key={plan.id} className={`relative rounded-3xl border p-6 ${plan.badge ? 'border-violet-500/40 bg-violet-500/[0.06]' : 'border-white/[0.06] bg-white/[0.02]'}`}>
                            {plan.badge && (
                                <div className="absolute right-5 top-5 rounded-full border border-violet-400/40 bg-violet-500/15 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-violet-200">
                                    {plan.badge}
                                </div>
                            )}
                            <p className="text-sm font-semibold text-white">{plan.title}</p>
                            <p className="mt-2 text-4xl font-bold text-white">{plan.price}</p>
                            <p className="mt-2 text-sm text-gray-400">{plan.subtitle}</p>
                            <div className="mt-5 rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3 text-sm font-semibold text-cyan-300">
                                {plan.credits}
                            </div>
                            <div className="mt-5 space-y-3">
                                {plan.features.map((feature) => (
                                    <div key={feature} className="flex items-start gap-2 text-sm text-gray-300">
                                        <BadgeCheck className="mt-0.5 h-4 w-4 shrink-0 text-emerald-300" />
                                        <span>{feature}</span>
                                    </div>
                                ))}
                            </div>
                            <button
                                type="button"
                                onClick={() => void startPlanCheckout(plan.id)}
                                className="mt-6 w-full rounded-xl bg-white/[0.06] px-4 py-3 text-sm font-semibold text-white transition hover:bg-white/[0.1]"
                            >
                                {planCheckoutLoadingId === plan.id
                                    ? 'Opening...'
                                    : usesManualPayPalSubscription
                                        ? (plan.id === normalizedPlan ? 'Extend 1 Month' : 'Switch with PayPal')
                                        : billingActive
                                            ? 'Manage Subscription'
                                            : 'Start Subscription'}
                            </button>
                        </section>
                    ))}
                </div>
                {subscriptionActionError && (
                    <p className="mt-4 text-sm text-amber-300">{subscriptionActionError}</p>
                )}
                {subscriptionResult === 'success' && (
                    <p className="mt-4 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-100">
                        {activePlanLabel} monthly access is live. PayPal renews manually for now, so click the same plan again whenever you want to extend another month.
                    </p>
                )}
                {subscriptionResult === 'manual' && (
                    <p className="mt-4 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3 text-sm text-cyan-100">
                        This subscription is on manual PayPal renewal. Choose your current plan to extend another month, or pick a different tier to switch immediately.
                    </p>
                )}
                {subscriptionResult === 'cancelled' && (
                    <p className="mt-4 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                        Monthly checkout was cancelled.{subscriptionError ? ` ${subscriptionError}` : ''}
                    </p>
                )}
            </div>
        </>
    );
}

function MetricCard({ label, value }: { label: string; value: number }) {
    return (
        <div className="rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3">
            <p className="text-[10px] uppercase tracking-[0.18em] text-gray-500">{label}</p>
            <p className="mt-2 text-2xl font-bold text-white">{value}</p>
        </div>
    );
}
