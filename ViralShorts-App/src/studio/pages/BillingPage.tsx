import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { ArrowLeft, CheckCircle2, CreditCard, WalletCards, X } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, STUDIO_SITE_URL, isBillingHost } from '../shared';

const subscriptionPlanMeta = [
    {
        id: 'starter',
        title: 'Starter',
        fallbackPrice: '$14/mo',
        clipperCredits: 'Starter recurring access',
        desc: 'Best for solo creators who want Chat Story plus starter AutoClipper access.',
        features: [
            'Chat Story access',
            'AutoClipper beta access',
            'Recurring monthly access',
            'Keep buying one-time AC packs separately',
            'Standard queue priority',
        ],
    },
    {
        id: 'creator',
        title: 'Creator',
        fallbackPrice: '$24/mo',
        clipperCredits: 'Balanced recurring access',
        desc: 'For active creators who need more monthly headroom for Chat Story and clipping.',
        features: [
            'Chat Story access',
            'AutoClipper beta access',
            'More monthly headroom',
            'Priority queue handling',
            'Built to pair with AC animation packs',
        ],
        badge: 'Popular',
    },
    {
        id: 'pro',
        title: 'Pro',
        fallbackPrice: '$39/mo',
        clipperCredits: 'Highest public monthly tier',
        desc: 'For operators and teams running Chat Story plus repeat clipping on a regular schedule.',
        features: [
            'Chat Story access',
            'AutoClipper beta access',
            'Highest public monthly headroom',
            'Top queue priority',
            'Best fit for repeat operator volume',
        ],
    },
];

export default function BillingPage({ onNavigate }: { onNavigate: PageNav }) {
    const {
        session,
        plan,
        billingActive,
        nextRenewalSource,
        checkout,
        checkoutTopup,
        manageBilling,
        topupPacks,
        topupCreditsRemaining,
        monthlyCreditsRemaining,
        creditsTotalRemaining,
        publicPlanPrices,
        requiresTopup,
    } = useContext(AuthContext);
    const params = useMemo(() => {
        if (typeof window === 'undefined') return new URLSearchParams();
        return new URLSearchParams(window.location.search);
    }, []);
    const requestedPackId = String(params.get('pack') || '').trim();
    const topupResult = String(params.get('topup') || '').trim().toLowerCase();
    const subscriptionResult = String(params.get('subscription') || '').trim().toLowerCase();
    const subscriptionError = String(params.get('error') || '').trim();
    const requestedSubscriptionPlan = String(params.get('plan') || '').trim().toLowerCase();
    const [selectedPackId, setSelectedPackId] = useState('');
    const [paymentModalOpen, setPaymentModalOpen] = useState(false);
    const [checkoutLoadingMethod, setCheckoutLoadingMethod] = useState<'paypal' | null>(null);
    const [checkoutError, setCheckoutError] = useState('');
    const [planCheckoutLoadingId, setPlanCheckoutLoadingId] = useState('');
    const [subscriptionActionError, setSubscriptionActionError] = useState('');

    const sortedPacks = useMemo(() => [...topupPacks].sort((a, b) => a.credits - b.credits), [topupPacks]);
    const formatPlanPrice = useCallback((planId: string, fallbackPrice: string) => {
        const raw = Number((publicPlanPrices as Record<string, number>)[planId]);
        if (!Number.isFinite(raw) || raw <= 0) return fallbackPrice;
        return `$${raw.toFixed(raw % 1 === 0 ? 0 : 2)}/mo`;
    }, [publicPlanPrices]);
    const subscriptionPlans = useMemo(() => {
        return subscriptionPlanMeta.map((plan) => ({
            ...plan,
            price: formatPlanPrice(plan.id, plan.fallbackPrice),
        }));
    }, [formatPlanPrice]);
    const normalizedPlan = String(plan || '').trim().toLowerCase();
    const usesManualPayPalSubscription = billingActive && nextRenewalSource === 'paypal_manual';
    const activePlanLabel = useMemo(() => {
        const match = subscriptionPlanMeta.find((item) => item.id === (normalizedPlan || requestedSubscriptionPlan));
        return match?.title || 'Your';
    }, [normalizedPlan, requestedSubscriptionPlan]);

    useEffect(() => {
        if (!sortedPacks.length) return;
        const requestedExists = requestedPackId && sortedPacks.some((pack) => pack.price_id === requestedPackId);
        if (requestedExists) {
            setSelectedPackId(requestedPackId);
            return;
        }
        if (!selectedPackId || !sortedPacks.some((pack) => pack.price_id === selectedPackId)) {
            setSelectedPackId(sortedPacks[0].price_id);
        }
    }, [sortedPacks, requestedPackId, selectedPackId]);

    const selectedPack = useMemo(
        () => sortedPacks.find((pack) => pack.price_id === selectedPackId) || null,
        [sortedPacks, selectedPackId],
    );

    const openCheckout = useCallback((priceId: string) => {
        if (!session) {
            onNavigate('auth');
            return;
        }
        setSelectedPackId(priceId);
        setCheckoutError('');
        setPaymentModalOpen(true);
    }, [session, onNavigate]);

    const handleConfirmPayPal = useCallback(async () => {
        if (!selectedPack) {
            setCheckoutError('Select a package first.');
            return;
        }
        setCheckoutLoadingMethod('paypal');
        setCheckoutError('');
        const err = await checkoutTopup(selectedPack.price_id, 'paypal');
        if (err) {
            setCheckoutLoadingMethod(null);
            setCheckoutError(err);
        }
    }, [checkoutTopup, selectedPack]);

    const handleBackToDashboard = () => {
        if (isBillingHost) {
            window.location.href = STUDIO_SITE_URL;
            return;
        }
        onNavigate('dashboard');
    };

    const handleOpenSubscriptions = () => {
        if (isBillingHost) {
            window.location.href = `${BILLING_SITE_URL}?page=subscription`;
            return;
        }
        onNavigate('subscription');
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
            <NavBar onNavigate={onNavigate} active="billing" />
            <div className="mx-auto max-w-7xl px-6 pt-24 pb-12">
                <div className="mb-8 flex flex-wrap items-center justify-between gap-4">
                    <div>
                        <div className="flex items-center gap-2 text-cyan-300 text-sm font-semibold uppercase tracking-[0.18em]">
                            <WalletCards className="h-4 w-4" />
                            Billing
                        </div>
                        <h1 className="mt-3 text-3xl font-bold text-white">Pricing & Billing</h1>
                        <p className="mt-2 text-sm text-gray-400">
                            Images stay free. Buy one-time AC packs for animation, or use a monthly plan to unlock Chat Story plus AutoClipper beta. PayPal monthly access renews manually for now.
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
                            onClick={handleOpenSubscriptions}
                            className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500"
                        >
                            Manage Subscription
                        </button>
                    </div>
                </div>

                <div className="grid gap-6 xl:grid-cols-[1.15fr,0.85fr]">
                    <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                        <div className="mb-5 flex flex-wrap items-center justify-between gap-4">
                            <div>
                                <h2 className="text-lg font-semibold text-white">Select your AC package</h2>
                                <p className="mt-1 text-sm text-gray-400">Ten one-time top-up packs, from trial buys to larger operator packs.</p>
                            </div>
                            {!session && (
                                <button
                                    type="button"
                                    onClick={() => onNavigate('auth')}
                                    className="rounded-xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-2.5 text-sm font-semibold text-cyan-200 transition hover:bg-cyan-500/15"
                                >
                                    Sign in to checkout
                                </button>
                            )}
                        </div>
                        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                            {sortedPacks.map((pack) => {
                                const active = pack.price_id === selectedPackId;
                                return (
                                    <button
                                        key={pack.price_id}
                                        type="button"
                                        onClick={() => setSelectedPackId(pack.price_id)}
                                        className={`rounded-2xl border p-4 text-left transition ${
                                            active
                                                ? 'border-violet-500 bg-violet-500/10'
                                                : 'border-white/[0.08] bg-black/20 hover:border-violet-500/30'
                                        }`}
                                    >
                                        <p className="text-sm font-semibold text-white">{String(pack.pack || '').toUpperCase()} Pack</p>
                                        <p className="mt-1 text-xs text-gray-500">{pack.credits} AC credits</p>
                                        <p className="mt-5 text-2xl font-bold text-white">${Number(pack.price_usd || 0).toFixed(2)}</p>
                                        <p className="mt-1 text-[11px] text-gray-500">one-time</p>
                                    </button>
                                );
                            })}
                        </div>
                    </section>

                    <aside className="space-y-6">
                        <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                            <h2 className="text-lg font-semibold text-white">Checkout Summary</h2>
                            {selectedPack ? (
                                <>
                                    <div className="mt-5 rounded-2xl border border-white/[0.08] bg-black/20 p-4">
                                        <p className="text-xs uppercase tracking-[0.18em] text-gray-500">Package</p>
                                        <p className="mt-2 text-lg font-semibold text-white">{String(selectedPack.pack || '').toUpperCase()} Pack</p>
                                        <div className="mt-4 flex items-center justify-between text-sm">
                                            <span className="text-gray-400">Credits</span>
                                            <span className="font-semibold text-cyan-300">{selectedPack.credits} AC</span>
                                        </div>
                                        <div className="mt-2 flex items-center justify-between text-sm">
                                            <span className="text-gray-400">Price</span>
                                            <span className="font-semibold text-white">${Number(selectedPack.price_usd || 0).toFixed(2)}</span>
                                        </div>
                                    </div>
                                    <button
                                        type="button"
                                        onClick={() => openCheckout(selectedPack.price_id)}
                                        className="mt-5 w-full rounded-xl bg-violet-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-violet-500"
                                    >
                                        Buy Now
                                    </button>
                                    <p className="mt-3 text-xs text-gray-500">PayPal is the active checkout path. Stripe remains coming soon here.</p>
                                </>
                            ) : (
                                <p className="mt-4 text-sm text-gray-400">Select a package to continue.</p>
                            )}
                        </section>

                        <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                            <h2 className="text-lg font-semibold text-white">Current Balance</h2>
                            <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-3 xl:grid-cols-1">
                                <div className="rounded-2xl border border-cyan-500/20 bg-cyan-500/10 p-4">
                                    <p className="text-xs uppercase tracking-[0.18em] text-cyan-200/70">Purchased AC</p>
                                    <p className="mt-2 text-2xl font-bold text-cyan-100">{Number(topupCreditsRemaining || 0)}</p>
                                </div>
                                <div className="rounded-2xl border border-violet-500/20 bg-violet-500/10 p-4">
                                    <p className="text-xs uppercase tracking-[0.18em] text-violet-200/70">Subscription Credits</p>
                                    <p className="mt-2 text-2xl font-bold text-violet-100">{Number(monthlyCreditsRemaining || 0)}</p>
                                </div>
                                <div className="rounded-2xl border border-emerald-500/20 bg-emerald-500/10 p-4">
                                    <p className="text-xs uppercase tracking-[0.18em] text-emerald-200/70">Total AC</p>
                                    <p className="mt-2 text-2xl font-bold text-emerald-100">{Number(creditsTotalRemaining || 0)}</p>
                                    <p className={`mt-2 text-xs font-medium ${requiresTopup ? 'text-amber-300' : 'text-emerald-200/80'}`}>
                                        {requiresTopup ? 'Top up before the next animation render.' : 'Ready for animation renders.'}
                                    </p>
                                </div>
                            </div>
                        </section>
                    </aside>
                </div>

                <section className="mt-8 rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                    <div className="mb-5 flex flex-wrap items-center justify-between gap-4">
                        <div>
                            <h2 className="text-lg font-semibold text-white">Monthly Plans</h2>
                            <p className="mt-1 text-sm text-gray-400">
                                Every monthly plan unlocks Chat Story, includes clipper credits, and keeps animation on separate one-time AC packs.
                            </p>
                        </div>
                        <button
                            type="button"
                            onClick={handleOpenSubscriptions}
                            className="rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-2.5 text-sm font-medium text-white transition hover:border-white/[0.14] hover:bg-white/[0.06]"
                        >
                            Compare Plans
                        </button>
                    </div>
                    <div className="grid gap-4 lg:grid-cols-3">
                        {subscriptionPlans.map((plan) => (
                            <div key={plan.id} className={`rounded-2xl border p-5 ${plan.badge ? 'border-violet-500/40 bg-violet-500/[0.06]' : 'border-white/[0.08] bg-black/20'}`}>
                                <div className="flex items-start justify-between gap-3">
                                    <div>
                                        <p className="text-sm font-semibold text-white">{plan.title}</p>
                                        <p className="mt-2 text-3xl font-bold text-white">{plan.price}</p>
                                        <p className="mt-2 text-sm text-gray-400">{plan.desc}</p>
                                    </div>
                                    {plan.badge && (
                                        <span className="rounded-full border border-violet-400/40 bg-violet-500/15 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-violet-200">
                                            {plan.badge}
                                        </span>
                                    )}
                                </div>
                                <div className="mt-4 rounded-xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3 text-sm font-semibold text-cyan-200">
                                    {plan.clipperCredits}
                                </div>
                                <ul className="mt-4 space-y-2 text-sm text-gray-300">
                                    {plan.features.map((feature) => (
                                        <li key={feature} className="flex items-start gap-2">
                                            <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-emerald-300" />
                                            <span>{feature}</span>
                                        </li>
                                    ))}
                                </ul>
                                <button
                                    type="button"
                                    onClick={() => void startPlanCheckout(plan.id)}
                                    className="mt-5 w-full rounded-xl bg-white/[0.06] px-4 py-3 text-sm font-semibold text-white transition hover:bg-white/[0.1]"
                                >
                                    {planCheckoutLoadingId === plan.id
                                        ? 'Opening...'
                                        : usesManualPayPalSubscription
                                            ? (plan.id === normalizedPlan ? 'Extend 1 Month' : 'Switch with PayPal')
                                            : billingActive
                                                ? 'Manage Subscription'
                                                : 'Start Subscription'}
                                </button>
                            </div>
                        ))}
                    </div>
                    {subscriptionActionError && (
                        <p className="mt-4 text-sm text-amber-300">{subscriptionActionError}</p>
                    )}
                    {subscriptionResult === 'success' && (
                        <p className="mt-4 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-100">
                            {activePlanLabel} monthly access is live. PayPal renews manually for now, so use the same plan button again whenever you want to extend another month.
                        </p>
                    )}
                    {subscriptionResult === 'manual' && (
                        <p className="mt-4 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3 text-sm text-cyan-100">
                            This subscription is on manual PayPal renewal. Click your current plan to extend another month, or choose a different tier to switch immediately.
                        </p>
                    )}
                    {subscriptionResult === 'cancelled' && (
                        <p className="mt-4 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                            Monthly checkout was cancelled.{subscriptionError ? ` ${subscriptionError}` : ''}
                        </p>
                    )}
                </section>

                {topupResult === 'success' && (
                    <div className="mt-6 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-5 py-4 text-sm text-emerald-100">
                        Payment received. Your AC credits are updating on this billing surface now.
                    </div>
                )}

                {paymentModalOpen && selectedPack && (
                    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4">
                        <div className="w-full max-w-lg rounded-2xl border border-white/[0.08] bg-[#0d0d11] shadow-2xl">
                            <div className="flex items-center justify-between border-b border-white/[0.08] px-5 py-4">
                                <div>
                                    <h3 className="text-lg font-semibold text-white">Choose Payment Method</h3>
                                    <p className="mt-1 text-sm text-gray-400">{selectedPack.credits} AC credits for ${Number(selectedPack.price_usd || 0).toFixed(2)}</p>
                                </div>
                                <button
                                    type="button"
                                    onClick={() => {
                                        if (checkoutLoadingMethod) return;
                                        setPaymentModalOpen(false);
                                        setCheckoutError('');
                                    }}
                                    className="rounded-lg p-2 text-gray-400 transition hover:bg-white/[0.05] hover:text-white"
                                >
                                    <X className="h-4 w-4" />
                                </button>
                            </div>
                            <div className="p-5 space-y-4">
                                <div className="rounded-xl border border-white/[0.08] bg-black/20 p-4">
                                    <p className="text-xs uppercase tracking-[0.18em] text-gray-500">Package</p>
                                    <p className="mt-2 text-base font-semibold text-white">{String(selectedPack.pack || '').toUpperCase()} Pack</p>
                                    <div className="mt-3 flex items-center justify-between text-sm">
                                        <span className="text-gray-400">Credits</span>
                                        <span className="font-semibold text-cyan-300">{selectedPack.credits} AC</span>
                                    </div>
                                    <div className="mt-2 flex items-center justify-between text-sm">
                                        <span className="text-gray-400">Price</span>
                                        <span className="font-semibold text-white">${Number(selectedPack.price_usd || 0).toFixed(2)}</span>
                                    </div>
                                </div>

                                <button
                                    type="button"
                                    disabled
                                    className="flex w-full items-center justify-center gap-2 rounded-xl border border-violet-500/30 bg-violet-500/10 px-4 py-3 text-sm font-semibold text-violet-200/80 opacity-70"
                                >
                                    <CreditCard className="w-4 h-4" />
                                    Stripe Coming Soon
                                </button>
                                <button
                                    type="button"
                                    onClick={() => void handleConfirmPayPal()}
                                    disabled={Boolean(checkoutLoadingMethod)}
                                    className="w-full rounded-xl bg-[#0070ba] px-4 py-3 text-sm font-semibold text-white transition hover:bg-[#04619f] disabled:opacity-60"
                                >
                                    {checkoutLoadingMethod === 'paypal' ? 'Opening PayPal...' : 'Pay with PayPal'}
                                </button>
                                {checkoutError && <p className="text-sm text-amber-300">{checkoutError}</p>}
                                <div className="flex items-center gap-2 text-xs text-gray-500">
                                    <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                                    NYPTID Studio keeps image generation free. Only animation uses AC credits.
                                </div>
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </>
    );
}
