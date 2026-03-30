import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { CheckCircle2, CreditCard, Crown, MessageCircleMore, User, WalletCards, X, Zap } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, hasChatStoryTemplateAccess, isBillingHost } from '../shared';

export default function AccountPage({ onNavigate }: { onNavigate: PageNav }) {
    const {
        session, plan, role, signOut,
        checkoutTopup, topupPacks, topupCreditsRemaining, creditsTotalRemaining, requiresTopup,
        monthlyCreditsRemaining, publicPlanLimits, billingActive, nextRenewalUnix, nextRenewalSource,
    } = useContext(AuthContext);
    const isAdmin = role === 'admin';
    const billingParams = useMemo(() => {
        if (typeof window === 'undefined') return new URLSearchParams();
        return new URLSearchParams(window.location.search);
    }, []);
    const checkoutView = isBillingHost && billingParams.get('view') === 'checkout';
    const requestedPackId = checkoutView ? String(billingParams.get('pack') || '').trim() : '';
    const topupResult = String(billingParams.get('topup') || '').trim().toLowerCase();
    const [selectedPackId, setSelectedPackId] = useState<string>('');
    const [paymentModalOpen, setPaymentModalOpen] = useState(false);
    const [checkoutLoadingMethod, setCheckoutLoadingMethod] = useState<'card' | 'paypal' | null>(null);
    const [checkoutError, setCheckoutError] = useState('');

    useEffect(() => {
        if (!session) onNavigate('auth');
    }, [session, onNavigate]);

    const sortedPacks = useMemo(() => {
        return [...topupPacks].sort((a, b) => a.credits - b.credits);
    }, [topupPacks]);
    const planLimits = publicPlanLimits[plan] || {};
    const maxResolution = String(planLimits.max_resolution || '720p');
    const cloneEnabled = Boolean(planLimits.can_clone || plan === 'creator' || plan === 'pro' || plan === 'elite');
    const priorityQueue = Boolean(planLimits.priority || plan === 'creator' || plan === 'pro' || plan === 'elite');
    const nextRenewalLabel = useMemo(() => {
        if (!nextRenewalUnix || nextRenewalUnix <= 0) return 'No automatic renewal';
        return new Date(nextRenewalUnix * 1000).toLocaleString();
    }, [nextRenewalUnix]);
    const hasLegacyRecurring = !isAdmin && (
        Boolean(billingActive)
        || Number(nextRenewalUnix || 0) > 0
        || Number(monthlyCreditsRemaining || 0) > 0
    );
    const chatStoryUnlocked = hasChatStoryTemplateAccess(plan, billingActive, role);

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
    }, [sortedPacks, selectedPackId, requestedPackId]);

    const selectedPack = useMemo(
        () => sortedPacks.find((pack) => pack.price_id === selectedPackId) || null,
        [sortedPacks, selectedPackId],
    );

    const routeToBillingPack = useCallback((priceId: string) => {
        const params = new URLSearchParams({ view: 'checkout', pack: priceId });
        window.location.href = `${BILLING_SITE_URL}?${params.toString()}`;
    }, []);

    const handleOpenCheckout = useCallback((priceId: string) => {
        if (!session) {
            onNavigate('auth');
            return;
        }
        if (!isBillingHost || !checkoutView) {
            routeToBillingPack(priceId);
            return;
        }
        setSelectedPackId(priceId);
        setCheckoutError('');
        setPaymentModalOpen(true);
    }, [session, onNavigate, routeToBillingPack, checkoutView]);

    const handleConfirmMethod = useCallback(async (preferredMethod: 'card' | 'paypal') => {
        if (!selectedPack) {
            setCheckoutError('Select a package first.');
            return;
        }
        setCheckoutError('');
        setCheckoutLoadingMethod(preferredMethod);
        const err = await checkoutTopup(selectedPack.price_id, preferredMethod);
        if (err) {
            setCheckoutLoadingMethod(null);
            setCheckoutError(err);
            return;
        }
    }, [checkoutTopup, selectedPack]);

    const handleOpenBillingPage = useCallback(() => {
        window.location.href = `${BILLING_SITE_URL}?view=checkout`;
    }, []);
    const handleOpenSubscriptionPage = useCallback(() => {
        window.location.href = `${BILLING_SITE_URL}?page=subscription`;
    }, []);

    if (!session) return null;

    const billingModelLabel = hasLegacyRecurring || chatStoryUnlocked
        ? 'Monthly + usage-based'
        : 'Usage-based only';

    return (
        <>
            <NavBar onNavigate={onNavigate} active="account" />
            <div className="pt-24 max-w-6xl mx-auto px-6 pb-10">
                <div className="flex items-center justify-between gap-4 mb-8">
                    <div>
                        <h1 className="text-3xl font-bold">{checkoutView ? 'Billing Checkout' : 'Your Account'}</h1>
                        <p className="text-sm text-gray-400 mt-1">
                            {checkoutView
                                ? 'Select your AC credit package, then continue with PayPal. Stripe is coming soon.'
                                : isBillingHost
                                ? 'Billing surface for one-time AC credit purchases.'
                                : 'Account, access, and AC credit management.'}
                        </p>
                    </div>
                </div>

                <div className="space-y-6">
                    {topupResult === 'success' && (
                        <div className="rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-5 py-4">
                            <p className="text-sm font-semibold text-emerald-300">Payment received.</p>
                            <p className="text-sm text-emerald-100/80 mt-1">Your AC credits are updating on this billing surface now.</p>
                        </div>
                    )}
                    {topupResult === 'cancelled' && (
                        <div className="rounded-2xl border border-amber-500/20 bg-amber-500/10 px-5 py-4">
                            <p className="text-sm font-semibold text-amber-300">Checkout canceled.</p>
                            <p className="text-sm text-amber-100/80 mt-1">No charge was made. You can pick a package and try again.</p>
                        </div>
                    )}

                    <div className="p-6 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                        <div className="flex items-center gap-4 mb-4">
                            <div className="w-14 h-14 rounded-full bg-violet-500/10 flex items-center justify-center">
                                <User className="w-7 h-7 text-violet-400" />
                            </div>
                            <div>
                                <p className="font-bold text-lg">{session.user.email}</p>
                                {isAdmin ? (
                                    <div className="flex items-center gap-2 mt-0.5">
                                        <Crown className="w-4 h-4 text-emerald-400" />
                                        <span className="text-sm font-medium text-emerald-400">Owner Admin Access</span>
                                    </div>
                                ) : (
                                    <div className="flex items-center gap-2 mt-0.5">
                                        <Zap className="w-4 h-4 text-cyan-300" />
                                        <span className="text-sm font-medium text-cyan-300">Usage-Based Access</span>
                                    </div>
                                )}
                            </div>
                        </div>
                        <p className="text-sm text-gray-400">
                            Images and slideshows stay free. Only animated renders consume purchased AC credits.
                        </p>
                    </div>

                    <div className="p-6 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                        <div className="flex items-center justify-between gap-4 mb-4">
                            <div>
                                <h3 className="text-lg font-semibold text-white">AC Credits</h3>
                                <p className="text-sm text-gray-400 mt-1">
                                    Animation credits are the only paid part of Studio.
                                </p>
                            </div>
                            {checkoutView && (
                                <span className="rounded-full border border-cyan-500/20 bg-cyan-500/10 px-3 py-1 text-xs font-semibold text-cyan-300">
                                    Select your package
                                </span>
                            )}
                        </div>
                        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                            <div className="rounded-xl bg-black/30 border border-white/[0.06] p-3">
                                <p className="text-xs text-gray-500">Purchased AC Credits</p>
                                <p className="text-lg font-bold text-cyan-300">{Number(topupCreditsRemaining || 0)}</p>
                            </div>
                            <div className="rounded-xl bg-black/30 border border-white/[0.06] p-3">
                                <p className="text-xs text-gray-500">Clipper Credits</p>
                                <p className="text-lg font-bold text-white">{Number(monthlyCreditsRemaining || 0)}</p>
                            </div>
                            <div className="rounded-xl bg-black/30 border border-white/[0.06] p-3">
                                <p className="text-xs text-gray-500">Total AC Credits</p>
                                <p className="text-lg font-bold text-emerald-300">{Number(creditsTotalRemaining || 0)}</p>
                            </div>
                            <div className="rounded-xl bg-black/30 border border-white/[0.06] p-3 sm:col-span-3">
                                <p className="text-xs text-gray-500">Animation Status</p>
                                <p className={`text-sm font-semibold mt-1 ${requiresTopup ? 'text-amber-300' : 'text-emerald-300'}`}>
                                    {isAdmin ? 'Owner override active' : (requiresTopup ? 'Top up before your next animation run' : 'Ready to animate')}
                                </p>
                            </div>
                        </div>
                    </div>

                    {checkoutView ? (
                        <div id="billing-credit-packs" className="p-6 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                            <div className="grid gap-6 lg:grid-cols-[1.2fr,0.8fr]">
                                <div>
                                    <h3 className="text-lg font-semibold text-white">Select your package</h3>
                                    <p className="text-sm text-gray-400 mt-1">
                                        Pick one of the 10 one-time AC packs below. PayPal is live now. Stripe is coming soon.
                                    </p>
                                    <div className="mt-5 space-y-3">
                                        {sortedPacks.length === 0 ? (
                                            <p className="text-sm text-amber-300">No credit packs are configured yet.</p>
                                        ) : sortedPacks.map((pack) => {
                                            const active = selectedPack?.price_id === pack.price_id;
                                            return (
                                                <button
                                                    key={pack.price_id}
                                                    type="button"
                                                    onClick={() => {
                                                        setSelectedPackId(pack.price_id);
                                                        setCheckoutError('');
                                                    }}
                                                    className={`w-full rounded-2xl border p-4 text-left transition ${
                                                        active
                                                            ? 'border-violet-500 bg-violet-500/10 shadow-lg shadow-violet-600/10'
                                                            : 'border-white/[0.08] bg-black/20 hover:border-violet-500/30'
                                                    }`}
                                                >
                                                    <div className="flex items-center justify-between gap-3">
                                                        <div>
                                                            <p className="text-sm font-semibold text-white">{String(pack.pack || '').toUpperCase()} Pack</p>
                                                            <p className="text-xs text-gray-500 mt-1">{pack.credits} AC credits</p>
                                                        </div>
                                                        <div className="text-right">
                                                            <p className="text-lg font-bold text-white">${Number(pack.price_usd || 0).toFixed(2)}</p>
                                                            <p className="text-[11px] text-gray-500">one-time</p>
                                                        </div>
                                                    </div>
                                                </button>
                                            );
                                        })}
                                    </div>
                                </div>
                                <div className="rounded-2xl border border-white/[0.08] bg-black/25 p-5 lg:sticky lg:top-24">
                                    <div className="flex items-center gap-2 text-cyan-300 text-sm font-semibold">
                                        <WalletCards className="w-4 h-4" />
                                        Checkout Summary
                                    </div>
                                    {selectedPack ? (
                                        <>
                                            <div className="mt-5 rounded-xl border border-white/[0.08] bg-white/[0.02] p-4">
                                                <p className="text-xs uppercase tracking-[0.18em] text-gray-500">Selected Package</p>
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
                                                onClick={() => handleOpenCheckout(selectedPack.price_id)}
                                                className="mt-5 w-full rounded-xl bg-violet-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-violet-500"
                                            >
                                                Buy Now
                                            </button>
                                            <p className="mt-3 text-xs text-gray-500">
                                                Clicking buy now opens the payment window. PayPal is live now and Stripe is marked coming soon.
                                            </p>
                                            {checkoutError && (
                                                <p className="mt-3 text-sm text-amber-300">{checkoutError}</p>
                                            )}
                                        </>
                                    ) : (
                                        <p className="mt-4 text-sm text-gray-400">Choose a package to continue.</p>
                                    )}
                                </div>
                            </div>
                        </div>
                    ) : (
                        <div id="billing-credit-packs" className="p-6 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                            <div className="flex items-center justify-between gap-4 mb-5">
                                <div>
                                    <h3 className="text-lg font-semibold text-white">One-Time AC Packs</h3>
                                    <p className="text-sm text-gray-400 mt-1">
                                        Choose any package below. Payment method is picked on the billing domain.
                                    </p>
                                </div>
                                <button
                                    type="button"
                                    onClick={handleOpenBillingPage}
                                    className="rounded-lg border border-white/[0.1] bg-white/[0.03] px-4 py-2 text-sm font-medium text-white transition hover:bg-white/[0.06]"
                                >
                                    Open Billing Page
                                </button>
                            </div>
                            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
                                {sortedPacks.map((pack) => (
                                    <div key={pack.price_id} className="rounded-2xl border border-white/[0.08] bg-black/20 p-4">
                                        <p className="text-sm font-semibold text-white">{String(pack.pack || '').toUpperCase()} Pack</p>
                                        <p className="mt-1 text-xs text-gray-500">{pack.credits} AC credits</p>
                                        <p className="mt-4 text-2xl font-bold text-white">${Number(pack.price_usd || 0).toFixed(2)}</p>
                                        <button
                                            type="button"
                                            onClick={() => handleOpenCheckout(pack.price_id)}
                                            className="mt-5 w-full rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500"
                                        >
                                            Buy Now
                                        </button>
                                    </div>
                                ))}
                            </div>
                            {isAdmin && (
                                <p className="mt-4 text-xs text-emerald-300">
                                    Owner account: this page previews the same public pack flow while your account itself does not require top-ups.
                                </p>
                            )}
                        </div>
                    )}

                    <div className="p-6 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                        <h3 className="text-lg font-semibold text-white mb-4">Studio Access</h3>
                        <div className="grid grid-cols-2 gap-4 text-sm">
                            <div>
                                <p className="text-gray-500">Free image/slideshow access</p>
                                <p className="font-bold text-emerald-300">Enabled</p>
                            </div>
                            <div>
                                <p className="text-gray-500">Billing model</p>
                                <p className="font-bold text-emerald-300">{billingModelLabel}</p>
                            </div>
                            <div>
                                <p className="text-gray-500">Max resolution</p>
                                <p className="font-bold text-white">{maxResolution}</p>
                            </div>
                            <div>
                                <p className="text-gray-500">Priority queue</p>
                                <p className={`font-bold ${priorityQueue ? 'text-emerald-300' : 'text-gray-500'}`}>
                                    {priorityQueue ? 'Yes' : 'No'}
                                </p>
                            </div>
                            <div>
                                <p className="text-gray-500">Clone feature</p>
                                <p className={`font-bold ${cloneEnabled ? 'text-emerald-300' : 'text-gray-500'}`}>
                                    {cloneEnabled ? 'Enabled' : 'Locked'}
                                </p>
                            </div>
                            <div>
                                <p className="text-gray-500">Renewal status</p>
                                <p className={`font-bold ${hasLegacyRecurring ? 'text-amber-300' : 'text-emerald-300'}`}>
                                    {hasLegacyRecurring ? 'Legacy billing metadata still attached' : 'No automatic renewal'}
                                </p>
                                {nextRenewalSource && (
                                    <p className="text-[11px] text-gray-500 mt-1">source: {nextRenewalSource || nextRenewalLabel}</p>
                                )}
                            </div>
                        </div>
                    </div>

                    <div className="p-6 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                        <div className="flex flex-wrap items-center justify-between gap-4">
                            <div>
                                <div className="flex items-center gap-2 text-cyan-300">
                                    <MessageCircleMore className="w-4 h-4" />
                                    <span className="text-sm font-semibold uppercase tracking-[0.18em]">Premium Alpha</span>
                                </div>
                                <h3 className="mt-2 text-lg font-semibold text-white">Chat Story</h3>
                                <p className="mt-1 text-sm text-gray-400">
                                    Text-message style shorts with a live phone preview, theme presets, background-video picks, and audio controls.
                                </p>
                            </div>
                            <div className={`rounded-xl border px-4 py-2 text-sm font-semibold ${chatStoryUnlocked ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300' : 'border-amber-500/20 bg-amber-500/10 text-amber-300'}`}>
                                {chatStoryUnlocked ? 'Unlocked on this account' : 'Locked behind premium access'}
                            </div>
                        </div>
                        <div className="mt-5 flex flex-wrap gap-3">
                            <button
                                type="button"
                                onClick={() => onNavigate('dashboard')}
                                className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500"
                            >
                                Open Dashboard
                            </button>
                            {!chatStoryUnlocked && (
                                <button
                                    type="button"
                                    onClick={handleOpenSubscriptionPage}
                                    className="rounded-xl border border-white/[0.1] bg-white/[0.03] px-4 py-2.5 text-sm font-medium text-white transition hover:bg-white/[0.06]"
                                >
                                    Open Subscription Plans
                                </button>
                            )}
                        </div>
                    </div>

                    <button
                        onClick={signOut}
                        className="w-full py-3 bg-red-500/10 hover:bg-red-500/20 text-red-400 font-medium rounded-xl transition border border-red-500/20"
                    >
                        Sign Out
                    </button>
                </div>
            </div>

            {paymentModalOpen && selectedPack && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4">
                    <div className="w-full max-w-lg rounded-2xl border border-white/[0.08] bg-[#0d0d11] shadow-2xl">
                        <div className="flex items-center justify-between border-b border-white/[0.08] px-5 py-4">
                            <div>
                                <h3 className="text-lg font-semibold text-white">Choose Payment Method</h3>
                                <p className="text-sm text-gray-400 mt-1">
                                    {selectedPack.credits} AC credits for ${Number(selectedPack.price_usd || 0).toFixed(2)}
                                </p>
                            </div>
                            <button
                                type="button"
                                onClick={() => {
                                    if (checkoutLoadingMethod) return;
                                    setPaymentModalOpen(false);
                                    setCheckoutError('');
                                }}
                                className="rounded-lg p-2 text-gray-400 transition hover:bg-white/[0.05] hover:text-white"
                                title="Close"
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
                                onClick={() => void handleConfirmMethod('paypal')}
                                disabled={Boolean(checkoutLoadingMethod)}
                                className="w-full rounded-xl bg-[#0070ba] px-4 py-3 text-sm font-semibold text-white transition hover:bg-[#04619f] disabled:opacity-60"
                            >
                                {checkoutLoadingMethod === 'paypal' ? 'Opening PayPal...' : 'Pay with PayPal'}
                            </button>
                            {checkoutError && (
                                <p className="text-sm text-amber-300">{checkoutError}</p>
                            )}
                            <p className="text-xs text-gray-500">
                                PayPal is the active checkout path right now so credit purchases clear faster.
                            </p>
                            <div className="flex items-center gap-2 text-xs text-gray-500">
                                <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                                NYPTID Studio keeps images free. Only animation uses AC credits.
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </>
    );
}
