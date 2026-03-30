import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { ArrowLeft, CheckCircle2, Sparkles, WalletCards } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, STUDIO_SITE_URL, isBillingHost } from '../shared';

export default function BillingPage({ onNavigate }: { onNavigate: PageNav }) {
    const {
        session,
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
        defaultMembershipPlanId,
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
    const [selectedPackId, setSelectedPackId] = useState('');
    const [checkoutError, setCheckoutError] = useState('');
    const [packCheckoutLoadingId, setPackCheckoutLoadingId] = useState('');
    const [membershipLoading, setMembershipLoading] = useState(false);

    const sortedPacks = useMemo(() => [...topupPacks].sort((a, b) => a.credits - b.credits), [topupPacks]);
    const selectedPack = useMemo(
        () => sortedPacks.find((pack) => pack.price_id === selectedPackId) || null,
        [selectedPackId, sortedPacks],
    );
    const membershipPrice = useMemo(() => {
        const raw = Number((publicPlanPrices as Record<string, number>)[defaultMembershipPlanId || 'starter']);
        if (!Number.isFinite(raw) || raw <= 0) return '$14/mo';
        return `$${raw.toFixed(raw % 1 === 0 ? 0 : 2)}/mo`;
    }, [defaultMembershipPlanId, publicPlanPrices]);
    const usesManualPayPalMembership = billingActive && nextRenewalSource === 'paypal_manual';

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
    }, [requestedPackId, selectedPackId, sortedPacks]);

    const handleBack = () => {
        if (isBillingHost) {
            window.location.href = STUDIO_SITE_URL;
            return;
        }
        onNavigate('dashboard');
    };

    const handleOpenMembership = () => {
        if (isBillingHost) {
            window.location.href = `${BILLING_SITE_URL}?page=subscription`;
            return;
        }
        onNavigate('subscription');
    };

    const handleMembershipCheckout = useCallback(async () => {
        if (!session) {
            onNavigate('auth');
            return;
        }
        setCheckoutError('');
        setMembershipLoading(true);
        try {
            if (billingActive && !usesManualPayPalMembership) {
                const err = await manageBilling();
                if (err) setCheckoutError(err);
                return;
            }
            const err = await checkout('membership');
            if (err) setCheckoutError(err);
        } finally {
            setMembershipLoading(false);
        }
    }, [billingActive, checkout, manageBilling, onNavigate, session, usesManualPayPalMembership]);

    const handlePackCheckout = useCallback(async () => {
        if (!selectedPack) {
            setCheckoutError('Select a credit pack first.');
            return;
        }
        if (!session) {
            onNavigate('auth');
            return;
        }
        setCheckoutError('');
        setPackCheckoutLoadingId(selectedPack.price_id);
        try {
            const err = await checkoutTopup(selectedPack.price_id, 'paypal');
            if (err) setCheckoutError(err);
        } finally {
            setPackCheckoutLoadingId('');
        }
    }, [checkoutTopup, onNavigate, selectedPack, session]);

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
                        <h1 className="mt-3 text-3xl font-bold text-white">Catalyst Membership + Credit Wallet</h1>
                        <p className="mt-2 max-w-3xl text-sm text-gray-400">
                            NYPTID Studio now sells one faceless YouTube operating system. Membership unlocks the public Studio lanes and includes starter credits. Wallet top-ups cover heavier animation usage on the same account.
                        </p>
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
                            onClick={handleOpenMembership}
                            className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500"
                        >
                            Membership Details
                        </button>
                    </div>
                </div>

                <div className="grid gap-6 xl:grid-cols-[1.2fr,0.8fr]">
                    <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                        <div className="flex flex-wrap items-center justify-between gap-4">
                            <div>
                                <p className="text-xs uppercase tracking-[0.18em] text-violet-300">Unified Offer</p>
                                <h2 className="mt-2 text-2xl font-bold text-white">Catalyst Membership</h2>
                                <p className="mt-2 text-sm text-gray-400">
                                    Unlock Create, Thumbnails, Clone, Long Form, and Chat Story on one account. Included credits burn before the wallet, so hybrid customers do not lose their monthly headroom.
                                </p>
                            </div>
                            <div className="rounded-2xl border border-violet-500/20 bg-violet-500/10 px-5 py-4 text-right">
                                <p className="text-[10px] uppercase tracking-[0.18em] text-violet-200/70">Membership</p>
                                <p className="mt-2 text-3xl font-bold text-white">{membershipPrice}</p>
                            </div>
                        </div>

                        <div className="mt-6 grid gap-4 md:grid-cols-2">
                            {[
                                'Create, Thumbnails, Clone, Long Form, and Chat Story on one login',
                                'Starter included credits reset monthly when membership is active',
                                'Wallet top-ups stack on top for heavier renders and operator usage',
                                'PayPal is the live membership checkout path for now',
                            ].map((feature) => (
                                <div key={feature} className="flex items-start gap-2 rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3 text-sm text-gray-300">
                                    <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-emerald-300" />
                                    <span>{feature}</span>
                                </div>
                            ))}
                        </div>

                        <button
                            type="button"
                            onClick={() => void handleMembershipCheckout()}
                            className="mt-6 inline-flex items-center gap-2 rounded-xl bg-violet-600 px-5 py-3 text-sm font-semibold text-white transition hover:bg-violet-500"
                        >
                            <Sparkles className="h-4 w-4" />
                            {membershipLoading
                                ? 'Opening...'
                                : usesManualPayPalMembership
                                    ? 'Extend Membership'
                                    : billingActive
                                        ? 'Manage Membership'
                                        : 'Start Membership'}
                        </button>
                    </section>

                    <aside className="space-y-6">
                        <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                            <h2 className="text-lg font-semibold text-white">Balance Overview</h2>
                            <div className="mt-4 grid gap-3">
                                <BalanceCard label="Credit Wallet" value={Number(topupCreditsRemaining || 0)} accent="cyan" helper="Pay-as-you-go balance" />
                                <BalanceCard label="Included Credits" value={Number(monthlyCreditsRemaining || 0)} accent="violet" helper="Burns first when membership is active" />
                                <BalanceCard label="Total Available" value={Number(creditsTotalRemaining || 0)} accent="emerald" helper={requiresTopup ? 'Top up before the next animation-heavy run' : 'Ready for Catalyst jobs'} />
                            </div>
                        </section>

                        <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                            <h2 className="text-lg font-semibold text-white">Selected Credit Pack</h2>
                            {selectedPack ? (
                                <>
                                    <div className="mt-4 rounded-2xl border border-white/[0.08] bg-black/20 p-4">
                                        <p className="text-xs uppercase tracking-[0.18em] text-gray-500">Pack</p>
                                        <p className="mt-2 text-lg font-semibold text-white">{String(selectedPack.pack || '').toUpperCase()} Pack</p>
                                        <div className="mt-4 flex items-center justify-between text-sm">
                                            <span className="text-gray-400">Credits</span>
                                            <span className="font-semibold text-cyan-300">{selectedPack.credits}</span>
                                        </div>
                                        <div className="mt-2 flex items-center justify-between text-sm">
                                            <span className="text-gray-400">Price</span>
                                            <span className="font-semibold text-white">${Number(selectedPack.price_usd || 0).toFixed(2)}</span>
                                        </div>
                                    </div>
                                    <button
                                        type="button"
                                        onClick={() => void handlePackCheckout()}
                                        className="mt-5 w-full rounded-xl bg-cyan-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-cyan-500"
                                    >
                                        {packCheckoutLoadingId === selectedPack.price_id ? 'Opening PayPal...' : 'Buy Credits with PayPal'}
                                    </button>
                                </>
                            ) : (
                                <p className="mt-4 text-sm text-gray-400">Select a credit pack below.</p>
                            )}
                        </section>
                    </aside>
                </div>

                <section className="mt-8 rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                    <div className="mb-5 flex flex-wrap items-center justify-between gap-4">
                        <div>
                            <h2 className="text-lg font-semibold text-white">Credit Wallet Packs</h2>
                            <p className="mt-1 text-sm text-gray-400">
                                Use wallet packs if you want pay-as-you-go only, or combine them with membership for a hybrid setup.
                            </p>
                        </div>
                    </div>
                    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
                        {sortedPacks.map((pack) => {
                            const active = pack.price_id === selectedPackId;
                            return (
                                <button
                                    key={pack.price_id}
                                    type="button"
                                    onClick={() => setSelectedPackId(pack.price_id)}
                                    className={`rounded-2xl border p-4 text-left transition ${
                                        active
                                            ? 'border-cyan-500 bg-cyan-500/10'
                                            : 'border-white/[0.08] bg-black/20 hover:border-cyan-500/30'
                                    }`}
                                >
                                    <p className="text-sm font-semibold text-white">{String(pack.pack || '').toUpperCase()} Pack</p>
                                    <p className="mt-1 text-xs text-gray-500">{pack.credits} credits</p>
                                    <p className="mt-4 text-2xl font-bold text-white">${Number(pack.price_usd || 0).toFixed(2)}</p>
                                    <p className="mt-1 text-[11px] text-gray-500">one-time top-up</p>
                                </button>
                            );
                        })}
                    </div>
                </section>

                {checkoutError && (
                    <p className="mt-6 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-5 py-4 text-sm text-amber-100">
                        {checkoutError}
                    </p>
                )}
                {topupResult === 'success' && (
                    <p className="mt-6 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-5 py-4 text-sm text-emerald-100">
                        Credit wallet payment received. Your balance is refreshing now.
                    </p>
                )}
                {subscriptionResult === 'success' && (
                    <p className="mt-6 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-5 py-4 text-sm text-emerald-100">
                        Catalyst Membership is active. Included credits will burn before the wallet on eligible jobs.
                    </p>
                )}
                {subscriptionResult === 'cancelled' && (
                    <p className="mt-6 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-5 py-4 text-sm text-amber-100">
                        Membership checkout was cancelled.{subscriptionError ? ` ${subscriptionError}` : ''}
                    </p>
                )}
            </div>
        </>
    );
}

function BalanceCard({
    label,
    value,
    helper,
    accent,
}: {
    label: string;
    value: number;
    helper: string;
    accent: 'cyan' | 'violet' | 'emerald';
}) {
    const accentClasses = accent === 'cyan'
        ? 'border-cyan-500/20 bg-cyan-500/10 text-cyan-100'
        : accent === 'violet'
            ? 'border-violet-500/20 bg-violet-500/10 text-violet-100'
            : 'border-emerald-500/20 bg-emerald-500/10 text-emerald-100';

    return (
        <div className={`rounded-2xl border px-4 py-3 ${accentClasses}`}>
            <p className="text-[10px] uppercase tracking-[0.18em] opacity-70">{label}</p>
            <p className="mt-2 text-2xl font-bold">{value}</p>
            <p className="mt-1 text-xs opacity-75">{helper}</p>
        </div>
    );
}
