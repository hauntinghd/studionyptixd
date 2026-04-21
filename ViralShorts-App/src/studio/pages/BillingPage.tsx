import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { ArrowLeft, CheckCircle2, WalletCards } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, GENERATION_API, STUDIO_SITE_URL, isBillingHost } from '../shared';
import { trackMembershipPurchaseCompleted, trackOnce, trackTopupPurchaseCompleted } from '../lib/googleAds';

type PublicPlanId = 'free' | 'starter' | 'creator' | 'pro';

const PUBLIC_PLAN_ORDER: PublicPlanId[] = ['free', 'starter', 'creator', 'pro'];

export default function BillingPage({ onNavigate }: { onNavigate: PageNav }) {
    const {
        session,
        billingActive,
        membershipPlanId,
        membershipSource,
        nextRenewalSource,
        plan,
        checkout,
        checkoutTopup,
        manageBilling,
        verifyPayPalOrder,
        topupPacks,
        topupCreditsRemaining,
        monthlyCreditsRemaining,
        creditsTotalRemaining,
        publicPlanLimits,
        publicPlanPrices,
        requiresTopup,
    } = useContext(AuthContext);
    const [locationState, setLocationState] = useState(() => ({
        search: typeof window === 'undefined' ? '' : window.location.search,
        hash: typeof window === 'undefined' ? '' : window.location.hash,
    }));
    const params = useMemo(() => {
        return new URLSearchParams(locationState.search);
    }, [locationState.search]);
    const requestedSection = String(params.get('section') || '').trim().toLowerCase();
    const requestedPackId = String(params.get('pack') || '').trim();
    const requestedPlanId = String(params.get('plan') || '').trim().toLowerCase();
    const topupResult = String(params.get('topup') || '').trim().toLowerCase();
    const subscriptionResult = String(params.get('subscription') || '').trim().toLowerCase();
    const subscriptionError = String(params.get('error') || '').trim();
    const paypalProvider = String(params.get('provider') || '').trim().toLowerCase() === 'paypal';
    const paypalOrderId = String(params.get('order_id') || '').trim();
    const [paypalVerifyState, setPaypalVerifyState] = useState<'idle' | 'verifying' | 'verified' | 'failed' | 'revoked'>('idle');
    const [paypalVerifyError, setPaypalVerifyError] = useState('');
    const requestedHash = String(locationState.hash || '').replace(/^#/, '').trim().toLowerCase();
    const [selectedPackId, setSelectedPackId] = useState('');
    const [checkoutError, setCheckoutError] = useState('');
    const [packCheckoutLoadingId, setPackCheckoutLoadingId] = useState('');
    const [planLoadingId, setPlanLoadingId] = useState('');
    const topupSectionRef = useRef<HTMLElement | null>(null);

    const normalizedMembershipSource = String(membershipSource || nextRenewalSource || '').trim().toLowerCase();
    const usesStripeMembership = billingActive && normalizedMembershipSource === 'stripe';
    const usesManualPayPalMembership = billingActive && normalizedMembershipSource === 'paypal_manual';
    const sortedPacks = useMemo(() => [...topupPacks].sort((a, b) => a.credits - b.credits), [topupPacks]);
    const selectedPack = useMemo(
        () => sortedPacks.find((pack) => pack.price_id === selectedPackId) || null,
        [selectedPackId, sortedPacks],
    );
    const normalizedCurrentPlan = useMemo<PublicPlanId>(() => {
        const raw = String(membershipPlanId || plan || 'free').trim().toLowerCase();
        if (raw === 'creator' || raw === 'pro' || raw === 'starter') return raw;
        return 'free';
    }, [membershipPlanId, plan]);
    const publicPlans = useMemo(() => {
        return PUBLIC_PLAN_ORDER.map((planId) => {
            const limits = (publicPlanLimits as Record<string, any>)[planId] || {};
            const price = Number((publicPlanPrices as Record<string, number>)[planId] || 0);
            const durationMinutes = Math.max(1, Math.round(Number(limits.max_duration_sec || 0) / 60));
            const animatedCredits = Number(limits.animated_renders_per_month || 0);
            return {
                id: planId,
                title: planId === 'free' ? 'Free' : capitalizePlan(planId),
                price,
                priceLabel: planId === 'free' ? '$0' : `$${price.toFixed(price % 1 === 0 ? 0 : 2)}/mo`,
                description:
                    planId === 'free'
                        ? 'Try the short-form Studio workflow and get enough included credits for two animated renders.'
                        : planId === 'starter'
                            ? 'Best for solo operators shipping consistent short-form content without overcommitting.'
                            : planId === 'creator'
                                ? 'More monthly headroom for active creators publishing shorts every week.'
                                : 'Highest short-form headroom for daily operators and teams.',
                features: [
                    `${animatedCredits} included animation credits${planId === 'free' ? '' : ' per month'}`,
                    `${durationMinutes} minute max job length`,
                    `${String(limits.max_resolution || '720p').toUpperCase()} output cap`,
                    planId === 'free'
                        ? 'Create workspace with AI Stories, Motivation, Skeleton AI, and Day Trading'
                        : 'Create workspace + Chat Story template access',
                ],
            };
        });
    }, [publicPlanLimits, publicPlanPrices]);

    useEffect(() => {
        const syncLocationState = () => {
            setLocationState({
                search: window.location.search,
                hash: window.location.hash,
            });
        };
        window.addEventListener('popstate', syncLocationState);
        window.addEventListener('hashchange', syncLocationState);
        window.addEventListener('nyptid:navigation', syncLocationState as EventListener);
        return () => {
            window.removeEventListener('popstate', syncLocationState);
            window.removeEventListener('hashchange', syncLocationState);
            window.removeEventListener('nyptid:navigation', syncLocationState as EventListener);
        };
    }, []);

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

    useEffect(() => {
        const wantsTopups = requestedSection === 'topups' || requestedHash === 'topup-packs' || Boolean(requestedPackId);
        if (!wantsTopups) return;
        if (!topupSectionRef.current) return;
        const target = topupSectionRef.current;
        let frameId = 0;
        let timeoutId = 0;
        const scrollIntoTopups = () => {
            target.scrollIntoView({ behavior: 'smooth', block: 'start' });
        };
        timeoutId = window.setTimeout(() => {
            scrollIntoTopups();
            frameId = window.requestAnimationFrame(scrollIntoTopups);
        }, 60);
        return () => {
            window.clearTimeout(timeoutId);
            window.cancelAnimationFrame(frameId);
        };
    }, [requestedHash, requestedPackId, requestedSection, sortedPacks.length]);

    useEffect(() => {
        if (topupResult !== 'success') return;
        const topupValue = Number(selectedPack?.price_usd || 0);
        trackOnce(`billing_topup_success:${locationState.search}`, () => {
            trackTopupPurchaseCompleted(topupValue);
        });
    }, [locationState.search, selectedPack?.price_usd, topupResult]);

    // Second-factor verification: after a PayPal redirect, ask the backend whether the
    // order actually captured. We don't celebrate success until this confirms.
    useEffect(() => {
        if (!paypalProvider || !paypalOrderId) return;
        if (topupResult !== 'success' && subscriptionResult !== 'success') return;
        let cancelled = false;
        setPaypalVerifyState('verifying');
        setPaypalVerifyError('');
        (async () => {
            const result = await verifyPayPalOrder(paypalOrderId);
            if (cancelled) return;
            if (!result.ok) {
                setPaypalVerifyState('failed');
                setPaypalVerifyError(result.error || 'Unable to verify payment');
                return;
            }
            if (result.revoked) {
                setPaypalVerifyState('revoked');
                return;
            }
            setPaypalVerifyState(result.captured ? 'verified' : 'failed');
            if (!result.captured) setPaypalVerifyError('PayPal has not confirmed capture yet. Refresh in a moment.');
        })();
        return () => { cancelled = true; };
    }, [paypalProvider, paypalOrderId, topupResult, subscriptionResult, verifyPayPalOrder]);

    useEffect(() => {
        if (subscriptionResult !== 'success') return;
        const planId = requestedPlanId || normalizedCurrentPlan;
        const value = Number((publicPlanPrices as Record<string, number>)[planId] || 0);
        trackOnce(`billing_membership_success:${locationState.search}`, () => {
            trackMembershipPurchaseCompleted(planId, value);
        });
    }, [locationState.search, normalizedCurrentPlan, publicPlanPrices, requestedPlanId, subscriptionResult]);

    const handleBack = () => {
        if (isBillingHost) {
            window.location.href = STUDIO_SITE_URL;
            return;
        }
        onNavigate('dashboard');
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
        setCheckoutError('');
        setPlanLoadingId(planId);
        try {
            if (billingActive && normalizedCurrentPlan === planId) {
                if (usesStripeMembership) {
                    const err = await manageBilling();
                    if (err) setCheckoutError(err);
                    return;
                }
                if (usesManualPayPalMembership) {
                    const err = await checkout(planId);
                    if (err) setCheckoutError(err);
                    return;
                }
            }
            const err = await checkout(planId);
            if (err) setCheckoutError(err);
        } finally {
            setPlanLoadingId('');
        }
    }, [billingActive, checkout, manageBilling, normalizedCurrentPlan, onNavigate, session, usesManualPayPalMembership, usesStripeMembership]);

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
                        <h1 className="mt-3 text-3xl font-bold text-white">One price. Every short ships.</h1>
                        <p className="mt-2 max-w-3xl text-sm text-gray-400">
                            Start free with two animated shorts. Upgrade when you're ready to post every day — no babysitting the renderer, no mystery errors, and any failed render refunds your credits automatically.
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
                    </div>
                </div>

                <div className="grid gap-6 xl:grid-cols-[minmax(0,1.45fr),minmax(320px,0.75fr)]">
                    <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                        <div className="mb-5">
                            <p className="text-xs uppercase tracking-[0.18em] text-violet-300">Plans</p>
                            <h2 className="mt-2 text-2xl font-bold text-white">Pick the plan that matches your posting cadence</h2>
                            <p className="mt-2 text-sm text-gray-400">
                                Every render goes through the same reliability stack: content-filter errors surface with edit-prompt guidance, and failed renders automatically refund your credits. Pick the plan based on how many shorts you want to ship this month.
                            </p>
                        </div>
                        <div className="grid gap-4 md:grid-cols-2 2xl:grid-cols-4">
                            {publicPlans.map((planCard) => {
                                const isCurrent = normalizedCurrentPlan === planCard.id;
                                const isPaidCurrent = billingActive && isCurrent && planCard.id !== 'free';
                                const actionLabel = planCard.id === 'free'
                                    ? (session ? (isCurrent && !billingActive ? 'Current plan' : 'Included with account') : 'Sign in to start')
                                    : isPaidCurrent
                                        ? (usesStripeMembership ? 'Manage plan' : 'Extend plan')
                                        : billingActive
                                            ? `Switch to ${planCard.title}`
                                            : `Start ${planCard.title}`;
                                return (
                                    <div
                                        key={planCard.id}
                                        className={`rounded-3xl border p-5 ${
                                            isCurrent
                                                ? 'border-violet-500/40 bg-violet-500/[0.08]'
                                                : 'border-white/[0.08] bg-black/20'
                                        }`}
                                    >
                                        <div className="flex items-start justify-between gap-3">
                                            <div>
                                                <p className="text-xs uppercase tracking-[0.18em] text-gray-500">{planCard.id === 'free' ? 'Free plan' : 'Monthly plan'}</p>
                                                <h3 className="mt-2 text-xl font-bold text-white">{planCard.title}</h3>
                                            </div>
                                            {isCurrent && (
                                                <span className="rounded-full border border-violet-400/30 bg-violet-500/10 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-violet-200">
                                                    Current
                                                </span>
                                            )}
                                        </div>
                                        <p className="mt-4 text-3xl font-bold text-white">{planCard.priceLabel}</p>
                                        <p className="mt-3 text-sm text-gray-300">{planCard.description}</p>
                                        <div className="mt-5 space-y-3">
                                            {planCard.features.map((feature) => (
                                                <div key={feature} className="flex items-start gap-2 text-sm text-gray-300">
                                                    <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-emerald-300" />
                                                    <span>{feature}</span>
                                                </div>
                                            ))}
                                        </div>
                                        <button
                                            type="button"
                                            onClick={() => void handlePlanAction(planCard.id)}
                                            disabled={planLoadingId === planCard.id || (planCard.id === 'free' && !!session && isCurrent && !billingActive)}
                                            className={`mt-6 w-full rounded-xl px-4 py-3 text-sm font-semibold transition ${
                                                isCurrent
                                                    ? 'bg-white/[0.08] text-white hover:bg-white/[0.14]'
                                                    : 'bg-violet-600 text-white hover:bg-violet-500'
                                            } disabled:opacity-60`}
                                        >
                                            {planLoadingId === planCard.id ? 'Opening...' : actionLabel}
                                        </button>
                                    </div>
                                );
                            })}
                        </div>
                    </section>

                    <aside className="space-y-6">
                        <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                            <h2 className="text-lg font-semibold text-white">Balance Overview</h2>
                            <div className="mt-4 grid gap-3">
                                <BalanceCard label="Credit Wallet" value={Number(topupCreditsRemaining || 0)} accent="cyan" helper="Pay-as-you-go balance" />
                                <BalanceCard label="Included Credits" value={Number(monthlyCreditsRemaining || 0)} accent="violet" helper="Monthly plan or free-plan included credits" />
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
                                <p className="mt-4 text-sm text-gray-400">Select a top-up pack below.</p>
                            )}
                        </section>
                        <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                            <h2 className="text-lg font-semibold text-white">What plans actually unlock</h2>
                            <div className="mt-4 space-y-3 text-sm text-gray-300">
                                <p>1. Free and paid plans are for short-form only.</p>
                                <p>2. Paid monthly plans add more included credits and unlock Chat Story.</p>
                                <p>3. Clone, Thumbnails, and Long Form stay outside the public plan promise while they are still in beta.</p>
                                <p>4. Wallet packs stack on top for heavier animation usage.</p>
                            </div>
                        </section>
                    </aside>
                </div>

                <section
                    id="topup-packs"
                    ref={topupSectionRef}
                    className="mt-8 scroll-mt-28 rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6"
                >
                    <div className="mb-5">
                        <h2 className="text-lg font-semibold text-white">Top-up packs</h2>
                        <p className="mt-1 text-sm text-gray-400">
                            Pay-as-you-go credits for bursty weeks. Stack on any plan — your monthly credits burn first, then top-up credits kick in.
                        </p>
                    </div>
                    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
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
                {topupResult === 'success' && (!paypalProvider || paypalVerifyState === 'verified') && (
                    <p className="mt-6 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-5 py-4 text-sm text-emerald-100">
                        Credit wallet payment received. Your balance is refreshing now.
                    </p>
                )}
                {subscriptionResult === 'success' && (!paypalProvider || paypalVerifyState === 'verified') && (
                    <p className="mt-6 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-5 py-4 text-sm text-emerald-100">
                        Your monthly plan is active. Included credits now burn before the wallet.
                    </p>
                )}
                {paypalProvider && paypalVerifyState === 'verifying' && (
                    <p className="mt-6 rounded-2xl border border-sky-500/20 bg-sky-500/10 px-5 py-4 text-sm text-sky-100">
                        Confirming your PayPal payment with our servers…
                    </p>
                )}
                {paypalProvider && paypalVerifyState === 'failed' && (
                    <p className="mt-6 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-5 py-4 text-sm text-amber-100">
                        We couldn't confirm your PayPal payment yet. {paypalVerifyError ? `Details: ${paypalVerifyError}. ` : ''}If funds were charged, they will show up within a minute — refresh this page to re-check.
                    </p>
                )}
                {paypalProvider && paypalVerifyState === 'revoked' && (
                    <p className="mt-6 rounded-2xl border border-rose-500/20 bg-rose-500/10 px-5 py-4 text-sm text-rose-100">
                        This PayPal order was refunded or reversed. Credits/access have been removed. If this is a mistake, contact support.
                    </p>
                )}
                {subscriptionResult === 'cancelled' && (
                    <p className="mt-6 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-5 py-4 text-sm text-amber-100">
                        Monthly checkout was cancelled.{subscriptionError ? ` ${subscriptionError}` : ''}
                    </p>
                )}
                <RefundRequestCard />
            </div>
        </>
    );
}

const MAX_PROOF_BYTES = 2 * 1024 * 1024;

function RefundRequestCard() {
    const { session } = useContext(AuthContext);
    const [open, setOpen] = useState(false);
    const [reason, setReason] = useState('');
    const [amount, setAmount] = useState('');
    const [paymentRef, setPaymentRef] = useState('');
    const [imageProof, setImageProof] = useState<string>('');
    const [imageProofName, setImageProofName] = useState<string>('');
    const [submitting, setSubmitting] = useState(false);
    const [submitted, setSubmitted] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const amountNumber = Number(amount);
    const amountValid = amount.trim().length > 0 && Number.isFinite(amountNumber) && amountNumber > 0;
    const canSubmit =
        reason.trim().length > 10 &&
        amountValid &&
        paymentRef.trim().length > 0 &&
        imageProof.length > 0 &&
        !submitting;

    const handleProofFile = (file: File | null) => {
        setError(null);
        if (!file) {
            setImageProof('');
            setImageProofName('');
            return;
        }
        if (!file.type.startsWith('image/')) {
            setError('Image proof must be an image file (PNG, JPG, etc.).');
            return;
        }
        if (file.size > MAX_PROOF_BYTES) {
            setError(`Image proof is too large (max 2 MB). Yours is ${(file.size / 1024 / 1024).toFixed(1)} MB.`);
            return;
        }
        const reader = new FileReader();
        reader.onerror = () => setError('Could not read the selected file.');
        reader.onload = () => {
            const result = typeof reader.result === 'string' ? reader.result : '';
            if (!result) {
                setError('Could not read the selected file.');
                return;
            }
            setImageProof(result);
            setImageProofName(file.name);
        };
        reader.readAsDataURL(file);
    };

    const submit = async () => {
        if (!session || !canSubmit) return;
        setSubmitting(true);
        setError(null);
        try {
            const res = await fetch(`${GENERATION_API}/api/billing/refund-request`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${session.access_token}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    reason: reason.trim(),
                    amount_usd: amountNumber,
                    payment_reference: paymentRef.trim(),
                    image_proof: imageProof,
                }),
            });
            if (!res.ok) {
                const txt = await res.text().catch(() => '');
                throw new Error(txt || `HTTP ${res.status}`);
            }
            setSubmitted(true);
            setReason('');
            setAmount('');
            setPaymentRef('');
            setImageProof('');
            setImageProofName('');
        } catch (e: any) {
            setError(e?.message || 'Could not submit refund request');
        } finally {
            setSubmitting(false);
        }
    };

    return (
        <div className="mt-8 rounded-2xl border border-white/[0.08] bg-white/[0.02] p-5">
            <div className="flex items-center justify-between gap-3">
                <div>
                    <h3 className="text-base font-semibold text-white">Need a refund?</h3>
                    <p className="mt-1 text-[12px] text-gray-400">
                        Submit a request directly here. No Discord join required — we'll review and respond by email.
                    </p>
                </div>
                {!open && !submitted && (
                    <button
                        type="button"
                        onClick={() => setOpen(true)}
                        className="rounded-lg border border-white/[0.08] bg-white/[0.02] px-3 py-1.5 text-xs font-semibold text-gray-200 transition hover:bg-white/[0.05]"
                    >
                        Request refund
                    </button>
                )}
            </div>
            {submitted && (
                <div className="mt-4 rounded-xl border border-emerald-500/30 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-100">
                    Got it — your refund request is in the admin queue. You'll get an email once it's been reviewed.
                </div>
            )}
            {open && !submitted && (
                <div className="mt-4 space-y-3">
                    <p className="text-[11px] text-gray-500">
                        All four fields are required so we can match your request to the PayPal charge and respond quickly.
                    </p>
                    <label className="block">
                        <span className="text-[11px] font-semibold uppercase tracking-wider text-gray-500">Reason <span className="text-red-400">(required)</span></span>
                        <textarea
                            value={reason}
                            onChange={(e) => setReason(e.target.value)}
                            rows={4}
                            placeholder="What happened, and what are you hoping we do?"
                            className="mt-1 w-full resize-y rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white placeholder-gray-600 focus:border-violet-400 focus:outline-none"
                        />
                    </label>
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                        <label className="block">
                            <span className="text-[11px] font-semibold uppercase tracking-wider text-gray-500">Amount paid (USD) <span className="text-red-400">(required)</span></span>
                            <input
                                type="number"
                                value={amount}
                                onChange={(e) => setAmount(e.target.value)}
                                step="0.01"
                                min="0.01"
                                required
                                placeholder="29.00"
                                className="mt-1 w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white placeholder-gray-600 focus:border-violet-400 focus:outline-none"
                            />
                        </label>
                        <label className="block">
                            <span className="text-[11px] font-semibold uppercase tracking-wider text-gray-500">PayPal order / invoice id <span className="text-red-400">(required)</span></span>
                            <input
                                type="text"
                                value={paymentRef}
                                onChange={(e) => setPaymentRef(e.target.value)}
                                required
                                placeholder="8AB123456789"
                                className="mt-1 w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white placeholder-gray-600 focus:border-violet-400 focus:outline-none"
                            />
                        </label>
                    </div>
                    <label className="block">
                        <span className="text-[11px] font-semibold uppercase tracking-wider text-gray-500">
                            Image proof <span className="text-red-400">(required)</span>
                            <span className="ml-1 font-normal normal-case tracking-normal text-gray-500">— PayPal receipt, order page, or bank statement screenshot (max 2 MB)</span>
                        </span>
                        <input
                            type="file"
                            accept="image/*"
                            onChange={(e) => handleProofFile(e.target.files?.[0] ?? null)}
                            required
                            className="mt-1 block w-full cursor-pointer rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-xs text-gray-300 file:mr-3 file:rounded-md file:border-0 file:bg-violet-500/20 file:px-3 file:py-1.5 file:text-xs file:font-semibold file:text-violet-100 hover:file:bg-violet-500/30"
                        />
                        {imageProof && (
                            <span className="mt-1 flex items-center gap-2 text-[11px] text-emerald-300">
                                <CheckCircle2 className="h-3.5 w-3.5" />
                                Uploaded: {imageProofName} ({Math.round(imageProof.length / 1024)} KB encoded)
                            </span>
                        )}
                    </label>
                    {error && (
                        <p className="text-[11px] text-red-300">{error}</p>
                    )}
                    <div className="flex items-center justify-end gap-2">
                        <button
                            type="button"
                            onClick={() => { setOpen(false); setError(null); }}
                            className="rounded-lg px-3 py-1.5 text-xs font-semibold text-gray-400 transition hover:text-white"
                        >
                            Cancel
                        </button>
                        <button
                            type="button"
                            onClick={() => void submit()}
                            disabled={!canSubmit}
                            className="rounded-lg bg-gradient-to-r from-violet-600 to-cyan-600 px-4 py-1.5 text-xs font-semibold text-white transition hover:opacity-95 disabled:cursor-not-allowed disabled:opacity-50"
                        >
                            {submitting ? 'Submitting…' : 'Submit refund request'}
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
}

function capitalizePlan(planId: PublicPlanId) {
    return planId.charAt(0).toUpperCase() + planId.slice(1);
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
            <p className="text-[10px] uppercase tracking-[0.18em] text-white/60">{label}</p>
            <p className="mt-2 text-2xl font-bold text-white">{value}</p>
            <p className="mt-1 text-xs text-white/70">{helper}</p>
        </div>
    );
}
