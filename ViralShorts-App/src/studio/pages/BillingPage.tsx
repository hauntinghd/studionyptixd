import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { ArrowLeft, CheckCircle2 } from 'lucide-react';
import BillingPremiumView from '../components/billing/BillingPremiumView';
import StudioShell from '../components/layout/StudioShell';
import StudioSidebar, { buildSidebarItems } from '../components/layout/StudioSidebar';
import { estimateShortsRemaining } from '../lib/studioProduct';
import { type PageNav } from '../components/NavBar';
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
        creditsTotalRemaining,
        publicPlanLimits,
        publicPlanPrices,
        role,
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
                        ? 'Create workspace with Alt-History Battles, Moral Dilemma, Scary Stories, and Historical Epic'
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

    const isAdmin = role === 'admin';
    const totalAc = Number(creditsTotalRemaining || 0);
    const shortsEstimate = estimateShortsRemaining(totalAc);

    const paypalBanner = (
        <>
            {topupResult === 'success' && (!paypalProvider || paypalVerifyState === 'verified') && (
                <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-100">
                    Credit wallet payment received. Your balance is refreshing now.
                </div>
            )}
            {subscriptionResult === 'success' && (!paypalProvider || paypalVerifyState === 'verified') && (
                <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-100">
                    Your monthly plan is active. Included credits burn before wallet fuel.
                </div>
            )}
            {paypalProvider && paypalVerifyState === 'verifying' && (
                <div className="rounded-xl border border-sky-500/20 bg-sky-500/10 px-4 py-3 text-sm text-sky-100">
                    Confirming your PayPal payment…
                </div>
            )}
            {paypalProvider && paypalVerifyState === 'failed' && (
                <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                    Payment not confirmed yet. {paypalVerifyError} Refresh if you were charged.
                </div>
            )}
        </>
    );

    const billingBody = (
        <>
            {!isBillingHost && (
                <button
                    type="button"
                    onClick={handleBack}
                    className="mb-4 inline-flex items-center gap-2 text-sm text-gray-400 transition hover:text-white"
                >
                    <ArrowLeft className="h-4 w-4" />
                    Back to Studio
                </button>
            )}
            <BillingPremiumView
                publicPlans={publicPlans}
                normalizedCurrentPlan={normalizedCurrentPlan}
                billingActive={billingActive}
                totalAc={totalAc}
                shortsEstimate={shortsEstimate}
                selectedPack={selectedPack}
                sortedPacks={sortedPacks}
                onSelectPack={setSelectedPackId}
                onPlanAction={(id) => void handlePlanAction(id as PublicPlanId)}
                onPackCheckout={() => void handlePackCheckout()}
                planLoadingId={planLoadingId}
                packCheckoutLoadingId={packCheckoutLoadingId}
                checkoutError={checkoutError}
                paypalBanner={paypalBanner}
                topUpSectionRef={topupSectionRef}
                refundSection={<RefundRequestCard />}
            />
        </>
    );

    if (session) {
        return (
            <StudioShell
                onNavigate={onNavigate}
                sidebar={
                    <StudioSidebar
                        active="home"
                        items={buildSidebarItems(isAdmin)}
                        onCreate={() => onNavigate('dashboard')}
                        onSelect={() => onNavigate('dashboard')}
                    />
                }
            >
                {billingBody}
            </StudioShell>
        );
    }

    return (
        <div className="min-h-screen bg-[#09090b] px-6 py-24 text-gray-100">
            <div className="mx-auto max-w-6xl">{billingBody}</div>
        </div>
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
