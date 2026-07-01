import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { ArrowLeft, CheckCircle2 } from 'lucide-react';
import BillingPremiumView from '../components/billing/BillingPremiumView';
import StudioShell from '../components/layout/StudioShell';
import { UNIFIED_PLANS, UNIFIED_TOPUP_PACKS, type UnifiedPlanId } from '../lib/studioProduct';
import { type PageNav } from '../components/NavBar';
import { AuthContext, GENERATION_API, STUDIO_SITE_URL, isBillingHost, resolveStudioBackendUrl } from '../shared';
import { trackMembershipPurchaseCompleted, trackOnce, trackTopupPurchaseCompleted } from '../lib/googleAds';

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
    } = useContext(AuthContext);
    const [locationState, setLocationState] = useState(() => ({
        search: typeof window === 'undefined' ? '' : window.location.search,
        hash: typeof window === 'undefined' ? '' : window.location.hash,
    }));
    const [unifiedBalance, setUnifiedBalance] = useState<number | null>(null);
    const params = useMemo(() => new URLSearchParams(locationState.search), [locationState.search]);
    const requestedSection = String(params.get('section') || '').trim().toLowerCase();
    const requestedPackId = String(params.get('pack') || '').trim();
    const requestedPlanId = String(params.get('plan') || '').trim().toLowerCase();
    const topupResult = String(params.get('topup') || '').trim().toLowerCase();
    const subscriptionResult = String(params.get('subscription') || '').trim().toLowerCase();
    const stripeProvider = String(params.get('provider') || '').trim().toLowerCase() === 'stripe';
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
    const sortedPacks = useMemo(() => {
        const hasUnified = topupPacks.some((p) => p.price_id.startsWith('uc_'));
        const source = hasUnified ? topupPacks : UNIFIED_TOPUP_PACKS;
        return [...source].sort((a, b) => a.credits - b.credits);
    }, [topupPacks]);
    const selectedPack = useMemo(
        () => sortedPacks.find((pack) => pack.price_id === selectedPackId) || null,
        [selectedPackId, sortedPacks],
    );
    const normalizedCurrentPlan = useMemo<UnifiedPlanId | ''>(() => {
        const raw = String(membershipPlanId || plan || '').trim().toLowerCase();
        const alias = raw === 'creator' ? 'studio_pro_2500' : raw === 'studio' ? 'studio_pro_11k' : raw;
        if (UNIFIED_PLANS.some((p) => p.id === alias)) return alias as UnifiedPlanId;
        return '';
    }, [membershipPlanId, plan]);

    const publicPlans = useMemo(
        () =>
            UNIFIED_PLANS.map((p) => ({
                id: p.id,
                title: p.title,
                priceUsd: p.priceUsd,
                monthlyCredits: p.monthlyCredits,
                priceLabel: `$${p.priceUsd}/mo`,
                description: p.description,
                features: p.features,
                bestValue: Boolean(p.bestValue),
            })),
        [],
    );

    useEffect(() => {
        const tok = session?.access_token;
        if (!tok) return;
        let cancelled = false;
        (async () => {
            try {
                const res = await fetch(resolveStudioBackendUrl('/api/studio-agent/credits'), {
                    headers: { Authorization: `Bearer ${tok}` },
                });
                if (!res.ok) return;
                const data = (await res.json()) as { balance?: number; unlimited?: boolean };
                if (!cancelled) {
                    setUnifiedBalance(data.unlimited ? 999999 : Number(data.balance || 0));
                }
            } catch {
                /* fallback to legacy total */
            }
        })();
        return () => {
            cancelled = true;
        };
    }, [session]);

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
        if (!wantsTopups || !topupSectionRef.current) return;
        const target = topupSectionRef.current;
        const timeoutId = window.setTimeout(() => target.scrollIntoView({ behavior: 'smooth', block: 'start' }), 60);
        return () => window.clearTimeout(timeoutId);
    }, [requestedHash, requestedPackId, requestedSection, sortedPacks.length]);

    useEffect(() => {
        if (topupResult !== 'success') return;
        trackOnce(`billing_topup_success:${locationState.search}`, () => {
            trackTopupPurchaseCompleted(Number(selectedPack?.price_usd || 0));
        });
    }, [locationState.search, selectedPack?.price_usd, topupResult]);

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
        return () => {
            cancelled = true;
        };
    }, [paypalProvider, paypalOrderId, topupResult, subscriptionResult, verifyPayPalOrder]);

    useEffect(() => {
        if (subscriptionResult !== 'success') return;
        const planId = requestedPlanId || normalizedCurrentPlan || 'studio_pro_1k';
        const match = UNIFIED_PLANS.find((p) => p.id === planId);
        trackOnce(`billing_membership_success:${locationState.search}`, () => {
            trackMembershipPurchaseCompleted(planId, match?.priceUsd || 0);
        });
    }, [locationState.search, normalizedCurrentPlan, requestedPlanId, subscriptionResult]);

    const handleBack = () => {
        if (isBillingHost) {
            window.location.href = STUDIO_SITE_URL;
            return;
        }
        onNavigate('dashboard');
    };

    const handlePlanAction = useCallback(
        async (planId: UnifiedPlanId) => {
            if (!session) {
                onNavigate('auth');
                return;
            }
            setCheckoutError('');
            setPlanLoadingId(planId);
            try {
                if (billingActive && usesStripeMembership) {
                    const err = await manageBilling();
                    if (err) setCheckoutError(err);
                    return;
                }
                if (billingActive && normalizedCurrentPlan === planId) {
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
        },
        [billingActive, checkout, manageBilling, normalizedCurrentPlan, onNavigate, session, usesManualPayPalMembership, usesStripeMembership],
    );

    const handlePackCheckout = useCallback(async (method: 'stripe' | 'paypal' = 'stripe') => {
        if (!selectedPack) {
            setCheckoutError('Select a credit pack first.');
            return;
        }
        if (!session) {
            onNavigate('auth');
            return;
        }
        setCheckoutError('');
        setPackCheckoutLoadingId(method);
        try {
            const err = await checkoutTopup(selectedPack.price_id, method === 'paypal' ? 'paypal' : 'card');
            if (err) setCheckoutError(err);
        } finally {
            setPackCheckoutLoadingId('');
        }
    }, [checkoutTopup, onNavigate, selectedPack, session]);

    const creditBalance = unifiedBalance ?? Number(creditsTotalRemaining || 0);

    const paypalBanner = (
        <>
            {topupResult === 'success' && (stripeProvider || (!paypalProvider || paypalVerifyState === 'verified')) && (
                <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-100">
                    Credit top-up received. Your balance is refreshing now.
                </div>
            )}
            {subscriptionResult === 'success' && (stripeProvider || (!paypalProvider || paypalVerifyState === 'verified')) && (
                <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-100">
                    Your plan is active. Monthly credits have been added to your wallet.
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
                creditBalance={creditBalance}
                selectedPack={selectedPack}
                sortedPacks={sortedPacks}
                onSelectPack={setSelectedPackId}
                onPlanAction={(id) => void handlePlanAction(id as UnifiedPlanId)}
                onPackCheckout={(method) => void handlePackCheckout(method)}
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
            <StudioShell onNavigate={onNavigate} fullWidth>
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
        reason.trim().length > 10 && amountValid && paymentRef.trim().length > 0 && imageProof.length > 0 && !submitting;

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
                        <span className="text-[11px] font-semibold uppercase tracking-wider text-gray-500">
                            Reason <span className="text-red-400">(required)</span>
                        </span>
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
                            <span className="text-[11px] font-semibold uppercase tracking-wider text-gray-500">
                                Amount paid (USD) <span className="text-red-400">(required)</span>
                            </span>
                            <input
                                type="number"
                                value={amount}
                                onChange={(e) => setAmount(e.target.value)}
                                step="0.01"
                                min="0.01"
                                required
                                placeholder="60.00"
                                className="mt-1 w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white placeholder-gray-600 focus:border-violet-400 focus:outline-none"
                            />
                        </label>
                        <label className="block">
                            <span className="text-[11px] font-semibold uppercase tracking-wider text-gray-500">
                                PayPal order / invoice id <span className="text-red-400">(required)</span>
                            </span>
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
                                Uploaded: {imageProofName}
                            </span>
                        )}
                    </label>
                    {error && <p className="text-[11px] text-red-300">{error}</p>}
                    <div className="flex items-center justify-end gap-2">
                        <button
                            type="button"
                            onClick={() => {
                                setOpen(false);
                                setError(null);
                            }}
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
