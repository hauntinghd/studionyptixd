import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import MembershipPremiumView from '../components/membership/MembershipPremiumView';
import StudioShell from '../components/layout/StudioShell';
import { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, STUDIO_SITE_URL, isBillingHost } from '../shared';
import { trackMembershipPurchaseCompleted, trackOnce } from '../lib/googleAds';

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
        verifyPayPalOrder,
    } = useContext(AuthContext);
    const params = useMemo(() => {
        if (typeof window === 'undefined') return new URLSearchParams();
        return new URLSearchParams(window.location.search);
    }, []);
    const requestedPlanId = String(params.get('plan') || '').trim().toLowerCase();
    const subscriptionResult = String(params.get('subscription') || '').trim().toLowerCase();
    const subscriptionError = String(params.get('error') || '').trim();
    const paypalProvider = String(params.get('provider') || '').trim().toLowerCase() === 'paypal';
    const paypalOrderId = String(params.get('order_id') || '').trim();
    const [paypalVerifyState, setPaypalVerifyState] = useState<'idle' | 'verifying' | 'verified' | 'failed' | 'revoked'>('idle');
    const [paypalVerifyError, setPaypalVerifyError] = useState('');
    const [actionError, setActionError] = useState('');
    const [loadingPlanId, setLoadingPlanId] = useState('');
    const normalizedMembershipSource = String(membershipSource || nextRenewalSource || '').trim().toLowerCase();
    const usesStripeMembership = billingActive && normalizedMembershipSource === 'stripe';
    const usesManualPayPalMembership = billingActive && normalizedMembershipSource === 'paypal_manual';
    const normalizedCurrentPlan = useMemo<PublicPlanId>(() => {
        const raw = String(membershipPlanId || 'free').trim().toLowerCase();
        if (raw === 'starter' || raw === 'creator' || raw === 'pro') return raw;
        return 'free';
    }, [membershipPlanId]);

    const planCards = useMemo(() => {
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
                        ? 'Two included animation credits to try Create.'
                        : planId === 'starter'
                            ? 'Best for solo operators getting started.'
                            : planId === 'creator'
                                ? 'More monthly headroom for active creators.'
                                : 'Highest public headroom for daily operators.',
                bullets: [
                    `${animatedCredits} included animation credits${planId === 'free' ? '' : ' per month'}`,
                    `${Math.max(1, Math.round(Number(limits.max_duration_sec || 0) / 60))} minute max jobs`,
                    `${String(limits.max_resolution || '720p').toUpperCase()} output`,
                    planId === 'free'
                        ? 'All public short-form niches in Create'
                        : 'Create + Chat Story template access',
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

    useEffect(() => {
        if (!paypalProvider || !paypalOrderId || subscriptionResult !== 'success') return;
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
            if (!result.captured) setPaypalVerifyError('PayPal has not confirmed capture yet.');
        })();
        return () => { cancelled = true; };
    }, [paypalProvider, paypalOrderId, subscriptionResult, verifyPayPalOrder]);

    const currentStatus = billingActive ? capitalizePlan(normalizedCurrentPlan) : 'Free';

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

    const banners = (
        <>
            {subscriptionResult === 'success' && (!paypalProvider || paypalVerifyState === 'verified') && (
                <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-100">
                    Your monthly plan is active. Included credits now burn before the wallet.
                </div>
            )}
            {paypalProvider && paypalVerifyState === 'verifying' && (
                <div className="rounded-xl border border-sky-500/20 bg-sky-500/10 px-4 py-3 text-sm text-sky-100">
                    Confirming your PayPal payment with our servers…
                </div>
            )}
            {paypalProvider && paypalVerifyState === 'failed' && (
                <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                    We couldn't confirm your PayPal payment yet. {paypalVerifyError ? `Details: ${paypalVerifyError}. ` : ''}If funds were charged, refresh in a minute.
                </div>
            )}
            {paypalProvider && paypalVerifyState === 'revoked' && (
                <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-100">
                    This PayPal order was refunded or reversed. Access has been removed.
                </div>
            )}
            {subscriptionResult === 'manual' && (
                <div className="rounded-xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3 text-sm text-cyan-100">
                    Manual PayPal renewal — click the same plan again to extend another month.
                </div>
            )}
            {subscriptionResult === 'cancelled' && (
                <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                    Checkout was cancelled.{subscriptionError ? ` ${subscriptionError}` : ''}
                </div>
            )}
        </>
    );

    const plans = planCards.map((planCard) => {
        const isCurrent = normalizedCurrentPlan === planCard.id;
        const isPaidCurrent = billingActive && isCurrent && planCard.id !== 'free';
        const actionLabel = planCard.id === 'free'
            ? (isCurrent && !billingActive ? 'Current plan' : 'Included with account')
            : isPaidCurrent
                ? (usesStripeMembership ? 'Manage plan' : 'Extend plan')
                : billingActive
                    ? `Switch to ${planCard.title}`
                    : `Start ${planCard.title}`;
        return {
            ...planCard,
            isCurrent,
            actionLabel,
            loading: loadingPlanId === planCard.id,
            disabled: planCard.id === 'free' && isCurrent && !billingActive,
            onAction: () => void handlePlanAction(planCard.id),
        };
    });

    return (
        <StudioShell onNavigate={onNavigate}>
            <MembershipPremiumView
                currentStatus={currentStatus}
                plans={plans}
                includedCredits={Number(monthlyCreditsRemaining || 0)}
                walletCredits={Number(topupCreditsRemaining || 0)}
                totalCredits={Number(creditsTotalRemaining || 0)}
                onBack={handleBack}
                onOpenBilling={handleOpenBilling}
                banners={banners}
                actionError={actionError}
            />
        </StudioShell>
    );
}

function capitalizePlan(planId: PublicPlanId) {
    return planId.charAt(0).toUpperCase() + planId.slice(1);
}
