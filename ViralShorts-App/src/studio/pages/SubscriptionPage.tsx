import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import MembershipPremiumView from '../components/membership/MembershipPremiumView';
import StudioShell from '../components/layout/StudioShell';
import { UNIFIED_PLANS, type UnifiedPlanId } from '../lib/studioProduct';
import { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, STUDIO_SITE_URL, isBillingHost, resolveStudioBackendUrl } from '../shared';
import { trackMembershipPurchaseCompleted, trackOnce } from '../lib/googleAds';
import {
    BILLING_CHECKOUT_STARTED_EVENT,
    BILLING_CHECKOUT_STATE_EVENT,
    beginBillingCheckout,
    clearPendingBillingCheckout,
    readPendingBillingCheckout,
    type BillingCheckoutSyncStatus,
} from '../lib/billingCheckoutSync';

export default function SubscriptionPage({ onNavigate }: { onNavigate: PageNav }) {
    const {
        session,
        billingActive,
        membershipPlanId,
        membershipSource,
        creditsTotalRemaining,
        nextRenewalSource,
        checkout,
        manageBilling,
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
    const [unifiedBalance, setUnifiedBalance] = useState<number | null>(null);
    const [stripeCheckoutSync, setStripeCheckoutSync] = useState<{
        status: BillingCheckoutSyncStatus | 'idle';
        startedAt: number;
    }>(() => {
        const pending = readPendingBillingCheckout();
        if (pending?.kind === 'subscription') return { status: 'pending', startedAt: pending.startedAt };
        if (subscriptionResult === 'success') return { status: 'pending', startedAt: 0 };
        return { status: 'idle', startedAt: 0 };
    });
    const normalizedMembershipSource = String(membershipSource || nextRenewalSource || '').trim().toLowerCase();
    const usesStripeMembership = billingActive && normalizedMembershipSource === 'stripe';
    const normalizedCurrentPlan = useMemo<UnifiedPlanId | ''>(() => {
        const raw = String(membershipPlanId || '').trim().toLowerCase();
        const alias = raw === 'creator' ? 'studio_pro_2500' : raw === 'studio' ? 'studio_pro_11k' : raw;
        if (UNIFIED_PLANS.some((p) => p.id === alias)) return alias as UnifiedPlanId;
        return '';
    }, [membershipPlanId]);

    const planCards = useMemo(
        () =>
            UNIFIED_PLANS.map((p) => ({
                id: p.id,
                title: p.title,
                priceLabel: `$${p.priceUsd}/mo`,
                subtitle: p.description,
                bullets: p.features,
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
                /* fallback */
            }
        })();
        return () => {
            cancelled = true;
        };
    }, [session]);

    useEffect(() => {
        const handleStarted = (event: Event) => {
            const pending = (event as CustomEvent).detail;
            if (pending?.kind !== 'subscription') return;
            setStripeCheckoutSync({ status: 'pending', startedAt: Number(pending.startedAt || 0) });
        };
        const handleState = (event: Event) => {
            const detail = (event as CustomEvent).detail;
            if (detail?.pending?.kind !== 'subscription') return;
            setStripeCheckoutSync({
                status: detail.status || 'pending',
                startedAt: Number(detail.pending.startedAt || 0),
            });
        };
        window.addEventListener(BILLING_CHECKOUT_STARTED_EVENT, handleStarted);
        window.addEventListener(BILLING_CHECKOUT_STATE_EVENT, handleState);
        return () => {
            window.removeEventListener(BILLING_CHECKOUT_STARTED_EVENT, handleStarted);
            window.removeEventListener(BILLING_CHECKOUT_STATE_EVENT, handleState);
        };
    }, []);

    useEffect(() => {
        if (stripeCheckoutSync.status !== 'confirmed') return;
        const url = new URL(window.location.href);
        url.searchParams.delete('subscription');
        if (url.searchParams.get('provider') === 'stripe') url.searchParams.delete('provider');
        window.history.replaceState(window.history.state, '', `${url.pathname}${url.search}${url.hash}`);
    }, [stripeCheckoutSync.status]);

    useEffect(() => {
        const confirmed = stripeCheckoutSync.status === 'confirmed';
        if (!confirmed) return;
        const planId = requestedPlanId || normalizedCurrentPlan || 'studio_pro_1k';
        const match = UNIFIED_PLANS.find((p) => p.id === planId);
        trackOnce(`subscription_membership_confirmed:${stripeCheckoutSync.startedAt}:${planId}`, () => {
            trackMembershipPurchaseCompleted(planId, match?.priceUsd || 0);
        });
    }, [normalizedCurrentPlan, requestedPlanId, stripeCheckoutSync]);

    const currentStatus = billingActive && normalizedCurrentPlan
        ? UNIFIED_PLANS.find((p) => p.id === normalizedCurrentPlan)?.title || 'Active'
        : 'No plan';

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

    const handlePlanAction = useCallback(
        async (planId: UnifiedPlanId) => {
            if (!session) {
                onNavigate('auth');
                return;
            }
            setActionError('');
            setLoadingPlanId(planId);
            try {
                const startPlanCheckout = async () => {
                    beginBillingCheckout({ kind: 'subscription', expectedPlanId: planId });
                    const err = await checkout(planId);
                    if (err) clearPendingBillingCheckout(readPendingBillingCheckout());
                    return err;
                };
                if (billingActive && usesStripeMembership) {
                    const err = await manageBilling();
                    if (err) setActionError(err);
                    return;
                }
                const err = await startPlanCheckout();
                if (err) setActionError(err);
            } finally {
                setLoadingPlanId('');
            }
        },
        [billingActive, checkout, manageBilling, onNavigate, session, usesStripeMembership],
    );

    const banners = (
        <>
            {stripeCheckoutSync.status === 'confirmed' && (
                <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-100">
                    Your plan is active. Monthly credits have been added to your wallet.
                </div>
            )}
            {stripeCheckoutSync.status === 'pending' && (
                <div className="rounded-xl border border-sky-500/20 bg-sky-500/10 px-4 py-3 text-sm text-sky-100">
                    Checkout returned. Waiting for Studio to confirm the payment and refresh your account...
                </div>
            )}
            {stripeCheckoutSync.status === 'timed_out' && (
                <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                    Payment is not confirmed in Studio yet. No credits or plan access are being claimed; refocus or reopen the app to check again.
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
        const isCurrent = billingActive && normalizedCurrentPlan === planCard.id;
        const actionLabel = isCurrent
            ? 'Manage plan'
            : billingActive
                ? `Switch to ${planCard.title}`
                : `Start ${planCard.title}`;
        return {
            ...planCard,
            isCurrent,
            actionLabel,
            loading: loadingPlanId === planCard.id,
            disabled: false,
            onAction: () => void handlePlanAction(planCard.id),
        };
    });

    const creditBalance = unifiedBalance ?? Number(creditsTotalRemaining || 0);

    return (
        <StudioShell onNavigate={onNavigate}>
            <MembershipPremiumView
                currentStatus={currentStatus}
                plans={plans}
                creditBalance={creditBalance}
                onBack={handleBack}
                onOpenBilling={handleOpenBilling}
                banners={banners}
                actionError={actionError}
            />
        </StudioShell>
    );
}
