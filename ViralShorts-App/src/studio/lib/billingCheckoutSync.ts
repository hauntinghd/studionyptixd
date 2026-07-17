export type BillingCheckoutKind = 'subscription' | 'topup';

export type PendingBillingCheckout = {
    kind: BillingCheckoutKind;
    provider: 'stripe';
    startedAt: number;
    expectedPlanId?: string;
    expectedCredits?: number;
    baselineBalance?: number;
};

export type BillingCheckoutSyncStatus = 'pending' | 'confirmed' | 'timed_out' | 'error';

export type BillingCheckoutSnapshot = {
    viewerVerified?: boolean;
    billingActive?: boolean;
    membershipPlanId?: string;
    balance?: number;
    recent?: Array<{
        type?: string;
        credits?: number;
        reason?: string;
        ts?: number;
    }>;
};

export const BILLING_CHECKOUT_STARTED_EVENT = 'nyptid:billing-checkout-started';
export const BILLING_CHECKOUT_STATE_EVENT = 'nyptid:billing-checkout-state';
export const BILLING_VIEWER_REFRESH_EVENT = 'nyptid:viewer-state-refresh';

const STORAGE_KEY = 'nyptid:pending-billing-checkout:v1';
const MAX_PENDING_AGE_MS = 30 * 60 * 1000;

const normalizePlanId = (raw: unknown) => {
    const value = String(raw || '').trim().toLowerCase();
    if (value === 'creator') return 'studio_pro_2500';
    if (value === 'studio') return 'studio_pro_11k';
    return value;
};

export function readPendingBillingCheckout(): PendingBillingCheckout | null {
    if (typeof window === 'undefined') return null;
    try {
        const parsed = JSON.parse(window.localStorage.getItem(STORAGE_KEY) || 'null') as PendingBillingCheckout | null;
        if (!parsed || parsed.provider !== 'stripe' || !['subscription', 'topup'].includes(parsed.kind)) return null;
        if (!Number.isFinite(parsed.startedAt) || Date.now() - parsed.startedAt > MAX_PENDING_AGE_MS) {
            window.localStorage.removeItem(STORAGE_KEY);
            return null;
        }
        return parsed;
    } catch {
        window.localStorage.removeItem(STORAGE_KEY);
        return null;
    }
}

export function beginBillingCheckout(input: Omit<PendingBillingCheckout, 'provider' | 'startedAt'>) {
    if (typeof window === 'undefined') return;
    const pending: PendingBillingCheckout = {
        ...input,
        provider: 'stripe',
        startedAt: Date.now(),
    };
    try {
        window.localStorage.setItem(STORAGE_KEY, JSON.stringify(pending));
    } catch {
        // The in-memory event still lets the current desktop session reconcile.
    }
    window.dispatchEvent(new CustomEvent(BILLING_CHECKOUT_STARTED_EVENT, { detail: pending }));
}

export function clearPendingBillingCheckout(pending?: PendingBillingCheckout | null) {
    if (typeof window === 'undefined') return;
    const current = readPendingBillingCheckout();
    if (pending && current && current.startedAt !== pending.startedAt) return;
    try {
        window.localStorage.removeItem(STORAGE_KEY);
    } catch {
        // Ignore storage cleanup failures.
    }
}

export function checkoutIsConfirmed(
    pending: PendingBillingCheckout,
    snapshot: BillingCheckoutSnapshot,
): boolean {
    if (!snapshot.viewerVerified) return false;
    if (pending.kind === 'subscription') {
        if (!snapshot.billingActive) return false;
        const expected = normalizePlanId(pending.expectedPlanId);
        const actual = normalizePlanId(snapshot.membershipPlanId);
        return !expected || expected === actual;
    }

    const expectedCredits = Math.max(0, Number(pending.expectedCredits || 0));
    const startedAtSeconds = pending.startedAt / 1000;
    const matchingLedgerCredit = (snapshot.recent || []).some((entry) => (
        String(entry.type || '').toLowerCase() === 'credit'
        && String(entry.reason || '').toLowerCase() === 'stripe_topup'
        && Number(entry.ts || 0) >= startedAtSeconds - 5
        && Number(entry.credits || 0) >= expectedCredits
    ));
    if (matchingLedgerCredit) return true;

    const baseline = Number(pending.baselineBalance);
    const balance = Number(snapshot.balance);
    return expectedCredits > 0
        && Number.isFinite(baseline)
        && Number.isFinite(balance)
        && balance >= baseline + expectedCredits;
}

export function emitBillingCheckoutState(
    status: BillingCheckoutSyncStatus,
    pending: PendingBillingCheckout,
    snapshot?: BillingCheckoutSnapshot,
    error?: string,
) {
    if (typeof window === 'undefined') return;
    window.dispatchEvent(new CustomEvent(BILLING_CHECKOUT_STATE_EVENT, {
        detail: { status, pending, snapshot, error: String(error || '') },
    }));
}
