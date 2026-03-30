import { useCallback, useContext, useMemo, useState } from 'react';
import { ArrowLeft, BadgeCheck, Sparkles } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, STUDIO_SITE_URL, isBillingHost } from '../shared';

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
        defaultMembershipPlanId,
    } = useContext(AuthContext);
    const params = useMemo(() => {
        if (typeof window === 'undefined') return new URLSearchParams();
        return new URLSearchParams(window.location.search);
    }, []);
    const subscriptionResult = String(params.get('subscription') || '').trim().toLowerCase();
    const subscriptionError = String(params.get('error') || '').trim();
    const [actionError, setActionError] = useState('');
    const [loading, setLoading] = useState(false);

    const membershipPrice = useMemo(() => {
        const raw = Number((publicPlanPrices as Record<string, number>)[defaultMembershipPlanId || 'starter']);
        if (!Number.isFinite(raw) || raw <= 0) return '$14/mo';
        return `$${raw.toFixed(raw % 1 === 0 ? 0 : 2)}/mo`;
    }, [defaultMembershipPlanId, publicPlanPrices]);
    const normalizedMembershipSource = String(membershipSource || nextRenewalSource || '').trim().toLowerCase();
    const usesStripeMembership = billingActive && normalizedMembershipSource === 'stripe';
    const usesManualPayPalMembership = billingActive && normalizedMembershipSource === 'paypal_manual';
    const starterLimits = (publicPlanLimits as Record<string, any>)[defaultMembershipPlanId || 'starter'] || {};
    const includedAnimatedCredits = Number(starterLimits.animated_renders_per_month || 0);
    const membershipMaxDurationMinutes = Math.max(1, Math.round(Number(starterLimits.max_duration_sec || 0) / 60));
    const membershipResolution = String(starterLimits.max_resolution || '720p').toUpperCase();
    const membershipStatus = billingActive
        ? `Active on ${membershipPlanId || defaultMembershipPlanId || 'starter'}`
        : 'Inactive';

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

    const handleMembershipAction = useCallback(async () => {
        if (!session) {
            onNavigate('auth');
            return;
        }
        setActionError('');
        setLoading(true);
        try {
            if (usesStripeMembership) {
                const err = await manageBilling();
                if (err) setActionError(err);
                return;
            }
            const err = billingActive && !usesManualPayPalMembership
                ? await manageBilling()
                : await checkout('membership');
            if (err) setActionError(err);
        } finally {
            setLoading(false);
        }
    }, [billingActive, checkout, manageBilling, onNavigate, session, usesManualPayPalMembership, usesStripeMembership]);

    return (
        <>
            <NavBar onNavigate={onNavigate} active="subscription" />
            <div className="mx-auto max-w-6xl px-6 pt-24 pb-12">
                <div className="mb-8 flex flex-wrap items-center justify-between gap-4">
                    <div>
                        <div className="flex items-center gap-2 text-violet-300 text-sm font-semibold uppercase tracking-[0.18em]">
                            <Sparkles className="h-4 w-4" />
                            Membership
                        </div>
                        <h1 className="mt-3 text-3xl font-bold text-white">Catalyst Membership</h1>
                        <p className="mt-2 max-w-3xl text-sm text-gray-400">
                            One membership unlocks the public Studio lanes. Included credits burn before the wallet so you can run membership-only, usage-only, or hybrid accounts without changing products.
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
                            onClick={handleOpenBilling}
                            className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500"
                        >
                            Open Billing
                        </button>
                    </div>
                </div>

                <div className="grid gap-6 lg:grid-cols-[1.1fr,0.9fr]">
                    <section className="rounded-3xl border border-violet-500/20 bg-violet-500/[0.06] p-6">
                        <div className="flex flex-wrap items-start justify-between gap-4">
                            <div>
                                <p className="text-xs uppercase tracking-[0.18em] text-violet-200/70">Current Status</p>
                                <h2 className="mt-2 text-3xl font-bold text-white">{membershipStatus}</h2>
                                <p className="mt-3 max-w-2xl text-sm text-gray-300">
                                    Membership is the simple unlock layer for the public Catalyst lanes: Create, Thumbnails, Clone, Long Form, and Chat Story. Wallet credits remain available for heavier render usage.
                                </p>
                            </div>
                            <div className="rounded-2xl border border-white/[0.08] bg-black/20 px-5 py-4 text-right">
                                <p className="text-[10px] uppercase tracking-[0.18em] text-gray-500">Price</p>
                                <p className="mt-2 text-3xl font-bold text-white">{membershipPrice}</p>
                            </div>
                        </div>

                        <div className="mt-6 grid gap-3 md:grid-cols-2">
                            {[
                                'Unlocks Create, Thumbnails, Clone, Long Form, and Chat Story',
                                includedAnimatedCredits > 0
                                    ? `${includedAnimatedCredits} included animation credits reset every billing cycle`
                                    : 'Included animation credits reset every billing cycle',
                                `${membershipResolution} output cap with up to ${membershipMaxDurationMinutes} minute jobs on the Starter unlock`,
                                'Wallet top-ups still stack for hybrid accounts',
                                'PayPal membership renewals are manual for now',
                            ].map((feature) => (
                                <div key={feature} className="flex items-start gap-2 rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3 text-sm text-gray-300">
                                    <BadgeCheck className="mt-0.5 h-4 w-4 shrink-0 text-emerald-300" />
                                    <span>{feature}</span>
                                </div>
                            ))}
                        </div>

                        <button
                            type="button"
                            onClick={() => void handleMembershipAction()}
                            className="mt-6 rounded-xl bg-white/[0.1] px-5 py-3 text-sm font-semibold text-white transition hover:bg-white/[0.16]"
                        >
                            {loading
                                ? 'Opening...'
                                : usesManualPayPalMembership
                                    ? 'Extend Membership'
                                    : usesStripeMembership
                                        ? 'Manage Membership'
                                        : billingActive
                                            ? 'Membership Details'
                                        : 'Start Membership'}
                        </button>
                    </section>

                    <aside className="space-y-6">
                        <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                            <h2 className="text-lg font-semibold text-white">Balance</h2>
                            <div className="mt-4 grid gap-3">
                                <MetricCard label="Included Credits" value={Number(monthlyCreditsRemaining || 0)} />
                                <MetricCard label="Credit Wallet" value={Number(topupCreditsRemaining || 0)} />
                                <MetricCard label="Total Available" value={Number(creditsTotalRemaining || 0)} />
                            </div>
                        </section>
                        <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                            <h2 className="text-lg font-semibold text-white">How Billing Works</h2>
                            <div className="mt-4 space-y-3 text-sm text-gray-300">
                                <p>1. Membership unlocks the public Studio lanes.</p>
                                <p>2. Included credits burn first on eligible jobs.</p>
                                <p>3. Wallet credits cover overflow and usage-only accounts.</p>
                                <p>4. If membership expires, wallet credits stay on the account.</p>
                            </div>
                        </section>
                    </aside>
                </div>

                {actionError && (
                    <p className="mt-6 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-5 py-4 text-sm text-amber-100">
                        {actionError}
                    </p>
                )}
                {subscriptionResult === 'success' && (
                    <p className="mt-6 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-5 py-4 text-sm text-emerald-100">
                        Catalyst Membership is active. Included credits will now burn before the wallet.
                    </p>
                )}
                {subscriptionResult === 'manual' && (
                    <p className="mt-6 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-5 py-4 text-sm text-cyan-100">
                        This account is on manual PayPal renewal. Click the membership button again whenever you want to extend another month.
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

function MetricCard({ label, value }: { label: string; value: number }) {
    return (
        <div className="rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3">
            <p className="text-[10px] uppercase tracking-[0.18em] text-gray-500">{label}</p>
            <p className="mt-2 text-2xl font-bold text-white">{value}</p>
        </div>
    );
}
