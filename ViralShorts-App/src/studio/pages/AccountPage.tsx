import { useContext, useEffect, useMemo } from 'react';
import { Crown, Sparkles, User, WalletCards } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, STUDIO_SITE_URL, isBillingHost } from '../shared';

export default function AccountPage({ onNavigate }: { onNavigate: PageNav }) {
    const {
        session,
        role,
        ownerOverride,
        billingActive,
        membershipPlanId,
        studioLaneAccess,
        monthlyCreditsRemaining,
        topupCreditsRemaining,
        creditsTotalRemaining,
        requiresTopup,
        signOut,
    } = useContext(AuthContext);

    useEffect(() => {
        if (!session) onNavigate('auth');
    }, [session, onNavigate]);

    const laneEntries = useMemo(() => {
        const entries = [
            ['Create', Boolean(studioLaneAccess.create || ownerOverride)],
            ['Chat Story', Boolean(studioLaneAccess.chatstory || ownerOverride)],
            ['Thumbnails (beta)', Boolean(studioLaneAccess.thumbnails || ownerOverride)],
            ['Clone (beta)', Boolean(studioLaneAccess.clone || ownerOverride)],
            ['Long Form (beta)', Boolean(studioLaneAccess.longform || ownerOverride)],
            ['AutoClipper', Boolean(studioLaneAccess.autoclipper || ownerOverride)],
        ] as const;
        return entries;
    }, [ownerOverride, studioLaneAccess]);

    if (!session) return null;

    const normalizedPlan = String(membershipPlanId || '').trim().toLowerCase();
    const currentPlanLabel = ownerOverride
        ? 'Owner override (Pro)'
        : billingActive
            ? `Active (${normalizedPlan || 'starter'})`
            : 'Free';

    const handleOpenBilling = () => {
        if (isBillingHost) {
            window.location.href = `${window.location.origin}?page=billing`;
            return;
        }
        window.location.href = `${BILLING_SITE_URL}?page=billing`;
    };

    const handleOpenMembership = () => {
        if (isBillingHost) {
            window.location.href = `${window.location.origin}?page=subscription`;
            return;
        }
        window.location.href = `${STUDIO_SITE_URL}?page=subscription`;
    };

    return (
        <>
            <NavBar onNavigate={onNavigate} active="account" />
            <div className="mx-auto max-w-6xl px-6 pt-24 pb-10">
                <div className="grid gap-6 lg:grid-cols-[0.9fr,1.1fr]">
                    <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                        <div className="flex items-center gap-4">
                            <div className="flex h-14 w-14 items-center justify-center rounded-full bg-violet-500/10">
                                <User className="h-7 w-7 text-violet-300" />
                            </div>
                            <div>
                                <p className="text-lg font-bold text-white">{session.user.email}</p>
                                <div className="mt-1 flex items-center gap-2 text-sm">
                                    {ownerOverride || role === 'admin' ? (
                                        <>
                                            <Crown className="h-4 w-4 text-emerald-300" />
                                            <span className="font-medium text-emerald-300">Owner override active</span>
                                        </>
                                    ) : (
                                        <>
                                            <Sparkles className="h-4 w-4 text-cyan-300" />
                                            <span className="font-medium text-cyan-300">Catalyst account</span>
                                        </>
                                    )}
                                </div>
                            </div>
                        </div>

                        <div className="mt-6 grid gap-3">
                            <OverviewCard label="Current Plan" value={currentPlanLabel} />
                            <OverviewCard label="Included Credits" value={String(Number(monthlyCreditsRemaining || 0))} />
                            <OverviewCard label="Credit Wallet" value={String(Number(topupCreditsRemaining || 0))} />
                            <OverviewCard label="Total Available" value={String(Number(creditsTotalRemaining || 0))} />
                        </div>

                        <div className="mt-6 flex flex-wrap gap-3">
                            <button
                                type="button"
                                onClick={handleOpenMembership}
                                className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500"
                            >
                                Membership
                            </button>
                            <button
                                type="button"
                                onClick={handleOpenBilling}
                                className="inline-flex items-center gap-2 rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:border-white/[0.14] hover:bg-white/[0.06]"
                            >
                                <WalletCards className="h-4 w-4" />
                                Billing
                            </button>
                        </div>

                        <p className={`mt-4 text-sm ${requiresTopup ? 'text-amber-300' : 'text-emerald-300'}`}>
                            {ownerOverride
                                ? 'Owner account bypass is active on this workspace.'
                                : requiresTopup
                                    ? 'Top up the credit wallet before your next animation-heavy run.'
                                    : 'Your account is ready for Catalyst jobs.'}
                        </p>
                    </section>

                    <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                        <h2 className="text-xl font-bold text-white">Lane Access</h2>
                        <p className="mt-2 text-sm text-gray-400">
                            Studio now uses one normalized access model: membership, wallet, included credits, and lane-level access. Public plans are short-form only right now while the heavier lanes stay in beta.
                        </p>

                        <div className="mt-6 grid gap-3 sm:grid-cols-2">
                            {laneEntries.map(([label, enabled]) => (
                                <div
                                    key={label}
                                    className={`rounded-2xl border px-4 py-3 text-sm ${
                                        enabled
                                            ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-100'
                                            : 'border-white/[0.08] bg-black/20 text-gray-400'
                                    }`}
                                >
                                    <p className="font-semibold">{label}</p>
                                    <p className="mt-1 text-xs opacity-80">{enabled ? 'Available on this account' : 'Not enabled yet'}</p>
                                </div>
                            ))}
                        </div>

                        <div className="mt-6 rounded-2xl border border-white/[0.08] bg-black/20 p-4 text-sm text-gray-300">
                            <p className="font-semibold text-white">Billing Behavior</p>
                            <p className="mt-2">Included credits burn first when membership is active.</p>
                            <p className="mt-1">Credit wallet remains available for overflow and usage-only accounts.</p>
                            <p className="mt-1">If membership expires, wallet credits stay attached to the account.</p>
                        </div>

                        <button
                            onClick={signOut}
                            className="mt-6 w-full rounded-xl border border-red-500/20 bg-red-500/10 py-3 text-sm font-medium text-red-300 transition hover:bg-red-500/20"
                        >
                            Sign Out
                        </button>
                    </section>
                </div>
            </div>
        </>
    );
}

function OverviewCard({ label, value }: { label: string; value: string }) {
    return (
        <div className="rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3">
            <p className="text-[10px] uppercase tracking-[0.18em] text-gray-500">{label}</p>
            <p className="mt-2 text-lg font-bold text-white">{value}</p>
        </div>
    );
}
