import { useContext, useEffect, useMemo } from 'react';
import { ArrowLeft, CheckCircle2, LogOut, User, WalletCards } from 'lucide-react';
import StudioShell from '../components/layout/StudioShell';
import { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, isBillingHost } from '../shared';

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
            ['Thumbnails', Boolean(studioLaneAccess.thumbnails || ownerOverride)],
            ['Clone', Boolean(studioLaneAccess.clone || ownerOverride)],
            ['Long Form', Boolean(studioLaneAccess.longform || ownerOverride)],
            ['AutoClipper', Boolean(studioLaneAccess.autoclipper || ownerOverride)],
        ] as const;
        return entries;
    }, [ownerOverride, studioLaneAccess]);

    if (!session) return null;

    const normalizedPlan = String(membershipPlanId || '').trim().toLowerCase();
    const currentPlanLabel = ownerOverride
        ? 'Owner override (Pro)'
        : billingActive
            ? capitalize(normalizedPlan || 'starter')
            : 'Free';

    const handleOpenBilling = () => {
        if (isBillingHost) {
            window.location.href = `${window.location.origin}?page=billing`;
            return;
        }
        window.location.href = `${BILLING_SITE_URL}?page=billing`;
    };

    const handleOpenMembership = () => onNavigate('subscription');

    const handleBack = () => onNavigate('dashboard');

    return (
        <StudioShell onNavigate={onNavigate}>
            <div className="mx-auto max-w-6xl space-y-8">
                <section className="relative overflow-hidden rounded-2xl border border-white/[0.08] bg-gradient-to-br from-violet-950/30 via-[#0c0c10] to-cyan-950/20 p-6 sm:p-8">
                    <div className="relative z-10 flex flex-wrap items-start justify-between gap-6">
                        <div className="flex items-start gap-4">
                            <div className="flex h-14 w-14 items-center justify-center rounded-2xl border border-white/[0.08] bg-white/[0.04]">
                                <User className="h-7 w-7 text-violet-300" />
                            </div>
                            <div>
                                <p className="text-xs font-semibold uppercase tracking-[0.2em] text-violet-300">Account</p>
                                <h1 className="mt-2 text-2xl font-bold text-white sm:text-3xl">{session.user.email}</h1>
                                <p className="mt-2 text-sm text-gray-400">
                                    {ownerOverride || role === 'admin'
                                        ? 'Owner override — all lanes open on this workspace.'
                                        : requiresTopup
                                            ? 'Top up before your next animation-heavy run.'
                                            : 'Ready for Create and Chat Story jobs.'}
                                </p>
                            </div>
                        </div>
                        <button
                            type="button"
                            onClick={handleBack}
                            className="inline-flex items-center gap-2 rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:border-white/[0.14] hover:bg-white/[0.06]"
                        >
                            <ArrowLeft className="h-4 w-4" />
                            Back to Studio
                        </button>
                    </div>
                </section>

                <div className="grid gap-6 lg:grid-cols-[0.95fr,1.05fr]">
                    <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-6">
                        <h2 className="text-lg font-bold text-white">Credits & plan</h2>
                        <div className="mt-4 grid gap-3 sm:grid-cols-2">
                            <Stat label="Plan" value={currentPlanLabel} />
                            <Stat label="Total available" value={String(Number(creditsTotalRemaining || 0))} accent />
                            <Stat label="Included credits" value={String(Number(monthlyCreditsRemaining || 0))} />
                            <Stat label="Credit wallet" value={String(Number(topupCreditsRemaining || 0))} />
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
                    </section>

                    <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-6">
                        <h2 className="text-lg font-bold text-white">Lane access</h2>
                        <p className="mt-2 text-sm text-gray-500">
                            One model: membership, wallet, included credits, and per-lane gates.
                        </p>
                        <div className="mt-4 grid gap-2 sm:grid-cols-2">
                            {laneEntries.map(([label, enabled]) => (
                                <div
                                    key={label}
                                    className={`flex items-center justify-between rounded-xl border px-4 py-3 text-sm ${
                                        enabled
                                            ? 'border-emerald-500/20 bg-emerald-500/[0.06] text-emerald-100'
                                            : 'border-white/[0.06] bg-black/20 text-gray-500'
                                    }`}
                                >
                                    <span className="font-medium">{label}</span>
                                    {enabled ? (
                                        <CheckCircle2 className="h-4 w-4 text-emerald-400" />
                                    ) : (
                                        <span className="text-[10px] uppercase tracking-wider">Locked</span>
                                    )}
                                </div>
                            ))}
                        </div>
                        <div className="mt-6 rounded-xl border border-white/[0.06] bg-black/20 p-4 text-sm text-gray-400">
                            <p className="font-semibold text-white">Billing behavior</p>
                            <p className="mt-2">Included credits burn first when membership is active.</p>
                            <p className="mt-1">Wallet credits persist if membership expires.</p>
                        </div>
                        <button
                            onClick={signOut}
                            className="mt-6 inline-flex w-full items-center justify-center gap-2 rounded-xl border border-red-500/20 bg-red-500/10 py-3 text-sm font-medium text-red-300 transition hover:bg-red-500/20"
                        >
                            <LogOut className="h-4 w-4" />
                            Sign out
                        </button>
                    </section>
                </div>
            </div>
        </StudioShell>
    );
}

function Stat({ label, value, accent }: { label: string; value: string; accent?: boolean }) {
    return (
        <div className={`rounded-xl border px-4 py-3 ${accent ? 'border-violet-500/25 bg-violet-500/10' : 'border-white/[0.08] bg-black/20'}`}>
            <p className="text-[10px] uppercase tracking-[0.18em] text-gray-500">{label}</p>
            <p className="mt-2 text-lg font-bold text-white">{value}</p>
        </div>
    );
}

function capitalize(s: string) {
    if (!s) return s;
    return s.charAt(0).toUpperCase() + s.slice(1);
}
