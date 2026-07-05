import { ArrowLeft, CalendarClock, CreditCard, Gauge, WalletCards } from 'lucide-react';
import type { ReactNode } from 'react';

export default function MembershipPremiumView({
    currentStatus,
    creditBalance,
    monthlyCreditsRemaining,
    topupCreditsRemaining,
    nextRenewalUnix,
    membershipSource,
    onBack,
    onOpenBilling,
    banners,
    actionError,
}: {
    currentStatus: string;
    creditBalance: number;
    monthlyCreditsRemaining: number;
    topupCreditsRemaining: number;
    nextRenewalUnix: number;
    membershipSource: string;
    onBack: () => void;
    onOpenBilling: () => void;
    banners: ReactNode;
    actionError: string;
}) {
    const renewalLabel = nextRenewalUnix > 0
        ? new Date(nextRenewalUnix * 1000).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })
        : 'Not scheduled';
    const sourceLabel = membershipSource ? membershipSource.replace(/_/g, ' ') : 'None';

    return (
        <div className="mx-auto max-w-5xl space-y-6">
            {banners}
            {actionError && (
                <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                    {actionError}
                </div>
            )}

            <section className="rounded-lg border border-white/[0.08] bg-white/[0.03] p-6 shadow-2xl shadow-black/20 sm:p-8">
                <div className="flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
                    <div>
                        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300">Membership</p>
                        <h1 className="mt-3 text-3xl font-bold text-white">Your Studio plan</h1>
                        <p className="mt-3 max-w-2xl text-sm leading-6 text-gray-400">
                            This page only shows your current access and credit balance. Upgrades, top-ups, payment methods, and invoices stay inside Billing.
                        </p>
                        <div className="mt-5 flex flex-wrap gap-3">
                            <button
                                type="button"
                                onClick={onBack}
                                className="inline-flex items-center gap-2 rounded-lg border border-white/[0.08] bg-white/[0.02] px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:border-white/[0.14] hover:bg-white/[0.06]"
                            >
                                <ArrowLeft className="h-4 w-4" />
                                Back to Studio
                            </button>
                            <button
                                type="button"
                                onClick={onOpenBilling}
                                className="inline-flex items-center gap-2 rounded-lg bg-cyan-500 px-4 py-2.5 text-sm font-bold text-black transition hover:bg-cyan-300"
                            >
                                <CreditCard className="h-4 w-4" />
                                Open Billing
                            </button>
                        </div>
                    </div>
                    <div className="rounded-lg border border-cyan-400/20 bg-cyan-400/10 px-5 py-4">
                        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-200">Current plan</p>
                        <p className="mt-2 text-2xl font-black text-white">{currentStatus}</p>
                        <p className="mt-1 text-xs capitalize text-gray-400">{sourceLabel}</p>
                    </div>
                </div>
            </section>

            <section className="grid gap-3 md:grid-cols-3">
                <StatusTile icon={Gauge} label="Total credits" value={creditBalance >= 999999 ? 'Unlimited' : creditBalance.toLocaleString()} />
                <StatusTile icon={WalletCards} label="Monthly credits" value={monthlyCreditsRemaining >= 999999 ? 'Unlimited' : monthlyCreditsRemaining.toLocaleString()} />
                <StatusTile icon={WalletCards} label="Top-up credits" value={topupCreditsRemaining >= 999999 ? 'Unlimited' : topupCreditsRemaining.toLocaleString()} />
            </section>

            <section className="rounded-lg border border-white/[0.08] bg-white/[0.025] p-5">
                <div className="flex items-start gap-3">
                    <CalendarClock className="mt-0.5 h-5 w-5 text-violet-300" />
                    <div>
                        <p className="font-semibold text-white">Next renewal</p>
                        <p className="mt-1 text-sm text-gray-400">{renewalLabel}</p>
                    </div>
                </div>
            </section>
        </div>
    );
}

function StatusTile({ icon: Icon, label, value }: { icon: typeof Gauge; label: string; value: string }) {
    return (
        <div className="rounded-lg border border-white/[0.08] bg-white/[0.025] p-5">
            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.16em] text-gray-500">
                <Icon className="h-4 w-4 text-cyan-300" />
                {label}
            </div>
            <p className="mt-3 text-2xl font-bold text-white">{value}</p>
        </div>
    );
}
