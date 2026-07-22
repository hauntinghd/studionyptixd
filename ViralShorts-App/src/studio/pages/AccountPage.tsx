import { useContext, useEffect, useMemo, useState } from 'react';
import { ArrowLeft, CheckCircle2, CreditCard, Gauge, Globe2, LogOut, Save, ShieldCheck, User, WalletCards } from 'lucide-react';
import StudioShell from '../components/layout/StudioShell';
import { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, isBillingHost } from '../shared';
import { loadStudioHubState, patchStudioHubState } from '../lib/studioHubState';

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
    const [website, setWebsite] = useState('');
    const [profileStatus, setProfileStatus] = useState('');
    const accessToken = session?.access_token || '';

    useEffect(() => {
        if (!session) onNavigate('auth');
    }, [session, onNavigate]);

    useEffect(() => {
        if (!accessToken) return;
        let cancelled = false;
        loadStudioHubState(accessToken)
            .then((state) => {
                if (cancelled) return;
                setWebsite(String(state.profile?.website || ''));
            })
            .catch(() => {});
        return () => {
            cancelled = true;
        };
    }, [accessToken]);

    const laneEntries = useMemo(() => {
        return [
            ['Create', Boolean(studioLaneAccess.create || ownerOverride)],
            ['Studio Agent', Boolean(studioLaneAccess.agent || ownerOverride)],
            ['Thumbnails', Boolean(studioLaneAccess.thumbnails || ownerOverride)],
            ['Long Form', Boolean(studioLaneAccess.longform || ownerOverride)],
            ['Chat Story', Boolean(studioLaneAccess.chatstory || ownerOverride)],
            ['Clone', Boolean(studioLaneAccess.clone || ownerOverride)],
            ['AutoClipper', Boolean(studioLaneAccess.autoclipper || ownerOverride)],
        ] as const;
    }, [ownerOverride, studioLaneAccess]);

    if (!session) return null;

    const normalizedPlan = String(membershipPlanId || '').trim().toLowerCase();
    const currentPlanLabel = ownerOverride
        ? 'Owner override'
        : billingActive
            ? normalizedPlan === 'creator'
                ? 'Studio'
                : normalizedPlan === 'studio'
                    ? 'Studio Pro'
                    : capitalize(normalizedPlan || 'starter')
            : 'Free';
    const totalCredits = Number(creditsTotalRemaining || 0);

    const handleOpenBilling = () => {
        if (isBillingHost) {
            window.location.href = `${window.location.origin}?page=billing`;
            return;
        }
        window.location.href = `${BILLING_SITE_URL}?page=billing`;
    };

    const saveProfile = async () => {
        setProfileStatus('');
        try {
            const cleanWebsite = normalizeWebsite(website);
            if (accessToken) {
                await patchStudioHubState(accessToken, { profile: { website: cleanWebsite } });
            }
            setWebsite(cleanWebsite);
            setProfileStatus('Product website saved. Studio Agent can use it when you say “my website.”');
        } catch (e: any) {
            setProfileStatus(e?.message || 'Website save failed.');
        }
    };

    return (
        <StudioShell onNavigate={onNavigate}>
            <div className="mx-auto max-w-7xl space-y-8">
                <section className="relative overflow-hidden rounded-2xl border border-white/[0.08] bg-[radial-gradient(circle_at_14%_0%,rgba(124,58,237,0.22),transparent_34%),linear-gradient(135deg,rgba(46,16,101,0.32),rgba(9,9,11,0.97)_48%,rgba(8,47,73,0.25))] p-6 shadow-2xl shadow-black/30 sm:p-8">
                    <div className="relative z-10 flex flex-wrap items-start justify-between gap-6">
                        <div className="flex min-w-0 items-start gap-4">
                            <div className="flex h-14 w-14 shrink-0 items-center justify-center rounded-2xl border border-white/[0.1] bg-white/[0.05]">
                                <User className="h-7 w-7 text-violet-300" />
                            </div>
                            <div className="min-w-0">
                                <p className="text-xs font-semibold uppercase tracking-[0.22em] text-violet-300">Account</p>
                                <h1 className="mt-2 break-words text-2xl font-bold text-white sm:text-4xl">{session.user.email}</h1>
                                <p className="mt-2 text-sm text-gray-400">
                                    {ownerOverride || role === 'admin'
                                        ? 'Owner workspace - unlimited testing and all lanes open.'
                                        : requiresTopup
                                            ? 'Top up before your next animation-heavy run.'
                                            : 'Workspace is ready for production runs.'}
                                </p>
                            </div>
                        </div>
                        <div className="flex flex-wrap items-center gap-3">
                            <AccountPill icon={ShieldCheck} label="Plan" value={currentPlanLabel} />
                            <AccountPill icon={Gauge} label="Credits" value={totalCredits >= 999999 ? 'Unlimited' : totalCredits.toLocaleString()} />
                            <button
                                type="button"
                                onClick={() => onNavigate('dashboard')}
                                className="inline-flex items-center gap-2 rounded-xl border border-white/[0.08] bg-white/[0.03] px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:border-white/[0.14] hover:bg-white/[0.06]"
                            >
                                <ArrowLeft className="h-4 w-4" />
                                Back to Studio
                            </button>
                        </div>
                    </div>
                </section>

                <div className="grid gap-6 lg:grid-cols-[0.9fr,1.1fr]">
                    <section className="rounded-2xl border border-white/[0.08] bg-gradient-to-br from-white/[0.04] to-white/[0.015] p-6 shadow-sm shadow-black/30 lg:col-span-2">
                        <div className="flex items-start justify-between gap-4">
                            <div>
                                <h2 className="text-lg font-bold text-white">Product website</h2>
                                <p className="mt-1 max-w-3xl text-sm text-gray-500">
                                    Save the website Studio Agent should inspect when you ask it to make a product demo or advertisement from “my website.”
                                </p>
                            </div>
                            <Globe2 className="h-5 w-5 text-cyan-300" />
                        </div>
                        <div className="mt-5 flex flex-col gap-3 sm:flex-row sm:items-end">
                            <label className="block">
                                <span className="text-xs uppercase tracking-wider text-gray-500">Website URL</span>
                                <input
                                    value={website}
                                    onChange={(e) => setWebsite(e.target.value)}
                                    placeholder="https://yourproduct.com"
                                    inputMode="url"
                                    className="mt-1 h-12 w-full min-w-0 rounded-xl border border-white/[0.08] bg-black/30 px-4 text-sm text-white outline-none placeholder:text-gray-600 focus:border-cyan-400/40 sm:min-w-[520px]"
                                />
                            </label>
                            <button
                                type="button"
                                onClick={() => void saveProfile()}
                                className="inline-flex h-12 items-center justify-center gap-2 rounded-xl bg-cyan-600 px-5 text-sm font-semibold text-white transition hover:bg-cyan-500"
                            >
                                <Save className="h-4 w-4" />
                                Save website
                            </button>
                        </div>
                        {profileStatus && <p className="mt-3 text-sm text-cyan-200">{profileStatus}</p>}
                    </section>

                    <section className="rounded-2xl border border-white/[0.08] bg-gradient-to-br from-white/[0.04] to-white/[0.015] p-6 shadow-sm shadow-black/30">
                        <div className="flex items-start justify-between gap-4">
                            <div>
                                <h2 className="text-lg font-bold text-white">Wallet & plan</h2>
                                <p className="mt-1 text-sm text-gray-500">One balance across direct Anthropic, fal.ai media, and Studio production runs.</p>
                            </div>
                            <WalletCards className="h-5 w-5 text-cyan-300" />
                        </div>
                        <div className="mt-5 grid gap-3 sm:grid-cols-2">
                            <Stat label="Plan" value={currentPlanLabel} />
                            <Stat label="Total available" value={totalCredits >= 999999 ? 'Unlimited' : totalCredits.toLocaleString()} accent />
                            <Stat label="Included credits" value={Number(monthlyCreditsRemaining || 0).toLocaleString()} />
                            <Stat label="Credit wallet" value={Number(topupCreditsRemaining || 0).toLocaleString()} />
                        </div>
                        <div className="mt-6 flex flex-wrap gap-3">
                            <button
                                type="button"
                                onClick={() => onNavigate('subscription')}
                                className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500"
                            >
                                Membership
                            </button>
                            <button
                                type="button"
                                onClick={handleOpenBilling}
                                className="inline-flex items-center gap-2 rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:border-white/[0.14] hover:bg-white/[0.06]"
                            >
                                <CreditCard className="h-4 w-4" />
                                Billing
                            </button>
                        </div>
                    </section>

                    <section className="rounded-2xl border border-white/[0.08] bg-gradient-to-br from-white/[0.04] to-white/[0.015] p-6 shadow-sm shadow-black/30">
                        <h2 className="text-lg font-bold text-white">Lane access</h2>
                        <p className="mt-1 text-sm text-gray-500">Every premium workflow is gated from the same wallet and membership model.</p>
                        <div className="mt-5 grid gap-2 sm:grid-cols-2">
                            {laneEntries.map(([label, enabled]) => (
                                <div
                                    key={label}
                                    className={`flex items-center justify-between rounded-xl border px-4 py-3 text-sm ${
                                        enabled
                                            ? 'border-emerald-500/20 bg-emerald-500/[0.07] text-emerald-100'
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
                            <p className="mt-2">Expiring rollover credits burn first, then the current monthly grant.</p>
                            <p className="mt-1">Purchased reload credits persist if membership expires.</p>
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

function AccountPill({ icon: Icon, label, value }: { icon: typeof User; label: string; value: string }) {
    return (
        <div className="rounded-xl border border-white/[0.08] bg-black/25 px-3 py-2.5">
            <div className="flex items-center gap-2 text-[10px] uppercase tracking-wider text-gray-500">
                <Icon className="h-3.5 w-3.5 text-cyan-300" />
                {label}
            </div>
            <p className="mt-1 text-sm font-semibold text-white">{value}</p>
        </div>
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

function normalizeWebsite(value: string) {
    const clean = value.trim();
    if (!clean) return '';
    const candidate = /^https?:\/\//i.test(clean) ? clean : `https://${clean}`;
    const parsed = new URL(candidate);
    if (!parsed.hostname) throw new Error('Enter a valid public website URL.');
    return parsed.toString().replace(/\/$/, '');
}
