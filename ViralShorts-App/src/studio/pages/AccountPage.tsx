import { useContext, useEffect, useMemo, useState } from 'react';
import { ArrowLeft, Camera, CheckCircle2, CreditCard, Gauge, LogOut, Save, ShieldCheck, User, WalletCards } from 'lucide-react';
import StudioShell from '../components/layout/StudioShell';
import { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, isBillingHost } from '../shared';
import { loadStudioHubState, patchStudioHubState } from '../lib/studioHubState';

export default function AccountPage({ onNavigate }: { onNavigate: PageNav }) {
    const {
        session,
        supabase,
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
    const profileStorageKey = session?.user?.id ? `nyptid_profile_${session.user.id}` : 'nyptid_profile';
    const [displayName, setDisplayName] = useState(() => {
        const metaName = String((session?.user?.user_metadata as any)?.display_name || (session?.user?.user_metadata as any)?.name || '').trim();
        if (metaName) return metaName;
        if (typeof window !== 'undefined') return localStorage.getItem(profileStorageKey) || '';
        return '';
    });
    const [profileDetails, setProfileDetails] = useState({
        company: '',
        website: '',
        avatar_url: '',
        bio: '',
    });
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
                const persistedName = String(state.profile?.display_name || '').trim();
                if (persistedName) setDisplayName(persistedName);
                setProfileDetails({
                    company: String((state.profile as any)?.company || ''),
                    website: String((state.profile as any)?.website || ''),
                    avatar_url: String((state.profile as any)?.avatar_url || ''),
                    bio: String((state.profile as any)?.bio || ''),
                });
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
            ? capitalize(normalizedPlan || 'starter')
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
        const clean = displayName.trim();
        setProfileStatus('');
        const syncWarnings: string[] = [];
        try {
            if (typeof window !== 'undefined') localStorage.setItem(profileStorageKey, clean);
            if (supabase?.auth?.updateUser) {
                const { error } = await supabase.auth.updateUser({ data: { display_name: clean, avatar_url: profileDetails.avatar_url } });
                if (error) syncWarnings.push(error.message);
            }
            if (accessToken) {
                await patchStudioHubState(accessToken, { profile: { display_name: clean, ...profileDetails } });
            } else {
                syncWarnings.push('Studio profile sync is waiting for an active session.');
            }
            setProfileStatus(syncWarnings.length ? 'Profile saved in Studio. Account metadata sync is pending.' : 'Profile saved.');
        } catch (e: any) {
            setProfileStatus(e?.message || 'Saved locally. Cloud profile update failed.');
        }
    };

    const handleAvatarFile = (file?: File | null) => {
        if (!file) return;
        if (!file.type.startsWith('image/')) {
            setProfileStatus('Use a PNG, JPG, WebP, or GIF image.');
            return;
        }
        const reader = new FileReader();
        reader.onload = () => {
            setProfileDetails((prev) => ({ ...prev, avatar_url: String(reader.result || '') }));
            setProfileStatus('Profile image ready. Press Save to keep it.');
        };
        reader.readAsDataURL(file);
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
                                <h2 className="text-lg font-bold text-white">Profile</h2>
                                <p className="mt-1 text-sm text-gray-500">Edit the account identity Studio shows across the Hub, Network, and production tools.</p>
                            </div>
                            <User className="h-5 w-5 text-cyan-300" />
                        </div>
                        <div className="mt-5 grid gap-5 lg:grid-cols-[180px_1fr]">
                            <div>
                                <span className="text-xs uppercase tracking-wider text-gray-500">Profile picture</span>
                                <div className="mt-2 flex flex-col items-start gap-3">
                                    <div className="flex h-28 w-28 items-center justify-center overflow-hidden rounded-2xl border border-white/[0.08] bg-black/30">
                                        {profileDetails.avatar_url ? (
                                            <img src={profileDetails.avatar_url} alt="Profile preview" className="h-full w-full object-cover" />
                                        ) : (
                                            <User className="h-10 w-10 text-gray-500" />
                                        )}
                                    </div>
                                    <label className="inline-flex cursor-pointer items-center gap-2 rounded-xl border border-white/[0.08] bg-white/[0.03] px-3 py-2 text-xs font-semibold text-gray-200 transition hover:bg-white/[0.06]">
                                        <Camera className="h-4 w-4 text-cyan-300" />
                                        Upload image
                                        <input type="file" accept="image/png,image/jpeg,image/webp,image/gif" className="hidden" onChange={(e) => handleAvatarFile(e.target.files?.[0])} />
                                    </label>
                                </div>
                            </div>
                            <div>
                                <div className="grid gap-3 md:grid-cols-[1fr_1fr_auto]">
                            <label className="block">
                                <span className="text-xs uppercase tracking-wider text-gray-500">Display name</span>
                                <input
                                    value={displayName}
                                    onChange={(e) => setDisplayName(e.target.value)}
                                    placeholder="Your Studio name"
                                    className="mt-1 h-11 w-full rounded-xl border border-white/[0.08] bg-black/30 px-3 text-sm text-white outline-none placeholder:text-gray-600 focus:border-cyan-400/40"
                                />
                            </label>
                            <label className="block">
                                <span className="text-xs uppercase tracking-wider text-gray-500">Email</span>
                                <input
                                    value={session.user.email || ''}
                                    readOnly
                                    className="mt-1 h-11 w-full rounded-xl border border-white/[0.08] bg-black/20 px-3 text-sm text-gray-400 outline-none"
                                />
                            </label>
                            <button
                                type="button"
                                onClick={() => void saveProfile()}
                                className="self-end inline-flex h-11 items-center justify-center gap-2 rounded-xl bg-cyan-600 px-4 text-sm font-semibold text-white transition hover:bg-cyan-500"
                            >
                                <Save className="h-4 w-4" />
                                Save
                            </button>
                        </div>
                        <div className="mt-3 grid gap-3 md:grid-cols-2">
                            <ProfileInput label="Company" value={profileDetails.company} onChange={(value) => setProfileDetails((prev) => ({ ...prev, company: value }))} placeholder="NYPTID Industries" />
                            <ProfileInput label="Website" value={profileDetails.website} onChange={(value) => setProfileDetails((prev) => ({ ...prev, website: value }))} placeholder="https://..." />
                        </div>
                        <label className="mt-3 block">
                            <span className="text-xs uppercase tracking-wider text-gray-500">Bio</span>
                            <textarea
                                value={profileDetails.bio}
                                onChange={(e) => setProfileDetails((prev) => ({ ...prev, bio: e.target.value }))}
                                placeholder="What you build, your niche, proof, goals..."
                                className="mt-1 min-h-24 w-full resize-y rounded-xl border border-white/[0.08] bg-black/30 px-3 py-3 text-sm text-white outline-none placeholder:text-gray-600 focus:border-cyan-400/40"
                            />
                        </label>
                            </div>
                        </div>
                        {profileStatus && <p className="mt-3 text-sm text-cyan-200">{profileStatus}</p>}
                    </section>

                    <section className="rounded-2xl border border-white/[0.08] bg-gradient-to-br from-white/[0.04] to-white/[0.015] p-6 shadow-sm shadow-black/30">
                        <div className="flex items-start justify-between gap-4">
                            <div>
                                <h2 className="text-lg font-bold text-white">Wallet & plan</h2>
                                <p className="mt-1 text-sm text-gray-500">One balance across OpenRouter, fal, ElevenLabs, and Studio Agent runs.</p>
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
                            <p className="mt-2">Included monthly credits burn first when membership is active.</p>
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

function ProfileInput({ label, value, onChange, placeholder }: { label: string; value: string; onChange: (value: string) => void; placeholder: string }) {
    return (
        <label className="block">
            <span className="text-xs uppercase tracking-wider text-gray-500">{label}</span>
            <input
                value={value}
                onChange={(e) => onChange(e.target.value)}
                placeholder={placeholder}
                className="mt-1 h-11 w-full rounded-xl border border-white/[0.08] bg-black/30 px-3 text-sm text-white outline-none placeholder:text-gray-600 focus:border-cyan-400/40"
            />
        </label>
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
