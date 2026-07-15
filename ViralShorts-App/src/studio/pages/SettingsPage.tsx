import { useCallback, useContext, useEffect, useState } from 'react';
import { ArrowLeft, Bell, BrainCircuit, CheckCircle2, Globe2, ShieldCheck, SlidersHorizontal, Trash2, WalletCards, Youtube } from 'lucide-react';
import StudioShell from '../components/layout/StudioShell';
import { type PageNav } from '../components/NavBar';
import { API, AuthContext, BILLING_SITE_URL, resolveStudioBackendUrl, startYouTubeBrowserConnect } from '../shared';

type ConnectedYouTubeChannel = {
    channel_id: string;
    title: string;
    channel_handle?: string;
    analytics_snapshot?: {
        channel_summary?: string;
    };
};

type TrainingConsent = {
    training_opt_in: boolean;
    human_review_opt_in: boolean;
    include_prompts: boolean;
    include_uploads: boolean;
    include_outputs: boolean;
    include_feedback: boolean;
    consent_version: string;
};

export default function SettingsPage({ onNavigate }: { onNavigate: PageNav }) {
    const { session, role, longformOwnerBeta } = useContext(AuthContext);
    const isAdmin = role === 'admin';
    const [youtubeChannels, setYoutubeChannels] = useState<ConnectedYouTubeChannel[]>([]);
    const [youtubeDefaultChannelId, setYoutubeDefaultChannelId] = useState('');
    const [youtubeLoading, setYoutubeLoading] = useState(false);
    const [youtubeConnecting, setYoutubeConnecting] = useState(false);
    const [youtubeError, setYoutubeError] = useState('');
    const [trainingConsent, setTrainingConsent] = useState<TrainingConsent | null>(null);
    const [trainingBusy, setTrainingBusy] = useState(false);
    const [trainingMessage, setTrainingMessage] = useState('');

    useEffect(() => {
        if (!session) onNavigate('auth');
    }, [session, onNavigate]);

    const accessToken = session?.access_token ?? '';

    const loadYouTubeChannels = useCallback(async (sync = true) => {
        if (!accessToken) return;
        setYoutubeLoading(true);
        setYoutubeError('');
        try {
            const res = await fetch(resolveStudioBackendUrl(`/api/youtube/channels?sync=${sync ? 'true' : 'false'}`), {
                headers: { Authorization: `Bearer ${accessToken}` },
            });
            const payload = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(String((payload as any).detail || `Request failed (${res.status})`));
            setYoutubeChannels(Array.isArray((payload as any).channels) ? (payload as any).channels : []);
            setYoutubeDefaultChannelId(String((payload as any).default_channel_id || '').trim());
        } catch (e: any) {
            setYoutubeChannels([]);
            setYoutubeError(e?.message || 'Failed to load connected YouTube channels');
        } finally {
            setYoutubeLoading(false);
        }
    }, [accessToken]);

    const startYouTubeConnect = useCallback(async () => {
        if (!accessToken) return;
        setYoutubeConnecting(true);
        setYoutubeError('');
        try {
            startYouTubeBrowserConnect(accessToken, window.location.href);
        } catch (e: any) {
            setYoutubeError(e?.message || 'Failed to start Google YouTube connection');
            setYoutubeConnecting(false);
        }
    }, [accessToken]);

    useEffect(() => {
        if (!accessToken) return;
        void loadYouTubeChannels(false);
    }, [accessToken, loadYouTubeChannels]);

    const loadTrainingConsent = useCallback(async () => {
        if (!accessToken) return;
        const res = await fetch(resolveStudioBackendUrl('/api/studio-agent/training-consent'), {
            headers: { Authorization: `Bearer ${accessToken}` },
        });
        const payload = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(String((payload as any)?.detail || 'Could not load training controls'));
        setTrainingConsent((payload as any).consent as TrainingConsent);
    }, [accessToken]);

    useEffect(() => {
        if (!accessToken) return;
        void loadTrainingConsent().catch(() => undefined);
    }, [accessToken, loadTrainingConsent]);

    const saveTrainingConsent = useCallback(async (next: TrainingConsent) => {
        if (!accessToken) return;
        setTrainingBusy(true);
        setTrainingMessage('');
        try {
            const res = await fetch(resolveStudioBackendUrl('/api/studio-agent/training-consent'), {
                method: 'PATCH',
                headers: {
                    Authorization: `Bearer ${accessToken}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(next),
            });
            const payload = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(String((payload as any)?.detail || 'Could not save training controls'));
            setTrainingConsent((payload as any).consent as TrainingConsent);
            setTrainingMessage(next.training_opt_in ? 'Training contribution enabled.' : 'Training contribution disabled.');
        } catch (e: any) {
            setTrainingMessage(String(e?.message || 'Could not save training controls'));
        } finally {
            setTrainingBusy(false);
        }
    }, [accessToken]);

    const deleteTrainingData = useCallback(async () => {
        if (!accessToken || !window.confirm('Permanently delete your collected NYPTID training data and disable future collection?')) return;
        setTrainingBusy(true);
        setTrainingMessage('');
        try {
            const res = await fetch(resolveStudioBackendUrl('/api/studio-agent/training-data'), {
                method: 'DELETE',
                headers: { Authorization: `Bearer ${accessToken}` },
            });
            const payload = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(String((payload as any)?.detail || 'Could not delete training data'));
            await loadTrainingConsent();
            setTrainingMessage(`Training data deletion completed. Removed ${Number((payload as any)?.deletion?.deleted_rows || 0)} records.`);
        } catch (e: any) {
            setTrainingMessage(String(e?.message || 'Could not delete training data'));
        } finally {
            setTrainingBusy(false);
        }
    }, [accessToken, loadTrainingConsent]);

    if (!session) return null;

    return (
        <StudioShell onNavigate={onNavigate}>
            <div className="mx-auto max-w-6xl space-y-8">
                <section className="relative overflow-hidden rounded-2xl border border-white/[0.08] bg-[radial-gradient(circle_at_14%_0%,rgba(6,182,212,0.18),transparent_32%),linear-gradient(135deg,rgba(8,47,73,0.25),rgba(9,9,11,0.96)_48%,rgba(46,16,101,0.32))] p-6 shadow-2xl shadow-black/30 sm:p-8">
                    <div className="relative z-10 flex flex-wrap items-end justify-between gap-4">
                        <div>
                            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-cyan-300">Workspace controls</p>
                            <h1 className="mt-2 text-2xl font-bold text-white sm:text-3xl">Settings</h1>
                            <p className="mt-2 max-w-xl text-sm text-gray-400">
                                YouTube connections, defaults, and billing shortcuts — same shell as the rest of Studio v2.
                            </p>
                        </div>
                        <div className="grid min-w-[260px] grid-cols-2 gap-2">
                            <MiniStatus label="YouTube" value={youtubeChannels.length ? `${youtubeChannels.length} live` : 'Not linked'} active={youtubeChannels.length > 0} />
                            <MiniStatus label="Access" value={isAdmin ? 'Owner' : 'User'} active />
                        </div>
                        <button
                            type="button"
                            onClick={() => onNavigate('dashboard')}
                            className="inline-flex items-center gap-2 rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:border-white/[0.14] hover:bg-white/[0.06]"
                        >
                            <ArrowLeft className="h-4 w-4" />
                            Back to Studio
                        </button>
                    </div>
                </section>

                <SettingsBlock
                    icon={Youtube}
                    iconClass="text-red-300"
                    title="YouTube channels"
                    description="Connect channels so Catalyst can learn from private title, thumbnail, and analytics patterns."
                    actions={
                        <>
                            <button
                                type="button"
                                onClick={() => void loadYouTubeChannels()}
                                disabled={youtubeLoading}
                                className="rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-2.5 text-sm font-medium text-white transition hover:bg-white/[0.06] disabled:opacity-60"
                            >
                                {youtubeLoading ? 'Refreshing…' : 'Refresh'}
                            </button>
                            <button
                                type="button"
                                onClick={startYouTubeConnect}
                                disabled={youtubeConnecting}
                                className="rounded-xl bg-red-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-red-500 disabled:opacity-60"
                            >
                                {youtubeConnecting ? 'Opening Google…' : 'Connect YouTube'}
                            </button>
                        </>
                    }
                >
                    {!longformOwnerBeta && (
                        <p className="text-xs text-amber-300/90">
                            Deep Catalyst analysis is owner beta — you can still connect now for when it ships.
                        </p>
                    )}
                    {youtubeError && <p className="text-sm text-red-400">{youtubeError}</p>}
                    {youtubeChannels.length > 0 ? (
                        <div className="grid gap-3 md:grid-cols-2">
                            {youtubeChannels.map((channel) => (
                                <div
                                    key={channel.channel_id}
                                    className={`rounded-xl border p-4 ${
                                        youtubeDefaultChannelId === channel.channel_id
                                            ? 'border-cyan-400/30 bg-cyan-500/10'
                                            : 'border-white/[0.08] bg-black/20'
                                    }`}
                                >
                                    <p className="text-sm font-semibold text-white">{channel.title}</p>
                                    {channel.channel_handle && (
                                        <p className="mt-1 text-xs text-cyan-200">{channel.channel_handle}</p>
                                    )}
                                    {channel.analytics_snapshot?.channel_summary && (
                                        <p className="mt-2 text-xs text-gray-400">{channel.analytics_snapshot.channel_summary}</p>
                                    )}
                                    {youtubeDefaultChannelId === channel.channel_id && (
                                        <p className="mt-3 text-[11px] uppercase tracking-[0.18em] text-cyan-300">Default for Catalyst</p>
                                    )}
                                </div>
                            ))}
                        </div>
                    ) : (
                        <p className="text-sm text-gray-500">No YouTube channels connected yet.</p>
                    )}
                </SettingsBlock>

                <div className="grid gap-4 md:grid-cols-2">
                    <SettingsBlock
                        icon={Globe2}
                        iconClass="text-cyan-300"
                        title="Language"
                        description="English is the default UI language. Multi-language narration expands here."
                    />
                    <SettingsBlock
                        icon={SlidersHorizontal}
                        iconClass="text-violet-300"
                        title="Creation defaults"
                        description="720p launch profile · ElevenLabs voice · Generate-first quick run."
                    />
                </div>

                <SettingsBlock
                    icon={BrainCircuit}
                    iconClass="text-violet-300"
                    title="Help train NYPTID models"
                    description="Optional, consent-based collection of your Studio prompts, uploads, generated outputs, edits, tool results, and feedback."
                    actions={
                        trainingConsent ? (
                            <button
                                type="button"
                                disabled={trainingBusy}
                                onClick={() => void saveTrainingConsent({
                                    ...trainingConsent,
                                    training_opt_in: !trainingConsent.training_opt_in,
                                    include_prompts: !trainingConsent.training_opt_in,
                                    include_uploads: !trainingConsent.training_opt_in,
                                    include_outputs: !trainingConsent.training_opt_in,
                                    include_feedback: !trainingConsent.training_opt_in,
                                })}
                                className={`rounded-xl px-4 py-2.5 text-sm font-semibold transition disabled:opacity-60 ${
                                    trainingConsent.training_opt_in
                                        ? 'border border-violet-400/30 bg-violet-500/10 text-violet-100 hover:bg-violet-500/20'
                                        : 'bg-violet-600 text-white hover:bg-violet-500'
                                }`}
                            >
                                {trainingBusy ? 'Saving...' : trainingConsent.training_opt_in ? 'Disable contribution' : 'Enable contribution'}
                            </button>
                        ) : null
                    }
                >
                    <p className="text-xs leading-relaxed text-gray-400">
                        YouTube OAuth analytics are isolated and excluded from general model-training exports. Secrets, tokens, payment data, email addresses, and authorization headers are redacted.
                    </p>
                    {trainingConsent?.training_opt_in && (
                        <label className="flex items-start gap-3 rounded-xl border border-white/[0.08] bg-black/20 p-3 text-sm text-gray-300">
                            <input
                                type="checkbox"
                                checked={trainingConsent.human_review_opt_in}
                                disabled={trainingBusy}
                                onChange={(e) => void saveTrainingConsent({ ...trainingConsent, human_review_opt_in: e.target.checked })}
                                className="mt-0.5"
                            />
                            Permit authorized NYPTID reviewers to inspect selected examples for quality control. Automated training collection remains available without this permission.
                        </label>
                    )}
                    <div className="flex flex-wrap items-center gap-3">
                        <button
                            type="button"
                            onClick={() => void deleteTrainingData()}
                            disabled={trainingBusy}
                            className="inline-flex items-center gap-2 rounded-xl border border-rose-500/25 px-3 py-2 text-xs font-semibold text-rose-200 hover:bg-rose-500/10 disabled:opacity-50"
                        >
                            <Trash2 className="h-3.5 w-3.5" />
                            Delete my training data
                        </button>
                        {trainingMessage && <p className="text-xs text-gray-300">{trainingMessage}</p>}
                    </div>
                </SettingsBlock>

                <SettingsBlock
                    icon={Bell}
                    iconClass="text-amber-300"
                    title="Notifications"
                    description="Render complete, upload outcomes, and billing alerts — deeper controls coming soon."
                />

                <SettingsBlock
                    icon={WalletCards}
                    iconClass="text-emerald-300"
                    title="Billing"
                    description="Monthly plans and wallet top-ups live on the billing page."
                    actions={
                        <button
                            type="button"
                            onClick={() => { window.location.href = `${BILLING_SITE_URL}?page=billing`; }}
                            className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500"
                        >
                            Open Billing
                        </button>
                    }
                />

                {isAdmin && <AdminRefundPanel />}
            </div>
        </StudioShell>
    );
}

function SettingsBlock({
    icon: Icon,
    iconClass,
    title,
    description,
    actions,
    children,
}: {
    icon: typeof Youtube;
    iconClass: string;
    title: string;
    description: string;
    actions?: React.ReactNode;
    children?: React.ReactNode;
}) {
    return (
        <section className="rounded-2xl border border-white/[0.08] bg-gradient-to-br from-white/[0.04] to-white/[0.015] p-6 shadow-sm shadow-black/25">
            <div className="flex flex-wrap items-start justify-between gap-4">
                <div className="flex gap-3">
                    <div className={`mt-0.5 flex h-10 w-10 shrink-0 items-center justify-center rounded-xl border border-white/[0.08] bg-black/35 ${iconClass}`}>
                        <Icon className="h-4 w-4" />
                    </div>
                    <div>
                        <h2 className="text-base font-semibold text-white">{title}</h2>
                        <p className="mt-1 text-sm text-gray-500">{description}</p>
                    </div>
                </div>
                {actions && <div className="flex flex-wrap gap-2">{actions}</div>}
            </div>
            {children && <div className="mt-4 space-y-3">{children}</div>}
        </section>
    );
}

function MiniStatus({ label, value, active }: { label: string; value: string; active?: boolean }) {
    return (
        <div className="rounded-xl border border-white/[0.08] bg-black/25 px-3 py-3">
            <div className="flex items-center gap-2">
                {active ? <CheckCircle2 className="h-3.5 w-3.5 text-emerald-300" /> : <ShieldCheck className="h-3.5 w-3.5 text-gray-500" />}
                <p className="text-[10px] uppercase tracking-wider text-gray-500">{label}</p>
            </div>
            <p className="mt-1 text-sm font-semibold text-white">{value}</p>
        </div>
    );
}

function AdminRefundPanel() {
    const { session } = useContext(AuthContext);
    const [email, setEmail] = useState('');
    const [credits, setCredits] = useState('');
    const [reason, setReason] = useState('');
    const [source, setSource] = useState<'auto' | 'topup' | 'monthly'>('auto');
    const [busy, setBusy] = useState(false);
    const [result, setResult] = useState('');
    const [errorMsg, setErrorMsg] = useState('');

    const submit = async () => {
        if (!session) return;
        setBusy(true);
        setResult('');
        setErrorMsg('');
        try {
            const res = await fetch(`${API}/api/admin/refund-credits`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${session.access_token}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ email: email.trim(), credits: Number(credits || 0), source, reason: reason.trim() }),
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(String((data as any)?.detail || `Refund failed (${res.status})`));
            setResult(`Refunded ${(data as any)?.credits || credits} AC to ${(data as any)?.email || email}.`);
            setCredits('');
            setReason('');
        } catch (e: any) {
            setErrorMsg(String(e?.message || 'Refund failed'));
        } finally {
            setBusy(false);
        }
    };

    return (
        <section className="rounded-2xl border border-rose-500/20 bg-rose-500/[0.04] p-6">
            <h2 className="text-base font-semibold text-white">Admin · issue refund</h2>
            <p className="mt-1 text-sm text-gray-500">Credit AC back to a user wallet. Logged and Discord-alerted.</p>
            <div className="mt-5 grid gap-3 md:grid-cols-2">
                <Field label="User email" type="email" value={email} onChange={setEmail} placeholder="user@example.com" />
                <Field label="Credits to refund" type="number" value={credits} onChange={setCredits} placeholder="40" />
                <div>
                    <label className="text-xs uppercase tracking-wider text-gray-500">Source</label>
                    <select
                        value={source}
                        onChange={(e) => setSource(e.target.value as 'auto' | 'topup' | 'monthly')}
                        className="mt-1 w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white"
                    >
                        <option value="auto">Auto (top-up wallet)</option>
                        <option value="topup">Top-up wallet</option>
                        <option value="monthly">Monthly included</option>
                    </select>
                </div>
                <Field label="Reason (logs)" type="text" value={reason} onChange={setReason} placeholder="Support credit" />
            </div>
            <div className="mt-5 flex flex-wrap items-center gap-3">
                <button
                    type="button"
                    onClick={() => void submit()}
                    disabled={busy || !email.trim() || !credits || Number(credits) <= 0}
                    className="rounded-xl bg-rose-600 px-5 py-2.5 text-sm font-semibold text-white transition hover:bg-rose-500 disabled:opacity-40"
                >
                    {busy ? 'Issuing…' : 'Issue refund'}
                </button>
                {result && <p className="text-sm text-emerald-300">{result}</p>}
                {errorMsg && <p className="text-sm text-rose-300">{errorMsg}</p>}
            </div>
        </section>
    );
}

function Field({
    label,
    type,
    value,
    onChange,
    placeholder,
}: {
    label: string;
    type: string;
    value: string;
    onChange: (v: string) => void;
    placeholder?: string;
}) {
    return (
        <div>
            <label className="text-xs uppercase tracking-wider text-gray-500">{label}</label>
            <input
                type={type}
                value={value}
                onChange={(e) => onChange(e.target.value)}
                placeholder={placeholder}
                className="mt-1 w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white placeholder:text-gray-600"
            />
        </div>
    );
}
