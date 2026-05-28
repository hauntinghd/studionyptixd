import { useCallback, useContext, useEffect, useState } from 'react';
import { ArrowLeft, Bell, Globe2, SlidersHorizontal, WalletCards, Youtube } from 'lucide-react';
import StudioShell from '../components/layout/StudioShell';
import { type PageNav } from '../components/NavBar';
import { API, AuthContext, BILLING_SITE_URL, startYouTubeBrowserConnect } from '../shared';

type ConnectedYouTubeChannel = {
    channel_id: string;
    title: string;
    channel_handle?: string;
    analytics_snapshot?: {
        channel_summary?: string;
    };
};

export default function SettingsPage({ onNavigate }: { onNavigate: PageNav }) {
    const { session, role, longformOwnerBeta } = useContext(AuthContext);
    const isAdmin = role === 'admin';
    const [youtubeChannels, setYoutubeChannels] = useState<ConnectedYouTubeChannel[]>([]);
    const [youtubeDefaultChannelId, setYoutubeDefaultChannelId] = useState('');
    const [youtubeLoading, setYoutubeLoading] = useState(false);
    const [youtubeConnecting, setYoutubeConnecting] = useState(false);
    const [youtubeError, setYoutubeError] = useState('');

    useEffect(() => {
        if (!session) onNavigate('auth');
    }, [session, onNavigate]);

    const loadYouTubeChannels = useCallback(async () => {
        if (!session) return;
        setYoutubeLoading(true);
        setYoutubeError('');
        try {
            const res = await fetch(`${API}/api/youtube/channels?sync=true`, {
                headers: { Authorization: `Bearer ${session.access_token}` },
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
    }, [session]);

    const startYouTubeConnect = useCallback(async () => {
        if (!session) return;
        setYoutubeConnecting(true);
        setYoutubeError('');
        try {
            startYouTubeBrowserConnect(session.access_token, window.location.href);
        } catch (e: any) {
            setYoutubeError(e?.message || 'Failed to start Google YouTube connection');
            setYoutubeConnecting(false);
        }
    }, [session]);

    useEffect(() => {
        if (!session) return;
        void loadYouTubeChannels();
    }, [session, loadYouTubeChannels]);

    if (!session) return null;

    return (
        <StudioShell onNavigate={onNavigate}>
            <div className="mx-auto max-w-5xl space-y-8">
                <section className="relative overflow-hidden rounded-2xl border border-white/[0.08] bg-gradient-to-br from-cyan-950/20 via-[#0c0c10] to-violet-950/30 p-6 sm:p-8">
                    <div className="relative z-10 flex flex-wrap items-end justify-between gap-4">
                        <div>
                            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-cyan-300">Workspace</p>
                            <h1 className="mt-2 text-2xl font-bold text-white sm:text-3xl">Settings</h1>
                            <p className="mt-2 max-w-xl text-sm text-gray-400">
                                YouTube connections, defaults, and billing shortcuts — same shell as the rest of Studio v2.
                            </p>
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

                <div className="grid gap-6 md:grid-cols-2">
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
                            onClick={() => { window.location.href = `${BILLING_SITE_URL}?view=checkout`; }}
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
        <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-6">
            <div className="flex flex-wrap items-start justify-between gap-4">
                <div className="flex gap-3">
                    <div className={`mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-white/[0.08] bg-black/30 ${iconClass}`}>
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
