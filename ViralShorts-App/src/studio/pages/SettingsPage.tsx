import { useCallback, useContext, useEffect, useState } from 'react';
import { Bell, Globe2, SlidersHorizontal, WalletCards, Youtube } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
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
        <>
            <NavBar onNavigate={onNavigate} active="settings" />
            <div className="mx-auto max-w-5xl px-6 pt-24 pb-10">
                <div className="mb-8">
                    <h1 className="text-3xl font-bold text-white">Settings</h1>
                    <p className="mt-2 text-sm text-gray-400">Workspace preferences, rendering defaults, and billing shortcuts.</p>
                </div>

                <div className="space-y-6">
                    <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-6">
                        <div className="flex items-center justify-between gap-4">
                            <div className="flex items-center gap-3">
                                <Youtube className="w-5 h-5 text-red-300" />
                                <div>
                                    <h2 className="text-lg font-semibold text-white">YouTube Channels</h2>
                                    <p className="mt-1 text-sm text-gray-400">Connect one or more channels so Catalyst can learn from private title, thumbnail, and analytics patterns.</p>
                                </div>
                            </div>
                            <div className="flex gap-2">
                                <button type="button" onClick={() => void loadYouTubeChannels()} disabled={!longformOwnerBeta || youtubeLoading} className="rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-2.5 text-sm font-medium text-white transition hover:bg-white/[0.06] disabled:opacity-60">
                                    {youtubeLoading ? 'Refreshing...' : 'Refresh'}
                                </button>
                                <button type="button" onClick={startYouTubeConnect} disabled={!longformOwnerBeta} className="rounded-xl bg-red-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-red-500 disabled:opacity-60">
                                    {youtubeConnecting ? 'Opening Google...' : 'Connect YouTube'}
                                </button>
                            </div>
                        </div>
                        {!longformOwnerBeta ? (
                            <p className="mt-4 text-xs text-amber-300">
                                Connected-channel deep analysis is owner beta right now. Public Studio users stay on the lighter manual Long Form workflow while Catalyst is being tuned.
                            </p>
                        ) : null}
                        {youtubeError ? <p className="mt-4 text-sm text-red-400">{youtubeError}</p> : null}
                        {youtubeChannels.length > 0 ? (
                            <div className="mt-4 grid gap-3 md:grid-cols-2">
                                {youtubeChannels.map((channel) => (
                                    <div key={channel.channel_id} className={`rounded-xl border p-4 ${youtubeDefaultChannelId === channel.channel_id ? 'border-cyan-400/30 bg-cyan-500/10' : 'border-white/[0.08] bg-black/20'}`}>
                                        <p className="text-sm font-semibold text-white">{channel.title}</p>
                                        {channel.channel_handle ? <p className="mt-1 text-xs text-cyan-200">{channel.channel_handle}</p> : null}
                                        {channel.analytics_snapshot?.channel_summary ? <p className="mt-2 text-xs text-gray-400">{channel.analytics_snapshot.channel_summary}</p> : null}
                                        {youtubeDefaultChannelId === channel.channel_id ? <p className="mt-3 text-[11px] uppercase tracking-[0.18em] text-cyan-300">Default channel for Catalyst</p> : null}
                                    </div>
                                ))}
                            </div>
                        ) : (
                            <p className="mt-4 text-sm text-gray-400">No YouTube channels connected yet.</p>
                        )}
                    </section>

                    <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-6">
                        <div className="flex items-center gap-3">
                            <Globe2 className="w-5 h-5 text-cyan-300" />
                            <h2 className="text-lg font-semibold text-white">Language + Regional Defaults</h2>
                        </div>
                        <p className="mt-3 text-sm text-gray-400">
                            English is the current default UI language. Multi-language narration and region presets will expand here as the dashboard overhaul continues.
                        </p>
                    </section>

                    <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-6">
                        <div className="flex items-center gap-3">
                            <SlidersHorizontal className="w-5 h-5 text-violet-300" />
                            <h2 className="text-lg font-semibold text-white">Creation Defaults</h2>
                        </div>
                        <div className="mt-4 grid gap-4 md:grid-cols-2">
                            <div className="rounded-xl border border-white/[0.08] bg-black/20 p-4">
                                <p className="text-xs uppercase tracking-[0.18em] text-gray-500">Default Quality</p>
                                <p className="mt-2 text-sm font-semibold text-white">720p launch profile</p>
                                <p className="mt-2 text-xs text-gray-500">Keeps render reliability high while preserving the current paid animation lane.</p>
                            </div>
                            <div className="rounded-xl border border-white/[0.08] bg-black/20 p-4">
                                <p className="text-xs uppercase tracking-[0.18em] text-gray-500">Voice Providers</p>
                                <p className="mt-2 text-sm font-semibold text-white">ElevenLabs first</p>
                                <p className="mt-2 text-xs text-gray-500">ElevenLabs is the main live voice provider now. The custom Catalyst rack stays available as a fallback and for tuned house voices.</p>
                            </div>
                        </div>
                    </section>

                    <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-6">
                        <div className="flex items-center gap-3">
                            <Bell className="w-5 h-5 text-amber-300" />
                            <h2 className="text-lg font-semibold text-white">Notifications</h2>
                        </div>
                        <p className="mt-3 text-sm text-gray-400">
                            Notification controls are being moved into this page as part of the new dashboard architecture. The shell is live now; deeper controls come next.
                        </p>
                    </section>

                    <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-6">
                        <div className="flex items-center justify-between gap-4">
                            <div className="flex items-center gap-3">
                                <WalletCards className="w-5 h-5 text-emerald-300" />
                                <div>
                                    <h2 className="text-lg font-semibold text-white">Billing</h2>
                                    <p className="mt-1 text-sm text-gray-400">Open the dedicated billing surface for Free, the three monthly plans, and the top-up packs.</p>
                                </div>
                            </div>
                            <button type="button" onClick={() => { window.location.href = `${BILLING_SITE_URL}?view=checkout`; }} className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500">
                                Open Billing
                            </button>
                        </div>
                        {isAdmin && <p className="mt-4 text-xs text-emerald-300">Admin preview account detected. Settings architecture is live without changing owner overrides.</p>}
                    </section>

                    {isAdmin && <AdminRefundPanel />}
                </div>
            </div>
        </>
    );
}

function AdminRefundPanel() {
    const { session } = useContext(AuthContext);
    const [email, setEmail] = useState('');
    const [credits, setCredits] = useState('');
    const [reason, setReason] = useState('');
    const [source, setSource] = useState<'auto' | 'topup' | 'monthly'>('auto');
    const [busy, setBusy] = useState(false);
    const [result, setResult] = useState<string>('');
    const [errorMsg, setErrorMsg] = useState<string>('');

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
            setResult(`Refunded ${(data as any)?.credits || credits} AC to ${(data as any)?.email || email} via ${(data as any)?.source || source}.`);
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
            <div className="flex items-center gap-3">
                <WalletCards className="w-5 h-5 text-rose-300" />
                <div>
                    <h2 className="text-lg font-semibold text-white">Admin · Issue Refund</h2>
                    <p className="mt-1 text-sm text-gray-400">Credit AC back to a user's wallet. Use for chargeback resolution or support refunds. Logged + Discord-alerted.</p>
                </div>
            </div>
            <div className="mt-5 grid gap-3 md:grid-cols-2">
                <div>
                    <label className="text-xs uppercase tracking-wider text-gray-400">User Email</label>
                    <input
                        type="email"
                        value={email}
                        onChange={(e) => setEmail(e.target.value)}
                        placeholder="user@example.com"
                        className="mt-1 w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white placeholder:text-gray-600"
                    />
                </div>
                <div>
                    <label className="text-xs uppercase tracking-wider text-gray-400">Credits to Refund</label>
                    <input
                        type="number"
                        min={1}
                        max={10000}
                        value={credits}
                        onChange={(e) => setCredits(e.target.value)}
                        placeholder="e.g. 40"
                        className="mt-1 w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white placeholder:text-gray-600"
                    />
                </div>
                <div>
                    <label className="text-xs uppercase tracking-wider text-gray-400">Source</label>
                    <select
                        value={source}
                        onChange={(e) => setSource(e.target.value as 'auto' | 'topup' | 'monthly')}
                        className="mt-1 w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white"
                    >
                        <option value="auto">Auto (recommended — refunds to top-up wallet)</option>
                        <option value="topup">Top-up wallet</option>
                        <option value="monthly">Monthly included credits</option>
                    </select>
                </div>
                <div>
                    <label className="text-xs uppercase tracking-wider text-gray-400">Reason (optional)</label>
                    <input
                        type="text"
                        value={reason}
                        onChange={(e) => setReason(e.target.value)}
                        placeholder="e.g. broken render, content-filter false-flag, support credit"
                        className="mt-1 w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white placeholder:text-gray-600"
                    />
                </div>
            </div>
            <div className="mt-5 flex flex-wrap items-center justify-between gap-3">
                <button
                    type="button"
                    onClick={() => void submit()}
                    disabled={busy || !email.trim() || !credits || Number(credits) <= 0}
                    className="rounded-xl bg-rose-600 px-5 py-2.5 text-sm font-semibold text-white transition hover:bg-rose-500 disabled:opacity-40"
                >
                    {busy ? 'Issuing refund…' : 'Issue Refund'}
                </button>
                {result && <p className="text-sm text-emerald-300">{result}</p>}
                {errorMsg && <p className="text-sm text-rose-300">{errorMsg}</p>}
            </div>
        </section>
    );
}
