import { useContext, useEffect } from 'react';
import { Bell, Globe2, SlidersHorizontal, WalletCards } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL } from '../shared';

export default function SettingsPage({ onNavigate }: { onNavigate: PageNav }) {
    const { session, role } = useContext(AuthContext);
    const isAdmin = role === 'admin';

    useEffect(() => {
        if (!session) onNavigate('auth');
    }, [session, onNavigate]);

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
                                <p className="mt-2 text-sm font-semibold text-white">Custom voice library first</p>
                                <p className="mt-2 text-xs text-gray-500">ElevenLabs stays optional, but the default path is shifting toward owned or local voice assets.</p>
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
                                    <p className="mt-1 text-sm text-gray-400">Open the dedicated billing surface for plans and AC credit packs.</p>
                                </div>
                            </div>
                            <button type="button" onClick={() => { window.location.href = `${BILLING_SITE_URL}?view=checkout`; }} className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500">
                                Open Billing
                            </button>
                        </div>
                        {isAdmin && <p className="mt-4 text-xs text-emerald-300">Admin preview account detected. Settings architecture is live without changing owner overrides.</p>}
                    </section>
                </div>
            </div>
        </>
    );
}
