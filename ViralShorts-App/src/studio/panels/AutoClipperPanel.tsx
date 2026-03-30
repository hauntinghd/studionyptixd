import { useContext, useState } from 'react';
import { Crown, Scissors, Sparkles } from 'lucide-react';
import { AuthContext, BILLING_SITE_URL } from '../shared';

export default function AutoClipperPanel() {
    const { role, billingActive, plan } = useContext(AuthContext);
    const isAdmin = role === 'admin';
    const unlocked = isAdmin || billingActive || ['starter', 'creator', 'pro', 'elite', 'scale'].includes(plan);
    const [url, setUrl] = useState('');

    return (
        <div className="rounded-3xl border border-white/[0.08] bg-white/[0.02] p-6">
            <div className="flex items-start justify-between gap-4">
                <div>
                    <div className="flex items-center gap-2 text-violet-300">
                        <Scissors className="w-4 h-4" />
                        <span className="text-xs font-semibold uppercase tracking-[0.18em]">Premium Lane</span>
                    </div>
                    <h2 className="mt-3 text-2xl font-bold text-white">Auto Clipper</h2>
                        <p className="mt-2 max-w-2xl text-sm text-gray-400">
                        Extract viral-ready clips from long-form uploads and scored URLs. Catalyst membership will unlock beta access once the clipping and scoring slice is ready for public use.
                        </p>
                </div>
                <div className={`rounded-xl border px-4 py-2 text-sm font-semibold ${unlocked ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300' : 'border-amber-500/20 bg-amber-500/10 text-amber-300'}`}>
                    {unlocked ? 'Premium alpha access' : 'Upgrade required'}
                </div>
            </div>

            <div className="mt-8 grid gap-6 lg:grid-cols-[1.1fr,0.9fr]">
                <div className="rounded-2xl border border-white/[0.08] bg-black/20 p-5">
                    <label className="text-xs uppercase tracking-[0.18em] text-gray-500">Source URL</label>
                    <input
                        value={url}
                        onChange={(e) => setUrl(e.target.value)}
                        placeholder="Paste a YouTube URL..."
                        className="mt-3 w-full rounded-xl border border-white/[0.08] bg-black/30 px-4 py-3 text-sm text-white placeholder:text-gray-600 focus:border-violet-500/50 focus:outline-none"
                    />
                    <button
                        type="button"
                        disabled={!unlocked}
                        className="mt-4 w-full rounded-xl bg-violet-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-violet-500 disabled:opacity-50"
                    >
                        <Sparkles className="mr-2 inline-block h-4 w-4" />
                        Extract Clips
                    </button>
                    {!unlocked && (
                        <p className="mt-3 text-xs text-amber-300">
                            Auto Clipper stays in the roadmap until the quality bar is real. Membership and credit metering will come after the backend scoring slice is stable.
                        </p>
                    )}
                </div>

                <div className="rounded-2xl border border-white/[0.08] bg-black/20 p-5">
                    <h3 className="text-sm font-semibold text-white">Planned scoring signals</h3>
                    <ul className="mt-4 space-y-2 text-sm text-gray-400">
                        <li>Emotion and reaction spikes</li>
                        <li>Comment-bait and curiosity hooks</li>
                        <li>Clarity of standalone payoff</li>
                        <li>Caption density and spoken intensity</li>
                        <li>Likelihood of retaining short-form viewers</li>
                    </ul>
                    <button
                        type="button"
                        onClick={() => { window.location.href = `${BILLING_SITE_URL}?view=checkout`; }}
                        className="mt-6 inline-flex items-center gap-2 rounded-xl border border-white/[0.1] bg-white/[0.03] px-4 py-2.5 text-sm font-medium text-white transition hover:bg-white/[0.06]"
                    >
                        <Crown className="w-4 h-4 text-violet-300" />
                        Open Billing
                    </button>
                </div>
            </div>
        </div>
    );
}
