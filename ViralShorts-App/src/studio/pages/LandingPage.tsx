import { useContext, useState } from 'react';
import { ArrowRight, CheckCircle2, Rocket, Sparkles, Workflow, Wrench } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, Logo, STUDIO_SITE_URL, isBillingHost } from '../shared';

export default function LandingPage({ onNavigate }: { onNavigate: PageNav }) {
    const { session, signInWithGoogle, topupPacks } = useContext(AuthContext);
    const billingHost = isBillingHost;
    const sortedPacks = [...topupPacks].sort((a, b) => a.credits - b.credits);
    const [googleLoading, setGoogleLoading] = useState(false);
    const openBilling = () => {
        window.location.href = billingHost ? `${window.location.origin}?page=billing` : `${BILLING_SITE_URL}?page=billing`;
    };

    const openDashboard = () => {
        if (session) {
            onNavigate('dashboard');
            return;
        }
        onNavigate('auth');
    };

    const openGoogle = () => {
        if (session) {
            onNavigate('dashboard');
            return;
        }
        void (async () => {
            setGoogleLoading(true);
            const error = await signInWithGoogle();
            setGoogleLoading(false);
            if (error) onNavigate('auth');
        })();
    };

    const liveLanes = [
        {
            title: 'Skeleton AI',
            desc: '3D skeleton comparison shorts with a locked visual identity across every scene.',
            icon: <Wrench className="h-5 w-5" />,
        },
        {
            title: 'Day Trading',
            desc: 'Trading and investing shorts with a fast ElevenLabs voice and hook-forward pacing.',
            icon: <Rocket className="h-5 w-5" />,
        },
        {
            title: 'Moral Dilemma',
            desc: 'Cinematic impossible-choice shorts with forced binary-choice CTAs that drive comments.',
            icon: <Sparkles className="h-5 w-5" />,
        },
        {
            title: 'Scary Stories',
            desc: 'Horror and true-crime shorts in a David Fincher color palette.',
            icon: <Workflow className="h-5 w-5" />,
        },
        {
            title: 'Historical Epic',
            desc: 'Ridley-Scott-scale historical shorts, period-accurate, cinematic color grading.',
            icon: <Rocket className="h-5 w-5" />,
        },
        {
            title: 'Business / Finance / Tech / Crypto',
            desc: 'Four Bloomberg-grade niches sharing the same premium short-form documentary style.',
            icon: <Workflow className="h-5 w-5" />,
        },
    ];


    return (
        <>
            <NavBar onNavigate={onNavigate} />

            <section className="relative overflow-hidden pt-32 pb-24">
                <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,_rgba(6,182,212,0.14),_transparent_40%),radial-gradient(circle_at_70%_20%,_rgba(139,92,246,0.18),_transparent_32%)]" />
                <div className="relative mx-auto max-w-6xl px-6">
                    <div className="grid items-center gap-10 lg:grid-cols-[1.1fr,0.9fr]">
                        <div>
                            <div className="inline-flex items-center gap-2 rounded-full border border-cyan-500/20 bg-cyan-500/10 px-4 py-1.5 text-sm font-medium text-cyan-300">
                                <Rocket className="h-4 w-4" />
                                AI Content Creation Engine
                            </div>
                            <h1 className="mt-6 text-5xl font-extrabold leading-[1.02] tracking-tight text-white md:text-7xl">
                                Ship a short every day. Without breaking.
                            </h1>
                            <p className="mt-6 max-w-3xl text-lg leading-relaxed text-gray-400">
                                Pick a niche, type a topic, hit Generate. Studio writes the script, renders the scenes, narrates it, bakes in captions, and hands you an MP4 — or publishes straight to your connected YouTube channel in one click. Nine live niches, two free renders to try it.
                            </p>
                            <div className="mt-8 flex flex-col gap-4 sm:flex-row">
                                <button
                                    type="button"
                                    onClick={() => onNavigate('waitlist')}
                                    className="inline-flex items-center justify-center gap-2 rounded-xl bg-gradient-to-r from-violet-600 via-fuchsia-600 to-cyan-600 px-8 py-4 text-lg font-semibold text-white shadow-lg shadow-violet-900/30 transition hover:opacity-95"
                                >
                                    Join Waiting List
                                    <ArrowRight className="h-5 w-5" />
                                </button>
                                <button
                                    type="button"
                                    onClick={openDashboard}
                                    className="inline-flex items-center justify-center gap-2 rounded-xl bg-violet-600 px-8 py-4 text-lg font-semibold text-white transition hover:bg-violet-500"
                                >
                                    {session ? 'Open Studio' : 'Open Sign In'}
                                    <ArrowRight className="h-5 w-5" />
                                </button>
                                {session ? (
                                    <button
                                        type="button"
                                        onClick={openBilling}
                                        className="rounded-xl border border-white/[0.08] bg-white/[0.03] px-8 py-4 text-lg font-medium text-white transition hover:border-white/[0.14] hover:bg-white/[0.07]"
                                    >
                                        View Pricing
                                    </button>
                                ) : (
                                    <button
                                        type="button"
                                        onClick={openGoogle}
                                        disabled={googleLoading}
                                        className="rounded-xl border border-white/[0.08] bg-white/[0.03] px-8 py-4 text-lg font-medium text-white transition hover:border-white/[0.14] hover:bg-white/[0.07] disabled:opacity-60"
                                    >
                                        {googleLoading ? 'Opening Google...' : 'Continue with Google'}
                                    </button>
                                )}
                            </div>
                            <div className="mt-10 grid gap-6 sm:grid-cols-3">
                                <StatCard label="Live Niches" value="9" />
                                <StatCard label="Free Renders" value="2" />
                                <StatCard label="1-Click Publish" value="YouTube" />
                            </div>
                        </div>

                        <div className="rounded-[32px] border border-white/[0.08] bg-white/[0.03] p-6 shadow-2xl shadow-black/30">
                            <div className="flex items-center gap-3">
                                <Logo size={30} />
                                <div>
                                    <p className="text-xs uppercase tracking-[0.18em] text-cyan-300">What you get</p>
                                    <h2 className="text-xl font-bold text-white">Everything a short needs</h2>
                                </div>
                            </div>
                            <div className="mt-6 space-y-3">
                                {[
                                    'Script, voice-over, scene images, animation, and captions in one pass',
                                    'One-click publish direct to your connected YouTube channel',
                                    'Failed renders auto-refund your credits — no tickets, no chargebacks',
                                    'Nine niches live: Skeleton AI, Day Trading, Moral Dilemma, Business, Finance, Tech, Crypto, Scary, History',
                                ].map((item) => (
                                    <div key={item} className="flex items-start gap-2 rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3 text-sm text-gray-300">
                                        <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-emerald-300" />
                                        <span>{item}</span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                </div>
            </section>

            <section className="border-t border-white/[0.06] py-20">
                <div className="mx-auto max-w-6xl px-6">
                    <div className="mb-10 text-center">
                        <p className="text-sm font-semibold uppercase tracking-[0.18em] text-violet-300">Live Now</p>
                        <h2 className="mt-3 text-4xl font-bold text-white">Nine live short-form niches</h2>
                        <p className="mx-auto mt-3 max-w-3xl text-gray-400">
                            Every niche ships the same way: script, scenes, voice, captions, and the final MP4 — plus a one-click publish to YouTube. If a render fails, your credits refund automatically.
                        </p>
                    </div>
                    <div className="grid gap-5 md:grid-cols-2 xl:grid-cols-3">
                        {liveLanes.map((lane) => (
                            <div key={lane.title} className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                                <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-violet-500/10 text-violet-300">
                                    {lane.icon}
                                </div>
                                <h3 className="mt-5 text-xl font-bold text-white">{lane.title}</h3>
                                <p className="mt-2 text-sm leading-relaxed text-gray-400">{lane.desc}</p>
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            <section className="border-t border-white/[0.06] py-20">
                <div className="mx-auto max-w-6xl px-6">
                    <div className="mb-10 text-center">
                        <p className="text-sm font-semibold uppercase tracking-[0.18em] text-cyan-300">Offer Design</p>
                        <h2 className="mt-3 text-4xl font-bold text-white">One product, two ways to pay</h2>
                        <p className="mx-auto mt-3 max-w-3xl text-gray-400">
                            Use the monthly plans for recurring short-form output, or buy top-up packs when animation usage spikes. Billing stays inside Studio.
                        </p>
                    </div>
                    <div className="grid gap-6">
                        <div className="rounded-[32px] border border-white/[0.06] bg-white/[0.02] p-6">
                            <p className="text-xs uppercase tracking-[0.18em] text-cyan-300">Credit Wallet</p>
                            <h3 className="mt-3 text-2xl font-bold text-white">Top up only when usage spikes</h3>
                            <p className="mt-3 text-sm text-gray-400">
                                The wallet is for heavier animation runs, hybrid accounts, or usage-only buyers who do not want membership yet.
                            </p>
                            <div className="mt-6 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                                {sortedPacks.slice(0, 4).map((pack) => (
                                    <div key={pack.price_id} className="rounded-2xl border border-white/[0.08] bg-black/20 p-4">
                                        <p className="text-sm font-semibold text-white">{String(pack.pack || '').toUpperCase()}</p>
                                        <p className="mt-1 text-xs text-gray-500">{pack.credits} credits</p>
                                        <p className="mt-4 text-2xl font-bold text-white">${Number(pack.price_usd || 0).toFixed(2)}</p>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                </div>
            </section>


            <section className="border-t border-white/[0.06] py-24">
                <div className="mx-auto max-w-4xl px-6 text-center">
                    <p className="text-sm font-semibold uppercase tracking-[0.18em] text-cyan-300">Start Here</p>
                    <h2 className="mt-3 text-4xl font-bold text-white">Open Studio and build from one workspace.</h2>
                    <p className="mx-auto mt-4 max-w-2xl text-gray-400">
                        If you already know your niche, open Studio now. Email + password works even when Google or YouTube OAuth is having a bad day.
                    </p>
                    <div className="mt-8 flex flex-col justify-center gap-4 sm:flex-row">
                        <button
                            type="button"
                            onClick={openDashboard}
                            className="inline-flex items-center justify-center gap-2 rounded-xl bg-violet-600 px-8 py-4 text-lg font-semibold text-white transition hover:bg-violet-500"
                        >
                            {session ? 'Open Studio' : 'Open Sign In'}
                            <ArrowRight className="h-5 w-5" />
                        </button>
                        {session ? (
                            <button
                                type="button"
                                onClick={openBilling}
                                className="rounded-xl border border-white/[0.08] bg-white/[0.03] px-8 py-4 text-lg font-medium text-white transition hover:border-white/[0.14] hover:bg-white/[0.07]"
                            >
                                Pricing
                            </button>
                        ) : (
                            <button
                                type="button"
                                onClick={openGoogle}
                                disabled={googleLoading}
                                className="rounded-xl border border-white/[0.08] bg-white/[0.03] px-8 py-4 text-lg font-medium text-white transition hover:border-white/[0.14] hover:bg-white/[0.07] disabled:opacity-60"
                            >
                                {googleLoading ? 'Opening Google...' : 'Continue with Google'}
                            </button>
                        )}
                    </div>
                    {!billingHost && (
                        <p className="mt-6 text-xs text-gray-500">
                            ThumbLab now redirects into Studio. The thumbnail engine lives inside the same product at <span className="text-gray-300">{STUDIO_SITE_URL}</span>.
                        </p>
                    )}
                </div>
            </section>
        </>
    );
}

function StatCard({ label, value }: { label: string; value: string }) {
    return (
        <div className="rounded-2xl border border-white/[0.08] bg-white/[0.02] px-4 py-4">
            <p className="text-xs uppercase tracking-[0.18em] text-gray-500">{label}</p>
            <p className="mt-3 text-2xl font-bold text-white">{value}</p>
        </div>
    );
}
