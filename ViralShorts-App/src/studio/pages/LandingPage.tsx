import { useContext } from 'react';
import { ArrowRight, CheckCircle2, Clapperboard, Clock, Flame, Globe, MessageSquareText, Monitor, Shield, Wand2, Zap } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, BILLING_SITE_URL, Logo, isBillingHost } from '../shared';

export default function LandingPage({ onNavigate }: { onNavigate: PageNav }) {
    const { session, topupPacks } = useContext(AuthContext);
    const sortedPacks = [...topupPacks].sort((a, b) => a.credits - b.credits);
    const billingHost = isBillingHost;

    const handleBuyPack = (priceId: string) => {
        if (!session) {
            onNavigate('auth');
            return;
        }
        const params = new URLSearchParams({ view: 'checkout', pack: priceId });
        window.location.href = billingHost ? `?${params.toString()}` : `${BILLING_SITE_URL}?${params.toString()}`;
    };

    const liveTemplates = [
        {
            title: 'AI Stories',
            desc: 'Scene-first faceless story shorts with strong hooks, editable visuals, and slideshow fallback.',
            icon: <Clapperboard className="w-5 h-5" />,
            color: 'from-violet-600 to-violet-800',
        },
        {
            title: 'Motivation',
            desc: 'Faceless motivation clips built for repeatable posting, strong pacing, and clean voice-led delivery.',
            icon: <Flame className="w-5 h-5" />,
            color: 'from-amber-600 to-amber-800',
        },
        {
            title: 'Skeleton AI',
            desc: 'Locked skeleton identity for comparison channels, with editable scenes, clothed variants, and consistent character continuity.',
            icon: <Shield className="w-5 h-5" />,
            color: 'from-cyan-600 to-sky-800',
        },
        {
            title: 'Chat Story',
            desc: 'Premium text-message shorts for faceless drama and niche story channels, with a dedicated phone-preview editor.',
            icon: <MessageSquareText className="w-5 h-5" />,
            color: 'from-fuchsia-600 to-violet-800',
        },
    ];

    const comingSoonTemplates = [
        'Business',
        'Finance',
        'Tech',
        'Crypto',
        'Objects Explain',
        'Would You Rather',
        'Scary Stories',
        'Historical Epic',
        'What If',
        'Clone',
        'Long Form',
        'Thumbnails',
        'Product Demo',
        'Auto Clipper',
    ];

    const proofLinks = [
        'https://youtube.com/shorts/36-AAocHhg0?feature=share',
        'https://youtube.com/shorts/K8-W6xmXF7w?feature=share',
        'https://youtube.com/shorts/1y10LtdyQ_I?feature=share',
        'https://youtube.com/shorts/UcTCAOUNa1I?feature=share',
    ];

    return (
        <>
            <NavBar onNavigate={onNavigate} />

            <section className="relative overflow-hidden pt-32 pb-24">
                <div className="absolute inset-0 bg-gradient-to-b from-violet-600/10 via-transparent to-transparent" />
                <div className="absolute top-16 left-1/2 h-[760px] w-[760px] -translate-x-1/2 rounded-full bg-violet-600/5 blur-[140px]" />
                <div className="relative max-w-5xl mx-auto px-6 text-center">
                    <div className="inline-flex items-center gap-2 rounded-full border border-emerald-500/20 bg-emerald-500/10 px-4 py-1.5 text-sm font-medium text-emerald-300 mb-8">
                        <Zap className="w-4 h-4" />
                        Built for Faceless YouTube Automation
                    </div>

                    <h1 className="text-6xl md:text-7xl font-extrabold tracking-tight leading-[1.05] mb-6">
                        Build Faceless
                        <br />
                        <span className="bg-gradient-to-r from-violet-400 via-purple-400 to-indigo-400 bg-clip-text text-transparent">
                            YouTube Shorts Faster
                        </span>
                    </h1>

                    <p className="mx-auto mb-10 max-w-3xl text-xl leading-relaxed text-gray-400">
                        NYPTID Studio is a scene-first workspace for faceless channels. Build AI Stories, Motivation, Skeleton AI, and Chat Story formats without living in timelines, then pay only when you animate or when you want the monthly Chat Story lane.
                    </p>

                    <div className="flex flex-col sm:flex-row items-center justify-center gap-4 mb-14">
                        <button
                            onClick={() => {
                                window.location.href = `${BILLING_SITE_URL}?page=billing`;
                            }}
                            className="group flex items-center gap-2 rounded-xl bg-violet-600 px-8 py-4 text-lg font-bold text-white transition-all hover:bg-violet-500 shadow-lg shadow-violet-600/20"
                        >
                            View Pricing
                            <ArrowRight className="w-5 h-5 transition-transform group-hover:translate-x-1" />
                        </button>
                        <button
                            onClick={() => onNavigate(session ? 'dashboard' : 'auth')}
                            className="rounded-xl border border-white/10 bg-white/5 px-8 py-4 text-lg font-medium text-white transition hover:bg-white/10"
                        >
                            {session ? 'Open Create Workspace' : 'Sign In'}
                        </button>
                    </div>

                    <div className="grid grid-cols-2 gap-6 md:grid-cols-4 max-w-3xl mx-auto">
                        {[
                            { value: '4', label: 'Live Templates' },
                            { value: 'Scene-First', label: 'Create Flow' },
                            { value: 'PayPal', label: 'Main Checkout' },
                            { value: 'Faceless', label: 'Channel Focus' },
                        ].map((item) => (
                            <div key={item.label}>
                                <div className="text-3xl font-bold text-white">{item.value}</div>
                                <div className="text-sm text-gray-500 mt-1">{item.label}</div>
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            <section className="py-16 border-t border-white/5">
                <div className="max-w-6xl mx-auto px-6">
                    <div className="text-center mb-10">
                        <h2 className="text-3xl md:text-4xl font-bold mb-3">Live Channel Formats</h2>
                        <p className="text-gray-400 max-w-2xl mx-auto">
                            The launch is intentionally narrow around faceless formats that are closest to stable and sellable right now.
                        </p>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
                        {liveTemplates.map((template) => (
                            <div
                                key={template.title}
                                className="group relative rounded-2xl border border-white/[0.06] bg-white/[0.03] p-6 transition-all hover:border-violet-500/30 hover:bg-violet-500/[0.03]"
                            >
                                <span className="absolute top-4 right-4 rounded-full border border-emerald-500/30 bg-emerald-500/20 px-2 py-0.5 text-[10px] font-bold tracking-wider text-emerald-300">
                                    LIVE
                                </span>
                                <div className={`mb-4 flex h-12 w-12 items-center justify-center rounded-xl bg-gradient-to-br ${template.color}`}>
                                    {template.icon}
                                </div>
                                <h3 className="text-lg font-bold transition-colors group-hover:text-violet-300">{template.title}</h3>
                                <p className="mt-2 text-sm leading-relaxed text-gray-500">{template.desc}</p>
                            </div>
                        ))}
                    </div>
                    <div className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-5">
                        <p className="mb-3 text-xs uppercase tracking-wider text-gray-500">Coming Soon</p>
                        <div className="flex flex-wrap gap-2">
                            {comingSoonTemplates.map((name) => (
                                <span key={name} className="rounded-full border border-white/[0.08] bg-black/20 px-3 py-1.5 text-xs text-gray-400">
                                    {name}
                                </span>
                            ))}
                        </div>
                    </div>
                </div>
            </section>

            <section className="py-16 border-t border-white/5">
                <div className="max-w-6xl mx-auto px-6">
                    <div className="text-center mb-10">
                        <h2 className="text-3xl md:text-4xl font-bold mb-3">Proof</h2>
                        <p className="text-gray-400 max-w-2xl mx-auto">
                            Public YouTube shorts are linked directly here. This is the content style the workspace is being shaped around.
                        </p>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        <div className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-4">
                            <p className="mb-3 text-xs uppercase tracking-wider text-gray-500">Featured Short</p>
                            <div className="mx-auto aspect-[9/16] max-h-[540px] overflow-hidden rounded-xl border border-white/[0.08] bg-black">
                                <iframe
                                    title="NYPTID Featured Short"
                                    src="https://www.youtube.com/embed/36-AAocHhg0"
                                    className="h-full w-full"
                                    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                                    allowFullScreen
                                />
                            </div>
                        </div>
                        <div className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-4">
                            <p className="mb-3 text-xs uppercase tracking-wider text-gray-500">Public Proof Links</p>
                            <div className="space-y-2">
                                {proofLinks.map((url, index) => (
                                    <a
                                        key={url}
                                        href={url}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="flex items-center justify-between rounded-xl border border-white/[0.08] bg-black/20 px-4 py-3 text-sm text-gray-300 transition hover:border-violet-500/40 hover:text-white"
                                    >
                                        <span>Watch Short #{index + 1}</span>
                                        <ArrowRight className="w-4 h-4" />
                                    </a>
                                ))}
                            </div>
                        </div>
                    </div>
                </div>
            </section>

            <section className="py-24 border-t border-white/5">
                <div className="max-w-6xl mx-auto px-6">
                    <div className="text-center mb-16">
                        <h2 className="text-4xl font-bold mb-4">Why This Fits Faceless Operators</h2>
                        <p className="text-gray-400 text-lg max-w-3xl mx-auto">
                            The product is being narrowed around repeatable faceless workflows instead of trying to look like a general-purpose editor.
                        </p>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                        <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-8">
                            <div className="mb-5 flex h-12 w-12 items-center justify-center rounded-xl bg-violet-500/10">
                                <Zap className="w-6 h-6 text-violet-400" />
                            </div>
                            <h3 className="text-xl font-bold mb-3">Scene-First Editing</h3>
                            <p className="text-gray-500 leading-relaxed">
                                You write the script, generate scenes, tune prompts, and keep the short editable before you spend on animation.
                            </p>
                        </div>
                        <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-8">
                            <div className="mb-5 flex h-12 w-12 items-center justify-center rounded-xl bg-emerald-500/10">
                                <Shield className="w-6 h-6 text-emerald-400" />
                            </div>
                            <h3 className="text-xl font-bold mb-3">Pay for Motion, Not Guesswork</h3>
                            <p className="text-gray-500 leading-relaxed">
                                Slideshows stay available, animation is pay-as-you-go, and monthly plans are reserved for the premium Chat Story lane.
                            </p>
                        </div>
                        <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-8">
                            <div className="mb-5 flex h-12 w-12 items-center justify-center rounded-xl bg-amber-500/10">
                                <Monitor className="w-6 h-6 text-amber-400" />
                            </div>
                            <h3 className="text-xl font-bold mb-3">Retention-Friendly Formats</h3>
                            <p className="text-gray-500 leading-relaxed">
                                AI Stories, Motivation, Skeleton AI, and Chat Story are the formats most aligned with faceless YouTube retention loops, so they lead the launch.
                            </p>
                        </div>
                    </div>
                </div>
            </section>

            <section className="py-24 border-t border-white/5" id="pricing">
                <div className="max-w-6xl mx-auto px-6">
                    <div className="text-center mb-16">
                        <div className="inline-flex items-center gap-2 rounded-full border border-emerald-500/20 bg-emerald-500/10 px-4 py-1.5 text-sm font-medium text-emerald-300 mb-6">
                            <Zap className="w-4 h-4" />
                            Free Slideshows + PayPal Credit Top-Ups
                        </div>
                        <h2 className="text-4xl font-bold mb-4">AC Credit Packs</h2>
                        <p className="text-gray-400 text-lg">
                            Buy animation credits with PayPal when you want motion. Monthly plans on the same billing surface unlock Chat Story.
                        </p>
                    </div>

                    <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-5 gap-6">
                        {sortedPacks.map((pack, index) => {
                            const isPopular = index === 4;
                            return (
                                <div
                                    key={pack.price_id}
                                    className={`relative rounded-2xl p-6 transition-all ${
                                        isPopular
                                            ? 'border-2 border-violet-500/30 bg-violet-500/[0.05] shadow-xl shadow-violet-500/5'
                                            : 'border border-white/[0.06] bg-white/[0.02]'
                                    }`}
                                >
                                    {isPopular && (
                                        <span className="absolute -top-3 left-1/2 -translate-x-1/2 rounded-full bg-violet-600 px-3 py-1 text-[10px] font-bold tracking-wide text-white">
                                            MOST POPULAR
                                        </span>
                                    )}
                                    <h3 className="text-lg font-bold">{String(pack.pack || '').toUpperCase()} Pack</h3>
                                    <p className="mt-1 text-xs text-gray-500">{pack.credits} AC credits</p>
                                    <div className="mt-5 flex items-baseline gap-1">
                                        <span className="text-3xl font-extrabold">${Number(pack.price_usd || 0).toFixed(2)}</span>
                                        <span className="text-sm text-gray-500">one-time</span>
                                    </div>
                                    <ul className="mt-5 space-y-2.5">
                                        <li className="flex items-center gap-2 text-xs text-gray-300">
                                            <CheckCircle2 className="w-3.5 h-3.5 shrink-0 text-violet-400" />
                                            {pack.credits} animation attempts
                                        </li>
                                        <li className="flex items-center gap-2 text-xs text-gray-300">
                                            <CheckCircle2 className="w-3.5 h-3.5 shrink-0 text-violet-400" />
                                            No monthly commitment
                                        </li>
                                        <li className="flex items-center gap-2 text-xs text-gray-300">
                                            <CheckCircle2 className="w-3.5 h-3.5 shrink-0 text-violet-400" />
                                            Free images and slideshows remain enabled
                                        </li>
                                    </ul>
                                    <button
                                        onClick={() => handleBuyPack(pack.price_id)}
                                        className={`mt-6 w-full rounded-lg py-2.5 text-sm font-bold transition-all ${
                                            isPopular
                                                ? 'bg-violet-600 text-white hover:bg-violet-500 shadow-lg shadow-violet-600/20'
                                                : 'border border-white/10 bg-white/5 text-white hover:bg-white/10'
                                        }`}
                                    >
                                        Buy Now
                                    </button>
                                </div>
                            );
                        })}
                    </div>
                </div>
            </section>

            <section className="py-24 border-t border-white/5">
                <div className="max-w-6xl mx-auto px-6">
                    <div className="text-center mb-16">
                            <div className="inline-flex items-center gap-2 rounded-full border border-amber-500/20 bg-amber-500/10 px-4 py-1.5 text-sm font-medium text-amber-300 mb-6">
                                <Clock className="w-4 h-4" />
                                Roadmap
                            </div>
                        <h2 className="text-4xl font-bold mb-4">What Comes Next For Faceless Channels</h2>
                        <p className="text-gray-400 text-lg max-w-3xl mx-auto">
                            New templates, long-form tooling, thumbnails, and deeper automation stay gated until they are stable enough to sell without support drag.
                        </p>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">
                        {[
                            {
                                title: 'Q1: More Faceless Niches',
                                desc: 'Business, Finance, Tech, Crypto, Objects Explain, Would You Rather, Scary Stories, and Historical Epic move public after they hit the same quality bar as the live set.',
                                icon: <Globe className="w-6 h-6 text-violet-400" />,
                            },
                            {
                                title: 'Q2: Long-Form Builder',
                                desc: 'Long-form chapter planning and scene editing return after the short-form create workspace is fully stable.',
                                icon: <Wand2 className="w-6 h-6 text-cyan-400" />,
                            },
                            {
                                title: 'Q3: Automation Layers',
                                desc: 'Prompt-level scene revision, stronger continuity controls, and repeatable channel automation for recurring content formats.',
                                icon: <Monitor className="w-6 h-6 text-amber-400" />,
                            },
                            {
                                title: 'Q4: Higher-End Rendering',
                                desc: 'Premium animation lanes, better exports, and stronger long-form rendering once the short-form engine is fully hardened.',
                                icon: <Clock className="w-6 h-6 text-emerald-400" />,
                            },
                        ].map((item) => (
                            <div key={item.title} className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-8">
                                <div className="mb-5 flex h-12 w-12 items-center justify-center rounded-xl bg-white/5">
                                    {item.icon}
                                </div>
                                <h3 className="text-xl font-bold mb-3">{item.title}</h3>
                                <p className="text-gray-500 leading-relaxed">{item.desc}</p>
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            <footer className="py-12 border-t border-white/5">
                <div className="max-w-6xl mx-auto px-6 flex flex-col md:flex-row items-center justify-between gap-4">
                    <div className="flex items-center gap-2">
                        <Logo size={22} />
                        <span className="font-bold">NYPTID Studio</span>
                        <span className="ml-2 text-sm text-gray-600">by NYPTID Industries</span>
                    </div>
                    <div className="flex flex-wrap items-center justify-center gap-3 text-sm">
                        <a href="/ai-story-video-generator.html" className="text-gray-500 transition-colors hover:text-white">AI Stories</a>
                        <a href="/motivation-video-maker.html" className="text-gray-500 transition-colors hover:text-white">Motivation</a>
                    </div>
                    <p className="text-sm text-gray-600">&copy; 2026 NYPTID Industries. All rights reserved.</p>
                </div>
            </footer>
        </>
    );
}
