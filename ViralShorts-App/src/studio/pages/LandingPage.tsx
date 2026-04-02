import { useContext, useState } from 'react';
import { ArrowRight, CheckCircle2, Copy, Film, Image, Rocket, ScissorsLineDashed, Sparkles, Workflow, Wrench } from 'lucide-react';
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
        void (async () => {
            setGoogleLoading(true);
            const error = await signInWithGoogle();
            setGoogleLoading(false);
            if (error) onNavigate('auth');
        })();
    };

    const liveLanes = [
        {
            title: 'AI Stories',
            desc: 'Cinematic story shorts inside the Create workspace.',
            icon: <Sparkles className="h-5 w-5" />,
        },
        {
            title: 'Motivation',
            desc: 'Short-form motivational videos with stronger pacing and narration.',
            icon: <Workflow className="h-5 w-5" />,
        },
        {
            title: 'Skeleton AI',
            desc: '3D comparison shorts with the locked skeleton visual identity.',
            icon: <Wrench className="h-5 w-5" />,
        },
        {
            title: 'Day Trading',
            desc: 'Trading and investing shorts with Catalyst channel context.',
            icon: <Rocket className="h-5 w-5" />,
        },
        {
            title: 'Chat Story',
            desc: 'Premium text-message shorts unlocked on the paid monthly plans.',
            icon: <Workflow className="h-5 w-5" />,
        },
    ];

    const roadmapLanes = [
        {
            title: 'Thumbnails',
            desc: 'Still being sharpened as an operator-beta lane. Not part of the public short-form billing promise yet.',
            icon: <Image className="h-5 w-5" />,
        },
        {
            title: 'Clone',
            desc: 'Structure-cloning is still being refined, so it stays off the public plan promise for now.',
            icon: <Copy className="h-5 w-5" />,
        },
        {
            title: 'Long Form',
            desc: 'Machine-learning and editing work is still underway, so Long Form stays in the private operator lane for now.',
            icon: <Film className="h-5 w-5" />,
        },
        {
            title: 'AutoClipper',
            desc: 'Visible in Studio now, but still held back until clipping quality, scoring, and packaging beat the lazy "coming soon" bar.',
            icon: <ScissorsLineDashed className="h-5 w-5" />,
        },
        {
            title: 'Catalyst',
            desc: 'The shared engine behind thumbnails, cloning, long-form generation, and future automation layers.',
            icon: <Workflow className="h-5 w-5" />,
        },
        {
            title: 'Operator Tools',
            desc: 'Internal demo and analytics lanes stay private so the public offer remains tight and credible.',
            icon: <Wrench className="h-5 w-5" />,
        },
    ];
    const marketingDoctrine = [
        'Be active in the Daily Marketing Channel.',
        'Analyze and Improve. Evaluate each marketing piece to understand what works and what doesn’t. Think about how you could improve it.',
        'Small, daily improvements in your marketing skills can lead to significant progress over time due to compounding.',
        'Just like in boxing or other martial arts, consistent practice and real-world application are crucial for mastering marketing.',
        'Engage with the daily challenges to continuously hone your skills. Missing a day occasionally is okay, but don’t make it a habit.',
        'Regardless of your field or business, understanding and practicing marketing is fundamental to success.',
        'Treat the daily marketing challenges seriously and make it a part of your routine to see substantial benefits in your marketing abilities.',
        'Mastering marketing has enabled Arno to start and scale companies and avoid manual labor by understanding how to attract clients and improve businesses.',
        'It is a long-lasting skill. Marketing has been around for millennia and will continue to be valuable in the future.',
        'Anyone can learn it. It doesn’t require special skills, abilities, or connections. Pay attention, focus, and you can succeed.',
        'High ROI (Return On Investment). Direct response marketing offers the highest and most reliable return on investment, outperforming traditional investments.',
        'Learning marketing helps you see opportunities and gaps that others miss, making life easier.',
        'You don’t need to be the world’s best marketer; being better than most is enough to succeed.',
        'It is a fast skill to learn. With ten days of dedicated study, you can acquire valuable marketing skills.',
        'Be ready for a significant change as you learn and apply these marketing skills.',
    ];
    const marketingDoctrineDisplay = marketingDoctrine.map((point) =>
        point
            .split('â€™').join("'")
            .split('â€œ').join('"')
            .split('â€\u009d').join('"')
            .split('â€"').join(' - '),
    );

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
                                Faceless YouTube Operating System
                            </div>
                            <h1 className="mt-6 text-5xl font-extrabold leading-[1.02] tracking-tight text-white md:text-7xl">
                                Build faceless YouTube content with one Studio.
                            </h1>
                            <p className="mt-6 max-w-3xl text-lg leading-relaxed text-gray-400">
                                NYPTID Studio now sells one short-form product. Catalyst powers the Create workflow and Chat Story now, while the heavier lanes stay behind the curtain until they are genuinely ready.
                            </p>
                            <div className="mt-8 flex flex-col gap-4 sm:flex-row">
                                <button
                                    type="button"
                                    onClick={openDashboard}
                                    disabled={!session && googleLoading}
                                    className="inline-flex items-center justify-center gap-2 rounded-xl bg-violet-600 px-8 py-4 text-lg font-semibold text-white transition hover:bg-violet-500"
                                >
                                    {session ? 'Open Studio' : (googleLoading ? 'Opening Google...' : 'Continue with Google')}
                                    <ArrowRight className="h-5 w-5" />
                                </button>
                                <button
                                    type="button"
                                    onClick={openBilling}
                                    className="rounded-xl border border-white/[0.08] bg-white/[0.03] px-8 py-4 text-lg font-medium text-white transition hover:border-white/[0.14] hover:bg-white/[0.07]"
                                >
                                    View Pricing
                                </button>
                            </div>
                            <div className="mt-10 grid gap-6 sm:grid-cols-3">
                                <StatCard label="Live Templates" value="5" />
                                <StatCard label="Free Renders" value="2" />
                                <StatCard label="Primary Auth" value="Google" />
                            </div>
                        </div>

                        <div className="rounded-[32px] border border-white/[0.08] bg-white/[0.03] p-6 shadow-2xl shadow-black/30">
                            <div className="flex items-center gap-3">
                                <Logo size={30} />
                                <div>
                                    <p className="text-xs uppercase tracking-[0.18em] text-cyan-300">Catalyst</p>
                                    <h2 className="text-xl font-bold text-white">One engine, multiple lanes</h2>
                                </div>
                            </div>
                            <div className="mt-6 space-y-3">
                                {[ 
                                    'Create scripts, scenes, and renders without leaving Studio',
                                    'Use membership and wallet credits together instead of juggling separate subscriptions',
                                    'Keep Chat Story inside the same short-form account model',
                                    'Expand into the heavier lanes later without changing the billing structure',
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
                        <h2 className="mt-3 text-4xl font-bold text-white">Public launch surface</h2>
                        <p className="mx-auto mt-3 max-w-3xl text-gray-400">
                            The public offer is intentionally tight: one Create workspace with five live short-form templates. Product Demo stays internal. Analytics stays owner-only. Clone, Thumbnails, Long Form, and AutoClipper remain visible in the vision but are not part of the public billing promise yet.
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

            <section className="border-t border-white/[0.06] py-20">
                <div className="mx-auto max-w-6xl px-6">
                    <div className="mb-10 text-center">
                        <p className="text-sm font-semibold uppercase tracking-[0.18em] text-emerald-300">Growth Doctrine</p>
                        <h2 className="mt-3 text-4xl font-bold text-white">The operating principles behind Catalyst</h2>
                        <p className="mx-auto mt-3 max-w-3xl text-gray-400">
                            These lesson points now sit inside the Studio positioning and Long Form workflow so the product stays aligned with direct-response, measurable YouTube growth instead of generic creator fluff.
                        </p>
                    </div>
                    <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                        {marketingDoctrineDisplay.map((point) => (
                            <div key={point} className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-4 text-sm leading-relaxed text-gray-300">
                                {point}
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            <section className="border-t border-white/[0.06] py-20">
                <div className="mx-auto max-w-6xl px-6">
                    <div className="mb-10 text-center">
                        <p className="text-sm font-semibold uppercase tracking-[0.18em] text-amber-300">Roadmap</p>
                        <h2 className="mt-3 text-4xl font-bold text-white">What stays private or coming soon</h2>
                    </div>
                    <div className="grid gap-5 md:grid-cols-3">
                        {roadmapLanes.map((lane) => (
                            <div key={lane.title} className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                                <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-amber-500/10 text-amber-300">
                                    {lane.icon}
                                </div>
                                <h3 className="mt-5 text-xl font-bold text-white">{lane.title}</h3>
                                <p className="mt-2 text-sm leading-relaxed text-gray-400">{lane.desc}</p>
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            <section className="border-t border-white/[0.06] py-24">
                <div className="mx-auto max-w-4xl px-6 text-center">
                    <p className="text-sm font-semibold uppercase tracking-[0.18em] text-cyan-300">Start Here</p>
                    <h2 className="mt-3 text-4xl font-bold text-white">Open Studio and build from one workspace.</h2>
                    <p className="mx-auto mt-4 max-w-2xl text-gray-400">
                        If you already know your niche, open Studio now. If you want to test pricing first, go straight to the monthly plans and top-up packs.
                    </p>
                    <div className="mt-8 flex flex-col justify-center gap-4 sm:flex-row">
                        <button
                            type="button"
                            onClick={openDashboard}
                            disabled={!session && googleLoading}
                            className="inline-flex items-center justify-center gap-2 rounded-xl bg-violet-600 px-8 py-4 text-lg font-semibold text-white transition hover:bg-violet-500"
                        >
                            {session ? 'Open Studio' : (googleLoading ? 'Opening Google...' : 'Continue with Google')}
                            <ArrowRight className="h-5 w-5" />
                        </button>
                        <button
                            type="button"
                            onClick={openBilling}
                            className="rounded-xl border border-white/[0.08] bg-white/[0.03] px-8 py-4 text-lg font-medium text-white transition hover:border-white/[0.14] hover:bg-white/[0.07]"
                        >
                            Pricing
                        </button>
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
