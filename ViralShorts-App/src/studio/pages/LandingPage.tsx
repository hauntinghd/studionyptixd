import { useContext, useEffect, useMemo, useState } from 'react';
import {
    ArrowRight,
    BarChart3,
    CheckCircle2,
    Clapperboard,
    Image,
    Mic2,
    Play,
    Rocket,
    Search,
    Sparkles,
    UploadCloud,
} from 'lucide-react';
import ProofVideoGrid from '../components/landing/ProofVideoGrid';
import NavBar, { type PageNav } from '../components/NavBar';
import { landingProofVideos } from '../data/landingProofVideos';
import { landingUpdatePosts, type LandingUpdatePost } from '../data/landingUpdates';
import { AuthContext, BILLING_SITE_URL, GENERATION_API, Logo, STUDIO_SITE_URL, isBillingHost } from '../shared';

const stackSteps = [
    { title: 'Research trends', desc: 'Pull public demand signals before you commit to an angle.', icon: Search },
    { title: 'Learn from analytics', desc: 'Use connected channel performance so the next idea is not random.', icon: BarChart3 },
    { title: 'Write scripts/hooks', desc: 'Turn the topic into a paced video plan with a clear first-frame promise.', icon: Sparkles },
    { title: 'Generate stills', desc: 'Create reviewable scenes before expensive animation starts.', icon: Image },
    { title: 'Animate scenes', desc: 'Choose standard or premium motion depending on the video budget.', icon: Clapperboard },
    { title: 'Add voice/SFX/captions', desc: 'Voice, sound effects, music beds, subtitles, and timing in one workflow.', icon: Mic2 },
    { title: 'Export and publish', desc: 'Package the final MP4 and publish to the connected YouTube channel.', icon: UploadCloud },
];

const heroVideos = landingProofVideos.slice(0, 6);

const analyticsProof = [
    {
        channel: 'We Are Lacuna',
        views: '610,390',
        watchTime: '4.2K hours',
        subscribers: '+579',
        image: '/landing-proof/we-are-lacuna-610k.png',
    },
    {
        channel: 'CrypticScience',
        views: '177,540',
        watchTime: '942.8 hours',
        subscribers: '+168',
        image: '/landing-proof/cryptic-science-177k.png',
    },
    {
        channel: 'ZeroTier',
        views: '35,450',
        watchTime: '92.3 hours',
        subscribers: '+42',
        image: '/landing-proof/zerotier-35k.png',
    },
];

const publicPricingTiers = [
    { label: 'Test', price: '$25/mo', credits: '1,000 credits', detail: 'Enough to validate Studio Agent, scripts, stills, voice, and short-form tests.' },
    { label: 'Creator', price: '$50/mo', credits: '2,500 credits', detail: 'More monthly room for active creators testing multiple video ideas.' },
    { label: 'Growth', price: '$100/mo', credits: '5,000 credits', detail: 'Daily short-form testing capacity for a focused channel.' },
    { label: 'Operator', price: '$200/mo', credits: '11,000 credits', detail: 'Best value for daily operators running repeat shorts and variants.' },
];

export default function LandingPage({ onNavigate }: { onNavigate: PageNav }) {
    const { session, signInWithGoogle, topupPacks } = useContext(AuthContext);
    const billingHost = isBillingHost;
    const sortedPacks = useMemo(() => [...topupPacks].sort((a, b) => a.credits - b.credits), [topupPacks]);
    const [googleLoading, setGoogleLoading] = useState(false);
    const [ownerPosts, setOwnerPosts] = useState<LandingUpdatePost[]>([]);

    useEffect(() => {
        let cancelled = false;
        void (async () => {
            try {
                const res = await fetch(`${GENERATION_API}/api/blog/posts`);
                if (!res.ok) return;
                const data = await res.json();
                const posts = Array.isArray(data?.posts)
                    ? (data.posts as LandingUpdatePost[])
                    : [];
                if (!cancelled) setOwnerPosts(posts);
            } catch {
                // Static posts remain available when the API is unavailable.
            }
        })();
        return () => {
            cancelled = true;
        };
    }, []);

    const updatePosts = useMemo(() => {
        const seen = new Set<string>();
        return [...ownerPosts, ...landingUpdatePosts].filter((post) => {
            const key = String(post.slug || post.title || '').toLowerCase();
            if (!key || seen.has(key)) return false;
            seen.add(key);
            return true;
        }).slice(0, 6);
    }, [ownerPosts]);

    const openBilling = () => {
        window.location.href = billingHost ? `${window.location.origin}?page=billing` : `${BILLING_SITE_URL}?page=billing`;
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

    const scrollToProof = () => {
        document.getElementById('proof')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    };

    const scrollToPricing = () => {
        document.getElementById('pricing')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    };

    const openPricingPurchase = () => {
        if (!session) {
            onNavigate('auth');
            return;
        }
        openBilling();
    };


    return (
        <>
            <NavBar onNavigate={onNavigate} />

            <section className="relative overflow-hidden border-b border-white/[0.06] bg-[#07080a] pt-20 sm:pt-24">
                <div className="mx-auto grid min-h-[calc(100vh-3.5rem)] max-w-7xl items-center gap-10 px-4 pb-14 sm:min-h-[calc(100vh-4rem)] sm:px-6 sm:pb-16 lg:grid-cols-[0.95fr,1.05fr]">
                    <div className="max-w-3xl">
                        <div className="inline-flex items-center gap-2 rounded-lg border border-cyan-500/25 bg-cyan-500/10 px-3 py-1.5 text-sm font-semibold text-cyan-200">
                            <Rocket className="h-4 w-4" />
                            AI content creation engine
                        </div>
                        <div className="mt-5 grid grid-cols-3 gap-2 lg:hidden">
                            {heroVideos.slice(0, 3).map((video) => (
                                <img
                                    key={video.youtubeVideoId}
                                    src={video.thumbnailUrl}
                                    alt=""
                                    width="160"
                                    height="120"
                                    loading="eager"
                                    decoding="async"
                                    className="aspect-[9/12] w-full rounded-lg border border-white/[0.08] object-cover"
                                />
                            ))}
                        </div>
                        <h1 className="mt-6 text-[2.35rem] font-extrabold leading-[1.03] text-white sm:text-5xl lg:text-6xl">
                            Studio gives you an all-in-one workflow for creating content.
                        </h1>
                        <p className="mt-5 max-w-2xl text-base leading-7 text-gray-300 sm:mt-6 sm:text-lg sm:leading-8">
                            Research the idea, write the script, generate visuals, animate scenes, add voice, caption, package, publish, and learn from analytics inside one workspace.
                        </p>
                        <div className="mt-7 flex flex-col gap-3 sm:mt-8 sm:flex-row">
                            <button
                                type="button"
                                onClick={openPricingPurchase}
                                className="inline-flex items-center justify-center gap-2 rounded-lg bg-cyan-500 px-6 py-3 text-base font-bold text-black transition hover:bg-cyan-300 disabled:opacity-60"
                            >
                                {session ? 'Choose a plan' : 'Start creating'}
                                <ArrowRight className="h-5 w-5" />
                            </button>
                            <button
                                type="button"
                                onClick={scrollToProof}
                                className="inline-flex items-center justify-center gap-2 rounded-lg border border-white/[0.1] bg-white/[0.04] px-6 py-3 text-base font-semibold text-white transition hover:border-white/[0.18] hover:bg-white/[0.08]"
                            >
                                <Play className="h-5 w-5" />
                                See real videos made with Studio
                            </button>
                            <button
                                type="button"
                                onClick={scrollToPricing}
                                className="inline-flex items-center justify-center gap-2 rounded-lg border border-white/[0.1] bg-black/20 px-6 py-3 text-base font-semibold text-white transition hover:border-violet-400/40 hover:bg-violet-500/10"
                            >
                                View pricing
                            </button>
                        </div>
                        <p className="mt-3 text-xs leading-5 text-gray-500">
                            Free trial includes 1,000 credits, enough for one 2-minute short-form test run or equivalent shorter tests. Stripe requires a card to prevent trial abuse, and billing starts only after the trial ends unless you cancel.
                        </p>
                        <div className="mt-8 grid gap-3 sm:grid-cols-3">
                            <StatCard label="Verified channel views" value="823K+" />
                            <StatCard label="Proof videos loaded" value={String(landingProofVideos.length)} />
                            <StatCard label="YouTube embeds at load" value="0" />
                        </div>
                    </div>

                    <div className="relative">
                        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
                            {heroVideos.map((video, index) => (
                                <a
                                    key={video.youtubeVideoId}
                                    href={`https://www.youtube.com/watch?v=${video.youtubeVideoId}`}
                                    target="_blank"
                                    rel="noreferrer"
                                    className={[
                                        'group relative aspect-[9/14] overflow-hidden rounded-lg border border-white/[0.08] bg-white/[0.03]',
                                        index === 0 ? 'sm:col-span-2 sm:row-span-2' : '',
                                    ].join(' ')}
                                >
                                    <img
                                        src={video.thumbnailUrl}
                                        alt=""
                                        width="480"
                                        height="360"
                                        loading={index < 3 ? 'eager' : 'lazy'}
                                        decoding="async"
                                        className="h-full w-full object-cover opacity-85 transition group-hover:scale-[1.03] group-hover:opacity-100"
                                    />
                                    <div className="absolute inset-x-0 bottom-0 bg-gradient-to-t from-black via-black/70 to-transparent p-3">
                                        <p className="text-xs font-semibold text-cyan-200">{video.channelLabel}</p>
                                        <p className="mt-1 line-clamp-2 text-xs font-bold leading-snug text-white">{video.title}</p>
                                    </div>
                                </a>
                            ))}
                        </div>
                    </div>
                </div>
            </section>

            <section className="border-b border-white/[0.06] py-20">
                <div className="mx-auto grid max-w-7xl gap-10 px-6 lg:grid-cols-[0.88fr,1.12fr] lg:items-center">
                    <div className="max-w-2xl">
                        <p className="text-sm font-semibold uppercase tracking-[0.18em] text-cyan-300">Creator proof</p>
                        <h2 className="mt-3 text-3xl font-bold text-white sm:text-4xl">Real channel momentum, shown the way creators actually send receipts.</h2>
                        <p className="mt-4 text-base leading-7 text-gray-400">
                            Studio is being built around channels with real watch history, not a polished fake demo. These screenshots show more than 823,000 verified views across three creator channels, with analytics that Catalyst can learn from.
                        </p>
                        <div className="mt-6 grid grid-cols-3 gap-3">
                            {analyticsProof.map((item) => (
                                <div key={item.channel} className="rounded-lg border border-white/[0.08] bg-white/[0.03] p-3">
                                    <p className="text-lg font-black text-white">{item.views}</p>
                                    <p className="mt-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-gray-500">views</p>
                                </div>
                            ))}
                        </div>
                    </div>
                    <div className="relative min-h-[23rem] sm:min-h-[30rem]">
                        {analyticsProof.map((item, index) => (
                            <figure
                                key={item.channel}
                                className={[
                                    'absolute w-[78%] overflow-hidden rounded-xl border border-white/[0.1] bg-[#101114] shadow-2xl shadow-black/50',
                                    index === 0 ? 'left-0 top-4 z-30 rotate-[-2deg]' : '',
                                    index === 1 ? 'right-0 top-16 z-20 rotate-[2.5deg]' : '',
                                    index === 2 ? 'left-[10%] top-40 z-10 rotate-[-1deg] sm:top-56' : '',
                                ].join(' ')}
                            >
                                <img
                                    src={item.image}
                                    alt={`${item.channel} YouTube Studio analytics showing ${item.views} views`}
                                    width="1186"
                                    height="800"
                                    loading={index === 0 ? 'eager' : 'lazy'}
                                    decoding="async"
                                    className="aspect-[1.48/1] w-full object-cover"
                                />
                                <figcaption className="grid grid-cols-3 gap-2 border-t border-white/[0.08] bg-black/70 p-3 text-xs">
                                    <div>
                                        <p className="font-bold text-white">{item.channel}</p>
                                        <p className="mt-1 text-gray-500">channel</p>
                                    </div>
                                    <div>
                                        <p className="font-bold text-cyan-200">{item.watchTime}</p>
                                        <p className="mt-1 text-gray-500">watch time</p>
                                    </div>
                                    <div>
                                        <p className="font-bold text-emerald-200">{item.subscribers}</p>
                                        <p className="mt-1 text-gray-500">subs</p>
                                    </div>
                                </figcaption>
                            </figure>
                        ))}
                    </div>
                </div>
            </section>

            <section id="proof" className="scroll-mt-20 border-b border-white/[0.06] py-20">
                <div className="mx-auto max-w-7xl px-6">
                    <div className="mb-8 max-w-3xl">
                        <p className="text-sm font-semibold uppercase tracking-[0.18em] text-cyan-300">Real Output</p>
                        <h2 className="mt-3 text-3xl font-bold text-white sm:text-4xl">Proof from actual channels, not a fake demo reel.</h2>
                        <p className="mt-3 text-gray-400">
                            These are lightweight thumbnails on page load. The Lume examples are fully AI-made long-form videos, and the YouTube player only loads after someone chooses a video, keeping the landing page fast while showing real output across multiple channel styles.
                        </p>
                    </div>
                    <ProofVideoGrid videos={landingProofVideos} />
                </div>
            </section>

            <section className="border-b border-white/[0.06] py-20">
                <div className="mx-auto max-w-7xl px-6">
                    <div className="grid gap-10 lg:grid-cols-[0.9fr,1.1fr]">
                        <div>
                            <p className="text-sm font-semibold uppercase tracking-[0.18em] text-violet-300">One workspace</p>
                            <h2 className="mt-3 text-3xl font-bold text-white sm:text-4xl">Studio replaces the creator tool chain.</h2>
                            <p className="mt-4 text-gray-400">
                                Most AI video products solve one step. Studio is built around the whole production path: find the idea, shape the script, create the visuals, animate what matters, add sound, and ship.
                            </p>
                        </div>
                        <div className="grid gap-3 sm:grid-cols-2">
                            {stackSteps.map((step) => {
                                const Icon = step.icon;
                                return (
                                    <div key={step.title} className="rounded-lg border border-white/[0.08] bg-white/[0.03] p-4">
                                        <div className="flex items-center gap-3">
                                            <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-cyan-500/10 text-cyan-200">
                                                <Icon className="h-5 w-5" />
                                            </div>
                                            <h3 className="text-base font-bold text-white">{step.title}</h3>
                                        </div>
                                        <p className="mt-3 text-sm leading-6 text-gray-400">{step.desc}</p>
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                </div>
            </section>

            <section className="border-b border-white/[0.06] py-20">
                <div className="mx-auto max-w-7xl px-6">
                    <div className="mb-10 max-w-3xl">
                        <p className="text-sm font-semibold uppercase tracking-[0.18em] text-emerald-300">Why Studio wins</p>
                        <h2 className="mt-3 text-3xl font-bold text-white sm:text-4xl">No duct-taped workflow. No guessing what to make.</h2>
                    </div>
                    <div className="grid gap-4 md:grid-cols-3">
                        {[
                            'Connect a channel and let Catalyst learn what that audience actually watches.',
                            'Review stills before expensive animation, so credits go toward scenes worth keeping.',
                            'Keep creation, billing, progress, refunds, and publishing inside one product.',
                        ].map((item) => (
                            <div key={item} className="flex items-start gap-3 rounded-lg border border-white/[0.08] bg-white/[0.03] p-5">
                                <CheckCircle2 className="mt-0.5 h-5 w-5 shrink-0 text-emerald-300" />
                                <p className="text-sm leading-6 text-gray-300">{item}</p>
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            <section className="border-b border-white/[0.06] py-20">
                <div className="mx-auto max-w-7xl px-6">
                    <div className="mb-10 flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
                        <div className="max-w-3xl">
                            <p className="text-sm font-semibold uppercase tracking-[0.18em] text-cyan-300">Studio updates</p>
                            <h2 className="mt-3 text-3xl font-bold text-white sm:text-4xl">Recent product notes from the owner.</h2>
                            <p className="mt-3 text-gray-400">
                                A lightweight public update log for what changed, what shipped, and what is still being tested before wider release.
                            </p>
                        </div>
                        <p className="text-sm text-gray-500">Manual owner-written posts.</p>
                    </div>
                    <div className="grid gap-4 lg:grid-cols-3">
                        {updatePosts.map((post) => (
                            <article key={post.slug} className="rounded-lg border border-white/[0.08] bg-white/[0.03] p-5">
                                <div className="flex flex-wrap items-center gap-2 text-xs">
                                    <span className="rounded bg-cyan-400/10 px-2 py-1 font-bold uppercase tracking-[0.14em] text-cyan-200">{post.label}</span>
                                    <time className="text-gray-500" dateTime={post.date}>{post.date}</time>
                                </div>
                                <h3 className="mt-4 text-xl font-bold leading-tight text-white">{post.title}</h3>
                                <p className="mt-3 text-sm leading-6 text-gray-400">{post.summary}</p>
                                <ul className="mt-4 space-y-2">
                                    {post.bullets.map((bullet) => (
                                        <li key={bullet} className="flex gap-2 text-sm leading-6 text-gray-300">
                                            <CheckCircle2 className="mt-1 h-4 w-4 shrink-0 text-emerald-300" />
                                            <span>{bullet}</span>
                                        </li>
                                    ))}
                                </ul>
                            </article>
                        ))}
                    </div>
                </div>
            </section>

            <section id="pricing" className="scroll-mt-20 border-b border-white/[0.06] py-20">
                <div className="mx-auto max-w-6xl px-6">
                    <div className="mb-10 text-center">
                        <p className="text-sm font-semibold uppercase tracking-[0.18em] text-cyan-300">Pricing</p>
                        <h2 className="mt-3 text-3xl font-bold text-white sm:text-4xl">Pricing is public. Purchasing requires an account and card.</h2>
                        <p className="mx-auto mt-3 max-w-3xl text-gray-400">
                            Visitors can see the credit ladder before signing in. When they start a trial or subscribe, Stripe collects payment details first so credits cannot be abused.
                        </p>
                    </div>
                    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                        {publicPricingTiers.map((tier) => (
                            <div key={tier.label} className="flex min-h-[18rem] flex-col rounded-lg border border-white/[0.08] bg-white/[0.03] p-5">
                                <p className="text-xs font-bold uppercase tracking-[0.18em] text-cyan-300">{tier.label}</p>
                                <p className="mt-4 text-3xl font-black text-white">{tier.price}</p>
                                <p className="mt-2 text-sm font-semibold text-gray-200">{tier.credits} / month</p>
                                <p className="mt-4 flex-1 text-sm leading-6 text-gray-400">{tier.detail}</p>
                                <button
                                    type="button"
                                    onClick={openPricingPurchase}
                                    className="mt-5 inline-flex items-center justify-center gap-2 rounded-lg bg-violet-600 px-4 py-3 text-sm font-bold text-white transition hover:bg-violet-500"
                                >
                                    {session ? 'Choose plan' : 'Sign in to purchase'}
                                    <ArrowRight className="h-4 w-4" />
                                </button>
                            </div>
                        ))}
                    </div>
                    <div className="mt-6 rounded-lg border border-white/[0.06] bg-white/[0.02] p-6">
                        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
                            <div>
                                <p className="text-xs uppercase tracking-[0.18em] text-cyan-300">Top ups</p>
                                <h3 className="mt-2 text-2xl font-bold text-white">Need more credits mid-month?</h3>
                                <p className="mt-2 text-sm leading-6 text-gray-400">Top-up packs are available after login for heavier image-to-video runs and bulk production days.</p>
                            </div>
                            <button
                                type="button"
                                onClick={openPricingPurchase}
                                className="inline-flex items-center justify-center gap-2 rounded-lg bg-violet-600 px-5 py-3 text-sm font-bold text-white transition hover:bg-violet-500"
                            >
                                {session ? 'Open billing' : 'Sign in for billing'}
                                <ArrowRight className="h-4 w-4" />
                            </button>
                        </div>
                        <div className="mt-6 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                            {sortedPacks.slice(0, 4).map((pack) => (
                                <div key={pack.price_id} className="rounded-lg border border-white/[0.08] bg-black/20 p-4">
                                    <p className="text-sm font-semibold text-white">{String(pack.pack || '').toUpperCase()}</p>
                                    <p className="mt-1 text-xs text-gray-500">{pack.credits} credits</p>
                                    <p className="mt-4 text-2xl font-bold text-white">${Number(pack.price_usd || 0).toFixed(2)}</p>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>
            </section>

            <section className="py-24">
                <div className="mx-auto max-w-4xl px-6 text-center">
                    <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-lg border border-cyan-400/30 bg-cyan-400/10">
                        <Logo size={32} />
                    </div>
                    <h2 className="mt-6 text-3xl font-bold text-white sm:text-4xl">Open Studio and build from one conversation.</h2>
                    <p className="mx-auto mt-4 max-w-2xl text-gray-400">
                        Start with a topic, channel, or rough idea. Studio handles the production path and shows the work as it runs.
                    </p>
                    <div className="mt-8 flex flex-col justify-center gap-4 sm:flex-row">
                        <button
                            type="button"
                            onClick={openPricingPurchase}
                            className="inline-flex items-center justify-center gap-2 rounded-lg bg-cyan-500 px-7 py-3 text-base font-bold text-black transition hover:bg-cyan-300"
                        >
                            {session ? 'Choose a plan' : 'Start creating'}
                            <ArrowRight className="h-5 w-5" />
                        </button>
                        <button
                            type="button"
                            onClick={openGoogle}
                            disabled={googleLoading}
                            className="rounded-lg border border-white/[0.08] bg-white/[0.03] px-7 py-3 text-base font-semibold text-white transition hover:border-white/[0.14] hover:bg-white/[0.07] disabled:opacity-60"
                        >
                            {session ? 'Create a video' : googleLoading ? 'Opening Google...' : 'Continue with Google'}
                        </button>
                    </div>
                    {!billingHost && (
                        <p className="mt-6 text-xs text-gray-500">
                            <a href="/privacy" className="text-gray-400 underline-offset-2 hover:text-gray-200">Privacy Policy</a>
                            {' - '}
                            <a href="/terms" className="text-gray-400 underline-offset-2 hover:text-gray-200">Terms of Service</a>
                            {' - '}
                            <span className="text-gray-600">{STUDIO_SITE_URL}</span>
                        </p>
                    )}
                </div>
            </section>
        </>
    );
}

function StatCard({ label, value }: { label: string; value: string }) {
    return (
        <div className="rounded-lg border border-white/[0.08] bg-white/[0.03] px-4 py-3">
            <p className="text-2xl font-black text-white">{value}</p>
            <p className="mt-1 text-xs font-semibold uppercase tracking-[0.12em] text-gray-500">{label}</p>
        </div>
    );
}
