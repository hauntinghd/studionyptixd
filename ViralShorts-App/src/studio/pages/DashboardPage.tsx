import { Suspense, lazy, useCallback, useContext, useEffect, useMemo, useState, type ComponentType } from 'react';
import { ArrowLeft, BarChart3, BrainCircuit, Clapperboard, Copy, Film, Image, LayoutDashboard, Loader2, Monitor, PanelLeftOpen, Receipt, Sparkles, Wand2 } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext } from '../shared';
import CreatePanel from '../panels/CreatePanel';

// Heavy/rarely-clicked panels are code-split via React.lazy — each one ships
// as its own chunk Vite serves on demand. Before this, the whole bundle
// shipped every panel upfront (~900 KB gz 230 KB), making cold refreshes
// feel slow. CreatePanel stays eager because it's the landing surface.
const AdminAnalyticsPanel = lazy(() => import('../panels/AdminAnalyticsPanel'));
const AutoClipperPanel = lazy(() => import('../panels/AutoClipperPanel'));
const CatalystPanel = lazy(() => import('../panels/CatalystPanel'));
const ClonePanel = lazy(() => import('../panels/ClonePanel'));
const DemoPanel = lazy(() => import('../panels/DemoPanel'));
const LongFormPanel = lazy(() => import('../panels/LongFormPanel'));
const RefundsPanel = lazy(() => import('../panels/RefundsPanel'));
const ThumbnailPanel = lazy(() => import('../panels/ThumbnailPanel'));

const PanelFallback = () => (
    <div className="flex h-[40vh] items-center justify-center gap-2 text-sm text-gray-500">
        <Loader2 className="h-4 w-4 animate-spin" /> Loading…
    </div>
);

type DashboardTab = 'create' | 'clone' | 'longform' | 'thumbnails' | 'demo' | 'autoclipper' | 'analytics' | 'catalyst' | 'refunds';

type SidebarItem = {
    id: DashboardTab;
    label: string;
    icon: ComponentType<{ className?: string }>;
    comingSoon?: boolean;
    hidden?: boolean;
};

const OWNER_ALL_ACCESS = {
    create: true,
    clone: true,
    longform: true,
    thumbnails: true,
    demo: true,
    autoclipper: true,
    analytics: true,
    catalyst: true,
    refunds: true,
};

export default function DashboardPage({ onNavigate }: { onNavigate: PageNav }) {
    const {
        session,
        loading,
        role,
        ownerOverride,
        topupCreditsRemaining,
        requiresTopup,
        monthlyCreditsRemaining,
        studioLaneAccess,
    } = useContext(AuthContext);
    const isAdmin = role === 'admin' || ownerOverride;
    const laneAccess = ownerOverride ? OWNER_ALL_ACCESS : studioLaneAccess;
    const [tab, setTab] = useState<DashboardTab>('create');
    const [createWorkspaceOpen, setCreateWorkspaceOpen] = useState(false);
    const [selectedNiche, setSelectedNiche] = useState<string | null>(null);
    const [sidebarPeekOpen, setSidebarPeekOpen] = useState(false);
    const walletCredits = Number(topupCreditsRemaining || 0);
    const includedCredits = Number(monthlyCreditsRemaining || 0);
    const greeting = useMemo(() => {
        const hour = new Date().getHours();
        if (hour < 12) return 'Good morning';
        if (hour < 18) return 'Good afternoon';
        return 'Good evening';
    }, []);
    const isTabUnlocked = useCallback((nextTab: DashboardTab) => {
        if (nextTab === 'create') return true;
        if (nextTab === 'analytics' || nextTab === 'catalyst' || nextTab === 'refunds') return isAdmin;
        return Boolean((laneAccess as Record<string, boolean>)[nextTab]);
    }, [isAdmin, laneAccess]);

    useEffect(() => {
        // Wait for the AuthContext to finish its initial Supabase session bootstrap
        // before deciding to redirect. Without this gate, a logged-in user hitting
        // refresh gets bounced to the auth page because `session` is null for the
        // first tick while getSession() reads localStorage.
        if (loading) return;
        if (!session) onNavigate('auth');
    }, [session, loading, onNavigate]);

    useEffect(() => {
        if (typeof window === 'undefined') return;
        const params = new URLSearchParams(window.location.search);
        const requestedTab = String(params.get('tab') || params.get('focus') || '').trim().toLowerCase();
        if (!requestedTab) return;
        const allowedTabs = new Set<DashboardTab>(['create', 'clone', 'longform', 'thumbnails', 'demo', 'autoclipper', 'analytics', 'catalyst', 'refunds']);
        if (!allowedTabs.has(requestedTab as DashboardTab)) return;
        const nextTab = requestedTab as DashboardTab;
        const unlocked = isTabUnlocked(nextTab);
        if (!unlocked) return;
        setTab(nextTab);
        setCreateWorkspaceOpen(nextTab === 'create');
    }, [isTabUnlocked]);

    useEffect(() => {
        if (tab === 'create') return;
        if (!isTabUnlocked(tab)) {
            setTab('create');
            setCreateWorkspaceOpen(false);
        }
    }, [isTabUnlocked, tab]);

    if (!session) return null;

    const sidebarItems = ([{
        id: 'create',
        label: 'Create',
        icon: Sparkles,
    }, {
        id: 'clone',
        label: 'Clone',
        icon: Copy,
        comingSoon: !ownerOverride,
    }, {
        id: 'longform',
        label: 'Long Form',
        icon: Film,
        comingSoon: !ownerOverride,
    }, {
        id: 'thumbnails',
        label: 'Thumbnails',
        icon: Image,
        comingSoon: !ownerOverride,
    }, {
        id: 'demo',
        label: 'Product Demo',
        icon: Monitor,
        hidden: !ownerOverride,
    }, {
        id: 'autoclipper',
        label: 'Auto Clipper',
        icon: Clapperboard,
        comingSoon: !ownerOverride,
    }, {
        id: 'analytics',
        label: 'Product Analytics',
        icon: BarChart3,
        hidden: !isAdmin,
    }, {
        id: 'catalyst',
        label: 'Catalyst',
        icon: BrainCircuit,
        hidden: !isAdmin,
    }, {
        id: 'refunds',
        label: 'Refunds',
        icon: Receipt,
        hidden: !isAdmin,
    }] as SidebarItem[]).filter((item) => !item.hidden);

    const openCreateWorkspace = () => {
        setTab('create');
        setCreateWorkspaceOpen(true);
        setSidebarPeekOpen(false);
    };

    const selectTab = (item: SidebarItem) => {
        if (item.id === 'create') {
            openCreateWorkspace();
            return;
        }
        const unlocked = isTabUnlocked(item.id);
        if (unlocked && !item.comingSoon) {
            setTab(item.id);
            setCreateWorkspaceOpen(false);
            setSidebarPeekOpen(false);
        }
    };

    const createImmersive = tab === 'create' && createWorkspaceOpen;
    const sidebarVisible = !createImmersive || sidebarPeekOpen;

    const lazyPanel = (node: React.ReactNode) => (
        <Suspense fallback={<PanelFallback />}>{node}</Suspense>
    );
    const panel = (() => {
        if (tab === 'analytics' && isAdmin) return lazyPanel(<AdminAnalyticsPanel />);
        if (tab === 'catalyst' && isAdmin) return lazyPanel(<CatalystPanel />);
        if (tab === 'refunds' && isAdmin) return lazyPanel(<RefundsPanel />);
        if (tab === 'clone' && laneAccess.clone) return lazyPanel(<ClonePanel />);
        if (tab === 'longform' && laneAccess.longform) return lazyPanel(<LongFormPanel />);
        if (tab === 'thumbnails' && laneAccess.thumbnails) return lazyPanel(<ThumbnailPanel />);
        if (tab === 'demo' && ownerOverride) return lazyPanel(<DemoPanel />);
        if (tab === 'autoclipper' && ownerOverride) return lazyPanel(<AutoClipperPanel />);
        return <CreatePanel initialTemplate={selectedNiche ?? undefined} />;
    })();

    // Only show the Create workspace once the user has picked a niche. Otherwise
    // the niche gallery is the landing view — a passive CreatePanel below it was
    // cluttering the first-run experience.
    const panelVisible = tab !== 'create' || createImmersive;
    const exitCreateWorkspace = () => {
        setCreateWorkspaceOpen(false);
        setSelectedNiche(null);
    };

    return (
        <div className="min-h-screen">
            <NavBar onNavigate={onNavigate} active="dashboard" />

            {createImmersive && !sidebarVisible && (
                <button
                    type="button"
                    onClick={() => setSidebarPeekOpen((value) => !value)}
                    onMouseEnter={() => setSidebarPeekOpen(true)}
                    className="fixed left-3 top-[92px] z-30 inline-flex items-center gap-2 rounded-full border border-white/[0.08] bg-[#0d0d11]/95 px-3 py-2 text-xs font-semibold text-white shadow-lg shadow-black/40 transition hover:border-violet-500/40 hover:bg-violet-500/10"
                >
                    <PanelLeftOpen className="h-4 w-4 text-violet-300" />
                    Menu
                </button>
            )}

            <div className="pl-0 pr-4 pt-20 pb-8 sm:pr-6 lg:pr-8">
                <div className="flex items-start gap-5">
                    <aside
                        onMouseLeave={() => {
                            if (createImmersive) setSidebarPeekOpen(false);
                        }}
                        className={`shrink-0 overflow-hidden transition-all duration-300 ${
                            sidebarVisible ? 'w-[300px] opacity-100 lg:w-[312px]' : 'pointer-events-none w-0 -translate-x-6 opacity-0'
                        }`}
                    >
                        <div className="rounded-none rounded-r-[30px] border border-l-0 border-white/[0.06] bg-white/[0.02] p-4">
                            <button
                                type="button"
                                onClick={openCreateWorkspace}
                                className="flex w-full items-center gap-3 rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-4 text-left text-white transition hover:border-violet-500/40 hover:bg-violet-500/10"
                            >
                                <Wand2 className="h-5 w-5 text-violet-300" />
                                <span className="font-semibold">Create New</span>
                            </button>

                            <div className="mt-4 rounded-2xl border border-white/[0.06] bg-black/20 p-2">
                                <div className="space-y-1">
                                    {sidebarItems.map((item) => {
                                        const active = tab === item.id;
                                        const disabled = item.id === 'create'
                                            ? false
                                            : item.comingSoon || !isTabUnlocked(item.id);
                                        const Icon = item.icon;
                                        return (
                                            <button
                                                key={item.id}
                                                type="button"
                                                onClick={() => selectTab(item)}
                                                disabled={disabled}
                                                className={`flex w-full items-center justify-between gap-3 rounded-xl px-3 py-3 text-sm transition ${
                                                    active ? 'bg-white/[0.08] text-white' : 'text-gray-400 hover:bg-white/[0.04] hover:text-white'
                                                } ${disabled ? 'cursor-not-allowed opacity-85' : ''}`}
                                            >
                                                <span className="flex items-center gap-3">
                                                    <Icon className={`h-4 w-4 ${active ? 'text-violet-300' : 'text-gray-500'}`} />
                                                    {item.label}
                                                </span>
                                                {item.comingSoon && (
                                                    <span className="rounded border border-amber-500/30 bg-amber-500/10 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-[0.18em] text-amber-200">
                                                        Soon
                                                    </span>
                                                )}
                                            </button>
                                        );
                                    })}
                                </div>
                            </div>
                        </div>
                    </aside>

                    <main className="min-w-0 flex-1 space-y-6">
                        {!createImmersive && (
                            <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                                <div className="flex flex-wrap items-start justify-between gap-6">
                                    <div>
                                        <div className="flex items-center gap-2 text-sm font-semibold text-violet-300">
                                            <LayoutDashboard className="h-4 w-4" />
                                            Workspace
                                        </div>
                                        <h1 className="mt-3 text-4xl font-bold text-white">{greeting}, {session.user.email?.split('@')[0] || 'creator'}</h1>
                                        <p className="mt-3 max-w-3xl text-sm text-gray-400">
                                            {ownerOverride
                                                ? 'Owner preview — every Studio lane is open on this account.'
                                                : 'Pick a niche and start creating. Nine live niches: Skeleton AI, Day Trading, Moral Dilemma, Business, Finance, Tech, Crypto, Scary Stories, Historical Epic.'}
                                        </p>
                                    </div>
                                    <div className="grid gap-3 sm:grid-cols-2">
                                        <MetricCard
                                            label="Credit Wallet"
                                            value={walletCredits}
                                            accent="cyan"
                                            helper={requiresTopup ? 'Top up before your next animation run' : 'Wallet ready for heavier usage'}
                                        />
                                        <MetricCard
                                            label="Included Credits"
                                            value={includedCredits}
                                            accent="violet"
                                            helper={ownerOverride ? 'Owner preview lane unlocked' : 'Membership burns before wallet credits'}
                                        />
                                    </div>
                                </div>
                            </section>
                        )}

                        {tab === 'create' && !createImmersive && (
                            <NicheGallery onPick={(nicheId) => {
                                setSelectedNiche(nicheId);
                                setCreateWorkspaceOpen(true);
                            }} />
                        )}

                        {tab === 'create' && createImmersive && (
                            <div className="flex items-center justify-between">
                                <button
                                    type="button"
                                    onClick={exitCreateWorkspace}
                                    className="inline-flex items-center gap-2 rounded-full border border-white/[0.08] bg-white/[0.02] px-4 py-2 text-sm font-semibold text-gray-300 transition hover:border-violet-500/40 hover:bg-violet-500/10 hover:text-white"
                                >
                                    <ArrowLeft className="h-4 w-4" />
                                    Back to Dashboard
                                </button>
                            </div>
                        )}

                        {panelVisible && panel}
                    </main>
                </div>
            </div>
        </div>
    );
}

function NicheGallery({ onPick }: { onPick: (nicheId: string) => void }) {
    // Keep in sync with CreatePanel.tsx `templates` array (same ids, titles, icons).
    // All 9 live niches appear here so the gallery matches the Switch-Niche modal
    // inside the Create workspace.
    const niches: { id: string; title: string; desc: string; icon: string; badge?: string }[] = [
        { id: 'skeleton', title: 'Skeleton AI', desc: '3D skeleton comparison shorts', icon: '💀', badge: 'Most Popular' },
        { id: 'daytrading', title: 'Day Trading', desc: 'Hook-forward trading shorts', icon: '📈', badge: 'Trending' },
        { id: 'dilemma', title: 'Moral Dilemma', desc: 'Forced binary-choice CTAs', icon: '⚖️', badge: 'New' },
        { id: 'business', title: 'Business', desc: 'Founder and operator stories', icon: '💼' },
        { id: 'finance', title: 'Finance', desc: 'Money and markets explainers', icon: '💸' },
        { id: 'tech', title: 'Tech', desc: 'AI and startup updates', icon: '🧠' },
        { id: 'crypto', title: 'Crypto', desc: 'Crypto trends and narratives', icon: '₿' },
        { id: 'scary', title: 'Scary Stories', desc: 'Horror & true-crime atmosphere', icon: '👻', badge: 'Trending' },
        { id: 'history', title: 'Historical Epic', desc: 'Ridley-Scott-scale visuals', icon: '⚔️', badge: 'Trending' },
    ];
    return (
        <section className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
            <div className="mb-5 flex items-baseline justify-between">
                <h2 className="text-lg font-bold text-white">🔥 Currently trending niches</h2>
                <span className="text-xs uppercase tracking-[0.18em] text-gray-500">Click any tile to start</span>
            </div>
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                {niches.map((n) => (
                    <button
                        key={n.id}
                        type="button"
                        onClick={() => onPick(n.id)}
                        className="group relative overflow-hidden rounded-2xl border border-white/[0.08] bg-zinc-900 text-left transition hover:border-violet-500/40 hover:shadow-lg hover:shadow-violet-900/30"
                    >
                        <div className="relative aspect-[16/10]">
                            <img
                                src={`/niche_thumbs/${n.id}.jpg`}
                                alt={n.title}
                                loading="lazy"
                                className="absolute inset-0 h-full w-full object-cover transition group-hover:scale-105"
                            />
                            <div className="absolute inset-0 bg-gradient-to-t from-black/85 via-black/30 to-transparent" />
                            <div className="absolute inset-x-0 bottom-0 p-4">
                                <div className="flex items-center gap-2">
                                    <span className="text-xl drop-shadow-[0_2px_4px_rgba(0,0,0,0.6)]">{n.icon}</span>
                                    <h3 className="text-base font-bold text-white drop-shadow-[0_2px_4px_rgba(0,0,0,0.6)]">{n.title}</h3>
                                </div>
                                <p className="mt-1 text-xs text-gray-300 drop-shadow-[0_2px_4px_rgba(0,0,0,0.6)]">{n.desc}</p>
                            </div>
                            {n.badge && (
                                <span className="absolute right-3 top-3 rounded-full border border-white/20 bg-black/50 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-white backdrop-blur-sm">
                                    {n.badge}
                                </span>
                            )}
                        </div>
                    </button>
                ))}
            </div>
        </section>
    );
}

function MetricCard({ label, value, helper, accent }: { label: string; value: number; helper: string; accent: 'cyan' | 'violet' }) {
    const accentClasses = accent === 'cyan'
        ? 'border-cyan-500/20 bg-cyan-500/10 text-cyan-100'
        : 'border-violet-500/20 bg-violet-500/10 text-violet-100';

    return (
        <div className={`rounded-2xl border px-4 py-3 ${accentClasses}`}>
            <p className="text-[10px] uppercase tracking-[0.18em] opacity-70">{label}</p>
            <p className="mt-2 text-2xl font-bold">{value}</p>
            <p className="mt-1 text-xs opacity-75">{helper}</p>
        </div>
    );
}
