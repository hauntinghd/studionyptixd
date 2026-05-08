import { Suspense, lazy, useCallback, useContext, useEffect, useMemo, useState, type ComponentType } from 'react';
import { ArrowLeft, BarChart3, BrainCircuit, Film, LayoutDashboard, Loader2, PanelLeftOpen, Receipt, Sparkles, Users, Wand2 } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext } from '../shared';
import CreatePanel from '../panels/CreatePanel';

// Studio strip-down 2026-05-05 (per Casey): kept only Create, Catalyst, Refunds,
// Waitlist, Product Analytics. Removed: Clone, Long Form, Thumbnails, Product Demo,
// Auto Clipper. Their pipelines are being rebuilt from scratch one at a time,
// starting with Skeleton AI (inside Create) — and now Long Form (its own tab,
// 6 channels with Catalyst-fed outlines).
const AdminAnalyticsPanel = lazy(() => import('../panels/AdminAnalyticsPanel'));
const CatalystPanel = lazy(() => import('../panels/CatalystPanel'));
const LongFormPanel = lazy(() => import('../panels/LongFormPanel'));
const RefundsPanel = lazy(() => import('../panels/RefundsPanel'));
const WaitlistPanel = lazy(() => import('../panels/WaitlistPanel'));
const ZeroTierPrivatePanel = lazy(() => import('../panels/ZeroTierPrivatePanel'));
const AltHistoryPrivatePanel = lazy(() => import('../panels/AltHistoryPrivatePanel'));
const HistoryRewindPrivatePanel = lazy(() => import('../panels/HistoryRewindPrivatePanel'));

const PanelFallback = () => (
    <div className="flex h-[40vh] items-center justify-center gap-2 text-sm text-gray-500">
        <Loader2 className="h-4 w-4 animate-spin" /> Loading…
    </div>
);

type DashboardTab = 'create' | 'longform' | 'analytics' | 'catalyst' | 'refunds' | 'waitlist';

type SidebarItem = {
    id: DashboardTab;
    label: string;
    icon: ComponentType<{ className?: string }>;
    comingSoon?: boolean;
    hidden?: boolean;
};

const OWNER_ALL_ACCESS = {
    create: true,
    longform: true,
    analytics: true,
    catalyst: true,
    refunds: true,
    waitlist: true,
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
    // Hydrate selectedNiche from `?niche=X` URL param so that returning from
    // an external flow (e.g. YouTube OAuth) lands the user back on the niche
    // surface they kicked off from instead of the niche gallery root.
    const [selectedNiche, setSelectedNiche] = useState<string | null>(() => {
        if (typeof window === 'undefined') return null;
        try {
            const p = new URL(window.location.href).searchParams.get('niche');
            return p ? String(p).trim() || null : null;
        } catch { return null; }
    });
    const [sidebarPeekOpen, setSidebarPeekOpen] = useState(false);

    // If we hydrated a niche from the URL, auto-open the create workspace so
    // the user lands directly inside the niche panel rather than seeing the
    // gallery first. Runs once on mount.
    useEffect(() => {
        if (selectedNiche) setCreateWorkspaceOpen(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);
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
        if (nextTab === 'longform') return isAdmin;   // owner-only until public launch
        if (nextTab === 'analytics' || nextTab === 'catalyst' || nextTab === 'refunds' || nextTab === 'waitlist') return isAdmin;
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
        const allowedTabs = new Set<DashboardTab>(['create', 'analytics', 'catalyst', 'refunds', 'waitlist']);
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
        id: 'longform',
        label: 'Long Form',
        icon: Film,
        hidden: !isAdmin,
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
    }, {
        id: 'waitlist',
        label: 'Waitlist',
        icon: Users,
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
        if (tab === 'longform' && isAdmin) return lazyPanel(<LongFormPanel />);
        if (tab === 'analytics' && isAdmin) return lazyPanel(<AdminAnalyticsPanel />);
        if (tab === 'catalyst' && isAdmin) return lazyPanel(<CatalystPanel />);
        if (tab === 'refunds' && isAdmin) return lazyPanel(<RefundsPanel />);
        if (tab === 'waitlist' && isAdmin) return lazyPanel(<WaitlistPanel />);
        // ZeroTier (Private) niche gets its own Catalyst-powered surface.
        if (selectedNiche === 'zerotier_private' && isAdmin) return lazyPanel(<ZeroTierPrivatePanel />);
        // Alt-History (Private) — same Catalyst-fed UX as ZT Private but
        // for Cryptic Science (where alt-battles uploads land). Delegates
        // the actual render to alt_battles CreatePanel via onPickNiche so
        // the topic is prefilled.
        if (selectedNiche === 'alt_history_private' && isAdmin) {
            return lazyPanel(<AltHistoryPrivatePanel onPickNiche={(n, opts) => {
                setSelectedNiche(n);
                if (opts?.topic) {
                    const u = new URL(window.location.href);
                    u.searchParams.set('topic', opts.topic);
                    window.history.replaceState({}, '', u.toString());
                }
            }} />);
        }
        // History Rewind (Private) — Catalyst-fed sleep-doc topic surface
        // for the HR channel. Render delegates to the existing Long-Form
        // panel (sleep_doc pipeline). On "Build with this topic" we flip
        // to the longform tab — HistoryRewindPrivatePanel has already
        // stashed topic + channel-key in sessionStorage so LongFormPanel
        // hydrates them on mount.
        if (selectedNiche === 'history_rewind_private' && isAdmin) {
            return lazyPanel(<HistoryRewindPrivatePanel onLongformHandoff={() => {
                setTab('longform');
                setSelectedNiche(null);
                setCreateWorkspaceOpen(false);
            }} />);
        }
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
                    className="fixed left-3 top-[80px] z-30 inline-flex items-center gap-1.5 rounded-full border border-white/[0.08] bg-[#0d0d11]/95 px-2.5 py-1.5 text-[11px] font-semibold text-white shadow-lg shadow-black/40 transition hover:border-violet-500/40 hover:bg-violet-500/10"
                >
                    <PanelLeftOpen className="h-3.5 w-3.5 text-violet-300" />
                    Menu
                </button>
            )}

            <div className="pl-0 pr-4 pt-[72px] pb-6 sm:pr-6 lg:pr-8">
                <div className="flex items-start gap-4">
                    <aside
                        onMouseLeave={() => {
                            if (createImmersive) setSidebarPeekOpen(false);
                        }}
                        className={`shrink-0 overflow-hidden transition-all duration-300 ${
                            sidebarVisible ? 'w-[244px] opacity-100 lg:w-[256px]' : 'pointer-events-none w-0 -translate-x-6 opacity-0'
                        }`}
                    >
                        <div className="rounded-none rounded-r-2xl border border-l-0 border-white/[0.06] bg-white/[0.02] p-3">
                            <button
                                type="button"
                                onClick={openCreateWorkspace}
                                className="flex w-full items-center gap-2.5 rounded-xl border border-white/[0.08] bg-black/20 px-3 py-2.5 text-left text-white transition hover:border-violet-500/40 hover:bg-violet-500/10"
                            >
                                <Wand2 className="h-4 w-4 text-violet-300" />
                                <span className="text-sm font-semibold">Create New</span>
                            </button>

                            <div className="mt-2 rounded-xl border border-white/[0.06] bg-black/20 p-1.5">
                                <div className="space-y-0.5">
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
                                                className={`flex w-full items-center justify-between gap-2 rounded-lg px-2.5 py-2 text-[13px] transition ${
                                                    active ? 'bg-white/[0.08] text-white' : 'text-gray-400 hover:bg-white/[0.04] hover:text-white'
                                                } ${disabled ? 'cursor-not-allowed opacity-85' : ''}`}
                                            >
                                                <span className="flex items-center gap-2.5">
                                                    <Icon className={`h-4 w-4 ${active ? 'text-violet-300' : 'text-gray-500'}`} />
                                                    {item.label}
                                                </span>
                                                {item.comingSoon && (
                                                    <span className="rounded border border-amber-500/30 bg-amber-500/10 px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-[0.18em] text-amber-200">
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

                    <main className="min-w-0 flex-1 space-y-4">
                        {!createImmersive && (
                            <section className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
                                <div className="flex flex-wrap items-start justify-between gap-4">
                                    <div>
                                        <div className="flex items-center gap-1.5 text-xs font-semibold text-violet-300">
                                            <LayoutDashboard className="h-3.5 w-3.5" />
                                            Workspace
                                        </div>
                                        <h1 className="mt-1.5 text-2xl font-bold text-white">{greeting}, {session.user.email?.split('@')[0] || 'creator'}</h1>
                                        <p className="mt-1.5 max-w-2xl text-[13px] text-gray-400">
                                            {ownerOverride
                                                ? 'Owner preview — every Studio lane is open on this account.'
                                                : 'Pick a niche and start creating. Four live niches: Alternate History Battles, Moral Dilemma, Scary Stories, Historical Epic.'}
                                        </p>
                                    </div>
                                    <div className="grid gap-2.5 sm:grid-cols-2">
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
                            <NicheGallery
                                isOwner={ownerOverride}
                                onPick={(nicheId) => {
                                    setSelectedNiche(nicheId);
                                    setCreateWorkspaceOpen(true);
                                }}
                            />
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

function NicheGallery({ onPick, isOwner }: { onPick: (nicheId: string) => void; isOwner: boolean }) {
    // Keep in sync with CreatePanel.tsx `templates` array (same ids, titles, icons).
    // 4 public niches + 1 owner-only ZeroTier private niche (DC speedster fan-fic shorts).
    // Niche taxonomy refactor 2026-05-08:
    //   - Removed: skeleton, daytrading, business, finance, tech, crypto
    //   - Replaced Skeleton AI slot with Alternate History Battles (alt_battles)
    //   - Added private ZeroTier niche (owner-only, hidden from regular users)
    const niches: { id: string; title: string; desc: string; icon: string; badge?: string; ownerOnly?: boolean }[] = [
        { id: 'alt_battles', title: 'Alt-History Battles', desc: 'Napoleon vs Alexander, Romans vs Aztecs — AI battle scenes', icon: '🛡️', badge: 'New' },
        { id: 'dilemma', title: 'Moral Dilemma', desc: 'Forced binary-choice CTAs', icon: '⚖️', badge: 'Trending' },
        { id: 'scary', title: 'Scary Stories', desc: 'Horror & true-crime atmosphere', icon: '👻', badge: 'Trending' },
        { id: 'history', title: 'Historical Epic', desc: 'Ridley-Scott-scale visuals', icon: '⚔️', badge: 'Trending' },
        { id: 'zerotier_private', title: 'ZeroTier (Private)', desc: 'DC speedster comic-book shorts — owner only', icon: '⚡', badge: 'Private', ownerOnly: true },
        { id: 'alt_history_private', title: 'Alt-History (Private)', desc: 'Catalyst-fed alt-battles for Cryptic Science — owner only', icon: '🛡️', badge: 'Private', ownerOnly: true },
        { id: 'history_rewind_private', title: 'History Rewind (Private)', desc: '9-hour sleep-doc topics for History Rewind — owner only', icon: '📜', badge: 'Private', ownerOnly: true },
    ].filter((n) => !n.ownerOnly || isOwner);
    return (
        <section className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
            <div className="mb-3 flex items-baseline justify-between">
                <h2 className="text-base font-bold text-white">🔥 Currently trending niches</h2>
                <span className="text-[11px] uppercase tracking-[0.18em] text-gray-500">Click any tile to start</span>
            </div>
            <div className="grid gap-2.5 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
                {niches.map((n) => (
                    <button
                        key={n.id}
                        type="button"
                        onClick={() => onPick(n.id)}
                        className="group relative overflow-hidden rounded-xl border border-white/[0.08] bg-zinc-900 text-left transition hover:border-violet-500/40 hover:shadow-lg hover:shadow-violet-900/30"
                    >
                        <div className="relative aspect-[16/9]">
                            <img
                                src={`/niche_thumbs/${n.id}.jpg`}
                                alt={n.title}
                                loading="lazy"
                                className="absolute inset-0 h-full w-full object-cover transition group-hover:scale-105"
                            />
                            <div className="absolute inset-0 bg-gradient-to-t from-black/85 via-black/30 to-transparent" />
                            <div className="absolute inset-x-0 bottom-0 p-3">
                                <div className="flex items-center gap-1.5">
                                    <span className="text-base drop-shadow-[0_2px_4px_rgba(0,0,0,0.6)]">{n.icon}</span>
                                    <h3 className="text-sm font-bold text-white drop-shadow-[0_2px_4px_rgba(0,0,0,0.6)]">{n.title}</h3>
                                </div>
                                <p className="mt-0.5 text-[11px] text-gray-300 drop-shadow-[0_2px_4px_rgba(0,0,0,0.6)]">{n.desc}</p>
                            </div>
                            {n.badge && (
                                <span className="absolute right-2 top-2 rounded-full border border-white/20 bg-black/50 px-2 py-0.5 text-[9px] font-semibold uppercase tracking-[0.18em] text-white backdrop-blur-sm">
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
        <div className={`rounded-xl border px-3 py-2.5 ${accentClasses}`}>
            <p className="text-[10px] uppercase tracking-[0.18em] opacity-70">{label}</p>
            <p className="mt-1 text-xl font-bold leading-tight">{value}</p>
            <p className="mt-0.5 text-[11px] opacity-75">{helper}</p>
        </div>
    );
}
