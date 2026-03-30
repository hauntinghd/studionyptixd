import { useContext, useEffect, useMemo, useState, type ComponentType } from 'react';
import { BarChart3, Clapperboard, Copy, Film, Image, LayoutDashboard, Monitor, PanelLeftOpen, Sparkles, Wand2 } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, isOwnerEmail } from '../shared';
import AdminAnalyticsPanel from '../panels/AdminAnalyticsPanel';
import AutoClipperPanel from '../panels/AutoClipperPanel';
import ClonePanel from '../panels/ClonePanel';
import CreatePanel from '../panels/CreatePanel';
import DemoPanel from '../panels/DemoPanel';
import LongFormPanel from '../panels/LongFormPanel';
import ThumbnailPanel from '../panels/ThumbnailPanel';

type DashboardTab = 'create' | 'clone' | 'longform' | 'thumbnails' | 'demo' | 'autoclipper' | 'analytics';

type SidebarItem = {
    id: DashboardTab;
    label: string;
    icon: ComponentType<{ className?: string }>;
    comingSoon?: boolean;
    hidden?: boolean;
};

export default function DashboardPage({ onNavigate }: { onNavigate: PageNav }) {
    const { session, role, backendOffline, creditsTotalRemaining, requiresTopup, monthlyCreditsRemaining } = useContext(AuthContext);
    const isAdmin = role === 'admin';
    const isOwner = isOwnerEmail(session?.user?.email);
    const ownerToolAccess = isOwner;
    const [tab, setTab] = useState<DashboardTab>('create');
    const [createWorkspaceOpen, setCreateWorkspaceOpen] = useState(false);
    const [sidebarPeekOpen, setSidebarPeekOpen] = useState(false);
    const acCredits = Number(creditsTotalRemaining || 0);
    const clipperCredits = Number(monthlyCreditsRemaining || 0);
    const greeting = useMemo(() => {
        const hour = new Date().getHours();
        if (hour < 12) return 'Good morning';
        if (hour < 18) return 'Good afternoon';
        return 'Good evening';
    }, []);

    useEffect(() => {
        if (!session) onNavigate('auth');
    }, [session, onNavigate]);

    if (!session) return null;

    const sidebarItems = ([{
        id: 'create',
        label: 'Create',
        icon: Sparkles,
    }, {
        id: 'clone',
        label: 'Clone',
        icon: Copy,
        comingSoon: !ownerToolAccess,
    }, {
        id: 'longform',
        label: 'Long Form',
        icon: Film,
        comingSoon: !ownerToolAccess,
    }, {
        id: 'thumbnails',
        label: 'Thumbnails',
        icon: Image,
        comingSoon: !ownerToolAccess,
    }, {
        id: 'demo',
        label: 'Product Demo',
        icon: Monitor,
        comingSoon: !ownerToolAccess,
    }, {
        id: 'autoclipper',
        label: 'Auto Clipper',
        icon: Clapperboard,
        comingSoon: !ownerToolAccess,
    }, {
        id: 'analytics',
        label: 'Product Analytics',
        icon: BarChart3,
        hidden: !isAdmin && !ownerToolAccess,
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
        const analyticsUnlocked = item.id === 'analytics' && (isAdmin || ownerToolAccess);
        const ownerUnlocked = item.id !== 'analytics' && ownerToolAccess;
        if (analyticsUnlocked || ownerUnlocked) {
            setTab(item.id);
            setCreateWorkspaceOpen(false);
            setSidebarPeekOpen(false);
            return;
        }
    };

    const createImmersive = tab === 'create' && createWorkspaceOpen;
    const sidebarVisible = !createImmersive || sidebarPeekOpen;

    const panel = (() => {
        if (tab === 'analytics' && (isAdmin || ownerToolAccess)) return <AdminAnalyticsPanel />;
        if (tab === 'clone' && ownerToolAccess) return <ClonePanel />;
        if (tab === 'longform' && ownerToolAccess) return <LongFormPanel />;
        if (tab === 'thumbnails' && ownerToolAccess) return <ThumbnailPanel />;
        if (tab === 'demo' && ownerToolAccess) return <DemoPanel />;
        if (tab === 'autoclipper' && ownerToolAccess) return <AutoClipperPanel />;
        return <CreatePanel />;
    })();

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

            <div className="pl-0 pr-4 sm:pr-6 lg:pr-8 pt-20 pb-8">
                <div className="flex items-start gap-5">
                    <aside
                        onMouseLeave={() => {
                            if (createImmersive) setSidebarPeekOpen(false);
                        }}
                        className={`shrink-0 overflow-hidden transition-all duration-300 ${
                            sidebarVisible ? 'w-[300px] opacity-100 lg:w-[312px]' : 'w-0 -translate-x-6 opacity-0 pointer-events-none'
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
                                            : item.id === 'analytics'
                                                ? !(isAdmin || ownerToolAccess)
                                                : !ownerToolAccess;
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
                                            {ownerToolAccess
                                                ? 'Owner preview is active on this account. Every Studio lane is open here while non-owner accounts still see the public launch surface.'
                                                : 'NYPTID Studio is now focused on one live path: Create. The rail shows the future surface, but only the sellable workflow is active.'}
                                        </p>
                                    </div>
                                    <div className="grid gap-3 sm:grid-cols-2">
                                        <MetricCard label="AI Credits" value={acCredits} accent="cyan" helper={requiresTopup ? 'Top up before your next animation run' : 'Ready to animate'} />
                                        <MetricCard label="Clipper Credits" value={clipperCredits} accent="violet" helper={ownerToolAccess ? 'Owner preview lane unlocked' : 'AutoClipper stays marked coming soon'} />
                                    </div>
                                </div>
                            </section>
                        )}

                        {backendOffline && !createImmersive && (
                            <div className="rounded-2xl border border-amber-400/30 bg-amber-500/10 px-5 py-4 text-sm text-amber-100">
                                <div className="mb-1 font-semibold">Hosted fallback mode active</div>
                                <div>Studio is still available. Local GPU lanes are offline, so the current session is leaning on the hosted fallback stack.</div>
                            </div>
                        )}

                        {panel}
                    </main>
                </div>
            </div>
        </div>
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
