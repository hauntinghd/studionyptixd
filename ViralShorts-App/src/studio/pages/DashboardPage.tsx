import { Suspense, lazy, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { ArrowLeft, Loader2 } from 'lucide-react';
import type { PageNav } from '../components/NavBar';
import StudioShell from '../components/layout/StudioShell';
import StudioSidebar, { buildSidebarItems } from '../components/layout/StudioSidebar';
import { StudioHomeHero, StudioToolsRow } from '../components/home/NicheGalleryV2';
import ChannelHomePanel from '../components/home/ChannelHomePanel';
import RenderTierPicker from '../components/home/RenderTierPicker';
import {
    nicheById,
    type DashboardTab,
    type NicheId,
    type RenderTierId,
} from '../lib/studioProduct';
import { AuthContext } from '../shared';
import CreatePanel from '../panels/CreatePanel';

const AdminAnalyticsPanel = lazy(() => import('../panels/AdminAnalyticsPanel'));
const CatalystPanel = lazy(() => import('../panels/CatalystPanel'));
const LongFormPanel = lazy(() => import('../panels/LongFormPanel'));
const AgentPanel = lazy(() => import('../panels/AgentPanel'));
const ThumbnailPanel = lazy(() => import('../panels/ThumbnailPanel'));
const ClipLabPanel = lazy(() => import('../panels/ClipLabPanel'));
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

const OWNER_ALL_ACCESS: Record<string, boolean> = {
    create: true,
    agent: true,
    longform: true,
    thumbnails: true,
    cliplab: true,
    analytics: true,
    catalyst: true,
    refunds: true,
    waitlist: true,
    campus: true,
};

function tabFromUrl(): DashboardTab {
    if (typeof window === 'undefined') return 'agent';
    try {
        const t = new URL(window.location.href).searchParams.get('tab');
        if (!t || t === 'home' || t === 'campus' || t === 'network' || t === 'wins' || t === 'leaderboard' || t === 'checklist') {
            return 'agent';
        }
        const allowed: DashboardTab[] = [
            'create', 'agent', 'longform', 'thumbnails', 'cliplab', 'analytics', 'catalyst', 'refunds', 'waitlist',
        ];
        if (t && allowed.includes(t as DashboardTab)) return t as DashboardTab;
    } catch {
        /* ignore */
    }
    return 'agent';
}

export default function DashboardPage({ onNavigate }: { onNavigate: PageNav }) {
    const { session, loading, role, ownerOverride, studioLaneAccess } = useContext(AuthContext);
    const isAdmin = role === 'admin' || ownerOverride;
    const laneAccess = ownerOverride ? OWNER_ALL_ACCESS : studioLaneAccess;

    const [tab, setTab] = useState<DashboardTab>(tabFromUrl);
    const [createOpen, setCreateOpen] = useState(() => tabFromUrl() === 'create');
    const [selectedNiche, setSelectedNiche] = useState<NicheId | null>(() => {
        if (typeof window === 'undefined') return null;
        try {
            const p = new URL(window.location.href).searchParams.get('niche');
            return p ? (String(p).trim() as NicheId) : null;
        } catch {
            return null;
        }
    });
    const [renderTier, setRenderTier] = useState<RenderTierId>('draft');

    const greeting = useMemo(() => {
        const hour = new Date().getHours();
        if (hour < 12) return 'Good morning';
        if (hour < 18) return 'Good afternoon';
        return 'Good evening';
    }, []);

    const canUseAgent = isAdmin || Boolean((laneAccess as Record<string, boolean>).agent);

    const isTabUnlocked = useCallback(
        (nextTab: DashboardTab) => {
            if (nextTab === 'home' || nextTab === 'create') return true;
            if (nextTab === 'longform') return isAdmin;
            if (nextTab === 'campus' || nextTab === 'network' || nextTab === 'wins' || nextTab === 'leaderboard') return canUseAgent;
            if (nextTab === 'agent') return canUseAgent;
            if (nextTab === 'thumbnails') return isAdmin || Boolean((laneAccess as Record<string, boolean>).thumbnails);
            if (nextTab === 'cliplab') return isAdmin || Boolean((laneAccess as Record<string, boolean>).cliplab);
            if (['analytics', 'catalyst', 'refunds', 'waitlist'].includes(nextTab)) return isAdmin;
            return Boolean((laneAccess as Record<string, boolean>)[nextTab]);
        },
        [isAdmin, canUseAgent, laneAccess],
    );

    useEffect(() => {
        if (loading) return;
        if (!session) onNavigate('auth');
    }, [session, loading, onNavigate]);

    useEffect(() => {
        if (loading) return;
        const urlTab = tabFromUrl();
        const hasExplicitTab = (() => {
            if (typeof window === 'undefined') return false;
            try {
                return new URL(window.location.href).searchParams.has('tab');
            } catch {
                return false;
            }
        })();
        if (!hasExplicitTab) {
            setTab('agent');
            try {
                const u = new URL(window.location.href);
                u.searchParams.set('tab', 'agent');
                window.history.replaceState({}, '', u.toString());
            } catch {
                /* ignore */
            }
            setCreateOpen(false);
            setSelectedNiche(null);
            return;
        }
        if (urlTab === 'agent' && canUseAgent) {
            setTab('agent');
            setCreateOpen(false);
            setSelectedNiche(null);
            return;
        }
        if (urlTab === 'agent' && !canUseAgent) {
            setTab('agent');
            setCreateOpen(false);
            setSelectedNiche(null);
            return;
        }
        if (urlTab === 'longform' && isAdmin) {
            setTab('longform');
            setCreateOpen(false);
            setSelectedNiche(null);
            return;
        }
        if (urlTab === 'thumbnails' && (isAdmin || laneAccess.thumbnails)) {
            setTab('thumbnails');
            setCreateOpen(false);
            setSelectedNiche(null);
            return;
        }
        try {
            const focus = new URL(window.location.href).searchParams.get('focus');
            if (focus === 'thumbnails' && (isAdmin || laneAccess.thumbnails)) {
                setTab('thumbnails');
                setCreateOpen(false);
                setSelectedNiche(null);
            }
        } catch { /* ignore */ }
        // Only open niche builder when URL doesn't explicitly request another tab.
        if (selectedNiche && (urlTab === 'home' || urlTab === 'create' || !urlTab)) {
            setCreateOpen(true);
            setTab('create');
        }
    }, [loading, isAdmin, canUseAgent, selectedNiche]);

    if (!session) {
        if (loading) {
            return (
                <StudioShell onNavigate={onNavigate}>
                    <PanelFallback />
                </StudioShell>
            );
        }
        return null;
    }

    const sidebarItems = buildSidebarItems(isAdmin, laneAccess as Record<string, boolean>);
    const displayName = session.user.email?.split('@')[0] || 'creator';

    const openAgent = () => {
        if (!canUseAgent) return;
        selectTab('agent');
    };

    const selectTab = (id: DashboardTab) => {
        const normalizedId: DashboardTab = (
            id === 'home' || id === 'campus' || id === 'network' || id === 'wins' || id === 'leaderboard'
        ) ? 'agent' : id;
        if (!isTabUnlocked(normalizedId)) return;
        try {
            const u = new URL(window.location.href);
            u.searchParams.set('tab', normalizedId);
            if (normalizedId === 'agent' || normalizedId === 'longform') {
                u.searchParams.delete('niche');
            }
            window.history.replaceState({}, '', u.toString());
        } catch {
            /* ignore */
        }
        if (normalizedId === 'create') {
            setTab('create');
            setCreateOpen(true);
            setSelectedNiche(null);
            return;
        }
        setTab(normalizedId);
        setCreateOpen(false);
        setSelectedNiche(null);
    };

    const handleTool = (action: string) => {
        if (action === 'agent') {
            if (canUseAgent) openAgent();
            return;
        }
        if (action === 'longform' && isAdmin) {
            selectTab('longform');
            return;
        }
        if (action === 'campus') return openAgent();
        if (action === 'thumbnails' && (isAdmin || laneAccess.thumbnails)) {
            selectTab('thumbnails');
            return;
        }
        if (action === 'cliplab' && (isAdmin || (laneAccess as Record<string, boolean>).cliplab)) {
            selectTab('cliplab');
            return;
        }
        if (action === 'automate') return;
        selectTab('create');
    };

    const lazyPanel = (node: React.ReactNode) => (
        <Suspense fallback={<PanelFallback />}>{node}</Suspense>
    );

    const panel = (() => {
        if (tab === 'longform' && isAdmin) return lazyPanel(<LongFormPanel />);
        if (tab === 'thumbnails' && (isAdmin || laneAccess.thumbnails)) return lazyPanel(<ThumbnailPanel />);
        if (tab === 'cliplab' && (isAdmin || (laneAccess as Record<string, boolean>).cliplab)) {
            return lazyPanel(<ClipLabPanel />);
        }
        if (tab === 'agent' && canUseAgent) {
            return lazyPanel(<AgentPanel />);
        }
        if (tab === 'analytics' && isAdmin) return lazyPanel(<AdminAnalyticsPanel />);
        if (tab === 'catalyst' && isAdmin) return lazyPanel(<CatalystPanel />);
        if (tab === 'refunds' && isAdmin) return lazyPanel(<RefundsPanel />);
        if (tab === 'waitlist' && isAdmin) return lazyPanel(<WaitlistPanel />);
        if (selectedNiche === 'zerotier_private' && isAdmin) return lazyPanel(<ZeroTierPrivatePanel />);
        if (selectedNiche === 'alt_history_private' && isAdmin) {
            return lazyPanel(
                <AltHistoryPrivatePanel
                    onPickNiche={(n, opts) => {
                        setSelectedNiche(n as NicheId);
                        if (opts?.topic) {
                            const u = new URL(window.location.href);
                            u.searchParams.set('topic', opts.topic);
                            window.history.replaceState({}, '', u.toString());
                        }
                    }}
                />,
            );
        }
        if (selectedNiche === 'history_rewind_private' && isAdmin) {
            return lazyPanel(
                <HistoryRewindPrivatePanel
                    onLongformHandoff={() => {
                        setTab('longform');
                        setSelectedNiche(null);
                        setCreateOpen(false);
                    }}
                />,
            );
        }
        if (createOpen && tab === 'create' && selectedNiche) {
            const niche = nicheById(selectedNiche);
            return (
                <CreatePanel
                    key={selectedNiche}
                    nicheId={selectedNiche}
                    categoryKey={niche?.categoryKey || 'people_blogs'}
                    renderTier={renderTier}
                    nicheTitle={niche?.title}
                    onNicheChange={setSelectedNiche}
                    isOwner={ownerOverride}
                />
            );
        }
        return null;
    })();

    const inBuilder = tab === 'create' && createOpen;
    const agentRequested = tab === 'agent';
    const isAgentTab = agentRequested && (loading || canUseAgent);
    const showHome = tab === 'create' && !createOpen;

    return (
        <StudioShell
            onNavigate={onNavigate}
            fullWidth={inBuilder || isAgentTab}
            flush={isAgentTab}
            sidebar={
                inBuilder || isAgentTab ? undefined : (
                    <StudioSidebar
                        active={tab === 'create' && !createOpen ? 'home' : tab}
                        items={sidebarItems}
                        onCreate={() => selectTab('create')}
                        onOpenAgent={openAgent}
                        onSelect={selectTab}
                    />
                )
            }
        >
            {showHome && (
                <div className="mx-auto max-w-7xl space-y-8">
                    <StudioHomeHero greeting={greeting} name={displayName} ownerPreview={ownerOverride} />
                    <StudioToolsRow onTool={handleTool} isAdmin={isAdmin} />
                    <section className="rounded-2xl border border-white/[0.08] bg-white/[0.025] p-5 shadow-sm shadow-black/30">
                        <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
                            <div>
                                <h2 className="text-lg font-bold text-white">Render tier</h2>
                                <p className="mt-1 text-sm text-gray-500">Draft to iterate. Ship for cinematic export. Documentary for long-form episodes.</p>
                            </div>
                            <span className="rounded-full border border-cyan-400/20 bg-cyan-500/10 px-3 py-1 text-[10px] font-semibold uppercase tracking-wider text-cyan-200">
                                Cost-aware
                            </span>
                        </div>
                        <RenderTierPicker value={renderTier} onChange={setRenderTier} />
                    </section>
                    <ChannelHomePanel onOpenAgent={openAgent} isAdmin={isAdmin} />
                </div>
            )}

            {inBuilder && (
                <div className="mx-auto max-w-5xl space-y-4">
                    <button
                        type="button"
                        onClick={() => {
                            setCreateOpen(false);
                            setTab('home');
                            setSelectedNiche(null);
                        }}
                        className="inline-flex items-center gap-2 rounded-full border border-white/[0.08] bg-white/[0.02] px-4 py-2 text-sm font-semibold text-gray-300 transition hover:border-violet-500/40 hover:text-white"
                    >
                        <ArrowLeft className="h-4 w-4" />
                        Back to Home
                    </button>
                    {!selectedNiche ? (
                        <ChannelHomePanel onOpenAgent={openAgent} isAdmin={isAdmin} />
                    ) : (
                        panel
                    )}
                </div>
            )}

            {agentRequested && (
                <div className="flex h-[calc(100dvh-3.5rem)] flex-col overflow-hidden">
                    {loading ? (
                        <PanelFallback />
                    ) : canUseAgent ? (
                        panel ?? <PanelFallback />
                    ) : (
                        <div className="flex flex-1 flex-col items-center justify-center gap-4 p-8 text-center">
                            <p className="max-w-md text-sm text-gray-400">
                                Studio Agent requires Studio or Studio Pro. Upgrade in Billing to unlock the agent.
                            </p>
                            <button
                                type="button"
                                onClick={() => onNavigate('billing')}
                                className="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-500"
                            >
                                View plans
                            </button>
                            <button
                                type="button"
                                onClick={() => selectTab('home')}
                                className="text-xs text-gray-500 hover:text-gray-300"
                            >
                                Back to Home
                            </button>
                        </div>
                    )}
                </div>
            )}

            {tab !== 'create' && tab !== 'home' && !isAgentTab && (
                <div className="mx-auto max-w-7xl">{panel}</div>
            )}
        </StudioShell>
    );
}
