import { Suspense, lazy, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { ArrowLeft, Loader2 } from 'lucide-react';
import type { PageNav } from '../components/NavBar';
import StudioShell from '../components/layout/StudioShell';
import StudioSidebar, { buildSidebarItems } from '../components/layout/StudioSidebar';
import NicheGalleryV2, { StudioHomeHero, StudioToolsRow } from '../components/home/NicheGalleryV2';
import RenderTierPicker from '../components/home/RenderTierPicker';
import {
    nicheById,
    type DashboardTab,
    type NicheId,
    type RenderTierId,
    type StudioNiche,
} from '../lib/studioProduct';
import { AuthContext } from '../shared';
import CreatePanel from '../panels/CreatePanel';

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

const OWNER_ALL_ACCESS = {
    create: true,
    longform: true,
    analytics: true,
    catalyst: true,
    refunds: true,
    waitlist: true,
};

export default function DashboardPage({ onNavigate }: { onNavigate: PageNav }) {
    const { session, loading, role, ownerOverride, studioLaneAccess } = useContext(AuthContext);
    const isAdmin = role === 'admin' || ownerOverride;
    const laneAccess = ownerOverride ? OWNER_ALL_ACCESS : studioLaneAccess;

    const [tab, setTab] = useState<DashboardTab>('home');
    const [createOpen, setCreateOpen] = useState(false);
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

    const isTabUnlocked = useCallback(
        (nextTab: DashboardTab) => {
            if (nextTab === 'home' || nextTab === 'create') return true;
            if (nextTab === 'longform') return isAdmin;
            if (['analytics', 'catalyst', 'refunds', 'waitlist'].includes(nextTab)) return isAdmin;
            return Boolean((laneAccess as Record<string, boolean>)[nextTab]);
        },
        [isAdmin, laneAccess],
    );

    useEffect(() => {
        if (loading) return;
        if (!session) onNavigate('auth');
    }, [session, loading, onNavigate]);

    useEffect(() => {
        if (selectedNiche) {
            setCreateOpen(true);
            setTab('create');
        }
    }, []);

    if (!session) return null;

    const sidebarItems = buildSidebarItems(isAdmin);
    const displayName = session.user.email?.split('@')[0] || 'creator';

    const openNiche = (niche: StudioNiche) => {
        if (niche.id === 'longform') {
            if (isAdmin) {
                setTab('longform');
                setCreateOpen(false);
            }
            return;
        }
        if (niche.id === 'style_clone') {
            setTab('create');
            setCreateOpen(true);
            setSelectedNiche('alt_battles');
            return;
        }
        setSelectedNiche(niche.id);
        setTab('create');
        setCreateOpen(true);
    };

    const handleTool = (action: string) => {
        if (action === 'longform' && isAdmin) {
            setTab('longform');
            setCreateOpen(false);
            return;
        }
        if (action === 'automate') return;
        setTab('create');
        setCreateOpen(true);
    };

    const selectTab = (id: DashboardTab) => {
        if (!isTabUnlocked(id)) return;
        if (id === 'home') {
            setTab('home');
            setCreateOpen(false);
            return;
        }
        if (id === 'create') {
            setTab('create');
            setCreateOpen(true);
            return;
        }
        setTab(id);
        setCreateOpen(false);
    };

    const lazyPanel = (node: React.ReactNode) => (
        <Suspense fallback={<PanelFallback />}>{node}</Suspense>
    );

    const panel = (() => {
        if (tab === 'longform' && isAdmin) return lazyPanel(<LongFormPanel />);
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
        if (createOpen && tab === 'create') {
            const niche = nicheById(selectedNiche || 'alt_battles');
            return (
                <CreatePanel
                    initialTemplate={selectedNiche || 'alt_battles'}
                    categoryKey={niche?.categoryKey || 'classical_clash'}
                    renderTier={renderTier}
                    nicheTitle={niche?.title}
                />
            );
        }
        return null;
    })();

    const showHome = tab === 'home' || (tab === 'create' && !createOpen);

    return (
        <StudioShell
            onNavigate={onNavigate}
            sidebar={
                <StudioSidebar
                    active={tab === 'create' && !createOpen ? 'home' : tab}
                    items={sidebarItems}
                    onCreate={() => {
                        setTab('create');
                        setCreateOpen(true);
                    }}
                    onSelect={selectTab}
                />
            }
        >
            {showHome && (
                <div className="mx-auto max-w-6xl space-y-8">
                    <StudioHomeHero greeting={greeting} name={displayName} ownerPreview={ownerOverride} />
                    <StudioToolsRow onTool={handleTool} />
                    <section>
                        <h2 className="mb-3 text-lg font-bold text-white">Render tier</h2>
                        <p className="mb-3 text-sm text-gray-500">Draft to iterate. Ship for cinematic export. Documentary for long-form episodes.</p>
                        <RenderTierPicker value={renderTier} onChange={setRenderTier} />
                    </section>
                    <NicheGalleryV2 isOwner={ownerOverride} onPick={openNiche} />
                </div>
            )}

            {tab === 'create' && createOpen && (
                <div className="mx-auto max-w-6xl space-y-4">
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
                    {panel}
                </div>
            )}

            {tab !== 'create' && tab !== 'home' && (
                <div className="mx-auto max-w-6xl">{panel}</div>
            )}
        </StudioShell>
    );
}
