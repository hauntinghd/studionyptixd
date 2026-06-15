import { useContext, useEffect, useMemo, useState } from 'react';
import {
    Bell,
    BriefcaseBusiness,
    ChevronDown,
    Crown,
    Film,
    Home,
    LogOut,
    Megaphone,
    Network,
    Plus,
    Search,
    Send,
    Settings,
    Sparkles,
    Trophy,
    Upload,
    User,
    UserCircle,
    Users,
    WalletCards,
    X,
} from 'lucide-react';
import type { PageNav } from '../components/NavBar';
import type { DashboardTab } from '../lib/studioProduct';
import { AuthContext } from '../shared';
import {
    addStudioHubMessage,
    addStudioHubWin,
    defaultStudioHubState,
    loadStudioHubState,
    patchStudioHubState,
    type StudioHubMessage,
    type StudioHubPowerSignal,
    type StudioHubWin,
} from '../lib/studioHubState';

type StudioKey = 'content' | 'growth' | 'automation' | 'marketing';
export type HubSection = 'hub' | 'network' | 'wins' | 'leaderboard';

const studios: Array<{
    key: StudioKey;
    name: string;
    subtitle: string;
    icon: typeof Film;
    badge?: string;
}> = [
    { key: 'content', name: 'Content Studio', subtitle: 'Videos, shorts, packaging', icon: Film, badge: '3' },
    { key: 'growth', name: 'Growth Studio', subtitle: 'Offers, ads, outreach', icon: BriefcaseBusiness, badge: 'Next' },
    { key: 'automation', name: 'Automation Studio', subtitle: 'Agents, workflows, data', icon: Sparkles, badge: 'Soon' },
    { key: 'marketing', name: 'Marketing Studio', subtitle: 'Angles, funnels, tests', icon: Megaphone, badge: 'Soon' },
];

const announcements = [
    {
        title: 'Studio Hub is live',
        body: 'Content Studio is now the default room for general chat, wins, leaderboard, and production access.',
        tag: 'Update',
    },
    {
        title: 'Proof wins require screenshots',
        body: 'Wins now work like proof posts: write the result, attach the screenshot, then send it to the feed.',
        tag: 'Production',
    },
    {
        title: 'Network is separate',
        body: 'Network is no longer buried inside Content Studio. It is its own direct messaging and operator space.',
        tag: 'Build',
    },
];

const launchers: Array<{
    title: string;
    body: string;
    action: DashboardTab;
    icon: typeof Sparkles;
}> = [
    { title: 'Studio Agent', body: 'Chat-first video production, research, packaging, and documentaries.', action: 'agent', icon: Sparkles },
    { title: 'ThumbLab', body: 'Thumbnail and title packaging for long-form videos.', action: 'thumbnails', icon: Film },
    { title: 'ClipLab', body: 'Turn long-form videos into scored 9:16 clips.', action: 'cliplab', icon: Film },
    { title: 'Create Short', body: 'Start a fast short-form production lane.', action: 'create', icon: Plus },
];

export default function GrowthCampusPanel({
    onNavigate,
    onSelectStudioTab,
    section = 'hub',
}: {
    onNavigate: PageNav;
    onSelectStudioTab: (tab: DashboardTab) => void;
    section?: HubSection;
}) {
    const { session, signOut } = useContext(AuthContext);
    const [activeStudio, setActiveStudio] = useState<StudioKey>('content');
    const [studioMenuOpen, setStudioMenuOpen] = useState(false);
    const [settingsOpen, setSettingsOpen] = useState(false);
    const [whatsNewOpen, setWhatsNewOpen] = useState(false);
    const [productionOpen, setProductionOpen] = useState(false);
    const [profile, setProfile] = useState(defaultStudioHubState.profile);
    const [powerSignals, setPowerSignals] = useState<StudioHubPowerSignal[]>(defaultStudioHubState.power_signals);
    const [channels, setChannels] = useState(defaultStudioHubState.channels);
    const [messages, setMessages] = useState<StudioHubMessage[]>(defaultStudioHubState.network_messages);
    const [wins, setWins] = useState<StudioHubWin[]>(defaultStudioHubState.wins);
    const [newChannel, setNewChannel] = useState('');
    const [hubMessage, setHubMessage] = useState('');
    const [winMessage, setWinMessage] = useState('');
    const [winImage, setWinImage] = useState('');
    const [winImageName, setWinImageName] = useState('');
    const [winError, setWinError] = useState('');
    const accessToken = session?.access_token || '';

    const studio = useMemo(() => studios.find((s) => s.key === activeStudio) || studios[0], [activeStudio]);
    const StudioIcon = studio.icon;
    const profileName = String(profile.display_name || '').trim();
    const accountName =
        profileName ||
        String((session?.user?.user_metadata as any)?.display_name || (session?.user?.user_metadata as any)?.name || '').trim() ||
        session?.user?.email?.split('@')[0] ||
        'Operator';

    const navigate = (page: Parameters<PageNav>[0]) => {
        setSettingsOpen(false);
        onNavigate(page);
    };

    useEffect(() => {
        if (!accessToken) return;
        let cancelled = false;
        loadStudioHubState(accessToken)
            .then((state) => {
                if (cancelled) return;
                setChannels(state.channels);
                setPowerSignals(state.power_signals);
                setMessages(state.network_messages);
                setWins(state.wins);
                setProfile(state.profile);
            })
            .catch(() => {
                // Local defaults keep the Hub usable if the persistence API is temporarily unavailable.
            });
        return () => {
            cancelled = true;
        };
    }, [accessToken]);

    const addChannel = () => {
        const clean = newChannel.trim();
        if (!clean) return;
        const next = Array.from(new Set([...channels, clean]));
        setChannels(next);
        setNewChannel('');
        if (accessToken) {
            patchStudioHubState(accessToken, { channels: next }).catch(() => {});
        }
    };

    const sendNetworkMessage = async () => {
        const clean = hubMessage.trim();
        if (!clean) return;
        const optimistic = {
            id: `local-${Date.now()}`,
            name: accountName,
            body: clean,
            created_at: new Date().toISOString(),
        };
        setMessages((items) => [...items, optimistic]);
        setHubMessage('');
        if (!accessToken) return;
        addStudioHubMessage(accessToken, clean, optimistic.name)
            .then((state) => setMessages(state.network_messages))
            .catch(() => {});
    };

    const addWin = async () => {
        const clean = winMessage.trim();
        setWinError('');
        if (!clean) {
            setWinError('Write the win before posting.');
            return;
        }
        if (!winImage) {
            setWinError('Attach a screenshot before posting a win.');
            return;
        }
        const title = clean.split('\n').find(Boolean)?.slice(0, 120) || 'Proof win';
        const optimistic = { id: `local-${Date.now()}`, title, body: clean, image_url: winImage, image_name: winImageName, created_at: new Date().toISOString() };
        setWins((items) => [optimistic, ...items]);
        setWinMessage('');
        setWinImage('');
        setWinImageName('');
        if (!accessToken) return;
        addStudioHubWin(accessToken, optimistic.title, optimistic.body, optimistic.image_url, optimistic.image_name || '')
            .then((state) => setWins(state.wins))
            .catch(() => {});
    };

    const selectWinImage = (file?: File | null) => {
        if (!file) return;
        if (!file.type.startsWith('image/')) {
            setWinError('Use a PNG, JPG, or other image file.');
            return;
        }
        const reader = new FileReader();
        reader.onload = () => {
            setWinImage(String(reader.result || ''));
            setWinImageName(file.name);
            setWinError('');
        };
        reader.readAsDataURL(file);
    };

    return (
        <div className="flex h-full min-h-0 bg-[#0b1722] text-gray-100">
            <aside className="flex w-14 shrink-0 flex-col items-center border-r border-white/[0.08] bg-[#07111b] py-3">
                <div className="mb-5 flex h-9 w-9 items-center justify-center rounded-xl border border-cyan-400/20 bg-cyan-500/10">
                    <Sparkles className="h-4 w-4 text-cyan-200" />
                </div>
                <nav className="flex flex-1 flex-col items-center gap-2">
                    <RailButton label="Hub" icon={Home} active={section === 'hub'} onClick={() => onSelectStudioTab('campus')} />
                    <RailButton label="Create" icon={Sparkles} active={false} onClick={() => onSelectStudioTab('create')} />
                    <RailButton label="Network" icon={Network} active={section === 'network'} onClick={() => onSelectStudioTab('network')} />
                    <RailButton label="Wins" icon={Trophy} active={section === 'wins'} onClick={() => onSelectStudioTab('wins')} />
                    <RailButton label="Leaderboard" icon={Crown} active={section === 'leaderboard'} onClick={() => onSelectStudioTab('leaderboard')} />
                    <RailButton label="Wallet" icon={WalletCards} active={false} onClick={() => {}} disabled />
                </nav>
                <div className="relative">
                    <button
                        type="button"
                        title="Settings"
                        onClick={() => setSettingsOpen((open) => !open)}
                        className={`flex h-10 w-10 items-center justify-center rounded-xl transition ${
                            settingsOpen ? 'bg-amber-400/15 text-amber-100 ring-1 ring-amber-300/30' : 'text-gray-400 hover:bg-white/[0.06] hover:text-white'
                        }`}
                    >
                        <Settings className="h-5 w-5" />
                    </button>
                    {settingsOpen && (
                        <div className="absolute bottom-0 left-12 z-30 w-72 rounded-xl border border-amber-300/40 bg-[#07111b] p-2 shadow-2xl shadow-black/50">
                            <p className="px-3 py-2 text-xs font-bold uppercase tracking-[0.16em] text-gray-400">Settings</p>
                            <HubMenuButton icon={UserCircle} label="My Account" onClick={() => navigate('account')} featured />
                            <HubMenuButton icon={Settings} label="Settings" onClick={() => navigate('settings')} />
                            <HubMenuButton icon={User} label="Profile" onClick={() => navigate('account')} />
                            <HubMenuButton icon={Crown} label="My Membership" onClick={() => navigate('subscription')} />
                            <HubMenuButton icon={WalletCards} label="Billing" onClick={() => navigate('billing')} />
                            <div className="my-2 border-t border-white/[0.08]" />
                            <HubMenuButton icon={Bell} label="What's New" onClick={() => { setSettingsOpen(false); setWhatsNewOpen(true); }} />
                            <button
                                type="button"
                                onClick={() => {
                                    setSettingsOpen(false);
                                    void signOut();
                                }}
                                className="mt-2 flex w-full items-center gap-3 rounded-lg border border-red-500/30 px-3 py-2.5 text-left text-sm font-semibold text-red-300 transition hover:bg-red-500/10"
                            >
                                <span className="flex h-8 w-8 items-center justify-center rounded-lg bg-red-500/10">
                                    <LogOut className="h-4 w-4" />
                                </span>
                                Logout
                            </button>
                            <p className="px-3 pb-1 pt-3 text-[10px] uppercase tracking-[0.14em] text-gray-600">build studio-web · v0.0.0</p>
                        </div>
                    )}
                </div>
            </aside>

            <aside className="hidden w-[270px] shrink-0 border-r border-white/[0.08] bg-[#102033] md:block">
                <div className="relative flex h-16 items-center justify-between border-b border-white/[0.08] px-4">
                    <button
                        type="button"
                        onClick={() => setStudioMenuOpen((open) => !open)}
                        className="flex min-w-0 items-center gap-3 rounded-xl p-1 text-left transition hover:bg-white/[0.04]"
                    >
                        <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-white/10 bg-white/[0.06]">
                            <StudioIcon className="h-4 w-4 text-cyan-200" />
                        </span>
                        <span className="min-w-0">
                            <span className="block truncate text-base font-bold text-white">{studio.name}</span>
                            <span className="block text-xs text-gray-400">Studio Hub</span>
                        </span>
                        <ChevronDown className={`h-4 w-4 text-gray-500 transition ${studioMenuOpen ? 'rotate-180' : ''}`} />
                    </button>
                    {studioMenuOpen && (
                        <div className="absolute left-3 right-3 top-14 z-20 rounded-xl border border-white/[0.08] bg-[#07111b] p-2 shadow-2xl shadow-black/50">
                            <p className="px-3 py-2 text-xs font-bold uppercase tracking-[0.16em] text-gray-500">Switch studio</p>
                            {studios.map((item) => {
                                const Icon = item.icon;
                                const active = item.key === activeStudio;
                                return (
                                    <button
                                        key={item.key}
                                        type="button"
                                        onClick={() => {
                                            setActiveStudio(item.key);
                                            setStudioMenuOpen(false);
                                        }}
                                        className={`flex w-full items-center gap-3 rounded-lg px-3 py-2.5 text-left transition ${
                                            active ? 'bg-cyan-500/20 text-white' : 'text-gray-300 hover:bg-white/[0.06]'
                                        }`}
                                    >
                                        <Icon className={active ? 'h-4 w-4 text-cyan-200' : 'h-4 w-4 text-gray-500'} />
                                        <span className="min-w-0 flex-1">
                                            <span className="block truncate text-sm font-semibold">{item.name}</span>
                                            <span className="block truncate text-xs text-gray-500">{item.subtitle}</span>
                                        </span>
                                        {item.badge && (
                                            <span className="rounded-full bg-black/25 px-2 py-0.5 text-[10px] font-bold text-gray-300">{item.badge}</span>
                                        )}
                                    </button>
                                );
                            })}
                        </div>
                    )}
                </div>

                <div className="border-b border-white/[0.08] p-3">
                    <div className="relative">
                        <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-500" />
                        <input
                            aria-label="Search Studio Hub"
                            placeholder="Search"
                            className="h-9 w-full rounded-lg border border-white/[0.08] bg-[#07111b] pl-9 pr-3 text-sm text-white outline-none placeholder:text-gray-500 focus:border-cyan-400/40"
                        />
                    </div>
                </div>

                <div className="max-h-[calc(100%-8rem)] overflow-y-auto p-3">
                    <div className="mb-5">
                        <p className="mb-2 px-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500">Channels</p>
                        <div className="space-y-1">
                            {channels.map((channel) => (
                                <div key={channel} className="rounded-lg bg-white/[0.03] px-3 py-2 text-sm font-medium text-gray-200">
                                    {channel}
                                </div>
                            ))}
                        </div>
                        <div className="mt-2 flex gap-2">
                            <input
                                value={newChannel}
                                onChange={(e) => setNewChannel(e.target.value)}
                                placeholder="New channel"
                                className="h-9 min-w-0 flex-1 rounded-lg border border-white/[0.08] bg-[#07111b] px-3 text-sm text-white outline-none placeholder:text-gray-500"
                            />
                            <button type="button" onClick={addChannel} className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-cyan-500/80 text-white">
                                <Plus className="h-4 w-4" />
                            </button>
                        </div>
                    </div>

                    <SidebarSection title="Information" items={['Announcements']} onPick={() => setWhatsNewOpen(true)} />
                    <SidebarSection
                        title="Content Studio"
                        items={['General Chat', 'Wins', 'Leaderboard']}
                        onPick={(item) => {
                            if (item === 'General Chat') onSelectStudioTab('campus');
                            if (item === 'Wins') onSelectStudioTab('wins');
                            if (item === 'Leaderboard') onSelectStudioTab('leaderboard');
                        }}
                        active={section === 'hub' ? 'General Chat' : section === 'wins' ? 'Wins' : section === 'leaderboard' ? 'Leaderboard' : undefined}
                    />
                    <SidebarSection
                        title="Production"
                        items={[
                            'Studio Agent',
                            { label: 'ClipLab', badge: 'Soon', disabled: true },
                            { label: 'Automate', badge: 'Soon', disabled: true },
                        ]}
                        onPick={(item) => {
                            if (item === 'Studio Agent') onSelectStudioTab('agent');
                        }}
                    />
                </div>
            </aside>

            <main className="flex min-w-0 flex-1 flex-col">
                <header className="flex h-16 shrink-0 items-center justify-between border-b border-white/[0.08] bg-[#102033] px-4">
                    <div className="flex items-center gap-2">
                        <span className="text-xl text-gray-500">#</span>
                        <h1 className="text-base font-bold text-white">{sectionTitle(section, studio.name)}</h1>
                    </div>
                    <div className="flex items-center gap-2">
                        <button
                            type="button"
                            onClick={() => setWhatsNewOpen(true)}
                            className="flex h-9 w-9 items-center justify-center rounded-lg text-gray-400 hover:bg-white/[0.06] hover:text-white"
                        >
                            <Bell className="h-4 w-4" />
                        </button>
                        <button
                            type="button"
                            onClick={() => setProductionOpen(true)}
                            className="hidden h-9 items-center gap-2 rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 text-sm font-semibold text-gray-200 hover:bg-white/[0.06] sm:flex"
                        >
                            <Plus className="h-4 w-4" />
                            New production
                        </button>
                    </div>
                </header>

                <div className="min-h-0 flex-1 overflow-y-auto px-4 py-4">
                    {section === 'hub' && <FeedView studio={studio.name} onOpenAgent={() => onSelectStudioTab('agent')} />}
                    {section === 'network' && <EmbeddedNetworkView powerSignals={powerSignals} messages={messages} message={hubMessage} onMessage={setHubMessage} onSend={() => void sendNetworkMessage()} />}
                    {section === 'wins' && <EmbeddedWinsView wins={wins} message={winMessage} image={winImage} imageName={winImageName} error={winError} onMessage={setWinMessage} onImage={selectWinImage} onClearImage={() => { setWinImage(''); setWinImageName(''); }} onSave={() => void addWin()} />}
                    {section === 'leaderboard' && <LeaderboardView accountName={accountName} />}
                </div>

                {section === 'hub' && <footer className="shrink-0 border-t border-white/[0.08] bg-[#102033] p-3">
                    <div className="flex items-center gap-2 rounded-xl bg-[#07111b] px-3 py-2">
                        <label className="flex h-9 w-9 shrink-0 cursor-pointer items-center justify-center rounded-lg bg-cyan-500/10 text-cyan-200 hover:bg-cyan-500/15" title="Upload image">
                            <Plus className="h-4 w-4" />
                            <input type="file" accept="image/*" className="hidden" />
                        </label>
                        <input
                            aria-label="Hub message"
                            placeholder={`Message # ${studio.name.toLowerCase().replace(/\s+/g, '-')}`}
                            className="h-9 min-w-0 flex-1 bg-transparent text-sm text-white outline-none placeholder:text-gray-500"
                        />
                        <button type="button" className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-cyan-500/90 text-white">
                            <Send className="h-4 w-4" />
                        </button>
                    </div>
                </footer>}
            </main>

            <aside className="hidden w-[330px] shrink-0 border-l border-white/[0.08] bg-[#102033] xl:flex xl:flex-col">
                <section className="border-b border-white/[0.08] p-4">
                    <div className="rounded-xl border border-white/[0.08] bg-[#07111b] p-4">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center gap-3">
                                <div className="flex h-11 w-11 items-center justify-center rounded-full border border-cyan-400/20 bg-cyan-500/10">
                                    <Crown className="h-5 w-5 text-cyan-200" />
                                </div>
                                <div>
                                    <h2 className="text-sm font-bold text-white">Power Level 1</h2>
                                    <p className="text-xs text-gray-500">{accountName}</p>
                                </div>
                            </div>
                            <span className="h-2.5 w-2.5 rounded-full bg-emerald-400" />
                        </div>
                        <div className="mt-4 h-2 overflow-hidden rounded-full bg-white/[0.08]">
                            <div className="h-full w-[8%] rounded-full bg-cyan-400" />
                        </div>
                        <p className="mt-2 text-xs text-gray-500">0 / 1,000 XP</p>
                    </div>
                </section>

                <section className="min-h-0 flex-1 overflow-y-auto p-4">
                    <div className="mb-4 flex items-center justify-between">
                        <h2 className="text-sm font-bold uppercase tracking-[0.16em] text-gray-400">Level Up</h2>
                        <Users className="h-4 w-4 text-gray-500" />
                    </div>
                    <div className="space-y-4">
                        {powerSignals.slice(0, 6).map((signal) => (
                            <div key={signal.name} className="flex items-center gap-3">
                                <div className="relative flex h-9 w-9 items-center justify-center rounded-full bg-[#07111b] text-xs font-bold text-cyan-100">
                                    +{signal.xp}
                                    <span className="absolute bottom-0 right-0 h-2.5 w-2.5 rounded-full border border-[#102033] bg-emerald-400" />
                                </div>
                                <div className="min-w-0">
                                    <p className="truncate text-sm font-semibold text-white">{signal.name}</p>
                                    <p className="truncate text-xs text-gray-500">{signal.detail}</p>
                                </div>
                            </div>
                        ))}
                    </div>
                </section>
            </aside>

            {whatsNewOpen && <WhatsNewPanel onClose={() => setWhatsNewOpen(false)} />}
            {productionOpen && (
                <NewProductionPanel
                    onClose={() => setProductionOpen(false)}
                    onLaunch={(tab) => {
                        setProductionOpen(false);
                        onSelectStudioTab(tab);
                    }}
                />
            )}
        </div>
    );
}

function FeedView({
    studio,
    onOpenAgent,
}: {
    studio: string;
    onOpenAgent: () => void;
}) {
    return (
        <>
            <div className="mb-3 rounded-lg border-l-2 border-amber-300 bg-[#162a3d] px-4 py-3">
                <p className="text-xs font-bold uppercase tracking-[0.16em] text-amber-200">General Chat</p>
                <div className="mt-1 flex flex-wrap items-center justify-between gap-3">
                    <p className="text-sm text-gray-200">{studio} is for channel context, production notes, and decisions. Product updates live in Announcements.</p>
                    <div className="flex items-center gap-2">
                        <button type="button" onClick={onOpenAgent} className="rounded-lg bg-cyan-500 px-3 py-1.5 text-xs font-bold text-white hover:bg-cyan-400">
                            Open Studio Agent
                        </button>
                    </div>
                </div>
            </div>
            <div className="rounded-lg bg-[#162a3d] px-4 py-3">
                <p className="text-sm text-gray-400">No general messages yet. Use this room for live production context, not product update logs.</p>
            </div>
        </>
    );
}

function WhatsNewPanel({ onClose }: { onClose: () => void }) {
    return (
        <div className="fixed inset-0 z-40 flex justify-end bg-black/30 backdrop-blur-sm" onClick={onClose}>
            <aside className="h-full w-full max-w-md border-l border-white/[0.08] bg-[#07111b] p-5 shadow-2xl shadow-black/60 transition" onClick={(e) => e.stopPropagation()}>
                <div className="flex items-center justify-between">
                    <h2 className="text-xl font-bold text-white">What's New</h2>
                    <button type="button" onClick={onClose} className="rounded-lg p-2 text-gray-400 hover:bg-white/[0.06] hover:text-white">
                        <X className="h-5 w-5" />
                    </button>
                </div>
                <div className="mt-5 space-y-3">
                    {announcements.map((item) => (
                        <article key={item.title} className="rounded-xl bg-[#102033] p-4">
                            <p className="text-xs font-bold uppercase tracking-[0.16em] text-cyan-200">{item.tag}</p>
                            <h3 className="mt-2 text-sm font-bold text-white">{item.title}</h3>
                            <p className="mt-1 text-sm leading-6 text-gray-400">{item.body}</p>
                        </article>
                    ))}
                </div>
            </aside>
        </div>
    );
}

function NewProductionPanel({ onClose, onLaunch }: { onClose: () => void; onLaunch: (tab: DashboardTab) => void }) {
    return (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/35 p-4 backdrop-blur-sm" onClick={onClose}>
            <section className="w-full max-w-2xl rounded-2xl border border-white/[0.08] bg-[#07111b] p-5 shadow-2xl shadow-black/60" onClick={(e) => e.stopPropagation()}>
                <div className="flex items-center justify-between">
                    <div>
                        <h2 className="text-xl font-bold text-white">New production</h2>
                        <p className="mt-1 text-sm text-gray-400">Choose the fastest path. Studio Agent should orchestrate the serious work.</p>
                    </div>
                    <button type="button" onClick={onClose} className="rounded-lg p-2 text-gray-400 hover:bg-white/[0.06] hover:text-white">
                        <X className="h-5 w-5" />
                    </button>
                </div>
                <div className="mt-5 grid gap-3 sm:grid-cols-2">
                    {launchers.map((item) => {
                        const Icon = item.icon;
                        return (
                            <button key={item.title} type="button" onClick={() => onLaunch(item.action)} className="rounded-xl bg-[#102033] p-4 text-left transition hover:bg-[#162a3d]">
                                <Icon className="h-5 w-5 text-cyan-200" />
                                <h3 className="mt-3 text-sm font-bold text-white">{item.title}</h3>
                                <p className="mt-1 text-sm text-gray-400">{item.body}</p>
                            </button>
                        );
                    })}
                </div>
            </section>
        </div>
    );
}

type SidebarItem = string | { label: string; badge?: string; disabled?: boolean };

function SidebarSection({ title, items, onPick, active }: { title: string; items: SidebarItem[]; onPick: (item: string) => void; active?: string }) {
    return (
        <div className="mb-5">
            <p className="mb-1 px-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500">{title}</p>
            <div className="space-y-1">
                {items.map((entry) => {
                    const item = typeof entry === 'string' ? { label: entry } : entry;
                    return (
                    <button key={item.label} type="button" disabled={item.disabled} onClick={() => !item.disabled && onPick(item.label)} className={`flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-sm hover:text-white ${active === item.label ? 'bg-cyan-500/15 text-white' : 'text-gray-300 hover:bg-white/[0.06]'} ${item.disabled ? 'cursor-not-allowed opacity-55 hover:bg-transparent hover:text-gray-300' : ''}`}>
                        <span className={`h-1.5 w-1.5 rounded-full ${active === item.label ? 'bg-cyan-200' : 'bg-cyan-300/70'}`} />
                        <span className="min-w-0 flex-1 truncate">{item.label}</span>
                        {item.badge && <span className="rounded-full bg-[#07111b] px-2 py-0.5 text-[10px] font-bold text-gray-200">{item.badge}</span>}
                    </button>
                    );
                })}
            </div>
        </div>
    );
}

function sectionTitle(section: HubSection, studioName: string): string {
    if (section === 'network') return 'network';
    if (section === 'wins') return 'wins';
    if (section === 'leaderboard') return 'leaderboard';
    return studioName.toLowerCase().replace(/\s+/g, '-');
}

function EmbeddedNetworkView({
    powerSignals,
    messages,
    message,
    onMessage,
    onSend,
}: {
    powerSignals: StudioHubPowerSignal[];
    messages: StudioHubMessage[];
    message: string;
    onMessage: (value: string) => void;
    onSend: () => void;
}) {
    return (
        <div className="grid gap-4 xl:grid-cols-[1fr_320px]">
            <section className="rounded-xl bg-[#162a3d]">
                <div className="border-b border-white/[0.08] px-4 py-3">
                    <p className="text-sm font-bold text-white">Network</p>
                    <p className="mt-1 text-xs text-gray-400">Central discussion for creators, operators, collaborators, and clients.</p>
                </div>
                <div className="space-y-3 p-4">
                    {messages.map((item) => (
                        <article key={item.id} className="flex items-start gap-3 rounded-lg bg-[#102033] p-3">
                            <div className="flex h-9 w-9 items-center justify-center rounded-full bg-cyan-500/15 text-xs font-bold text-cyan-100">{(item.name || 'O').slice(0, 1)}</div>
                            <div>
                                <p className="text-sm font-bold text-white">{item.name || 'Operator'}</p>
                                <p className="mt-1 text-sm leading-6 text-gray-300">{item.body}</p>
                            </div>
                        </article>
                    ))}
                </div>
                <div className="border-t border-white/[0.08] p-3">
                    <div className="flex items-center gap-2 rounded-xl bg-[#07111b] px-3 py-2">
                        <input
                            value={message}
                            onChange={(e) => onMessage(e.target.value)}
                            onKeyDown={(e) => {
                                if (e.key === 'Enter') onSend();
                            }}
                            placeholder="Message Network"
                            className="h-9 min-w-0 flex-1 bg-transparent text-sm text-white outline-none placeholder:text-gray-500"
                        />
                        <button type="button" onClick={onSend} className="rounded-lg bg-cyan-500 px-3 py-2 text-xs font-bold text-white">Send</button>
                    </div>
                </div>
            </section>
            <aside className="rounded-xl bg-[#162a3d] p-4">
                <p className="text-sm font-bold text-white">Power levels</p>
                <p className="mt-1 text-xs leading-5 text-gray-400">No manual titles. Everyone levels up through useful activity.</p>
                <div className="mt-3 space-y-2">
                    {powerSignals.map((signal) => (
                        <div key={signal.name} className="rounded-lg bg-[#102033] p-3">
                            <p className="text-sm font-bold text-white">{signal.name}</p>
                            <p className="mt-1 text-xs text-gray-400">{signal.detail}</p>
                            <p className="mt-2 text-[10px] font-bold uppercase tracking-[0.14em] text-cyan-200">+{signal.xp} XP</p>
                        </div>
                    ))}
                </div>
            </aside>
        </div>
    );
}

function EmbeddedWinsView({
    wins,
    message,
    image,
    imageName,
    error,
    onMessage,
    onImage,
    onClearImage,
    onSave,
}: {
    wins: StudioHubWin[];
    message: string;
    image: string;
    imageName: string;
    error: string;
    onMessage: (value: string) => void;
    onImage: (file?: File | null) => void;
    onClearImage: () => void;
    onSave: () => void;
}) {
    return (
        <section className="rounded-xl bg-[#162a3d] p-4">
            <div className="grid gap-3 md:grid-cols-3">
                <MiniMetric label="Proof posts" value={String(wins.length)} />
                <MiniMetric label="With screenshots" value={String(wins.filter((w) => w.image_url).length)} />
                <MiniMetric label="Proof entries" value={String(wins.length)} />
            </div>
            <div className="mt-4 rounded-xl border border-white/[0.08] bg-[#07111b] p-3">
                <textarea
                    value={message}
                    onChange={(e) => onMessage(e.target.value)}
                    placeholder="Post the win. Add the result, context, and what changed."
                    className="min-h-24 w-full resize-y rounded-lg border border-white/[0.08] bg-black/20 px-3 py-3 text-sm text-white outline-none placeholder:text-gray-600 focus:border-cyan-400/40"
                />
                <div className="mt-3 flex flex-wrap items-center gap-3">
                    <label className="inline-flex cursor-pointer items-center gap-2 rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 py-2 text-xs font-bold text-cyan-100 hover:bg-white/[0.06]">
                        <Upload className="h-4 w-4" />
                        Attach screenshot
                        <input type="file" accept="image/png,image/jpeg,image/webp,image/gif" className="hidden" onChange={(e) => onImage(e.target.files?.[0])} />
                    </label>
                    {imageName && <span className="text-xs text-gray-400">{imageName}</span>}
                    {image && (
                        <button type="button" onClick={onClearImage} className="rounded-lg px-2 py-1 text-xs font-bold text-red-300 hover:bg-red-500/10">
                            Remove
                        </button>
                    )}
                    <button type="button" onClick={onSave} className="ml-auto rounded-lg bg-cyan-600 px-4 py-2 text-xs font-bold text-white hover:bg-cyan-500">
                        Send win
                    </button>
                </div>
                {image && <img src={image} alt="Win proof preview" className="mt-3 max-h-72 rounded-xl border border-white/[0.08] object-contain" />}
                {error && <p className="mt-3 text-sm text-amber-200">{error}</p>}
            </div>
            <div className="mt-4 space-y-2">
                {wins.length === 0 && <p className="text-sm text-gray-400">Wins require proof: a short message plus a screenshot.</p>}
                {wins.map((win) => (
                    <article key={win.id} className="rounded-lg bg-[#102033] p-3">
                        <p className="text-sm font-bold text-white">{win.title}</p>
                        {win.body && <p className="mt-1 whitespace-pre-wrap text-sm leading-6 text-gray-300">{win.body}</p>}
                        {win.image_url && <img src={win.image_url} alt={win.image_name || 'Win proof'} className="mt-3 max-h-96 rounded-xl border border-white/[0.08] object-contain" />}
                    </article>
                ))}
            </div>
        </section>
    );
}

function LeaderboardView({ accountName }: { accountName: string }) {
    const entries: Array<{ rank: number; name: string; detail: string; revenue: string }> = [];
    return (
        <section className="rounded-xl bg-[#162a3d] p-4">
            <div className="flex flex-wrap items-end justify-between gap-3">
                <div>
                    <p className="text-xs font-bold uppercase tracking-[0.18em] text-cyan-300">Leaderboard</p>
                    <h2 className="mt-1 text-lg font-bold text-white">Monthly money board</h2>
                    <p className="mt-1 text-sm text-gray-400">Top 10 is reviewed monthly from verified content-creation earnings, not placeholder roles.</p>
                </div>
                <span className="rounded-full bg-cyan-500/10 px-3 py-1 text-xs font-bold text-cyan-100">Top 10</span>
            </div>
            <div className="mt-4 space-y-2">
                {entries.length === 0 && (
                    <div className="rounded-lg border border-dashed border-white/[0.12] bg-[#102033] px-4 py-6">
                        <p className="text-sm font-bold text-white">No monthly winners selected yet.</p>
                        <p className="mt-2 max-w-2xl text-sm leading-6 text-gray-400">
                            {accountName}, this board will fill after verified monthly proof is reviewed: content revenue, client wins, monetization gains, and screenshots.
                        </p>
                    </div>
                )}
                {entries.map((entry) => (
                    <article key={`${entry.rank}-${entry.name}`} className="grid gap-3 rounded-lg bg-[#102033] px-4 py-3 md:grid-cols-[48px_1fr_120px] md:items-center">
                        <div className="text-xl font-black text-cyan-100">#{entry.rank}</div>
                        <div className="min-w-0">
                            <p className="truncate text-sm font-bold text-white">{entry.name}</p>
                            <p className="mt-1 line-clamp-2 text-xs leading-5 text-gray-400">{entry.detail}</p>
                        </div>
                        <div className="text-left md:text-right">
                            <p className="text-sm font-bold text-white">{entry.revenue}</p>
                            <p className="text-[10px] uppercase tracking-[0.16em] text-gray-500">Verified</p>
                        </div>
                    </article>
                ))}
            </div>
        </section>
    );
}

function MiniMetric({ label, value }: { label: string; value: string }) {
    return (
        <div className="rounded-lg bg-[#102033] p-3">
            <p className="text-[10px] font-bold uppercase tracking-[0.16em] text-gray-500">{label}</p>
            <p className="mt-2 text-2xl font-bold text-white">{value}</p>
        </div>
    );
}

function RailButton({ label, icon: Icon, active, onClick, disabled }: { label: string; icon: typeof Home; active: boolean; onClick: () => void; disabled?: boolean }) {
    return (
        <button
            type="button"
            title={disabled ? `${label} - coming soon` : label}
            onClick={onClick}
            disabled={disabled}
            className={`relative flex h-10 w-10 items-center justify-center rounded-xl transition ${
                active ? 'bg-cyan-500/15 text-cyan-100 ring-1 ring-cyan-400/30' : 'text-gray-400 hover:bg-white/[0.06] hover:text-white'
            } ${disabled ? 'cursor-not-allowed opacity-40 hover:bg-transparent hover:text-gray-400' : ''}`}
        >
            <Icon className="h-5 w-5" />
            {disabled && <span className="absolute right-1 top-1 h-1.5 w-1.5 rounded-full bg-amber-300" />}
        </button>
    );
}

function HubMenuButton({
    icon: Icon,
    label,
    onClick,
    featured,
}: {
    icon: typeof Settings;
    label: string;
    onClick: () => void;
    featured?: boolean;
}) {
    return (
        <button
            type="button"
            onClick={onClick}
            className={`flex w-full items-center justify-between rounded-lg px-3 py-2.5 text-left text-sm font-semibold transition ${
                featured ? 'bg-amber-300 text-black hover:bg-amber-200' : 'text-gray-200 hover:bg-white/[0.06] hover:text-white'
            }`}
        >
            <span className="flex items-center gap-3">
                <Icon className="h-4 w-4" />
                {label}
            </span>
            <ChevronDown className="-rotate-90 h-4 w-4 opacity-70" />
        </button>
    );
}
