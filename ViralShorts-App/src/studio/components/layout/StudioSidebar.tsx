import type { ComponentType } from 'react';
import {
    BarChart3,
    BrainCircuit,
    CalendarClock,
    Clapperboard,
    Film,
    Image as ImageIcon,
    Home,
    Plus,
    Receipt,
    Sparkles,
    Users,
    Wand2,
} from 'lucide-react';
import type { DashboardTab } from '../../lib/studioProduct';

export type SidebarItem = {
    id: DashboardTab;
    label: string;
    icon: ComponentType<{ className?: string }>;
    hidden?: boolean;
    comingSoon?: boolean;
    badge?: string;
};

export default function StudioSidebar({
    active,
    items,
    onCreate,
    onOpenAgent,
    onSelect,
}: {
    active: DashboardTab;
    items: SidebarItem[];
    onCreate: () => void;
    onOpenAgent?: () => void;
    onSelect: (id: DashboardTab) => void;
}) {
    return (
        <aside className="flex h-full w-[248px] shrink-0 flex-col border-r border-white/[0.06] bg-[#08080a] px-3 py-4 lg:w-[256px]">
            <div className="mb-4 px-1">
                <p className="text-lg font-bold tracking-tight text-white">NYPTID Studio</p>
                <p className="text-[11px] text-gray-500">Draft · Ship · Documentary</p>
            </div>

            <button
                type="button"
                onClick={onCreate}
                className="mb-2 flex w-full items-center gap-2.5 rounded-xl bg-gradient-to-r from-violet-600 to-violet-500 px-3 py-2.5 text-left text-sm font-semibold text-white shadow-lg shadow-violet-900/30 transition hover:from-violet-500 hover:to-violet-400"
            >
                <Plus className="h-4 w-4" />
                Create New
            </button>
            {onOpenAgent && (
            <button
                type="button"
                onClick={onOpenAgent}
                className="mb-4 flex w-full items-center gap-2.5 rounded-xl border border-violet-500/25 bg-violet-500/10 px-3 py-2 text-left text-xs font-medium text-violet-100 transition hover:border-violet-400/40 hover:bg-violet-500/15"
            >
                <Wand2 className="h-3.5 w-3.5 text-violet-300" />
                Studio Agent
                <span className="ml-auto rounded border border-violet-400/30 bg-violet-500/15 px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-violet-200">
                    Beta
                </span>
            </button>
            )}

            <nav className="flex-1 space-y-0.5 overflow-y-auto">
                {items.map((item) => {
                    const Icon = item.icon;
                    const isActive = active === item.id;
                    return (
                        <button
                            key={item.id}
                            type="button"
                            disabled={item.comingSoon}
                            onClick={() => onSelect(item.id)}
                            className={`flex w-full items-center justify-between gap-2 rounded-lg px-2.5 py-2 text-[13px] font-medium transition ${
                                isActive
                                    ? 'bg-white/[0.08] text-white'
                                    : 'text-gray-400 hover:bg-white/[0.04] hover:text-white'
                            } ${item.comingSoon ? 'cursor-not-allowed opacity-60' : ''}`}
                        >
                            <span className="flex items-center gap-2.5">
                                <Icon className={`h-4 w-4 ${isActive ? 'text-violet-300' : 'text-gray-500'}`} />
                                {item.label}
                            </span>
                            {item.badge && (
                                <span className="rounded border border-white/10 bg-black/40 px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-gray-400">
                                    {item.badge}
                                </span>
                            )}
                        </button>
                    );
                })}
            </nav>

            <div className="mt-4 rounded-xl border border-violet-500/20 bg-violet-500/5 p-3">
                <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-violet-300">Ship tier</p>
                <p className="mt-1 text-xs leading-relaxed text-gray-400">
                    Higgsfield cinematic lane unlocks on Pro+. Draft with fal, ship with peak realism.
                </p>
            </div>
        </aside>
    );
}

export function buildSidebarItems(
    isAdmin: boolean,
    laneAccess: Record<string, boolean> = {},
): SidebarItem[] {
    const canAgent = isAdmin || Boolean(laneAccess.agent);
    return ([
        { id: 'home', label: 'Home', icon: Home },
        { id: 'create', label: 'Create', icon: Sparkles },
        { id: 'agent', label: 'Studio Agent', icon: Wand2, hidden: !canAgent, badge: 'Beta' },
        { id: 'thumbnails', label: 'ThumbLab', icon: ImageIcon, hidden: !isAdmin, badge: 'Beta' },
        { id: 'cliplab', label: 'ClipLab', icon: Clapperboard, hidden: !isAdmin, badge: 'Beta' },
        { id: 'longform', label: 'Documentary', icon: Film, hidden: !isAdmin, badge: 'Beta' },
        { id: 'automate', label: 'Automate', icon: CalendarClock, comingSoon: true, badge: 'Soon' },
        { id: 'analytics', label: 'Analytics', icon: BarChart3, hidden: !isAdmin },
        { id: 'catalyst', label: 'Catalyst', icon: BrainCircuit, hidden: !isAdmin },
        { id: 'refunds', label: 'Refunds', icon: Receipt, hidden: !isAdmin },
        { id: 'waitlist', label: 'Waitlist', icon: Users, hidden: !isAdmin },
    ] as SidebarItem[]).filter((i) => !i.hidden);
}
