import { useEffect, useMemo, useState } from 'react';
import { Bell, CheckCheck, CreditCard, Info, Radio, Sparkles, Video } from 'lucide-react';
import {
    listNotifications,
    markAllRead,
    markRead,
    subscribeNotifications,
    type StudioNotification,
} from '../../lib/studioNotifications';

export default function NotificationBell() {
    const [open, setOpen] = useState(false);
    const [items, setItems] = useState<StudioNotification[]>(() => listNotifications());
    const [filter, setFilter] = useState<'all' | StudioNotification['kind']>('all');

    useEffect(() => subscribeNotifications(() => setItems(listNotifications())), []);

    const unread = items.filter((n) => !n.read).length;
    const filteredItems = useMemo(
        () => (filter === 'all' ? items : items.filter((item) => item.kind === filter)),
        [filter, items],
    );
    const filters: Array<{ id: 'all' | StudioNotification['kind']; label: string }> = [
        { id: 'all', label: 'All' },
        { id: 'render', label: 'Production' },
        { id: 'billing', label: 'Billing' },
        { id: 'success', label: 'Updates' },
        { id: 'warn', label: 'Warnings' },
        { id: 'info', label: 'Info' },
    ];

    return (
        <div className="relative">
            <button
                type="button"
                aria-label="Notifications"
                onClick={() => setOpen((v) => !v)}
                className="relative flex h-9 w-9 items-center justify-center rounded-lg border border-white/[0.08] bg-white/[0.03] text-gray-300 transition hover:border-violet-500/30 hover:text-white"
            >
                <Bell className="h-4 w-4" />
                {unread > 0 && (
                    <span className="absolute -right-1 -top-1 flex h-4 min-w-[16px] items-center justify-center rounded-full bg-rose-500 px-1 text-[10px] font-bold text-white">
                        {unread > 9 ? '9+' : unread}
                    </span>
                )}
            </button>
            {open && (
                <>
                    <button
                        type="button"
                        aria-label="Close notifications"
                        className="fixed inset-0 z-40"
                        onClick={() => setOpen(false)}
                    />
                    <div className="absolute right-0 z-50 mt-2 w-[min(430px,calc(100vw-2rem))] overflow-hidden rounded-lg border border-white/[0.1] bg-[#0c0c10] shadow-2xl shadow-black/60">
                        <div className="flex items-center justify-between border-b border-white/[0.06] px-4 py-3">
                            <div>
                                <p className="text-sm font-semibold text-white">Notifications</p>
                                <p className="mt-0.5 text-xs text-gray-500">{unread} unread</p>
                            </div>
                            {unread > 0 && (
                                <button
                                    type="button"
                                    onClick={() => markAllRead()}
                                    className="inline-flex items-center gap-1.5 rounded-md border border-white/[0.08] bg-white/[0.03] px-2.5 py-1.5 text-xs font-medium text-violet-200 hover:bg-white/[0.07]"
                                >
                                    <CheckCheck className="h-3.5 w-3.5" />
                                    Mark read
                                </button>
                            )}
                        </div>
                        <div className="flex gap-2 overflow-x-auto border-b border-white/[0.06] px-4 py-3">
                            {filters.map((item) => (
                                <button
                                    key={item.id}
                                    type="button"
                                    onClick={() => setFilter(item.id)}
                                    className={`shrink-0 rounded-md px-2.5 py-1.5 text-xs font-semibold transition ${
                                        filter === item.id
                                            ? 'bg-cyan-400/12 text-cyan-100 ring-1 ring-cyan-400/30'
                                            : 'bg-white/[0.03] text-gray-400 hover:text-white'
                                    }`}
                                >
                                    {item.label}
                                </button>
                            ))}
                        </div>
                        <div className="max-h-[360px] overflow-y-auto">
                            {filteredItems.length === 0 && (
                                <p className="px-4 py-6 text-center text-sm text-gray-500">No notifications yet.</p>
                            )}
                            {filteredItems.map((n) => (
                                <button
                                    key={n.id}
                                    type="button"
                                    onClick={() => {
                                        markRead(n.id);
                                        if (n.href) window.location.assign(n.href);
                                    }}
                                    className={`flex w-full gap-3 border-b border-white/[0.04] px-4 py-3 text-left transition hover:bg-white/[0.03] ${
                                        n.read ? 'opacity-70' : 'bg-violet-500/[0.04]'
                                    }`}
                                >
                                    <NotificationIcon kind={n.kind} />
                                    <span className="min-w-0 flex-1">
                                        <span className="flex items-center justify-between gap-3">
                                            <span className="text-sm font-semibold text-white">{n.title}</span>
                                            <span className="shrink-0 text-[10px] uppercase tracking-[0.12em] text-gray-600">{formatAge(n.createdAt)}</span>
                                        </span>
                                        <span className="mt-1 block text-xs leading-relaxed text-gray-400">{n.body}</span>
                                    </span>
                                </button>
                            ))}
                        </div>
                    </div>
                </>
            )}
        </div>
    );
}

function NotificationIcon({ kind }: { kind: StudioNotification['kind'] }) {
    const className = 'h-4 w-4';
    const iconClass = 'mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-md border';
    if (kind === 'billing') return <span className={`${iconClass} border-emerald-400/20 bg-emerald-400/10 text-emerald-200`}><CreditCard className={className} /></span>;
    if (kind === 'render') return <span className={`${iconClass} border-cyan-400/20 bg-cyan-400/10 text-cyan-200`}><Video className={className} /></span>;
    if (kind === 'success') return <span className={`${iconClass} border-violet-400/20 bg-violet-400/10 text-violet-200`}><Sparkles className={className} /></span>;
    if (kind === 'warn') return <span className={`${iconClass} border-amber-400/20 bg-amber-400/10 text-amber-200`}><Radio className={className} /></span>;
    return <span className={`${iconClass} border-white/[0.08] bg-white/[0.03] text-gray-300`}><Info className={className} /></span>;
}

function formatAge(createdAt: number) {
    const delta = Math.max(0, Date.now() - Number(createdAt || 0));
    const minutes = Math.floor(delta / 60_000);
    if (minutes < 1) return 'now';
    if (minutes < 60) return `${minutes}m`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h`;
    return `${Math.floor(hours / 24)}d`;
}
