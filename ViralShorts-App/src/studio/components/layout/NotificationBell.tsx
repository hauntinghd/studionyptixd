import { useEffect, useState } from 'react';
import { Bell } from 'lucide-react';
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

    useEffect(() => subscribeNotifications(() => setItems(listNotifications())), []);

    const unread = items.filter((n) => !n.read).length;

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
                    <div className="absolute right-0 z-50 mt-2 w-[min(360px,calc(100vw-2rem))] overflow-hidden rounded-2xl border border-white/[0.1] bg-[#0c0c10] shadow-2xl shadow-black/60">
                        <div className="flex items-center justify-between border-b border-white/[0.06] px-4 py-3">
                            <p className="text-sm font-semibold text-white">Updates</p>
                            {unread > 0 && (
                                <button
                                    type="button"
                                    onClick={() => markAllRead()}
                                    className="text-xs font-medium text-violet-300 hover:text-violet-200"
                                >
                                    Mark all read
                                </button>
                            )}
                        </div>
                        <div className="max-h-[360px] overflow-y-auto">
                            {items.length === 0 && (
                                <p className="px-4 py-6 text-center text-sm text-gray-500">No notifications yet.</p>
                            )}
                            {items.map((n) => (
                                <button
                                    key={n.id}
                                    type="button"
                                    onClick={() => {
                                        markRead(n.id);
                                        if (n.href) window.location.assign(n.href);
                                    }}
                                    className={`block w-full border-b border-white/[0.04] px-4 py-3 text-left transition hover:bg-white/[0.03] ${
                                        n.read ? 'opacity-70' : 'bg-violet-500/[0.04]'
                                    }`}
                                >
                                    <p className="text-sm font-semibold text-white">{n.title}</p>
                                    <p className="mt-1 text-xs leading-relaxed text-gray-400">{n.body}</p>
                                </button>
                            ))}
                        </div>
                    </div>
                </>
            )}
        </div>
    );
}
