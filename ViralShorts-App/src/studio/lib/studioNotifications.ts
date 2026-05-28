/** In-app notification store (local-first; API hook later). */

export type StudioNotificationKind = 'info' | 'success' | 'warn' | 'billing' | 'render';

export interface StudioNotification {
    id: string;
    kind: StudioNotificationKind;
    title: string;
    body: string;
    createdAt: number;
    read: boolean;
    href?: string;
}

const STORAGE_KEY = 'nyptid_studio_notifications_v1';

function load(): StudioNotification[] {
    if (typeof window === 'undefined') return [];
    try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (!raw) return seedDefaults();
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : seedDefaults();
    } catch {
        return seedDefaults();
    }
}

function save(items: StudioNotification[]) {
    if (typeof window === 'undefined') return;
    try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(items.slice(0, 50)));
    } catch {
        // ignore quota
    }
}

function seedDefaults(): StudioNotification[] {
    const now = Date.now();
    return [
        {
            id: 'welcome',
            kind: 'info',
            title: 'Studio v2 is live',
            body: 'Pick a niche, choose Draft or Ship tier, connect YouTube in Settings for outcome insights.',
            createdAt: now,
            read: false,
        },
        {
            id: 'refund-policy',
            kind: 'billing',
            title: 'Failed renders refund automatically',
            body: 'If a render fails after charging credits, your wallet is refunded — no Discord ticket needed.',
            createdAt: now - 60_000,
            read: true,
        },
    ];
}

export function listNotifications(): StudioNotification[] {
    return load().sort((a, b) => b.createdAt - a.createdAt);
}

export function unreadCount(): number {
    return load().filter((n) => !n.read).length;
}

export function markRead(id: string) {
    const items = load().map((n) => (n.id === id ? { ...n, read: true } : n));
    save(items);
    dispatchChange();
}

export function markAllRead() {
    save(load().map((n) => ({ ...n, read: true })));
    dispatchChange();
}

export function pushNotification(input: Omit<StudioNotification, 'id' | 'createdAt' | 'read'> & { id?: string }) {
    const items = load();
    const note: StudioNotification = {
        id: input.id || `n_${Date.now()}`,
        kind: input.kind,
        title: input.title,
        body: input.body,
        href: input.href,
        createdAt: Date.now(),
        read: false,
    };
    save([note, ...items.filter((n) => n.id !== note.id)]);
    dispatchChange();
}

export function subscribeNotifications(cb: () => void): () => void {
    if (typeof window === 'undefined') return () => {};
    const handler = () => cb();
    window.addEventListener('nyptid:notifications', handler);
    return () => window.removeEventListener('nyptid:notifications', handler);
}

function dispatchChange() {
    if (typeof window !== 'undefined') {
        window.dispatchEvent(new CustomEvent('nyptid:notifications'));
    }
}
