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

const STORAGE_KEY = 'nyptid_studio_notifications_v2';
const RETIRED_IDS = new Set(['studio-campus-live', 'studio-hub-simplified', 'welcome', 'refund-policy']);

function load(): StudioNotification[] {
    if (typeof window === 'undefined') return [];
    try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (!raw) return seedDefaults();
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed)
            ? mergeDefaults(parsed.filter((item) => !RETIRED_IDS.has(String(item?.id || ''))))
            : seedDefaults();
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
            id: 'trial-plan-card-live',
            kind: 'billing',
            title: 'Free trial checkout is live',
            body: 'New users can start a 1,000-credit trial from Billing after adding a card in Stripe.',
            createdAt: now,
            read: false,
            href: '?page=billing',
        },
        {
            id: 'grok-model-picker-live',
            kind: 'render',
            title: 'Grok image and video models added',
            body: 'Studio Agent now lets you choose image and video models from the composer for visual testing.',
            createdAt: now - 60_000,
            read: false,
        },
        {
            id: 'trial-cost-guards-live',
            kind: 'warn',
            title: 'Trial spend guards are active',
            body: 'Trial renders now reserve credits before provider calls and enforce a provider-cost cap.',
            createdAt: now - 120_000,
            read: false,
        },
        {
            id: 'owner-blog-editor-live',
            kind: 'info',
            title: 'Owner update log is being added',
            body: 'Studio now has an owner-only blog/update editor for public product notes on the landing page.',
            createdAt: now - 180_000,
            read: false,
        },
        {
            id: 'studio-agent-visibility',
            kind: 'render',
            title: 'Studio Agent progress is visible',
            body: 'Production jobs now show stage, progress, errors, and retry guidance instead of failing silently.',
            createdAt: now - 240_000,
            read: false,
        },
        {
            id: 'billing-unified-credits',
            kind: 'billing',
            title: 'Unified credits are live',
            body: 'Plans and top-ups now feed one wallet for OpenRouter, fal.ai, ElevenLabs, and production usage.',
            createdAt: now - 300_000,
            read: false,
            href: '?page=billing',
        },
        {
            id: 'landing-proof-wall',
            kind: 'success',
            title: 'Landing page proof wall added',
            body: 'The public page now shows real creator outputs with lazy YouTube playback for faster first load.',
            createdAt: now - 360_000,
            read: true,
        },
    ];
}

function mergeDefaults(items: StudioNotification[]): StudioNotification[] {
    const existingIds = new Set(items.map((item) => item.id));
    const missing = seedDefaults().filter((item) => !existingIds.has(item.id));
    const merged = [...missing, ...items].slice(0, 50);
    if (missing.length) save(merged);
    return merged;
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
