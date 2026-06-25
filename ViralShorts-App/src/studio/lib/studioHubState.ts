import { resolveStudioBackendUrl } from '../shared';

export type StudioHubPowerSignal = { name: string; detail: string; xp: number };
export type StudioHubChecklistItem = { id: string; label: string; done: boolean };
export type StudioHubMessage = { id: string; name: string; body: string; created_at?: string };
export type StudioHubWin = {
    id: string;
    title: string;
    body?: string;
    image_url?: string;
    image_name?: string;
    created_at?: string;
};

export type StudioHubState = {
    profile: {
        website?: string;
        [key: string]: string | undefined;
    };
    channels: string[];
    power_signals: StudioHubPowerSignal[];
    checklist: StudioHubChecklistItem[];
    network_messages: StudioHubMessage[];
    wins: StudioHubWin[];
    updated_at?: string;
};

export const defaultStudioHubState: StudioHubState = {
    profile: { website: '' },
    channels: ['Empire Magnates', 'ZeroTier', 'NYPTID Clips'],
    power_signals: [
        { name: 'Post wins', detail: 'Share proof with a screenshot and result context.', xp: 125 },
        { name: 'Talk in Network', detail: 'Ask useful questions, help others, and document lessons.', xp: 25 },
        { name: 'Ship productions', detail: 'Complete videos, shorts, ads, and client assets.', xp: 250 },
        { name: 'Improve results', detail: 'Increase retention, watch time, CTR, subscribers, or ROI.', xp: 300 },
    ],
    checklist: [
        { id: 'connect-channels', label: 'Connect channels', done: true },
        { id: 'choose-studio', label: 'Choose a studio', done: false },
        { id: 'open-agent', label: 'Open Studio Agent', done: false },
        { id: 'approve-packaging', label: 'Approve packaging', done: false },
        { id: 'ship-production', label: 'Ship production', done: false },
        { id: 'review-results', label: 'Review results', done: false },
    ],
    network_messages: [
        { id: 'welcome', name: 'Studio', body: 'Use Network for questions, feedback, collaboration, and useful operator discussion.' },
    ],
    wins: [],
};

export function normalizeStudioHubState(raw: any): StudioHubState {
    const base = structuredCloneSafe(defaultStudioHubState);
    if (!raw || typeof raw !== 'object') return base;
    return {
        ...base,
        profile: { ...base.profile, ...(typeof raw.profile === 'object' && raw.profile ? raw.profile : {}) },
        channels: Array.isArray(raw.channels) ? raw.channels.map(String).filter(Boolean) : base.channels,
        power_signals: Array.isArray(raw.power_signals)
            ? raw.power_signals.filter(isPowerSignal).map((s: any) => ({ name: String(s.name), detail: String(s.detail || ''), xp: Number(s.xp || 0) }))
            : base.power_signals,
        checklist: Array.isArray(raw.checklist) ? raw.checklist.filter(isChecklistItem).map((item: any) => ({ id: String(item.id), label: String(item.label), done: Boolean(item.done) })) : base.checklist,
        network_messages: Array.isArray(raw.network_messages) ? raw.network_messages.filter(isMessage) : base.network_messages,
        wins: Array.isArray(raw.wins) ? raw.wins.filter(isWin) : base.wins,
        updated_at: typeof raw.updated_at === 'string' ? raw.updated_at : undefined,
    };
}

export async function loadStudioHubState(accessToken: string): Promise<StudioHubState> {
    const cached = readCachedStudioHubState(accessToken);
    const res = await fetch(resolveStudioBackendUrl('/api/studio-hub/state'), {
        headers: { Authorization: `Bearer ${accessToken}` },
    });
    if (!res.ok) {
        if (cached) return cached;
        throw new Error(`Hub state load failed (${res.status})`);
    }
    const data = await res.json();
    const state = normalizeStudioHubState(data?.state);
    writeCachedStudioHubState(accessToken, state);
    return state;
}

export async function patchStudioHubState(accessToken: string, patch: Partial<StudioHubState>): Promise<StudioHubState> {
    const optimistic = normalizeStudioHubState(mergeStudioHubPatch(readCachedStudioHubState(accessToken) || defaultStudioHubState, patch));
    writeCachedStudioHubState(accessToken, optimistic);
    const res = await fetch(resolveStudioBackendUrl('/api/studio-hub/state'), {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${accessToken}` },
        body: JSON.stringify(patch),
    });
    if (!res.ok) return optimistic;
    const data = await res.json();
    const state = normalizeStudioHubState(data?.state);
    writeCachedStudioHubState(accessToken, state);
    return state;
}

export async function addStudioHubMessage(accessToken: string, body: string, name = 'Operator'): Promise<StudioHubState> {
    const res = await fetch(resolveStudioBackendUrl('/api/studio-hub/network/messages'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${accessToken}` },
        body: JSON.stringify({ body, name }),
    });
    if (!res.ok) throw new Error(`Network message save failed (${res.status})`);
    const data = await res.json();
    const state = normalizeStudioHubState(data?.state);
    writeCachedStudioHubState(accessToken, state);
    return state;
}

export async function addStudioHubWin(accessToken: string, title: string, body = '', imageUrl = '', imageName = ''): Promise<StudioHubState> {
    const res = await fetch(resolveStudioBackendUrl('/api/studio-hub/wins'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${accessToken}` },
        body: JSON.stringify({ title, body, image_url: imageUrl, image_name: imageName }),
    });
    if (!res.ok) throw new Error(`Win save failed (${res.status})`);
    const data = await res.json();
    const state = normalizeStudioHubState(data?.state);
    writeCachedStudioHubState(accessToken, state);
    return state;
}

function mergeStudioHubPatch(current: StudioHubState, patch: Partial<StudioHubState>): StudioHubState {
    return {
        ...current,
        ...patch,
        profile: { ...(current.profile || {}), ...(patch.profile || {}) },
    };
}

function cacheKey(accessToken: string): string {
    const token = String(accessToken || 'anonymous');
    let hash = 0;
    for (let i = 0; i < token.length; i += 1) {
        hash = ((hash << 5) - hash + token.charCodeAt(i)) | 0;
    }
    return `studio_hub_state_v1:${Math.abs(hash)}`;
}

function readCachedStudioHubState(accessToken: string): StudioHubState | null {
    if (typeof window === 'undefined') return null;
    try {
        const raw = window.localStorage.getItem(cacheKey(accessToken));
        if (!raw) return null;
        return normalizeStudioHubState(JSON.parse(raw));
    } catch {
        return null;
    }
}

function writeCachedStudioHubState(accessToken: string, state: StudioHubState): void {
    if (typeof window === 'undefined') return;
    try {
        window.localStorage.setItem(cacheKey(accessToken), JSON.stringify(state));
    } catch {
        // Browser storage can be disabled; the API remains the source of truth.
    }
}

function structuredCloneSafe<T>(value: T): T {
    if (typeof structuredClone === 'function') return structuredClone(value);
    return JSON.parse(JSON.stringify(value)) as T;
}

function isPowerSignal(value: any): boolean {
    return value && typeof value === 'object' && typeof value.name === 'string';
}

function isChecklistItem(value: any): boolean {
    return value && typeof value === 'object' && typeof value.id === 'string' && typeof value.label === 'string';
}

function isMessage(value: any): value is StudioHubMessage {
    return value && typeof value === 'object' && typeof value.id === 'string' && typeof value.body === 'string';
}

function isWin(value: any): value is StudioHubWin {
    return value && typeof value === 'object' && typeof value.id === 'string' && typeof value.title === 'string';
}
