/** In-app notification store (local-first; synced from /api/studio/release-notes). */

import { resolveStudioBackendUrl } from '../shared';

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
const SYNCED_RELEASES_KEY = 'nyptid_studio_synced_releases_v1';
const RELEASE_NOTES_SYNC_COOLDOWN_MS = 60_000;
let releaseNotesLastSyncAt = 0;
let releaseNotesSyncPromise: Promise<void> | null = null;
const RETIRED_IDS = new Set(['studio-campus-live', 'studio-hub-simplified']);

const BUNDLED_RELEASES: Array<Pick<StudioNotification, 'id' | 'kind' | 'title' | 'body'>> = [
    {
        id: 'release-2026-07-18-studio-1-0-public',
        kind: 'success',
        title: 'NYPTID Studio 1.0 is live',
        body:
            'Studio is publicly available as a desktop-first production app for owned short-form and long-form planning, production, review, repair, animation, and export.',
    },
    {
        id: 'release-2026-07-13-owner-credits-longform-compose',
        kind: 'success',
        title: 'Owner credits, long-form status, HR compose fix',
        body:
            'Owner accounts show infinite credits in the top bar. Long-form polls read Fly workspaces so completed renders stop stuck at 0%. History Rewind compose uses full narration duration.',
    },
    {
        id: 'release-2026-07-13-studio-boot-routing-fix',
        kind: 'success',
        title: 'Studio boot + agent routing fix',
        body:
            'Hard refresh no longer hammers RunPod with /api/config and /api/me. Boot reads hit Fly, agent HTTP uses api-studio with CORS on errors, and release-note sync is deduped.',
    },
    {
        id: 'release-2026-07-13-thumbnail-review-downloads',
        kind: 'success',
        title: 'Thumbnail review + in-app downloads',
        body:
            'Long-form thumbnail-only jobs reconcile into a pinned review strip with three candidates and Download buttons through Studio.',
    },
    {
        id: 'release-2026-07-13-longform-chapter-json-fix',
        kind: 'success',
        title: 'Long-form chapter JSON hardening',
        body:
            'Sleep-doc chapters no longer truncate narration into broken JSON. Narration and scene prompts generate in separate passes.',
    },
    {
        id: 'release-2026-07-13-admin-lane-gate',
        kind: 'info',
        title: 'Long-form + ClipLab admin gate',
        body: 'Long-form and ClipLab are owner/admin-only at launch while short-form stays live for paying users.',
    },
    {
        id: 'release-2026-07-13-ty-beta-promo',
        kind: 'billing',
        title: 'TY promo — one month free on Studio Pro',
        body: 'Stripe checkout for studio_pro_1k accepts promo code TY for 100% off the first month.',
    },
    {
        id: 'release-2026-07-12-sleep-doc-concept-plans',
        kind: 'success',
        title: 'Channel-aware sleep-doc concept plans',
        body: 'History Rewind long-form plans use channel-aware beats, hooks, and hour-scale durations.',
    },
    {
        id: 'release-2026-07-08-session-grounded-cost-quotes',
        kind: 'success',
        title: 'Render costs use your session models',
        body:
            'Cost quotes now use estimate_shortform_render_cost with your active Grok/Seedream session pickers — no more LTX/Seedream T2I guesses from memory.',
    },
    {
        id: 'release-2026-07-08-channel-winner-predictions',
        kind: 'success',
        title: 'Predicted moves follow your channel',
        body:
            'Connected-channel retention winners now rank first in predicted moves. Generic public psychology outliers stay in evidence unless they match your title patterns.',
    },
    {
        id: 'release-2026-07-08-public-search-niche-gate',
        kind: 'success',
        title: 'Public demand stays on-niche',
        body:
            'Public YouTube search now coerces channel-aware niche queries, filters unrelated viral outliers from evidence, and gates predicted moves on channel relevance.',
    },
    {
        id: 'release-2026-07-08-korpi-skeleton-parity',
        kind: 'success',
        title: 'KORPI-level skeleton still lock',
        body:
            'Create → Scenes requires your skeleton reference upload. Stills use Seedream 4.5 edit from that reference with editable prompts and per-beat regenerate.',
    },
    {
        id: 'release-2026-07-07-public-youtube-research-autorun',
        kind: 'success',
        title: 'Public YouTube research auto-run',
        body:
            'Niche/market search requests now auto-run get_public_search_trends and search_youtube_public. The agent no longer claims the YouTube search tool is missing.',
    },
    {
        id: 'release-2026-07-07-reference-failed-card-purge',
        kind: 'success',
        title: 'Reference failed card purge',
        body:
            'The red Reference analysis failed card no longer sticks after analysis succeeds. Ghost JSON errors are stripped even when mislabeled as competitor.',
    },
    {
        id: 'release-2026-07-07-ghost-deliverable-strip-fix',
        kind: 'success',
        title: 'Ghost production card purge',
        body:
            'The red Production failed card no longer sticks to chat after reject or status?. Ghost deliverables are stripped without deleting your analysis text.',
    },
    {
        id: 'release-2026-07-07-stats-status-intent-fix',
        kind: 'success',
        title: 'Stats/status intent fix',
        body:
            'stats? and status? no longer surface a stale Start short-form video approval or Production failed card. They poll reference analysis instead.',
    },
    {
        id: 'release-2026-07-07-reference-ghost-shortform-fix',
        kind: 'success',
        title: 'Reference ghost shortform fix',
        body:
            'Stale shortform poll tracks no longer show a parallel Production failed card during reference analysis. Ghost failures are stripped on resume and competitor completion.',
    },
    {
        id: 'release-2026-07-07-reference-poll-stale-job-fix',
        kind: 'success',
        title: 'Reference poll stale-job fix',
        body:
            'Ghost shortform JSON failures no longer mask running reference analysis. Poll routing prefers competitor workspaces and prunes stale shortform tracks.',
    },
    {
        id: 'release-2026-07-07-reference-poll-kind-fix',
        kind: 'success',
        title: 'Reference poll routing fix',
        body:
            'Uploaded reference jobs no longer misroute to shortform polling (the root cause of Expecting value JSON / Production failed). Backend auto-detects competitor workspaces.',
    },
    {
        id: 'release-2026-07-08-dictation-auth-fallback',
        kind: 'success',
        title: 'Live mic auth + record fallback',
        body:
            'Live voice now authenticates like chat (auth frame + token). Auth/plan failures auto-switch to record+server STT instead of a stuck “Authentication required” banner.',
    },
    {
        id: 'release-2026-07-07-dictation-stt-json-fix',
        kind: 'success',
        title: 'Dictation unlock + JSON hardening',
        body:
            'Mic dictation no longer blocks the prompt after xAI transcribes. Reference analysis uses safer JSON parsing and full Fly toolset stays enabled.',
    },
    {
        id: 'release-2026-07-07-studio-agent-research-v4',
        kind: 'success',
        title: 'Transcript retry + live xAI voice',
        body:
            'Failed transcript stages auto-retry on the saved upload. Mic dictation routes through xAI STT with live streaming. Updates sync on your next message.',
    },
    {
        id: 'release-2026-07-07-studio-agent-research-v3',
        kind: 'success',
        title: 'Studio Agent research upgrade',
        body:
            'Upload + public-data turns now run deep video analysis first, gate public search on a real topic, and surface exact stage errors instead of pacing-only fake completions.',
    },
    {
        id: 'release-2026-07-07-agent-intent-routing',
        kind: 'info',
        title: 'Studio Agent intent routing fixes',
        body:
            '“Watch this video” plus public YouTube research routes correctly; channel analytics is not required unless you ask for channel performance.',
    },
    {
        id: 'release-2026-07-06-catalyst-regenerate',
        kind: 'success',
        title: 'Catalyst Regenerate',
        body: 'Scene Regenerate audits artifacts, preserves style, fixes hands/diptychs, and teaches Catalyst — no RunPod GPU needed.',
    },
    {
        id: 'release-2026-07-06-hand-guard',
        kind: 'success',
        title: 'Extra-hand artifact guard',
        body: 'Split-screen layouts are blocked and skeleton stills enforce exactly two hands, with auto-retry on risky prompts.',
    },
    {
        id: 'release-2026-07-06-live-scene-refresh',
        kind: 'info',
        title: 'Scene stills update live',
        body: 'Production cards refresh as scenes regenerate — no Ctrl+Shift+R needed.',
    },
    {
        id: 'release-2026-07-06-bulk-scene-ship',
        kind: 'success',
        title: 'Approve all → animate all → finish',
        body: 'Natural language like “approve all scenes, animate them, finish the video” ships every scene automatically.',
    },
    {
        id: 'release-2026-07-06-upload-package-v2',
        kind: 'info',
        title: 'Upload packages rebuilt',
        body: 'Topic-specific hooks, tags, timestamps, and CC Off notes — no more generic wrong descriptions.',
    },
    {
        id: 'release-2026-07-06-agent-preview-v2',
        kind: 'success',
        title: 'Studio Agent previews restored',
        body: 'Scene stills and animated clips update live in chat again. Final MP4 fits at normal zoom.',
    },
    {
        id: 'release-2026-07-06-cc-off',
        kind: 'info',
        title: 'CC Off honored on export',
        body: 'Captions Off in Studio Agent means no burned word captions on your exported short.',
    },
    {
        id: 'release-2026-07-06-chat-scene-fix',
        kind: 'info',
        title: 'Fix scenes in chat',
        body: 'Describe what is wrong (e.g. add eyeballs) — no Edit button required.',
    },
    {
        id: 'release-2026-07-06-fal-voice',
        kind: 'success',
        title: 'Short-form voice uses fal.ai',
        body: 'Finalize and re-edit narration now runs through fal MiniMax TTS instead of ElevenLabs.',
    },
    {
        id: 'release-2026-07-06-fresh-production-reset',
        kind: 'info',
        title: 'New shorts reset stale scenes',
        body: 'Asking for a new short clears old scene-review jobs so artifacted renders do not come back.',
    },
    {
        id: 'release-2026-07-06-chat-context-fork',
        kind: 'info',
        title: 'New chat with prior context',
        body: 'Use With context or ask to ingest your previous chat to continue planning without old renders.',
    },
    {
        id: 'release-2026-07-06-expand-proof-short',
        kind: 'info',
        title: 'Finish from approved scene 1',
        body: 'Reply to your fixed scene and say make the rest — scene 1 stays locked while the remaining scenes generate.',
    },
    {
        id: 'release-2026-07-06-natural-expand-routing',
        kind: 'info',
        title: 'Natural scene continuation',
        body: 'Plain speech like “use this as scene 1, make the rest, 30 seconds” expands the same job — no duplicate approval cards.',
    },
    {
        id: 'release-2026-07-06-deliverable-layout',
        kind: 'info',
        title: 'Tighter video preview',
        body: 'Finished shorts no longer blow up the chat — preview stays readable at 100% zoom.',
    },
    {
        id: 'agent-progress-visible',
        kind: 'success',
        title: 'Studio Agent progress is visible',
        body: 'Production jobs now show stage, progress, errors, and retry guidance instead of failing silently.',
    },
    {
        id: 'unified-credits-live',
        kind: 'billing',
        title: 'Unified credits are live',
        body: 'Plans and top-ups now feed one wallet for OpenRouter, fal.ai, ElevenLabs, and production usage.',
    },
];

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
        ...BUNDLED_RELEASES.map((row, index) => ({
            ...row,
            createdAt: now - (index + 2) * 60_000,
            read: false,
        })),
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

export async function syncReleaseNotifications(accessToken?: string, force = false): Promise<void> {
    if (typeof window === 'undefined') return;
    const now = Date.now();
    if (!force && releaseNotesSyncPromise && now - releaseNotesLastSyncAt < RELEASE_NOTES_SYNC_COOLDOWN_MS) {
        return releaseNotesSyncPromise;
    }
    releaseNotesLastSyncAt = now;
    releaseNotesSyncPromise = (async () => {
    try {
        const headers: Record<string, string> = {};
        if (accessToken) headers.Authorization = `Bearer ${accessToken}`;
        const res = await fetch(resolveStudioBackendUrl('/api/studio/release-notes?limit=50'), { headers });
        if (!res.ok) return;
        const data = (await res.json().catch(() => ({}))) as {
            releases?: Array<{ id?: string; kind?: string; title?: string; body?: string; created_at?: number }>;
        };
        const synced = new Set<string>(
            JSON.parse(localStorage.getItem(SYNCED_RELEASES_KEY) || '[]') as string[],
        );
        const releases = Array.isArray(data.releases) ? data.releases : [];
        for (const row of releases) {
            const id = String(row.id || '').trim();
            if (!id || synced.has(id)) continue;
            const kindRaw = String(row.kind || 'info').toLowerCase();
            const kind: StudioNotificationKind =
                kindRaw === 'success' || kindRaw === 'warn' || kindRaw === 'billing' || kindRaw === 'render'
                    ? kindRaw
                    : 'info';
            pushNotification({
                id,
                kind,
                title: String(row.title || 'Studio update'),
                body: String(row.body || ''),
            });
            synced.add(id);
        }
        localStorage.setItem(SYNCED_RELEASES_KEY, JSON.stringify([...synced].slice(-120)));
    } catch {
        /* best-effort */
    }
    })();
    try {
        await releaseNotesSyncPromise;
    } finally {
        releaseNotesSyncPromise = null;
    }
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
