import { resolveStudioBackendUrl } from './backend';

const agentApi = (path: string) => resolveStudioBackendUrl(path);

export type AgentJobKind = 'longform' | 'shortform' | 'competitor' | 'cliplab';

export type AgentJobTrack = {
    job_id: string;
    kind: AgentJobKind;
    title?: string;
    started_at?: number;
};

export function normalizeAgentJobKind(jobId: string, kind?: string, title?: string): AgentJobKind {
    const explicit = String(kind || '').trim().toLowerCase();
    if (explicit === 'longform' || explicit === 'shortform' || explicit === 'competitor' || explicit === 'cliplab') {
        return explicit as AgentJobKind;
    }
    const id = String(jobId || '');
    if (/^(clipi_|clipa_|clipr_|remix_)/i.test(id)) return 'cliplab';
    const label = String(title || '').toLowerCase();
    // Do not infer competitor from generic "analysis" titles — that mislabels stale shortform polls.
    if (/reference video|competitor video|reference analysis/.test(label)) return 'competitor';
    if (/\breference\b/.test(label) && /\b(video|upload)\b/.test(label)) return 'competitor';
    return 'shortform';
}

export type AgentSceneSnapshot = {
    index: number;
    sid?: string | null;
    narration?: string | null;
    scene_action?: string | null;
    prompt?: string | null;
    prompt_user_override?: boolean;
    duration_sec?: number;
    status?: string | null;
    animate?: boolean;
    approved_for_video?: boolean;
    approved_for_animation?: boolean;
    has_clip?: boolean;
    video_model?: string | null;
    still_qa?: {
        status?: string;
        pass?: boolean;
        confidence?: number;
        summary?: string;
        issues?: string[];
    } | null;
    last_edit?: unknown;
    still_preview_url?: string;
    clip_preview_url?: string | null;
};

export type AgentJobSnapshot = {
    job_id: string;
    kind: AgentJobKind;
    status: 'running' | 'complete' | 'failed' | 'awaiting_approval' | string;
    progress: number;
    stage?: string;
    stage_label?: string;
    stage_detail?: string;
    error?: string | null;
    running?: boolean;
    title?: string;
    job_type?: 'cliplab_ingest' | 'cliplab_analyze' | 'cliplab_render' | 'cliplab_remix' | string;
    video_id?: string;
    cue_count?: number;
    mp4_url?: string;
    download_url?: string;
    package_url?: string;
    preview_url?: string;
    thumbnail_only?: boolean;
    visual_proof_only?: boolean;
    thumbnail_urls?: string[];
    model_url?: string;
    model_urls?: string[];
    asset_urls?: string[];
    current_scene?: number;
    total_scenes?: number;
    current_chapter?: number;
    total_chapters?: number;
    analysis_ready?: boolean;
    can_finalize?: boolean;
    still_count?: number;
    still_preview_urls?: string[];
    scenes?: AgentSceneSnapshot[];
    approved_scene_count?: number;
    all_scenes_approved?: boolean;
    animation_pending_count?: number;
    animation_complete_count?: number;
    pacing?: {
        avg_shot_sec?: number | null;
        cut_count?: number;
        duration_sec?: number;
        hook_window_sec?: number;
    };
    visual_summary?: string;
    pacing_warnings?: string[];
    storytelling_summary?: string;
    hook_summary?: string;
    packaging_notes?: string;
    transcript_excerpt?: string;
    engagement?: Record<string, number>;
    frame_count?: number;
    blueprint_hint?: string;
    segments?: unknown[];
    segment_count?: number;
    clips?: unknown[];
    clip_count?: number;
    upload_packages?: unknown[];
    upload_package_count?: number;
    remix?: unknown;
    next_action?: string;
    client_updated_at?: number;
    cost?: {
        actual_usd?: number;
        actual_usd_decimal?: string;
        event_count?: number;
        by_provider?: Record<string, number>;
        by_provider_decimal?: Record<string, string>;
        provider_breakdown?: Array<{
            provider: string;
            label: string;
            usd: number;
            usd_decimal: string;
        }>;
        fal_usd?: number;
        fal_usd_decimal?: string;
        xai_usd?: number;
        xai_usd_decimal?: string;
        spend_label?: string;
        status?: string;
    };
};

export type ProductionProgressUpdate = {
    job_id: string;
    kind: AgentJobKind;
    stage_label: string;
    progress: number;
    title?: string;
};

export function agentJobExpandProofUrl(jobId: string) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/expand-proof`);
}

export function agentJobStillUrl(jobId: string, sceneIdx: number) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/still/${sceneIdx}`);
}

export function agentJobClipUrl(jobId: string, sceneIdx: number) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/clip/${sceneIdx}`);
}

export function agentJobSceneApprovalUrl(jobId: string, sceneIdx: number) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/scene/${sceneIdx}/approval`);
}

export function agentJobSceneRegenerateUrl(jobId: string, sceneIdx: number) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/scene/${sceneIdx}/regenerate`);
}

export function agentJobScenePromptUrl(jobId: string, sceneIdx: number) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/scene/${sceneIdx}/prompt`);
}

export function agentJobScenesApprovalUrl(jobId: string) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/scenes/approval`);
}

export function agentJobAnimateUrl(jobId: string) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/animate`);
}

export function agentJobFinalizeUrl(
    jobId: string,
    kind: AgentJobKind = 'longform',
    opts?: { captions_enabled?: boolean; caption_mode?: 'word' | 'off' },
) {
    const qs = new URLSearchParams({ kind });
    if (typeof opts?.captions_enabled === 'boolean') {
        qs.set('captions_enabled', String(opts.captions_enabled));
    }
    if (opts?.caption_mode) {
        qs.set('caption_mode', opts.caption_mode);
    }
    return agentApi(`/api/studio-agent/jobs/${jobId}/finalize?${qs.toString()}`);
}

export async function finalizeLongformJob(
    jobId: string,
    accessToken: string,
): Promise<{ active_jobs?: unknown[] }> {
    const idempotencyKey = crypto.randomUUID();
    const res = await fetch(agentJobFinalizeUrl(jobId, 'longform'), {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${accessToken}`,
            'X-Idempotency-Key': idempotencyKey,
        },
    });
    const data = (await res.json().catch(() => ({}))) as { detail?: string; active_jobs?: unknown[] };
    if (!res.ok) {
        throw new Error(data.detail || `Finalize failed (${res.status})`);
    }
    return data;
}

export async function cancelJob(
    jobId: string,
    kind: string,
    accessToken: string,
    sessionId?: string | null,
): Promise<void> {
    const qs = new URLSearchParams({ kind });
    if (sessionId) qs.set('session_id', sessionId);
    const res = await fetch(agentApi(`/api/studio-agent/jobs/${jobId}/cancel?${qs.toString()}`), {
        method: 'POST',
        headers: { Authorization: `Bearer ${accessToken}` },
    });
    if (!res.ok) {
        const data = (await res.json().catch(() => ({}))) as { detail?: string };
        throw new Error(data.detail || `Cancel failed (${res.status})`);
    }
}

export function resolveMediaAssetUrl(path: string) {
    const value = String(path || '').trim();
    if (!value) return '';
    if (/^https?:\/\//i.test(value) || /^(?:blob:|data:(?:image|video|audio)\/)/i.test(value)) {
        return value;
    }
    // Never turn an untrusted URI scheme into a clickable/renderable media URL.
    if (/^[a-z][a-z0-9+.-]*:/i.test(value)) return '';
    return agentApi(value.startsWith('/') ? value : `/${value}`);
}

export function isProtectedStudioMedia(path: string) {
    const value = String(path || '').trim();
    if (!value) return false;
    if (!/^https?:\/\//i.test(value)) {
        return !/^(?:blob:|data:)/i.test(value);
    }
    try {
        const origin = new URL(value).origin;
        const trustedOrigins = new Set([
            agentApi('/api/studio-agent'),
            agentApi('/api/download'),
            agentApi('/api/auto/scene-image'),
        ].map((candidate) => new URL(candidate).origin));
        return trustedOrigins.has(origin);
    } catch {
        return false;
    }
}

export async function fetchMediaAsset(
    path: string,
    accessToken: string,
    init: RequestInit = {},
) {
    const url = resolveMediaAssetUrl(path);
    if (!url) throw new Error('Invalid media URL');
    const headers = new Headers(init.headers || undefined);
    if (isProtectedStudioMedia(path)) {
        if (!accessToken) throw new Error('Authentication required for this Studio asset');
        headers.set('Authorization', `Bearer ${accessToken}`);
    } else {
        // A Studio JWT must never be forwarded to a third-party asset host.
        headers.delete('Authorization');
        headers.delete('X-Access-Token');
        headers.delete('X-Auth-Token');
    }
    return fetch(url, { ...init, headers });
}

/** Download a Studio-served asset (thumbnails, stills, packages) without opening FAL URLs. */
export async function downloadStudioAsset(path: string, accessToken: string, filename: string) {
    const url = resolveMediaAssetUrl(path);
    if (!url) throw new Error('Invalid download URL');
    if (!isProtectedStudioMedia(path)) {
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = filename;
        anchor.rel = 'noopener noreferrer';
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        return;
    }
    const res = await fetchMediaAsset(path, accessToken);
    if (!res.ok) {
        throw new Error(`Download failed (${res.status})`);
    }
    const blob = await res.blob();
    const objectUrl = URL.createObjectURL(blob);
    try {
        const anchor = document.createElement('a');
        anchor.href = objectUrl;
        anchor.download = filename;
        anchor.rel = 'noopener';
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
    } finally {
        window.setTimeout(() => URL.revokeObjectURL(objectUrl), 0);
    }
}

/** Agent media route uses session Bearer; video tag cannot — fetch via blob in component if needed. */
export function agentJobMediaUrl(jobId: string, kind: AgentJobKind) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/media?kind=${kind}`);
}

export function agentJobPackageUrl(jobId: string, kind: AgentJobKind) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/package?kind=${kind}`);
}

export function agentJobPollUrl(jobId: string, kind: AgentJobKind, sessionId: string) {
    const q = new URLSearchParams({ kind, session_id: sessionId });
    return agentApi(`/api/studio-agent/jobs/${jobId}?${q.toString()}`);
}

export function isTerminalJob(snap: AgentJobSnapshot) {
    return snap.status === 'complete'
        || snap.status === 'failed'
        || snap.status === 'cancelled'
        || snap.status === 'awaiting_approval'
        || snap.status === 'awaiting_thumbnail_review'
        || Boolean(snap.thumbnail_only && snap.thumbnail_urls?.length);
}

export function isStaleLongformChapterFailure(
    snap: Pick<AgentJobSnapshot, 'kind' | 'status' | 'error'> | null | undefined,
): boolean {
    if (!snap || snap.kind !== 'longform' || snap.status !== 'failed') return false;
    return /LFRenderError|chapter \d+ JSON parse failed/i.test(String(snap.error || ''));
}

/** Stale long-form poll with no workspace progress (0% / unknown phase). */
export function isStaleDeadLongformPoll(
    snap: Pick<AgentJobSnapshot, 'kind' | 'status' | 'stage' | 'stage_label' | 'progress' | 'thumbnail_only'> | null | undefined,
): boolean {
    if (!snap || snap.kind !== 'longform' || snap.thumbnail_only) return false;
    if (snap.status !== 'running') return false;
    const progress = Number(snap.progress || 0);
    if (progress > 0) return false;
    const stage = String(snap.stage || '').toLowerCase();
    const label = String(snap.stage_label || '').toLowerCase();
    return stage === 'unknown'
        || stage === 'connecting'
        || label === 'unknown'
        || label.includes('connecting production');
}

/** Idle long-form workspace polled as failed when nothing is actively rendering. */
export function isStaleIdleLongformFailure(
    snap: Pick<AgentJobSnapshot, 'kind' | 'status' | 'error' | 'progress' | 'stage'> | null | undefined,
): boolean {
    if (!snap || snap.kind !== 'longform' || snap.status !== 'failed') return false;
    const err = String(snap.error || '').toLowerCase();
    if (err.includes('no active render')) return true;
    return Number(snap.progress || 0) === 0 && ['failed', 'unknown', ''].includes(String(snap.stage || '').toLowerCase());
}

const GHOST_POLL_ERROR_PATTERNS = [
    /Expecting value: line 1 column 1/i,
    /Production result file was empty or invalid/i,
    /Production workspace was lost/i,
    /reference status file empty or invalid/i,
];

/** Stale empty-workspace poll failures (often mislabeled competitor via title inference). */
export function isGhostJobPollFailure(
    snap: Pick<AgentJobSnapshot, 'job_id' | 'kind' | 'status' | 'error' | 'title'> | null | undefined,
): boolean {
    if (!snap || snap.status !== 'failed') return false;
    const err = String(snap.error || '');
    return GHOST_POLL_ERROR_PATTERNS.some((pattern) => pattern.test(err));
}

/** @deprecated Use isGhostJobPollFailure — kept for call-site compatibility. */
export function isGhostShortformPollFailure(
    snap: Pick<AgentJobSnapshot, 'job_id' | 'kind' | 'status' | 'error' | 'title'> | null | undefined,
): boolean {
    return isGhostJobPollFailure(snap);
}

export function isCompleteReferenceDeliverable(
    snap: Pick<AgentJobSnapshot, 'kind' | 'status' | 'visual_summary' | 'storytelling_summary' | 'transcript_excerpt' | 'pacing'> | null | undefined,
): boolean {
    if (!snap || snap.kind !== 'competitor' || snap.status !== 'complete') return false;
    return Boolean(
        snap.visual_summary
        || snap.storytelling_summary
        || snap.transcript_excerpt
        || snap.pacing?.duration_sec != null
        || snap.pacing?.cut_count != null,
    );
}

export function hasCompleteReferenceDeliverable<T extends { jobDeliverable?: AgentJobSnapshot }>(
    messages: T[],
): boolean {
    return messages.some((msg) => isCompleteReferenceDeliverable(msg.jobDeliverable));
}

export function messagesIndicateReferenceAnalysisComplete<T extends { role?: string; content?: string }>(
    messages: T[],
): boolean {
    return messages.some((msg) => {
        if (msg.role !== 'assistant') return false;
        const text = String(msg.content || '');
        if (!text.trim()) return false;
        return (
            /reference analysis is complete/i.test(text)
            || /analysis is complete and stored/i.test(text)
            || /I analyzed the video you uploaded/i.test(text)
            || (/visual style/i.test(text) && /pacing/i.test(text) && /hook/i.test(text))
            || (/Key Issue/i.test(text) && /Recommendations/i.test(text))
        );
    });
}

export function isImplicitProductionCancel(
    snap: Pick<AgentJobSnapshot, 'status' | 'error' | 'stage_label' | 'stage'> | null | undefined,
): boolean {
    if (!snap) return false;
    const label = String(snap.stage_label || snap.stage || '').toLowerCase();
    if (snap.status === 'cancelled' || label.includes('cancelled')) return true;
    return snap.status === 'failed'
        && /cancel+ed by user/i.test(String(snap.error || snap.stage_label || snap.stage || ''));
}

export function isBlockedJobId(jobId: string, blocked: string[] = []): boolean {
    const id = String(jobId || '').trim();
    if (!id || !blocked.length) return false;
    return blocked.some((blockedId) => {
        const bid = String(blockedId || '').trim();
        if (!bid) return false;
        return bid === id || bid.slice(0, 8) === id.slice(0, 8);
    });
}

/** Remove stale/blocked production cards and progress rows from chat history. */
export function stripStaleProductionArtifacts<
    T extends { role?: string; content?: string; jobDeliverable?: AgentJobSnapshot; productionUpdate?: ProductionProgressUpdate },
>(
    messages: T[],
    shouldSuppress: (jobId: string, title?: string) => boolean,
): T[] {
    let changed = false;
    const next = messages.map((row) => {
        const snap = row.jobDeliverable;
        const update = row.productionUpdate;
        const snapId = snap?.job_id || '';
        const updateId = update?.job_id || '';
        const dropSnap = Boolean(snapId && shouldSuppress(snapId, snap?.title));
        const dropUpdate = Boolean(updateId && shouldSuppress(updateId, update?.title));
        if (!dropSnap && !dropUpdate) return row;
        changed = true;
        const rest = { ...row };
        if (dropSnap) delete rest.jobDeliverable;
        if (dropUpdate) delete rest.productionUpdate;
        return rest;
    });
    return changed ? next : messages;
}

export function hasReferenceAnalysisSignal<T extends { role?: string; content?: string; jobDeliverable?: AgentJobSnapshot }>(
    messages: T[],
): boolean {
    return hasCompleteReferenceDeliverable(messages) || messagesIndicateReferenceAnalysisComplete(messages);
}

/** Hide stale failed cards when reference analysis already succeeded in this chat. */
export function shouldHideJobDeliverable<T extends { role?: string; content?: string; jobDeliverable?: AgentJobSnapshot }>(
    snap: AgentJobSnapshot | null | undefined,
    messages: T[] = [],
): boolean {
    if (!snap) return true;
    if (isGhostJobPollFailure(snap)) return true;
    if (isImplicitProductionCancel(snap)) return true;
    // Hide the 1-scene proof card only while expansion is actively rendering
    // the remaining scenes. Never hide awaiting_approval / animation-review —
    // that is when the creator must see and sign off Scene 1.
    const expansionRendering = messages.some((msg) => (
        msg.role === 'assistant'
        && /Before I build the remaining scenes|expanding (?:the )?short|building the remaining scenes/i.test(String(msg.content || ''))
    ));
    if (
        snap.kind === 'shortform'
        && expansionRendering
        && snap.status === 'running'
        && Number(snap.total_scenes || 0) <= 1
    ) {
        return true;
    }
    if (snap.status !== 'failed') return false;
    const kind = normalizeAgentJobKind(snap.job_id, snap.kind, snap.title);
    const hasActiveSuccessor = messages.some((msg) => {
        const other = msg.jobDeliverable;
        if (!other?.job_id || other.job_id === snap.job_id) return false;
        if (normalizeAgentJobKind(other.job_id, other.kind, other.title) !== kind) return false;
        return other.status === 'running' || other.status === 'awaiting_approval' || other.status === 'complete';
    });
    if (hasActiveSuccessor) return true;
    if ((kind === 'competitor' || kind === 'shortform') && hasReferenceAnalysisSignal(messages)) {
        return true;
    }
    return false;
}

/** Drop orphan shortform tracks when reference analysis is active in this session. */
export function pruneOrphanShortformTracks(
    tracks: AgentJobTrack[],
    serverJobs: AgentJobTrack[] = [],
): AgentJobTrack[] {
    const serverIds = new Set(serverJobs.map((job) => job.job_id).filter(Boolean));
    const hasReference = tracks.some((job) => job.kind === 'competitor')
        || serverJobs.some((job) => job.kind === 'competitor');
    if (!hasReference) return tracks;
    return tracks.filter((job) => job.kind !== 'shortform' || serverIds.has(job.job_id));
}

/** Clear stale / superseded deliverable cards while keeping the chat text. */
export function stripGhostJobDeliverables<T extends { jobDeliverable?: AgentJobSnapshot }>(
    messages: T[],
): T[] {
    const hide = hasReferenceAnalysisSignal(messages);
    let changed = false;
    const next = messages.map((msg) => {
        const snap = msg.jobDeliverable;
        const drop = snap && (
            isGhostJobPollFailure(snap)
            || (hide && snap.status === 'failed' && (
                snap.kind === 'competitor'
                || snap.kind === 'shortform'
                || normalizeAgentJobKind(snap.job_id, snap.kind, snap.title) === 'competitor'
            ))
        );
        if (!drop) return msg;
        changed = true;
        const { jobDeliverable: _dropDeliverable, ...rest } = msg;
        return rest as T;
    });
    return changed ? next : messages;
}

/** Active server jobs plus any in-chat deliverable cards that still need polling. */
function extractJobIdFromMessageContent(content: string): { job_id?: string; title?: string } {
    const text = String(content || '').trim();
    if (!text) return {};
    try {
        const direct = JSON.parse(text) as { job_id?: string; outline_title?: string; title?: string };
        if (direct?.job_id) {
            return { job_id: direct.job_id, title: direct.outline_title || direct.title };
        }
    } catch {
        /* not raw JSON */
    }
    const start = text.indexOf('{');
    const end = text.lastIndexOf('}');
    if (start >= 0 && end > start) {
        try {
            const data = JSON.parse(text.slice(start, end + 1)) as { job_id?: string; outline_title?: string; title?: string };
            if (data?.job_id) {
                return { job_id: data.job_id, title: data.outline_title || data.title };
            }
        } catch {
            /* ignore malformed embedded JSON */
        }
    }
    const match = text.match(/\b([a-f0-9]{12})\b/);
    return match ? { job_id: match[1] } : {};
}

export function collectTracksFromTranscript(
    messages: Array<{ role?: string; content?: string; jobDeliverable?: AgentJobSnapshot }>,
): AgentJobTrack[] {
    const map = new Map<string, AgentJobTrack>();
    for (const msg of [...messages].reverse()) {
        const snap = msg.jobDeliverable;
        if (snap?.job_id) {
            map.set(snap.job_id, {
                job_id: snap.job_id,
                kind: normalizeAgentJobKind(snap.job_id, snap.kind, snap.title),
                title: snap.title,
                started_at: Date.now(),
            });
            continue;
        }
        const role = String(msg.role || '');
        if (!['tool', 'system', 'assistant', 'user'].includes(role)) continue;
        const extracted = extractJobIdFromMessageContent(String(msg.content || ''));
        if (!extracted.job_id || map.has(extracted.job_id)) continue;
        map.set(extracted.job_id, {
            job_id: extracted.job_id,
            // A bare 12-hex Studio job id is the native short-form shape. Do
            // not force transcript-recovered ids into the long-form lane;
            // the status response can still correct the kind after polling.
            kind: normalizeAgentJobKind(extracted.job_id, undefined, extracted.title),
            title: extracted.title,
            started_at: Date.now(),
        });
    }
    return [...map.values()];
}

export function collectTracksToRefresh(
    tracks: AgentJobTrack[],
    messages: Array<{ role?: string; content?: string; jobDeliverable?: AgentJobSnapshot }>,
    blockedJobIds: string[] = [],
): AgentJobTrack[] {
    const map = new Map<string, AgentJobTrack>();
    for (const job of tracks) {
        if (!job.job_id || isBlockedJobId(job.job_id, blockedJobIds)) continue;
        map.set(job.job_id, {
            ...job,
            kind: normalizeAgentJobKind(job.job_id, job.kind, job.title),
        });
    }
    for (const job of collectTracksFromTranscript(messages)) {
        if (!job.job_id || map.has(job.job_id) || isBlockedJobId(job.job_id, blockedJobIds)) continue;
        map.set(job.job_id, job);
    }
    for (const msg of messages) {
        const snap = msg.jobDeliverable;
        if (!snap?.job_id) continue;
        if (map.has(snap.job_id)) continue;
        map.set(snap.job_id, {
            job_id: snap.job_id,
            kind: normalizeAgentJobKind(snap.job_id, snap.kind, snap.title),
            title: snap.title,
            started_at: Date.now(),
        });
    }
    return [...map.values()];
}

export function mergeJobTracks(existing: AgentJobTrack[], incoming: AgentJobTrack[]) {
    const map = new Map<string, AgentJobTrack>();
    for (const j of existing) {
        if (j.job_id) map.set(j.job_id, { ...j, kind: normalizeAgentJobKind(j.job_id, j.kind, j.title) });
    }
    for (const j of incoming) {
        if (!j.job_id) continue;
        const merged = { ...map.get(j.job_id), ...j };
        map.set(j.job_id, { ...merged, kind: normalizeAgentJobKind(j.job_id, merged.kind, merged.title) });
    }
    return [...map.values()];
}

const JOBS_STORAGE_KEY = 'studio_agent_active_jobs';

export function loadPersistedJobs(sessionId: string): AgentJobTrack[] {
    try {
        const raw = localStorage.getItem(`${JOBS_STORAGE_KEY}_${sessionId}`);
        if (!raw) return [];
        const parsed = JSON.parse(raw) as AgentJobTrack[];
        return Array.isArray(parsed)
            ? parsed
                .filter((job) => job?.job_id)
                .map((job) => ({ ...job, kind: normalizeAgentJobKind(job.job_id, job.kind, job.title) }))
            : [];
    } catch {
        return [];
    }
}

export function persistJobs(sessionId: string, jobs: AgentJobTrack[]) {
    try {
        localStorage.setItem(`${JOBS_STORAGE_KEY}_${sessionId}`, JSON.stringify(jobs));
    } catch {
        /* ignore */
    }
}

export function lastSessionStorageKey(userId?: string | null) {
    const uid = String(userId || '').trim();
    return uid ? `studio_agent_last_session_${uid}` : 'studio_agent_last_session';
}

export async function pollJobSnapshot(
    track: AgentJobTrack,
    sessionId: string,
    accessToken: string,
): Promise<AgentJobSnapshot | null> {
    const pollKind = normalizeAgentJobKind(track.job_id, track.kind, track.title);
    let res = await fetch(agentJobPollUrl(track.job_id, pollKind, sessionId), {
        headers: { Authorization: `Bearer ${accessToken}` },
    });
    if (res.status === 429 || res.status === 502 || res.status === 503) {
        await new Promise((r) => setTimeout(r, 2000));
        res = await fetch(agentJobPollUrl(track.job_id, pollKind, sessionId), {
            headers: { Authorization: `Bearer ${accessToken}` },
        });
    }
    if (!res.ok) return null;
    let data = (await res.json().catch(() => null)) as AgentJobSnapshot | null;
    if (!data || typeof data !== 'object') return null;

    if (isGhostJobPollFailure(data)) {
        const retryRes = await fetch(
            agentJobPollUrl(track.job_id, 'competitor', sessionId),
            { headers: { Authorization: `Bearer ${accessToken}` } },
        );
        if (retryRes.ok) {
            const retryData = (await retryRes.json().catch(() => null)) as AgentJobSnapshot | null;
            if (retryData && typeof retryData === 'object' && !isGhostJobPollFailure(retryData)) {
                data = retryData;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    const kind = normalizeAgentJobKind(track.job_id, data.kind || pollKind, data.title || track.title);
    return { ...data, kind, job_id: data.job_id || track.job_id };
}

export async function fetchJobSnapshot(
    track: AgentJobTrack,
    sessionId: string,
    accessToken: string,
): Promise<AgentJobSnapshot | null> {
    return pollJobSnapshot(track, sessionId, accessToken);
}

/** Poll saved jobs and return terminal snapshots for in-chat deliverable cards. */
export async function rehydrateJobSnapshots(
    sessionId: string,
    tracks: AgentJobTrack[],
    accessToken: string,
): Promise<{ deliverables: AgentJobSnapshot[] }> {
    const deliverables: AgentJobSnapshot[] = [];
    await Promise.all(
        tracks.map(async (track) => {
            const snap = await fetchJobSnapshot(track, sessionId, accessToken);
            if (
                snap
                && !isGhostJobPollFailure(snap)
                && (
                    Boolean(snap.thumbnail_only && snap.thumbnail_urls?.length)
                    ||
                    isTerminalJob(snap)
                    || snap.status === 'running'
                    || Boolean(snap.still_preview_urls?.length)
                    || Boolean(snap.scenes?.length)
                )
            ) {
                deliverables.push(snap);
            }
        }),
    );
    return { deliverables };
}
