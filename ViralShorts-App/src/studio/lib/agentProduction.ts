import { resolveStudioBackendUrl } from './backend';

const agentApi = (path: string) => resolveStudioBackendUrl(path);

export type AgentJobKind = 'longform' | 'shortform' | 'competitor' | 'cliplab';

export type AgentJobTrack = {
    job_id: string;
    kind: AgentJobKind;
    title?: string;
    started_at?: number;
};

export function normalizeAgentJobKind(_jobId: string, kind?: string): AgentJobKind {
    if (kind === 'longform' || kind === 'shortform' || kind === 'competitor' || kind === 'cliplab') return kind;
    return 'shortform';
}

export type AgentSceneSnapshot = {
    index: number;
    sid?: string | null;
    narration?: string | null;
    scene_action?: string | null;
    duration_sec?: number;
    status?: string | null;
    animate?: boolean;
    approved_for_video?: boolean;
    approved_for_animation?: boolean;
    has_clip?: boolean;
    video_model?: string | null;
    last_edit?: unknown;
    still_preview_url?: string;
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
    cost?: {
        actual_usd?: number;
        actual_usd_decimal?: string;
        event_count?: number;
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

export function agentJobStillUrl(jobId: string, sceneIdx: number) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/still/${sceneIdx}`);
}

export function agentJobSceneApprovalUrl(jobId: string, sceneIdx: number) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/scene/${sceneIdx}/approval`);
}

export function agentJobScenesApprovalUrl(jobId: string) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/scenes/approval`);
}

export function agentJobAnimateUrl(jobId: string) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/animate`);
}

export function agentJobFinalizeUrl(jobId: string, kind: AgentJobKind = 'longform') {
    const qs = new URLSearchParams({ kind });
    return agentApi(`/api/studio-agent/jobs/${jobId}/finalize?${qs.toString()}`);
}

export async function finalizeLongformJob(
    jobId: string,
    accessToken: string,
): Promise<{ active_jobs?: unknown[] }> {
    const res = await fetch(agentJobFinalizeUrl(jobId, 'longform'), {
        method: 'POST',
        headers: { Authorization: `Bearer ${accessToken}` },
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

export function mediaUrl(path: string, accessToken: string) {
    const base = path.startsWith('http') ? path : agentApi(path.startsWith('/') ? path : `/${path}`);
    const sep = base.includes('?') ? '&' : '?';
    return `${base}${sep}token=${encodeURIComponent(accessToken)}`;
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
    return snap.status === 'complete' || snap.status === 'failed' || snap.status === 'awaiting_approval';
}

export function mergeJobTracks(existing: AgentJobTrack[], incoming: AgentJobTrack[]) {
    const map = new Map<string, AgentJobTrack>();
    for (const j of existing) {
        if (j.job_id) map.set(j.job_id, { ...j, kind: normalizeAgentJobKind(j.job_id, j.kind) });
    }
    for (const j of incoming) {
        if (!j.job_id) continue;
        const merged = { ...map.get(j.job_id), ...j };
        map.set(j.job_id, { ...merged, kind: normalizeAgentJobKind(j.job_id, merged.kind) });
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
                .map((job) => ({ ...job, kind: normalizeAgentJobKind(job.job_id, job.kind) }))
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

export async function fetchJobSnapshot(
    track: AgentJobTrack,
    sessionId: string,
    accessToken: string,
): Promise<AgentJobSnapshot | null> {
    const res = await fetch(agentJobPollUrl(track.job_id, track.kind, sessionId), {
        headers: { Authorization: `Bearer ${accessToken}` },
    });
    if (!res.ok) return null;
    return (await res.json().catch(() => null)) as AgentJobSnapshot | null;
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
            if (snap && isTerminalJob(snap)) deliverables.push(snap);
        }),
    );
    return { deliverables };
}
