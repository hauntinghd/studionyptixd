import { resolveStudioBackendUrl } from './backend';

const agentApi = (path: string) => resolveStudioBackendUrl(path);

export type AgentJobKind = 'longform' | 'shortform' | 'competitor';

export type AgentJobTrack = {
    job_id: string;
    kind: AgentJobKind;
    title?: string;
    started_at?: number;
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
    mp4_url?: string;
    download_url?: string;
    preview_url?: string;
    current_scene?: number;
    total_scenes?: number;
    current_chapter?: number;
    total_chapters?: number;
    analysis_ready?: boolean;
    can_finalize?: boolean;
    still_count?: number;
    still_preview_urls?: string[];
    pacing?: {
        avg_shot_sec?: number | null;
        cut_count?: number;
        duration_sec?: number;
        hook_window_sec?: number;
    };
    engagement?: Record<string, number>;
    frame_count?: number;
    blueprint_hint?: string;
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

export function agentJobFinalizeUrl(jobId: string) {
    return agentApi(`/api/studio-agent/jobs/${jobId}/finalize`);
}

export async function finalizeLongformJob(
    jobId: string,
    accessToken: string,
): Promise<{ active_jobs?: unknown[] }> {
    const res = await fetch(agentJobFinalizeUrl(jobId), {
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
        if (j.job_id) map.set(j.job_id, j);
    }
    for (const j of incoming) {
        if (!j.job_id) continue;
        map.set(j.job_id, { ...map.get(j.job_id), ...j });
    }
    return [...map.values()];
}

const JOBS_STORAGE_KEY = 'studio_agent_active_jobs';

export function loadPersistedJobs(sessionId: string): AgentJobTrack[] {
    try {
        const raw = localStorage.getItem(`${JOBS_STORAGE_KEY}_${sessionId}`);
        if (!raw) return [];
        const parsed = JSON.parse(raw) as AgentJobTrack[];
        return Array.isArray(parsed) ? parsed : [];
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
