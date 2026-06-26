import { useCallback, useEffect, useRef, useState } from 'react';
import {
    type AgentJobSnapshot,
    type AgentJobTrack,
    type ProductionProgressUpdate,
    agentJobPollUrl,
    finalizeLongformJob,
    isTerminalJob,
} from '../lib/agentProduction';

const POLL_MS = 2500;

export function useAgentProductionJobs({
    sessionId,
    tracks,
    getToken,
    onJobComplete,
    onJobFailed,
    onAwaitingApproval,
    onProgress,
    autoFinalizeLongform = false,
    onAutoFinalizeStarted,
    pollResetKey = 0,
}: {
    sessionId: string | null;
    tracks: AgentJobTrack[];
    getToken: () => Promise<string>;
    /** Bump after Retry to drop stale failed snapshots and restart polling. */
    pollResetKey?: number;
    onJobComplete?: (snap: AgentJobSnapshot) => void;
    onJobFailed?: (snap: AgentJobSnapshot) => void;
    onAwaitingApproval?: (snap: AgentJobSnapshot) => void;
    onProgress?: (update: ProductionProgressUpdate, snap: AgentJobSnapshot) => void;
    /** When true, POST finalize as soon as stills gate opens (auto-approve mode). */
    autoFinalizeLongform?: boolean;
    onAutoFinalizeStarted?: (jobId: string, activeJobs?: unknown) => void;
}) {
    const [snapshots, setSnapshots] = useState<Record<string, AgentJobSnapshot>>({});
    const completedRef = useRef<Set<string>>(new Set());
    const lastStageRef = useRef<Record<string, string>>({});
    const autoFinalizeRef = useRef<Set<string>>(new Set());

    const pollOne = useCallback(
        async (track: AgentJobTrack) => {
            if (!sessionId) return;
            const tok = await getToken();
            const url = agentJobPollUrl(track.job_id, track.kind, sessionId);
            let res = await fetch(url, {
                headers: { Authorization: `Bearer ${tok}` },
            });
            if (res.status === 429 || res.status === 502 || res.status === 503) {
                await new Promise((r) => setTimeout(r, 2000));
                res = await fetch(url, { headers: { Authorization: `Bearer ${tok}` } });
            }
            const data = (await res.json().catch(() => ({}))) as AgentJobSnapshot;
            if (!res.ok) return;

            const stageKey = `${data.stage || ''}:${data.progress}:${data.status}`;
            const prevStage = lastStageRef.current[track.job_id];
            if (prevStage !== stageKey) {
                lastStageRef.current[track.job_id] = stageKey;
                onProgress?.(
                    {
                        job_id: track.job_id,
                        kind: track.kind,
                        stage_label: data.stage_label || data.stage || 'Working',
                        progress: Number(data.progress || 0),
                        title: track.title || data.title,
                    },
                    data,
                );
            }

            setSnapshots((prev) => ({ ...prev, [track.job_id]: data }));

            const key = `${track.kind}:${track.job_id}`;
            if (!isTerminalJob(data)) return;
            if (completedRef.current.has(key)) return;
            completedRef.current.add(key);
            if (data.status === 'complete') onJobComplete?.(data);
            else if (data.status === 'failed') onJobFailed?.(data);
            else if (data.status === 'awaiting_approval') {
                onAwaitingApproval?.(data);
                if (
                    autoFinalizeLongform
                    && data.kind === 'longform'
                    && data.can_finalize
                    && !autoFinalizeRef.current.has(track.job_id)
                ) {
                    autoFinalizeRef.current.add(track.job_id);
                    void (async () => {
                        try {
                            const tok = await getToken();
                            const out = await finalizeLongformJob(track.job_id, tok);
                            onAutoFinalizeStarted?.(track.job_id, out.active_jobs);
                        } catch {
                            autoFinalizeRef.current.delete(track.job_id);
                        }
                    })();
                }
            }
        },
        [
            autoFinalizeLongform,
            getToken,
            onAutoFinalizeStarted,
            onAwaitingApproval,
            onJobComplete,
            onJobFailed,
            onProgress,
            sessionId,
        ],
    );

    useEffect(() => {
        completedRef.current = new Set();
        lastStageRef.current = {};
        autoFinalizeRef.current = new Set();
        setSnapshots({});
    }, [sessionId, pollResetKey]);

    useEffect(() => {
        const running = tracks.filter((t) => {
            const snap = snapshots[t.job_id];
            return !snap || (snap.running !== false && !isTerminalJob(snap));
        });
        if (!running.length || !sessionId) return;

        let cancelled = false;
        const tick = async () => {
            await Promise.all(
                running.map(async (track) => {
                    if (cancelled) return;
                    try {
                        await pollOne(track);
                    } catch {
                        /* retry */
                    }
                }),
            );
        };
        void tick();
        const id = window.setInterval(() => void tick(), POLL_MS);
        return () => {
            cancelled = true;
            clearInterval(id);
        };
    }, [tracks, sessionId, pollOne]);

    const activeTracks = tracks.filter((t) => {
        const s = snapshots[t.job_id];
        return s && s.running !== false && !isTerminalJob(s);
    });

    const primary =
        activeTracks[0]
        ?? [...tracks].sort((a, b) => Number(b.started_at || 0) - Number(a.started_at || 0))[0];
    const primarySnap = primary
        ? snapshots[primary.job_id] ?? {
              job_id: primary.job_id,
              kind: primary.kind,
              status: 'running',
              progress: 8,
              stage: 'starting',
              stage_label: primary.kind === 'competitor' ? 'Starting analysis' : 'Starting production',
              stage_detail: 'Spawning render on the server…',
              running: true,
              title: primary.title,
          }
        : undefined;

    return { snapshots, activeTracks, primary, primarySnap, pollOne };
}
