import { useCallback, useEffect, useRef, useState } from 'react';
import {
    type AgentJobSnapshot,
    type AgentJobTrack,
    type ProductionProgressUpdate,
    finalizeLongformJob,
    isGhostJobPollFailure,
    isImplicitProductionCancel,
    isStaleDeadLongformPoll,
    isStaleIdleLongformFailure,
    isStaleLongformChapterFailure,
    isTerminalJob,
    normalizeAgentJobKind,
    pollJobSnapshot,
} from '../lib/agentProduction';

// Status reads are non-enqueueing GETs. Three seconds is responsive enough for
// renders while avoiding the old ~3,000 requests/job/hour cadence that became
// catastrophic when a legacy proxy incorrectly converted every read to /runsync.
const POLL_MS = 3000;

function shouldPollTrack(snap: AgentJobSnapshot | undefined): boolean {
    if (!snap) return true;
    if (snap.status === 'failed') return false;
    if (snap.status === 'complete') return false;
    if (isStaleDeadLongformPoll(snap)) return false;
    if (snap.kind === 'shortform') {
        return snap.status === 'running'
            || snap.status === 'awaiting_approval'
            || snap.status === 'restarting';
    }
    return snap.running !== false && !isTerminalJob(snap);
}

function deliverableSignature(data: AgentJobSnapshot): string {
    const scenes = (data.scenes || [])
        .map((scene) => [
            scene.index,
            scene.has_clip ? 1 : 0,
            scene.approved_for_video ? 1 : 0,
            scene.approved_for_animation ? 1 : 0,
            scene.animate ? 1 : 0,
            scene.still_preview_url || '',
            scene.clip_preview_url || '',
        ].join(':'))
        .join(',');
    const stillUrls = (data.still_preview_urls || []).join('|');
    return [
        data.status,
        data.stage,
        data.stage_detail || '',
        data.progress,
        data.current_scene,
        data.total_scenes,
        data.approved_scene_count,
        data.animation_pending_count,
        data.animation_complete_count,
        data.mp4_url || '',
        scenes,
        stillUrls,
    ].join('|');
}

function shouldRefreshDeliverable(data: AgentJobSnapshot): boolean {
    if (data.kind !== 'shortform') return false;
    return (
        data.status === 'running'
        || data.status === 'awaiting_approval'
        || data.status === 'complete'
    ) && (
        Boolean(data.scenes?.length)
        || Boolean(data.still_preview_urls?.length)
        || Boolean(data.mp4_url)
    );
}

export function useAgentProductionJobs({
    sessionId,
    tracks,
    getToken,
    onJobComplete,
    onJobFailed,
    onGhostJobDropped,
    onAwaitingApproval,
    onRunningPreview,
    onProgress,
    autoFinalizeLongform = false,
    onAutoFinalizeStarted,
    shouldPollJobTrack,
    shouldAcceptSnapshot,
    pollResetKey = 0,
}: {
    sessionId: string | null;
    tracks: AgentJobTrack[];
    getToken: () => Promise<string>;
    /** Skip polling/rendering dock tracks that belong to a prior short. */
    shouldPollJobTrack?: (track: AgentJobTrack) => boolean;
    /** Reject a stale status read before it can replace live stream state. */
    shouldAcceptSnapshot?: (snapshot: AgentJobSnapshot, track: AgentJobTrack) => boolean;
    /** Bump after Retry to drop stale failed snapshots and restart polling. */
    pollResetKey?: number;
    onJobComplete?: (snap: AgentJobSnapshot) => void;
    onJobFailed?: (snap: AgentJobSnapshot) => void;
    /** Stale shortform poll with empty result.json — drop track silently. */
    onGhostJobDropped?: (track: AgentJobTrack) => void;
    onAwaitingApproval?: (snap: AgentJobSnapshot) => void;
    onRunningPreview?: (snap: AgentJobSnapshot) => void;
    onProgress?: (update: ProductionProgressUpdate, snap: AgentJobSnapshot) => void;
    /** When true, POST finalize as soon as stills gate opens (auto-approve mode). */
    autoFinalizeLongform?: boolean;
    onAutoFinalizeStarted?: (jobId: string, activeJobs?: unknown) => void;
}) {
    const [snapshots, setSnapshots] = useState<Record<string, AgentJobSnapshot>>({});
    const snapshotsRef = useRef(snapshots);
    snapshotsRef.current = snapshots;
    const completedRef = useRef<Set<string>>(new Set());
    const lastStageRef = useRef<Record<string, string>>({});
    const lastDeliverableSigRef = useRef<Record<string, string>>({});
    const autoFinalizeRef = useRef<Set<string>>(new Set());
    const tracksRef = useRef(tracks);
    tracksRef.current = tracks;
    // AgentPanel intentionally supplies context-aware callbacks. Several are
    // inline because they read refs, so their function identities can change on
    // harmless parent renders. Keep the polling loop stable and always invoke
    // the latest callbacks through this ref instead of rebuilding the interval.
    const configRef = useRef({
        sessionId,
        getToken,
        onJobComplete,
        onJobFailed,
        onGhostJobDropped,
        onAwaitingApproval,
        onRunningPreview,
        onProgress,
        autoFinalizeLongform,
        onAutoFinalizeStarted,
        shouldPollJobTrack,
        shouldAcceptSnapshot,
    });
    configRef.current = {
        sessionId,
        getToken,
        onJobComplete,
        onJobFailed,
        onGhostJobDropped,
        onAwaitingApproval,
        onRunningPreview,
        onProgress,
        autoFinalizeLongform,
        onAutoFinalizeStarted,
        shouldPollJobTrack,
        shouldAcceptSnapshot,
    };

    const pollOne = useCallback(
        async (track: AgentJobTrack) => {
            const config = configRef.current;
            if (!config.sessionId) return;
            if (config.shouldPollJobTrack && !config.shouldPollJobTrack(track)) return;
            const tok = await config.getToken();
            const data = await pollJobSnapshot(track, config.sessionId, tok);
            // A transient HTTP/auth/network miss is not proof that a render is
            // gone. Keep the track and retry instead of freezing/removing it.
            if (!data) return;
            if (isGhostJobPollFailure(data)) {
                config.onGhostJobDropped?.(track);
                return;
            }

            const snapshotKind = normalizeAgentJobKind(
                track.job_id,
                data.kind || track.kind,
                data.title || track.title,
            );
            const snapshot = {
                ...data,
                kind: snapshotKind,
                job_id: data.job_id || track.job_id,
                client_updated_at: Date.now(),
            };
            if (config.shouldAcceptSnapshot && !config.shouldAcceptSnapshot(snapshot, track)) return;
            const stageKey = `${snapshot.stage || ''}:${snapshot.progress}:${snapshot.status}`;
            const prevStage = lastStageRef.current[track.job_id];
            if (prevStage !== stageKey) {
                lastStageRef.current[track.job_id] = stageKey;
                config.onProgress?.(
                    {
                        job_id: track.job_id,
                        kind: snapshotKind,
                        stage_label: snapshot.stage_label || snapshot.stage || 'Working',
                        progress: Number(snapshot.progress || 0),
                        title: track.title || snapshot.title,
                    },
                    snapshot,
                );
            }

            setSnapshots((prev) => ({ ...prev, [track.job_id]: snapshot }));

            if (shouldRefreshDeliverable(snapshot)) {
                const sig = deliverableSignature(snapshot);
                if (lastDeliverableSigRef.current[track.job_id] !== sig) {
                    lastDeliverableSigRef.current[track.job_id] = sig;
                    config.onRunningPreview?.(snapshot);
                }
            }

            const key = `${snapshotKind}:${track.job_id}`;
            if (!isTerminalJob(snapshot)) return;
            if (completedRef.current.has(key)) return;
            completedRef.current.add(key);
            if (snapshot.status === 'complete') config.onJobComplete?.(snapshot);
            else if (snapshot.status === 'failed' || snapshot.status === 'cancelled') {
                if (
                    isImplicitProductionCancel(snapshot)
                    || isStaleLongformChapterFailure(snapshot)
                    || isStaleIdleLongformFailure(snapshot)
                ) {
                    return;
                }
                config.onJobFailed?.(snapshot);
            }
            else if (snapshot.status === 'awaiting_approval') {
                config.onAwaitingApproval?.(snapshot);
                if (
                    config.autoFinalizeLongform
                    && snapshot.kind === 'longform'
                    && snapshot.can_finalize
                    && !autoFinalizeRef.current.has(track.job_id)
                ) {
                    autoFinalizeRef.current.add(track.job_id);
                    void (async () => {
                        try {
                            const commandSessionId = config.sessionId;
                            if (!commandSessionId) {
                                autoFinalizeRef.current.delete(track.job_id);
                                return;
                            }
                            const tok = await configRef.current.getToken();
                            const out = await finalizeLongformJob(
                                track.job_id,
                                tok,
                                commandSessionId,
                            );
                            configRef.current.onAutoFinalizeStarted?.(track.job_id, out.active_jobs);
                        } catch {
                            autoFinalizeRef.current.delete(track.job_id);
                        }
                    })();
                }
            }
        },
        [],
    );

    const eligibleTracks = shouldPollJobTrack
        ? tracks.filter((track) => shouldPollJobTrack(track))
        : tracks;
    const eligibleTrackKey = eligibleTracks.map((t) => t.job_id).join(',');

    useEffect(() => {
        completedRef.current = new Set();
        lastStageRef.current = {};
        lastDeliverableSigRef.current = {};
        autoFinalizeRef.current = new Set();
        setSnapshots({});
    }, [sessionId, pollResetKey]);

    useEffect(() => {
        if (!sessionId || !eligibleTrackKey) return;
        const currentEligibleTracks = tracksRef.current.filter((track) => (
            !configRef.current.shouldPollJobTrack
            || configRef.current.shouldPollJobTrack(track)
        ));
        void Promise.all(currentEligibleTracks.map((track) => pollOne(track).catch(() => {})));
    }, [eligibleTrackKey, sessionId, pollResetKey, pollOne]);

    useEffect(() => {
        if (!sessionId || !eligibleTrackKey) return;

        let cancelled = false;
        const tick = async () => {
            if (document.visibilityState === 'hidden') return;
            const currentEligibleTracks = tracksRef.current.filter((track) => (
                !configRef.current.shouldPollJobTrack
                || configRef.current.shouldPollJobTrack(track)
            ));
            const running = currentEligibleTracks.filter((t) => shouldPollTrack(snapshotsRef.current[t.job_id]));
            if (!running.length) return;
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
        // The preceding effect performs the immediate read. This effect owns
        // only the recurring cadence, so mounting a track does not double-poll.
        const id = window.setInterval(() => void tick(), POLL_MS);
        const refreshNow = () => void tick();
        window.addEventListener('focus', refreshNow);
        window.addEventListener('online', refreshNow);
        const refreshWhenVisible = () => {
            if (document.visibilityState === 'visible') void tick();
        };
        document.addEventListener('visibilitychange', refreshWhenVisible);
        return () => {
            cancelled = true;
            clearInterval(id);
            window.removeEventListener('focus', refreshNow);
            window.removeEventListener('online', refreshNow);
            document.removeEventListener('visibilitychange', refreshWhenVisible);
        };
    }, [eligibleTrackKey, sessionId, pollOne]);

    const activeTracks = eligibleTracks.filter((t) => shouldPollTrack(snapshots[t.job_id]));

    const dockCandidates = eligibleTracks.filter((track) => !isStaleDeadLongformPoll(snapshots[track.job_id]));
    const primary =
        activeTracks[0]
        ?? [...dockCandidates].sort((a, b) => Number(b.started_at || 0) - Number(a.started_at || 0))[0];
    const primarySnap = primary
        ? snapshots[primary.job_id] ?? {
              job_id: primary.job_id,
              kind: primary.kind,
              status: 'running',
              progress: 0,
              stage: 'connecting',
              stage_label: primary.kind === 'competitor' ? 'Connecting analysis' : 'Connecting production',
              stage_detail: 'Fetching live status from the server…',
              running: true,
              title: primary.title,
          }
        : undefined;

    const clearSnapshot = useCallback((jobId: string) => {
        const key = String(jobId || '').trim();
        if (!key) return;
        setSnapshots((prev) => {
            if (!prev[key]) return prev;
            const next = { ...prev };
            delete next[key];
            return next;
        });
        delete lastStageRef.current[key];
        delete lastDeliverableSigRef.current[key];
        for (const completedKey of [...completedRef.current]) {
            if (completedKey.endsWith(`:${key}`)) completedRef.current.delete(completedKey);
        }
    }, []);

    return { snapshots, activeTracks, primary, primarySnap, pollOne, clearSnapshot };
}
