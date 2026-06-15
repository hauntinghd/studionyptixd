import { ArrowLeft, Box, Check, CheckCircle2, Clapperboard, Download, FileText, Film, Loader2, Play, RefreshCw, Search, Wand2, X } from 'lucide-react';
import { createElement, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { AuthContext } from '../../shared';
import type { AgentJobSnapshot, AgentSceneSnapshot } from '../../lib/agentProduction';
import {
    agentJobFinalizeUrl,
    agentJobMediaUrl,
    agentJobPackageUrl,
    agentJobSceneApprovalUrl,
    agentJobStillUrl,
    mediaUrl,
} from '../../lib/agentProduction';

export type SceneReplyPreset = 'edit' | 'regenerate';

function StillThumb({ jobId, idx }: { jobId: string; idx: number }) {
    const { session } = useContext(AuthContext);
    const [src, setSrc] = useState('');

    useEffect(() => {
        const tok = session?.access_token;
        if (!tok) return;
        let cancelled = false;
        let objectUrl = '';
        (async () => {
            try {
                const res = await fetch(agentJobStillUrl(jobId, idx), {
                    headers: { Authorization: `Bearer ${tok}` },
                });
                if (!res.ok || cancelled) return;
                const blob = await res.blob();
                objectUrl = URL.createObjectURL(blob);
                if (!cancelled) setSrc(objectUrl);
            } catch {
                /* ignore */
            }
        })();
        return () => {
            cancelled = true;
            if (objectUrl) URL.revokeObjectURL(objectUrl);
        };
    }, [jobId, idx, session?.access_token]);

    if (!src) {
        return (
            <div className="aspect-video animate-pulse rounded-lg bg-white/[0.035] border border-white/[0.06] flex items-center justify-center">
                <Loader2 className="h-3 w-3 text-gray-600 animate-spin" />
            </div>
        );
    }
    return (
        <img
            src={src}
            alt={`Scene ${idx + 1}`}
            className="aspect-video w-full rounded-lg border border-white/[0.1] object-cover shadow-sm"
        />
    );
}

function isModelUrl(url?: string) {
    return /\.(glb|gltf)(\?|#|$)/i.test(String(url || ''));
}

function useModelViewerScript(enabled: boolean) {
    const [ready, setReady] = useState(() => (
        typeof customElements !== 'undefined' && Boolean(customElements.get('model-viewer'))
    ));

    useEffect(() => {
        if (typeof document === 'undefined' || typeof customElements === 'undefined') return;
        if (!enabled) return;
        if (customElements.get('model-viewer')) {
            setReady(true);
            return;
        }
        const existing = document.querySelector<HTMLScriptElement>('script[data-studio-model-viewer]');
        if (existing) {
            existing.addEventListener('load', () => setReady(true), { once: true });
            return;
        }
        const script = document.createElement('script');
        script.type = 'module';
        script.src = 'https://unpkg.com/@google/model-viewer/dist/model-viewer.min.js';
        script.dataset.studioModelViewer = 'true';
        script.addEventListener('load', () => setReady(true), { once: true });
        document.head.appendChild(script);
    }, [enabled]);

    return ready;
}

function CharacterModelModal({
    title,
    modelUrl,
    onClose,
}: {
    title: string;
    modelUrl: string;
    onClose: () => void;
}) {
    const ready = useModelViewerScript(Boolean(modelUrl));

    return (
        <div className="fixed inset-0 z-[90] flex items-center justify-center bg-black/80 p-4 backdrop-blur-md">
            <div className="flex h-[min(760px,90vh)] w-[min(980px,94vw)] flex-col overflow-hidden rounded-2xl border border-cyan-400/20 bg-[#07090d] shadow-2xl shadow-black">
                <div className="flex items-center justify-between border-b border-white/10 px-4 py-3">
                    <div>
                        <p className="text-sm font-semibold text-white">{title}</p>
                        <p className="text-[11px] text-gray-500">Drag to rotate. Scroll to zoom. Inspect before animation.</p>
                    </div>
                    <button
                        type="button"
                        onClick={onClose}
                        className="rounded-lg border border-white/10 bg-white/[0.04] p-2 text-gray-300 hover:bg-white/[0.08] hover:text-white"
                        title="Close 3D preview"
                    >
                        <X className="h-4 w-4" />
                    </button>
                </div>
                <div className="relative min-h-0 flex-1 bg-[radial-gradient(circle_at_center,rgba(34,211,238,0.12),transparent_45%),linear-gradient(180deg,#090d14,#030405)]">
                    {ready ? (
                        createElement('model-viewer', {
                            src: modelUrl,
                            'camera-controls': true,
                            'auto-rotate': true,
                            exposure: '1',
                            'shadow-intensity': '0.7',
                            style: { width: '100%', height: '100%', display: 'block' },
                        })
                    ) : (
                        <div className="flex h-full items-center justify-center text-sm text-gray-400">
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                            Loading 3D viewer...
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}

function StillInspectionModal({
    jobId,
    idx,
    title,
    onClose,
    onEdit,
    onRegenerate,
    onApproveStill,
    onApproveAnimate,
}: {
    jobId: string;
    idx: number;
    title: string;
    onClose: () => void;
    onEdit?: () => void;
    onRegenerate?: () => void;
    onApproveStill?: () => void;
    onApproveAnimate?: () => void;
}) {
    const { session } = useContext(AuthContext);
    const [src, setSrc] = useState('');
    const [zoom, setZoom] = useState(1);
    const [pan, setPan] = useState({ x: 0, y: 0 });
    const [dragStart, setDragStart] = useState<{ pointerId: number; x: number; y: number; panX: number; panY: number } | null>(null);

    useEffect(() => {
        const tok = session?.access_token;
        if (!tok) return;
        let cancelled = false;
        let objectUrl = '';
        (async () => {
            try {
                const res = await fetch(agentJobStillUrl(jobId, idx), {
                    headers: { Authorization: `Bearer ${tok}` },
                });
                if (!res.ok || cancelled) return;
                const blob = await res.blob();
                objectUrl = URL.createObjectURL(blob);
                if (!cancelled) setSrc(objectUrl);
            } catch {
                /* ignore */
            }
        })();
        return () => {
            cancelled = true;
            if (objectUrl) URL.revokeObjectURL(objectUrl);
        };
    }, [idx, jobId, session?.access_token]);

    const reset = () => {
        setZoom(1);
        setPan({ x: 0, y: 0 });
        setDragStart(null);
    };

    const adjustZoom = (next: number) => {
        setZoom(Math.max(0.5, Math.min(5, next)));
    };

    return (
        <div className="fixed inset-0 z-[95] flex items-center justify-center bg-black/85 p-4 backdrop-blur-md">
            <div className="flex h-[min(860px,94vh)] w-[min(1180px,96vw)] flex-col overflow-hidden rounded-2xl border border-cyan-400/20 bg-[#07090d] shadow-2xl shadow-black">
                <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/10 px-4 py-3">
                    <div>
                        <p className="text-sm font-semibold text-white">{title}</p>
                        <p className="text-[11px] text-gray-500">Zoom and pan to inspect artifacts before animation.</p>
                    </div>
                    <div className="flex items-center gap-2">
                        <button type="button" onClick={() => adjustZoom(zoom - 0.25)} className="rounded-lg border border-white/10 bg-white/[0.04] px-3 py-2 text-xs font-semibold text-gray-200 hover:bg-white/[0.08]">-</button>
                        <div className="min-w-14 rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-center text-xs tabular-nums text-gray-300">{Math.round(zoom * 100)}%</div>
                        <button type="button" onClick={() => adjustZoom(zoom + 0.25)} className="rounded-lg border border-white/10 bg-white/[0.04] px-3 py-2 text-xs font-semibold text-gray-200 hover:bg-white/[0.08]">+</button>
                        <button type="button" onClick={reset} className="rounded-lg border border-white/10 bg-white/[0.04] px-3 py-2 text-xs font-semibold text-gray-200 hover:bg-white/[0.08]">Reset</button>
                        {onEdit && (
                            <button type="button" onClick={onEdit} className="rounded-lg border border-cyan-400/20 bg-cyan-500/10 px-3 py-2 text-xs font-semibold text-cyan-100 hover:bg-cyan-500/15">
                                Edit this scene
                            </button>
                        )}
                        {onRegenerate && (
                            <button type="button" onClick={onRegenerate} className="rounded-lg border border-white/10 bg-white/[0.04] px-3 py-2 text-xs font-semibold text-gray-200 hover:bg-white/[0.08]">
                                Regenerate
                            </button>
                        )}
                        {onApproveStill && (
                            <button type="button" onClick={onApproveStill} className="rounded-lg border border-emerald-400/20 bg-emerald-500/10 px-3 py-2 text-xs font-semibold text-emerald-100 hover:bg-emerald-500/15">
                                Approve still
                            </button>
                        )}
                        {onApproveAnimate && (
                            <button type="button" onClick={onApproveAnimate} className="rounded-lg border border-violet-400/20 bg-violet-500/10 px-3 py-2 text-xs font-semibold text-violet-100 hover:bg-violet-500/15">
                                Approve + animate
                            </button>
                        )}
                        <button type="button" onClick={onClose} className="rounded-lg border border-white/10 bg-white/[0.04] p-2 text-gray-300 hover:bg-white/[0.08] hover:text-white" title="Close still inspector">
                            <X className="h-4 w-4" />
                        </button>
                    </div>
                </div>
                <div
                    className="relative min-h-0 flex-1 overflow-hidden bg-[radial-gradient(circle_at_center,rgba(34,211,238,0.08),transparent_45%),linear-gradient(180deg,#090d14,#030405)]"
                    onWheel={(event) => {
                        event.preventDefault();
                        adjustZoom(zoom + (event.deltaY < 0 ? 0.2 : -0.2));
                    }}
                    onPointerDown={(event) => {
                        event.currentTarget.setPointerCapture(event.pointerId);
                        setDragStart({ pointerId: event.pointerId, x: event.clientX, y: event.clientY, panX: pan.x, panY: pan.y });
                    }}
                    onPointerMove={(event) => {
                        if (!dragStart || dragStart.pointerId !== event.pointerId) return;
                        setPan({
                            x: dragStart.panX + event.clientX - dragStart.x,
                            y: dragStart.panY + event.clientY - dragStart.y,
                        });
                    }}
                    onPointerUp={(event) => {
                        if (dragStart?.pointerId === event.pointerId) setDragStart(null);
                    }}
                    onPointerCancel={() => setDragStart(null)}
                >
                    {src ? (
                        <img
                            src={src}
                            alt={`Scene ${idx + 1} inspection`}
                            draggable={false}
                            className={`absolute left-1/2 top-1/2 max-h-[88%] max-w-[88%] select-none object-contain ${dragStart ? 'cursor-grabbing' : 'cursor-grab'}`}
                            style={{
                                transform: `translate(calc(-50% + ${pan.x}px), calc(-50% + ${pan.y}px)) scale(${zoom})`,
                                transformOrigin: 'center',
                            }}
                        />
                    ) : (
                        <div className="flex h-full items-center justify-center text-sm text-gray-400">
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                            Loading still...
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}

export default function AgentJobDeliverable({
    snapshot,
    onFinalizeStarted,
    onReply,
    onSnapshotUpdate,
}: {
    snapshot: AgentJobSnapshot;
    onFinalizeStarted?: (jobId: string, activeJobs?: unknown) => void;
    onReply?: (snapshot: AgentJobSnapshot, sceneIndex?: number, preset?: SceneReplyPreset) => void;
    onSnapshotUpdate?: (snapshot: AgentJobSnapshot) => void;
}) {
    const { session } = useContext(AuthContext);
    const [videoSrc, setVideoSrc] = useState('');
    const [finalizing, setFinalizing] = useState(false);
    const [finalizeError, setFinalizeError] = useState('');
    const [sceneActionBusy, setSceneActionBusy] = useState('');
    const [sceneActionError, setSceneActionError] = useState('');
    const [openModelUrl, setOpenModelUrl] = useState('');
    const [inspectSceneIdx, setInspectSceneIdx] = useState<number | null>(null);

    const title = snapshot.title || (snapshot.kind === 'shortform' ? 'Your Short' : 'Production');
    const isAnalysis = snapshot.kind === 'competitor';
    const complete = snapshot.status === 'complete';
    const failed = snapshot.status === 'failed';
    const awaiting = snapshot.status === 'awaiting_approval';
    const running = snapshot.running !== false && !complete && !failed && !awaiting;

    const stills = snapshot.still_preview_urls || [];
    const sceneCards = useMemo<AgentSceneSnapshot[]>(() => {
        if (snapshot.scenes?.length) return snapshot.scenes;
        const count = Math.max(snapshot.total_scenes || 0, stills.length);
        return Array.from({ length: count }, (_, idx) => ({
            index: idx,
            duration_sec: 5,
            approved_for_video: false,
            approved_for_animation: false,
            animate: false,
            still_preview_url: stills[idx],
        }));
    }, [snapshot.scenes, snapshot.total_scenes, stills]);
    const modelUrls = useMemo(() => {
        const raw = [
            snapshot.model_url,
            ...(snapshot.model_urls || []),
            ...(snapshot.asset_urls || []),
        ].filter(Boolean) as string[];
        const tok = session?.access_token || '';
        return raw
            .filter(isModelUrl)
            .map((url) => (url.startsWith('http') ? url : tok ? mediaUrl(url, tok) : url));
    }, [session?.access_token, snapshot.asset_urls, snapshot.model_url, snapshot.model_urls]);
    const totalScenes = snapshot.total_scenes || stills.length || 0;
    const currentScene = snapshot.current_scene || 0;
    const pct = Math.max(0, Math.min(100, Number(snapshot.progress || (awaiting ? 80 : running ? 35 : complete ? 100 : 0))));

    const loadVideo = useCallback(async () => {
        const tok = session?.access_token;
        if (!tok || !snapshot.job_id || isAnalysis) return;
        const url = agentJobMediaUrl(snapshot.job_id, snapshot.kind);
        try {
            const res = await fetch(url, { headers: { Authorization: `Bearer ${tok}` } });
            if (!res.ok) return;
            const blob = await res.blob();
            setVideoSrc((prev) => {
                if (prev) URL.revokeObjectURL(prev);
                return URL.createObjectURL(blob);
            });
        } catch {
            /* ignore */
        }
    }, [isAnalysis, session?.access_token, snapshot.job_id, snapshot.kind]);

    useEffect(() => {
        if ((complete || awaiting) && snapshot.mp4_url) void loadVideo();
    }, [complete, awaiting, snapshot.mp4_url, loadVideo]);

    useEffect(() => () => {
        if (videoSrc) URL.revokeObjectURL(videoSrc);
    }, [videoSrc]);

    const runFinalize = async () => {
        const tok = session?.access_token;
        if (!tok || !snapshot.job_id) return;
        setFinalizing(true);
        setFinalizeError('');
        try {
            const res = await fetch(agentJobFinalizeUrl(snapshot.job_id), {
                method: 'POST',
                headers: { Authorization: `Bearer ${tok}` },
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(String((data as { detail?: string }).detail || res.statusText));
            onFinalizeStarted?.(snapshot.job_id, (data as { active_jobs?: unknown }).active_jobs);
        } catch (e) {
            setFinalizeError((e as Error).message);
        } finally {
            setFinalizing(false);
        }
    };

    const approveScene = async (sceneIndex: number, animate: boolean) => {
        const tok = session?.access_token;
        if (!tok || !snapshot.job_id) return;
        const busyKey = `${sceneIndex}:${animate ? 'animate' : 'still'}`;
        setSceneActionBusy(busyKey);
        setSceneActionError('');
        try {
            const res = await fetch(agentJobSceneApprovalUrl(snapshot.job_id, sceneIndex), {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${tok}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ animate }),
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(String((data as { detail?: string }).detail || res.statusText));
            const next = (data as { snapshot?: AgentJobSnapshot }).snapshot;
            if (next) onSnapshotUpdate?.(next);
        } catch (e) {
            setSceneActionError((e as Error).message);
        } finally {
            setSceneActionBusy('');
        }
    };

    // Failed state
    if (failed) {
        return (
            <div className="mt-2 rounded-2xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm">
                <div className="flex items-center gap-2 text-red-300">
                    <span className="text-lg">⚠️</span>
                    <span className="font-semibold">Production failed</span>
                </div>
                {snapshot.error && <p className="mt-1 text-xs text-red-200/90">{snapshot.error}</p>}
                <p className="mt-1 text-[10px] text-red-300/70">Tap Retry in the dock or ask the agent to try again.</p>
            </div>
        );
    }

    // === Main live production card (the "magic happening in the chat") ===
    const isShortform = snapshot.kind === 'shortform';
    const stageLabel = snapshot.stage_label || (running ? 'Working…' : awaiting ? 'Review stills' : complete ? 'Complete' : '');

    return (
        <div className="mt-3 overflow-hidden rounded-2xl border border-white/[0.08] bg-[#0b0b11]/95 shadow-inner">
            {/* Header */}
            <div className="flex items-center justify-between border-b border-white/[0.06] bg-white/[0.015] px-4 py-2.5">
                <div className="flex items-center gap-2.5">
                    {isShortform ? (
                        <Clapperboard className="h-4 w-4 text-violet-400" />
                    ) : (
                        <Film className="h-4 w-4 text-cyan-400" />
                    )}
                    <div>
                        <p className="text-sm font-semibold text-white">{title}</p>
                        <p className="text-[10px] text-gray-500 tabular-nums">
                            {snapshot.kind} · {snapshot.job_id?.slice(0, 8)}
                        </p>
                    </div>
                </div>

                <div className="flex items-center gap-2">
                    {running && (
                        <div className="flex items-center gap-1.5 rounded-full bg-cyan-500/10 px-2.5 py-0.5 text-[10px] font-medium text-cyan-300">
                            <Loader2 className="h-3 w-3 animate-spin" />
                            {stageLabel}
                        </div>
                    )}
                    {awaiting && (
                        <div className="rounded-full bg-amber-500/10 px-2.5 py-0.5 text-[10px] font-medium text-amber-300">
                            Awaiting your review
                        </div>
                    )}
                    {complete && (
                        <div className="flex items-center gap-1 rounded-full bg-emerald-500/10 px-2.5 py-0.5 text-[10px] font-medium text-emerald-300">
                            <CheckCircle2 className="h-3 w-3" /> Ready
                        </div>
                    )}
                    {complete && onReply && (
                        <button
                            type="button"
                            onClick={() => onReply(snapshot)}
                            className="ml-1 rounded-md border border-white/10 bg-white/5 p-1 text-white/70 transition hover:bg-violet-500/20 hover:text-violet-300"
                            title="Reply in this chat to re-edit: fix pacing/storytelling/packaging, ensure strong subscribe CTA"
                        >
                            <ArrowLeft className="h-3.5 w-3.5" />
                        </button>
                    )}
                    <div className="text-[10px] tabular-nums text-gray-400">{pct}%</div>
                </div>
            </div>

            {/* Progress bar */}
            {(running || awaiting) && (
                <div className="h-px w-full bg-white/[0.06]">
                    <div
                        className="h-px bg-gradient-to-r from-violet-500 via-cyan-400 to-violet-500 transition-all duration-500"
                        style={{ width: `${pct}%` }}
                    />
                </div>
            )}

            {/* Live content area */}
            <div className="p-3">
                {awaiting && isShortform && sceneCards.length > 0 && (
                    <div className="mb-3">
                        <div className="mb-2 flex items-center justify-between gap-2">
                            <div>
                                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-200">Scene review</p>
                                <p className="text-[11px] text-gray-500">Approve stills one by one. Animate only scenes you explicitly approve.</p>
                            </div>
                            <div className="rounded-full border border-cyan-400/20 bg-cyan-500/10 px-2.5 py-1 text-[10px] font-semibold text-cyan-100">
                                {sceneCards.filter((scene) => scene.approved_for_video || scene.approved_for_animation).length}/{sceneCards.length} approved
                            </div>
                        </div>
                        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                            {sceneCards.map((scene, rawIdx) => {
                                const idx = Number.isFinite(scene.index) ? scene.index : rawIdx;
                                const approvedForAnimation = Boolean(scene.approved_for_animation || scene.animate);
                                const approvedForVideo = Boolean(scene.approved_for_video || approvedForAnimation);
                                const statusLabel = approvedForAnimation ? 'Animate approved' : approvedForVideo ? 'Still approved' : 'Needs review';
                                const busyStill = sceneActionBusy === `${idx}:still`;
                                const busyAnimate = sceneActionBusy === `${idx}:animate`;
                                return (
                                    <div
                                        key={`${snapshot.job_id}-scene-${idx}`}
                                        className={`overflow-hidden rounded-2xl border bg-black/25 ${
                                            approvedForAnimation
                                                ? 'border-violet-400/30'
                                                : approvedForVideo
                                                ? 'border-emerald-400/25'
                                                : 'border-white/[0.08]'
                                        }`}
                                    >
                                        <button
                                            type="button"
                                            onClick={() => setInspectSceneIdx(idx)}
                                            title={`Inspect scene ${idx + 1}`}
                                            className="block w-full bg-black/30 transition hover:bg-cyan-500/5"
                                        >
                                            <StillThumb jobId={snapshot.job_id} idx={idx} />
                                        </button>
                                        <div className="space-y-2 p-3">
                                            <div className="flex items-start justify-between gap-2">
                                                <div>
                                                    <p className="text-sm font-semibold text-white">Scene {idx + 1}</p>
                                                    <p className="text-[10px] text-gray-500">
                                                        {scene.duration_sec ? `${Math.round(scene.duration_sec)}s` : 'Still'}{scene.has_clip ? ' - clip ready' : ''}
                                                    </p>
                                                </div>
                                                <span className={`shrink-0 rounded-full px-2 py-0.5 text-[9px] font-semibold ${
                                                    approvedForAnimation
                                                        ? 'bg-violet-500/15 text-violet-200'
                                                        : approvedForVideo
                                                        ? 'bg-emerald-500/15 text-emerald-200'
                                                        : 'bg-amber-500/15 text-amber-200'
                                                }`}>
                                                    {statusLabel}
                                                </span>
                                            </div>
                                            {(scene.narration || scene.scene_action) && (
                                                <p className="line-clamp-3 text-[11px] leading-relaxed text-gray-400">
                                                    {scene.narration || scene.scene_action}
                                                </p>
                                            )}
                                            <div className="grid grid-cols-2 gap-1.5">
                                                <button
                                                    type="button"
                                                    onClick={() => setInspectSceneIdx(idx)}
                                                    className="inline-flex items-center justify-center gap-1.5 rounded-lg border border-white/10 bg-white/[0.035] px-2 py-1.5 text-[10px] font-semibold text-gray-200 hover:bg-white/[0.07]"
                                                >
                                                    <Search className="h-3 w-3" /> Inspect
                                                </button>
                                                {onReply && (
                                                    <button
                                                        type="button"
                                                        onClick={() => onReply(snapshot, idx, 'edit')}
                                                        className="inline-flex items-center justify-center gap-1.5 rounded-lg border border-cyan-400/20 bg-cyan-500/10 px-2 py-1.5 text-[10px] font-semibold text-cyan-100 hover:bg-cyan-500/15"
                                                    >
                                                        <Wand2 className="h-3 w-3" /> Edit
                                                    </button>
                                                )}
                                                {onReply && (
                                                    <button
                                                        type="button"
                                                        onClick={() => onReply(snapshot, idx, 'regenerate')}
                                                        className="inline-flex items-center justify-center gap-1.5 rounded-lg border border-white/10 bg-white/[0.035] px-2 py-1.5 text-[10px] font-semibold text-gray-200 hover:bg-white/[0.07]"
                                                    >
                                                        <RefreshCw className="h-3 w-3" /> Regenerate
                                                    </button>
                                                )}
                                                <button
                                                    type="button"
                                                    disabled={Boolean(sceneActionBusy)}
                                                    onClick={() => void approveScene(idx, false)}
                                                    className="inline-flex items-center justify-center gap-1.5 rounded-lg border border-emerald-400/20 bg-emerald-500/10 px-2 py-1.5 text-[10px] font-semibold text-emerald-100 hover:bg-emerald-500/15 disabled:opacity-50"
                                                >
                                                    {busyStill ? <Loader2 className="h-3 w-3 animate-spin" /> : <Check className="h-3 w-3" />}
                                                    Approve
                                                </button>
                                                <button
                                                    type="button"
                                                    disabled={Boolean(sceneActionBusy)}
                                                    onClick={() => void approveScene(idx, true)}
                                                    className="col-span-2 inline-flex items-center justify-center gap-1.5 rounded-lg border border-violet-400/20 bg-violet-500/10 px-2 py-1.5 text-[10px] font-semibold text-violet-100 hover:bg-violet-500/15 disabled:opacity-50"
                                                >
                                                    {busyAnimate ? <Loader2 className="h-3 w-3 animate-spin" /> : <Play className="h-3 w-3" />}
                                                    Approve + animate this scene
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                        {sceneActionError && <p className="mt-2 text-[11px] text-red-300">{sceneActionError}</p>}
                    </div>
                )}
                {/* Scene / stills strip — the real-time "watch it being made" part */}
                {!(awaiting && isShortform && sceneCards.length > 0) && isShortform && (stills.length > 0 || totalScenes > 0 || running) && (
                    <div className="mb-3">
                        <div className="mb-1.5 flex items-center justify-between text-[10px] uppercase tracking-wider text-gray-500">
                            <span>Scenes</span>
                            {totalScenes > 0 && (
                                <span className="text-gray-400">
                                    {currentScene || stills.length}/{totalScenes}
                                </span>
                            )}
                        </div>
                        <div className="flex gap-2 overflow-x-auto pb-1 scrollbar-thin">
                            {Array.from({ length: Math.max(totalScenes || 8, stills.length || 4) }).map((_, idx) => {
                                const hasStill = idx < stills.length;
                                const isCurrent = idx === (currentScene || 0) - 1 && running;
                                return (
                                    <div
                                        key={idx}
                                        className={`relative w-20 shrink-0 overflow-hidden rounded-xl border text-center text-[9px] transition-all ${
                                            hasStill
                                                ? 'border-white/10 hover:border-cyan-400/40 hover:bg-cyan-500/5'
                                                : isCurrent
                                                ? 'border-cyan-400/40 bg-cyan-950/40 ring-1 ring-cyan-400/20'
                                                : 'border-white/[0.06] bg-white/[0.015]'
                                        }`}
                                    >
                                        <button
                                            type="button"
                                            onClick={() => hasStill && setInspectSceneIdx(idx)}
                                            disabled={!hasStill}
                                            title={hasStill ? `Inspect scene ${idx + 1}` : `Scene ${idx + 1}`}
                                            className="block w-full disabled:cursor-default"
                                        >
                                        {hasStill ? (
                                            <StillThumb jobId={snapshot.job_id} idx={idx} />
                                        ) : (
                                            <div className="aspect-video flex flex-col items-center justify-center bg-white/[0.015] text-[9px] text-gray-500">
                                                {isCurrent ? (
                                                    <>
                                                        <Loader2 className="h-3 w-3 animate-spin mb-0.5" />
                                                        <span>live</span>
                                                    </>
                                                ) : (
                                                    '—'
                                                )}
                                            </div>
                                        )}
                                        <div className="border-t border-white/5 py-[1px] text-[7.5px] text-gray-500 tracking-tight">
                                            Scene {idx + 1}
                                        </div>
                                        </button>
                                        {hasStill && onReply && (
                                            <button
                                                type="button"
                                                onClick={() => onReply(snapshot, idx)}
                                                className="w-full border-t border-white/5 bg-white/[0.025] py-1 text-[8px] font-semibold uppercase tracking-wide text-cyan-200/80 transition hover:bg-cyan-500/10 hover:text-cyan-100"
                                                title={`Reply to edit scene ${idx + 1}`}
                                            >
                                                Edit
                                            </button>
                                        )}
                                    </div>
                                );
                            })}
                        </div>
                        <p className="mt-1 text-[10px] text-gray-500">
                            {running ? 'Agent is generating stills + motion in real time...' : awaiting ? 'Open a scene to inspect it. Use Edit only when you want Studio Agent to revise that exact still before animation.' : ''}
                        </p>
                    </div>
                )}

                {/* Stage detail */}
                {snapshot.stage_detail && (
                    <p className="mb-2 text-xs text-gray-400">{snapshot.stage_detail}</p>
                )}

                {modelUrls.length > 0 && (
                    <div className="mb-3 rounded-xl border border-cyan-400/20 bg-cyan-500/5 p-3">
                        <div className="mb-2 flex items-center gap-2 text-xs font-semibold text-cyan-100">
                            <Box className="h-4 w-4" />
                            Character model
                        </div>
                        <div className="flex flex-wrap gap-2">
                            {modelUrls.map((url, idx) => (
                                <button
                                    key={`${url}-${idx}`}
                                    type="button"
                                    onClick={() => setOpenModelUrl(url)}
                                    className="inline-flex items-center gap-2 rounded-lg border border-cyan-400/20 bg-black/30 px-3 py-2 text-xs font-semibold text-cyan-100 transition hover:border-cyan-300/50 hover:bg-cyan-500/10"
                                >
                                    <Box className="h-3.5 w-3.5" />
                                    Open 3D preview {modelUrls.length > 1 ? idx + 1 : ''}
                                </button>
                            ))}
                        </div>
                    </div>
                )}

                {/* Awaiting approval actions (beautiful finalize gate) */}
                {awaiting && isShortform && (
                    <div className="mt-2 rounded-xl border border-cyan-500/20 bg-cyan-500/5 p-3 text-xs text-cyan-100/90">
                        <p className="font-semibold text-cyan-100">Scene review required</p>
                        <p className="mt-1 text-cyan-100/70">
                            No image-to-video should run until these stills are approved. Reply with the scene number and edit request, or tell Studio Agent which scenes to approve for animation.
                        </p>
                    </div>
                )}
                {awaiting && !isShortform && (
                    <div className="mt-2 rounded-xl border border-amber-500/20 bg-amber-500/5 p-3">
                        <button
                            type="button"
                            disabled={finalizing}
                            onClick={() => void runFinalize()}
                            className="flex w-full items-center justify-center gap-2 rounded-xl bg-amber-600 py-2.5 text-sm font-semibold text-white transition active:scale-[0.985] disabled:opacity-60"
                        >
                            {finalizing ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                            Finalize &amp; export final MP4
                        </button>
                        {finalizeError && <p className="mt-1.5 text-center text-[10px] text-red-300">{finalizeError}</p>}
                    </div>
                )}

                {/* Final video (the payoff, right in the chat) */}
                {complete && !isAnalysis && (
                    <div className="mt-1">
                        {videoSrc ? (
                            <video
                                src={videoSrc}
                                controls
                                className="w-full rounded-xl border border-white/10 bg-black"
                                playsInline
                            />
                        ) : (
                            <div className="flex h-40 items-center justify-center rounded-xl border border-white/10 bg-black/40 text-sm text-gray-400">
                                Loading final video preview…
                            </div>
                        )}
                    </div>
                )}

                {/* Competitor / reference analysis (keep compact & useful) */}
                {isAnalysis && complete && (
                    <div className="text-[11px] text-gray-300">
                        {snapshot.pacing && (
                            <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
                                {snapshot.pacing.avg_shot_sec != null && <div>Avg shot: <span className="text-white">{snapshot.pacing.avg_shot_sec}s</span></div>}
                                {snapshot.pacing.cut_count != null && <div>Cuts: <span className="text-white">{snapshot.pacing.cut_count}</span></div>}
                            </div>
                        )}
                        {snapshot.blueprint_hint && (
                            <p className="mt-2 border-t border-white/10 pt-2 text-[10px] text-gray-400">{snapshot.blueprint_hint}</p>
                        )}
                    </div>
                )}
            </div>

            {/* Bottom actions */}
            {complete && !isAnalysis && (
                <div className="border-t border-white/[0.06] bg-black/20 px-3 py-2">
                    {(() => {
                        const tok = session?.access_token || '';
                        const downloadHref =
                            videoSrc ||
                            (snapshot.download_url && tok ? mediaUrl(snapshot.download_url, tok) : '') ||
                            (snapshot.mp4_url && tok ? mediaUrl(snapshot.mp4_url, tok) : '');
                        const packageHref =
                            snapshot.package_url && tok
                                ? mediaUrl(snapshot.package_url, tok)
                                : snapshot.job_id && tok
                                ? mediaUrl(agentJobPackageUrl(snapshot.job_id, snapshot.kind), tok)
                                : '';
                        return downloadHref ? (
                            <>
                                <a
                                    href={downloadHref}
                                    download={`${snapshot.job_id}.mp4`}
                                    className="flex items-center justify-center gap-2 rounded-xl bg-emerald-600 py-2 text-sm font-semibold text-white transition hover:bg-emerald-500"
                                >
                                    <Download className="h-4 w-4" /> Download MP4
                                </a>
                                {packageHref && (
                                    <a
                                        href={packageHref}
                                        download={`${snapshot.job_id}_upload_package.txt`}
                                        className="mt-2 flex items-center justify-center gap-2 rounded-xl border border-cyan-400/20 bg-cyan-500/10 py-2 text-sm font-semibold text-cyan-100 transition hover:bg-cyan-500/15"
                                    >
                                        <FileText className="h-4 w-4" /> Upload package
                                    </a>
                                )}
                                {onReply && (
                                    <button
                                        type="button"
                                        onClick={() => onReply(snapshot)}
                                        className="flex items-center justify-center gap-2 rounded-xl border border-white/10 bg-white/5 py-2 text-sm font-semibold text-white/90 transition hover:bg-white/10"
                                        title="Reply to this video in chat to request re-edits (fix pacing, storytelling, packaging, add CTA, etc.)"
                                    >
                                        <ArrowLeft className="h-4 w-4" /> Reply &amp; re-edit
                                    </button>
                                )}
                            </>
                        ) : null;
                    })()}
                </div>
            )}
            {openModelUrl && (
                <CharacterModelModal
                    title={`${title} - 3D character preview`}
                    modelUrl={openModelUrl}
                    onClose={() => setOpenModelUrl('')}
                />
            )}
            {inspectSceneIdx != null && (
                <StillInspectionModal
                    jobId={snapshot.job_id}
                    idx={inspectSceneIdx}
                    title={`${title} - Scene ${inspectSceneIdx + 1}`}
                    onClose={() => setInspectSceneIdx(null)}
                    onEdit={onReply ? () => {
                        onReply(snapshot, inspectSceneIdx, 'edit');
                        setInspectSceneIdx(null);
                    } : undefined}
                    onRegenerate={onReply ? () => {
                        onReply(snapshot, inspectSceneIdx, 'regenerate');
                        setInspectSceneIdx(null);
                    } : undefined}
                    onApproveStill={() => void approveScene(inspectSceneIdx, false)}
                    onApproveAnimate={() => void approveScene(inspectSceneIdx, true)}
                />
            )}
        </div>
    );
}
