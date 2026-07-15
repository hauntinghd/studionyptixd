import { ArrowLeft, Box, Check, CheckCircle2, Clapperboard, Download, FileText, Film, Loader2, Play, RefreshCw, Search, Square, Wand2, X } from 'lucide-react';
import { createElement, memo, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { AuthContext } from '../../shared';
import { useAuthenticatedMediaUrls } from '../../hooks/useAuthenticatedMedia';
import type { AgentJobSnapshot, AgentSceneSnapshot } from '../../lib/agentProduction';
import {
    agentJobAnimateUrl,
    agentJobExpandProofUrl,
    agentJobFinalizeUrl,
    agentJobMediaUrl,
    agentJobPackageUrl,
    agentJobSceneApprovalUrl,
    agentJobScenePromptUrl,
    agentJobSceneRegenerateUrl,
    agentJobScenesApprovalUrl,
    agentJobClipUrl,
    agentJobStillUrl,
    downloadStudioAsset,
    fetchJobSnapshot,
    normalizeAgentJobKind,
} from '../../lib/agentProduction';

export type SceneReplyPreset = 'edit' | 'regenerate';

function SceneClipPlayer({
    jobId,
    idx,
    cacheKey = '',
}: {
    jobId: string;
    idx: number;
    cacheKey?: string;
}) {
    const { session } = useContext(AuthContext);
    const [src, setSrc] = useState('');

    useEffect(() => {
        const tok = session?.access_token;
        if (!tok) return;
        let cancelled = false;
        let objectUrl = '';
        (async () => {
            try {
                const base = agentJobClipUrl(jobId, idx);
                const url = cacheKey
                    ? `${base}${base.includes('?') ? '&' : '?'}v=${encodeURIComponent(cacheKey)}`
                    : base;
                const res = await fetch(url, {
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
    }, [jobId, idx, session?.access_token, cacheKey]);

    if (!src) {
        return (
            <div className="aspect-video flex items-center justify-center bg-black/40">
                <Loader2 className="h-4 w-4 animate-spin text-gray-500" />
            </div>
        );
    }

    return (
        <video
            key={src}
            src={src}
            controls
            playsInline
            className="aspect-[9/16] max-h-[min(420px,50vh)] w-full bg-black object-contain"
        />
    );
}

function StillThumb({
    jobId,
    idx,
    cacheKey = '',
}: {
    jobId: string;
    idx: number;
    cacheKey?: string;
}) {
    const { session } = useContext(AuthContext);
    const [src, setSrc] = useState('');

    useEffect(() => {
        const tok = session?.access_token;
        if (!tok) return;
        let cancelled = false;
        let objectUrl = '';
        (async () => {
            try {
                // Snapshot preview URLs are backend-relative.  Using them
                // directly makes Vercel request its own /api route, leaving a
                // black card even though Inspect (which uses agentApi) works.
                // Always use the authenticated backend URL and keep only the
                // snapshot value as a cache-busting generation key.
                const base = agentJobStillUrl(jobId, idx);
                const url = `${base}${base.includes('?') ? '&' : '?'}v=${encodeURIComponent(cacheKey || Date.now())}`;
                const res = await fetch(url, {
                    headers: { Authorization: `Bearer ${tok}` },
                    cache: 'no-store',
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
    }, [cacheKey, jobId, idx, session?.access_token]);

    if (!src) {
        return (
            <div className="aspect-[9/16] max-h-[360px] animate-pulse rounded-lg bg-white/[0.035] border border-white/[0.06] flex items-center justify-center">
                <Loader2 className="h-3 w-3 text-gray-600 animate-spin" />
            </div>
        );
    }
    return (
        <img
            src={src}
            alt={`Scene ${idx + 1}`}
            className="aspect-[9/16] max-h-[360px] w-full rounded-lg border border-white/[0.1] bg-black object-contain shadow-sm"
            loading="lazy"
        />
    );
}

function isModelUrl(url?: string) {
    return /\.(glb|gltf)(\?|#|$)/i.test(String(url || ''));
}

type ClipLabClip = {
    index?: number;
    filename?: string;
    url?: string;
    start?: number;
    end?: number;
    duration_sec?: number;
    virality_score?: number;
    score_breakdown?: Record<string, number>;
    hook_text?: string;
    why_it_matches?: string;
    visual_notes?: string;
    audio_notes?: string;
    narrative_role?: string;
    retention_reason?: string;
    edit_plan?: string[];
};

type ClipLabUploadPackage = {
    clip_index?: number;
    title?: string;
    description?: string;
    tags?: string[];
    hook?: string;
    rationale?: string;
    visual_notes?: string;
    audio_notes?: string;
    narrative_role?: string;
    retention_reason?: string;
    edit_plan?: string[];
    score_breakdown?: Record<string, number>;
    start?: number;
    end?: number;
    virality_score?: number;
};

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
    hasClip = false,
    clipCacheKey = '',
    onClose,
    onEdit,
    onRegenerate,
    onApproveStill,
    onApproveAnimate,
}: {
    jobId: string;
    idx: number;
    title: string;
    hasClip?: boolean;
    clipCacheKey?: string;
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
                        <p className="text-[11px] text-gray-500">
                            {hasClip
                                ? 'Animation clip ready — review motion, then close and export when satisfied.'
                                : 'Zoom and pan to inspect artifacts before animation.'}
                        </p>
                    </div>
                    <div className="flex items-center gap-2">
                        {!hasClip && (
                            <>
                                <button type="button" onClick={() => adjustZoom(zoom - 0.25)} className="rounded-lg border border-white/10 bg-white/[0.04] px-3 py-2 text-xs font-semibold text-gray-200 hover:bg-white/[0.08]">-</button>
                                <div className="min-w-14 rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-center text-xs tabular-nums text-gray-300">{Math.round(zoom * 100)}%</div>
                                <button type="button" onClick={() => adjustZoom(zoom + 0.25)} className="rounded-lg border border-white/10 bg-white/[0.04] px-3 py-2 text-xs font-semibold text-gray-200 hover:bg-white/[0.08]">+</button>
                                <button type="button" onClick={reset} className="rounded-lg border border-white/10 bg-white/[0.04] px-3 py-2 text-xs font-semibold text-gray-200 hover:bg-white/[0.08]">Reset</button>
                            </>
                        )}
                        {onEdit && (
                            <button type="button" onClick={onEdit} className="rounded-lg border border-cyan-400/20 bg-cyan-500/10 px-3 py-2 text-xs font-semibold text-cyan-100 hover:bg-cyan-500/15">
                                Edit this scene
                            </button>
                        )}
                        {onRegenerate && (
                            <button type="button" onClick={onRegenerate} className="rounded-lg border border-white/10 bg-white/[0.04] px-3 py-2 text-xs font-semibold text-gray-200 hover:bg-white/[0.08]">
                                Regenerate scene + animation
                            </button>
                        )}
                        {!hasClip && onApproveStill && (
                            <button type="button" onClick={onApproveStill} className="rounded-lg border border-emerald-400/20 bg-emerald-500/10 px-3 py-2 text-xs font-semibold text-emerald-100 hover:bg-emerald-500/15">
                                Approve still
                            </button>
                        )}
                        {!hasClip && onApproveAnimate && (
                            <button type="button" onClick={onApproveAnimate} className="rounded-lg border border-violet-400/20 bg-violet-500/10 px-3 py-2 text-xs font-semibold text-violet-100 hover:bg-violet-500/15">
                                Approve + animate
                            </button>
                        )}
                        <button type="button" onClick={onClose} className="rounded-lg border border-white/10 bg-white/[0.04] p-2 text-gray-300 hover:bg-white/[0.08] hover:text-white" title="Close still inspector">
                            <X className="h-4 w-4" />
                        </button>
                    </div>
                </div>
                {hasClip ? (
                    <div className="relative flex min-h-0 flex-1 items-center justify-center bg-black p-4">
                        <div className="w-full max-w-md overflow-hidden rounded-xl border border-white/10">
                            <SceneClipPlayer jobId={jobId} idx={idx} cacheKey={clipCacheKey} />
                        </div>
                    </div>
                ) : (
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
                )}
            </div>
        </div>
    );
}

function AgentJobDeliverable({
    snapshot,
    sessionId,
    onFinalizeStarted,
    onReply,
    onSnapshotUpdate,
    onCancel,
    onRetry,
    retrying = false,
    cancelling = false,
    enableVideoPreview = true,
    captionsEnabled = true,
}: {
    snapshot: AgentJobSnapshot;
    sessionId?: string | null;
    onFinalizeStarted?: (jobId: string, activeJobs?: unknown) => void;
    onReply?: (snapshot: AgentJobSnapshot, sceneIndex?: number, preset?: SceneReplyPreset) => void;
    onSnapshotUpdate?: (snapshot: AgentJobSnapshot) => void;
    onCancel?: () => void;
    onRetry?: () => void;
    retrying?: boolean;
    cancelling?: boolean;
    /** Gate final MP4 + animated clip blobs only; scene stills always load. */
    enableVideoPreview?: boolean;
    captionsEnabled?: boolean;
}) {
    const { session } = useContext(AuthContext);
    const [videoSrc, setVideoSrc] = useState('');
    const [videoLoadFailed, setVideoLoadFailed] = useState(false);
    const [finalizing, setFinalizing] = useState(false);
    const [finalizeError, setFinalizeError] = useState('');
    const [expandingProof, setExpandingProof] = useState(false);
    const [expandProofError, setExpandProofError] = useState('');
    const [animating, setAnimating] = useState(false);
    const [animateError, setAnimateError] = useState('');
    const [sceneActionBusy, setSceneActionBusy] = useState('');
    const [sceneActionError, setSceneActionError] = useState('');
    const [thumbnailDownloadBusy, setThumbnailDownloadBusy] = useState<number | null>(null);
    const [assetDownloadBusy, setAssetDownloadBusy] = useState('');
    const [assetDownloadError, setAssetDownloadError] = useState('');
    const [openModelUrl, setOpenModelUrl] = useState('');
    const [inspectSceneIdx, setInspectSceneIdx] = useState<number | null>(null);
    const [clockMs, setClockMs] = useState(() => Date.now());

    const title = snapshot.title || (snapshot.kind === 'shortform' ? 'Your Short' : 'Production');
    const isAnalysis = snapshot.kind === 'competitor';
    const isClipLab = snapshot.kind === 'cliplab';
    const cliplabJobType = String(snapshot.job_type || '').toLowerCase();
    const isClipLabIngest = isClipLab && (cliplabJobType === 'cliplab_ingest' || snapshot.job_id.startsWith('clipi_'));
    const isClipLabAnalyze = isClipLab && (cliplabJobType === 'cliplab_analyze' || snapshot.job_id.startsWith('clipa_'));
    const isClipLabRender = isClipLab && (cliplabJobType === 'cliplab_render' || snapshot.job_id.startsWith('clipr_'));
    const clipLabStepLabel = isClipLabIngest
        ? 'Ingest ready'
        : isClipLabAnalyze
            ? 'Analysis ready'
            : isClipLabRender
                ? 'Clips ready'
                : 'ClipLab ready';
    const clipLabStepDetail = isClipLabIngest
        ? `Source video is ingested${snapshot.cue_count != null ? ` with ${snapshot.cue_count} transcript cues` : ''}. Send continue to analyze and select clip moments.`
        : isClipLabAnalyze
            ? `${snapshot.segment_count || snapshot.segments?.length || 0} candidate segment(s) found. Approve/render the strongest picks to create 9:16 clips.`
            : isClipLabRender
                ? `${snapshot.clip_count || snapshot.clips?.length || 0} rendered clip(s) and ${snapshot.upload_package_count || snapshot.upload_packages?.length || 0} upload package(s) are ready.`
                : snapshot.next_action || 'Continue to the next ClipLab step.';
    const complete = snapshot.status === 'complete';
    const failed = snapshot.status === 'failed' || Boolean(
        snapshot.error
        && snapshot.status !== 'complete'
        && snapshot.status !== 'awaiting_approval',
    );
    const awaiting = snapshot.status === 'awaiting_approval';
    const running = snapshot.running !== false && !complete && !failed && !awaiting;
    const updatedAgoSec = snapshot.client_updated_at
        ? Math.max(0, Math.floor((clockMs - snapshot.client_updated_at) / 1000))
        : null;

    useEffect(() => {
        if (!running) return;
        const id = window.setInterval(() => setClockMs(Date.now()), 1000);
        return () => clearInterval(id);
    }, [running]);

    useEffect(() => {
        if (!running || !snapshot.job_id || !onSnapshotUpdate || !sessionId) return;
        const updatedAt = Number(snapshot.client_updated_at || 0);
        const stale = !updatedAt || (Date.now() - updatedAt > 3500);
        if (!stale) return;
        let cancelled = false;
        void (async () => {
            const tok = session?.access_token;
            if (!tok) return;
            const fresh = await fetchJobSnapshot(
                {
                    job_id: snapshot.job_id,
                    kind: normalizeAgentJobKind(snapshot.job_id, snapshot.kind, snapshot.title),
                    title: snapshot.title,
                },
                sessionId,
                tok,
            );
            if (cancelled || !fresh) return;
            onSnapshotUpdate({ ...fresh, client_updated_at: Date.now() });
        })();
        return () => {
            cancelled = true;
        };
    }, [
        running,
        session?.access_token,
        sessionId,
        snapshot.client_updated_at,
        snapshot.job_id,
        snapshot.kind,
        snapshot.title,
        onSnapshotUpdate,
    ]);

    const stills = snapshot.still_preview_urls || [];
    const thumbnailUrls = useMemo(
        () => (snapshot.thumbnail_urls || []).filter(Boolean),
        [snapshot.thumbnail_urls],
    );
    const thumbnailMedia = useAuthenticatedMediaUrls(
        thumbnailUrls,
        session?.access_token || '',
        thumbnailUrls.length > 0,
    );
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
    const approvedSceneCount = sceneCards.filter(
        (scene) => scene.approved_for_video || scene.approved_for_animation,
    ).length;
    const allScenesApproved = sceneCards.length > 0 && approvedSceneCount === sceneCards.length;
    const modelPaths = useMemo(() => (
        [
            snapshot.model_url,
            ...(snapshot.model_urls || []),
            ...(snapshot.asset_urls || []),
        ].filter(Boolean).filter((url) => isModelUrl(String(url))) as string[]
    ), [snapshot.asset_urls, snapshot.model_url, snapshot.model_urls]);
    const modelMedia = useAuthenticatedMediaUrls(
        modelPaths,
        session?.access_token || '',
        modelPaths.length > 0,
    );
    const modelUrls = modelMedia.urls.filter(Boolean);
    const totalScenes = snapshot.total_scenes || stills.length || 0;
    const currentScene = snapshot.current_scene || 0;
    const pct = Math.max(0, Math.min(100, Number(
        snapshot.progress ?? (awaiting ? 80 : complete ? 100 : 0),
    )));
    const clipLabClips = useMemo(
        () => (Array.isArray(snapshot.clips) ? snapshot.clips : []).filter(Boolean) as ClipLabClip[],
        [snapshot.clips],
    );
    const clipLabMedia = useAuthenticatedMediaUrls(
        clipLabClips.map((clip) => clip.url || ''),
        session?.access_token || '',
        complete && isClipLabRender && clipLabClips.length > 0,
    );
    const clipLabPackages = useMemo(
        () => (Array.isArray(snapshot.upload_packages) ? snapshot.upload_packages : []).filter(Boolean) as ClipLabUploadPackage[],
        [snapshot.upload_packages],
    );

    const loadVideo = useCallback(async () => {
        const tok = session?.access_token;
        if (!tok || !snapshot.job_id || isAnalysis || isClipLab) return;
        const url = agentJobMediaUrl(snapshot.job_id, snapshot.kind);
        try {
            const res = await fetch(url, { headers: { Authorization: `Bearer ${tok}` } });
            if (!res.ok) {
                setVideoLoadFailed(true);
                return;
            }
            const blob = await res.blob();
            setVideoLoadFailed(false);
            setVideoSrc((prev) => {
                if (prev) URL.revokeObjectURL(prev);
                return URL.createObjectURL(blob);
            });
        } catch {
            setVideoLoadFailed(true);
        }
    }, [isAnalysis, isClipLab, session?.access_token, snapshot.job_id, snapshot.kind]);

    useEffect(() => {
        if (!enableVideoPreview) {
            setVideoSrc((prev) => {
                if (prev) URL.revokeObjectURL(prev);
                return '';
            });
            return;
        }
        if (complete && snapshot.mp4_url) void loadVideo();
        else if (awaiting && snapshot.mp4_url) void loadVideo();
    }, [complete, awaiting, enableVideoPreview, snapshot.mp4_url, loadVideo]);

    useEffect(() => () => {
        if (videoSrc) URL.revokeObjectURL(videoSrc);
    }, [videoSrc]);

    const runExpandProof = async () => {
        const tok = session?.access_token;
        if (!tok || !snapshot.job_id) return;
        const idempotencyKey = crypto.randomUUID();
        setExpandingProof(true);
        setExpandProofError('');
        try {
            const res = await fetch(agentJobExpandProofUrl(snapshot.job_id), {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${tok}`,
                    'X-Idempotency-Key': idempotencyKey,
                },
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) {
                const detail = String((data as { detail?: string }).detail || res.statusText);
                throw new Error(detail.replace(/^expand_proof_failed:\s*/i, ''));
            }
            const next = (data as { snapshot?: AgentJobSnapshot }).snapshot;
            if (next) onSnapshotUpdate?.({ ...next, client_updated_at: Date.now() });
        } catch (e) {
            setExpandProofError((e as Error).message);
        } finally {
            setExpandingProof(false);
        }
    };

    const runFinalize = async () => {
        const tok = session?.access_token;
        if (!tok || !snapshot.job_id) return;
        const idempotencyKey = crypto.randomUUID();
        setFinalizing(true);
        setFinalizeError('');
        try {
            const res = await fetch(
                agentJobFinalizeUrl(snapshot.job_id, snapshot.kind, {
                    captions_enabled: captionsEnabled,
                    caption_mode: captionsEnabled ? 'word' : 'off',
                }),
                {
                    method: 'POST',
                    headers: {
                        Authorization: `Bearer ${tok}`,
                        'X-Idempotency-Key': idempotencyKey,
                    },
                },
            );
            const data = await res.json().catch(() => ({}));
            if (!res.ok) {
                const detail = (data as { detail?: unknown }).detail;
                if (detail && typeof detail === 'object' && Array.isArray((detail as { pending_animated_scenes?: unknown }).pending_animated_scenes)) {
                    const pending = ((detail as { pending_animated_scenes: unknown[] }).pending_animated_scenes)
                        .map((idx) => Number(idx) + 1)
                        .join(', ');
                    throw new Error(`Animate scene(s) ${pending} before exporting.`);
                }
                throw new Error(String(detail || res.statusText));
            }
            const next = (data as { snapshot?: AgentJobSnapshot }).snapshot;
            if (next) onSnapshotUpdate?.({ ...next, client_updated_at: Date.now() });
            onFinalizeStarted?.(snapshot.job_id, (data as { active_jobs?: unknown }).active_jobs);
        } catch (e) {
            setFinalizeError((e as Error).message);
        } finally {
            setFinalizing(false);
        }
    };

    const runAnimateApproved = async () => {
        const tok = session?.access_token;
        if (!tok || !snapshot.job_id) return;
        const idempotencyKey = crypto.randomUUID();
        setAnimating(true);
        setAnimateError('');
        try {
            const res = await fetch(agentJobAnimateUrl(snapshot.job_id), {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${tok}`,
                    'X-Idempotency-Key': idempotencyKey,
                },
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(String((data as { detail?: string }).detail || res.statusText));
            onFinalizeStarted?.(snapshot.job_id, (data as { active_jobs?: unknown }).active_jobs);
            const next = (data as { snapshot?: AgentJobSnapshot }).snapshot;
            if (next) onSnapshotUpdate?.({ ...next, client_updated_at: Date.now() });
        } catch (e) {
            setAnimateError((e as Error).message);
        } finally {
            setAnimating(false);
        }
    };

    const approveScene = async (sceneIndex: number, animate: boolean) => {
        const tok = session?.access_token;
        if (!tok || !snapshot.job_id) return;
        const idempotencyKey = crypto.randomUUID();
        const busyKey = `${sceneIndex}:${animate ? 'animate' : 'still'}`;
        setSceneActionBusy(busyKey);
        setSceneActionError('');
        try {
            const res = await fetch(agentJobSceneApprovalUrl(snapshot.job_id, sceneIndex), {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${tok}`,
                    'Content-Type': 'application/json',
                    'X-Idempotency-Key': idempotencyKey,
                },
                body: JSON.stringify({ animate }),
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(String((data as { detail?: string }).detail || res.statusText));
            if (animate) {
                onFinalizeStarted?.(snapshot.job_id, (data as { active_jobs?: unknown }).active_jobs);
            }
            const next = (data as { snapshot?: AgentJobSnapshot }).snapshot;
            if (next) onSnapshotUpdate?.({ ...next, client_updated_at: Date.now() });
        } catch (e) {
            setSceneActionError((e as Error).message);
        } finally {
            setSceneActionBusy('');
        }
    };

    const regenerateScene = async (sceneIndex: number) => {
        const tok = session?.access_token;
        if (!tok || !snapshot.job_id) return;
        const idempotencyKey = crypto.randomUUID();
        const busyKey = `${sceneIndex}:regenerate`;
        setSceneActionBusy(busyKey);
        setSceneActionError('');
        try {
            const res = await fetch(agentJobSceneRegenerateUrl(snapshot.job_id, sceneIndex), {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${tok}`,
                    'X-Idempotency-Key': idempotencyKey,
                },
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(String((data as { detail?: string }).detail || res.statusText));
            const next = (data as { snapshot?: AgentJobSnapshot }).snapshot;
            if (next) onSnapshotUpdate?.({ ...next, client_updated_at: Date.now() });
        } catch (e) {
            setSceneActionError((e as Error).message);
        } finally {
            setSceneActionBusy('');
        }
    };

    const editExactPrompt = async (scene: AgentSceneSnapshot, sceneIndex: number) => {
        const tok = session?.access_token;
        if (!tok || !snapshot.job_id) return;
        const current = String(scene.prompt || scene.scene_action || '').trim();
        const next = window.prompt(`Exact provider prompt for Scene ${sceneIndex + 1} (max 759 characters)`, current);
        if (next === null || next.trim() === current) return;
        setSceneActionBusy(`${sceneIndex}:prompt`);
        setSceneActionError('');
        try {
            const res = await fetch(agentJobScenePromptUrl(snapshot.job_id, sceneIndex), {
                method: 'PUT',
                headers: { Authorization: `Bearer ${tok}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({ prompt: next.trim() }),
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(String(data.detail || `Prompt update failed (${res.status})`));
            if (data.snapshot) onSnapshotUpdate?.(data.snapshot as AgentJobSnapshot);
        } catch (err) {
            setSceneActionError(err instanceof Error ? err.message : 'Could not save scene prompt');
        } finally {
            setSceneActionBusy('');
        }
    };

    const approveAllScenes = async (animate: boolean) => {
        const tok = session?.access_token;
        if (!tok || !snapshot.job_id) return;
        const idempotencyKey = crypto.randomUUID();
        setSceneActionBusy(`all:${animate ? 'animate' : 'still'}`);
        setSceneActionError('');
        try {
            const res = await fetch(agentJobScenesApprovalUrl(snapshot.job_id), {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${tok}`,
                    'Content-Type': 'application/json',
                    'X-Idempotency-Key': idempotencyKey,
                },
                body: JSON.stringify({ animate }),
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(String((data as { detail?: string }).detail || res.statusText));
            if (animate) {
                onFinalizeStarted?.(snapshot.job_id, (data as { active_jobs?: unknown }).active_jobs);
            }
            const next = (data as { snapshot?: AgentJobSnapshot }).snapshot;
            if (next) onSnapshotUpdate?.({ ...next, client_updated_at: Date.now() });
        } catch (e) {
            setSceneActionError((e as Error).message);
        } finally {
            setSceneActionBusy('');
        }
    };

    // Failed state
    if (failed) {
        const failLabel = isAnalysis
            ? 'Reference analysis failed'
            : isClipLab
              ? 'ClipLab failed'
              : 'Production failed';
        return (
            <div className="mt-2 rounded-2xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm">
                <div className="flex items-center gap-2 text-red-300">
                    <span className="text-lg">⚠️</span>
                    <span className="font-semibold">{failLabel}</span>
                </div>
                {snapshot.error && <p className="mt-1 text-xs text-red-200/90">{snapshot.error}</p>}
                {onRetry ? (
                    <button
                        type="button"
                        disabled={retrying}
                        onClick={onRetry}
                        className="mt-2 inline-flex items-center gap-1.5 rounded-lg border border-red-400/35 bg-red-500/15 px-3 py-1.5 text-[11px] font-semibold text-red-100 hover:bg-red-500/25 disabled:opacity-50"
                    >
                        <RefreshCw className={`h-3 w-3 ${retrying ? 'animate-spin' : ''}`} />
                        {retrying ? 'Retrying…' : 'Retry production'}
                    </button>
                ) : (
                    <p className="mt-1 text-[10px] text-red-300/70">Tap Retry in the dock or ask the agent to try again.</p>
                )}
            </div>
        );
    }

    // === Main live production card (the "magic happening in the chat") ===
    const isShortform = snapshot.kind === 'shortform';
    const isLongform = snapshot.kind === 'longform';
    const isVisualProof = Boolean(snapshot.visual_proof_only) && awaiting;
    const isThumbnailOnly = Boolean(snapshot.thumbnail_only);
    const stageLabel = snapshot.stage_label || (running ? 'Working…' : awaiting ? 'Review stills' : complete ? 'Complete' : '');
    // Keep the scene grid visible while i2v runs so the clip lands in-chat.
    const showSceneReviewGrid = (
        !isThumbnailOnly
        && sceneCards.length > 0
        && (isShortform || isLongform)
        && (awaiting || (isShortform && running))
    );

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
                        <div className={`hidden items-center gap-1 rounded-full px-2 py-0.5 text-[9px] tabular-nums sm:flex ${updatedAgoSec != null && updatedAgoSec <= 4 ? 'bg-emerald-500/10 text-emerald-300' : 'bg-amber-500/10 text-amber-200'}`}>
                            <span className="h-1.5 w-1.5 rounded-full bg-current" />
                            {updatedAgoSec == null ? 'Connecting' : `${updatedAgoSec}s ago`}
                        </div>
                    )}
                    {running && onCancel && isShortform ? (
                        <button
                            type="button"
                            disabled={cancelling}
                            onClick={onCancel}
                            className="inline-flex items-center gap-1 rounded-lg border border-red-500/30 bg-red-500/10 px-2 py-1 text-[10px] font-semibold text-red-100 hover:bg-red-500/20 disabled:opacity-50"
                            title="Stop this render and end further provider spend"
                        >
                            <Square className="h-3 w-3" />
                            {cancelling ? 'Stopping…' : 'Stop'}
                        </button>
                    ) : null}
                    {running && (
                        <div className="flex items-center gap-1.5 rounded-full bg-cyan-500/10 px-2.5 py-0.5 text-[10px] font-medium text-cyan-300">
                            <Loader2 className="h-3 w-3 animate-spin" />
                            {stageLabel}
                        </div>
                    )}
                    {awaiting && (
                        <div className="rounded-full bg-amber-500/10 px-2.5 py-0.5 text-[10px] font-medium text-amber-300">
                            {isThumbnailOnly
                                ? 'Thumbnail review'
                                : allScenesApproved
                                ? snapshot.animation_pending_count
                                    ? 'Approved — ready to animate'
                                    : 'Approved'
                                : 'Awaiting your review'}
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
                {isThumbnailOnly && (
                    <div className="mb-3 rounded-xl border border-violet-400/20 bg-violet-500/[0.06] p-3">
                        <div className="mb-3">
                            <p className="text-sm font-semibold text-violet-100">Thumbnail candidates</p>
                            <p className="mt-1 text-xs text-violet-100/65">
                                Packaging-only preview. The video render was not started or changed.
                            </p>
                        </div>
                        <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                            {thumbnailUrls.map((url, idx) => (
                                <div
                                    key={`${url}-${idx}`}
                                    className="overflow-hidden rounded-lg border border-white/10 bg-black/30"
                                >
                                    {thumbnailMedia.urls[idx] ? (
                                        <img
                                            src={thumbnailMedia.urls[idx]}
                                            alt={`Thumbnail candidate ${idx + 1}`}
                                            className="aspect-video w-full object-cover"
                                            loading="lazy"
                                        />
                                    ) : (
                                        <div className="aspect-video w-full animate-pulse bg-white/5" />
                                    )}
                                    <div className="flex items-center justify-between gap-2 px-2 py-1.5">
                                        <div className="text-[10px] font-medium text-white/75">Candidate {idx + 1}</div>
                                        <button
                                            type="button"
                                            disabled={!session?.access_token || thumbnailDownloadBusy === idx}
                                            onClick={() => {
                                                const tok = session?.access_token;
                                                if (!tok) return;
                                                setThumbnailDownloadBusy(idx);
                                                void downloadStudioAsset(
                                                    url,
                                                    tok,
                                                    `${String(snapshot.title || 'thumbnail').slice(0, 40).replace(/[^a-z0-9]+/gi, '-') || 'thumbnail'}-candidate-${idx + 1}.png`,
                                                ).catch((err) => {
                                                    setSceneActionError(err instanceof Error ? err.message : 'Download failed');
                                                }).finally(() => setThumbnailDownloadBusy(null));
                                            }}
                                            className="inline-flex items-center gap-1 rounded-md border border-violet-300/25 bg-violet-500/15 px-2 py-1 text-[10px] font-semibold text-violet-50 hover:bg-violet-500/25 disabled:opacity-50"
                                        >
                                            <Download className="h-3 w-3" />
                                            {thumbnailDownloadBusy === idx ? 'Saving…' : 'Download'}
                                        </button>
                                    </div>
                                </div>
                            ))}
                        </div>
                        {snapshot.next_action && (
                            <p className="mt-3 text-xs text-violet-100/70">{snapshot.next_action}</p>
                        )}
                    </div>
                )}
                {showSceneReviewGrid && (
                    <div className="mb-3">
                        <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                            <div>
                                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-200">
                                    {isVisualProof ? 'Visual proof review' : 'Scene review'}
                                </p>
                                <p className="text-[11px] text-gray-500">
                                    {isVisualProof
                                        ? 'Approve this reference still to unlock the full 18th-century gallery, or regenerate scene 1 if the look is wrong.'
                                        : 'Open a card only when you need to inspect or direct a change.'}
                                </p>
                            </div>
                            {isShortform ? (
                            <div className="flex flex-wrap items-center justify-end gap-1.5">
                                <button
                                    type="button"
                                    disabled={Boolean(sceneActionBusy)}
                                    onClick={() => void approveAllScenes(false)}
                                    className="inline-flex items-center gap-1.5 rounded-lg border border-emerald-400/20 bg-emerald-500/10 px-2.5 py-1 text-[10px] font-semibold text-emerald-100 hover:bg-emerald-500/15 disabled:opacity-50"
                                >
                                    {sceneActionBusy === 'all:still' ? <Loader2 className="h-3 w-3 animate-spin" /> : <Check className="h-3 w-3" />}
                                    Approve all as stills
                                </button>
                                <button
                                    type="button"
                                    disabled={Boolean(sceneActionBusy)}
                                    onClick={() => void approveAllScenes(true)}
                                    className="inline-flex items-center gap-1.5 rounded-lg border border-violet-400/20 bg-violet-500/10 px-2.5 py-1 text-[10px] font-semibold text-violet-100 hover:bg-violet-500/15 disabled:opacity-50"
                                >
                                    {sceneActionBusy === 'all:animate' ? <Loader2 className="h-3 w-3 animate-spin" /> : <Play className="h-3 w-3" />}
                                    Approve all for animation
                                </button>
                                <div className="rounded-full border border-cyan-400/20 bg-cyan-500/10 px-2.5 py-1 text-[10px] font-semibold text-cyan-100">
                                    {approvedSceneCount}/{sceneCards.length} approved
                                </div>
                            </div>
                            ) : null}
                        </div>
                        {allScenesApproved && Boolean(snapshot.animation_pending_count) && (
                            <div className="mb-3 rounded-xl border border-violet-400/20 bg-violet-500/10 p-3">
                                <p className="text-xs font-semibold text-violet-100">
                                    {snapshot.animation_pending_count} approved scene(s) still need i2v animation.
                                </p>
                                <button
                                    type="button"
                                    disabled={animating}
                                    onClick={() => void runAnimateApproved()}
                                    className="mt-2 inline-flex w-full items-center justify-center gap-2 rounded-lg bg-violet-600 px-3 py-2 text-xs font-semibold text-white transition active:scale-[0.985] disabled:opacity-60"
                                >
                                    {animating ? <Loader2 className="h-4 w-4 animate-spin" /> : <Film className="h-4 w-4" />}
                                    Animate approved scenes
                                </button>
                                {animateError && <p className="mt-1.5 text-center text-[10px] text-red-300">{animateError}</p>}
                            </div>
                        )}
                        <div className={`grid gap-2 ${isVisualProof && sceneCards.length === 1 ? 'grid-cols-1' : 'grid-cols-2 lg:grid-cols-3'}`}>
                            {sceneCards.map((scene, rawIdx) => {
                                const idx = Number.isFinite(scene.index) ? scene.index : rawIdx;
                                const stillQaFailed = Boolean(
                                    scene.still_qa
                                    && (scene.still_qa.status === 'fail' || scene.still_qa.pass === false),
                                );
                                const approvedForAnimation = Boolean(scene.approved_for_animation || scene.animate);
                                const approvedForVideo = Boolean(scene.approved_for_video || approvedForAnimation);
                                const statusLabel = stillQaFailed
                                    ? 'QA blocked'
                                    : approvedForAnimation
                                    ? 'Animate approved'
                                    : approvedForVideo
                                    ? 'Still approved'
                                    : 'Needs review';
                                const busyStill = sceneActionBusy === `${idx}:still`;
                                const busyAnimate = sceneActionBusy === `${idx}:animate`;
                                const busyRegenerate = sceneActionBusy === `${idx}:regenerate`;
                                return (
                                    <div
                                        key={`${snapshot.job_id}-scene-${idx}`}
                                        className={`overflow-hidden rounded-2xl border bg-black/25 ${
                                            stillQaFailed
                                                ? 'border-red-400/35'
                                                : approvedForAnimation
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
                                            {scene.has_clip ? (
                                                <SceneClipPlayer
                                                    jobId={snapshot.job_id}
                                                    idx={idx}
                                                    cacheKey={String(
                                                        scene.clip_preview_url
                                                        || snapshot.client_updated_at
                                                        || ''
                                                    )}
                                                />
                                            ) : (
                                                <StillThumb
                                                    jobId={snapshot.job_id}
                                                    idx={idx}
                                                    cacheKey={String(snapshot.client_updated_at || scene.still_preview_url || '')}
                                                />
                                            )}
                                        </button>
                                        <div className="space-y-1.5 p-2">
                                            <div className="flex items-start justify-between gap-2">
                                                <div>
                                                    <p className="text-xs font-semibold text-white">Scene {idx + 1}</p>
                                                    <p className="text-[10px] text-gray-500">
                                                        {scene.duration_sec ? `${Math.round(scene.duration_sec)}s` : 'Still'}{scene.has_clip ? ' - clip ready' : ''}
                                                    </p>
                                                </div>
                                                <span className={`shrink-0 rounded-full px-2 py-0.5 text-[9px] font-semibold ${
                                                    stillQaFailed
                                                        ? 'bg-red-500/15 text-red-200'
                                                        : approvedForAnimation
                                                        ? 'bg-violet-500/15 text-violet-200'
                                                        : approvedForVideo
                                                        ? 'bg-emerald-500/15 text-emerald-200'
                                                        : 'bg-amber-500/15 text-amber-200'
                                                }`}>
                                                    {statusLabel}
                                                </span>
                                            </div>
                                            {stillQaFailed && (
                                                <p className="rounded-lg border border-red-400/20 bg-red-500/10 px-2 py-1.5 text-[10px] leading-relaxed text-red-200">
                                                    {scene.still_qa?.summary || 'Visual QA could not prove canonical skeleton identity.'}
                                                </p>
                                            )}
                                            {(scene.narration || scene.scene_action) && (
                                                <p className="line-clamp-2 text-[10px] leading-relaxed text-gray-500">
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
                                                {isShortform ? (
                                                    <>
                                                <button
                                                    type="button"
                                                    disabled={Boolean(sceneActionBusy)}
                                                    onClick={() => void editExactPrompt(scene, idx)}
                                                    className="inline-flex items-center justify-center gap-1.5 rounded-lg border border-amber-400/20 bg-amber-500/10 px-2 py-1.5 text-[10px] font-semibold text-amber-100 hover:bg-amber-500/15 disabled:opacity-50"
                                                    title="Edit the exact provider prompt used when regenerating this scene"
                                                >
                                                    <FileText className="h-3 w-3" /> Prompt
                                                </button>
                                                <button
                                                    type="button"
                                                    disabled={Boolean(sceneActionBusy)}
                                                    onClick={() => void regenerateScene(idx)}
                                                    className="inline-flex items-center justify-center gap-1.5 rounded-lg border border-white/10 bg-white/[0.035] px-2 py-1.5 text-[10px] font-semibold text-gray-200 hover:bg-white/[0.07] disabled:opacity-50"
                                                    title="Studio re-directs this scene from its narration, then regenerates its still and animation"
                                                >
                                                    {busyRegenerate ? <Loader2 className="h-3 w-3 animate-spin" /> : <RefreshCw className="h-3 w-3" />}
                                                    Intelligent regenerate
                                                </button>
                                                <button
                                                    type="button"
                                                    disabled={Boolean(sceneActionBusy) || stillQaFailed}
                                                    onClick={() => void approveScene(idx, false)}
                                                    className="inline-flex items-center justify-center gap-1.5 rounded-lg border border-emerald-400/20 bg-emerald-500/10 px-2 py-1.5 text-[10px] font-semibold text-emerald-100 hover:bg-emerald-500/15 disabled:opacity-50"
                                                >
                                                    {busyStill ? <Loader2 className="h-3 w-3 animate-spin" /> : <Check className="h-3 w-3" />}
                                                    Approve
                                                </button>
                                                <button
                                                    type="button"
                                                    disabled={Boolean(sceneActionBusy) || stillQaFailed}
                                                    onClick={() => void approveScene(idx, true)}
                                                    className="col-span-2 inline-flex items-center justify-center gap-1.5 rounded-lg border border-violet-400/20 bg-violet-500/10 px-2 py-1.5 text-[10px] font-semibold text-violet-100 hover:bg-violet-500/15 disabled:opacity-50"
                                                >
                                                    {busyAnimate ? <Loader2 className="h-3 w-3 animate-spin" /> : <Play className="h-3 w-3" />}
                                                    Approve + animate
                                                </button>
                                                    </>
                                                ) : (
                                                    <button
                                                        type="button"
                                                        disabled={Boolean(sceneActionBusy)}
                                                        onClick={() => void regenerateScene(idx)}
                                                        className="col-span-2 inline-flex items-center justify-center gap-1.5 rounded-lg border border-white/10 bg-white/[0.035] px-2 py-1.5 text-[10px] font-semibold text-gray-200 hover:bg-white/[0.07] disabled:opacity-50"
                                                        title="Regenerate this still with the locked 18th-century art style"
                                                    >
                                                        {busyRegenerate ? <Loader2 className="h-3 w-3 animate-spin" /> : <RefreshCw className="h-3 w-3" />}
                                                        Regenerate still
                                                    </button>
                                                )}
                                            </div>
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                        {isLongform && isVisualProof ? (
                            <div className="mt-3 rounded-xl border border-amber-500/20 bg-amber-500/5 p-3">
                                <p className="text-xs font-semibold text-amber-100">Approve the visual look</p>
                                <p className="mt-1 text-[11px] leading-relaxed text-amber-100/70">
                                    This proof still locks the 18th-century style for the full 9-hour gallery. Regenerate if anything looks wrong, then continue.
                                </p>
                                <button
                                    type="button"
                                    disabled={expandingProof}
                                    onClick={() => void runExpandProof()}
                                    className="mt-3 flex w-full items-center justify-center gap-2 rounded-xl bg-amber-600 py-2.5 text-sm font-semibold text-white transition active:scale-[0.985] disabled:opacity-60"
                                >
                                    {expandingProof ? <Loader2 className="h-4 w-4 animate-spin" /> : <Check className="h-4 w-4" />}
                                    Looks good — build full gallery
                                </button>
                                {expandProofError && <p className="mt-1.5 text-center text-[10px] text-red-300">{expandProofError}</p>}
                            </div>
                        ) : null}
                        {sceneActionError && <p className="mt-2 text-[11px] text-red-300">{sceneActionError}</p>}
                    </div>
                )}
                {/* Scene / stills strip — the real-time "watch it being made" part */}
                {!(showSceneReviewGrid) && (isShortform || isLongform) && (stills.length > 0 || totalScenes > 0 || running) && (
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
                                            <StillThumb
                                                jobId={snapshot.job_id}
                                                idx={idx}
                                                cacheKey={String(snapshot.client_updated_at || sceneCards[idx]?.still_preview_url || stills[idx] || '')}
                                            />
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
                            {running
                                ? (isLongform && snapshot.stage === 'scenes'
                                    ? `Building the full 18th-century gallery${totalScenes > 0 ? ` — ${currentScene || stills.length}/${totalScenes} scenes` : ''}…`
                                    : 'Agent is generating stills + motion in real time...')
                                : awaiting
                                ? 'Open a scene to inspect it. Use Edit only when you want Studio Agent to revise that exact still before animation.'
                                : ''}
                        </p>
                    </div>
                )}

                {/* Stage detail */}
                {snapshot.stage_detail && (
                    <p className="mb-2 text-xs text-gray-400">{snapshot.stage_detail}</p>
                )}

                {isClipLab && complete && !isClipLabRender && (
                    <div className="mt-2 rounded-xl border border-cyan-400/20 bg-cyan-500/5 p-3 text-xs text-cyan-100/90">
                        <p className="font-semibold text-cyan-100">{clipLabStepLabel}</p>
                        <p className="mt-1 text-cyan-100/70">{clipLabStepDetail}</p>
                        {snapshot.next_action && (
                            <p className="mt-2 border-t border-cyan-400/10 pt-2 text-[10px] uppercase tracking-wide text-cyan-200/60">
                                Next: {snapshot.next_action}
                            </p>
                        )}
                    </div>
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
                        <p className="font-semibold text-cyan-100">
                            {allScenesApproved ? 'Scene approval complete' : 'Scene review required'}
                        </p>
                        <p className="mt-1 text-cyan-100/70">
                            {allScenesApproved
                                ? snapshot.animation_pending_count
                                    ? `${snapshot.animation_pending_count} approved scene(s) are ready for image-to-video. Run animation before exporting.`
                                    : snapshot.animation_complete_count
                                    ? 'Animated scenes are playable above. Finalize only when you want the full stitched MP4 export.'
                                    : 'All scenes are approved. Review any completed animation, then finalize the production.'
                                : 'No image-to-video should run until these stills are approved. Type the scene fix in chat, or approve scenes for animation.'}
                        </p>
                        {allScenesApproved && Boolean(snapshot.animation_pending_count) && (
                            <button
                                type="button"
                                disabled={animating}
                                onClick={() => void runAnimateApproved()}
                                className="mt-3 flex w-full items-center justify-center gap-2 rounded-xl bg-violet-600 py-2.5 text-sm font-semibold text-white transition active:scale-[0.985] disabled:opacity-60"
                            >
                                {animating ? <Loader2 className="h-4 w-4 animate-spin" /> : <Film className="h-4 w-4" />}
                                Animate approved scenes
                            </button>
                        )}
                        {allScenesApproved && !snapshot.animation_pending_count && (
                            <button
                                type="button"
                                disabled={finalizing}
                                onClick={() => void runFinalize()}
                                className="mt-3 flex w-full items-center justify-center gap-2 rounded-xl border border-emerald-400/25 bg-emerald-500/10 py-2.5 text-sm font-semibold text-emerald-100 transition active:scale-[0.985] disabled:opacity-60"
                            >
                                {finalizing ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                                {snapshot.animation_complete_count ? 'Export full stitched MP4' : 'Finalize & export MP4'}
                            </button>
                        )}
                        {finalizeError && <p className="mt-1.5 text-center text-[10px] text-red-300">{finalizeError}</p>}
                        {animateError && <p className="mt-1.5 text-center text-[10px] text-red-300">{animateError}</p>}
                    </div>
                )}
                {awaiting && isLongform && !isThumbnailOnly && !isVisualProof && (
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

                {complete && isClipLabRender && (
                    <div className="mt-2 space-y-3">
                        <div className="rounded-xl border border-emerald-400/20 bg-emerald-500/5 p-3 text-xs text-emerald-100/90">
                            <p className="font-semibold text-emerald-100">{clipLabStepLabel}</p>
                            <p className="mt-1 text-emerald-100/70">{clipLabStepDetail}</p>
                        </div>
                        {clipLabClips.length ? (
                            <div className="grid gap-3 sm:grid-cols-2">
                                {clipLabClips.map((clip, idx) => {
                                    const pkg = clipLabPackages.find((item) => Number(item.clip_index) === Number(clip.index ?? idx)) || clipLabPackages[idx];
                                    const tok = session?.access_token || '';
                                    const href = clipLabMedia.urls[idx] || '';
                                    const breakdown = clip.score_breakdown || pkg?.score_breakdown || {};
                                    const editPlan = clip.edit_plan?.length ? clip.edit_plan : pkg?.edit_plan || [];
                                    const score = clip.virality_score ?? pkg?.virality_score;
                                    return (
                                        <div key={`${clip.filename || clip.url || idx}`} className="overflow-hidden rounded-xl border border-white/10 bg-black/30">
                                            {href ? (
                                                <video
                                                    src={href}
                                                    controls
                                                    className="aspect-[9/16] w-full bg-black object-contain"
                                                    playsInline
                                                />
                                            ) : (
                                                <div className="flex aspect-[9/16] items-center justify-center bg-black/50 text-xs text-gray-500">
                                                    Clip URL missing
                                                </div>
                                            )}
                                            <div className="space-y-2 p-3">
                                                <div className="flex items-start justify-between gap-2">
                                                    <div>
                                                        <p className="text-xs font-semibold text-white">
                                                            Clip {idx + 1}{pkg?.title ? ` - ${pkg.title}` : ''}
                                                        </p>
                                                        {(clip.start != null || clip.end != null || score != null) && (
                                                            <p className="mt-0.5 text-[10px] text-gray-500">
                                                                {clip.start ?? '?'}s - {clip.end ?? '?'}s{score != null ? ` · ${Math.round(Number(score))}/100` : ''}
                                                            </p>
                                                        )}
                                                    </div>
                                                    {clip.url && (
                                                        <button
                                                            type="button"
                                                            disabled={!tok || assetDownloadBusy === `clip-${idx}`}
                                                            onClick={() => {
                                                                if (!tok || !clip.url) return;
                                                                setAssetDownloadBusy(`clip-${idx}`);
                                                                setAssetDownloadError('');
                                                                void downloadStudioAsset(
                                                                    clip.url,
                                                                    tok,
                                                                    clip.filename || `cliplab_clip_${idx + 1}.mp4`,
                                                                ).catch((error) => {
                                                                    setAssetDownloadError(error instanceof Error ? error.message : 'Download failed');
                                                                }).finally(() => setAssetDownloadBusy(''));
                                                            }}
                                                            className="shrink-0 rounded-lg border border-emerald-400/20 bg-emerald-500/10 px-2 py-1 text-[10px] font-semibold text-emerald-100 hover:bg-emerald-500/15"
                                                        >
                                                            {assetDownloadBusy === `clip-${idx}` ? 'Savingâ€¦' : 'Download'}
                                                        </button>
                                                    )}
                                                </div>
                                                {pkg?.hook && <p className="text-[11px] text-cyan-100/80">Hook: {pkg.hook}</p>}
                                                {(clip.why_it_matches || pkg?.rationale || clip.retention_reason || pkg?.retention_reason) && (
                                                    <div className="rounded-lg border border-cyan-400/15 bg-cyan-500/[0.04] p-2 text-[11px] text-cyan-50/80">
                                                        {(clip.why_it_matches || pkg?.rationale) && <p>{clip.why_it_matches || pkg?.rationale}</p>}
                                                        {(clip.retention_reason || pkg?.retention_reason) && (
                                                            <p className="mt-1 text-cyan-100/60">{clip.retention_reason || pkg?.retention_reason}</p>
                                                        )}
                                                    </div>
                                                )}
                                                {(clip.visual_notes || pkg?.visual_notes || clip.audio_notes || pkg?.audio_notes || Object.keys(breakdown).length > 0) && (
                                                    <details className="rounded-lg border border-white/10 bg-white/[0.025] p-2 text-[11px] text-gray-300">
                                                        <summary className="cursor-pointer font-semibold text-gray-100">Clip intelligence</summary>
                                                        {(clip.visual_notes || pkg?.visual_notes) && <p className="mt-2">Visual: {clip.visual_notes || pkg?.visual_notes}</p>}
                                                        {(clip.audio_notes || pkg?.audio_notes) && <p className="mt-1">Audio: {clip.audio_notes || pkg?.audio_notes}</p>}
                                                        {(clip.narrative_role || pkg?.narrative_role) && <p className="mt-1">Role: {clip.narrative_role || pkg?.narrative_role}</p>}
                                                        {Object.keys(breakdown).length > 0 && (
                                                            <div className="mt-2 flex flex-wrap gap-1.5">
                                                                {Object.entries(breakdown).slice(0, 6).map(([key, value]) => (
                                                                    <span key={key} className="rounded-full border border-white/10 px-2 py-1 text-[10px] text-gray-300">
                                                                        {key}: {Math.round(Number(value))}
                                                                    </span>
                                                                ))}
                                                            </div>
                                                        )}
                                                    </details>
                                                )}
                                                {editPlan.length ? (
                                                    <details className="rounded-lg border border-white/10 bg-white/[0.025] p-2 text-[11px] text-gray-300">
                                                        <summary className="cursor-pointer font-semibold text-gray-100">Edit plan</summary>
                                                        <ol className="mt-2 list-decimal space-y-1 pl-4">
                                                            {editPlan.slice(0, 7).map((step, stepIdx) => (
                                                                <li key={`${stepIdx}-${step}`}>{step}</li>
                                                            ))}
                                                        </ol>
                                                    </details>
                                                ) : null}
                                                {pkg?.description && (
                                                    <details className="rounded-lg border border-white/10 bg-white/[0.025] p-2 text-[11px] text-gray-300">
                                                        <summary className="cursor-pointer font-semibold text-gray-100">Upload package</summary>
                                                        <p className="mt-2 whitespace-pre-wrap">{pkg.description}</p>
                                                        {pkg.tags?.length ? (
                                                            <p className="mt-2 text-cyan-100/70">{pkg.tags.join(', ')}</p>
                                                        ) : null}
                                                    </details>
                                                )}
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                        ) : (
                            <div className="rounded-xl border border-amber-400/20 bg-amber-500/5 p-3 text-xs text-amber-100/80">
                                ClipLab says render is complete, but no clip URLs were returned in the job snapshot. Ask Studio Agent to poll this render again.
                            </div>
                        )}
                    </div>
                )}

                {/* Final video (the payoff, right in the chat) */}
                {complete && !isAnalysis && !isClipLab && (snapshot.mp4_url || snapshot.download_url || videoSrc) && (
                    <div className="mt-1 flex justify-center">
                        <div
                            className={`w-full ${
                                isShortform ? 'max-w-[min(280px,78%)]' : 'max-w-full'
                            }`}
                        >
                            {enableVideoPreview ? (
                                videoSrc ? (
                                    <video
                                        src={videoSrc}
                                        controls
                                        className={`w-full rounded-xl border border-white/10 bg-black object-contain ${
                                            isShortform
                                                ? 'aspect-[9/16] max-h-[min(480px,55vh)]'
                                                : 'aspect-video max-h-[min(360px,45vh)]'
                                        }`}
                                        playsInline
                                    />
                                ) : videoLoadFailed ? (
                                    <div
                                        className={`flex items-center justify-center rounded-xl border border-white/10 bg-black/40 px-4 text-center text-sm text-gray-400 ${
                                            isShortform
                                                ? 'aspect-[9/16] max-h-[min(480px,55vh)]'
                                                : 'aspect-video h-40'
                                        }`}
                                    >
                                        Final video is not ready yet — use Download MP4 when export finishes.
                                    </div>
                                ) : (
                                    <div
                                        className={`flex items-center justify-center rounded-xl border border-white/10 bg-black/40 text-sm text-gray-400 ${
                                            isShortform
                                                ? 'aspect-[9/16] max-h-[min(480px,55vh)]'
                                                : 'aspect-video h-40'
                                        }`}
                                    >
                                        Loading final video preview…
                                    </div>
                                )
                            ) : (
                                <div className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-center text-[11px] text-gray-400">
                                    Video ready — open the latest deliverable above to preview.
                                </div>
                            )}
                        </div>
                    </div>
                )}

                {/* Competitor / reference analysis (keep compact & useful) */}
                {isAnalysis && complete && (
                    <div className="text-[11px] text-gray-300">
                        {snapshot.visual_summary ? (
                            <p className="mb-2 text-xs leading-relaxed text-cyan-100/90">
                                {snapshot.visual_summary}
                            </p>
                        ) : null}
                        {snapshot.pacing && (
                            <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
                                {snapshot.pacing.avg_shot_sec != null && <div>Avg shot: <span className="text-white">{snapshot.pacing.avg_shot_sec}s</span></div>}
                                {snapshot.pacing.cut_count != null && <div>Cuts: <span className="text-white">{snapshot.pacing.cut_count}</span></div>}
                                {snapshot.pacing.duration_sec != null && <div>Duration: <span className="text-white">{snapshot.pacing.duration_sec}s</span></div>}
                            </div>
                        )}
                        {snapshot.hook_summary ? (
                            <p className="mb-2 text-xs leading-relaxed text-violet-100/90">
                                <span className="font-medium text-violet-200/90">Hook:</span> {snapshot.hook_summary}
                            </p>
                        ) : null}
                        {snapshot.storytelling_summary ? (
                            <p className="mb-2 text-xs leading-relaxed text-gray-200">
                                {snapshot.storytelling_summary}
                            </p>
                        ) : null}
                        {snapshot.packaging_notes ? (
                            <p className="mb-2 text-[10px] text-gray-400">
                                <span className="text-gray-300">Packaging:</span> {snapshot.packaging_notes}
                            </p>
                        ) : null}
                        {snapshot.pacing_warnings?.length ? (
                            <p className="mt-2 rounded-lg border border-amber-400/20 bg-amber-500/5 p-2 text-[10px] text-amber-100/80">
                                {snapshot.pacing_warnings[0]}
                            </p>
                        ) : null}
                        {snapshot.blueprint_hint && (
                            <p className="mt-2 border-t border-white/10 pt-2 text-[10px] text-gray-400">{snapshot.blueprint_hint}</p>
                        )}
                    </div>
                )}
            </div>

            {/* Bottom actions */}
                {complete && !isAnalysis && !isClipLab && (
                <div className="border-t border-white/[0.06] bg-black/20 px-3 py-2">
                    {(() => {
                        const tok = session?.access_token || '';
                        const downloadPath =
                            videoSrc ||
                            snapshot.download_url ||
                            snapshot.mp4_url ||
                            (snapshot.job_id ? agentJobMediaUrl(snapshot.job_id, snapshot.kind) : '');
                        const packagePath = snapshot.package_url || (
                            snapshot.job_id ? agentJobPackageUrl(snapshot.job_id, snapshot.kind) : ''
                        );
                        return downloadPath ? (
                            <>
                                <button
                                    type="button"
                                    disabled={!tok || assetDownloadBusy === 'video'}
                                    onClick={() => {
                                        if (!tok) return;
                                        setAssetDownloadBusy('video');
                                        setAssetDownloadError('');
                                        void downloadStudioAsset(downloadPath, tok, `${snapshot.job_id}.mp4`)
                                            .catch((error) => setAssetDownloadError(error instanceof Error ? error.message : 'Download failed'))
                                            .finally(() => setAssetDownloadBusy(''));
                                    }}
                                    className="flex items-center justify-center gap-2 rounded-xl bg-emerald-600 py-2 text-sm font-semibold text-white transition hover:bg-emerald-500"
                                >
                                    <Download className="h-4 w-4" /> {assetDownloadBusy === 'video' ? 'Savingâ€¦' : 'Download MP4'}
                                </button>
                                {packagePath && (
                                    <button
                                        type="button"
                                        disabled={!tok || assetDownloadBusy === 'package'}
                                        onClick={() => {
                                            if (!tok) return;
                                            setAssetDownloadBusy('package');
                                            setAssetDownloadError('');
                                            void downloadStudioAsset(
                                                packagePath,
                                                tok,
                                                `${snapshot.job_id}_upload_package.txt`,
                                            ).catch((error) => {
                                                setAssetDownloadError(error instanceof Error ? error.message : 'Download failed');
                                            }).finally(() => setAssetDownloadBusy(''));
                                        }}
                                        className="mt-2 flex items-center justify-center gap-2 rounded-xl border border-cyan-400/20 bg-cyan-500/10 py-2 text-sm font-semibold text-cyan-100 transition hover:bg-cyan-500/15"
                                    >
                                        <FileText className="h-4 w-4" /> {assetDownloadBusy === 'package' ? 'Savingâ€¦' : 'Upload package'}
                                    </button>
                                )}
                                {assetDownloadError ? <p className="mt-2 text-xs text-red-300">{assetDownloadError}</p> : null}
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
                                {thumbnailUrls.length > 0 && (
                                    <div className="mt-3 rounded-xl border border-violet-400/20 bg-violet-500/[0.06] p-3">
                                        <p className="mb-2 text-xs font-semibold text-violet-100">Thumbnail candidates</p>
                                        <p className="mb-3 text-[11px] text-violet-100/65">
                                            Packaging preview — reply in chat to revise (e.g. “make candidate 2 darker”).
                                        </p>
                                        <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                                            {thumbnailUrls.map((url, idx) => (
                                                <div key={`${url}-${idx}`} className="overflow-hidden rounded-lg border border-white/10 bg-black/30">
                                                    {thumbnailMedia.urls[idx] ? (
                                                        <img
                                                            src={thumbnailMedia.urls[idx]}
                                                            alt={`Thumbnail candidate ${idx + 1}`}
                                                            className="aspect-video w-full object-cover"
                                                            loading="lazy"
                                                        />
                                                    ) : (
                                                        <div className="aspect-video w-full animate-pulse bg-white/5" />
                                                    )}
                                                    <div className="flex items-center justify-between gap-2 px-2 py-1.5">
                                                        <div className="text-[10px] font-medium text-white/75">Candidate {idx + 1}</div>
                                                        <button
                                                            type="button"
                                                            disabled={!session?.access_token || thumbnailDownloadBusy === idx}
                                                            onClick={() => {
                                                                const tok = session?.access_token;
                                                                if (!tok) return;
                                                                setThumbnailDownloadBusy(idx);
                                                                void downloadStudioAsset(
                                                                    url,
                                                                    tok,
                                                                    `${String(snapshot.title || 'thumbnail').slice(0, 40).replace(/[^a-z0-9]+/gi, '-') || 'thumbnail'}-candidate-${idx + 1}.png`,
                                                                ).catch((err) => {
                                                                    setSceneActionError(err instanceof Error ? err.message : 'Download failed');
                                                                }).finally(() => setThumbnailDownloadBusy(null));
                                                            }}
                                                            className="inline-flex items-center gap-1 rounded-md border border-violet-300/25 bg-violet-500/15 px-2 py-1 text-[10px] font-semibold text-violet-50 hover:bg-violet-500/25 disabled:opacity-50"
                                                        >
                                                            <Download className="h-3 w-3" />
                                                            {thumbnailDownloadBusy === idx ? 'Saving…' : 'Download'}
                                                        </button>
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
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
                    hasClip={Boolean(sceneCards.find((scene) => Number(scene.index) === inspectSceneIdx)?.has_clip)}
                    clipCacheKey={String(
                        sceneCards.find((scene) => Number(scene.index) === inspectSceneIdx)?.clip_preview_url
                        || snapshot.client_updated_at
                        || ''
                    )}
                    onClose={() => setInspectSceneIdx(null)}
                    onEdit={onReply ? () => {
                        onReply(snapshot, inspectSceneIdx, 'edit');
                        setInspectSceneIdx(null);
                    } : undefined}
                    onRegenerate={() => {
                        void regenerateScene(inspectSceneIdx);
                        setInspectSceneIdx(null);
                    }}
                    onApproveStill={() => void approveScene(inspectSceneIdx, false)}
                    onApproveAnimate={() => void approveScene(inspectSceneIdx, true)}
                />
            )}
        </div>
    );
}

export default memo(AgentJobDeliverable);
