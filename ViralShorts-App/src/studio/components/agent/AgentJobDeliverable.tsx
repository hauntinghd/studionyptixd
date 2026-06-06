import { Download, Film, Loader2, Play, Clapperboard, CheckCircle2, ArrowLeft } from 'lucide-react';
import { useCallback, useContext, useEffect, useState } from 'react';
import { AuthContext } from '../../shared';
import type { AgentJobSnapshot } from '../../lib/agentProduction';
import {
    agentJobFinalizeUrl,
    agentJobMediaUrl,
    agentJobStillUrl,
    mediaUrl,
} from '../../lib/agentProduction';

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

export default function AgentJobDeliverable({
    snapshot,
    onFinalizeStarted,
    onReply,
}: {
    snapshot: AgentJobSnapshot;
    onFinalizeStarted?: (jobId: string, activeJobs?: unknown) => void;
    onReply?: (snapshot: AgentJobSnapshot) => void;
}) {
    const { session } = useContext(AuthContext);
    const [videoSrc, setVideoSrc] = useState('');
    const [finalizing, setFinalizing] = useState(false);
    const [finalizeError, setFinalizeError] = useState('');

    const title = snapshot.title || (snapshot.kind === 'shortform' ? 'Your Short' : 'Production');
    const isAnalysis = snapshot.kind === 'competitor';
    const complete = snapshot.status === 'complete';
    const failed = snapshot.status === 'failed';
    const awaiting = snapshot.status === 'awaiting_approval';
    const running = snapshot.running !== false && !complete && !failed && !awaiting;

    const stills = snapshot.still_preview_urls || [];
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
                {/* Scene / stills strip — the real-time "watch it being made" part */}
                {isShortform && (stills.length > 0 || totalScenes > 0 || running) && (
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
                                        className={`relative w-16 shrink-0 overflow-hidden rounded-xl border text-center text-[9px] transition-all ${
                                            hasStill
                                                ? 'border-white/10'
                                                : isCurrent
                                                ? 'border-cyan-400/40 bg-cyan-950/40 ring-1 ring-cyan-400/20'
                                                : 'border-white/[0.06] bg-white/[0.015]'
                                        }`}
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
                                            {idx + 1}
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                        <p className="mt-1 text-[10px] text-gray-500">
                            {running ? 'Agent is generating stills + motion in real time…' : awaiting ? 'Review the frames above, then finalize.' : ''}
                        </p>
                    </div>
                )}

                {/* Stage detail */}
                {snapshot.stage_detail && (
                    <p className="mb-2 text-xs text-gray-400">{snapshot.stage_detail}</p>
                )}

                {/* Awaiting approval actions (beautiful finalize gate) */}
                {awaiting && (
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
                        return downloadHref ? (
                            <>
                                <a
                                    href={downloadHref}
                                    download={`${snapshot.job_id}.mp4`}
                                    className="flex items-center justify-center gap-2 rounded-xl bg-emerald-600 py-2 text-sm font-semibold text-white transition hover:bg-emerald-500"
                                >
                                    <Download className="h-4 w-4" /> Download MP4
                                </a>
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
        </div>
    );
}
