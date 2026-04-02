import { useContext, useState } from 'react';
import { CheckCircle2, Loader2, Send, Star } from 'lucide-react';
import { API, AuthContext } from '../shared';

export function ThumbProgressBar({ progress, status }: { progress: number; status: string }) {
    const labels: Record<string, string> = {
        queued: 'In queue...',
        analyzing: 'AI designing your thumbnail...',
        generating: 'Rendering on GPU...',
        complete: 'Done!',
        error: 'Error occurred',
    };
    return (
        <div>
            <div className="flex justify-between text-sm mb-3">
                <span className="text-violet-300 font-medium">{labels[status] || status}</span>
                <span className="text-gray-600 tabular-nums">{progress}%</span>
            </div>
            <div className="w-full bg-white/[0.05] rounded-full h-2.5 overflow-hidden">
                <div className="h-full rounded-full bg-gradient-to-r from-violet-600 to-purple-500 transition-all duration-700 ease-out"
                    style={{ width: `${progress}%` }} />
            </div>
        </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════════════
   SHARED COMPONENTS
   ═══════════════════════════════════════════════════════════════════════════ */

export function FeedbackWidget({ jobId, template, feature, language }: { jobId?: string; template?: string; feature?: string; language?: string }) {
    const { session } = useContext(AuthContext);
    const [rating, setRating] = useState(0);
    const [hoveredStar, setHoveredStar] = useState(0);
    const [comment, setComment] = useState('');
    const [submitted, setSubmitted] = useState(false);
    const [submitting, setSubmitting] = useState(false);

    if (submitted) {
        return (
            <div className="flex items-center gap-2 text-emerald-400 text-sm py-2">
                <CheckCircle2 className="w-4 h-4" />
                <span>Thanks for the feedback!</span>
            </div>
        );
    }

    const handleSubmit = async () => {
        if (!rating || !session) return;
        setSubmitting(true);
        try {
            await fetch(`${API}/api/feedback`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${session.access_token}` },
                body: JSON.stringify({
                    job_id: jobId || '',
                    rating,
                    comment,
                    template: template || '',
                    language: language || 'en',
                    feature: feature || 'general',
                }),
            });
            setSubmitted(true);
        } catch { /* silent */ }
        setSubmitting(false);
    };

    return (
        <div className="space-y-3 pt-2 border-t border-white/[0.06]">
            <p className="text-xs text-gray-500 uppercase tracking-wider font-medium">Rate this generation</p>
            <div className="flex items-center gap-1">
                {[1, 2, 3, 4, 5].map(s => (
                    <button key={s}
                        onMouseEnter={() => setHoveredStar(s)}
                        onMouseLeave={() => setHoveredStar(0)}
                        onClick={() => setRating(s)}
                        className="p-0.5 transition-transform hover:scale-110">
                        <Star className={`w-7 h-7 transition-colors ${
                            s <= (hoveredStar || rating)
                                ? 'text-amber-400 fill-amber-400'
                                : 'text-gray-600'
                        }`} />
                    </button>
                ))}
                {rating > 0 && (
                    <span className="text-xs text-gray-500 ml-2">
                        {['', 'Poor', 'Fair', 'Good', 'Great', 'Amazing'][rating]}
                    </span>
                )}
            </div>
            {rating > 0 && (
                <>
                    <textarea value={comment} onChange={(e) => setComment(e.target.value)}
                        placeholder="What did you like? What could be better? Any features you want?"
                        rows={2}
                        className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-4 py-2.5 text-sm text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all resize-none" />
                    <button onClick={handleSubmit} disabled={submitting}
                        className="flex items-center justify-center gap-2 px-4 py-2 bg-violet-600 hover:bg-violet-500 text-white text-sm font-medium rounded-xl transition-all disabled:opacity-50">
                        {submitting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
                        Submit Feedback
                    </button>
                </>
            )}
        </div>
    );
}

export function ProgressBar({ progress, status }: { progress: number; status: string }) {
    const labels: Record<string, string> = {
        queued: "In queue...",
        analyzing: "Reverse-engineering viral formula...",
        generating_script: "AI is writing the script...",
        generating_images: "Generating scene images...",
        animating_scenes: "Animating scenes with AI video...",
        generating_voice: "Creating AI voiceover...",
        generating_sfx: "Generating sound effects...",
        compositing: "Compositing final video...",
        complete: "Done!",
        error: "Error occurred",
    };

    return (
        <div>
            <div className="flex justify-between text-sm mb-3">
                <span className="text-violet-300 font-medium">{labels[status] || status}</span>
                <span className="text-gray-600 tabular-nums">{progress}%</span>
            </div>
            <div className="w-full bg-white/[0.05] rounded-full h-2.5 overflow-hidden">
                <div className="h-full rounded-full bg-gradient-to-r from-violet-600 to-purple-500 transition-all duration-700 ease-out"
                    style={{ width: `${progress}%` }} />
            </div>
        </div>
    );
}

export function JobDiagnostics({ jobStatus }: { jobStatus: any }) {
    const diagnostics = jobStatus?.diagnostics;
    if (!diagnostics) return null;
    const durations = diagnostics.stage_durations_sec || {};
    const stageEntries = Object.entries(durations) as Array<[string, number]>;
    const latestSceneEvent = Array.isArray(diagnostics.scene_events) && diagnostics.scene_events.length > 0
        ? diagnostics.scene_events[diagnostics.scene_events.length - 1]
        : null;
    return (
        <div className="rounded-xl border border-white/[0.06] bg-white/[0.02] p-3 space-y-2">
            <p className="text-[11px] uppercase tracking-wide text-gray-500">Diagnostics</p>
            <p className="text-xs text-gray-300">
                Stage: <span className="text-violet-300">{diagnostics.current_stage || jobStatus?.status || 'unknown'}</span>
            </p>
            {jobStatus?.animation_warnings ? (
                <p className="text-xs text-amber-300">Animation warnings: {jobStatus.animation_warnings}</p>
            ) : null}
            {stageEntries.length > 0 ? (
                <p className="text-xs text-gray-400">
                    {stageEntries.slice(-4).map(([k, v]) => `${k}: ${v}s`).join(" | ")}
                </p>
            ) : null}
            {latestSceneEvent ? (
                <p className="text-xs text-gray-400">
                    Scene {latestSceneEvent.scene}/{latestSceneEvent.total_scenes}: {latestSceneEvent.event}
                    {latestSceneEvent.detail ? ` (${latestSceneEvent.detail})` : ""}
                </p>
            ) : null}
        </div>
    );
}

type RenderProgressWindowProps = {
    jobStatus: any;
    title?: string;
    previewUrl?: string | null;
    previewType?: 'image' | 'video';
    previewLabel?: string;
};

function buildRenderPreviewSrcDoc(previewUrl?: string | null, previewType: 'image' | 'video' = 'image', previewLabel?: string) {
    const safeUrl = String(previewUrl || '')
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/</g, '&lt;');
    const safeLabel = String(previewLabel || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
    const media = safeUrl
        ? previewType === 'video'
            ? `<video autoplay muted loop playsinline controls src="${safeUrl}"></video>`
            : `<img src="${safeUrl}" alt="${safeLabel || 'Render preview'}" />`
        : `<div class="placeholder">Preview will appear here as the render advances.</div>`;
    return `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <style>
      html, body {
        margin: 0;
        width: 100%;
        height: 100%;
        background: #05060a;
        color: #e5e7eb;
        font-family: Inter, system-ui, sans-serif;
      }
      .shell {
        position: relative;
        width: 100%;
        height: 100%;
        overflow: hidden;
        background:
          radial-gradient(circle at top, rgba(34,211,238,0.12), transparent 55%),
          linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.01));
      }
      img, video {
        width: 100%;
        height: 100%;
        object-fit: cover;
        display: block;
        background: #05060a;
      }
      .placeholder {
        display: flex;
        width: 100%;
        height: 100%;
        align-items: center;
        justify-content: center;
        text-align: center;
        padding: 24px;
        font-size: 13px;
        color: rgba(229,231,235,0.75);
        background:
          radial-gradient(circle at top, rgba(168,85,247,0.18), transparent 45%),
          linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0.01));
      }
      .label {
        position: absolute;
        left: 12px;
        right: 12px;
        bottom: 12px;
        border-radius: 999px;
        background: rgba(5, 6, 10, 0.78);
        border: 1px solid rgba(255,255,255,0.1);
        padding: 8px 12px;
        font-size: 11px;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        color: rgba(224,231,255,0.92);
        backdrop-filter: blur(8px);
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
      }
    </style>
  </head>
  <body>
    <div class="shell">
      ${media}
      ${safeLabel ? `<div class="label">${safeLabel}</div>` : ''}
    </div>
  </body>
</html>`;
}

export function RenderProgressWindow({
    jobStatus,
    title = 'Rendering in progress',
    previewUrl,
    previewType = 'image',
    previewLabel,
}: RenderProgressWindowProps) {
    if (!jobStatus || jobStatus.status === 'complete' || jobStatus.status === 'error') return null;
    const progress = Math.max(0, Math.min(100, Number(jobStatus.progress || 0)));
    const radius = 28;
    const circumference = 2 * Math.PI * radius;
    const dashOffset = circumference - (progress / 100) * circumference;
    const statusLabels: Record<string, string> = {
        queued: 'Queued',
        analyzing: 'Analyzing',
        generating_script: 'Writing Script',
        generating_images: 'Generating Images',
        animating_scenes: 'Animating Scenes',
        generating_voice: 'Generating Voice',
        generating_sfx: 'Building Sound Design',
        compositing: 'Compositing',
    };
    const currentScene = Number(jobStatus.current_scene || 0);
    const totalScenes = Number(jobStatus.total_scenes || 0);

    return (
        <div className="pointer-events-none fixed bottom-4 right-4 z-[70] w-[min(420px,calc(100vw-1.5rem))]">
            <div className="pointer-events-auto overflow-hidden rounded-[28px] border border-cyan-500/20 bg-[#090b11]/95 shadow-2xl shadow-black/50 backdrop-blur-xl">
                <div className="border-b border-white/[0.06] px-4 py-3">
                    <div className="flex items-center justify-between gap-4">
                        <div>
                            <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-cyan-300">Catalyst Render Monitor</p>
                            <h3 className="mt-1 text-base font-semibold text-white">{title}</h3>
                        </div>
                        <div className="relative h-16 w-16 shrink-0">
                            <svg className="h-16 w-16 -rotate-90" viewBox="0 0 64 64" aria-hidden="true">
                                <circle cx="32" cy="32" r={radius} fill="none" stroke="rgba(255,255,255,0.08)" strokeWidth="6" />
                                <circle
                                    cx="32"
                                    cy="32"
                                    r={radius}
                                    fill="none"
                                    stroke="url(#renderProgressGradient)"
                                    strokeWidth="6"
                                    strokeLinecap="round"
                                    strokeDasharray={circumference}
                                    strokeDashoffset={dashOffset}
                                />
                                <defs>
                                    <linearGradient id="renderProgressGradient" x1="0%" y1="0%" x2="100%" y2="100%">
                                        <stop offset="0%" stopColor="#22d3ee" />
                                        <stop offset="100%" stopColor="#a855f7" />
                                    </linearGradient>
                                </defs>
                            </svg>
                            <div className="absolute inset-0 flex items-center justify-center text-sm font-semibold text-white">
                                {progress}%
                            </div>
                        </div>
                    </div>
                    <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-gray-400">
                        <span className="rounded-full border border-white/[0.08] bg-white/[0.04] px-2.5 py-1 font-semibold uppercase tracking-[0.16em] text-gray-200">
                            {statusLabels[String(jobStatus.status || '')] || String(jobStatus.status || 'Rendering')}
                        </span>
                        {currentScene > 0 && totalScenes > 0 ? (
                            <span>Scene {currentScene} of {totalScenes}</span>
                        ) : null}
                        {jobStatus.resolution ? <span>{String(jobStatus.resolution)}</span> : null}
                    </div>
                </div>
                <div className="grid gap-4 p-4 md:grid-cols-[160px_minmax(0,1fr)]">
                    <div className="space-y-3">
                        <ProgressBar progress={progress} status={String(jobStatus.status || '')} />
                        {jobStatus.queue_position > 0 && jobStatus.status === 'queued' ? (
                            <p className="text-xs text-gray-400">
                                Queue position {jobStatus.queue_position} of {jobStatus.queue_total}
                            </p>
                        ) : null}
                        {jobStatus.current_scene && jobStatus.total_scenes ? (
                            <p className="text-xs text-gray-400">
                                Rendering scene {jobStatus.current_scene} of {jobStatus.total_scenes}
                            </p>
                        ) : null}
                    </div>
                    <div className="overflow-hidden rounded-2xl border border-white/[0.08] bg-black/40">
                        <div className="border-b border-white/[0.06] px-3 py-2">
                            <p className="text-[10px] font-semibold uppercase tracking-[0.22em] text-gray-400">Live Preview</p>
                        </div>
                        <iframe
                            title="Render progress preview"
                            className="h-56 w-full bg-black"
                            sandbox="allow-scripts allow-same-origin"
                            srcDoc={buildRenderPreviewSrcDoc(previewUrl, previewType, previewLabel)}
                        />
                    </div>
                </div>
            </div>
        </div>
    );
}
