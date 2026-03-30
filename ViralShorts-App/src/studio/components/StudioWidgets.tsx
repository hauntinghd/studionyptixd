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
