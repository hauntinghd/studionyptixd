import { Download, Loader2, RefreshCw, X, Square } from 'lucide-react';
import { mediaUrl, type AgentJobSnapshot, type AgentJobTrack } from '../../lib/agentProduction';

export default function AgentRenderDock({
    track,
    snapshot,
    accessToken,
    onDismiss,
    onRetry,
    retrying = false,
    onCancel,
    cancelling = false,
}: {
    track: AgentJobTrack | null;
    snapshot: AgentJobSnapshot | undefined;
    accessToken?: string;
    onDismiss?: () => void;
    onRetry?: () => void;
    retrying?: boolean;
    onCancel?: () => void;
    cancelling?: boolean;
}) {
    if (!track || !snapshot) return null;

    const failed = snapshot.status === 'failed';
    const complete = snapshot.status === 'complete';
    const running = !failed && !complete && snapshot.status !== 'awaiting_approval';
    const isAnalysis = track.kind === 'competitor' || snapshot.kind === 'competitor';
    const isClipLab = track.kind === 'cliplab' || snapshot.kind === 'cliplab';
    const cliplabJobType = String(snapshot.job_type || '').toLowerCase();
    const isClipLabIngest = isClipLab && (cliplabJobType === 'cliplab_ingest' || track.job_id.startsWith('clipi_'));
    const isClipLabAnalyze = isClipLab && (cliplabJobType === 'cliplab_analyze' || track.job_id.startsWith('clipa_'));
    const isClipLabRender = isClipLab && (cliplabJobType === 'cliplab_render' || track.job_id.startsWith('clipr_'));

    if (!running && !failed && !complete) return null;

    const progress = Math.max(0, Math.min(100, Number(snapshot.progress || 0)));
    const radius = 28;
    const circumference = 2 * Math.PI * radius;
    const dashOffset = circumference - (progress / 100) * circumference;
    const rawDl = snapshot.download_url || snapshot.mp4_url || '';
    const downloadUrl =
        rawDl.startsWith('http') ? rawDl : rawDl && accessToken ? mediaUrl(rawDl, accessToken) : rawDl;
    const actualCost = snapshot.cost?.actual_usd_decimal
        || (typeof snapshot.cost?.actual_usd === 'number' ? snapshot.cost.actual_usd.toFixed(6) : '');
    const costLabel = actualCost && Number(actualCost) > 0 ? `$${actualCost}` : '';
    const spendLabel = snapshot.cost?.spend_label || 'Provider spend so far';

    return (
        <div className="pointer-events-auto fixed bottom-5 right-5 z-[70] w-[min(100vw-2rem,340px)]">
            <div
                className={`rounded-2xl border p-3 shadow-2xl shadow-black/50 backdrop-blur-md ${
                    failed
                        ? 'border-red-500/30 bg-[#110a0a]/95'
                        : complete
                          ? 'border-emerald-500/25 bg-[#09110d]/95'
                          : 'border-cyan-500/25 bg-[#090b11]/95'
                }`}
            >
                <div className="flex items-start gap-3">
                    <div className="relative h-16 w-16 shrink-0">
                        {running ? (
                            <svg className="h-16 w-16 -rotate-90" viewBox="0 0 72 72" aria-hidden>
                                <circle cx="36" cy="36" r={radius} fill="none" stroke="rgba(255,255,255,0.08)" strokeWidth="6" />
                                <circle
                                    cx="36"
                                    cy="36"
                                    r={radius}
                                    fill="none"
                                    stroke="url(#agentDockGrad)"
                                    strokeWidth="6"
                                    strokeLinecap="round"
                                    strokeDasharray={circumference}
                                    strokeDashoffset={dashOffset}
                                    className="transition-[stroke-dashoffset] duration-700 ease-out"
                                />
                                <defs>
                                    <linearGradient id="agentDockGrad" x1="0%" y1="0%" x2="100%" y2="0%">
                                        <stop offset="0%" stopColor="#22d3ee" />
                                        <stop offset="100%" stopColor="#a78bfa" />
                                    </linearGradient>
                                </defs>
                            </svg>
                        ) : (
                            <div
                                className={`flex h-16 w-16 items-center justify-center rounded-full text-lg font-bold ${
                                    failed ? 'bg-red-500/15 text-red-300' : 'bg-emerald-500/15 text-emerald-300'
                                }`}
                            >
                                {failed ? '!' : '✓'}
                            </div>
                        )}
                        {running ? (
                            <div className="absolute inset-0 flex items-center justify-center">
                                <span className="text-[11px] font-bold tabular-nums text-white">{progress}%</span>
                            </div>
                        ) : null}
                    </div>
                    <div className="min-w-0 flex-1">
                        <p
                            className={`text-[10px] font-semibold uppercase tracking-[0.18em] ${
                                failed ? 'text-red-300/90' : complete ? 'text-emerald-300/90' : 'text-cyan-300/90'
                            }`}
                        >
                            {failed ? 'Failed' : complete ? 'Complete' : isClipLab ? 'ClipLab' : isAnalysis ? 'Analysis' : 'Production'}
                        </p>
                        <p className="truncate text-sm font-semibold text-white">
                            {failed
                                ? isAnalysis ? 'Reference analysis failed' : 'Production failed'
                                : complete
                                  ? isClipLabIngest
                                      ? 'Ingest ready'
                                      : isClipLabAnalyze
                                          ? 'Clip analysis ready'
                                          : track.title || (isAnalysis ? 'Analysis ready' : isClipLabRender ? 'ClipLab clips ready' : 'Video ready')
                                  : snapshot.stage_label || track.title || 'Rendering'}
                        </p>
                        <p className="mt-0.5 line-clamp-2 text-[11px] text-gray-400">
                            {failed
                                ? snapshot.error || (isAnalysis ? 'Ask Studio Agent to re-run the reference analysis.' : 'Tap Retry to run the same brief again.')
                                : complete
                                  ? isAnalysis
                                      ? 'The pacing and blueprint signals are ready in chat.'
                                      : isClipLabIngest
                                          ? 'Source video is ingested. Send continue to analyze clips.'
                                          : isClipLabAnalyze
                                              ? 'Segments are selected. Approve/render them into 9:16 clips.'
                                              : isClipLabRender
                                                  ? 'Rendered clips and upload packages are ready.'
                                                  : 'Download your MP4 or keep chatting.'
                                  : snapshot.stage_detail
                                    || (snapshot.total_scenes
                                        ? `Scene ${snapshot.current_scene || 0}/${snapshot.total_scenes}`
                                        : 'Server-side — keep this tab open')}
                        </p>
                        {costLabel ? (
                            <p className="mt-1 text-[10px] font-medium text-cyan-200/80">
                                {spendLabel}: <span className="tabular-nums">{costLabel}</span>
                            </p>
                        ) : null}
                        <div className="mt-2 flex flex-wrap items-center gap-2">
                            {running ? (
                                <span className="inline-flex items-center gap-1.5 text-[10px] text-gray-500">
                                    <Loader2 className="h-3 w-3 animate-spin text-cyan-400" />
                                    {track.kind} · {track.job_id.slice(0, 8)}
                                </span>
                            ) : null}
                            {running && onCancel && track.kind === 'shortform' ? (
                                <button
                                    type="button"
                                    disabled={cancelling}
                                    onClick={onCancel}
                                    className="inline-flex items-center gap-1 rounded-lg border border-red-500/30 bg-red-500/10 px-2 py-1 text-[11px] font-semibold text-red-100 hover:bg-red-500/20 disabled:opacity-50"
                                    title="Stop this render at the next scene (no more provider spend)"
                                >
                                    <Square className="h-3 w-3" />
                                    {cancelling ? 'Cancelling…' : 'Cancel'}
                                </button>
                            ) : null}
                            {failed && onRetry ? (
                                <button
                                    type="button"
                                    disabled={retrying}
                                    onClick={onRetry}
                                    className="inline-flex items-center gap-1 rounded-lg border border-red-500/30 bg-red-500/10 px-2 py-1 text-[11px] font-semibold text-red-100 hover:bg-red-500/20 disabled:opacity-50"
                                >
                                    <RefreshCw className={`h-3 w-3 ${retrying ? 'animate-spin' : ''}`} />
                                    {retrying ? 'Retrying…' : 'Retry'}
                                </button>
                            ) : null}
                            {complete && downloadUrl && (!isClipLab || isClipLabRender) ? (
                                <a
                                    href={downloadUrl}
                                    download
                                    className="inline-flex items-center gap-1 rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-2 py-1 text-[11px] font-semibold text-emerald-100 hover:bg-emerald-500/20"
                                >
                                    <Download className="h-3 w-3" /> Download
                                </a>
                            ) : null}
                        </div>
                    </div>
                    {onDismiss ? (
                        <button
                            type="button"
                            onClick={onDismiss}
                            className="rounded-lg p-1 text-gray-500 hover:bg-white/5 hover:text-white"
                            title="Hide monitor"
                        >
                            <X className="h-3.5 w-3.5" />
                        </button>
                    ) : null}
                </div>
            </div>
        </div>
    );
}
