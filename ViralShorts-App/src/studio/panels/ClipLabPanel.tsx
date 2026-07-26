import { useCallback, useContext, useEffect, useState } from 'react';
import {
    Clapperboard,
    Download,
    Loader2,
    Sparkles,
    Upload,
    Youtube,
} from 'lucide-react';
import { API, AuthContext, DIRECT_API, resolveStudioUploadUrl } from '../shared';
import { ThumbProgressBar } from '../components/StudioWidgets';

type Segment = {
    start: number;
    end: number;
    confidence: number;
    virality_score: number;
    why_it_matches: string;
    hook_text: string;
    transcript_snippet: string;
};

type Job = {
    status: string;
    progress: number;
    error?: string;
    video_id?: string;
    segments?: Segment[];
    clips?: Array<{ index: number; url?: string; filename?: string; error?: string; virality_score?: number }>;
};

function commandFingerprint(value: string) {
    let hash = 2166136261;
    for (let index = 0; index < value.length; index += 1) {
        hash ^= value.charCodeAt(index);
        hash = Math.imul(hash, 16777619);
    }
    return (hash >>> 0).toString(36);
}

function acquireProductionCommand(scope: string) {
    const storageKey = `studio:cliplab:command:${commandFingerprint(scope)}`;
    let commandId = '';
    try {
        commandId = window.sessionStorage.getItem(storageKey) || '';
    } catch {
        commandId = '';
    }
    if (!commandId) {
        commandId = globalThis.crypto?.randomUUID?.()
            || `cliplab_${Date.now()}_${Math.random().toString(36).slice(2)}`;
        try {
            window.sessionStorage.setItem(storageKey, commandId);
        } catch {
            // The backend still owns idempotency for the lifetime of this call.
        }
    }
    return {
        commandId,
        release: () => {
            try {
                window.sessionStorage.removeItem(storageKey);
            } catch {
                // Storage can be unavailable in privacy-restricted browsers.
            }
        },
    };
}

export default function ClipLabPanel() {
    const { session } = useContext(AuthContext);
    const token = session?.access_token || '';
    const api = DIRECT_API || API;

    const [youtubeUrl, setYoutubeUrl] = useState('');
    const [videoId, setVideoId] = useState('');
    const [prompt, setPrompt] = useState('Find the strongest hooks — controversy, reveals, emotional peaks');
    const [ingestJobId, setIngestJobId] = useState('');
    const [analyzeJobId, setAnalyzeJobId] = useState('');
    const [renderJobId, setRenderJobId] = useState('');
    const [ingestJob, setIngestJob] = useState<Job | null>(null);
    const [analyzeJob, setAnalyzeJob] = useState<Job | null>(null);
    const [renderJob, setRenderJob] = useState<Job | null>(null);
    const [selected, setSelected] = useState<Set<number>>(new Set());
    const [uploading, setUploading] = useState(false);
    const [busy, setBusy] = useState('');
    const [error, setError] = useState('');
    const [registry, setRegistry] = useState<Record<string, unknown> | null>(null);

    const pollJob = useCallback(async (jobId: string, setter: (j: Job) => void) => {
        if (!token || !jobId) return;
        let cancelled = false;
        const tick = async () => {
            try {
                const r = await fetch(`${api}/api/status/${jobId}`, {
                    headers: { Authorization: `Bearer ${token}` },
                });
                const data = await r.json();
                if (cancelled) return;
                setter(data);
                if (data.status === 'complete' || data.status === 'ready' || data.status === 'error') return;
                setTimeout(tick, 2000);
            } catch {
                if (!cancelled) setTimeout(tick, 3000);
            }
        };
        tick();
        return () => { cancelled = true; };
    }, [token, api]);

    useEffect(() => {
        if (!token) return;
        fetch(`${api}/api/cliplab/status`, { headers: { Authorization: `Bearer ${token}` } })
            .then((r) => r.json())
            .then(setRegistry)
            .catch(() => setRegistry(null));
    }, [token, api]);

    useEffect(() => {
        if (ingestJobId) pollJob(ingestJobId, setIngestJob);
    }, [ingestJobId, pollJob]);

    useEffect(() => {
        if (analyzeJobId) pollJob(analyzeJobId, setAnalyzeJob);
    }, [analyzeJobId, pollJob]);

    useEffect(() => {
        if (renderJobId) pollJob(renderJobId, setRenderJob);
    }, [renderJobId, pollJob]);

    useEffect(() => {
        if (ingestJob?.video_id) setVideoId(ingestJob.video_id);
    }, [ingestJob?.video_id]);

    useEffect(() => {
        if (analyzeJob?.segments?.length) {
            setSelected(new Set(analyzeJob.segments.map((_, i) => i).slice(0, 5)));
        }
    }, [analyzeJob?.segments]);

    const onUpload = async (file: File) => {
        if (!token) return;
        const command = acquireProductionCommand(
            `upload:${session?.user?.id || ''}:${file.name}:${file.size}:${file.lastModified}`,
        );
        setUploading(true);
        setError('');
        try {
            const fd = new FormData();
            fd.append('file', file);
            const r = await fetch(resolveStudioUploadUrl('/api/cliplab/ingest/upload'), {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${token}`,
                    'X-Idempotency-Key': command.commandId,
                },
                body: fd,
            });
            const data = await r.json();
            if (!r.ok) {
                command.release();
                throw new Error(data.detail || 'Upload failed');
            }
            setIngestJobId(String(data.job_id || ''));
            setVideoId(String(data.video_id || ''));
            setIngestJob({ status: 'queued', progress: 0 });
            command.release();
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : 'Upload failed');
        } finally {
            setUploading(false);
        }
    };

    const ingestYoutube = async () => {
        if (!token || !youtubeUrl.trim()) return;
        const command = acquireProductionCommand(
            `youtube:${session?.user?.id || ''}:${youtubeUrl.trim()}`,
        );
        setBusy('ingest');
        setError('');
        try {
            const r = await fetch(`${api}/api/cliplab/ingest/youtube`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${token}`,
                    'Content-Type': 'application/json',
                    'X-Idempotency-Key': command.commandId,
                },
                body: JSON.stringify({ youtube_url: youtubeUrl.trim() }),
            });
            const data = await r.json();
            if (!r.ok) {
                command.release();
                throw new Error(data.detail || 'YouTube ingest failed');
            }
            setIngestJobId(String(data.job_id || ''));
            setVideoId(String(data.video_id || ''));
            setIngestJob({ status: 'queued', progress: 0 });
            command.release();
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : 'Ingest failed');
        } finally {
            setBusy('');
        }
    };

    const analyze = async () => {
        if (!token || !videoId) {
            setError('Ingest a video first');
            return;
        }
        const command = acquireProductionCommand(
            `analyze:${session?.user?.id || ''}:${videoId}:${prompt.trim()}:12`,
        );
        setBusy('analyze');
        setError('');
        try {
            const r = await fetch(`${api}/api/cliplab/analyze`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${token}`,
                    'Content-Type': 'application/json',
                    'X-Idempotency-Key': command.commandId,
                },
                body: JSON.stringify({ video_id: videoId, prompt: prompt.trim(), max_segments: 12 }),
            });
            const data = await r.json();
            if (!r.ok) {
                command.release();
                throw new Error(data.detail || 'Analyze failed');
            }
            setAnalyzeJobId(String(data.job_id || ''));
            setAnalyzeJob({ status: 'queued', progress: 0 });
            command.release();
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : 'Analyze failed');
        } finally {
            setBusy('');
        }
    };

    const renderSelected = async () => {
        if (!token || !videoId || selected.size === 0) return;
        const selectedIndices = Array.from(selected).sort((left, right) => left - right);
        const command = acquireProductionCommand(
            `render:${session?.user?.id || ''}:${videoId}:${analyzeJobId}:${selectedIndices.join(',')}`,
        );
        setBusy('render');
        setError('');
        try {
            const r = await fetch(`${api}/api/cliplab/render`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${token}`,
                    'Content-Type': 'application/json',
                    'X-Idempotency-Key': command.commandId,
                },
                body: JSON.stringify({
                    video_id: videoId,
                    prompt_run_id: analyzeJobId,
                    segment_indices: selectedIndices,
                    burn_captions: true,
                }),
            });
            const data = await r.json();
            if (!r.ok) {
                command.release();
                throw new Error(data.detail || 'Render failed');
            }
            setRenderJobId(String(data.job_id || ''));
            setRenderJob({ status: 'queued', progress: 0 });
            command.release();
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : 'Render failed');
        } finally {
            setBusy('');
        }
    };

    const toggleSeg = (i: number) => {
        setSelected((prev) => {
            const next = new Set(prev);
            if (next.has(i)) next.delete(i);
            else next.add(i);
            return next;
        });
    };

    return (
        <div className="mx-auto max-w-5xl space-y-8 pb-16">
            <header>
                <div className="flex items-center gap-3">
                    <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-rose-500/15 text-rose-300">
                        <Clapperboard className="h-5 w-5" />
                    </div>
                    <div>
                        <h1 className="text-2xl font-bold text-white">ClipLab</h1>
                        <p className="text-sm text-gray-500">
                            Long-form → ranked 9:16 shorts. Face-track reframe + LLM virality scoring. RunPod weights plug in when trained.
                        </p>
                    </div>
                </div>
                {registry && (
                    <p className="mt-2 text-xs text-gray-600">
                        Virality: {String(registry.virality_backend || 'local_llm')}
                        {registry.virality_weights_ready ? ' · weights ready' : ''}
                        {' · '}
                        Reframe: {String(registry.reframe_backend || 'opencv_face')}
                        {registry.reframe_weights_ready ? ' · weights ready' : ''}
                    </p>
                )}
            </header>

            <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-5">
                <h2 className="text-sm font-semibold uppercase tracking-wider text-rose-300">1 · Ingest</h2>
                <p className="mt-1 text-xs text-gray-500">1 credit per minute of source video.</p>
                <label className="mt-4 flex cursor-pointer items-center justify-center gap-2 rounded-xl border border-dashed border-white/15 py-8 text-sm text-gray-400 hover:border-rose-500/40">
                    <input
                        type="file"
                        accept="video/*"
                        className="hidden"
                        onChange={(e) => {
                            const f = e.target.files?.[0];
                            if (f) onUpload(f);
                        }}
                    />
                    {uploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Upload className="h-4 w-4" />}
                    {uploading ? 'Uploading…' : 'Upload long-form video'}
                </label>
                <div className="mt-4 flex gap-2">
                    <div className="relative flex-1">
                        <Youtube className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-red-400/80" />
                        <input
                            value={youtubeUrl}
                            onChange={(e) => setYoutubeUrl(e.target.value)}
                            placeholder="Or paste YouTube URL"
                            className="w-full rounded-lg border border-white/10 bg-black/30 py-2 pl-10 pr-3 text-sm text-white"
                        />
                    </div>
                    <button
                        type="button"
                        onClick={ingestYoutube}
                        disabled={busy === 'ingest' || !youtubeUrl.trim()}
                        className="rounded-lg bg-rose-600 px-4 py-2 text-sm font-semibold text-white hover:bg-rose-500 disabled:opacity-40"
                    >
                        Pull
                    </button>
                </div>
                {ingestJob && (
                    <div className="mt-4">
                        <ThumbProgressBar progress={ingestJob.progress} status={ingestJob.status} />
                        {ingestJob.video_id && (
                            <p className="mt-1 text-xs text-gray-500">Video ID: {ingestJob.video_id}</p>
                        )}
                    </div>
                )}
            </section>

            <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-5">
                <h2 className="text-sm font-semibold uppercase tracking-wider text-rose-300">2 · Prompt</h2>
                <textarea
                    value={prompt}
                    onChange={(e) => setPrompt(e.target.value)}
                    rows={3}
                    className="mt-3 w-full rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
                />
                <button
                    type="button"
                    onClick={analyze}
                    disabled={busy === 'analyze' || !videoId}
                    className="mt-4 flex items-center gap-2 rounded-xl bg-gradient-to-r from-rose-600 to-orange-600 px-5 py-2.5 text-sm font-bold text-white disabled:opacity-50"
                >
                    {busy === 'analyze' ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
                    Find viral segments
                </button>
                {analyzeJob && (
                    <div className="mt-4">
                        <ThumbProgressBar progress={analyzeJob.progress} status={analyzeJob.status} />
                    </div>
                )}
                {analyzeJob?.segments && analyzeJob.segments.length > 0 && (
                    <ul className="mt-4 space-y-2">
                        {analyzeJob.segments.map((seg, i) => (
                            <li key={`${seg.start}-${i}`}>
                                <button
                                    type="button"
                                    onClick={() => toggleSeg(i)}
                                    className={`w-full rounded-lg border p-3 text-left text-xs transition ${
                                        selected.has(i)
                                            ? 'border-rose-500 bg-rose-500/10'
                                            : 'border-white/10 hover:border-white/20'
                                    }`}
                                >
                                    <span className="font-mono text-rose-300">
                                        {seg.start.toFixed(0)}s–{seg.end.toFixed(0)}s
                                    </span>
                                    <span className="ml-2 text-gray-500">score {seg.virality_score}</span>
                                    <p className="mt-1 text-gray-300">{seg.why_it_matches || seg.transcript_snippet}</p>
                                </button>
                            </li>
                        ))}
                    </ul>
                )}
            </section>

            <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-5">
                <h2 className="text-sm font-semibold uppercase tracking-wider text-rose-300">3 · Render 9:16</h2>
                <p className="mt-1 text-xs text-gray-500">Face-track crop + karaoke captions.</p>
                <button
                    type="button"
                    onClick={renderSelected}
                    disabled={busy === 'render' || selected.size === 0}
                    className="mt-4 rounded-xl bg-white/10 px-5 py-2.5 text-sm font-semibold text-white hover:bg-white/15 disabled:opacity-50"
                >
                    Render {selected.size} clip{selected.size === 1 ? '' : 's'}
                </button>
                {renderJob && (
                    <div className="mt-4">
                        <ThumbProgressBar progress={renderJob.progress} status={renderJob.status} />
                    </div>
                )}
                {renderJob?.clips && renderJob.clips.length > 0 && (
                    <ul className="mt-4 space-y-2">
                        {renderJob.clips.map((c) => (
                            <li key={c.index} className="flex items-center justify-between rounded-lg border border-white/10 p-3 text-sm">
                                <span className="text-gray-300">Clip #{c.index + 1}</span>
                                {c.url ? (
                                    <a
                                        href={`${api}${c.url}`}
                                        download
                                        className="inline-flex items-center gap-1 text-rose-300 hover:text-rose-200"
                                    >
                                        <Download className="h-4 w-4" /> MP4
                                    </a>
                                ) : (
                                    <span className="text-red-400">{c.error || 'Failed'}</span>
                                )}
                            </li>
                        ))}
                    </ul>
                )}
            </section>

            {error && <p className="text-sm text-red-400">{error}</p>}
        </div>
    );
}
