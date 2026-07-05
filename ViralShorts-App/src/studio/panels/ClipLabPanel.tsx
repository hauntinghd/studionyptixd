import { useCallback, useContext, useEffect, useState } from 'react';
import {
    Clapperboard,
    Download,
    Loader2,
    Sparkles,
    Upload,
    Youtube,
} from 'lucide-react';
import { AuthContext, resolveStudioBackendUrl } from '../shared';
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
    remix?: { url?: string; filename?: string; style_preset?: string; caption_style?: string; edit_intensity?: string; background_mode?: string };
};

export default function ClipLabPanel() {
    const { session } = useContext(AuthContext);
    const token = session?.access_token || '';
    const backendUrl = useCallback((path: string) => resolveStudioBackendUrl(path), []);

    const [youtubeUrl, setYoutubeUrl] = useState('');
    const [videoId, setVideoId] = useState('');
    const [prompt, setPrompt] = useState('Find the strongest hooks — controversy, reveals, emotional peaks');
    const [ingestJobId, setIngestJobId] = useState('');
    const [analyzeJobId, setAnalyzeJobId] = useState('');
    const [renderJobId, setRenderJobId] = useState('');
    const [remixJobId, setRemixJobId] = useState('');
    const [ingestJob, setIngestJob] = useState<Job | null>(null);
    const [analyzeJob, setAnalyzeJob] = useState<Job | null>(null);
    const [renderJob, setRenderJob] = useState<Job | null>(null);
    const [remixJob, setRemixJob] = useState<Job | null>(null);
    const [selected, setSelected] = useState<Set<number>>(new Set());
    const [uploading, setUploading] = useState(false);
    const [busy, setBusy] = useState('');
    const [error, setError] = useState('');
    const [registry, setRegistry] = useState<Record<string, unknown> | null>(null);
    const [remixStyle, setRemixStyle] = useState('clean_viral');
    const [remixCaptionStyle, setRemixCaptionStyle] = useState('bold');
    const [remixIntensity, setRemixIntensity] = useState('medium');

    const pollJob = useCallback(async (jobId: string, setter: (j: Job) => void) => {
        if (!token || !jobId) return;
        let cancelled = false;
        const tick = async () => {
            try {
                const r = await fetch(backendUrl(`/api/status/${jobId}`), {
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
    }, [token, backendUrl]);

    useEffect(() => {
        if (!token) return;
        fetch(backendUrl('/api/cliplab/status'), { headers: { Authorization: `Bearer ${token}` } })
            .then((r) => r.json())
            .then(setRegistry)
            .catch(() => setRegistry(null));
    }, [token, backendUrl]);

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
        if (remixJobId) pollJob(remixJobId, setRemixJob);
    }, [remixJobId, pollJob]);

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
        setUploading(true);
        setError('');
        try {
            const fd = new FormData();
            fd.append('file', file);
            const r = await fetch(backendUrl('/api/cliplab/ingest/upload'), {
                method: 'POST',
                headers: { Authorization: `Bearer ${token}` },
                body: fd,
            });
            const data = await r.json();
            if (!r.ok) throw new Error(data.detail || 'Upload failed');
            setIngestJobId(String(data.job_id || ''));
            setVideoId(String(data.video_id || ''));
            setIngestJob({ status: 'queued', progress: 0 });
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : 'Upload failed');
        } finally {
            setUploading(false);
        }
    };

    const ingestYoutube = async () => {
        if (!token || !youtubeUrl.trim()) return;
        setBusy('ingest');
        setError('');
        try {
            const r = await fetch(backendUrl('/api/cliplab/ingest/youtube'), {
                method: 'POST',
                headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({ youtube_url: youtubeUrl.trim() }),
            });
            const data = await r.json();
            if (!r.ok) throw new Error(data.detail || 'YouTube ingest failed');
            setIngestJobId(String(data.job_id || ''));
            setVideoId(String(data.video_id || ''));
            setIngestJob({ status: 'queued', progress: 0 });
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
        setBusy('analyze');
        setError('');
        try {
            const r = await fetch(backendUrl('/api/cliplab/analyze'), {
                method: 'POST',
                headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({ video_id: videoId, prompt: prompt.trim(), max_segments: 12 }),
            });
            const data = await r.json();
            if (!r.ok) throw new Error(data.detail || 'Analyze failed');
            setAnalyzeJobId(String(data.job_id || ''));
            setAnalyzeJob({ status: 'queued', progress: 0 });
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : 'Analyze failed');
        } finally {
            setBusy('');
        }
    };

    const renderSelected = async () => {
        if (!token || !videoId || selected.size === 0) return;
        setBusy('render');
        setError('');
        try {
            const r = await fetch(backendUrl('/api/cliplab/render'), {
                method: 'POST',
                headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    video_id: videoId,
                    prompt_run_id: analyzeJobId,
                    segment_indices: Array.from(selected),
                    burn_captions: true,
                }),
            });
            const data = await r.json();
            if (!r.ok) throw new Error(data.detail || 'Render failed');
            setRenderJobId(String(data.job_id || ''));
            setRenderJob({ status: 'queued', progress: 0 });
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : 'Render failed');
        } finally {
            setBusy('');
        }
    };

    const remixUploadedShort = async () => {
        if (!token || !videoId) {
            setError('Upload or ingest a short first');
            return;
        }
        setBusy('remix');
        setError('');
        try {
            const r = await fetch(backendUrl('/api/cliplab/remix'), {
                method: 'POST',
                headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    video_id: videoId,
                    style_preset: remixStyle,
                    caption_style: remixCaptionStyle,
                    edit_intensity: remixIntensity,
                    background_mode: 'blur',
                    burn_captions: true,
                    notes: 'Internal Remix Lab v1 test from ClipLab panel.',
                }),
            });
            const data = await r.json();
            if (!r.ok) throw new Error(data.detail || 'Remix failed');
            setRemixJobId(String(data.job_id || ''));
            setRemixJob({ status: 'queued', progress: 0 });
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : 'Remix failed');
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
                        {analyzeJob.error && (
                            <p className="mt-2 rounded-lg border border-red-400/20 bg-red-500/10 px-3 py-2 text-xs text-red-200">
                                {analyzeJob.error}
                            </p>
                        )}
                        {analyzeJob.status === 'complete' && (!analyzeJob.segments || analyzeJob.segments.length === 0) && (
                            <p className="mt-2 rounded-lg border border-yellow-400/20 bg-yellow-500/10 px-3 py-2 text-xs text-yellow-100">
                                No viral segments were returned. Try a clearer prompt or upload a video with speech/audio.
                            </p>
                        )}
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
                                        href={backendUrl(c.url || '')}
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

            <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-5">
                <h2 className="text-sm font-semibold uppercase tracking-wider text-cyan-300">Remix Lab - Internal</h2>
                <p className="mt-1 text-xs text-gray-500">
                    Upload an already-cut 9:16 short, then polish it with blurred background, captions, color, and Catalyst-ready edit metadata.
                </p>
                <div className="mt-4 grid gap-3 sm:grid-cols-3">
                    <label className="block text-xs font-semibold uppercase tracking-[0.16em] text-gray-500">
                        Style
                        <select
                            value={remixStyle}
                            onChange={(e) => setRemixStyle(e.target.value)}
                            className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm normal-case tracking-normal text-white"
                        >
                            <option value="clean_viral">Clean viral</option>
                            <option value="empire">Empire Magnates</option>
                            <option value="documentary">Documentary</option>
                            <option value="streamer">Streamer</option>
                            <option value="high_energy">High energy</option>
                        </select>
                    </label>
                    <label className="block text-xs font-semibold uppercase tracking-[0.16em] text-gray-500">
                        Captions
                        <select
                            value={remixCaptionStyle}
                            onChange={(e) => setRemixCaptionStyle(e.target.value)}
                            className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm normal-case tracking-normal text-white"
                        >
                            <option value="bold">Bold</option>
                            <option value="empire">Empire</option>
                            <option value="minimal">Minimal</option>
                        </select>
                    </label>
                    <label className="block text-xs font-semibold uppercase tracking-[0.16em] text-gray-500">
                        Intensity
                        <select
                            value={remixIntensity}
                            onChange={(e) => setRemixIntensity(e.target.value)}
                            className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm normal-case tracking-normal text-white"
                        >
                            <option value="low">Low</option>
                            <option value="medium">Medium</option>
                            <option value="high">High</option>
                        </select>
                    </label>
                </div>
                <button
                    type="button"
                    onClick={remixUploadedShort}
                    disabled={busy === 'remix' || !videoId}
                    className="mt-4 inline-flex items-center gap-2 rounded-xl bg-cyan-500/15 px-5 py-2.5 text-sm font-bold text-cyan-200 ring-1 ring-cyan-400/30 hover:bg-cyan-500/20 disabled:opacity-50"
                >
                    {busy === 'remix' ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
                    Remix uploaded short
                </button>
                {remixJob && (
                    <div className="mt-4">
                        <ThumbProgressBar progress={remixJob.progress} status={remixJob.status} />
                    </div>
                )}
                {remixJob?.remix?.url && (
                    <div className="mt-4 flex items-center justify-between rounded-lg border border-cyan-400/20 bg-cyan-400/5 p-3 text-sm">
                        <span className="text-gray-300">Remixed short ready</span>
                        <a
                            href={backendUrl(remixJob.remix.url)}
                            download
                            className="inline-flex items-center gap-1 text-cyan-200 hover:text-white"
                        >
                            <Download className="h-4 w-4" /> MP4
                        </a>
                    </div>
                )}
            </section>

            {error && <p className="text-sm text-red-400">{error}</p>}
        </div>
    );
}
