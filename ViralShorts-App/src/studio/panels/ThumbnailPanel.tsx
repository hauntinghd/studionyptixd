import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import {
    Check,
    Download,
    ExternalLink,
    ImageIcon,
    Loader2,
    Sparkles,
    Upload,
    Youtube,
} from 'lucide-react';
import { API, AuthContext, DIRECT_API } from '../shared';
import { FeedbackWidget, ThumbProgressBar } from '../components/StudioWidgets';
import { downloadStudioAsset } from '../lib/agentProduction';
import { useAuthenticatedMediaUrl } from '../hooks/useAuthenticatedMedia';

type RefVideo = {
    video_id: string;
    title: string;
    views: number;
    thumbnail_url: string;
    watch_url: string;
};

type ThumbModel = { id: string; label: string; credits: number };

type MyChannel = {
    channel_id: string;
    title: string;
    packaging_learnings: string[];
    title_pattern_hints: string[];
};

type VisionAnalysis = {
    composition?: string;
    color_palette?: string;
    text_style?: string;
    emotional_hook?: string;
    generation_directive?: string;
    patterns?: string[];
    vision_score?: number;
    reference_scores?: Array<{ index: number; vision_score: number; ctr_score?: number; score_reason?: string }>;
};

type AbScoring = {
    picked?: string;
    variant_a_score?: number;
    variant_b_score?: number;
    vision_score?: number;
};

const MAX_REFERENCE_THUMBS = 4;

type JobStatus = {
    status: string;
    progress: number;
    output_url?: string;
    error?: string;
    ai_analysis?: {
        title_text?: string;
        style_notes?: string;
        patterns?: string[];
        vision?: VisionAnalysis;
        catalyst_channel?: string;
        ab_scoring?: AbScoring;
    };
    credit_cost?: number;
};

export default function ThumbnailPanel() {
    const { session } = useContext(AuthContext);
    const token = session?.access_token || '';
    const api = DIRECT_API || API;

    const [videoTitle, setVideoTitle] = useState('');
    const [topic, setTopic] = useState('');
    const [creatorUrl, setCreatorUrl] = useState('');
    const [galleryLoading, setGalleryLoading] = useState(false);
    const [galleryError, setGalleryError] = useState('');
    const [channelTitle, setChannelTitle] = useState('');
    const [refVideos, setRefVideos] = useState<RefVideo[]>([]);
    const [selectedRefs, setSelectedRefs] = useState<RefVideo[]>([]);

    const [myChannels, setMyChannels] = useState<MyChannel[]>([]);
    const [channelId, setChannelId] = useState('');

    const [uploadInfo, setUploadInfo] = useState<{ upload_id: string; filename: string; duration_label: string; size_mb: number } | null>(null);
    const [framePreviewUrl, setFramePreviewUrl] = useState('');
    const [framePct, setFramePct] = useState(12);
    const [uploading, setUploading] = useState(false);
    const videoInputRef = useRef<HTMLInputElement>(null);

    const [models, setModels] = useState<ThumbModel[]>([]);
    const [imageModel, setImageModel] = useState('seedream45');

    const [jobId, setJobId] = useState('');
    const [job, setJob] = useState<JobStatus | null>(null);
    const [generating, setGenerating] = useState(false);
    const [genError, setGenError] = useState('');
    const [downloadBusy, setDownloadBusy] = useState(false);
    const [downloadError, setDownloadError] = useState('');
    const generationIdempotencyRef = useRef('');

    const selectedCredits = useMemo(
        () => models.find((m) => m.id === imageModel)?.credits ?? 5,
        [models, imageModel],
    );

    const extractFrame = useCallback(async (uploadId: string, pct: number) => {
        if (!token || !uploadId) return;
        try {
            const r = await fetch(
                `${api}/api/thumbnails/extract-frame?upload_id=${encodeURIComponent(uploadId)}&pct=${pct / 100}`,
                { method: 'POST', headers: { Authorization: `Bearer ${token}` } },
            );
            const data = await r.json();
            if (r.ok && data.preview_url) {
                setFramePreviewUrl(`${api}${data.preview_url}?t=${Date.now()}`);
            }
        } catch { /* optional */ }
    }, [token, api]);

    useEffect(() => {
        if (!token) return;
        fetch(`${api}/api/thumbnails/models`, {
            headers: { Authorization: `Bearer ${token}` },
        })
            .then((r) => r.json())
            .then((d) => {
                const list = Array.isArray(d.models) ? d.models : [];
                setModels(list);
                const seedream = list.find((m: ThumbModel) => m.id.includes('seedream'));
                setImageModel(seedream?.id || list[0]?.id || 'seedream45');
            })
            .catch(() => {
                setModels([{ id: 'seedream45', label: 'Seedream 4.5', credits: 5 }]);
            });
        fetch(`${api}/api/thumbnails/my-channels`, {
            headers: { Authorization: `Bearer ${token}` },
        })
            .then((r) => r.json())
            .then((d) => {
                const list = Array.isArray(d.channels) ? d.channels : [];
                setMyChannels(list);
                if (list[0]?.channel_id) setChannelId(list[0].channel_id);
            })
            .catch(() => setMyChannels([]));
    }, [token, api]);

    useEffect(() => {
        if (!jobId || !token) return;
        let cancelled = false;
        const poll = async () => {
            try {
                const r = await fetch(`${api}/api/status/${jobId}`, {
                    headers: { Authorization: `Bearer ${token}` },
                });
                const data = await r.json();
                if (cancelled) return;
                setJob(data);
                if (data.status === 'complete' || data.status === 'error') {
                    setGenerating(false);
                    return;
                }
                setTimeout(poll, 2000);
            } catch {
                if (!cancelled) setTimeout(poll, 3000);
            }
        };
        poll();
        return () => { cancelled = true; };
    }, [jobId, token, api]);

    const loadGallery = useCallback(async () => {
        const q = creatorUrl.trim();
        if (!q || !token) return;
        setGalleryLoading(true);
        setGalleryError('');
        try {
            const r = await fetch(
                `${api}/api/thumbnails/creator-gallery?url=${encodeURIComponent(q)}&max_results=36`,
                { headers: { Authorization: `Bearer ${token}` } },
            );
            const data = await r.json();
            if (!r.ok) throw new Error(data.detail || 'Could not load channel');
            setChannelTitle(String(data.channel_title || ''));
            setRefVideos(Array.isArray(data.videos) ? data.videos : []);
            setSelectedRefs([]);
        } catch (e: unknown) {
            setGalleryError(e instanceof Error ? e.message : 'Load failed');
            setRefVideos([]);
        } finally {
            setGalleryLoading(false);
        }
    }, [creatorUrl, token, api]);

    const toggleRef = (v: RefVideo) => {
        setSelectedRefs((prev) => {
            const exists = prev.some((x) => x.video_id === v.video_id);
            if (exists) return prev.filter((x) => x.video_id !== v.video_id);
            if (prev.length >= MAX_REFERENCE_THUMBS) return prev;
            return [...prev, v];
        });
    };

    const onVideoUpload = async (file: File) => {
        if (!token) return;
        setUploading(true);
        setUploadInfo(null);
        setFramePreviewUrl('');
        try {
            const fd = new FormData();
            fd.append('file', file);
            const r = await fetch(`${api}/api/thumbnails/upload-video`, {
                method: 'POST',
                headers: { Authorization: `Bearer ${token}` },
                body: fd,
            });
            const data = await r.json();
            if (!r.ok) throw new Error(data.detail || 'Upload failed');
            setUploadInfo(data);
            if (!videoTitle && file.name) {
                setVideoTitle(file.name.replace(/\.[^.]+$/, '').replace(/[_-]+/g, ' '));
            }
            await extractFrame(data.upload_id, framePct);
        } catch (e: unknown) {
            setGenError(e instanceof Error ? e.message : 'Upload failed');
        } finally {
            setUploading(false);
        }
    };

    const generate = async () => {
        if (!token) return;
        const desc = [videoTitle.trim(), topic.trim()].filter(Boolean).join(' — ');
        if (!desc) {
            setGenError('Add a video title or topic first');
            return;
        }
        setGenerating(true);
        setGenError('');
        setJob(null);
        setJobId('');

        const refAnalysis = selectedRefs.length
            ? selectedRefs.map((v) => `"${v.title}" (${v.views.toLocaleString()} views)`).join('\n')
            : '';

        const body = {
            mode: selectedRefs.length ? 'screenshot_analysis' : 'describe',
            description: desc,
            video_title: videoTitle.trim(),
            screenshot_description: refAnalysis || topic.trim(),
            reference_thumbnail_urls: selectedRefs.map((v) => v.thumbnail_url),
            reference_creator: channelTitle || creatorUrl.trim(),
            image_model: imageModel,
            video_upload_id: uploadInfo?.upload_id || '',
            channel_id: channelId,
            frame_at_pct: framePct / 100,
        };

        let receivedHttpResponse = false;
        try {
            const commandId = generationIdempotencyRef.current || crypto.randomUUID();
            generationIdempotencyRef.current = commandId;
            const r = await fetch(`${api}/api/thumbnails/generate`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${token}`,
                    'Content-Type': 'application/json',
                    'X-Idempotency-Key': commandId,
                },
                body: JSON.stringify(body),
            });
            receivedHttpResponse = true;
            const data = await r.json();
            if (!r.ok) {
                generationIdempotencyRef.current = '';
                throw new Error(data.detail || 'Generate failed');
            }
            generationIdempotencyRef.current = '';
            setJobId(String(data.job_id || ''));
            setJob({ status: 'queued', progress: 0, credit_cost: data.credit_cost });
        } catch (e: unknown) {
            // Preserve the command key only when no HTTP response arrived, so
            // a network retry cannot create a second billable job.
            if (receivedHttpResponse) generationIdempotencyRef.current = '';
            setGenError(e instanceof Error ? e.message : 'Generate failed');
            setGenerating(false);
        }
    };

    const outputUrl = job?.output_url ? `${api}${job.output_url}` : '';
    const outputMedia = useAuthenticatedMediaUrl(outputUrl, token, Boolean(outputUrl));
    const framePreviewMedia = useAuthenticatedMediaUrl(framePreviewUrl, token, Boolean(framePreviewUrl));
    const selectedChannel = myChannels.find((c) => c.channel_id === channelId);
    const vision = job?.ai_analysis?.vision;
    const abScoring = job?.ai_analysis?.ab_scoring;

    return (
        <div className="mx-auto max-w-5xl space-y-8 pb-16">
            <header>
                <div className="flex items-center gap-3">
                    <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-violet-500/15 text-violet-300">
                        <ImageIcon className="h-5 w-5" />
                    </div>
                    <div>
                        <h1 className="text-2xl font-bold text-white">ThumbLab</h1>
                        <p className="text-sm text-gray-500">
                            V2 — vision analysis on reference thumbs, Catalyst packaging, video frame → Seedream edit.
                        </p>
                    </div>
                </div>
            </header>

            {myChannels.length > 0 && (
                <section className="rounded-2xl border border-emerald-500/20 bg-emerald-500/5 p-4">
                    <label className="block text-xs font-medium uppercase tracking-wider text-emerald-300">
                        Your channel (Catalyst packaging)
                    </label>
                    <select
                        value={channelId}
                        onChange={(e) => setChannelId(e.target.value)}
                        className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white"
                    >
                        {myChannels.map((c) => (
                            <option key={c.channel_id} value={c.channel_id}>{c.title}</option>
                        ))}
                    </select>
                    {selectedChannel && selectedChannel.packaging_learnings.length > 0 && (
                        <ul className="mt-2 space-y-1 text-xs text-gray-400">
                            {selectedChannel.packaging_learnings.slice(0, 3).map((p) => (
                                <li key={p}>· {p}</li>
                            ))}
                        </ul>
                    )}
                </section>
            )}

            <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-5">
                <h2 className="text-sm font-semibold uppercase tracking-wider text-violet-300">1 · Your upload</h2>
                <p className="mt-1 text-xs text-gray-500">Any length. We grab a hero frame for Seedream reference.</p>
                <input
                    ref={videoInputRef}
                    type="file"
                    accept="video/*"
                    className="hidden"
                    onChange={(e) => {
                        const f = e.target.files?.[0];
                        if (f) onVideoUpload(f);
                    }}
                />
                <button
                    type="button"
                    onClick={() => videoInputRef.current?.click()}
                    disabled={uploading}
                    className="mt-4 flex w-full items-center justify-center gap-2 rounded-xl border border-dashed border-white/15 bg-black/20 py-8 text-sm text-gray-400 transition hover:border-violet-500/40 hover:text-white"
                >
                    {uploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Upload className="h-4 w-4" />}
                    {uploading ? 'Uploading…' : uploadInfo ? `${uploadInfo.filename} · ${uploadInfo.duration_label} · ${uploadInfo.size_mb} MB` : 'Drop or pick your video file'}
                </button>
                {uploadInfo && (
                    <div className="mt-4 flex flex-col gap-3 sm:flex-row sm:items-end">
                        <label className="flex-1 text-xs text-gray-500">
                            Frame grab point ({framePct}% into video)
                            <input
                                type="range"
                                min={1}
                                max={90}
                                value={framePct}
                                onChange={(e) => setFramePct(Number(e.target.value))}
                                onMouseUp={() => extractFrame(uploadInfo.upload_id, framePct)}
                                onTouchEnd={() => extractFrame(uploadInfo.upload_id, framePct)}
                                className="mt-1 w-full"
                            />
                        </label>
                    </div>
                )}
                {framePreviewUrl && framePreviewMedia.url && (
                    <img src={framePreviewMedia.url} alt="Video frame" className="mt-3 max-h-40 rounded-lg border border-white/10 object-cover" />
                )}
                <div className="mt-4 grid gap-3 sm:grid-cols-2">
                    <label className="block">
                        <span className="text-xs text-gray-500">Video title (YouTube)</span>
                        <input
                            value={videoTitle}
                            onChange={(e) => setVideoTitle(e.target.value)}
                            placeholder="March 19, 1997: When Bre-X Lost $6 Billion"
                            className="mt-1 w-full rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
                        />
                    </label>
                    <label className="block">
                        <span className="text-xs text-gray-500">Hook / angle</span>
                        <input
                            value={topic}
                            onChange={(e) => setTopic(e.target.value)}
                            placeholder="fugitive CEO, regulator failure, offshore shell"
                            className="mt-1 w-full rounded-lg border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
                        />
                    </label>
                </div>
            </section>

            <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-5">
                <h2 className="text-sm font-semibold uppercase tracking-wider text-violet-300">2 · Creator reference</h2>
                <p className="mt-1 text-xs text-gray-500">
                    Vision pass scores each pick (max {MAX_REFERENCE_THUMBS}). A/B prompt scoring picks the stronger packaging direction.
                </p>
                {selectedRefs.length > 0 && (
                    <p className="mt-2 text-xs text-violet-300/80">{selectedRefs.length}/{MAX_REFERENCE_THUMBS} selected</p>
                )}
                <div className="mt-4 flex gap-2">
                    <div className="relative flex-1">
                        <Youtube className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-red-400/80" />
                        <input
                            value={creatorUrl}
                            onChange={(e) => setCreatorUrl(e.target.value)}
                            placeholder="https://youtube.com/@MagnatesMedia"
                            className="w-full rounded-lg border border-white/10 bg-black/30 py-2 pl-10 pr-3 text-sm text-white"
                        />
                    </div>
                    <button
                        type="button"
                        onClick={loadGallery}
                        disabled={galleryLoading || !creatorUrl.trim()}
                        className="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-500 disabled:opacity-40"
                    >
                        {galleryLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : 'Load'}
                    </button>
                </div>
                {galleryError && <p className="mt-2 text-xs text-red-400">{galleryError}</p>}
                {refVideos.length > 0 && (
                    <div className="mt-4 grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-4">
                        {refVideos.map((v) => {
                            const picked = selectedRefs.some((x) => x.video_id === v.video_id);
                            return (
                                <button
                                    key={v.video_id}
                                    type="button"
                                    onClick={() => toggleRef(v)}
                                    className={`relative overflow-hidden rounded-lg border text-left transition ${
                                        picked ? 'border-violet-500 ring-2 ring-violet-500/40' : 'border-white/10 hover:border-white/25'
                                    }`}
                                >
                                    <img src={v.thumbnail_url} alt="" className="aspect-video w-full object-cover" />
                                    {picked && (
                                        <span className="absolute right-2 top-2 flex h-6 w-6 items-center justify-center rounded-full bg-violet-600">
                                            <Check className="h-3.5 w-3.5 text-white" />
                                        </span>
                                    )}
                                    <div className="p-2">
                                        <p className="line-clamp-2 text-[10px] font-medium text-gray-300">{v.title}</p>
                                        <p className="text-[9px] text-gray-600">{v.views.toLocaleString()} views</p>
                                    </div>
                                </button>
                            );
                        })}
                    </div>
                )}
            </section>

            <section className="rounded-2xl border border-white/[0.08] bg-white/[0.02] p-5">
                <h2 className="text-sm font-semibold uppercase tracking-wider text-violet-300">3 · Generate</h2>
                <div className="mt-4 flex flex-wrap gap-2">
                    {models.map((m) => (
                        <button
                            key={m.id}
                            type="button"
                            onClick={() => setImageModel(m.id)}
                            className={`rounded-full border px-3 py-1.5 text-xs font-medium transition ${
                                imageModel === m.id
                                    ? 'border-violet-500 bg-violet-500/15 text-violet-100'
                                    : 'border-white/10 text-gray-400 hover:border-white/20'
                            }`}
                        >
                            {m.label} · {m.credits} cr
                        </button>
                    ))}
                </div>
                <button
                    type="button"
                    onClick={generate}
                    disabled={generating}
                    className="mt-5 flex w-full items-center justify-center gap-2 rounded-xl bg-gradient-to-r from-violet-600 to-purple-600 py-3 text-sm font-bold text-white shadow-lg shadow-violet-900/30 hover:from-violet-500 disabled:opacity-50"
                >
                    {generating ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
                    Generate · {selectedCredits} credits
                </button>
                {genError && <p className="mt-2 text-xs text-red-400">{genError}</p>}
            </section>

            {(generating || job) && (
                <section className="rounded-2xl border border-violet-500/20 bg-violet-500/5 p-5">
                    <ThumbProgressBar progress={job?.progress ?? 0} status={job?.status ?? 'queued'} />
                    {job?.ai_analysis?.catalyst_channel && (
                        <p className="mt-2 text-xs text-emerald-400/80">Catalyst: {job.ai_analysis.catalyst_channel}</p>
                    )}
                    {job?.ai_analysis?.style_notes && (
                        <p className="mt-2 text-xs text-gray-400">{job.ai_analysis.style_notes}</p>
                    )}
                    {abScoring?.picked && (
                        <p className="mt-2 text-xs text-gray-500">
                            A/B picked variant {abScoring.picked.toUpperCase()} · vision {abScoring.vision_score ?? '—'} · A {abScoring.variant_a_score ?? '—'} vs B {abScoring.variant_b_score ?? '—'}
                        </p>
                    )}
                    {typeof vision?.vision_score === 'number' && vision.vision_score > 0 && (
                        <p className="mt-1 text-xs text-violet-300/70">Top reference vision score: {vision.vision_score}</p>
                    )}
                    {vision?.generation_directive && (
                        <p className="mt-2 text-xs text-violet-200/70">{vision.generation_directive}</p>
                    )}
                    {job?.status === 'error' && (
                        <p className="mt-2 text-sm text-red-400">{job.error}</p>
                    )}
                    {job?.status === 'complete' && outputUrl && (
                        <div className="mt-5 space-y-4">
                            {outputMedia.url ? (
                                <img src={outputMedia.url} alt="Generated thumbnail" className="w-full rounded-xl border border-white/10" />
                            ) : (
                                <div className="aspect-video w-full animate-pulse rounded-xl bg-white/5" />
                            )}
                            <div className="flex flex-wrap gap-2">
                                <button
                                    type="button"
                                    disabled={!token || downloadBusy}
                                    onClick={() => {
                                        setDownloadBusy(true);
                                        setDownloadError('');
                                        void downloadStudioAsset(outputUrl, token, `thumbnail-${jobId || 'output'}.png`)
                                            .catch((error) => setDownloadError(error instanceof Error ? error.message : 'Download failed'))
                                            .finally(() => setDownloadBusy(false));
                                    }}
                                    className="inline-flex items-center gap-2 rounded-lg bg-white/10 px-4 py-2 text-sm font-medium text-white hover:bg-white/15"
                                >
                                    <Download className="h-4 w-4" /> Download 1920×1080
                                </button>
                                <a href={outputMedia.url || undefined} target="_blank" rel="noreferrer" className="inline-flex items-center gap-2 rounded-lg border border-white/10 px-4 py-2 text-sm text-gray-300 hover:text-white">
                                    <ExternalLink className="h-4 w-4" /> Open
                                </a>
                            </div>
                            {downloadError || outputMedia.error ? (
                                <p className="text-xs text-red-300">{downloadError || outputMedia.error}</p>
                            ) : null}
                            <FeedbackWidget feature="thumbnails" template="thumb_lab" />
                        </div>
                    )}
                </section>
            )}
        </div>
    );
}
