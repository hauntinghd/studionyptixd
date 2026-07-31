/**
 * CreatePanel — Skeleton AI short builder (Studio Create tab).
 *
 * 3-tab UX: Script → Scenes (canonical stills) → Audio → full render.
 * Backend: /api/skeleton-ai/*
 * Stills: one locked master PNG + Seedream 4.5 edit per beat (background,
 *         outfit, props only — identity never drifts).
 */
import { useCallback, useContext, useEffect, useRef, useState } from 'react';
import { Sparkles, Wand2, Image as ImageIcon, Music, Loader2, X, Upload, RefreshCw } from 'lucide-react';
import { AuthContext, resolveStudioBackendUrl } from '../shared';
import { loadImageModelPref, saveImageModelPref } from '../lib/productionModelPrefs';
import {
    acquireProductionCommandLease,
} from '../lib/productionIdempotency';
import NichePickerStrip from '../components/create/NichePickerStrip';
import type { NicheId } from '../lib/studioProduct';

type Tab = 'script' | 'scenes' | 'audio';

interface ImageModelOption {
    id: string;
    label: string;
}

type VideoModel = 'ltx_budget' | 'seedance' | 'pixverse' | 'kling_pro';

interface CategoryInfo {
    key: string;
    label: string;
    tagline: string;
    seeds: string[];
    builtin?: boolean;
    custom?: boolean;
    youtube_category?: string;
}

interface Voice {
    voice_id: string;
    name: string;
    category?: string;
    preview_url?: string;
    labels?: Record<string, string>;
}

interface RenderedScene {
    beat_index: number;
    narration: string;
    outfit: string;
    scene_action: string;
    motion_prompt: string;
    image_path: string;
    edit_prompt?: string;
}

interface SkeletonReferenceState {
    previewUrl: string;
    payload: string;
    name: string;
}

interface CreatePanelProps {
    nicheId: string;
    categoryKey?: string;
    renderTier?: 'draft' | 'ship' | 'documentary';
    nicheTitle?: string;
    onNicheChange?: (id: NicheId) => void;
    isOwner?: boolean;
}

export default function CreatePanel({
    nicheId,
    categoryKey: categoryKeyProp = 'people_blogs',
    renderTier = 'draft',
    nicheTitle,
    onNicheChange,
    isOwner,
}: CreatePanelProps) {
    const { session } = useContext(AuthContext);
    const accessToken = session?.access_token || '';
    const [activeCategory, setActiveCategory] = useState(categoryKeyProp);

    useEffect(() => {
        setActiveCategory(categoryKeyProp);
    }, [categoryKeyProp]);

    const [tab, setTab] = useState<Tab>('script');
    const [script, setScript] = useState('');
    const [ideaModalOpen, setIdeaModalOpen] = useState(false);
    const [scriptStreaming, setScriptStreaming] = useState(false);
    const [voiceId, setVoiceId] = useState('');
    const [voiceSpeed, setVoiceSpeed] = useState(1.0);
    const [voicePitch, setVoicePitch] = useState(1.0);
    const [voiceLang, setVoiceLang] = useState('auto');
    const [captionFont, setCaptionFont] = useState('Komika Axis');
    // Persisted: a creator sets their brand once, not per render. Until now
    // every app render was watermarked 'Studio' regardless of whose channel
    // it was for.
    const [watermarkText, setWatermarkText] = useState(
        () => localStorage.getItem('studio.watermarkText') || '',
    );
    const [visualBrief, setVisualBrief] = useState(
        () => localStorage.getItem('studio.visualBrief') || '',
    );
    const [voices, setVoices] = useState<Voice[]>([]);
    const [videoModel, setVideoModel] = useState<VideoModel>(renderTier === 'ship' ? 'kling_pro' : 'seedance');
    const [imageModel, setImageModel] = useState('');
    const [imageModelOptions, setImageModelOptions] = useState<ImageModelOption[]>([]);
    const [imageModelCatalogError, setImageModelCatalogError] = useState('');

    useEffect(() => {
        setVideoModel(renderTier === 'ship' ? 'kling_pro' : 'seedance');
    }, [renderTier]);

    useEffect(() => {
        if (!accessToken) {
            setImageModelOptions([]);
            setImageModel('');
            setImageModelCatalogError('Sign in to load enabled FAL image models.');
            return;
        }
        let cancelled = false;
        setImageModelCatalogError('');
        void fetch(resolveStudioBackendUrl('/api/studio-agent/models'), {
            headers: { Authorization: `Bearer ${accessToken}` },
        })
            .then(async (response) => {
                const data = await response.json().catch(() => ({}));
                if (!response.ok) throw new Error(`model catalog failed (${response.status})`);
                const options: ImageModelOption[] = (Array.isArray(data?.image_models) ? data.image_models : [])
                    .filter((row: Record<string, unknown>) => (
                        String(row.provider || '').trim().toLowerCase() === 'fal'
                        && row.enabled !== false
                    ))
                    .map((row: Record<string, unknown>): ImageModelOption => ({
                        id: String(row.id || row.model_id || '').trim(),
                        label: String(row.label || row.name || row.id || '').trim(),
                    }))
                    .filter((row: ImageModelOption) => Boolean(row.id));
                if (cancelled) return;
                setImageModelOptions(options);
                const preferred = loadImageModelPref('seedream_edit');
                const selected = options.find((row) => row.id === preferred)?.id
                    || options.find((row) => row.id === 'seedream_edit')?.id
                    || options[0]?.id
                    || '';
                setImageModel(selected);
                if (selected) saveImageModelPref(selected);
                setImageModelCatalogError(
                    options.length ? '' : 'No enabled FAL image model is present in the current server catalog.',
                );
            })
            .catch((error: unknown) => {
                if (cancelled) return;
                setImageModelOptions([]);
                setImageModel('');
                setImageModelCatalogError(
                    error instanceof Error ? error.message : 'Could not load the image model catalog.',
                );
            });
        return () => { cancelled = true; };
    }, [accessToken]);
    const [generating, setGenerating] = useState(false);
    const [generatedVideoUrl, setGeneratedVideoUrl] = useState<string | null>(null);

    // Stills-only scene render state (Generate Scenes button on the Scenes tab).
    // SSE-streamed: scenes pop into renderedScenes as fal returns each one,
    // scenesProgress.total set on first event so the progress bar can render
    // immediately, scenesAbortRef holds the AbortController for the Stop button.
    const [scenesGenerating, setScenesGenerating] = useState(false);
    const [renderedScenes, setRenderedScenes] = useState<RenderedScene[]>([]);
    const [scenesProgress, setScenesProgress] = useState<{ done: number; total: number }>({ done: 0, total: 0 });
    const [sceneError, setSceneError] = useState<string>('');
    const [scenesJobId, setScenesJobId] = useState('');
    const [skeletonReference, setSkeletonReference] = useState<SkeletonReferenceState | null>(null);
    const [referenceUploading, setReferenceUploading] = useState(false);
    const scenesAbortRef = useRef<AbortController | null>(null);
    const referenceInputRef = useRef<HTMLInputElement>(null);
    const openReferencePicker = useCallback(() => {
        referenceInputRef.current?.click();
    }, []);

    // Fetch voices once we have an auth token (voices route is auth-gated).
    useEffect(() => {
        if (!accessToken) return;
        fetch(resolveStudioBackendUrl('/api/skeleton-ai/voices'), {
            headers: { Authorization: `Bearer ${accessToken}` },
        })
            .then((r) => r.json())
            .then((d) => {
                if (Array.isArray(d.voices)) setVoices(d.voices);
            })
            .catch(() => setVoices([]));
    }, [accessToken]);

    const scriptCharCount = script.length;
    const estimatedDuration = Math.round(scriptCharCount / 15); // rough: 15 chars/sec
    const estimatedScenes = Math.max(1, Math.ceil(estimatedDuration / 5));

    const uploadSkeletonReference = useCallback(async (file: File) => {
        if (!accessToken) {
            alert('You must be signed in to upload a skeleton reference.');
            return;
        }
        setReferenceUploading(true);
        try {
            const form = new FormData();
            form.append('reference_image', file);
            const r = await fetch(resolveStudioBackendUrl('/api/skeleton-ai/reference'), {
                method: 'POST',
                headers: { Authorization: `Bearer ${accessToken}` },
                body: form,
            });
            const d = await r.json().catch(() => ({}));
            if (!r.ok) {
                throw new Error(String(d?.detail || d?.error || `upload failed: ${r.status}`));
            }
            const payload = String(d.reference_image || d.reference_image_url || '').trim();
            if (!payload) throw new Error('upload returned no reference image');
            const previewUrl = URL.createObjectURL(file);
            setSkeletonReference({ previewUrl, payload, name: file.name });
        } catch (e) {
            const err = e as Error;
            setSceneError(err.message || String(err));
        } finally {
            setReferenceUploading(false);
        }
    }, [accessToken]);

    const onReferenceFileChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        if (file) void uploadSkeletonReference(file);
        event.target.value = '';
    }, [uploadSkeletonReference]);

    const updateRenderedScene = useCallback((beatIndex: number, patch: Partial<RenderedScene>) => {
        setRenderedScenes((prev) => prev.map((scene) => (
            scene.beat_index === beatIndex ? { ...scene, ...patch } : scene
        )));
    }, []);

    const regenerateScene = useCallback(async (scene: RenderedScene) => {
        if (!accessToken || !scenesJobId) return;
        setScenesGenerating(true);
        setSceneError('');
        const command = acquireProductionCommandLease(
            'skeleton-scene',
            `${scenesJobId}-${scene.beat_index}`,
        );
        try {
            const r = await fetch(resolveStudioBackendUrl('/api/skeleton-ai/scenes/regenerate'), {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${accessToken}`,
                    'X-Idempotency-Key': command.commandId,
                },
                body: JSON.stringify({
                    job_id: scenesJobId,
                    beat_index: scene.beat_index,
                    outfit: scene.outfit,
                    scene_action: scene.scene_action,
                    motion_prompt: scene.motion_prompt,
                    reference_image: skeletonReference?.payload || undefined,
                }),
            });
            const d = await r.json().catch(() => ({}));
            command.release();
            if (!r.ok) {
                throw new Error(String(d?.detail || d?.error || `regenerate failed: ${r.status}`));
            }
            if (d.scene) {
                const nextScene = d.scene as RenderedScene;
                updateRenderedScene(scene.beat_index, {
                    ...nextScene,
                    image_path: `${nextScene.image_path}?t=${Date.now()}`,
                });
            }
        } catch (e) {
            const err = e as Error;
            setSceneError(err.message || String(err));
        } finally {
            setScenesGenerating(false);
        }
    }, [accessToken, scenesJobId, skeletonReference?.payload, updateRenderedScene]);

    // Generate Scenes (stills only) — Korpi-style SSE streaming. Each scene
    // appears in the gallery the moment fal returns it. Stop button cancels
    // mid-flight via AbortController.
    const startGenerateScenes = useCallback(async () => {
        if (!script.trim()) {
            alert('Add or generate a script first.');
            return;
        }
        if (!accessToken) {
            alert('You must be signed in to generate scenes.');
            return;
        }
        if (!imageModel || !imageModelOptions.some((row) => row.id === imageModel)) {
            alert('Choose an enabled FAL image model from the current Studio catalog first.');
            return;
        }
        if (!skeletonReference?.payload) {
            alert('Upload your skeleton reference image first — this locks identity like KORPI custom niche creator.');
            return;
        }
        // Reset state for a fresh run.
        setScenesGenerating(true);
        setRenderedScenes([]);
        setSceneError('');
        setScenesJobId('');
        setScenesProgress({ done: 0, total: 0 });

        const controller = new AbortController();
        scenesAbortRef.current = controller;
        const command = acquireProductionCommandLease(
            'skeleton-scenes',
            `${activeCategory}-${script.length}-${imageModel}`,
        );
        let terminalResponse = false;

        try {
            const r = await fetch(resolveStudioBackendUrl('/api/skeleton-ai/scenes'), {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Accept: 'text/event-stream',
                    Authorization: `Bearer ${accessToken}`,
                    'X-Idempotency-Key': command.commandId,
                },
                body: JSON.stringify({
                    script,
                    image_model: imageModel,
                    category: activeCategory,
                    reference_image: skeletonReference.payload,
                    watermark_text: watermarkText.trim() || undefined,
                    visual_brief: visualBrief.trim() || undefined,
                }),
                signal: controller.signal,
            });
            if (!r.ok || !r.body) {
                const txt = await r.text().catch(() => '');
                command.release();
                throw new Error(`scenes failed: ${r.status} ${txt.slice(0, 240)}`);
            }
            // Parse SSE stream: events are 'event: <name>\ndata: <json>\n\n'.
            const reader = r.body.getReader();
            const decoder = new TextDecoder();
            let buf = '';
            let currentEvent = '';
            outer:
            for (;;) {
                const { value, done } = await reader.read();
                if (done) break;
                buf += decoder.decode(value, { stream: true });
                // Events are separated by blank lines (\n\n). Process complete events only.
                let sep: number;
                while ((sep = buf.indexOf('\n\n')) !== -1) {
                    const block = buf.slice(0, sep);
                    buf = buf.slice(sep + 2);
                    currentEvent = '';
                    let dataStr = '';
                    for (const line of block.split('\n')) {
                        if (line.startsWith('event: ')) currentEvent = line.slice(7).trim();
                        else if (line.startsWith('data: ')) dataStr += line.slice(6);
                    }
                    if (!dataStr) continue;
                    let payload: any;
                    try { payload = JSON.parse(dataStr); } catch { continue; }
                    if (currentEvent === 'meta') {
                        if (payload.job_id) setScenesJobId(String(payload.job_id));
                        setScenesProgress({ done: 0, total: Number(payload.total || 0) });
                    } else if (currentEvent === 'scene') {
                        setRenderedScenes((prev) => {
                            const next = [...prev, payload as RenderedScene];
                            next.sort((a, b) => a.beat_index - b.beat_index);
                            return next;
                        });
                        setScenesProgress((prev) => ({ ...prev, done: prev.done + 1 }));
                    } else if (currentEvent === 'error') {
                        setSceneError(String(payload.message || 'render error'));
                    } else if (currentEvent === 'complete') {
                        terminalResponse = true;
                        break outer;
                    }
                }
            }
            if (terminalResponse) command.release();
        } catch (e) {
            const err = e as Error;
            if (err.name === 'AbortError') {
                command.release();
            } else {
                setSceneError(err.message || String(err));
            }
        } finally {
            setScenesGenerating(false);
            scenesAbortRef.current = null;
        }
    }, [script, accessToken, activeCategory, imageModel, imageModelOptions, skeletonReference?.payload]);

    const stopGenerateScenes = useCallback(() => {
        scenesAbortRef.current?.abort();
        scenesAbortRef.current = null;
    }, []);

    const startGenerate = useCallback(async () => {
        if (!script.trim()) {
            alert('Add or generate a script on the Script tab before generating.');
            return;
        }
        if (!accessToken) {
            alert('You must be signed in to generate.');
            return;
        }
        if (!imageModel || !imageModelOptions.some((row) => row.id === imageModel)) {
            alert('Choose an enabled FAL image model from the current Studio catalog first.');
            return;
        }
        if (!skeletonReference?.payload) {
            alert('Upload your skeleton reference image before full render.');
            return;
        }
        setGenerating(true);
        const command = acquireProductionCommandLease(
            'skeleton-generate',
            `${activeCategory}-${script.length}-${imageModel}-${videoModel}`,
        );
        try {
            const r = await fetch(resolveStudioBackendUrl('/api/skeleton-ai/generate'), {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${accessToken}`,
                    'X-Idempotency-Key': command.commandId,
                },
                body: JSON.stringify({
                    category: activeCategory,
                    script_override: script,
                    render_tier: renderTier,
                    image_model: imageModel,
                    voice_id: voiceId,
                    voice_speed: voiceSpeed,
                    voice_pitch: voicePitch,
                    voice_language: voiceLang,
                    caption_font: captionFont,
                    tier: videoModel === 'kling_pro' ? 'premium' : 'standard',
                    video_model: videoModel,
                    reference_image: skeletonReference.payload,
                    watermark_text: watermarkText.trim() || undefined,
                    visual_brief: visualBrief.trim() || undefined,
                }),
            });
            const d = await r.json();
            command.release();
            // 402 insufficient_credits → render a structured Top Up prompt.
            if (r.status === 402) {
                const detail = d?.detail || {};
                if (detail?.code === 'insufficient_credits') {
                    const needed = Number(detail.needed || 0);
                    const have = Number(detail.have || 0);
                    const tierName = String(detail.tier || videoModel).toUpperCase();
                    if (window.confirm(
                        `${tierName} short needs ${needed} AC. You have ${have}.\n\n`
                        + `Top up to continue?`
                    )) {
                        // Send the user to the billing top-up flow.
                        window.location.assign('/billing?focus=topup');
                    }
                    return;
                }
            }
            if (r.ok && d.video_path) {
                setGeneratedVideoUrl(d.video_path);
                return;
            }
            if (r.ok && d.job_id) {
                setScenesJobId(String(d.job_id));
                setSceneError('Production queued. Studio is building the staged visual proof for review.');
                return;
            }
            const msg = typeof d.detail === 'string' ? d.detail
                : (d.detail?.code || d.error || `HTTP ${r.status}`);
            alert(`Generation failed: ${msg}`);
        } finally {
            setGenerating(false);
        }
    }, [script, voiceId, voiceSpeed, voicePitch, voiceLang, captionFont, videoModel, imageModel, imageModelOptions, accessToken, activeCategory, renderTier, skeletonReference?.payload, watermarkText, visualBrief]);

    const tierLabel = renderTier === 'ship' ? 'Ship tier · premium motion' : renderTier === 'documentary' ? 'Documentary lane' : 'Draft tier · fast iteration';
    const scriptHeading = nicheTitle ? `${nicheTitle} script` : 'Narration script';

    return (
        <div className="flex flex-col gap-6 py-2 max-w-5xl mx-auto">
            <header className="space-y-4">
                {onNicheChange && (
                    <NichePickerStrip
                        value={nicheId as NicheId}
                        onChange={onNicheChange}
                        isOwner={isOwner}
                        compact
                    />
                )}
                <div className="flex flex-wrap items-center justify-between gap-3">
                    <div>
                        <h1 className="text-2xl font-bold text-white">{nicheTitle || 'Short Builder'}</h1>
                        <p className="mt-1 text-xs text-zinc-400">{tierLabel}</p>
                    </div>
                    <div className="text-xs text-zinc-400">
                        Standard = 5 AC · Premium = 7 AC
                    </div>
                </div>
                <CategorySelector
                    accessToken={accessToken}
                    value={activeCategory}
                    onChange={setActiveCategory}
                />
            </header>

            <TabRow tab={tab} setTab={setTab} />

            {tab === 'script' && (
                <ScriptTab
                    script={script}
                    setScript={setScript}
                    streaming={scriptStreaming}
                    charCount={scriptCharCount}
                    duration={estimatedDuration}
                    heading={scriptHeading}
                    onOpenIdeaModal={() => setIdeaModalOpen(true)}
                />
            )}
            <input
                ref={referenceInputRef}
                type="file"
                accept="image/png,image/jpeg,image/webp"
                className="hidden"
                onChange={onReferenceFileChange}
            />
            {tab === 'scenes' && (
                <ScenesTab
                    charCount={scriptCharCount}
                    duration={estimatedDuration}
                    estimatedScenes={estimatedScenes}
                    scriptValid={script.trim().length > 0}
                    onGenerateScenes={startGenerateScenes}
                    onStopGenerateScenes={stopGenerateScenes}
                    onGenerateAndAnimate={() => { setTab('audio'); }}
                    scenesGenerating={scenesGenerating}
                    renderedScenes={renderedScenes}
                    scenesProgress={scenesProgress}
                    sceneError={sceneError}
                    accessToken={accessToken}
                    imageModel={imageModel}
                    imageModelOptions={imageModelOptions}
                    imageModelCatalogError={imageModelCatalogError}
                    setImageModel={(id) => {
                        setImageModel(id);
                        saveImageModelPref(id);
                    }}
                    skeletonReference={skeletonReference}
                    referenceUploading={referenceUploading}
                    onPickReference={openReferencePicker}
                    onClearReference={() => setSkeletonReference(null)}
                    onUpdateScene={updateRenderedScene}
                    onRegenerateScene={regenerateScene}
                />
            )}
            {tab === 'audio' && (
                <AudioTab
                    voices={voices}
                    voiceId={voiceId}
                    setVoiceId={setVoiceId}
                    voiceSpeed={voiceSpeed}
                    setVoiceSpeed={setVoiceSpeed}
                    voicePitch={voicePitch}
                    setVoicePitch={setVoicePitch}
                    voiceLang={voiceLang}
                    setVoiceLang={setVoiceLang}
                    captionFont={captionFont}
                    setCaptionFont={setCaptionFont}
                    watermarkText={watermarkText}
                    setWatermarkText={setWatermarkText}
                    visualBrief={visualBrief}
                    setVisualBrief={setVisualBrief}
                    videoModel={videoModel}
                    setVideoModel={setVideoModel}
                    onGenerate={startGenerate}
                    generating={generating}
                />
            )}

            {generatedVideoUrl && (
                <div className="rounded-lg border border-zinc-800 p-4 bg-zinc-950">
                    <div className="text-sm font-semibold mb-2 text-white">Latest render</div>
                    <video src={generatedVideoUrl} controls className="w-full max-w-sm rounded" />
                </div>
            )}

            {ideaModalOpen && (
                <IdeaModal
                    accessToken={accessToken}
                    selectedCategory={activeCategory}
                    onCategoryChange={setActiveCategory}
                    nicheTitle={nicheTitle}
                    onClose={() => setIdeaModalOpen(false)}
                    onScript={(text) => {
                        setScript(text);
                        setIdeaModalOpen(false);
                        setScriptStreaming(false);
                        // Auto-jump to Scenes so the user immediately sees the
                        // image-model picker + can hit "Generate Scenes" for
                        // a stills-only preview before burning fal on i2v.
                        setTab('scenes');
                    }}
                    onStreamStart={() => setScriptStreaming(true)}
                    onStreamEnd={() => setScriptStreaming(false)}
                />
            )}
        </div>
    );
}

function TabRow({ tab, setTab }: { tab: Tab; setTab: (t: Tab) => void }) {
    const tabs: { id: Tab; label: string; icon: typeof Wand2 }[] = [
        { id: 'script', label: 'Script', icon: Wand2 },
        { id: 'scenes', label: 'Scenes', icon: ImageIcon },
        { id: 'audio', label: 'Audio', icon: Music },
    ];
    return (
        <div className="flex gap-1 border-b border-zinc-800">
            {tabs.map(({ id, label, icon: Icon }) => (
                <button
                    key={id}
                    onClick={() => setTab(id)}
                    className={`flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                        tab === id
                            ? 'border-violet-500 text-white'
                            : 'border-transparent text-zinc-400 hover:text-zinc-200'
                    }`}
                >
                    <Icon className="h-4 w-4" />
                    {label}
                </button>
            ))}
        </div>
    );
}

function ScriptTab({
    script, setScript, streaming, charCount, duration, heading, onOpenIdeaModal,
}: {
    script: string;
    setScript: (s: string) => void;
    streaming: boolean;
    charCount: number;
    duration: number;
    heading: string;
    onOpenIdeaModal: () => void;
}) {
    return (
        <section className="flex flex-col gap-3">
            <div className="flex items-center justify-between">
                <h2 className="text-lg font-semibold text-white">{heading}</h2>
            </div>
            <div className="flex items-center justify-between">
                <label className="text-sm text-zinc-300">Narration Script</label>
                <div className="flex items-center gap-3">
                    <span className="text-xs text-zinc-500">
                        {charCount} chars · ~{duration}s
                    </span>
                    <button
                        onClick={onOpenIdeaModal}
                        className="inline-flex items-center gap-2 rounded-md bg-violet-500/15 border border-violet-500/30 px-3 py-1.5 text-xs font-semibold text-violet-200 hover:bg-violet-500/25"
                    >
                        <Sparkles className="h-3.5 w-3.5" />
                        Generate w/ AI
                    </button>
                </div>
            </div>
            <textarea
                value={script}
                onChange={(e) => setScript(e.target.value)}
                placeholder="Enter your narration script..."
                rows={14}
                className="w-full rounded-md bg-zinc-950 border border-zinc-800 px-3 py-2 text-sm text-white placeholder:text-zinc-600 focus:outline-none focus:border-violet-500/50 resize-y"
            />
            {streaming && (
                <div className="text-xs text-violet-400 flex items-center gap-2">
                    <Loader2 className="h-3 w-3 animate-spin" /> Streaming from Studio...
                </div>
            )}
            <div className="flex justify-end">
                <button
                    onClick={() => setScript('')}
                    className="text-xs text-zinc-500 hover:text-zinc-300"
                >
                    Clear
                </button>
            </div>
        </section>
    );
}

function ScenesTab({
    charCount, duration, estimatedScenes, scriptValid,
    onGenerateScenes, onStopGenerateScenes, onGenerateAndAnimate,
    scenesGenerating, renderedScenes, scenesProgress, sceneError, accessToken,
    imageModel, imageModelOptions, imageModelCatalogError, setImageModel,
    skeletonReference, referenceUploading, onPickReference,
    onClearReference, onUpdateScene, onRegenerateScene,
}: {
    charCount: number;
    duration: number;
    estimatedScenes: number;
    scriptValid: boolean;
    onGenerateScenes: () => void;
    onStopGenerateScenes: () => void;
    onGenerateAndAnimate: () => void;
    scenesGenerating: boolean;
    renderedScenes: RenderedScene[];
    scenesProgress: { done: number; total: number };
    sceneError: string;
    accessToken: string;
    imageModel: string;
    imageModelOptions: ImageModelOption[];
    imageModelCatalogError: string;
    setImageModel: (id: string) => void;
    skeletonReference: SkeletonReferenceState | null;
    referenceUploading: boolean;
    onPickReference: () => void;
    onClearReference: () => void;
    onUpdateScene: (beatIndex: number, patch: Partial<RenderedScene>) => void;
    onRegenerateScene: (scene: RenderedScene) => void;
}) {
    return (
        <section className="flex flex-col gap-4">
            <h2 className="text-lg font-semibold text-white">Generate Scenes</h2>

            <div className="rounded-md border border-violet-500/30 bg-violet-500/10 px-3 py-3">
                <div className="text-sm font-semibold text-white">Skeleton Reference (required)</div>
                <p className="text-xs text-zinc-400 mt-1">
                    Upload your approved skeleton image first — same as KORPI custom niche creator.
                    Seedream 4.5 Edit changes only background, props, and outfit while identity stays locked.
                </p>
                <div className="mt-3 flex flex-wrap items-center gap-3">
                    <button
                        type="button"
                        onClick={onPickReference}
                        disabled={referenceUploading}
                        className="inline-flex items-center gap-2 rounded-md bg-violet-500 px-3 py-2 text-xs font-semibold text-white hover:bg-violet-600 disabled:opacity-50"
                    >
                        {referenceUploading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Upload className="h-3.5 w-3.5" />}
                        {skeletonReference ? 'Replace Reference' : 'Upload Skeleton Reference'}
                    </button>
                    {skeletonReference && (
                        <button
                            type="button"
                            onClick={onClearReference}
                            className="text-xs text-zinc-400 hover:text-zinc-200"
                        >
                            Clear
                        </button>
                    )}
                    {skeletonReference && (
                        <div className="flex items-center gap-2 rounded-md border border-zinc-800 bg-zinc-950 px-2 py-1.5">
                            <img src={skeletonReference.previewUrl} alt="Skeleton reference" className="h-10 w-10 rounded object-cover" />
                            <span className="text-[11px] text-zinc-300 max-w-[180px] truncate">{skeletonReference.name}</span>
                        </div>
                    )}
                </div>
                <p className="text-xs text-violet-300 mt-2">~4 credits per scene still</p>
            </div>

            <div className="rounded-md border border-zinc-800 bg-zinc-950 px-3 py-3">
                <label className="text-xs font-semibold text-zinc-400 uppercase tracking-wide">
                    Image model (stills)
                </label>
                <select
                    value={imageModel}
                    onChange={(e) => setImageModel(e.target.value)}
                    disabled={imageModelOptions.length === 0}
                    className="mt-2 w-full rounded-md bg-zinc-900 border border-zinc-700 px-3 py-2 text-sm text-white"
                >
                    {imageModelOptions.length === 0 && (
                        <option value="">No enabled FAL models</option>
                    )}
                    {imageModelOptions.map((m) => (
                        <option key={m.id} value={m.id}>{m.label}</option>
                    ))}
                </select>
                <p className="text-[11px] text-zinc-500 mt-2">
                    Server catalog only. The same preference syncs with Studio Agent and Long-form.
                </p>
                {imageModelCatalogError && (
                    <p className="text-[11px] text-rose-300 mt-1">{imageModelCatalogError}</p>
                )}
            </div>

            <div className="grid grid-cols-3 gap-3">
                <Stat label="Characters" value={String(charCount)} />
                <Stat label="Duration" value={`~${duration}s`} />
                <Stat label="Est. Scenes" value={String(estimatedScenes)} />
            </div>

            <div className="flex flex-col gap-2">
                {scenesGenerating ? (
                    <button
                        onClick={onStopGenerateScenes}
                        className="w-full rounded-md bg-rose-500 px-4 py-2.5 text-sm font-semibold text-white hover:bg-rose-600 flex items-center justify-center gap-2"
                    >
                        <X className="h-4 w-4" />
                        Stop Image Generation
                    </button>
                ) : (
                    <button
                        disabled={!scriptValid || !skeletonReference || !imageModel}
                        onClick={onGenerateScenes}
                        className="w-full rounded-md bg-violet-500 px-4 py-2.5 text-sm font-semibold text-white hover:bg-violet-600 disabled:bg-zinc-800 disabled:text-zinc-500 disabled:cursor-not-allowed flex items-center justify-center gap-2"
                    >
                        <ImageIcon className="h-4 w-4" />
                        Generate Scenes
                    </button>
                )}
                <button
                    disabled={!scriptValid || scenesGenerating || !imageModel}
                    onClick={onGenerateAndAnimate}
                    className="w-full rounded-md bg-zinc-900 px-4 py-2.5 text-sm font-semibold text-white border border-zinc-800 hover:bg-zinc-800 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                    Generate Scenes &amp; Animate
                </button>
            </div>

            {(scenesGenerating || scenesProgress.total > 0) && (
                <ScenesProgressBar progress={scenesProgress} active={scenesGenerating} />
            )}

            {sceneError && (
                <div className="rounded-md bg-rose-500/10 border border-rose-500/30 px-3 py-2 text-sm text-rose-200">
                    {sceneError}
                </div>
            )}

            {renderedScenes.length > 0 && (
                <>
                    <div className="flex items-center justify-between mt-2">
                        <h3 className="text-sm font-semibold text-white">
                            Generated Scenes ({renderedScenes.length}{scenesProgress.total ? ` / ${scenesProgress.total}` : ''})
                        </h3>
                        {!scenesGenerating && renderedScenes.length === scenesProgress.total && scenesProgress.total > 0 && (
                            <button
                                onClick={onGenerateAndAnimate}
                                className="rounded-md bg-cyan-500 hover:bg-cyan-600 px-3 py-1.5 text-xs font-semibold text-white"
                            >
                                Animate All
                            </button>
                        )}
                    </div>
                    <SceneGallery
                        scenes={renderedScenes}
                        accessToken={accessToken}
                        scenesGenerating={scenesGenerating}
                        onUpdateScene={onUpdateScene}
                        onRegenerateScene={onRegenerateScene}
                    />
                </>
            )}

            {!scriptValid && (
                <div className="rounded-md bg-amber-500/10 border border-amber-500/30 px-3 py-2 text-sm text-amber-200">
                    Please add or generate a script on the Script tab before generating scenes.
                </div>
            )}
            {scriptValid && !skeletonReference && (
                <div className="rounded-md bg-amber-500/10 border border-amber-500/30 px-3 py-2 text-sm text-amber-200">
                    Upload your skeleton reference image before generating scenes.
                </div>
            )}

        </section>
    );
}

function ScenesProgressBar({ progress, active }: { progress: { done: number; total: number }; active: boolean }) {
    const total = progress.total || 0;
    const done = progress.done || 0;
    const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : (active ? 5 : 0);
    return (
        <div className="rounded-md border border-zinc-800 bg-zinc-950 px-3 py-2.5">
            <div className="flex items-center justify-between text-xs text-zinc-400 mb-1">
                <span>
                    {active ? (
                        total > 0 ? `Generating images ${done + 1}–${total} of ${total}…` : 'Generating scene prompts…'
                    ) : `Rendered ${done} / ${total}`}
                </span>
                <span className="font-mono">{pct}%</span>
            </div>
            <div className="h-1.5 w-full bg-zinc-800 rounded-full overflow-hidden">
                <div
                    className="h-full bg-violet-500 transition-all duration-300"
                    style={{ width: `${pct}%` }}
                />
            </div>
        </div>
    );
}

function SceneGallery({
    scenes,
    accessToken,
    scenesGenerating,
    onUpdateScene,
    onRegenerateScene,
}: {
    scenes: RenderedScene[];
    accessToken: string;
    scenesGenerating: boolean;
    onUpdateScene: (beatIndex: number, patch: Partial<RenderedScene>) => void;
    onRegenerateScene: (scene: RenderedScene) => void;
}) {
    // Each still is served by an auth-gated endpoint, so we have to fetch
    // with a Bearer header and convert to a blob URL — a plain <img src>
    // can't attach Authorization. Cleanup blobs on unmount/replace.
    const [blobUrls, setBlobUrls] = useState<Record<number, string>>({});
    useEffect(() => {
        let cancelled = false;
        const created: string[] = [];
        const next: Record<number, string> = {};
        (async () => {
            for (const s of scenes) {
                try {
                    const r = await fetch(s.image_path, {
                        headers: { Authorization: `Bearer ${accessToken}` },
                    });
                    if (!r.ok) continue;
                    const blob = await r.blob();
                    const url = URL.createObjectURL(blob);
                    created.push(url);
                    next[s.beat_index] = url;
                    if (!cancelled) setBlobUrls((prev) => ({ ...prev, [s.beat_index]: url }));
                } catch {
                    /* ignore one-image failures, keep loading the rest */
                }
            }
        })();
        return () => {
            cancelled = true;
            for (const u of created) URL.revokeObjectURL(u);
        };
    }, [scenes, accessToken]);

    return (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 mt-2">
            {scenes.map((s) => (
                <div key={s.beat_index} className="rounded-md overflow-hidden border border-zinc-800 bg-zinc-950">
                    <div className="grid grid-cols-[120px_1fr] gap-3 p-3">
                        <div className="aspect-[9/16] bg-zinc-900 flex items-center justify-center rounded-md overflow-hidden">
                            {blobUrls[s.beat_index] ? (
                                <img src={blobUrls[s.beat_index]} alt={`Beat ${s.beat_index + 1}`} className="w-full h-full object-cover" />
                            ) : (
                                <Loader2 className="h-6 w-6 text-zinc-600 animate-spin" />
                            )}
                        </div>
                        <div className="flex flex-col gap-2 min-w-0">
                            <div className="text-[11px] text-zinc-500">
                                Beat {s.beat_index + 1}
                            </div>
                            <label className="text-[10px] uppercase tracking-wide text-zinc-500">Scene action</label>
                            <textarea
                                value={s.scene_action}
                                onChange={(e) => onUpdateScene(s.beat_index, { scene_action: e.target.value })}
                                rows={3}
                                className="w-full rounded-md bg-zinc-900 border border-zinc-800 px-2 py-1.5 text-[11px] text-zinc-200 resize-y"
                            />
                            <label className="text-[10px] uppercase tracking-wide text-zinc-500">Outfit</label>
                            <textarea
                                value={s.outfit}
                                onChange={(e) => onUpdateScene(s.beat_index, { outfit: e.target.value })}
                                rows={2}
                                className="w-full rounded-md bg-zinc-900 border border-zinc-800 px-2 py-1.5 text-[11px] text-zinc-200 resize-y"
                            />
                            <label className="text-[10px] uppercase tracking-wide text-zinc-500">Motion prompt</label>
                            <textarea
                                value={s.motion_prompt}
                                onChange={(e) => onUpdateScene(s.beat_index, { motion_prompt: e.target.value })}
                                rows={2}
                                className="w-full rounded-md bg-zinc-900 border border-zinc-800 px-2 py-1.5 text-[11px] text-zinc-200 resize-y"
                            />
                            <button
                                type="button"
                                disabled={scenesGenerating}
                                onClick={() => onRegenerateScene(s)}
                                className="inline-flex items-center justify-center gap-1.5 rounded-md border border-zinc-700 px-2.5 py-1.5 text-[11px] font-semibold text-zinc-200 hover:bg-zinc-900 disabled:opacity-40"
                            >
                                <RefreshCw className="h-3 w-3" />
                                Regenerate still
                            </button>
                        </div>
                    </div>
                    <div className="px-3 pb-2 text-[10px] text-zinc-500 truncate border-t border-zinc-900 pt-2">
                        {s.narration}
                    </div>
                </div>
            ))}
        </div>
    );
}

function CategorySelector({
    accessToken, value, onChange,
}: {
    accessToken: string;
    value: string;
    onChange: (key: string) => void;
}) {
    const [categories, setCategories] = useState<CategoryInfo[]>([]);

    useEffect(() => {
        const headers: Record<string, string> = {};
        if (accessToken) headers.Authorization = `Bearer ${accessToken}`;
        fetch(resolveStudioBackendUrl('/api/skeleton-ai/categories'), { headers })
            .then((r) => r.json())
            .then((d) => Array.isArray(d.categories) && setCategories(d.categories))
            .catch(() => setCategories([]));
    }, [accessToken]);

    const selected = categories.find((c) => c.key === value);

    return (
        <div className="rounded-md border border-zinc-800 bg-zinc-950 px-3 py-3">
            <label className="text-xs font-semibold text-zinc-400 uppercase tracking-wide">
                Script category
            </label>
            <div className="mt-2 flex flex-wrap gap-2 items-center">
                <select
                    value={value}
                    onChange={(e) => onChange(e.target.value)}
                    className="min-w-[220px] flex-1 rounded-md bg-zinc-900 border border-zinc-700 px-3 py-2 text-sm text-white"
                >
                    {categories.length === 0 && (
                        <option value={value}>{value || 'Loading…'}</option>
                    )}
                    {categories.filter((c) => c.builtin).length > 0 && (
                        <optgroup label="YouTube-aligned (20)">
                            {categories.filter((c) => c.builtin).map((c) => (
                                <option key={c.key} value={c.key}>
                                    {c.label}
                                </option>
                            ))}
                        </optgroup>
                    )}
                    {categories.filter((c) => c.custom).length > 0 && (
                        <optgroup label="Your custom categories">
                            {categories.filter((c) => c.custom).map((c) => (
                                <option key={c.key} value={c.key}>
                                    {c.label}
                                </option>
                            ))}
                        </optgroup>
                    )}
                </select>
                {selected?.tagline && (
                    <span className="text-xs text-zinc-500 flex-1 min-w-[12rem]">{selected.tagline}</span>
                )}
            </div>
        </div>
    );
}

function Stat({ label, value }: { label: string; value: string }) {
    return (
        <div className="rounded-md border border-zinc-800 px-3 py-2 bg-zinc-950">
            <div className="text-xs text-zinc-500">{label}</div>
            <div className="text-lg font-bold text-white mt-0.5">{value}</div>
        </div>
    );
}

function AudioTab({
    voices, voiceId, setVoiceId, voiceSpeed, setVoiceSpeed, voicePitch, setVoicePitch,
    voiceLang, setVoiceLang, captionFont, setCaptionFont,
    watermarkText, setWatermarkText, visualBrief, setVisualBrief,
    videoModel, setVideoModel,
    onGenerate, generating,
}: {
    voices: Voice[];
    voiceId: string;
    setVoiceId: (v: string) => void;
    voiceSpeed: number;
    setVoiceSpeed: (n: number) => void;
    voicePitch: number;
    setVoicePitch: (n: number) => void;
    voiceLang: string;
    setVoiceLang: (s: string) => void;
    captionFont: string;
    watermarkText: string;
    setWatermarkText: (v: string) => void;
    visualBrief: string;
    setVisualBrief: (v: string) => void;
    setCaptionFont: (s: string) => void;
    videoModel: VideoModel;
    setVideoModel: (m: VideoModel) => void;
    onGenerate: () => void;
    generating: boolean;
}) {
    const cost = videoModel === 'kling_pro' ? 7 : videoModel === 'ltx_budget' ? 3 : 5;
    return (
        <section className="flex flex-col gap-4">
            <h2 className="text-lg font-semibold text-white">Narration Voice</h2>

            <div>
                <label className="text-sm text-zinc-300 block mb-1">FAL MiniMax Voice</label>
                <select
                    value={voiceId}
                    onChange={(e) => setVoiceId(e.target.value)}
                    className="w-full rounded-md bg-zinc-950 border border-zinc-800 px-3 py-2 text-sm text-white"
                >
                    <option value="">Select a voice…</option>
                    {voices.map((v) => (
                        <option key={v.voice_id} value={v.voice_id}>
                            {v.name} {v.category ? `(${v.category})` : ''}
                        </option>
                    ))}
                </select>
                <div className="text-xs text-zinc-500 mt-1">
                    {voices.length === 0 ? 'No configured FAL MiniMax voices are available.' : `${voices.length} FAL voices available.`}
                </div>
            </div>

            <RangeRow label="Voice Speed" value={voiceSpeed} setValue={setVoiceSpeed} min={0.5} max={2.0} step={0.05} suffix="x" />
            <RangeRow label="Voice Pitch" value={voicePitch} setValue={setVoicePitch} min={0.5} max={2.0} step={0.05} suffix="x" />

            <div>
                <label className="text-sm text-zinc-300 block mb-1">Voice Language</label>
                <select
                    value={voiceLang}
                    onChange={(e) => setVoiceLang(e.target.value)}
                    className="w-full rounded-md bg-zinc-950 border border-zinc-800 px-3 py-2 text-sm text-white"
                >
                    <option value="auto">Auto Detect</option>
                    <option value="en">English</option>
                    <option value="es">Spanish</option>
                    <option value="fr">French</option>
                    <option value="de">German</option>
                    <option value="pt">Portuguese</option>
                </select>
            </div>

            <div>
                <label className="text-sm text-zinc-300 block mb-1">Watermark / Brand</label>
                <input
                    type="text"
                    value={watermarkText}
                    maxLength={48}
                    placeholder="MrSkelewelly"
                    onChange={(e) => {
                        setWatermarkText(e.target.value);
                        localStorage.setItem('studio.watermarkText', e.target.value);
                    }}
                    className="w-full rounded-md bg-zinc-950 border border-zinc-800 px-3 py-2 text-sm text-white"
                />
                <div className="text-xs text-zinc-500 mt-1">
                    Burned into the video and used in the description. Blank renders as &quot;Studio&quot;.
                </div>
            </div>

            <div>
                <label className="text-sm text-zinc-300 block mb-1">Visual Brief</label>
                <textarea
                    value={visualBrief}
                    rows={3}
                    placeholder="Art direction for every beat: look, palette, lighting, environment."
                    onChange={(e) => {
                        setVisualBrief(e.target.value);
                        localStorage.setItem('studio.visualBrief', e.target.value);
                    }}
                    className="w-full rounded-md bg-zinc-950 border border-zinc-800 px-3 py-2 text-sm text-white"
                />
                <div className="text-xs text-zinc-500 mt-1">
                    Avoid asking for internal or chest glow - it fights the identity lock.
                </div>
            </div>

            <div>
                <label className="text-sm text-zinc-300 block mb-1">Caption Font</label>
                <select
                    value={captionFont}
                    onChange={(e) => setCaptionFont(e.target.value)}
                    className="w-full rounded-md bg-zinc-950 border border-zinc-800 px-3 py-2 text-sm text-white"
                >
                    <option value="Komika Axis">Komika Axis</option>
                    <option value="Impact">Impact</option>
                    <option value="Bebas Neue">Bebas Neue</option>
                    <option value="Anton">Anton</option>
                </select>
            </div>

            <div>
                <label className="text-sm text-zinc-300 block mb-1">Video model (animation)</label>
                <p className="text-xs text-zinc-500 mb-2">
                    Stills are locked: canonical skeleton + Seedream 4.5 edit only. You choose how each still is animated.
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-4 gap-2">
                    {([
                        { key: 'ltx_budget' as const, title: 'LTX Budget', sub: '3 AC', hint: 'Cheapest full animation' },
                        { key: 'seedance' as const, title: 'Seedance 2.0', sub: 'Default · 5 AC', hint: 'Auto-fallback to Pixverse if flagged' },
                        { key: 'pixverse' as const, title: 'Pixverse V6', sub: '5 AC', hint: 'Permissive moderation' },
                        { key: 'kling_pro' as const, title: 'Kling 2.1 Pro', sub: '7 AC', hint: 'Best motion' },
                    ]).map((m) => (
                        <button
                            key={m.key}
                            onClick={() => setVideoModel(m.key)}
                            className={`text-left rounded-md border px-3 py-2 text-sm ${
                                videoModel === m.key
                                    ? 'border-violet-500 bg-violet-500/10 text-white'
                                    : 'border-zinc-800 bg-zinc-950 text-zinc-300 hover:border-zinc-700'
                            }`}
                        >
                            <div className="font-bold">{m.title}</div>
                            <div className="text-xs text-zinc-400 mt-0.5">{m.sub}</div>
                            <div className="text-[10px] text-zinc-500 mt-0.5">{m.hint}</div>
                        </button>
                    ))}
                </div>
            </div>

            <div className="rounded-md bg-amber-500/10 border border-amber-500/30 px-3 py-2 text-xs text-amber-200">
                Background music is disabled — copyrighted music can take down your video on YouTube Shorts.
            </div>

            <button
                onClick={onGenerate}
                disabled={generating}
                className="w-full rounded-md bg-violet-500 px-4 py-3 text-sm font-bold text-white hover:bg-violet-600 disabled:bg-zinc-800 disabled:text-zinc-500"
            >
                {generating ? (
                    <>
                        <Loader2 className="inline h-4 w-4 animate-spin mr-2" />
                        Generating…
                    </>
                ) : (
                    `Generate Video (${cost} AC)`
                )}
            </button>
        </section>
    );
}

function RangeRow({
    label, value, setValue, min, max, step, suffix,
}: {
    label: string; value: number; setValue: (n: number) => void;
    min: number; max: number; step: number; suffix?: string;
}) {
    return (
        <div>
            <div className="flex items-center justify-between mb-1">
                <label className="text-sm text-zinc-300">{label}</label>
                <span className="text-xs text-zinc-400">{value.toFixed(2)}{suffix || ''}</span>
            </div>
            <input
                type="range"
                min={min}
                max={max}
                step={step}
                value={value}
                onChange={(e) => setValue(parseFloat(e.target.value))}
                className="w-full"
            />
        </div>
    );
}

function IdeaModal({
    accessToken, selectedCategory, onCategoryChange, nicheTitle, onClose, onScript, onStreamStart, onStreamEnd,
}: {
    accessToken: string;
    selectedCategory: string;
    onCategoryChange: (key: string) => void;
    nicheTitle?: string;
    onClose: () => void;
    onScript: (s: string) => void;
    onStreamStart: () => void;
    onStreamEnd: () => void;
}) {
    const [modalTab, setModalTab] = useState<'idea_list' | 'custom_topic' | 'create_category' | 'remix'>('idea_list');
    const [categories, setCategories] = useState<CategoryInfo[]>([]);
    const [customTopic, setCustomTopic] = useState('');
    const [remixUrl, setRemixUrl] = useState('');
    const [remixPlatform, setRemixPlatform] = useState<'youtube' | 'facebook' | 'tiktok' | 'instagram'>('youtube');
    const [busy, setBusy] = useState(false);
    const [newLabel, setNewLabel] = useState('');
    const [newTagline, setNewTagline] = useState('');
    const [newPrompt, setNewPrompt] = useState('');
    const [createError, setCreateError] = useState('');

    const loadCategories = useCallback(() => {
        const headers: Record<string, string> = {};
        if (accessToken) headers.Authorization = `Bearer ${accessToken}`;
        return fetch(resolveStudioBackendUrl('/api/skeleton-ai/categories'), { headers })
            .then((r) => r.json())
            .then((d) => {
                if (Array.isArray(d.categories)) setCategories(d.categories);
            })
            .catch(() => setCategories([]));
    }, [accessToken]);

    useEffect(() => {
        loadCategories();
    }, [loadCategories]);

    const generate = async (topic: string | null) => {
        if (!accessToken) {
            alert('You must be signed in to generate a script.');
            return;
        }
        setBusy(true);
        onStreamStart();
        onScript('');
        const command = acquireProductionCommandLease(
            'skeleton-script',
            `${selectedCategory}-${String(topic || '').slice(0, 80)}`,
        );
        try {
            // Non-streaming fallback: some runner SSE deltas interleave
            // multiple reasoning paths, which produces garbled text mid-stream
            // ("labsserman of vs" etc.). The /script endpoint returns clean
            // {script: "..."} when stream=false, so use that and reveal the
            // result in one write.
            const r = await fetch(resolveStudioBackendUrl('/api/skeleton-ai/script'), {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${accessToken}`,
                    'X-Idempotency-Key': command.commandId,
                },
                body: JSON.stringify({ category: selectedCategory, topic, stream: false }),
            });
            if (!r.ok) {
                const txt = await r.text().catch(() => '');
                command.release();
                throw new Error(`script gen failed: ${r.status} ${txt.slice(0, 200)}`);
            }
            command.release();
            const data = await r.json();
            const text = String(data.script || '').trim();
            if (!text) throw new Error('script gen returned empty content');
            onScript(text);
            onStreamEnd();
            onClose();
        } catch (e) {
            alert((e as Error).message);
            onStreamEnd();
        } finally {
            setBusy(false);
        }
    };

    return (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/70 p-4" onClick={onClose}>
            <div
                className="bg-zinc-950 border border-zinc-800 rounded-lg p-6 max-w-2xl w-full max-h-[85vh] overflow-y-auto"
                onClick={(e) => e.stopPropagation()}
            >
                <div className="flex items-center justify-between mb-4">
                    <h3 className="text-lg font-bold text-white flex items-center gap-2">
                        <Sparkles className="h-5 w-5 text-violet-400" />
                        Generate script with AI{nicheTitle ? ` · ${nicheTitle}` : ''}
                    </h3>
                    <button onClick={onClose} className="text-zinc-400 hover:text-white">
                        <X className="h-5 w-5" />
                    </button>
                </div>
                <div className="text-sm text-zinc-400 mb-4">
                    Pick an idea, enter a custom topic, or remix a script from an existing video.
                </div>

                <div className="grid grid-cols-4 gap-1 mb-4 border-b border-zinc-800">
                    <ModalTab id="idea_list" label="Idea List" current={modalTab} setTab={setModalTab} />
                    <ModalTab id="custom_topic" label="Custom Topic" current={modalTab} setTab={setModalTab} />
                    <ModalTab id="create_category" label="New Category" current={modalTab} setTab={setModalTab} />
                    <ModalTab id="remix" label="Remix Script" current={modalTab} setTab={setModalTab} />
                </div>

                {modalTab === 'idea_list' && (
                    <div>
                        <div className="text-sm font-semibold mb-2 text-white">Category &amp; seeds</div>
                        <div className="max-h-48 overflow-y-auto grid grid-cols-2 gap-2 mb-4 pr-1">
                            {categories.map((c) => (
                                <button
                                    key={c.key}
                                    onClick={() => onCategoryChange(c.key)}
                                    className={`text-left rounded-md border px-3 py-2 ${
                                        selectedCategory === c.key
                                            ? 'border-violet-500 bg-violet-500/5'
                                            : 'border-zinc-800 bg-zinc-900 hover:border-zinc-700'
                                    }`}
                                >
                                    <div className="text-sm font-bold text-white flex items-center gap-1">
                                        {c.label}
                                        {c.custom && (
                                            <span className="text-[10px] text-violet-300 font-normal">custom</span>
                                        )}
                                    </div>
                                    <div className="text-xs text-zinc-400 mt-0.5 line-clamp-2">{c.tagline}</div>
                                </button>
                            ))}
                        </div>
                        <button
                            onClick={() => generate(null)}
                            disabled={busy}
                            className="w-full rounded-md bg-violet-500 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-600 disabled:bg-zinc-800"
                        >
                            {busy ? 'Generating…' : 'Generate Ideas'}
                        </button>
                    </div>
                )}

                {modalTab === 'create_category' && (
                    <div className="space-y-3">
                        <p className="text-xs text-zinc-400">
                            Create a lane for your channel (e.g. Outcast, whistleblower hypotheticals). Saved to your account.
                        </p>
                        <label className="text-sm text-zinc-300 block">Name</label>
                        <input
                            value={newLabel}
                            onChange={(e) => setNewLabel(e.target.value)}
                            placeholder="Outcast"
                            className="w-full rounded-md bg-zinc-900 border border-zinc-800 px-3 py-2 text-sm text-white"
                        />
                        <label className="text-sm text-zinc-300 block">Tagline (optional)</label>
                        <input
                            value={newTagline}
                            onChange={(e) => setNewTagline(e.target.value)}
                            placeholder="Edgy social experiments, contrarian hooks"
                            className="w-full rounded-md bg-zinc-900 border border-zinc-800 px-3 py-2 text-sm text-white"
                        />
                        <label className="text-sm text-zinc-300 block">Tone prompt (optional)</label>
                        <textarea
                            value={newPrompt}
                            onChange={(e) => setNewPrompt(e.target.value)}
                            placeholder="Contrarian thought experiments, anti-establishment framing…"
                            rows={3}
                            className="w-full rounded-md bg-zinc-900 border border-zinc-800 px-3 py-2 text-sm text-white"
                        />
                        {createError && (
                            <div className="text-sm text-rose-300">{createError}</div>
                        )}
                        <button
                            disabled={busy || !newLabel.trim() || !accessToken}
                            onClick={async () => {
                                setCreateError('');
                                setBusy(true);
                                try {
                                    const r = await fetch(resolveStudioBackendUrl('/api/skeleton-ai/categories'), {
                                        method: 'POST',
                                        headers: {
                                            'Content-Type': 'application/json',
                                            Authorization: `Bearer ${accessToken}`,
                                        },
                                        body: JSON.stringify({
                                            label: newLabel.trim(),
                                            tagline: newTagline.trim() || undefined,
                                            system_prompt: newPrompt.trim() || undefined,
                                        }),
            });
            const d = await r.json().catch(() => ({}));
                                    if (!r.ok) {
                                        throw new Error(String(d.detail || `HTTP ${r.status}`));
                                    }
                                    const key = String(d.category?.key || '');
                                    if (key) onCategoryChange(key);
                                    setNewLabel('');
                                    setNewTagline('');
                                    setNewPrompt('');
                                    await loadCategories();
                                    setModalTab('idea_list');
                                } catch (e) {
                                    setCreateError((e as Error).message);
                                } finally {
                                    setBusy(false);
                                }
                            }}
                            className="w-full rounded-md bg-violet-500 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-600 disabled:bg-zinc-800"
                        >
                            {busy ? 'Saving…' : 'Create category'}
                        </button>
                    </div>
                )}

                {modalTab === 'custom_topic' && (
                    <div>
                        <label className="text-sm text-zinc-300 block mb-1">Your topic</label>
                        <textarea
                            value={customTopic}
                            onChange={(e) => setCustomTopic(e.target.value)}
                            placeholder="e.g., What aging really does to your bones year by year"
                            rows={3}
                            className="w-full rounded-md bg-zinc-900 border border-zinc-800 px-3 py-2 text-sm text-white mb-3"
                        />
                        <button
                            onClick={() => generate(customTopic)}
                            disabled={busy || !customTopic.trim()}
                            className="w-full rounded-md bg-violet-500 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-600 disabled:bg-zinc-800"
                        >
                            {busy ? 'Generating…' : 'Generate Script'}
                        </button>
                    </div>
                )}

                {modalTab === 'remix' && (
                    <div>
                        <label className="text-sm text-zinc-300 block mb-1">Source URL</label>
                        <input
                            value={remixUrl}
                            onChange={(e) => setRemixUrl(e.target.value)}
                            placeholder="YouTube / TikTok / Instagram / Facebook URL"
                            className="w-full rounded-md bg-zinc-900 border border-zinc-800 px-3 py-2 text-sm text-white mb-3"
                        />
                        <label className="text-sm text-zinc-300 block mb-1">Platform</label>
                        <div className="grid grid-cols-4 gap-1 mb-3">
                            {(['youtube', 'facebook', 'tiktok', 'instagram'] as const).map((p) => (
                                <button
                                    key={p}
                                    onClick={() => setRemixPlatform(p)}
                                    className={`rounded-md border px-2 py-1.5 text-xs capitalize ${
                                        remixPlatform === p
                                            ? 'border-violet-500 bg-violet-500/10 text-white'
                                            : 'border-zinc-800 bg-zinc-900 text-zinc-400'
                                    }`}
                                >
                                    {p}
                                </button>
                            ))}
                        </div>
                        <button
                            disabled
                            className="w-full rounded-md bg-zinc-800 px-4 py-2 text-sm font-semibold text-zinc-500 cursor-not-allowed"
                            title="Remix endpoint coming soon"
                        >
                            Remix Script (coming soon)
                        </button>
                    </div>
                )}
            </div>
        </div>
    );
}

function ModalTab({
    id, label, current, setTab,
}: {
    id: 'idea_list' | 'custom_topic' | 'create_category' | 'remix';
    label: string;
    current: string;
    setTab: (t: 'idea_list' | 'custom_topic' | 'create_category' | 'remix') => void;
}) {
    return (
        <button
            onClick={() => setTab(id)}
            className={`px-3 py-2 text-sm font-medium border-b-2 ${
                current === id
                    ? 'border-violet-500 text-white'
                    : 'border-transparent text-zinc-400 hover:text-zinc-200'
            }`}
        >
            {label}
        </button>
    );
}
