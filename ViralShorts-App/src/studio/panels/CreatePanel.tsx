/**
 * CreatePanel — Alt-History Battles short-form generator (rebuilt 2026-05-08).
 *
 * Niche taxonomy refactor 2026-05-08: this panel was originally Skeleton AI;
 * Casey replaced it with the Alternate History Battles niche (counterfactual
 * matchups: Napoleon vs Alexander, Mongols vs Romans, etc.). The skeleton_ai
 * backend module is reused — it now serves alt-battles content.
 *
 * 3-tab Korpi-shaped UX:
 *   1. Script  — narration textarea + "Generate w/ AI" modal (Idea List /
 *                Custom Topic / Remix Script). xAI Grok 4.1 Fast Reasoning
 *                streams the script in real time.
 *   2. Scenes  — image-model picker (paid + free tiers) + auto scene plan
 *                from script + Generate Scenes / Generate Scenes & Animate.
 *   3. Audio   — ElevenLabs voice picker + speed/pitch/language + caption
 *                font + final "Generate Video" button.
 *
 * Backend: /api/skeleton-ai/{categories,script,voices,generate,jobs/<id>}
 *          (URL kept for stability — internally serves alt-battles now).
 * Visual:  Kings-and-Generals / Total War / Ridley Scott painterly cinematic
 *          battle illustration. Period-correct gear. NOT photoreal. NOT
 *          modern. Period-correct historical painterly cinematic.
 */
import { useCallback, useContext, useEffect, useRef, useState } from 'react';
import { Sparkles, Wand2, Image as ImageIcon, Music, Loader2, X } from 'lucide-react';
import { AuthContext } from '../shared';
import NichePickerStrip from '../components/create/NichePickerStrip';
import type { NicheId } from '../lib/studioProduct';

type Tab = 'script' | 'scenes' | 'audio';
type IdeaCategory = 'classical_clash' | 'medieval_clash' | 'gunpowder_clash' | 'wildcard_clash';
type ImageModel = 'seedream_45' | 'flux_2_pro' | 'imagen4' | 'recraft_v4_pro' | 'nano_banana_pro' | 'ernie_image' | 'nano_banana_free';
type Tier = 'standard' | 'premium';

interface CategoryInfo {
    key: IdeaCategory;
    label: string;
    tagline: string;
    seeds: string[];
}

interface Voice {
    voice_id: string;
    name: string;
    category?: string;
    preview_url?: string;
    labels?: Record<string, string>;
}

interface ImageModelOption {
    key: ImageModel;
    name: string;
    description: string;
    tier: 'paid' | 'free';
    credits: number;
    speed: 'fast' | 'medium' | 'slow';
}

interface RenderedScene {
    beat_index: number;
    narration: string;
    outfit: string;
    scene_action: string;
    motion_prompt: string;
    image_path: string;
}

const IMAGE_MODELS: ImageModelOption[] = [
    { key: 'seedream_45', name: 'SeeDream 4.5', description: 'High quality with image input support', tier: 'paid', credits: 4, speed: 'fast' },
    { key: 'flux_2_pro', name: 'Flux 2 Pro', description: 'Fast and creative AI art generation', tier: 'paid', credits: 5, speed: 'fast' },
    { key: 'imagen4', name: 'Imagen 4', description: 'Google\'s highest quality model', tier: 'paid', credits: 5, speed: 'medium' },
    { key: 'recraft_v4_pro', name: 'Recraft V4 Pro', description: 'Designer-focused, vector-style outputs', tier: 'paid', credits: 4, speed: 'medium' },
    { key: 'nano_banana_pro', name: 'Nano Banana Pro', description: 'Google\'s premium fast model', tier: 'paid', credits: 5, speed: 'fast' },
    { key: 'ernie_image', name: 'ERNIE Image', description: 'Reliable cheap fallback', tier: 'free', credits: 0, speed: 'fast' },
    { key: 'nano_banana_free', name: 'Nano Banana Free', description: 'Free tier, sometimes falls back to a cheaper model', tier: 'free', credits: 0, speed: 'fast' },
];

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
    categoryKey: categoryKeyProp = 'classical_clash',
    renderTier = 'draft',
    nicheTitle,
    onNicheChange,
    isOwner,
}: CreatePanelProps) {
    const { session } = useContext(AuthContext);
    const accessToken = session?.access_token || '';
    const activeCategory = categoryKeyProp as IdeaCategory;

    const [tab, setTab] = useState<Tab>('script');
    const [script, setScript] = useState('');
    const [ideaModalOpen, setIdeaModalOpen] = useState(false);
    const [scriptStreaming, setScriptStreaming] = useState(false);
    const [imageModel, setImageModel] = useState<ImageModel>('seedream_45');
    const [voiceId, setVoiceId] = useState('');
    const [voiceSpeed, setVoiceSpeed] = useState(1.0);
    const [voicePitch, setVoicePitch] = useState(1.0);
    const [voiceLang, setVoiceLang] = useState('auto');
    const [captionFont, setCaptionFont] = useState('Komika Axis');
    const [voices, setVoices] = useState<Voice[]>([]);
    const [tier, setTier] = useState<Tier>(renderTier === 'ship' ? 'premium' : 'standard');

    useEffect(() => {
        setTier(renderTier === 'ship' ? 'premium' : 'standard');
    }, [renderTier]);
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
    const scenesAbortRef = useRef<AbortController | null>(null);

    // Fetch voices once we have an auth token (voices route is auth-gated).
    useEffect(() => {
        if (!accessToken) return;
        fetch('/api/skeleton-ai/voices', {
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
        // Reset state for a fresh run.
        setScenesGenerating(true);
        setRenderedScenes([]);
        setSceneError('');
        setScenesProgress({ done: 0, total: 0 });

        const controller = new AbortController();
        scenesAbortRef.current = controller;

        try {
            const r = await fetch('/api/skeleton-ai/scenes', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Accept: 'text/event-stream',
                    Authorization: `Bearer ${accessToken}`,
                },
                body: JSON.stringify({ script, image_model: imageModel, category: activeCategory }),
                signal: controller.signal,
            });
            if (!r.ok || !r.body) {
                const txt = await r.text().catch(() => '');
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
                        break outer;
                    }
                }
            }
        } catch (e) {
            const err = e as Error;
            if (err.name !== 'AbortError') {
                setSceneError(err.message || String(err));
            }
        } finally {
            setScenesGenerating(false);
            scenesAbortRef.current = null;
        }
    }, [script, imageModel, accessToken, activeCategory]);

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
        setGenerating(true);
        try {
            const r = await fetch('/api/skeleton-ai/generate', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${accessToken}`,
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
                    tier,
                }),
            });
            const d = await r.json();
            // 402 insufficient_credits → render a structured Top Up prompt.
            if (r.status === 402) {
                const detail = d?.detail || {};
                if (detail?.code === 'insufficient_credits') {
                    const needed = Number(detail.needed || 0);
                    const have = Number(detail.have || 0);
                    const tierName = String(detail.tier || tier).toUpperCase();
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
            const msg = typeof d.detail === 'string' ? d.detail
                : (d.detail?.code || d.error || `HTTP ${r.status}`);
            alert(`Generation failed: ${msg}`);
        } finally {
            setGenerating(false);
        }
    }, [script, imageModel, voiceId, voiceSpeed, voicePitch, voiceLang, captionFont, tier, accessToken, activeCategory, renderTier]);

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
            {tab === 'scenes' && (
                <ScenesTab
                    imageModel={imageModel}
                    setImageModel={setImageModel}
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
                    tier={tier}
                    setTier={setTier}
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
                    initialCategory={activeCategory}
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
                    <Loader2 className="h-3 w-3 animate-spin" /> Streaming from xAI Grok...
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
    imageModel, setImageModel, charCount, duration, estimatedScenes, scriptValid,
    onGenerateScenes, onStopGenerateScenes, onGenerateAndAnimate,
    scenesGenerating, renderedScenes, scenesProgress, sceneError, accessToken,
}: {
    imageModel: ImageModel;
    setImageModel: (m: ImageModel) => void;
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
}) {
    const [pickerOpen, setPickerOpen] = useState(false);
    const selected = IMAGE_MODELS.find((m) => m.key === imageModel)!;
    return (
        <section className="flex flex-col gap-4">
            <h2 className="text-lg font-semibold text-white">Generate Scenes</h2>

            <div>
                <label className="text-sm text-zinc-300 block mb-1">Image Generation Model</label>
                <button
                    onClick={() => setPickerOpen(true)}
                    className="w-full text-left rounded-md bg-zinc-950 border border-zinc-800 px-3 py-3 hover:border-zinc-700 flex items-center justify-between"
                >
                    <div>
                        <div className="text-sm font-semibold text-white">{selected.name}</div>
                        <div className="text-xs text-zinc-500">{selected.description}</div>
                    </div>
                    <div className="text-xs text-zinc-400">
                        {selected.tier === 'paid' ? `${selected.credits} credits/img` : 'Free'}
                    </div>
                </button>
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
                        disabled={!scriptValid}
                        onClick={onGenerateScenes}
                        className="w-full rounded-md bg-violet-500 px-4 py-2.5 text-sm font-semibold text-white hover:bg-violet-600 disabled:bg-zinc-800 disabled:text-zinc-500 disabled:cursor-not-allowed flex items-center justify-center gap-2"
                    >
                        <ImageIcon className="h-4 w-4" />
                        Generate Scenes
                    </button>
                )}
                <button
                    disabled={!scriptValid || scenesGenerating}
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
                    <SceneGallery scenes={renderedScenes} accessToken={accessToken} />
                </>
            )}

            {!scriptValid && (
                <div className="rounded-md bg-amber-500/10 border border-amber-500/30 px-3 py-2 text-sm text-amber-200">
                    Please add or generate a script on the Script tab before generating scenes.
                </div>
            )}

            {pickerOpen && (
                <ImageModelPicker
                    selected={imageModel}
                    onSelect={(m) => { setImageModel(m); setPickerOpen(false); }}
                    onClose={() => setPickerOpen(false)}
                />
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

function SceneGallery({ scenes, accessToken }: { scenes: RenderedScene[]; accessToken: string }) {
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
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3 mt-2">
            {scenes.map((s) => (
                <div key={s.beat_index} className="rounded-md overflow-hidden border border-zinc-800 bg-zinc-950">
                    <div className="aspect-[9/16] bg-zinc-900 flex items-center justify-center">
                        {blobUrls[s.beat_index] ? (
                            <img src={blobUrls[s.beat_index]} alt={`Beat ${s.beat_index + 1}`} className="w-full h-full object-cover" />
                        ) : (
                            <Loader2 className="h-6 w-6 text-zinc-600 animate-spin" />
                        )}
                    </div>
                    <div className="px-2 py-1.5 text-[10px] text-zinc-400 truncate">
                        <span className="text-zinc-500">{`b${String(s.beat_index).padStart(2, '0')}: `}</span>
                        {s.narration}
                    </div>
                </div>
            ))}
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

function ImageModelPicker({
    selected, onSelect, onClose,
}: {
    selected: ImageModel;
    onSelect: (m: ImageModel) => void;
    onClose: () => void;
}) {
    return (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/70 p-4" onClick={onClose}>
            <div
                className="bg-zinc-950 border border-zinc-800 rounded-lg p-6 max-w-3xl w-full max-h-[80vh] overflow-y-auto"
                onClick={(e) => e.stopPropagation()}
            >
                <div className="flex items-center justify-between mb-4">
                    <h3 className="text-lg font-bold text-white">Select Image Generation Model</h3>
                    <button onClick={onClose} className="text-zinc-400 hover:text-white">
                        <X className="h-5 w-5" />
                    </button>
                </div>
                <div className="grid grid-cols-3 gap-3">
                    {IMAGE_MODELS.map((m) => (
                        <button
                            key={m.key}
                            onClick={() => onSelect(m.key)}
                            className={`text-left rounded-md border p-3 transition ${
                                selected === m.key
                                    ? 'border-violet-500 bg-violet-500/5'
                                    : 'border-zinc-800 bg-zinc-900 hover:border-zinc-700'
                            }`}
                        >
                            <div className="text-sm font-bold text-white">{m.name}</div>
                            <div className="text-xs text-zinc-400 mt-1 mb-3">{m.description}</div>
                            <div className="flex items-center justify-between">
                                <span className="text-[10px] uppercase tracking-wide font-bold text-zinc-500">
                                    {m.speed}
                                </span>
                                <span
                                    className={`text-xs font-semibold ${
                                        m.tier === 'free' ? 'text-emerald-400' : 'text-violet-400'
                                    }`}
                                >
                                    {m.tier === 'free' ? 'Free' : `${m.credits} credits`}
                                </span>
                            </div>
                        </button>
                    ))}
                </div>
            </div>
        </div>
    );
}

function AudioTab({
    voices, voiceId, setVoiceId, voiceSpeed, setVoiceSpeed, voicePitch, setVoicePitch,
    voiceLang, setVoiceLang, captionFont, setCaptionFont, tier, setTier,
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
    setCaptionFont: (s: string) => void;
    tier: Tier;
    setTier: (t: Tier) => void;
    onGenerate: () => void;
    generating: boolean;
}) {
    const cost = tier === 'premium' ? 7 : 5;
    return (
        <section className="flex flex-col gap-4">
            <h2 className="text-lg font-semibold text-white">Narration Voice</h2>

            <div>
                <label className="text-sm text-zinc-300 block mb-1">ElevenLabs Voice</label>
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
                    {voices.length === 0 ? 'No voices loaded — check ELEVENLABS_API_KEY.' : `${voices.length} voices available.`}
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
                <label className="text-sm text-zinc-300 block mb-1">Quality Tier</label>
                <div className="grid grid-cols-2 gap-2">
                    <button
                        onClick={() => setTier('standard')}
                        className={`rounded-md border px-3 py-2 text-sm ${
                            tier === 'standard'
                                ? 'border-violet-500 bg-violet-500/10 text-white'
                                : 'border-zinc-800 bg-zinc-950 text-zinc-300 hover:border-zinc-700'
                        }`}
                    >
                        <div className="font-bold">Standard</div>
                        <div className="text-xs text-zinc-400 mt-0.5">Seedance 2.0 i2v · 5 AC</div>
                    </button>
                    <button
                        onClick={() => setTier('premium')}
                        className={`rounded-md border px-3 py-2 text-sm ${
                            tier === 'premium'
                                ? 'border-violet-500 bg-violet-500/10 text-white'
                                : 'border-zinc-800 bg-zinc-950 text-zinc-300 hover:border-zinc-700'
                        }`}
                    >
                        <div className="font-bold">Premium</div>
                        <div className="text-xs text-zinc-400 mt-0.5">Kling 2.1 Pro i2v · 7 AC</div>
                    </button>
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
    accessToken, initialCategory, nicheTitle, onClose, onScript, onStreamStart, onStreamEnd,
}: {
    accessToken: string;
    initialCategory?: IdeaCategory;
    nicheTitle?: string;
    onClose: () => void;
    onScript: (s: string) => void;
    onStreamStart: () => void;
    onStreamEnd: () => void;
}) {
    const [modalTab, setModalTab] = useState<'idea_list' | 'custom_topic' | 'remix'>('idea_list');
    const [categories, setCategories] = useState<CategoryInfo[]>([]);
    const [selectedCat, setSelectedCat] = useState<IdeaCategory>(initialCategory || 'classical_clash');
    const [customTopic, setCustomTopic] = useState('');
    const [remixUrl, setRemixUrl] = useState('');
    const [remixPlatform, setRemixPlatform] = useState<'youtube' | 'facebook' | 'tiktok' | 'instagram'>('youtube');
    const [busy, setBusy] = useState(false);

    // Categories endpoint is unauthed — populates the Idea List tab.
    useEffect(() => {
        fetch('/api/skeleton-ai/categories')
            .then((r) => r.json())
            .then((d) => Array.isArray(d.categories) && setCategories(d.categories))
            .catch(() => setCategories([]));
    }, []);

    const generate = async (topic: string | null) => {
        if (!accessToken) {
            alert('You must be signed in to generate a script.');
            return;
        }
        setBusy(true);
        onStreamStart();
        onScript('');
        try {
            // Non-streaming: grok-4-fast-reasoning's SSE deltas interleave
            // multiple reasoning paths, which produces garbled text mid-stream
            // ("labsserman of vs" etc.). The /script endpoint returns clean
            // {script: "..."} when stream=false, so use that and reveal the
            // result in one write.
            const r = await fetch('/api/skeleton-ai/script', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${accessToken}`,
                },
                body: JSON.stringify({ category: selectedCat, topic, stream: false }),
            });
            if (!r.ok) {
                const txt = await r.text().catch(() => '');
                throw new Error(`script gen failed: ${r.status} ${txt.slice(0, 200)}`);
            }
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

                <div className="grid grid-cols-3 gap-1 mb-4 border-b border-zinc-800">
                    <ModalTab id="idea_list" label="Idea List" current={modalTab} setTab={setModalTab} />
                    <ModalTab id="custom_topic" label="Custom Topic" current={modalTab} setTab={setModalTab} />
                    <ModalTab id="remix" label="Remix Script" current={modalTab} setTab={setModalTab} />
                </div>

                {modalTab === 'idea_list' && (
                    <div>
                        <div className="text-sm font-semibold mb-2 text-white">Idea Style</div>
                        <div className="grid grid-cols-2 gap-2 mb-4">
                            {categories.map((c) => (
                                <button
                                    key={c.key}
                                    onClick={() => setSelectedCat(c.key)}
                                    className={`text-left rounded-md border px-3 py-2 ${
                                        selectedCat === c.key
                                            ? 'border-violet-500 bg-violet-500/5'
                                            : 'border-zinc-800 bg-zinc-900 hover:border-zinc-700'
                                    }`}
                                >
                                    <div className="text-sm font-bold text-white">{c.label}</div>
                                    <div className="text-xs text-zinc-400 mt-0.5">{c.tagline}</div>
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
    id: 'idea_list' | 'custom_topic' | 'remix';
    label: string;
    current: string;
    setTab: (t: 'idea_list' | 'custom_topic' | 'remix') => void;
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
