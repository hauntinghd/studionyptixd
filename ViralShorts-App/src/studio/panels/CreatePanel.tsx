/**
 * CreatePanel — Skeleton AI short-form generator (rebuilt 2026-05-05).
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
 * Spec:    project_skeleton_spec_canonical.md (mint green BG, anatomical
 *          white skull with hollow dark sockets + dot pupils, real opaque
 *          clothing, ~12 narration beats per 60s, 2-tier captions).
 */
import { useCallback, useContext, useEffect, useState } from 'react';
import { Sparkles, Wand2, Image as ImageIcon, Music, Loader2, X } from 'lucide-react';
import { AuthContext } from '../shared';

type Tab = 'script' | 'scenes' | 'audio';
type IdeaCategory = 'human_limits' | 'marvel_vs_dc' | 'ancient_history' | 'futuristic_socrates';
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
    initialTemplate?: string;
}

export default function CreatePanel(_props: CreatePanelProps) {
    const { session } = useContext(AuthContext);
    const accessToken = session?.access_token || '';

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
    const [tier, setTier] = useState<Tier>('standard');
    const [generating, setGenerating] = useState(false);
    const [generatedVideoUrl, setGeneratedVideoUrl] = useState<string | null>(null);

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

    const startGenerate = useCallback(async () => {
        if (!script.trim()) {
            alert('Add or generate a skeleton script in Step 1 before generating.');
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
                    script,
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
            if (d.video_path) setGeneratedVideoUrl(d.video_path);
            else alert(`Generation failed: ${d.detail || d.error || 'unknown'}`);
        } finally {
            setGenerating(false);
        }
    }, [script, imageModel, voiceId, voiceSpeed, voicePitch, voiceLang, captionFont, tier, accessToken]);

    return (
        <div className="flex flex-col gap-6 px-6 py-8 max-w-5xl mx-auto">
            <header className="flex items-center justify-between">
                <h1 className="text-2xl font-bold text-white">Skeleton AI</h1>
                <div className="text-xs text-zinc-400">
                    Standard short = 5 AC · Premium short = 7 AC
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
                    onClose={() => setIdeaModalOpen(false)}
                    onScript={(text) => {
                        setScript(text);
                        setIdeaModalOpen(false);
                        setScriptStreaming(false);
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
    script, setScript, streaming, charCount, duration, onOpenIdeaModal,
}: {
    script: string;
    setScript: (s: string) => void;
    streaming: boolean;
    charCount: number;
    duration: number;
    onOpenIdeaModal: () => void;
}) {
    return (
        <section className="flex flex-col gap-3">
            <div className="flex items-center justify-between">
                <h2 className="text-lg font-semibold text-white">Skeleton AI Script</h2>
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
                placeholder="Enter your skeleton narration script..."
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
}: {
    imageModel: ImageModel;
    setImageModel: (m: ImageModel) => void;
    charCount: number;
    duration: number;
    estimatedScenes: number;
    scriptValid: boolean;
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
                <button
                    disabled={!scriptValid}
                    className="w-full rounded-md bg-violet-500 px-4 py-2.5 text-sm font-semibold text-white hover:bg-violet-600 disabled:bg-zinc-800 disabled:text-zinc-500 disabled:cursor-not-allowed"
                >
                    <ImageIcon className="inline h-4 w-4 mr-2" />
                    Generate Scenes
                </button>
                <button
                    disabled={!scriptValid}
                    className="w-full rounded-md bg-zinc-900 px-4 py-2.5 text-sm font-semibold text-white border border-zinc-800 hover:bg-zinc-800 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                    Generate Scenes & Animate
                </button>
            </div>

            {!scriptValid && (
                <div className="rounded-md bg-amber-500/10 border border-amber-500/30 px-3 py-2 text-sm text-amber-200">
                    Please add or generate a skeleton script in Step 1 before generating scenes.
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
    accessToken, onClose, onScript, onStreamStart, onStreamEnd,
}: {
    accessToken: string;
    onClose: () => void;
    onScript: (s: string) => void;
    onStreamStart: () => void;
    onStreamEnd: () => void;
}) {
    const [modalTab, setModalTab] = useState<'idea_list' | 'custom_topic' | 'remix'>('idea_list');
    const [categories, setCategories] = useState<CategoryInfo[]>([]);
    const [selectedCat, setSelectedCat] = useState<IdeaCategory>('human_limits');
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
                        Generate Skeleton Script with AI
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
