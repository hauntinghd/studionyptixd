import { useCallback, useContext, useEffect, useMemo, useRef, useState, type WheelEvent } from 'react';
import { ArrowRight, CheckCircle2, Clapperboard, Clock, Download, Film, Image, Loader2, Lock, Plus, Sliders, Sparkles, Trash2, Wand2, X } from 'lucide-react';
import { API, AuthContext, CREATE_WORKFLOW_PERSISTENCE_ENABLED, GENERATION_API, Logo, startYouTubeBrowserConnect } from '../shared';
import { FeedbackWidget, JobDiagnostics, ProgressBar, RenderProgressWindow } from '../components/StudioWidgets';
import ChatStoryPanel from './ChatStoryPanel';
import { storyArtStyleOptions } from '../lib/storyArtStyleCatalog';
import { customVoiceLibrary, customVoicePresetMap } from '../lib/studioVoiceLibrary';
import { trackShortProjectStarted, trackShortRenderCompleted } from '../lib/googleAds';

interface CreativeScene {
    index: number;
    narration: string;
    visual_description: string;
    negative_prompt?: string;
    duration_sec: number;
    imageData?: string;
    imageLoading?: boolean;
    generation_id?: string;
    imageError?: string;
    qa_ok?: boolean;
    qa_score?: number;
    qa_notes?: string[];
}

interface CreatePanelPersistedState {
    selectedTemplate: string;
    resolution: '720p' | '1080p';
    language: string;
    creativeMode: 'auto' | 'creative' | 'script_to_short';
    creativeStep: 'topic' | 'edit' | 'generating';
    prompt: string;
    sessionId: string | null;
    creativeScenes: CreativeScene[];
    creativeTitle: string;
    creativeNarration: string;
    creativeReferenceLockMode?: 'strict' | 'inspired';
    animateOutputEnabled?: boolean;
    storyAnimationEnabled?: boolean;
    storyVoiceId?: string;
    storyVoiceSpeed?: number;
    storyPacingMode?: 'standard' | 'fast' | 'very_fast';
    artStyle?: string;
    imageModelId?: string;
    videoModelId?: string;
    cinematicBoostEnabled?: boolean;
    createSubTab?: 'builder' | 'projects';
    workspaceStage?: 'script' | 'scenes' | 'audio';
    subtitlesEnabled?: boolean;
    voiceProvider?: 'custom' | 'elevenlabs';
    customVoiceId?: string;
    voicePitch?: number;
    captionFont?: string;
    backgroundMusic?: string;
    soundReferencePreset?: string;
    youtubeChannelId?: string;
    trendHuntEnabled?: boolean;
    jobId: string | null;
    ts: number;
}

interface ProjectRow {
    project_id: string;
    template: string;
    topic: string;
    mode: string;
    status: string;
    created_at: number;
    updated_at: number;
    scene_count?: number;
    session_id?: string;
    job_id?: string;
    output_file?: string;
    title?: string;
    resolution?: string;
    language?: string;
    animation_enabled?: boolean;
    story_animation_enabled?: boolean;
    scenes?: CreativeScene[];
    narration?: string;
    voice_id?: string;
    voice_speed?: number;
    pacing_mode?: 'standard' | 'fast' | 'very_fast';
    art_style?: string;
    image_model_id?: string;
    video_model_id?: string;
    youtube_channel_id?: string;
    trend_hunt_enabled?: boolean;
    cinematic_boost?: boolean;
    error?: string;
}

interface ConnectedYouTubeChannel {
    channel_id: string;
    title: string;
    channel_handle?: string;
    analytics_snapshot?: {
        channel_summary?: string;
        recent_upload_titles?: string[];
        top_video_titles?: string[];
    };
    last_sync_error?: string;
}

interface CreativeModelProfile {
    id: string;
    label: string;
    provider: string;
    tier: 'basic' | 'premium' | 'elite';
    summary: string;
    speed: string;
    enabled: boolean;
    estimated_unit_usd?: number;
    billing_unit?: string;
    credit_cost_per_image?: number;
    credit_multiplier?: number;
}

function SceneImageLoadingCard({ template }: { template: string }) {
    void template;
    const brandLabel = 'NYPTID Studio';
    return (
        <div className="rounded-lg bg-black/40 p-3">
            <div className="relative mx-auto flex h-[360px] max-h-[55vh] w-full max-w-[320px] items-center justify-center overflow-hidden rounded-md border border-violet-500/20 bg-[radial-gradient(circle_at_top,_rgba(139,92,246,0.24),_rgba(10,10,10,0.96)_55%)]">
                <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(139,92,246,0.06),rgba(6,182,212,0.04))]" />
                <div className="absolute h-48 w-48 rounded-full border border-cyan-400/20 animate-ping" />
                <div className="absolute h-64 w-64 rounded-full border border-violet-400/15 animate-pulse" />
                <div className="relative z-10 flex flex-col items-center gap-4 px-6 text-center">
                    <div className="flex h-20 w-20 items-center justify-center rounded-full border border-white/10 bg-black/35 shadow-[0_0_45px_rgba(139,92,246,0.25)]">
                        <Logo size={56} />
                    </div>
                    <div className="space-y-1">
                        <p className="text-sm font-semibold tracking-[0.24em] text-cyan-100/90 uppercase">{brandLabel}</p>
                        <p className="text-lg font-semibold text-white">Generating scene image</p>
                        <p className="text-sm text-gray-300">
                            Your logo-first preview stays here while NYPTID Studio renders the scene.
                        </p>
                    </div>
                    <div className="flex items-center gap-2 text-xs font-medium text-violet-200">
                        <Loader2 className="h-4 w-4 animate-spin" />
                        Rendering thumbnail canvas...
                    </div>
                </div>
            </div>
        </div>
    );
}

const finaleCaptionFonts = ['Komika Axis', 'Montserrat Bold', 'Anton', 'Bebas Neue', 'Satoshi', 'Oswald', 'Archivo Black', 'League Spartan', 'Teko', 'Playfair Display'];
const finaleMusicOptions = ['No Background Music', 'Dark Ambient', 'Cinematic Tension', 'Upbeat Energy', 'Auto (Match Template)'];
const backgroundMusicComingSoon = false;
const soundReferenceOptions = [
    { id: 'none', label: 'No Sound Reference', desc: 'Clean default timing and effects.' },
    { id: 'cinematic_impacts', label: 'Cinematic Impacts', desc: 'Trailer-style hits and transitions.' },
    { id: 'dark_tension', label: 'Dark Tension', desc: 'Low suspense bed with punchy accents.' },
    { id: 'social_hook', label: 'Social Hook', desc: 'Fast short-form pacing with crisp emphasis hits.' },
];
const fallbackImageModelCatalog: CreativeModelProfile[] = [
    { id: 'studio_default', label: 'Studio Default', provider: 'nyptid_hybrid', tier: 'basic', summary: "NYPTID's tuned hybrid lane for the best continuity.", speed: 'Balanced', enabled: true, estimated_unit_usd: 0.02, billing_unit: 'image', credit_cost_per_image: 0 },
    { id: 'grok_imagine', label: 'Grok Imagine', provider: 'fal', tier: 'basic', summary: 'Fast default image lane through fal.ai.', speed: 'Fast', enabled: true, estimated_unit_usd: 0.02, billing_unit: 'image', credit_cost_per_image: 0 },
    { id: 'imagen4_fast', label: 'Imagen 4 Fast', provider: 'fal', tier: 'basic', summary: "Google's faster image lane for quick scene passes.", speed: 'Very Fast', enabled: true, estimated_unit_usd: 0.02, billing_unit: 'image', credit_cost_per_image: 0 },
    { id: 'imagen4_ultra', label: 'Imagen 4 Ultra', provider: 'fal', tier: 'premium', summary: "Google's highest-quality text-to-image lane.", speed: 'Medium', enabled: true, estimated_unit_usd: 0.06, billing_unit: 'image', credit_cost_per_image: 4 },
    { id: 'recraft_v4', label: 'Recraft V4', provider: 'fal', tier: 'premium', summary: 'Design-first image generation with cleaner composition and ad polish.', speed: 'Medium', enabled: true, estimated_unit_usd: 0.04, billing_unit: 'image', credit_cost_per_image: 4 },
    { id: 'seedream45', label: 'Seedream 4.5', provider: 'fal', tier: 'premium', summary: 'High-end prompt adherence with polished commercial quality.', speed: 'Medium', enabled: true, estimated_unit_usd: 0.04, billing_unit: 'image', credit_cost_per_image: 4 },
    { id: 'flux_2_pro', label: 'FLUX 2 Pro', provider: 'fal', tier: 'premium', summary: 'High-fidelity prompt rendering with strong cinematic framing.', speed: 'Medium', enabled: true, estimated_unit_usd: 0.03, billing_unit: 'processed_megapixels', credit_cost_per_image: 4 },
    { id: 'nano_banana_pro', label: 'Nano Banana Pro', provider: 'fal', tier: 'elite', summary: 'Premium reasoning-based image generation with strong composition.', speed: 'Medium', enabled: true, estimated_unit_usd: 0.15, billing_unit: 'image', credit_cost_per_image: 5 },
    { id: 'recraft_v4_pro', label: 'Recraft V4 Pro', provider: 'fal', tier: 'elite', summary: 'Designer-grade generation for top-end ad and thumbnail style work.', speed: 'Slow', enabled: true, estimated_unit_usd: 0.25, billing_unit: 'image', credit_cost_per_image: 5 },
];
const fallbackVideoModelCatalog: CreativeModelProfile[] = [
    { id: 'kling21_standard', label: 'Kling 2.1 Standard', provider: 'fal', tier: 'basic', summary: 'Default animation lane for Studio renders.', speed: 'Balanced', enabled: true, estimated_unit_usd: 0.056, billing_unit: 'second', credit_multiplier: 1 },
    { id: 'kling21_pro', label: 'Kling 2.1 Pro', provider: 'fal', tier: 'premium', summary: 'Sharper motion and stronger camera handling.', speed: 'Balanced', enabled: true, estimated_unit_usd: 0.098, billing_unit: 'second', credit_multiplier: 4 },
    { id: 'veo3_fast', label: 'Veo 3 Fast', provider: 'fal', tier: 'premium', summary: 'Premium cinematic motion with heavier wallet burn.', speed: 'Slow', enabled: true, estimated_unit_usd: 0.1, billing_unit: 'second', credit_multiplier: 4 },
    { id: 'kling21_master', label: 'Kling 2.1 Master', provider: 'fal', tier: 'elite', summary: 'Highest-cost Kling lane for top-end shot quality.', speed: 'Slow', enabled: true, estimated_unit_usd: 0.28, billing_unit: 'second', credit_multiplier: 5 },
];

export default function CreatePanel() {
    const { session, role, creditsTotalRemaining, requiresTopup, checkout, checkoutTopup, topupPacks } = useContext(AuthContext);
    const isAdmin = role === 'admin';
    const [prompt, setPrompt] = useState("");
    const [selectedTemplate, setSelectedTemplate] = useState('skeleton');
    const [resolution, setResolution] = useState<'720p' | '1080p'>('720p');
    const [jobId, setJobId] = useState<string | null>(null);
    const [jobStatus, setJobStatus] = useState<any>(null);
    const [loading, setLoading] = useState(false);
    const [language, setLanguage] = useState('en');
    const [languages, setLanguages] = useState<{code: string; name: string}[]>([]);
    const [creativeMode, setCreativeMode] = useState<'auto' | 'creative' | 'script_to_short'>('auto');
    const [creativeStep, setCreativeStep] = useState<'topic' | 'edit' | 'generating'>('topic');
    const [sessionId, setSessionId] = useState<string | null>(null);
    const [creativeScenes, setCreativeScenes] = useState<CreativeScene[]>([]);
    const creativeScenesRef = useRef<CreativeScene[]>([]);
    creativeScenesRef.current = creativeScenes;
    const [scriptLoading, setScriptLoading] = useState(false);
    const [creativeTitle, setCreativeTitle] = useState("");
    const [creativeNarration, setCreativeNarration] = useState("");
    const [creativeReferenceImage, setCreativeReferenceImage] = useState<File | null>(null);
    const [creativeReferenceStatus, setCreativeReferenceStatus] = useState<'idle' | 'uploading' | 'ready' | 'error'>('idle');
    const [creativeReferenceAttached, setCreativeReferenceAttached] = useState(false);
    const [creativeReferenceLockMode, setCreativeReferenceLockMode] = useState<'strict' | 'inspired'>('strict');
    const [animateOutputEnabled, setAnimateOutputEnabled] = useState(true);
    const [storyAnimationEnabled, setStoryAnimationEnabled] = useState(true);
    const [storyVoiceId, setStoryVoiceId] = useState(customVoiceLibrary[0]?.backingVoiceId || "");
    const [storyVoiceSpeed, setStoryVoiceSpeed] = useState(1);
    const [storyPacingMode, setStoryPacingMode] = useState<'standard' | 'fast' | 'very_fast'>('standard');
    const [artStyle, setArtStyle] = useState('auto');
    const [imageModelCatalog, setImageModelCatalog] = useState<CreativeModelProfile[]>(fallbackImageModelCatalog);
    const [videoModelCatalog, setVideoModelCatalog] = useState<CreativeModelProfile[]>(fallbackVideoModelCatalog);
    const [imageModelId, setImageModelId] = useState('studio_default');
    const [videoModelId, setVideoModelId] = useState('kling21_standard');
    const [imageModelPickerOpen, setImageModelPickerOpen] = useState(false);
    const [videoModelPickerOpen, setVideoModelPickerOpen] = useState(false);
    const [cinematicBoostEnabled, setCinematicBoostEnabled] = useState(true);
    const [storyVoices, setStoryVoices] = useState<any[]>([]);
    const [storyVoicesLoading, setStoryVoicesLoading] = useState(false);
    const [storyPreviewLoading, setStoryPreviewLoading] = useState(false);
    const [storyPreviewError, setStoryPreviewError] = useState('');
    const [storyVoicesSource, setStoryVoicesSource] = useState<'unknown' | 'elevenlabs' | 'fallback'>('unknown');
    const [storyVoicesWarning, setStoryVoicesWarning] = useState('');
    const [sceneBuildLoading, setSceneBuildLoading] = useState(false);
    const [sceneBuildError, setSceneBuildError] = useState<string | null>(null);
    const [scriptScenesReady, setScriptScenesReady] = useState(false);
    const [subtitlesEnabled, setSubtitlesEnabled] = useState(true);
    const [bulkImageGenRunning, setBulkImageGenRunning] = useState(false);
    const [bulkImageGenDone, setBulkImageGenDone] = useState(0);
    const [bulkImageGenTotal, setBulkImageGenTotal] = useState(0);
    const [createSubTab, setCreateSubTab] = useState<'builder' | 'projects'>('builder');
    const [workspaceStage, setWorkspaceStage] = useState<'script' | 'scenes' | 'audio'>('script');
    const [scenePromptEditorIndex, setScenePromptEditorIndex] = useState<number | null>(null);
    const [projectDrafts, setProjectDrafts] = useState<ProjectRow[]>([]);
    const [projectRenders, setProjectRenders] = useState<ProjectRow[]>([]);
    const [projectsLoading, setProjectsLoading] = useState(false);
    const [projectsError, setProjectsError] = useState<string | null>(null);
    const [finalizeError, setFinalizeError] = useState<string | null>(null);
    const [regeneratingAutoScenes, setRegeneratingAutoScenes] = useState<Record<number, boolean>>({});
    const [showQuickStart, setShowQuickStart] = useState(false);
    const [quickStartStep, setQuickStartStep] = useState(0);
    const [voiceProvider, setVoiceProvider] = useState<'custom' | 'elevenlabs'>('elevenlabs');
    const [customVoiceId, setCustomVoiceId] = useState(customVoiceLibrary[0].id);
    const [voicePitch, setVoicePitch] = useState(1);
    const [captionFont, setCaptionFont] = useState(finaleCaptionFonts[0]);
    const [backgroundMusic, setBackgroundMusic] = useState(finaleMusicOptions[0]);
    const [soundReferencePreset, setSoundReferencePreset] = useState(soundReferenceOptions[0].id);
    const [youtubeChannels, setYoutubeChannels] = useState<ConnectedYouTubeChannel[]>([]);
    const [youtubeChannelId, setYoutubeChannelId] = useState('');
    const [trendHuntEnabled, setTrendHuntEnabled] = useState(false);
    const [youtubeLoading, setYoutubeLoading] = useState(false);
    const [youtubeError, setYoutubeError] = useState('');
    const [youtubeConnecting, setYoutubeConnecting] = useState(false);
    const [generateError, setGenerateError] = useState<string | null>(null);
    const [templateChooserOpen, setTemplateChooserOpen] = useState(false);
    const [subscriptionPromptOpen, setSubscriptionPromptOpen] = useState(false);
    const [subscriptionCheckoutPlan, setSubscriptionCheckoutPlan] = useState<string | null>(null);
    const [subscriptionPromptError, setSubscriptionPromptError] = useState<string | null>(null);
    const [animationCreditPromptRequired, setAnimationCreditPromptRequired] = useState<number | null>(null);
    const [animationCreditPromptMode, setAnimationCreditPromptMode] = useState<'video' | 'image'>('video');
    const [animationCreditPromptError, setAnimationCreditPromptError] = useState<string | null>(null);
    const [renderMonitorDismissed, setRenderMonitorDismissed] = useState(false);
    // Remix Script state — pull captions from a TikTok/YouTube/IG URL and drop into the textarea.
    const [remixUrl, setRemixUrl] = useState('');
    const [remixLoading, setRemixLoading] = useState(false);
    const [remixError, setRemixError] = useState('');
    const [remixSourceTitle, setRemixSourceTitle] = useState('');
    const [remixWarning, setRemixWarning] = useState('');
    const restoreDoneRef = useRef(false);
    const hydratedSceneImagesSessionRef = useRef<string | null>(null);
    const lastTrackedCompletedJobRef = useRef('');
    const persistKey = session ? `nyptid_create_state_${session.user.id}` : "nyptid_create_state_guest";
    const quickStartSeenKey = session ? `nyptid_quickstart_seen_${session.user.id}` : "nyptid_quickstart_seen_guest";
    const pendingChatStoryTemplateStorageKey = session ? `nyptid_pending_chatstory_${session.user.id}` : "nyptid_pending_chatstory_guest";
    const activePromptEditorScene = scenePromptEditorIndex !== null ? creativeScenes[scenePromptEditorIndex] : null;
    const selectedImageModel = useMemo(
        () => imageModelCatalog.find((model) => model.id === imageModelId) || fallbackImageModelCatalog.find((model) => model.id === imageModelId) || fallbackImageModelCatalog[0],
        [imageModelCatalog, imageModelId]
    );
    const selectedVideoModel = useMemo(
        () => videoModelCatalog.find((model) => model.id === videoModelId) || fallbackVideoModelCatalog.find((model) => model.id === videoModelId) || fallbackVideoModelCatalog[0],
        [videoModelCatalog, videoModelId]
    );
    const skeletonSceneModelLocked = selectedTemplate === 'skeleton';
    const sceneImageModelOptions = useMemo(() => {
        const enabledModels = imageModelCatalog.filter((model) => model.enabled !== false);
        if (!skeletonSceneModelLocked) return enabledModels;
        const lockedModels = enabledModels.filter((model) => model.id === 'grok_imagine');
        if (lockedModels.length > 0) return lockedModels;
        const fallbackGrok = fallbackImageModelCatalog.find((model) => model.id === 'grok_imagine');
        return fallbackGrok ? [fallbackGrok] : enabledModels;
    }, [imageModelCatalog, skeletonSceneModelLocked]);

    useEffect(() => {
        if (!skeletonSceneModelLocked) return;
        if (imageModelId !== 'grok_imagine') {
            setImageModelId('grok_imagine');
        }
    }, [skeletonSceneModelLocked, imageModelId]);

    useEffect(() => {
        (async () => {
            try {
                const res = await fetch(`${API}/api/languages`);
                if (res.ok) {
                    const { data } = await readJsonResponse<{ languages?: { code: string; name: string }[] }>(res);
                    setLanguages(Array.isArray(data?.languages) ? data.languages : []);
                }
            } catch { /* silent */ }
        })();
    }, []);

    useEffect(() => {
        let cancelled = false;
        (async () => {
            try {
                const res = await fetch(`${API}/api/config`);
                if (!res.ok) return;
                const { data } = await readJsonResponse<any>(res);
                const catalog = data?.creative_model_catalog;
                if (!catalog || cancelled) return;
                if (Array.isArray(catalog.image_models) && catalog.image_models.length > 0) {
                    setImageModelCatalog(catalog.image_models as CreativeModelProfile[]);
                }
                if (Array.isArray(catalog.video_models) && catalog.video_models.length > 0) {
                    setVideoModelCatalog(catalog.video_models as CreativeModelProfile[]);
                }
                if (typeof catalog.default_image_model_id === 'string' && catalog.default_image_model_id.trim()) {
                    setImageModelId((prev) => {
                        const trimmed = String(prev || "").trim();
                        const known = Array.isArray(catalog.image_models) && catalog.image_models.some((model: any) => String(model?.id || "") === trimmed);
                        return known ? trimmed : catalog.default_image_model_id;
                    });
                }
                if (typeof catalog.default_video_model_id === 'string' && catalog.default_video_model_id.trim()) {
                    setVideoModelId((prev) => {
                        const trimmed = String(prev || "").trim();
                        const known = Array.isArray(catalog.video_models) && catalog.video_models.some((model: any) => String(model?.id || "") === trimmed);
                        return known ? trimmed : catalog.default_video_model_id;
                    });
                }
            } catch {
                // fallback catalogs stay active
            }
        })();
        return () => { cancelled = true; };
    }, []);

    // 1080p is now enabled by default; backend still enforces plan/env caps.
    const canUse1080p = true;
    const animationCreditsAvailable = Number(creditsTotalRemaining || 0);
    const animationCreditExhausted = !isAdmin && (requiresTopup || animationCreditsAvailable <= 0);
    const effectiveAnimationEnabled = !animationCreditExhausted && animateOutputEnabled;
    // Voice controls supported on all kept templates (Casey 2026-04-15: dual-mode requires voice config)
    const templateSupportsVoiceControls = ['skeleton', 'daytrading', 'dilemma', 'business', 'finance', 'tech', 'crypto', 'scary', 'history'].includes(selectedTemplate);
    const cinematicBoostAlwaysOn = true;
    const effectiveCinematicBoostEnabled = cinematicBoostAlwaysOn || cinematicBoostEnabled;
    const defaultSkeletonStyleLockActive = selectedTemplate === 'skeleton' && !creativeReferenceImage && !creativeReferenceAttached;
    const effectiveReferenceAttached = creativeReferenceAttached || defaultSkeletonStyleLockActive;
    const workspaceTabs = [
        { id: 'script', label: 'Script' },
        { id: 'scenes', label: 'Scenes' },
        { id: 'audio', label: 'Audio' },
    ] as const;

    // Final 9-template list per Casey 2026-04-15. Removed: story, motivation, chatstory, argument,
    // reddit, top5, objects, wouldyourather, whatif. Added: dilemma (AI Moral Dilemma Arena, new niche).
    const templates = [
        { id: 'skeleton', title: 'Skeleton AI', desc: '3D skeleton comparisons', icon: '💀' },
        { id: 'daytrading', title: 'Day Trading', desc: 'Trading and investing shorts', icon: '📈' },
        { id: 'dilemma', title: 'Moral Dilemma', desc: 'Cinematic impossible-choice shorts', icon: '⚖️' },
        { id: 'business', title: 'Business', desc: 'Founder and operator stories', icon: '💼' },
        { id: 'finance', title: 'Finance', desc: 'Money and markets explainers', icon: '💸' },
        { id: 'tech', title: 'Tech', desc: 'AI and startup updates', icon: '🧠' },
        { id: 'crypto', title: 'Crypto', desc: 'Crypto trends and narratives', icon: '₿' },
        { id: 'scary', title: 'Scary Stories', desc: 'Horror & true crime', icon: '👻' },
        { id: 'history', title: 'Historical Epic', desc: 'Cinematic history', icon: '⚔️' },
    ];
    const supportsArtStyle = selectedTemplate !== 'skeleton';
    const publicDefaultTemplateId = 'skeleton';
    const templateIds = new Set(templates.map(t => t.id));
    const liveTemplateIds = new Set(['skeleton', 'daytrading', 'dilemma', 'business', 'finance', 'tech', 'crypto', 'scary', 'history']);
    const liveWorkspaceTemplates = templates.filter((template) => liveTemplateIds.has(template.id));
    const currentTemplateMeta = templates.find((template) => template.id === selectedTemplate) || templates[0];
    const supportsTrendHunt = selectedTemplate === 'skeleton';
    const effectiveTrendHuntEnabled = supportsTrendHunt && trendHuntEnabled;
    const selectedYouTubeChannel = useMemo(
        () => youtubeChannels.find((channel) => channel.channel_id === youtubeChannelId) || null,
        [youtubeChannels, youtubeChannelId],
    );
    useEffect(() => {
        if (!templateIds.has(selectedTemplate)) {
            setSelectedTemplate(publicDefaultTemplateId);
        }
    }, [selectedTemplate]);
    // Stale-template guard: if user lands on a removed template (chatstory/story/motivation/etc),
    // bounce them to the public default. Removed templates may still appear in sessionStorage from old sessions.
    useEffect(() => {
        const REMOVED = new Set(['story', 'motivation', 'chatstory', 'argument', 'reddit', 'top5', 'objects', 'wouldyourather', 'whatif']);
        if (REMOVED.has(selectedTemplate)) {
            setSelectedTemplate(publicDefaultTemplateId);
        }
    }, [selectedTemplate]);
    useEffect(() => {
        // Always keep storyAnimationEnabled true (legacy story-template flag, no-op for current templates)
        if (storyAnimationEnabled !== true) {
            setStoryAnimationEnabled(true);
        }
    }, [selectedTemplate, storyAnimationEnabled]);
    useEffect(() => {
        if (animationCreditExhausted && animateOutputEnabled) {
            setAnimateOutputEnabled(false);
            setStoryAnimationEnabled(false);
        }
    }, [animationCreditExhausted, animateOutputEnabled]);
    useEffect(() => {
        if (creativeMode !== 'script_to_short') {
            setSceneBuildLoading(false);
            setSceneBuildError(null);
            setScriptScenesReady(false);
            setBulkImageGenRunning(false);
            setBulkImageGenDone(0);
            setBulkImageGenTotal(0);
        }
    }, [creativeMode]);
    useEffect(() => {
        if (cinematicBoostAlwaysOn && !cinematicBoostEnabled) {
            setCinematicBoostEnabled(true);
        }
    }, [cinematicBoostAlwaysOn, cinematicBoostEnabled]);

    const authHeaders = (): Record<string, string> => {
        const h: Record<string, string> = { "Content-Type": "application/json" };
        if (session) h["Authorization"] = `Bearer ${session.access_token}`;
        return h;
    };
    const loadYouTubeChannels = useCallback(async (silent = false) => {
        if (!session) return;
        if (!silent) setYoutubeLoading(true);
        setYoutubeError('');
        try {
            const res = await fetch(`${API}/api/youtube/channels?sync=true`, {
                headers: { Authorization: `Bearer ${session.access_token}` },
            });
            const payload = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(String((payload as any).detail || `Request failed (${res.status})`));
            const rows = Array.isArray((payload as any).channels) ? (payload as any).channels as ConnectedYouTubeChannel[] : [];
            setYoutubeChannels(rows);
            const defaultId = String((payload as any).default_channel_id || '').trim();
            setYoutubeChannelId((prev) => {
                const trimmedPrev = String(prev || '').trim();
                if (trimmedPrev && rows.some((row) => String(row.channel_id || '').trim() === trimmedPrev)) {
                    return trimmedPrev;
                }
                if (defaultId) {
                    return defaultId;
                }
                return rows.length > 0 ? String(rows[0]?.channel_id || '').trim() : '';
            });
        } catch (e: any) {
            setYoutubeChannels([]);
            setYoutubeError(e?.message || 'Failed to load connected YouTube channels');
        } finally {
            if (!silent) setYoutubeLoading(false);
        }
    }, [session]);
    const startYouTubeConnect = useCallback(async () => {
        if (!session || youtubeConnecting) return;
        setYoutubeConnecting(true);
        setYoutubeError('');
        try {
            startYouTubeBrowserConnect(session.access_token, window.location.href);
        } catch (e: any) {
            setYoutubeError(e?.message || 'Failed to start YouTube connection');
            setYoutubeConnecting(false);
        }
    }, [session, youtubeConnecting]);
    const fileToDataUrl = (file: File): Promise<string> => new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(typeof reader.result === "string" ? reader.result : "");
        reader.onerror = () => reject(new Error("Failed to read reference image"));
        reader.readAsDataURL(file);
    });
    const availableCustomVoices = useMemo(
        () => customVoiceLibrary.filter((voice) => voice.available),
        [],
    );
    const applyCustomVoicePreset = useCallback((voiceId: string, shouldApplyTuning = true) => {
        const preset = customVoicePresetMap.get(voiceId);
        if (!preset || !preset.available) return;
        setCustomVoiceId(voiceId);
        if (preset.backingVoiceId) {
            setStoryVoiceId(preset.backingVoiceId);
        }
        if (shouldApplyTuning) {
            setStoryVoiceSpeed(preset.defaultSpeed);
            setVoicePitch(preset.defaultPitch);
        }
    }, [customVoicePresetMap]);
    useEffect(() => {
        if (selectedTemplate !== 'daytrading') return;
        if (voiceProvider !== 'elevenlabs') {
            setVoiceProvider('elevenlabs');
        }
        if (customVoiceId !== 'studio_voice_moneyline') {
            applyCustomVoicePreset('studio_voice_moneyline');
        }
        if (storyPacingMode !== 'fast') {
            setStoryPacingMode('fast');
        }
        if (soundReferencePreset !== 'social_hook') {
            setSoundReferencePreset('social_hook');
        }
    }, [selectedTemplate, voiceProvider, customVoiceId, storyPacingMode, soundReferencePreset, applyCustomVoicePreset]);

    const renderWorkspaceStageTabs = () => (
        <div className="flex flex-wrap items-center gap-1 rounded-2xl border border-white/[0.06] bg-white/[0.02] p-1.5">
            {workspaceTabs.map((tab, idx) => {
                const active = workspaceStage === tab.id;
                return (
                    <button
                        key={tab.id}
                        type="button"
                        onClick={() => setWorkspaceStage(tab.id)}
                        className={`group flex items-center gap-2.5 rounded-xl px-4 py-2 text-sm font-semibold transition ${
                            active
                                ? 'bg-gradient-to-r from-violet-600 to-cyan-600 text-white shadow-md shadow-violet-900/20'
                                : 'bg-transparent text-gray-400 hover:bg-white/[0.04] hover:text-white'
                        }`}
                    >
                        <span
                            className={`flex h-6 w-6 items-center justify-center rounded-full text-[11px] font-bold ${
                                active ? 'bg-white/20 text-white' : 'bg-white/[0.05] text-gray-400 group-hover:bg-white/[0.1] group-hover:text-white'
                            }`}
                        >
                            {idx + 1}
                        </span>
                        {tab.label}
                    </button>
                );
            })}
        </div>
    );
    const renderCustomVoiceLibraryCard = () => (
        <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4 space-y-4">
            <div className="flex items-start justify-between gap-3">
                <div>
                    <p className="text-sm font-semibold text-white">Audio Engine</p>
                    <p className="mt-1 text-xs text-gray-500">
                        ElevenLabs voice stack with custom presets per template. Background music is disabled — NYPTID testing showed it hurts retention.
                    </p>
                </div>
                <span className="rounded border border-cyan-500/30 bg-cyan-500/10 px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-cyan-200">
                    ElevenLabs First
                </span>
            </div>
            <div className="grid gap-3 md:grid-cols-2">
                <div>
                    <label className="mb-1 block text-xs uppercase tracking-wider text-gray-500">Voice Source</label>
                    <div className="grid grid-cols-3 gap-2">
                        <button
                            type="button"
                            onClick={() => setVoiceProvider('custom')}
                            className={`rounded-lg px-3 py-2 text-xs font-semibold transition ${
                                voiceProvider === 'custom'
                                    ? 'bg-violet-600 text-white'
                                    : 'bg-black/20 text-gray-300 hover:bg-white/[0.06]'
                            }`}
                        >
                            Custom Library
                        </button>
                        <button
                            type="button"
                            onClick={() => setVoiceProvider('elevenlabs')}
                            className={`rounded-lg px-3 py-2 text-xs font-semibold transition ${
                                voiceProvider === 'elevenlabs'
                                    ? 'bg-cyan-600 text-white'
                                    : 'bg-black/20 text-gray-300 hover:bg-white/[0.06]'
                            }`}
                        >
                            ElevenLabs
                        </button>
                        <button
                            type="button"
                            disabled
                            className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs font-semibold uppercase tracking-[0.12em] text-amber-200 opacity-90"
                        >
                            Custom Upload Soon
                        </button>
                    </div>
                </div>
                <div>
                    <label className="mb-1 block text-xs uppercase tracking-wider text-gray-500">Caption Font</label>
                    <select
                        value={captionFont}
                        onChange={(e) => setCaptionFont(e.target.value)}
                        className="w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white focus:outline-none"
                    >
                        {finaleCaptionFonts.map((font) => (
                            <option key={font} value={font}>{font}</option>
                        ))}
                    </select>
                </div>
                <div>
                    <label className="mb-1 block text-xs uppercase tracking-wider text-gray-500">Voice Speed ({storyVoiceSpeed.toFixed(2)}x)</label>
                    <input
                        type="range"
                        min={0.8}
                        max={1.4}
                        step={0.05}
                        value={storyVoiceSpeed}
                        onChange={(e) => setStoryVoiceSpeed(Number(e.target.value))}
                        className="w-full accent-violet-500"
                    />
                </div>
                <div>
                    <label className="mb-1 block text-xs uppercase tracking-wider text-gray-500">Voice Pitch (profile-locked)</label>
                    <input
                        type="range"
                        min={0.8}
                        max={1.2}
                        step={0.05}
                        value={voicePitch}
                        onChange={(e) => setVoicePitch(Number(e.target.value))}
                        disabled
                        className="w-full accent-cyan-500"
                    />
                </div>
                <div>
                    <label className="mb-1 block text-xs uppercase tracking-wider text-gray-500">Voice Language</label>
                    <select
                        value={language}
                        onChange={(e) => setLanguage(e.target.value)}
                        className="w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white focus:outline-none"
                    >
                        {languages.length > 0 ? languages.map((lang) => (
                            <option key={lang.code} value={lang.code}>{lang.name}</option>
                        )) : (
                            <option value="en">English</option>
                        )}
                    </select>
                </div>
                <div>
                    <label className="mb-1 block text-xs uppercase tracking-wider text-gray-500">Background Music</label>
                    <select
                        value={backgroundMusic}
                        onChange={(e) => setBackgroundMusic(e.target.value)}
                        disabled={backgroundMusicComingSoon}
                        className="w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white focus:outline-none"
                    >
                        {finaleMusicOptions.map((track) => (
                            <option key={track} value={track}>{track}</option>
                        ))}
                    </select>
                    <p className="mt-2 text-[11px] text-amber-300">Background music is coming soon. Voice, captions, and slideshow timing are the live finale controls for now.</p>
                </div>
            </div>
            {voiceProvider === 'custom' ? (
                <div className="rounded-xl border border-white/[0.08] bg-black/20 p-4">
                    <div className="flex items-center justify-between gap-3">
                        <div>
                            <p className="text-xs uppercase tracking-[0.18em] text-gray-500">Custom Voice Library</p>
                            <p className="mt-1 text-sm text-white">{availableCustomVoices.length}-profile Catalyst voice rack</p>
                        </div>
                        <p className="text-xs text-gray-500">Each preset resolves to a real render voice and tuned delivery profile.</p>
                    </div>
                    <div className="mt-4 grid max-h-[26rem] gap-2 overflow-y-auto pr-1 md:grid-cols-2 xl:grid-cols-3">
                        {availableCustomVoices.map((voice) => (
                            <button
                                key={voice.id}
                                type="button"
                                onClick={() => applyCustomVoicePreset(voice.id)}
                                className={`rounded-xl border p-3 text-left transition ${
                                    customVoiceId === voice.id
                                        ? 'border-violet-500 bg-violet-500/10'
                                        : 'border-white/[0.08] bg-white/[0.02] hover:border-violet-500/30'
                                }`}
                            >
                                <p className="text-sm font-semibold text-white">{voice.name}</p>
                                <p className="mt-1 text-[11px] text-gray-400">{voice.profile}</p>
                                <p className="mt-2 text-[10px] uppercase tracking-[0.18em] text-cyan-300">{voice.category || 'Catalyst'} · {voice.accent || 'global'}</p>
                                <p className="mt-1 text-[10px] uppercase tracking-[0.14em] text-gray-500">{voice.source}</p>
                            </button>
                        ))}
                    </div>
                    <p className="mt-3 text-[11px] text-gray-500">
                        Custom library mode is still available for the house voice rack, but ElevenLabs is the default live render path right now.
                    </p>
                </div>
            ) : (
                <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 p-4 text-sm text-amber-100">
                    ElevenLabs is the main live voice lane right now. If it fails upstream, Studio falls back to the custom voice stack or the default narrator.
                </div>
            )}
        </div>
    );
    const readJsonResponse = async <T = any>(res: Response): Promise<{ data: T | null; raw: string }> => {
        const raw = await res.text().catch(() => "");
        if (!raw) return { data: null, raw: "" };
        try {
            return { data: JSON.parse(raw) as T, raw };
        } catch {
            return { data: null, raw };
        }
    };
    const extractResponseErrorMessage = (data: any, raw: string, fallback: string) => {
        const detail = typeof data?.detail === 'string' && data.detail.trim()
            ? data.detail.trim()
            : (typeof data?.error === 'string' && data.error.trim() ? data.error.trim() : '');
        if (detail) return detail;
        const text = String(raw || '').trim();
        if (text && !text.startsWith('<!doctype') && !text.startsWith('<html')) {
            return text.length > 220 ? `${text.slice(0, 217)}...` : text;
        }
        return fallback;
    };

    useEffect(() => {
        if (!jobId) return;
        let pollCount = 0;
        const MAX_POLLS = 450; // 15 minutes at 2s intervals
        const interval = setInterval(async () => {
            pollCount++;
            if (pollCount > MAX_POLLS) {
                setJobStatus((prev: any) => ({
                    ...(prev || {}),
                    job_id: jobId,
                    status: 'error',
                    progress: typeof prev?.progress === 'number' ? prev.progress : 0,
                    error: 'Generation is taking longer than expected. Your video may still be rendering — refresh the page to check.',
                }));
                clearInterval(interval);
                setLoading(false);
                return;
            }
            try {
                const res = await fetch(`${GENERATION_API}/api/status/${jobId}`);
                if (!res.ok) {
                    const { data, raw } = await readJsonResponse<any>(res);
                    const detail = String(
                        data?.detail ||
                        raw ||
                        (res.status === 404
                            ? "Render job was not found. It likely expired, failed, or this tab is showing stale state."
                            : `Render status check failed (${res.status}).`)
                    ).trim();
                    setJobStatus((prev: any) => ({
                        ...(prev || {}),
                        job_id: jobId,
                        status: 'error',
                        progress: typeof prev?.progress === 'number' ? prev.progress : 0,
                        error: detail,
                    }));
                    clearInterval(interval);
                    setLoading(false);
                    return;
                }
                const { data } = await readJsonResponse<any>(res);
                if (!data || typeof data !== "object") return;
                setJobStatus(data);
                if (data.status === "complete" || data.status === "error") {
                    clearInterval(interval);
                    setLoading(false);
                }
            } catch { /* retry */ }
        }, 2000);
        return () => clearInterval(interval);
    }, [jobId]);

    useEffect(() => {
        if (!jobStatus || jobStatus.status !== 'complete') return;
        const completedJobId = String(jobStatus.job_id || jobId || '').trim();
        if (!completedJobId) return;
        if (lastTrackedCompletedJobRef.current === completedJobId) return;
        lastTrackedCompletedJobRef.current = completedJobId;
        trackShortRenderCompleted(
            selectedTemplate,
            String(jobStatus.resolution || resolution || '720p'),
            true,
        );
    }, [jobId, jobStatus, resolution, selectedTemplate]);

    useEffect(() => {
        if (!CREATE_WORKFLOW_PERSISTENCE_ENABLED) return;
        if (!session || restoreDoneRef.current) return;
        restoreDoneRef.current = true;
        try {
            const raw = localStorage.getItem(persistKey);
            if (!raw) return;
            const saved = JSON.parse(raw) as Partial<CreatePanelPersistedState>;
            if (saved.selectedTemplate) setSelectedTemplate(saved.selectedTemplate);
            if (saved.resolution === '720p' || saved.resolution === '1080p') setResolution(saved.resolution);
            if (typeof saved.language === 'string' && saved.language) setLanguage(saved.language);
            if (saved.creativeMode === 'auto' || saved.creativeMode === 'creative' || saved.creativeMode === 'script_to_short') setCreativeMode(saved.creativeMode);
            if (saved.creativeStep === 'topic' || saved.creativeStep === 'edit' || saved.creativeStep === 'generating') setCreativeStep(saved.creativeStep);
            if (typeof saved.prompt === 'string') setPrompt(saved.prompt);
            if (typeof saved.sessionId === 'string' || saved.sessionId === null) setSessionId(saved.sessionId ?? null);
            if (Array.isArray(saved.creativeScenes)) {
                setCreativeScenes(
                    saved.creativeScenes.map((s: any) => ({
                        ...s,
                        negative_prompt: typeof s?.negative_prompt === 'string' ? s.negative_prompt : "",
                        // Keep heavy base64 payloads on backend storage; hydrate on demand after restore.
                        imageData: (typeof s?.imageData === 'string' && s.imageData.startsWith('data:')) ? undefined : s?.imageData,
                    }))
                );
            }
            if (typeof saved.creativeTitle === 'string') setCreativeTitle(saved.creativeTitle);
            if (typeof saved.creativeNarration === 'string') setCreativeNarration(saved.creativeNarration);
            if (saved.creativeReferenceLockMode === 'strict' || saved.creativeReferenceLockMode === 'inspired') {
                setCreativeReferenceLockMode(saved.creativeReferenceLockMode);
            }
            if (typeof saved.animateOutputEnabled === 'boolean') setAnimateOutputEnabled(saved.animateOutputEnabled);
            if (typeof saved.storyAnimationEnabled === 'boolean') setStoryAnimationEnabled(saved.storyAnimationEnabled);
            if (typeof saved.storyVoiceId === 'string') setStoryVoiceId(saved.storyVoiceId);
            if (typeof saved.storyVoiceSpeed === 'number' && Number.isFinite(saved.storyVoiceSpeed)) {
                setStoryVoiceSpeed(Math.max(0.8, Math.min(1.35, saved.storyVoiceSpeed)));
            }
            if (saved.storyPacingMode === 'standard' || saved.storyPacingMode === 'fast' || saved.storyPacingMode === 'very_fast') {
                setStoryPacingMode(saved.storyPacingMode);
            }
            if (typeof saved.artStyle === 'string' && saved.artStyle) {
                setArtStyle(saved.artStyle);
            }
            if (typeof saved.imageModelId === 'string' && saved.imageModelId) {
                setImageModelId(saved.imageModelId);
            }
            if (typeof saved.videoModelId === 'string' && saved.videoModelId) {
                setVideoModelId(saved.videoModelId);
            }
            if (typeof saved.cinematicBoostEnabled === 'boolean') {
                setCinematicBoostEnabled(Boolean(saved.cinematicBoostEnabled) || cinematicBoostAlwaysOn);
            }
            if (saved.createSubTab === 'builder' || saved.createSubTab === 'projects') {
                setCreateSubTab(saved.createSubTab);
            }
            if (saved.workspaceStage === 'script' || saved.workspaceStage === 'scenes' || saved.workspaceStage === 'audio') {
                setWorkspaceStage(saved.workspaceStage);
            } else if ((saved.workspaceStage as unknown) === 'finale') {
                // Backward-compat: older saved state used 'finale'; migrate to 'audio'.
                setWorkspaceStage('audio');
            }
            if (typeof saved.subtitlesEnabled === 'boolean') {
                setSubtitlesEnabled(saved.subtitlesEnabled);
            }
            if (saved.voiceProvider === 'custom' || saved.voiceProvider === 'elevenlabs') {
                setVoiceProvider(saved.voiceProvider);
            }
            if (typeof saved.customVoiceId === 'string' && saved.customVoiceId) {
                applyCustomVoicePreset(saved.customVoiceId, false);
            }
            if (typeof saved.voicePitch === 'number' && Number.isFinite(saved.voicePitch)) {
                setVoicePitch(Math.max(0.8, Math.min(1.2, saved.voicePitch)));
            }
            if (typeof saved.captionFont === 'string' && saved.captionFont) {
                setCaptionFont(saved.captionFont);
            }
            if (typeof saved.backgroundMusic === 'string' && saved.backgroundMusic) {
                setBackgroundMusic(saved.backgroundMusic);
            }
            if (typeof saved.soundReferencePreset === 'string' && saved.soundReferencePreset) {
                setSoundReferencePreset(saved.soundReferencePreset);
            }
            if (typeof saved.youtubeChannelId === 'string') {
                setYoutubeChannelId(saved.youtubeChannelId);
            }
            if (typeof saved.trendHuntEnabled === 'boolean') {
                setTrendHuntEnabled(saved.trendHuntEnabled);
            }
            if (typeof saved.jobId === 'string' && saved.jobId) {
                setJobId(saved.jobId);
                setLoading(true);
            }
        } catch {
            // ignore malformed saved state
        }
    }, [session, persistKey]);

    useEffect(() => {
        if (!CREATE_WORKFLOW_PERSISTENCE_ENABLED) return;
        if (!session || !restoreDoneRef.current) return;
        const safeScenes = creativeScenes.map((s) => ({
            index: s.index,
            narration: s.narration,
            visual_description: s.visual_description,
            negative_prompt: s.negative_prompt || "",
            duration_sec: s.duration_sec,
            // Avoid persisting large base64 image payloads to localStorage.
            imageData: (typeof s.imageData === 'string' && s.imageData.startsWith('data:')) ? undefined : s.imageData,
            generation_id: s.generation_id,
            imageError: s.imageError,
            imageLoading: s.imageLoading,
        }));
        const snapshot: CreatePanelPersistedState = {
            selectedTemplate,
            resolution,
            language,
            creativeMode,
            creativeStep,
            prompt,
            sessionId,
            creativeScenes: safeScenes,
            creativeTitle,
            creativeNarration,
            creativeReferenceLockMode,
            animateOutputEnabled,
            storyAnimationEnabled,
            storyVoiceId,
            storyVoiceSpeed,
            storyPacingMode,
            artStyle,
            imageModelId,
            videoModelId,
            cinematicBoostEnabled: effectiveCinematicBoostEnabled,
            createSubTab,
            workspaceStage,
            subtitlesEnabled,
            voiceProvider,
            customVoiceId,
            voicePitch,
            captionFont,
            backgroundMusic,
            soundReferencePreset,
            youtubeChannelId,
            trendHuntEnabled,
            jobId,
            ts: Date.now(),
        };
        try {
            localStorage.setItem(persistKey, JSON.stringify(snapshot));
        } catch {
            // ignore storage quota failures
        }
    }, [
        session,
        persistKey,
        selectedTemplate,
        resolution,
        language,
        creativeMode,
        creativeStep,
        prompt,
        sessionId,
        creativeScenes,
        creativeTitle,
        creativeNarration,
        creativeReferenceLockMode,
        animateOutputEnabled,
        storyAnimationEnabled,
        storyVoiceId,
        storyVoiceSpeed,
        storyPacingMode,
        artStyle,
        imageModelId,
        videoModelId,
        effectiveCinematicBoostEnabled,
        createSubTab,
        workspaceStage,
        subtitlesEnabled,
        voiceProvider,
        customVoiceId,
        voicePitch,
        captionFont,
        backgroundMusic,
        soundReferencePreset,
        youtubeChannelId,
        trendHuntEnabled,
        jobId,
    ]);

    const quickStartSteps = [
        {
            title: "1) Pick Template + Mode",
            text: "Open Create, pick AI Stories, Motivation, Skeleton AI, or Chat Story, then stay in Creative Control or Script to Short.",
        },
        {
            title: "2) Set Output + Prompt",
            text: "Pick resolution/language, write your topic or full script, then attach optional style reference if needed.",
        },
        {
            title: "3) Animation Credit Gate",
            text: "If Catalyst credits cover the project, you can stay animated. If not, Studio will prompt you to buy credits or continue with slideshow.",
        },
        {
            title: "4) Generate Flow",
            text: "Scene-first flow: Script -> Scenes -> Generate Scenes -> Generate Images -> Finale.",
        },
    ];

    useEffect(() => {
        if (!session) return;
        try {
            const seen = localStorage.getItem(quickStartSeenKey) === "1";
            setShowQuickStart(!seen);
            setQuickStartStep(0);
        } catch {
            setShowQuickStart(true);
            setQuickStartStep(0);
        }
    }, [session, quickStartSeenKey]);

    const dismissQuickStart = (dontShowAgain: boolean) => {
        if (dontShowAgain) {
            try { localStorage.setItem(quickStartSeenKey, "1"); } catch { /* ignore */ }
        }
        setShowQuickStart(false);
    };

    const quickStartCard = (showQuickStart && createSubTab === 'builder') ? (
        <div className="fixed bottom-4 right-4 z-40 w-[330px] max-w-[calc(100vw-2rem)] rounded-2xl border border-amber-300/35 bg-[#2a1e09]/95 shadow-2xl shadow-black/50 p-4">
            <div className="flex items-start justify-between gap-2">
                <div>
                    <p className="text-xs uppercase tracking-wider text-amber-300 font-semibold">Studio Quick Start</p>
                    <p className="text-sm font-bold text-amber-100 mt-1">{quickStartSteps[quickStartStep]?.title}</p>
                </div>
                <button
                    onClick={() => dismissQuickStart(false)}
                    className="text-amber-200/80 hover:text-white transition"
                    aria-label="Close quick start"
                >
                    <X className="w-4 h-4" />
                </button>
            </div>
            <p className="text-xs text-amber-100/90 mt-2 leading-relaxed">
                {quickStartSteps[quickStartStep]?.text}
            </p>
            <div className="mt-3 flex items-center gap-1.5">
                {quickStartSteps.map((_, i) => (
                    <span key={`qs-dot-${i}`} className={`h-1.5 rounded-full transition-all ${i === quickStartStep ? 'w-5 bg-amber-300' : 'w-2 bg-amber-200/40'}`} />
                ))}
            </div>
            <div className="mt-4 flex items-center justify-between">
                <button
                    onClick={() => setQuickStartStep((s) => Math.max(0, s - 1))}
                    disabled={quickStartStep === 0}
                    className="px-2.5 py-1.5 text-xs rounded-md border border-amber-200/30 text-amber-100 disabled:opacity-40"
                >
                    Back
                </button>
                <div className="flex items-center gap-2">
                    <button
                        onClick={() => dismissQuickStart(true)}
                        className="px-2.5 py-1.5 text-xs rounded-md border border-amber-200/30 text-amber-100 hover:bg-amber-200/10"
                    >
                        Don't show again
                    </button>
                    {quickStartStep < quickStartSteps.length - 1 ? (
                        <button
                            onClick={() => setQuickStartStep((s) => Math.min(quickStartSteps.length - 1, s + 1))}
                            className="px-3 py-1.5 text-xs font-semibold rounded-md bg-amber-400 text-black hover:bg-amber-300"
                        >
                            Next
                        </button>
                    ) : (
                        <button
                            onClick={() => dismissQuickStart(false)}
                            className="px-3 py-1.5 text-xs font-semibold rounded-md bg-emerald-500 text-white hover:bg-emerald-400"
                        >
                            Got it
                        </button>
                    )}
                </div>
            </div>
        </div>
    ) : null;

    useEffect(() => {
        if (!CREATE_WORKFLOW_PERSISTENCE_ENABLED) return;
        if (!sessionId || (creativeMode !== 'creative' && creativeMode !== 'script_to_short') || !session) return;
        let cancelled = false;
        (async () => {
            try {
                const res = await fetch(`${GENERATION_API}/api/creative/session/${sessionId}/status`, {
                    headers: { Authorization: `Bearer ${session.access_token}` },
                });
                if (!res.ok) return;
                const { data } = await readJsonResponse<any>(res);
                if (!data || typeof data !== "object") return;
                if (cancelled) return;
                setCreativeReferenceAttached(Boolean(data?.has_reference_image));
                if (data?.reference_lock_mode === 'strict' || data?.reference_lock_mode === 'inspired') {
                    setCreativeReferenceLockMode(data.reference_lock_mode);
                }
                if (typeof data?.art_style === 'string' && data.art_style) {
                    setArtStyle(data.art_style);
                }
                if (typeof data?.image_model_id === 'string' && data.image_model_id) {
                    setImageModelId(data.image_model_id);
                }
                if (typeof data?.video_model_id === 'string' && data.video_model_id) {
                    setVideoModelId(data.video_model_id);
                }
                if (data?.has_reference_image && !creativeReferenceImage) {
                    setCreativeReferenceStatus('ready');
                }
            } catch {
                // ignore restore status errors
            }
        })();
        return () => { cancelled = true; };
    }, [sessionId, creativeMode, session, creativeReferenceImage]);

    useEffect(() => {
        if (!sessionId || (creativeMode !== 'creative' && creativeMode !== 'script_to_short') || !session) return;
        if (hydratedSceneImagesSessionRef.current === sessionId) return;
        if (!creativeScenes.some((s) => !s.imageData && !!s.generation_id)) return;
        hydratedSceneImagesSessionRef.current = sessionId;
        let cancelled = false;
        (async () => {
            try {
                const res = await fetch(`${GENERATION_API}/api/creative/session/${sessionId}/scene-images`, {
                    headers: { Authorization: `Bearer ${session.access_token}` },
                });
                if (!res.ok) return;
                const { data } = await readJsonResponse<any>(res);
                if (!data || typeof data !== "object") return;
                if (cancelled) return;
                const byIndex = new Map<number, string>();
                for (const item of (data?.scene_images || [])) {
                    if (typeof item?.scene_index === 'number' && typeof item?.image_data === 'string') {
                        byIndex.set(item.scene_index, item.image_data);
                    }
                }
                if (byIndex.size === 0) return;
                setCreativeScenes((prev) =>
                    prev.map((scene) => {
                        if (scene.imageData) return scene;
                        const hydrated = byIndex.get(scene.index);
                        return hydrated ? { ...scene, imageData: hydrated } : scene;
                    })
                );
            } catch {
                // silent hydrate failure; user can still regenerate per-scene
                hydratedSceneImagesSessionRef.current = null;
            }
        })();
        return () => { cancelled = true; };
    }, [sessionId, creativeMode, session, creativeScenes]);

    const loadProjects = useCallback(async () => {
        if (!session) return;
        setProjectsLoading(true);
        setProjectsError(null);
        try {
            const res = await fetch(`${API}/api/projects`, {
                headers: { Authorization: `Bearer ${session.access_token}` },
            });
            if (!res.ok) throw new Error("Failed to load projects");
            const { data } = await readJsonResponse<any>(res);
            const payload = data || {};
            setProjectDrafts((payload as any).drafts || []);
            setProjectRenders((payload as any).renders || []);
        } catch (e: any) {
            setProjectDrafts([]);
            setProjectRenders([]);
            setProjectsError(e?.message || "Failed to load projects");
        } finally {
            setProjectsLoading(false);
        }
    }, [session]);

    useEffect(() => {
        if (createSubTab !== 'projects') return;
        loadProjects();
    }, [createSubTab, loadProjects]);

    useEffect(() => {
        if (!session) return;
        void loadYouTubeChannels(true);
    }, [session, loadYouTubeChannels]);

    useEffect(() => {
        if (voiceProvider !== 'custom') return;
        const preset = customVoicePresetMap.get(customVoiceId);
        if (preset?.backingVoiceId && storyVoiceId !== preset.backingVoiceId) {
            setStoryVoiceId(preset.backingVoiceId);
        }
    }, [voiceProvider, customVoiceId, storyVoiceId]);

    useEffect(() => {
        if (!templateSupportsVoiceControls) return;
        if (!session) return;
        let cancelled = false;
        const run = async () => {
            setStoryVoicesLoading(true);
            try {
                const res = await fetch(`${API}/api/voices`, {
                    headers: { Authorization: `Bearer ${session.access_token}` },
                });
                const { data } = await readJsonResponse<any>(res);
                if (!cancelled) {
                    const voices = Array.isArray(data?.voices) ? data.voices : [];
                    setStoryVoices(voices);
                    setStoryVoicesSource(data?.source === 'elevenlabs' ? 'elevenlabs' : (data?.source === 'fallback' ? 'fallback' : 'unknown'));
                    setStoryVoicesWarning(String(data?.warning || ""));
                    if (voiceProvider !== 'custom' && !storyVoiceId && voices.length > 0) {
                        setStoryVoiceId(String(voices[0].voice_id || ""));
                    }
                }
            } catch {
                if (!cancelled) {
                    setStoryVoices([]);
                    setStoryVoicesSource('unknown');
                    setStoryVoicesWarning("Voice catalog unavailable right now.");
                }
            } finally {
                if (!cancelled) setStoryVoicesLoading(false);
            }
        };
        void run();
        return () => { cancelled = true; };
    }, [templateSupportsVoiceControls, session, storyVoiceId, voiceProvider]);

    const previewStoryVoice = async () => {
        if (!session || !storyVoiceId || storyPreviewLoading) return;
        setStoryPreviewLoading(true);
        setStoryPreviewError('');
        try {
            const res = await fetch(`${API}/api/voices/preview`, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${session.access_token}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ voice_id: storyVoiceId }),
            });
            if (!res.ok) {
                const { data, raw } = await readJsonResponse<any>(res);
                throw new Error(extractResponseErrorMessage(data, raw, "Voice preview failed"));
            }
            const blob = await res.blob();
            const url = URL.createObjectURL(blob);
            const audio = new Audio(url);
            audio.onended = () => URL.revokeObjectURL(url);
            void audio.play();
        } catch (e: any) {
            setStoryPreviewError(String(e?.message || "Voice preview failed"));
        } finally {
            setStoryPreviewLoading(false);
        }
    };

    const handleRemixIngest = useCallback(async () => {
        const url = remixUrl.trim();
        if (!url) return;
        setRemixError('');
        setRemixWarning('');
        setRemixSourceTitle('');
        setRemixLoading(true);
        try {
            const res = await fetch(`${API}/api/creative/ingest-url`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    ...authHeaders(),
                },
                body: JSON.stringify({ url, language }),
            });
            const { data, raw } = await readJsonResponse<any>(res);
            if (!res.ok) {
                const msg = extractResponseErrorMessage(data, raw, 'Could not read that URL');
                setRemixError(String(msg));
                return;
            }
            const payload = (data || {}) as any;
            const transcript = String(payload.transcript || '').trim();
            if (!transcript) {
                setRemixError('No captions found on that video.');
                return;
            }
            setPrompt(transcript);
            setRemixSourceTitle(String(payload.title || '').trim() || 'source video');
            setRemixWarning(String(payload.warning || '').trim());
        } catch (e: any) {
            setRemixError(String(e?.message || 'Failed to reach Studio backend'));
        } finally {
            setRemixLoading(false);
        }
    }, [remixUrl, language, authHeaders]);

    const handleGenerate = async () => {
        if (!prompt) return;
        setGenerateError(null);
        if (creativeMode === 'creative') {
            await handleCreativeStart();
            return;
        }
        if (creativeMode === 'script_to_short') {
            await handleScriptToShortStart();
            return;
        }
        const mintMode = ['skeleton', 'daytrading', 'dilemma', 'business', 'finance', 'tech', 'crypto', 'scary', 'history'].includes(selectedTemplate);
        const qualityMode = effectiveCinematicBoostEnabled ? 'cinematic' : (selectedTemplate === 'skeleton' ? 'cinematic' : 'standard');
        const transitionStyle = effectiveCinematicBoostEnabled ? 'cinematic' : (selectedTemplate === 'skeleton' ? 'dramatic' : 'smooth');
        const microEscalationMode = effectiveCinematicBoostEnabled ? true : ['skeleton', 'daytrading', 'dilemma', 'business', 'finance', 'tech', 'crypto', 'scary', 'history'].includes(selectedTemplate);
        setLoading(true);
        setGenerateError(null);
        setJobStatus(null);
        setJobId(null);
        try {
            let referenceImageDataUrl = "";
            if (creativeReferenceImage) {
                referenceImageDataUrl = await fileToDataUrl(creativeReferenceImage);
            }
            const res = await fetch(`${GENERATION_API}/api/generate`, {
                method: "POST",
                headers: authHeaders(),
                body: JSON.stringify({
                    template: selectedTemplate,
                    prompt,
                    resolution: canUse1080p ? resolution : '720p',
                    language,
                    mode: 'auto',
                    quality_mode: qualityMode,
                    mint_mode: mintMode,
                    transition_style: transitionStyle,
                    micro_escalation_mode: microEscalationMode,
                    cinematic_boost: effectiveCinematicBoostEnabled,
                    art_style: supportsArtStyle ? artStyle : 'auto',
                    image_model_id: imageModelId,
                    video_model_id: videoModelId,
                    animation_enabled: effectiveAnimationEnabled,
                    voice_id: templateSupportsVoiceControls ? storyVoiceId : "",
                    voice_speed: templateSupportsVoiceControls ? storyVoiceSpeed : 1,
                    pacing_mode: templateSupportsVoiceControls ? storyPacingMode : 'standard',
                    story_animation_enabled: templateSupportsVoiceControls ? effectiveAnimationEnabled : true,
                    youtube_channel_id: youtubeChannelId.trim(),
                    trend_hunt_enabled: effectiveTrendHuntEnabled,
                    reference_image_url: referenceImageDataUrl,
                    reference_lock_mode: creativeReferenceLockMode,
                    background_music: backgroundMusic !== 'No Background Music' ? backgroundMusic : '',
                }),
            });
            const { data, raw } = await readJsonResponse<any>(res);
            if (!res.ok) {
                throw new Error(extractResponseErrorMessage(data, raw, "Failed to start generation"));
            }
            if (data?.job_id) {
                setJobId(data.job_id);
                trackShortProjectStarted(selectedTemplate, 'auto', true);
            }
            else {
                setLoading(false);
                setGenerateError("Generation started without a job id. Retry once the generation lane is healthy.");
            }
        } catch (e: any) {
            setLoading(false);
            setGenerateError(String(e?.message || "Failed to start generation"));
        }
    };

    const handleRegenerateAutoScene = async (sceneIndex: number) => {
        const targetJobId = jobId || jobStatus?.job_id;
        if (!targetJobId) return;
        const mintMode = ['skeleton', 'daytrading', 'dilemma', 'business', 'finance', 'tech', 'crypto', 'scary', 'history'].includes(selectedTemplate);
        setGenerateError(null);
        setRegeneratingAutoScenes(prev => ({ ...prev, [sceneIndex]: true }));
        try {
            const res = await fetch(`${GENERATION_API}/api/auto/regenerate-scene-image`, {
                method: "POST",
                headers: authHeaders(),
                body: JSON.stringify({
                    job_id: targetJobId,
                    scene_index: sceneIndex,
                    mint_mode: mintMode,
                }),
            });
            const { data } = await readJsonResponse<any>(res);
            if (!res.ok) {
                throw new Error(data?.detail || "Failed to regenerate scene image");
            }
            const updatedImage = data?.image;
            if (updatedImage) {
                setJobStatus((prev: any) => {
                    if (!prev) return prev;
                    const nextImages = Array.isArray(prev.scene_images) ? [...prev.scene_images] : [];
                    while (nextImages.length <= sceneIndex) nextImages.push(null);
                    nextImages[sceneIndex] = updatedImage;
                    return { ...prev, scene_images: nextImages };
                });
            }
        } catch (e: any) {
            setGenerateError(e?.message || "Failed to regenerate scene image");
        } finally {
            setRegeneratingAutoScenes(prev => ({ ...prev, [sceneIndex]: false }));
        }
    };

    const handleScriptToShortStart = async () => {
        const scriptText = prompt.trim();
        if (!scriptText) return;
        setSessionId(null);
        setCreativeScenes([]);
        setCreativeTitle("Script to Short");
        setCreativeNarration(scriptText);
        setSceneBuildLoading(false);
        setSceneBuildError(null);
        setScriptScenesReady(false);
        setBulkImageGenRunning(false);
        setBulkImageGenDone(0);
        setBulkImageGenTotal(0);
        setCreativeStep('edit');
    };

    const handleGenerateScriptToShortScenes = async () => {
        const scriptText = (creativeMode === 'script_to_short'
            ? creativeNarration.trim()
            : (prompt.trim() || creativeNarration.trim())
        );
        if (!scriptText) return;
        const mintMode = ['skeleton', 'daytrading', 'dilemma', 'business', 'finance', 'tech', 'crypto', 'scary', 'history'].includes(selectedTemplate);
        const qualityMode = effectiveCinematicBoostEnabled ? 'cinematic' : (selectedTemplate === 'skeleton' ? 'cinematic' : 'standard');
        const transitionStyle = effectiveCinematicBoostEnabled ? 'cinematic' : (selectedTemplate === 'skeleton' ? 'dramatic' : 'smooth');
        const microEscalationMode = effectiveCinematicBoostEnabled ? true : ['skeleton', 'daytrading', 'dilemma', 'business', 'finance', 'tech', 'crypto', 'scary', 'history'].includes(selectedTemplate);
        const generationMode = creativeMode === 'script_to_short' ? 'script_to_short' : 'creative';
        setSceneBuildLoading(true);
        setSceneBuildError(null);
        setScriptScenesReady(false);
        setBulkImageGenDone(0);
        setBulkImageGenTotal(0);
        try {
            const res = await fetch(`${GENERATION_API}/api/creative/script`, {
                method: "POST",
                headers: authHeaders(),
                body: JSON.stringify({
                    template: selectedTemplate,
                    prompt: scriptText,
                    resolution: canUse1080p ? resolution : '720p',
                    language,
                    mode: generationMode,
                    quality_mode: qualityMode,
                    mint_mode: mintMode,
                    art_style: supportsArtStyle ? artStyle : 'auto',
                    image_model_id: imageModelId,
                    video_model_id: videoModelId,
                    transition_style: transitionStyle,
                    micro_escalation_mode: microEscalationMode,
                    cinematic_boost: effectiveCinematicBoostEnabled,
                    animation_enabled: effectiveAnimationEnabled,
                    voice_id: templateSupportsVoiceControls ? storyVoiceId : "",
                    voice_speed: templateSupportsVoiceControls ? storyVoiceSpeed : 1,
                    pacing_mode: templateSupportsVoiceControls ? storyPacingMode : 'standard',
                    story_animation_enabled: templateSupportsVoiceControls ? effectiveAnimationEnabled : true,
                    youtube_channel_id: youtubeChannelId.trim(),
                    trend_hunt_enabled: effectiveTrendHuntEnabled,
                }),
            });
            if (!res.ok) {
                const errText = await res.text().catch(() => "");
                throw new Error(normalizeUpstreamErrorMessage(errText, res.status, "Failed to start Script to Short"));
            }
            const { data } = await readJsonResponse<any>(res);
            if (!data || typeof data !== "object") {
                throw new Error("Invalid Script to Short response");
            }
            setSessionId(data.session_id);
            trackShortProjectStarted(selectedTemplate, generationMode, true);
            if (typeof data.image_model_id === 'string' && data.image_model_id) {
                setImageModelId(data.image_model_id);
            }
            if (typeof data.video_model_id === 'string' && data.video_model_id) {
                setVideoModelId(data.video_model_id);
            }
            if (typeof data.youtube_channel_id === 'string') {
                setYoutubeChannelId(data.youtube_channel_id);
            }
            if (typeof data.trend_hunt_enabled === 'boolean') {
                setTrendHuntEnabled(data.trend_hunt_enabled);
            }
            if (creativeReferenceImage) {
                const uploadForm = new FormData();
                uploadForm.append("session_id", data.session_id);
                uploadForm.append("reference_image", creativeReferenceImage);
                uploadForm.append("reference_lock_mode", creativeReferenceLockMode);
                const refRes = await fetch(`${GENERATION_API}/api/creative/reference-image`, {
                    method: "POST",
                    headers: session ? { Authorization: `Bearer ${session.access_token}` } : {},
                    body: uploadForm,
                });
                if (!refRes.ok) {
                    const errText = await refRes.text().catch(() => "");
                    throw new Error(normalizeUpstreamErrorMessage(errText, refRes.status, "Failed to upload reference style image"));
                }
                setCreativeReferenceStatus('ready');
                setCreativeReferenceAttached(true);
            }
            const generatedScenes: CreativeScene[] = (data.scenes || []).map((s: any, i: number) => ({
                index: i,
                narration: String(s?.narration || ""),
                visual_description: String(s?.visual_description || ""),
                negative_prompt: String(s?.negative_prompt || ""),
                duration_sec: Number(s?.duration_sec || 5),
            }));
            setCreativeScenes(generatedScenes.length > 0 ? generatedScenes : [{
                index: 0,
                narration: "",
                visual_description: "",
                negative_prompt: "",
                duration_sec: 5,
            }]);
            setCreativeTitle(data.title || (creativeMode === 'script_to_short' ? "Script to Short" : (prompt || "Creative Draft")));
            if (!creativeNarration.trim()) {
                setCreativeNarration(scriptText);
            }
            setScriptScenesReady(generatedScenes.length > 0);
        } catch (e: any) {
            setSceneBuildError(e?.message || "Failed to generate scenes");
        } finally {
            setSceneBuildLoading(false);
        }
    };

    const handleCreativeStart = async () => {
        const mintMode = ['skeleton', 'daytrading', 'dilemma', 'business', 'finance', 'tech', 'crypto', 'scary', 'history'].includes(selectedTemplate);
        const qualityMode = effectiveCinematicBoostEnabled ? 'cinematic' : (selectedTemplate === 'skeleton' ? 'cinematic' : 'standard');
        const transitionStyle = effectiveCinematicBoostEnabled ? 'cinematic' : (selectedTemplate === 'skeleton' ? 'dramatic' : 'smooth');
        const microEscalationMode = effectiveCinematicBoostEnabled ? true : ['skeleton', 'daytrading', 'dilemma', 'business', 'finance', 'tech', 'crypto', 'scary', 'history'].includes(selectedTemplate);
        setScriptLoading(true);
        setGenerateError(null);
        setCreativeReferenceStatus(creativeReferenceImage ? 'uploading' : 'idle');
        try {
            const res = await fetch(`${GENERATION_API}/api/creative/session`, {
                method: "POST",
                headers: authHeaders(),
                body: JSON.stringify({
                    template: selectedTemplate,
                    topic: prompt || "Untitled",
                    resolution: canUse1080p ? resolution : '720p',
                    language,
                    animation_enabled: effectiveAnimationEnabled,
                    story_animation_enabled: templateSupportsVoiceControls ? effectiveAnimationEnabled : true,
                    quality_mode: qualityMode,
                    mint_mode: mintMode,
                    art_style: supportsArtStyle ? artStyle : 'auto',
                    image_model_id: imageModelId,
                    video_model_id: videoModelId,
                    transition_style: transitionStyle,
                    micro_escalation_mode: microEscalationMode,
                    cinematic_boost: effectiveCinematicBoostEnabled,
                    voice_id: templateSupportsVoiceControls ? storyVoiceId : "",
                    voice_speed: templateSupportsVoiceControls ? storyVoiceSpeed : 1,
                    pacing_mode: templateSupportsVoiceControls ? storyPacingMode : 'standard',
                    youtube_channel_id: youtubeChannelId.trim(),
                    trend_hunt_enabled: effectiveTrendHuntEnabled,
                    reference_lock_mode: creativeReferenceLockMode,
                }),
            });
            if (!res.ok) {
                const errText = await res.text().catch(() => "");
                throw new Error(normalizeUpstreamErrorMessage(errText, res.status, "Failed to create session"));
            }
            const { data } = await readJsonResponse<any>(res);
            if (!data || typeof data !== "object") throw new Error("Invalid creative session response");
            setSessionId(data.session_id);
            trackShortProjectStarted(selectedTemplate, 'creative', true);
            if (typeof data.image_model_id === 'string' && data.image_model_id) {
                setImageModelId(data.image_model_id);
            }
            if (typeof data.video_model_id === 'string' && data.video_model_id) {
                setVideoModelId(data.video_model_id);
            }
            if (typeof data.youtube_channel_id === 'string') {
                setYoutubeChannelId(data.youtube_channel_id);
            }
            if (typeof data.trend_hunt_enabled === 'boolean') {
                setTrendHuntEnabled(data.trend_hunt_enabled);
            }

            if (creativeReferenceImage) {
                const uploadForm = new FormData();
                uploadForm.append("session_id", data.session_id);
                uploadForm.append("reference_image", creativeReferenceImage);
                uploadForm.append("reference_lock_mode", creativeReferenceLockMode);
                const refRes = await fetch(`${GENERATION_API}/api/creative/reference-image`, {
                    method: "POST",
                    headers: session ? { Authorization: `Bearer ${session.access_token}` } : {},
                    body: uploadForm,
                });
                if (!refRes.ok) {
                    const errText = await refRes.text().catch(() => "");
                    throw new Error(normalizeUpstreamErrorMessage(errText, refRes.status, "Failed to upload reference style image"));
                }
                setCreativeReferenceStatus('ready');
                setCreativeReferenceAttached(true);
            }

            setCreativeTitle(prompt || "Untitled Short");
            if (!creativeNarration.trim()) {
                setCreativeNarration(prompt || "");
            }
            setCreativeScenes([{
                index: 0,
                narration: "",
                visual_description: "",
                negative_prompt: "",
                duration_sec: 5,
            }]);
            setScriptScenesReady(false);
            setCreativeStep('edit');
        } catch (e: any) {
            setCreativeReferenceStatus(creativeReferenceImage ? 'error' : 'idle');
            setGenerateError(e?.message || "Failed to start creative session");
        } finally {
            setScriptLoading(false);
        }
    };

    const handleAddScene = () => {
        setCreativeScenes(prev => [...prev, {
            index: prev.length,
            narration: "",
            visual_description: "",
            negative_prompt: "",
            duration_sec: 5,
        }]);
    };

    const handleRemoveScene = (index: number) => {
        if (creativeScenes.length <= 1) return;
        setCreativeScenes(prev => prev.filter((_, i) => i !== index).map((s, i) => ({ ...s, index: i })));
    };

    const handleGenerateSceneImage = async (sceneIndex: number) => {
        if (!sessionId) return;
        const currentScenes = creativeScenesRef.current;
        if (sceneIndex >= currentScenes.length || !currentScenes[sceneIndex]) return;
        const scene = currentScenes[sceneIndex];
        if (!scene.visual_description.trim()) return;
        const imageCreditCost = Math.max(0, Number(selectedImageModel.credit_cost_per_image || 0));
        if (!isAdmin && imageCreditCost > 0 && imageCreditCost > animationCreditsAvailable) {
            openAnimationCreditPrompt(imageCreditCost, 'image');
            return;
        }
        const mintMode = ['skeleton', 'daytrading', 'dilemma', 'business', 'finance', 'tech', 'crypto', 'scary', 'history'].includes(selectedTemplate);
        const qualityMode = effectiveCinematicBoostEnabled ? 'cinematic' : (selectedTemplate === 'skeleton' ? 'cinematic' : 'standard');
        const transitionStyle = effectiveCinematicBoostEnabled ? 'cinematic' : (selectedTemplate === 'skeleton' ? 'dramatic' : 'smooth');
        const microEscalationMode = effectiveCinematicBoostEnabled ? true : ['skeleton', 'daytrading', 'dilemma', 'business', 'finance', 'tech', 'crypto', 'scary', 'history'].includes(selectedTemplate);
        setCreativeScenes(prev => prev.map((s, i) => i === sceneIndex ? { ...s, imageLoading: true, imageError: undefined } : s));
        try {
            const res = await fetch(`${GENERATION_API}/api/creative/scene-image`, {
                method: "POST",
                headers: authHeaders(),
                body: JSON.stringify({
                    prompt: scene.visual_description,
                    negative_prompt: String(scene.negative_prompt || "").trim(),
                    scene_index: sceneIndex,
                    session_id: sessionId,
                    template: selectedTemplate,
                    resolution: canUse1080p ? resolution : '720p',
                    quality_mode: qualityMode,
                    mint_mode: mintMode,
                    art_style: supportsArtStyle ? artStyle : 'auto',
                    image_model_id: imageModelId,
                    transition_style: transitionStyle,
                    micro_escalation_mode: microEscalationMode,
                    cinematic_boost: effectiveCinematicBoostEnabled,
                    youtube_channel_id: youtubeChannelId.trim(),
                    trend_hunt_enabled: effectiveTrendHuntEnabled,
                    reference_lock_mode: creativeReferenceLockMode,
                }),
            });
            const rawBody = await res.text().catch(() => "");
            if (!res.ok) {
                const errText = normalizeSceneErrorMessage(rawBody || "Unknown error", res.status);
                console.error(`Scene ${sceneIndex} image gen failed:`, res.status, errText);
                throw new Error(errText);
            }
            let data: any = {};
            try {
                data = rawBody ? JSON.parse(rawBody) : {};
            } catch {
                throw new Error(normalizeSceneErrorMessage(rawBody, res.status));
            }
            if (typeof data?.image_data !== "string" || !data.image_data.trim()) {
                throw new Error(normalizeSceneErrorMessage(rawBody, res.status) || "Scene image response did not contain image_data");
            }
            setCreativeScenes(prev => prev.map((s, i) =>
                i === sceneIndex ? {
                    ...s,
                    imageData: data.image_data,
                    imageLoading: false,
                    generation_id: data.generation_id,
                    qa_ok: typeof data.qa_ok === "boolean" ? data.qa_ok : true,
                    qa_score: typeof data.qa_score === "number" ? data.qa_score : undefined,
                    qa_notes: Array.isArray(data.qa_notes) ? data.qa_notes : [],
                } : s
            ));
        } catch (err: any) {
            const msg = err?.message || "Image generation failed";
            console.error(`Scene ${sceneIndex} image gen error:`, msg);
            setCreativeScenes(prev => prev.map((s, i) =>
                i === sceneIndex ? { ...s, imageLoading: false, imageError: msg } : s
            ));
        }
    };

    const handleGenerateSceneImageBatch = async () => {
        if (!sessionId) return;
        const scenes = creativeScenesRef.current;
        const allTargets = scenes
            .map((scene, idx) => ({ scene, idx }))
            .filter(({ scene }) => !!scene.visual_description.trim());
        if (allTargets.length === 0) return;
        const pendingTargets = allTargets.filter(({ scene }) => !scene.imageData);
        const targets = pendingTargets.length > 0 ? pendingTargets : allTargets;
        if (targets.length === 0) return;
        const imageCreditCost = Math.max(0, Number(selectedImageModel.credit_cost_per_image || 0));
        if (!isAdmin && imageCreditCost > 0) {
            const requiredCredits = Math.max(1, targets.length * imageCreditCost);
            if (requiredCredits > animationCreditsAvailable) {
                openAnimationCreditPrompt(requiredCredits, 'image');
                return;
            }
        }
        setBulkImageGenRunning(true);
        setBulkImageGenTotal(targets.length);
        setBulkImageGenDone(0);
        try {
            for (const { idx } of targets) {
                await handleGenerateSceneImage(idx);
                setBulkImageGenDone((prev) => prev + 1);
            }
        } finally {
            setBulkImageGenRunning(false);
        }
    };

    const normalizeUpstreamErrorMessage = (message?: string, statusCode?: number, fallback = "Request failed") => {
        const raw = String(message || "").trim();
        if (!raw) return statusCode ? `${fallback} (${statusCode})` : fallback;
        try {
            const parsed = JSON.parse(raw);
            const detail = parsed?.detail;
            if (typeof detail === "string" && detail.trim()) {
                return detail.trim();
            }
        } catch {
            // plain-text or HTML response
        }
        const lower = raw.toLowerCase();
        if (
            lower.includes("cloudflare") ||
            lower.includes("router_external_target_error") ||
            lower.includes("application error") ||
            lower.startsWith("<!doctype") ||
            lower.startsWith("<html") ||
            lower.includes("<html")
        ) {
            return "Studio backend is temporarily unavailable. Please retry in a few seconds.";
        }
        return raw;
    };

    const handleUpdateScene = (index: number, field: keyof CreativeScene, value: string | number) => {
        setCreativeScenes(prev => prev.map((s, i) =>
            i === index ? { ...s, [field]: value } : s
        ));
    };

    const normalizeSceneErrorMessage = (message?: string, statusCode?: number) => {
        const raw = String(message || "").trim();
        if (!raw) return statusCode ? `Request failed (${statusCode})` : "Image generation failed";
        try {
            const parsed = JSON.parse(raw);
            const detail = parsed?.detail;
            if (typeof detail === "string" && detail.trim()) {
                return detail.trim();
            }
        } catch {
            // plain-text or HTML response
        }
        const lower = raw.toLowerCase();
        if (lower.includes("router_external_target_error") || lower.includes("application error")) {
            return "The public tunnel lost the image request before the generator finished. Retry once the image lane is stable.";
        }
        if (lower.startsWith("<!doctype") || lower.startsWith("<html") || lower.includes("<html")) {
            if (statusCode === 524 || lower.includes("524")) {
                return "Gateway timeout while generating image. Click Generate Image again.";
            }
            if (statusCode === 502 || statusCode === 503 || statusCode === 504 || lower.includes("gateway")) {
                return "Upstream image engine is temporarily unavailable. Please retry in a few seconds.";
            }
            return statusCode
                ? `Unexpected upstream HTML error (${statusCode}). Please retry.`
                : "Unexpected upstream HTML error. Please retry.";
        }
        return raw;
    };

    const summarizeSceneError = (message?: string) => {
        const text = normalizeSceneErrorMessage(message);
        if (!text) return "Image generation failed";
        return text.length > 260 ? `${text.slice(0, 260)}...` : text;
    };

    const summarizeSceneQaWarning = (scene: CreativeScene) => {
        const notes = Array.isArray(scene.qa_notes) ? scene.qa_notes : [];
        if (notes.includes('brain_prop_missing_or_wrong') && notes.includes('money_prop_missing_or_wrong')) {
            return "Prompt mismatch: brain and money props are not clearly visible.";
        }
        if (notes.includes('brain_prop_missing_or_wrong')) {
            return "Prompt mismatch: brain prop is not clearly visible.";
        }
        if (notes.includes('money_prop_missing_or_wrong')) {
            return "Prompt mismatch: money prop is not clearly visible.";
        }
        if (notes.includes('interactive_soft_accept')) {
            return "Prompt match is weak. Regenerate for a tighter match.";
        }
        return "";
    };

    const formatModelSpendLabel = (model: CreativeModelProfile, mode: 'image' | 'video') => {
        if (mode === 'image') {
            const cost = Number(model.credit_cost_per_image || 0);
            if (cost <= 0) return 'Basic lane · no Catalyst credit burn';
            return `${cost} Catalyst credits per image`;
        }
        const multiplier = Math.max(1, Number(model.credit_multiplier || 1));
        if (multiplier <= 1) return 'Basic lane · 1 Catalyst credit per animated scene';
        return `${multiplier}x lane · ${multiplier} Catalyst credits per animated scene`;
    };

    const formatModelTierLabel = (model: CreativeModelProfile) => {
        if (model.tier === 'elite') return 'Elite Lane';
        if (model.tier === 'premium') return 'Premium Lane';
        return 'Basic Lane';
    };

    const handleFinalize = async () => {
        setFinalizeError(null);
        if (!sessionId) {
            setFinalizeError("No active session. Please go back and start a new project.");
            return;
        }
        if (!creativeNarration.trim()) {
            setFinalizeError("Please write a script / narration before rendering.");
            return;
        }
        setLoading(true);
        setJobStatus(null);
        setJobId(null);
        setCreativeStep('generating');
        const mintMode = ['skeleton', 'daytrading', 'dilemma', 'business', 'finance', 'tech', 'crypto', 'scary', 'history'].includes(selectedTemplate);
        const qualityMode = effectiveCinematicBoostEnabled ? 'cinematic' : (selectedTemplate === 'skeleton' ? 'cinematic' : 'standard');
        const transitionStyle = effectiveCinematicBoostEnabled ? 'cinematic' : (selectedTemplate === 'skeleton' ? 'dramatic' : 'smooth');
        const microEscalationMode = effectiveCinematicBoostEnabled ? true : ['skeleton', 'daytrading', 'dilemma', 'business', 'finance', 'tech', 'crypto', 'scary', 'history'].includes(selectedTemplate);
        try {
            const res = await fetch(`${GENERATION_API}/api/creative/finalize`, {
                method: "POST",
                headers: authHeaders(),
                body: JSON.stringify({
                    session_id: sessionId,
                    template: selectedTemplate,
                    resolution: canUse1080p ? resolution : '720p',
                    language,
                    animation_enabled: effectiveAnimationEnabled,
                    story_animation_enabled: templateSupportsVoiceControls ? effectiveAnimationEnabled : true,
                    quality_mode: qualityMode,
                    mint_mode: mintMode,
                    art_style: supportsArtStyle ? artStyle : 'auto',
                    image_model_id: imageModelId,
                    video_model_id: videoModelId,
                    transition_style: transitionStyle,
                    micro_escalation_mode: microEscalationMode,
                    cinematic_boost: effectiveCinematicBoostEnabled,
                    voice_id: templateSupportsVoiceControls ? storyVoiceId : "",
                    voice_speed: templateSupportsVoiceControls ? storyVoiceSpeed : 1,
                    pacing_mode: templateSupportsVoiceControls ? storyPacingMode : 'standard',
                    subtitles_enabled: templateSupportsVoiceControls ? subtitlesEnabled : true,
                    youtube_channel_id: youtubeChannelId.trim(),
                    trend_hunt_enabled: effectiveTrendHuntEnabled,
                    reference_lock_mode: creativeReferenceLockMode,
                    background_music: backgroundMusic !== 'No Background Music' ? backgroundMusic : '',
                    narration: creativeNarration,
                    scenes: creativeScenes.map(s => ({
                        narration: "",
                        visual_description: s.visual_description,
                        negative_prompt: String(s.negative_prompt || "").trim(),
                        duration_sec: s.duration_sec,
                    })),
                }),
            });
            if (!res.ok) {
                const { data: errData, raw: errRaw } = await readJsonResponse<any>(res);
                const msg = errData?.detail || errRaw || `Server error (${res.status}). The backend may be overloaded - try again in a moment.`;
                setFinalizeError(msg);
                setLoading(false);
                setCreativeStep('edit');
                return;
            }
            const { data } = await readJsonResponse<any>(res);
            if (!data || typeof data !== "object") {
                setFinalizeError("Server returned an invalid response payload. Please try again.");
                setLoading(false);
                setCreativeStep('edit');
                return;
            }
            if (data.job_id) {
                setJobId(data.job_id);
            } else {
                setFinalizeError("Server returned no job ID. Please try again.");
                setLoading(false);
                setCreativeStep('edit');
            }
        } catch (err: any) {
            setFinalizeError(err?.message || "Network error — the server may be down or overloaded. Please try again.");
            setLoading(false);
            setCreativeStep('edit');
        }
    };

    const handleResetCreative = () => {
        setCreativeStep('topic');
        setSessionId(null);
        setCreativeScenes([]);
        setCreativeTitle("");
        setCreativeNarration("");
        setSceneBuildLoading(false);
        setSceneBuildError(null);
        setScriptScenesReady(false);
        setCreativeReferenceStatus(creativeReferenceImage ? 'ready' : 'idle');
        setCreativeReferenceAttached(false);
        setBulkImageGenRunning(false);
        setBulkImageGenDone(0);
        setBulkImageGenTotal(0);
        setJobId(null);
        setJobStatus(null);
        setLoading(false);
        setFinalizeError(null);
        try { localStorage.removeItem(persistKey); } catch { /* ignore */ }
    };

    const openTemplateChooser = () => {
        setCreateSubTab('builder');
        setTemplateChooserOpen(true);
        setSubscriptionPromptOpen(false);
        setSubscriptionPromptError(null);
    };

    const handleWorkspaceCreateClick = () => {
        if (loading || scriptLoading) return;
        openTemplateChooser();
    };

    const handleTemplateSelection = (templateId: string) => {
        if (templateId !== selectedTemplate) {
            handleResetCreative();
        }
        try {
            sessionStorage.removeItem(pendingChatStoryTemplateStorageKey);
        } catch {
            // ignore storage failures
        }
        setSelectedTemplate(templateId);
        setCreateSubTab('builder');
        setWorkspaceStage('script');
        if (templateId !== 'chatstory') {
            setCreativeMode('creative');
        }
        setTemplateChooserOpen(false);
        setSubscriptionPromptOpen(false);
        setSubscriptionPromptError(null);
    };

    const handleMonthlyAccessCheckout = async (planName: 'starter' | 'creator' | 'pro') => {
        setSubscriptionCheckoutPlan(planName);
        setSubscriptionPromptError(null);
        try {
            try {
                sessionStorage.setItem(pendingChatStoryTemplateStorageKey, 'chatstory');
            } catch {
                // ignore storage failures
            }
            const error = await checkout(planName);
            if (error) {
                setSubscriptionPromptError(error);
                try {
                    sessionStorage.removeItem(pendingChatStoryTemplateStorageKey);
                } catch {
                    // ignore storage failures
                }
            }
        } finally {
            setSubscriptionCheckoutPlan(null);
        }
    };

    const openAnimationCreditPrompt = (requiredCredits: number, mode: 'video' | 'image' = 'video') => {
        setAnimationCreditPromptRequired(Math.max(1, requiredCredits));
        setAnimationCreditPromptMode(mode);
        setAnimationCreditPromptError(null);
    };

    const handleAnimationTopupCheckout = async (requiredCredits: number) => {
        const recommendedPack = topupPacks.find((pack) => pack.credits >= requiredCredits) || topupPacks[topupPacks.length - 1];
        if (!recommendedPack) {
            setAnimationCreditPromptError('No Catalyst credit packs are configured yet.');
            return;
        }
        setAnimationCreditPromptError(null);
        const error = await checkoutTopup(recommendedPack.price_id, 'paypal');
        if (error) {
            setAnimationCreditPromptError(error);
        }
    };

    const renderWorkspaceChrome = ({ subtitle, showBack }: { subtitle: string; showBack?: boolean }) => (
        <div className="rounded-[28px] border border-white/[0.06] bg-white/[0.02] p-4 sm:p-5">
            <div className="flex flex-wrap items-center justify-between gap-4">
                <div className="flex flex-wrap items-center gap-3">
                    <div className="grid grid-cols-2 gap-2 rounded-2xl border border-white/[0.06] bg-black/20 p-1">
                        <button
                            type="button"
                            onClick={handleWorkspaceCreateClick}
                            className={`rounded-xl px-4 py-2.5 text-sm font-semibold transition ${
                                createSubTab === 'builder'
                                    ? 'bg-violet-600 text-white'
                                    : 'text-gray-300 hover:bg-white/[0.04] hover:text-white'
                            }`}
                        >
                            Create
                        </button>
                        <button
                            type="button"
                            onClick={() => setCreateSubTab('projects')}
                            className={`rounded-xl px-4 py-2.5 text-sm font-semibold transition ${
                                createSubTab === 'projects'
                                    ? 'bg-violet-600 text-white'
                                    : 'text-gray-300 hover:bg-white/[0.04] hover:text-white'
                            }`}
                        >
                            Projects
                        </button>
                    </div>
                    <button
                        type="button"
                        onClick={openTemplateChooser}
                        className="inline-flex items-center gap-2 rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-2.5 text-left text-sm font-semibold text-white transition hover:border-violet-500/40 hover:bg-violet-500/10"
                    >
                        <Sparkles className="h-4 w-4 text-violet-300" />
                        {currentTemplateMeta.title}
                    </button>
                    {selectedTemplate === 'chatstory' ? (
                        <span className="rounded-full border border-violet-500/30 bg-violet-500/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-violet-200">
                            Catalyst Lane
                        </span>
                    ) : (
                        <span className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-200">
                            Live Now
                        </span>
                    )}
                </div>
                <div className="flex items-center gap-2">
                    {!showQuickStart && createSubTab === 'builder' && (
                        <button
                            onClick={() => { setQuickStartStep(0); setShowQuickStart(true); }}
                            className="rounded-lg border border-white/10 px-3 py-2 text-xs text-gray-300 transition hover:bg-white/5 hover:text-white"
                        >
                            Show Quick Start
                        </button>
                    )}
                    {showBack && (
                        <button
                            type="button"
                            onClick={handleResetCreative}
                            className="inline-flex items-center gap-2 rounded-lg border border-white/[0.08] bg-white/[0.03] px-4 py-2 text-sm text-gray-300 transition hover:bg-white/[0.06]"
                        >
                            <ArrowRight className="h-4 w-4 rotate-180" />
                            Back
                        </button>
                    )}
                </div>
            </div>
            <p className="mt-4 text-sm text-gray-400">{subtitle}</p>
        </div>
    );

    const templateChooserModal = templateChooserOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/72 px-4 py-8">
            <div className="w-full max-w-5xl rounded-[32px] border border-white/[0.08] bg-[#0d0d11] p-6 shadow-2xl shadow-black/50">
                <div className="flex items-start justify-between gap-4">
                    <div>
                        <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-violet-300">Create Template</p>
                        <h3 className="mt-2 text-2xl font-bold text-white">Pick the live workflow you want to open</h3>
                        <p className="mt-2 text-sm text-gray-400">Only the sellable launch templates are selectable. Everything else stays marked coming soon in the left rail.</p>
                    </div>
                    <button
                        type="button"
                        onClick={() => setTemplateChooserOpen(false)}
                        className="rounded-lg p-2 text-gray-400 transition hover:bg-white/[0.05] hover:text-white"
                        title="Close template picker"
                    >
                        <X className="h-4 w-4" />
                    </button>
                </div>
                <div className="mt-6 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                    {liveWorkspaceTemplates.map((template) => {
                        const active = selectedTemplate === template.id;
                        return (
                            <button
                                key={template.id}
                                type="button"
                                onClick={() => handleTemplateSelection(template.id)}
                                className={`rounded-[24px] border p-5 text-left transition ${
                                    active
                                        ? 'border-violet-500 bg-violet-500/10'
                                        : 'border-white/[0.08] bg-white/[0.03] hover:border-violet-500/30 hover:bg-violet-500/[0.03]'
                                }`}
                            >
                                <div className="flex items-start justify-between gap-3">
                                    <span className="text-2xl">{template.icon}</span>
                                    {active ? (
                                        <span className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-emerald-200">
                                            Active
                                        </span>
                                    ) : null}
                                </div>
                                <h4 className="mt-6 text-lg font-semibold text-white">{template.title}</h4>
                                <p className="mt-2 text-sm leading-relaxed text-gray-400">{template.desc}</p>
                            </button>
                        );
                    })}
                </div>
            </div>
        </div>
    ) : null;

    const subscriptionPromptModal = subscriptionPromptOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/72 px-4 py-8">
            <div className="w-full max-w-3xl rounded-[32px] border border-violet-500/20 bg-[#0d0d11] p-6 shadow-2xl shadow-black/50">
                <div className="flex items-start justify-between gap-4">
                    <div>
                        <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-violet-300">Chat Story Locked</p>
                        <h3 className="mt-2 text-2xl font-bold text-white">You do not have a monthly subscription yet</h3>
                        <p className="mt-2 text-sm text-gray-400">Choose Catalyst Membership to unlock Chat Story. After PayPal confirms the month, Studio will open the template automatically.</p>
                    </div>
                    <button
                        type="button"
                        onClick={() => setSubscriptionPromptOpen(false)}
                        className="rounded-lg p-2 text-gray-400 transition hover:bg-white/[0.05] hover:text-white"
                        title="Close membership prompt"
                    >
                        <X className="h-4 w-4" />
                    </button>
                </div>
                <div className="mt-6 grid gap-4 md:grid-cols-3">
                    {[
                        { id: 'starter', title: 'Starter', copy: 'Catalyst membership starts here and unlocks Chat Story on the same account.' },
                        { id: 'creator', title: 'Creator', copy: 'Higher membership headroom for operators who expect heavier usage.' },
                        { id: 'pro', title: 'Pro', copy: 'Highest public membership tier for daily operator volume.' },
                    ].map((entry) => (
                        <button
                            key={entry.id}
                            type="button"
                            onClick={() => { void handleMonthlyAccessCheckout(entry.id as 'starter' | 'creator' | 'pro'); }}
                            disabled={subscriptionCheckoutPlan !== null}
                            className="rounded-[24px] border border-white/[0.08] bg-white/[0.03] p-5 text-left transition hover:border-violet-500/40 hover:bg-violet-500/[0.04] disabled:opacity-60"
                        >
                            <p className="text-lg font-semibold text-white">{entry.title}</p>
                            <p className="mt-2 text-sm text-gray-400">{entry.copy}</p>
                            <div className="mt-5 inline-flex items-center gap-2 rounded-lg bg-violet-600 px-3 py-2 text-xs font-semibold text-white">
                                {subscriptionCheckoutPlan === entry.id ? (
                                    <>
                                        <Loader2 className="h-4 w-4 animate-spin" />
                                        Redirecting...
                                    </>
                                ) : (
                                    <>
                                        Unlock with PayPal
                                        <ArrowRight className="h-4 w-4" />
                                    </>
                                )}
                            </div>
                        </button>
                    ))}
                </div>
                {subscriptionPromptError && (
                    <p className="mt-4 rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-300">{subscriptionPromptError}</p>
                )}
            </div>
        </div>
    ) : null;

    useEffect(() => {
        if (typeof document === 'undefined') return;
        if (!imageModelPickerOpen && !videoModelPickerOpen) return;
        const previousOverflow = document.body.style.overflow;
        document.body.style.overflow = 'hidden';
        return () => {
            document.body.style.overflow = previousOverflow;
        };
    }, [imageModelPickerOpen, videoModelPickerOpen]);

    const handleModelPickerWheel = useCallback((event: WheelEvent<HTMLDivElement>) => {
        event.preventDefault();
        event.stopPropagation();
        const target = event.currentTarget;
        if (target && typeof target.scrollTop === 'number') {
            target.scrollTop += event.deltaY;
        }
    }, []);

    const imageModelPickerModal = imageModelPickerOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center overflow-y-auto bg-black/72 px-4 py-8">
            <div
                onWheelCapture={handleModelPickerWheel}
                className="w-full max-w-6xl max-h-[88vh] overflow-hidden rounded-[32px] border border-white/[0.08] bg-[#0d0d11] p-6 shadow-2xl shadow-black/50"
            >
                <div className="flex items-start justify-between gap-4">
                    <div>
                        <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-cyan-300">Image Generation Model</p>
                        <h3 className="mt-2 text-2xl font-bold text-white">Choose the image lane for this workspace</h3>
                        <p className="mt-2 text-sm text-gray-400">
                            {skeletonSceneModelLocked
                                ? 'Skeleton scene generation is locked to Grok Imagine via fal.ai. Seedream stays thumbnail-only.'
                                : 'Basic lanes stay in the normal Studio burn. Premium and elite lanes consume Catalyst credits first from included credits, then from the credit wallet.'}
                        </p>
                    </div>
                    <button
                        type="button"
                        onClick={() => setImageModelPickerOpen(false)}
                        className="rounded-lg p-2 text-gray-400 transition hover:bg-white/[0.05] hover:text-white"
                        title="Close image model picker"
                    >
                        <X className="h-4 w-4" />
                    </button>
                </div>
                <div className="mt-4 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3 text-sm text-cyan-100">
                    Available now: {animationCreditsAvailable} Catalyst credits across included credits + credit wallet.
                </div>
                <div onWheelCapture={handleModelPickerWheel} className="mt-6 max-h-[58vh] overflow-y-auto overscroll-contain pr-1">
                    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                        {sceneImageModelOptions.map((model) => {
                            const active = selectedImageModel.id === model.id;
                            return (
                                <button
                                    key={model.id}
                                    type="button"
                                    onClick={() => {
                                        setImageModelId(model.id);
                                        setImageModelPickerOpen(false);
                                    }}
                                    className={`rounded-[24px] border p-5 text-left transition ${
                                        active
                                            ? 'border-cyan-400 bg-cyan-500/10'
                                            : 'border-white/[0.08] bg-white/[0.03] hover:border-cyan-400/40 hover:bg-cyan-500/[0.04]'
                                    }`}
                                >
                                    <div className="flex flex-wrap items-center justify-between gap-2">
                                        <span className="text-lg font-semibold text-white">{model.label}</span>
                                        <div className="flex flex-wrap items-center gap-2">
                                            <span className={`rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] ${
                                                model.tier === 'elite'
                                                    ? 'border border-fuchsia-500/30 bg-fuchsia-500/10 text-fuchsia-200'
                                                    : model.tier === 'premium'
                                                        ? 'border border-amber-500/30 bg-amber-500/10 text-amber-200'
                                                        : 'border border-emerald-500/30 bg-emerald-500/10 text-emerald-200'
                                            }`}>
                                                {formatModelTierLabel(model)}
                                            </span>
                                            {active ? (
                                                <span className="rounded-full border border-cyan-400/30 bg-cyan-400/10 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-cyan-100">
                                                    Selected
                                                </span>
                                            ) : null}
                                        </div>
                                    </div>
                                    <p className="mt-3 text-sm leading-relaxed text-gray-300">{model.summary}</p>
                                    <div className="mt-4 flex items-center justify-between gap-3 text-xs text-gray-400">
                                        <span>{model.provider === 'fal' ? 'fal.ai' : 'NYPTID Hybrid'}</span>
                                        <span>{model.speed}</span>
                                    </div>
                                    <div className="mt-5 rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3">
                                        <p className="text-lg font-semibold text-white">{formatModelSpendLabel(model, 'image')}</p>
                                        <p className="mt-1 text-xs text-gray-400">Use premium lanes when you want higher-end prompt fidelity and composition.</p>
                                    </div>
                                </button>
                            );
                        })}
                    </div>
                </div>
            </div>
        </div>
    ) : null;

    const videoModelPickerModal = videoModelPickerOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center overflow-y-auto bg-black/72 px-4 py-8">
            <div
                onWheelCapture={handleModelPickerWheel}
                className="w-full max-w-5xl max-h-[88vh] overflow-hidden rounded-[32px] border border-white/[0.08] bg-[#0d0d11] p-6 shadow-2xl shadow-black/50"
            >
                <div className="flex items-start justify-between gap-4">
                    <div>
                        <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-cyan-300">Video Generation Model</p>
                        <h3 className="mt-2 text-2xl font-bold text-white">Choose the animation lane for final render</h3>
                        <p className="mt-2 text-sm text-gray-400">
                            Kling 2.1 Standard stays on the basic lane. Premium and elite video lanes multiply Catalyst credit burn per animated scene.
                        </p>
                    </div>
                    <button
                        type="button"
                        onClick={() => setVideoModelPickerOpen(false)}
                        className="rounded-lg p-2 text-gray-400 transition hover:bg-white/[0.05] hover:text-white"
                        title="Close video model picker"
                    >
                        <X className="h-4 w-4" />
                    </button>
                </div>
                <div className="mt-4 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3 text-sm text-cyan-100">
                    Available now: {animationCreditsAvailable} Catalyst credits across included credits + credit wallet.
                </div>
                <div onWheelCapture={handleModelPickerWheel} className="mt-6 max-h-[58vh] overflow-y-auto overscroll-contain pr-1">
                    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                        {videoModelCatalog.filter((model) => model.enabled !== false).map((model) => {
                            const active = selectedVideoModel.id === model.id;
                            return (
                                <button
                                    key={model.id}
                                    type="button"
                                    onClick={() => {
                                        setVideoModelId(model.id);
                                        setVideoModelPickerOpen(false);
                                    }}
                                    className={`rounded-[24px] border p-5 text-left transition ${
                                        active
                                            ? 'border-cyan-400 bg-cyan-500/10'
                                            : 'border-white/[0.08] bg-white/[0.03] hover:border-cyan-400/40 hover:bg-cyan-500/[0.04]'
                                    }`}
                                >
                                    <div className="flex flex-wrap items-center justify-between gap-2">
                                        <span className="text-lg font-semibold text-white">{model.label}</span>
                                        <div className="flex flex-wrap items-center gap-2">
                                            <span className={`rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] ${
                                                model.tier === 'elite'
                                                    ? 'border border-fuchsia-500/30 bg-fuchsia-500/10 text-fuchsia-200'
                                                    : model.tier === 'premium'
                                                        ? 'border border-amber-500/30 bg-amber-500/10 text-amber-200'
                                                        : 'border border-emerald-500/30 bg-emerald-500/10 text-emerald-200'
                                            }`}>
                                                {formatModelTierLabel(model)}
                                            </span>
                                            {active ? (
                                                <span className="rounded-full border border-cyan-400/30 bg-cyan-400/10 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-cyan-100">
                                                    Selected
                                                </span>
                                            ) : null}
                                        </div>
                                    </div>
                                    <p className="mt-3 text-sm leading-relaxed text-gray-300">{model.summary}</p>
                                    <div className="mt-4 flex items-center justify-between gap-3 text-xs text-gray-400">
                                        <span>{model.provider === 'fal' ? 'fal.ai' : model.provider}</span>
                                        <span>{model.speed}</span>
                                    </div>
                                    <div className="mt-5 rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3">
                                        <p className="text-lg font-semibold text-white">{formatModelSpendLabel(model, 'video')}</p>
                                        <p className="mt-1 text-xs text-gray-400">Use premium animation lanes when shot quality matters more than credit efficiency.</p>
                                    </div>
                                </button>
                            );
                        })}
                    </div>
                </div>
            </div>
        </div>
    ) : null;

    const animationCreditPromptModal = animationCreditPromptRequired !== null ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/72 px-4 py-8">
            <div className="w-full max-w-2xl rounded-[32px] border border-cyan-500/20 bg-[#0d0d11] p-6 shadow-2xl shadow-black/50">
                <div className="flex items-start justify-between gap-4">
                    <div>
                        <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-cyan-300">
                            {animationCreditPromptMode === 'image' ? 'Premium Image Lane' : 'Catalyst Credits Needed'}
                        </p>
                        <h3 className="mt-2 text-2xl font-bold text-white">
                            {animationCreditPromptMode === 'image'
                                ? `This image run needs ${animationCreditPromptRequired} Catalyst credit${animationCreditPromptRequired === 1 ? '' : 's'}`
                                : `This render needs ${animationCreditPromptRequired} Catalyst credit${animationCreditPromptRequired === 1 ? '' : 's'}`}
                        </h3>
                        <p className="mt-2 text-sm text-gray-400">
                            {animationCreditPromptMode === 'image'
                                ? `You currently have ${animationCreditsAvailable} available across included credits and your credit wallet. Buy more with PayPal or switch back to a basic image lane.`
                                : `You currently have ${animationCreditsAvailable} available across included credits and your credit wallet. Buy more credits with PayPal or continue in slideshow mode right now.`}
                        </p>
                    </div>
                    <button
                        type="button"
                        onClick={() => setAnimationCreditPromptRequired(null)}
                        className="rounded-lg p-2 text-gray-400 transition hover:bg-white/[0.05] hover:text-white"
                        title="Close credit prompt"
                    >
                        <X className="h-4 w-4" />
                    </button>
                </div>
                <div className="mt-6 flex flex-wrap gap-3">
                    <button
                        type="button"
                        onClick={() => { void handleAnimationTopupCheckout(animationCreditPromptRequired); }}
                        className="inline-flex items-center gap-2 rounded-xl bg-cyan-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-cyan-500"
                    >
                        Buy Credits with PayPal
                        <ArrowRight className="h-4 w-4" />
                    </button>
                    <button
                        type="button"
                        onClick={() => {
                            if (animationCreditPromptMode === 'image') {
                                const basicImageLane = (selectedTemplate === 'skeleton'
                                    ? imageModelCatalog.find((model) => model.id === 'grok_imagine')
                                    : undefined)
                                    || imageModelCatalog.find((model) => model.id === 'studio_default')
                                    || imageModelCatalog.find((model) => model.tier === 'basic')
                                    || fallbackImageModelCatalog[0];
                                setImageModelId(basicImageLane.id);
                            } else {
                                setAnimateOutputEnabled(false);
                            }
                            setAnimationCreditPromptRequired(null);
                            setAnimationCreditPromptError(null);
                        }}
                        className="rounded-xl border border-white/[0.08] bg-white/[0.03] px-4 py-3 text-sm font-semibold text-gray-200 transition hover:bg-white/[0.06]"
                    >
                        {animationCreditPromptMode === 'image' ? 'Switch to Basic Image Lane' : 'Continue with Slideshow'}
                    </button>
                </div>
                {animationCreditPromptError && (
                    <p className="mt-4 rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-300">{animationCreditPromptError}</p>
                )}
            </div>
        </div>
    ) : null;

    const openDraftProject = async (projectId: string) => {
        if (!session) return;
        setProjectsError(null);
        try {
            const res = await fetch(`${API}/api/projects/${projectId}`, {
                headers: { Authorization: `Bearer ${session.access_token}` },
            });
            if (!res.ok) throw new Error("Failed to open draft");
            const { data } = await readJsonResponse<any>(res);
            if (!data || typeof data !== "object") throw new Error("Draft payload was invalid");
            const p: ProjectRow | undefined = data.project;
            if (!p || typeof p !== "object") throw new Error("Draft project is missing");
            setCreateSubTab('builder');
            setSelectedTemplate(p.template || 'skeleton');
            setPrompt(p.topic || '');
            setWorkspaceStage('script');
            if (p.resolution === '720p' || p.resolution === '1080p') setResolution(p.resolution);
            if (p.language) setLanguage(p.language);
            if (typeof p.story_animation_enabled === 'boolean') {
                setStoryAnimationEnabled(p.story_animation_enabled);
            } else {
                setStoryAnimationEnabled(true);
            }
            if (typeof p.animation_enabled === 'boolean') {
                setAnimateOutputEnabled(p.animation_enabled);
            } else if (typeof p.story_animation_enabled === 'boolean') {
                setAnimateOutputEnabled(p.story_animation_enabled);
            } else {
                setAnimateOutputEnabled(true);
            }
            setCreativeMode(p.mode === 'script_to_short' ? 'script_to_short' : (p.mode === 'creative' ? 'creative' : 'auto'));
            if (typeof p.voice_id === 'string') setStoryVoiceId(p.voice_id);
            if (typeof p.voice_speed === 'number' && Number.isFinite(p.voice_speed)) {
                setStoryVoiceSpeed(Math.max(0.8, Math.min(1.35, p.voice_speed)));
            }
            if (p.pacing_mode === 'standard' || p.pacing_mode === 'fast' || p.pacing_mode === 'very_fast') {
                setStoryPacingMode(p.pacing_mode);
            }
            if (typeof p.art_style === 'string' && p.art_style) {
                setArtStyle(p.art_style);
            } else {
                setArtStyle('auto');
            }
            if (typeof p.image_model_id === 'string' && p.image_model_id) {
                setImageModelId(p.image_model_id);
            }
            if (typeof p.video_model_id === 'string' && p.video_model_id) {
                setVideoModelId(p.video_model_id);
            }
            if (typeof p.youtube_channel_id === 'string') {
                setYoutubeChannelId(p.youtube_channel_id);
            }
            if (typeof p.trend_hunt_enabled === 'boolean') {
                setTrendHuntEnabled(p.trend_hunt_enabled);
            }
            setCinematicBoostEnabled(Boolean(p.cinematic_boost) || cinematicBoostAlwaysOn);
            if (p.mode === 'creative' || p.mode === 'script_to_short') {
                const hydratedScenes: CreativeScene[] = Array.isArray(p.scenes) && p.scenes.length > 0
                    ? p.scenes.map((s: any, i: number) => ({
                        index: Number.isFinite(Number(s?.index)) ? Number(s.index) : i,
                        narration: String(s?.narration || ""),
                        visual_description: String(s?.visual_description || ""),
                        negative_prompt: String(s?.negative_prompt || ""),
                        duration_sec: Number(s?.duration_sec || 5),
                        imageData: typeof s?.imageData === 'string' ? s.imageData : undefined,
                        imageLoading: typeof s?.imageLoading === 'boolean' ? s.imageLoading : undefined,
                        generation_id: typeof s?.generation_id === 'string' ? s.generation_id : undefined,
                        imageError: typeof s?.imageError === 'string' ? s.imageError : undefined,
                        qa_ok: typeof s?.qa_ok === 'boolean' ? s.qa_ok : undefined,
                        qa_score: typeof s?.qa_score === 'number' ? s.qa_score : undefined,
                        qa_notes: Array.isArray(s?.qa_notes) ? s.qa_notes : undefined,
                    }))
                    : [{ index: 0, narration: "", visual_description: "", negative_prompt: "", duration_sec: 5 }];
                const readyPromptCount = hydratedScenes.filter((scene) => !!scene.visual_description.trim()).length;
                const readyImageCount = hydratedScenes.filter((scene) => !!scene.imageData).length;
                setCreativeStep('edit');
                setSessionId(p.session_id || null);
                setSceneBuildLoading(false);
                setCreativeScenes(hydratedScenes);
                setCreativeTitle(p.title || p.topic || 'Untitled Short');
                setCreativeNarration(p.narration || "");
                setScriptScenesReady(Boolean((p.session_id || "").trim()) && Array.isArray(p.scenes) && p.scenes.length > 0);
                setSceneBuildError(null);
                if (readyPromptCount > 0 && readyImageCount === readyPromptCount) {
                    setWorkspaceStage('audio');
                } else if (readyPromptCount > 0) {
                    setWorkspaceStage('scenes');
                } else {
                    setWorkspaceStage('script');
                }
            } else if (p.job_id) {
                setJobId(p.job_id);
                setLoading(true);
                setWorkspaceStage('audio');
            }
        } catch (e: any) {
            setProjectsError(e?.message || "Failed to open project");
        }
    };

    const globalToggleAnimationMode = useCallback(() => {
        if (loading || scriptLoading) return;
        if (effectiveAnimationEnabled) {
            setAnimateOutputEnabled(false);
            return;
        }
        if (animationCreditExhausted) {
            openAnimationCreditPrompt(1);
            return;
        }
        setAnimateOutputEnabled(true);
    }, [animationCreditExhausted, effectiveAnimationEnabled, loading, scriptLoading]);
    const globalSetRenderMode = useCallback((mode: 'slideshow' | 'animation') => {
        if (loading || scriptLoading) return;
        if (mode === 'slideshow') {
            setAnimateOutputEnabled(false);
            return;
        }
        if (effectiveAnimationEnabled) return;
        if (animationCreditExhausted) {
            openAnimationCreditPrompt(1);
            return;
        }
        setAnimateOutputEnabled(true);
    }, [animationCreditExhausted, effectiveAnimationEnabled, loading, scriptLoading]);
    const normalizeGenerationAssetUrl = useCallback((raw: unknown): string | null => {
        const value = String(raw || '').trim();
        if (!value) return null;
        if (/^(https?:|data:|blob:)/i.test(value)) return value;
        const sanitized = value.replace(/^\/+/, '');
        return value.startsWith('/') ? `${GENERATION_API}${value}` : `${GENERATION_API}/${sanitized}`;
    }, []);
    const extractScenePreviewUrl = useCallback((sceneAsset: unknown): string | null => {
        if (!sceneAsset) return null;
        if (typeof sceneAsset === 'string') return normalizeGenerationAssetUrl(sceneAsset);
        if (typeof sceneAsset === 'object') {
            const candidateMap = sceneAsset as Record<string, unknown>;
            for (const candidate of [
                candidateMap.image_url,
                candidateMap.url,
                candidateMap.imageData,
                candidateMap.image_data,
                candidateMap.preview_url,
            ]) {
                const resolved = normalizeGenerationAssetUrl(candidate);
                if (resolved) return resolved;
            }
        }
        return null;
    }, [normalizeGenerationAssetUrl]);
    const globalRenderProgressPreview = useMemo(() => {
        const sceneImages = Array.isArray(jobStatus?.scene_images) ? jobStatus.scene_images : [];
        const safeSceneIndex = Math.max(0, Number(jobStatus?.current_scene || 1) - 1);
        const candidates: unknown[] = [];
        if (sceneImages[safeSceneIndex]) candidates.push(sceneImages[safeSceneIndex]);
        for (let index = sceneImages.length - 1; index >= 0; index -= 1) {
            if (index !== safeSceneIndex && sceneImages[index]) candidates.push(sceneImages[index]);
        }
        if (creativeScenes[safeSceneIndex]?.imageData) candidates.push(creativeScenes[safeSceneIndex].imageData);
        for (let index = creativeScenes.length - 1; index >= 0; index -= 1) {
            if (index !== safeSceneIndex && creativeScenes[index]?.imageData) candidates.push(creativeScenes[index].imageData);
        }
        for (const candidate of candidates) {
            const url = extractScenePreviewUrl(candidate);
            if (url) {
                return {
                    url,
                    kind: 'image' as const,
                    label: `Scene ${safeSceneIndex + 1} preview`,
                };
            }
        }
        if (jobStatus?.output_file) {
            return {
                url: `${GENERATION_API}/api/download/${jobStatus.output_file}`,
                kind: 'video' as const,
                label: 'Current render output',
            };
        }
        return null;
    }, [creativeScenes, extractScenePreviewUrl, jobStatus]);
    useEffect(() => {
        if (creativeStep === 'generating') {
            setRenderMonitorDismissed(false);
        }
    }, [creativeStep, jobId]);
    const globalActiveRenderStatus = creativeStep === 'generating' && jobId
        ? (jobStatus || (loading ? { status: 'queued', progress: 0 } : null))
        : null;
    const globalRenderProgressWindow = globalActiveRenderStatus && !renderMonitorDismissed && globalActiveRenderStatus.status !== 'complete' && globalActiveRenderStatus.status !== 'error' ? (
        <RenderProgressWindow
            jobStatus={globalActiveRenderStatus}
            title={`${creativeTitle || currentTemplateMeta?.title || 'Untitled Project'} • ${effectiveAnimationEnabled ? 'Animation' : 'Slideshow'}`}
            previewUrl={globalRenderProgressPreview?.url || null}
            previewType={globalRenderProgressPreview?.kind || 'image'}
            previewLabel={globalRenderProgressPreview?.label || (effectiveAnimationEnabled ? 'Animation render preview' : 'Slideshow render preview')}
            onDismiss={() => setRenderMonitorDismissed(true)}
        />
    ) : null;

    if ((creativeMode === 'creative' || creativeMode === 'script_to_short') && creativeStep === 'edit' && createSubTab === 'builder') {
    const hasNarration = creativeNarration.trim().length > 0;
    const promptScenes = creativeScenes.filter((s) => !!s.visual_description.trim());
    const promptSceneCount = promptScenes.length;
    const imageReadyCount = promptScenes.filter((s) => !!s.imageData).length;
    const allPromptedImagesReady = promptSceneCount > 0 && imageReadyCount === promptSceneCount;
    const selectedImageCreditCost = Math.max(0, Number(selectedImageModel.credit_cost_per_image || 0));
    const selectedVideoCreditMultiplier = Math.max(1, Number(selectedVideoModel.credit_multiplier || 1));
    const pendingImageTargets = promptScenes.filter((scene) => !scene.imageData);
    const batchImageCreditsRequired = selectedImageCreditCost > 0 && pendingImageTargets.length > 0
        ? pendingImageTargets.length * selectedImageCreditCost
        : 0;
    const animationCreditsRequired = effectiveAnimationEnabled
        ? Math.max(1, (promptSceneCount || creativeScenes.length || 1) * selectedVideoCreditMultiplier)
        : 0;
    const imageCreditsShort = !isAdmin && batchImageCreditsRequired > animationCreditsAvailable;
    const animationCreditsShort = !isAdmin && effectiveAnimationEnabled && animationCreditsRequired > animationCreditsAvailable;
    const showGenerateScenes = creativeMode === 'creative' || creativeMode === 'script_to_short';
    const canAdvanceToScenes = hasNarration && !sceneBuildLoading;
    const activeTemplateMeta = templates.find((template) => template.id === selectedTemplate);
    const workspaceReadiness = [
        { label: 'Script ready', done: hasNarration },
        { label: 'Scenes planned', done: promptSceneCount > 0 },
        { label: 'Images ready', done: allPromptedImagesReady },
        { label: 'Audio ready', done: hasNarration && allPromptedImagesReady && Boolean(sessionId) },
    ];
    const activeStageCopy: Record<'script' | 'scenes' | 'audio', { title: string; description: string }> = {
        script: {
            title: 'Script',
            description: creativeMode === 'script_to_short'
                ? 'Paste the exact narration, then move into Scenes to generate prompt beats locked to the script order.'
                : 'Lock the narration, art style, and sound direction before moving into scene generation.',
        },
        scenes: {
            title: 'Scenes',
            description: creativeMode === 'script_to_short'
                ? 'Generate script-locked scene prompts, then fix or regenerate images scene-by-scene before render.'
                : 'Generate images, fix prompts scene-by-scene, and keep every beat production-ready before render.',
        },
        audio: {
            title: 'Audio',
            description: 'Lock voice, captions, pacing, and export settings, then render your short.',
        },
    };

        return (
            <div className="w-full max-w-none pb-10 space-y-6">
                {renderWorkspaceChrome({
                    subtitle: activeStageCopy[workspaceStage].description,
                    showBack: true,
                })}

                <div className="grid gap-3 md:grid-cols-4">
                    {workspaceReadiness.map((item) => (
                        <div key={item.label} className="rounded-2xl border border-white/[0.06] bg-white/[0.02] px-4 py-3">
                            <p className="text-[10px] uppercase tracking-[0.18em] text-gray-500">{item.label}</p>
                            <p className={`mt-2 text-sm font-semibold ${item.done ? 'text-emerald-200' : 'text-gray-300'}`}>
                                {item.done ? 'Ready' : 'Pending'}
                            </p>
                        </div>
                    ))}
                </div>

                <div className="space-y-6 min-w-0">
                <div className="flex flex-wrap items-center justify-between gap-3">
                    <div>
                        <h1 className="text-xl font-bold text-white">{creativeTitle || activeTemplateMeta?.title || 'Untitled Project'}</h1>
                        <p className="text-sm text-gray-500">{creativeScenes.length} scene{creativeScenes.length !== 1 ? 's' : ''} &middot; {creativeMode === 'script_to_short' ? 'Script to Short' : 'Creative Control'} &middot; {resolution} &middot; {language.toUpperCase()}</p>
                    </div>
                    {workspaceStage === 'audio' ? (
                        <button
                            type="button"
                            onClick={globalToggleAnimationMode}
                            disabled={loading || scriptLoading}
                            className={`rounded-full border px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] transition ${
                                effectiveAnimationEnabled
                                    ? 'border-emerald-500/40 bg-emerald-500/15 text-emerald-100 hover:bg-emerald-500/20'
                                    : 'border-cyan-500/30 bg-cyan-500/10 text-cyan-100 hover:bg-cyan-500/15'
                            } disabled:opacity-50`}
                            title="Switch output mode"
                        >
                            {effectiveAnimationEnabled ? 'Animation Enabled' : 'Slideshow Mode • Click to Animate'}
                        </button>
                    ) : (
                        <div className="rounded-full border border-white/[0.08] bg-black/20 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-gray-300">
                            {effectiveAnimationEnabled ? 'Animation Enabled' : 'Slideshow Mode'}
                        </div>
                    )}
                </div>
                {animationCreditsShort && (
                    <div className="rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                        This render currently needs {animationCreditsRequired} Catalyst credit{animationCreditsRequired === 1 ? '' : 's'} on {selectedVideoModel.label}, but your account only has {animationCreditsAvailable}. Switch to slideshow, choose a basic video lane, or top up before final render.
                    </div>
                )}
                {workspaceStage === 'scenes' && imageCreditsShort && (
                    <div className="rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                        Your selected image lane ({selectedImageModel.label}) needs {batchImageCreditsRequired} Catalyst credits to generate the remaining scene images, but only {animationCreditsAvailable} are available. Switch to a basic image lane or top up before batch generation.
                    </div>
                )}

                {renderWorkspaceStageTabs()}

                {workspaceStage === 'scenes' && (
                    <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,0.8fr)]">
                        <button
                            type="button"
                            onClick={() => setImageModelPickerOpen(true)}
                            className="rounded-2xl border border-white/[0.08] bg-white/[0.03] p-5 text-left transition hover:border-cyan-400/40 hover:bg-cyan-500/[0.04]"
                        >
                            <div className="flex flex-wrap items-start justify-between gap-3">
                                <div>
                                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-cyan-300">Image Generation Model</p>
                                    <h3 className="mt-2 text-lg font-semibold text-white">{selectedImageModel.label}</h3>
                                    <p className="mt-2 text-sm text-gray-400">
                                        {skeletonSceneModelLocked
                                            ? 'Skeleton AI short scenes are locked to Grok Imagine via fal.ai. Seedream is reserved for thumbnail work.'
                                            : selectedImageModel.summary}
                                    </p>
                                </div>
                                <span className={`rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] ${
                                    selectedImageModel.tier === 'elite'
                                        ? 'border border-fuchsia-500/30 bg-fuchsia-500/10 text-fuchsia-200'
                                        : selectedImageModel.tier === 'premium'
                                            ? 'border border-amber-500/30 bg-amber-500/10 text-amber-200'
                                            : 'border border-emerald-500/30 bg-emerald-500/10 text-emerald-200'
                                }`}>
                                    {formatModelTierLabel(selectedImageModel)}
                                </span>
                            </div>
                            <div className="mt-4 flex flex-wrap items-center justify-between gap-3 text-sm text-gray-300">
                                <span>{formatModelSpendLabel(selectedImageModel, 'image')}</span>
                                <span>{selectedImageModel.speed}</span>
                            </div>
                            <p className="mt-4 text-xs text-gray-500">
                                {skeletonSceneModelLocked
                                    ? 'Skeleton scenes stay on Grok Imagine. Other image lanes are not used for Skeleton scene generation.'
                                    : 'Premium image lanes pull Catalyst credits from included credits first, then your wallet. Click to change the model.'}
                            </p>
                        </button>
                        <div className="rounded-2xl border border-white/[0.08] bg-black/20 p-5">
                            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500">Catalyst Spend Snapshot</p>
                            <p className="mt-2 text-lg font-semibold text-white">
                                {skeletonSceneModelLocked
                                    ? 'Skeleton scene lane is locked to Grok Imagine via fal.ai'
                                    : selectedImageCreditCost > 0
                                    ? `${selectedImageCreditCost} credits per image on ${selectedImageModel.label}`
                                    : `${selectedImageModel.label} stays on the basic image lane`}
                            </p>
                            <p className="mt-2 text-sm text-gray-400">
                                Available now: {animationCreditsAvailable} combined credits across included monthly usage and the wallet.
                            </p>
                        </div>
                    </div>
                )}

                {workspaceStage === 'audio' && (
                    <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,0.8fr)]">
                        <button
                            type="button"
                            onClick={() => setVideoModelPickerOpen(true)}
                            className="rounded-2xl border border-white/[0.08] bg-white/[0.03] p-5 text-left transition hover:border-cyan-400/40 hover:bg-cyan-500/[0.04]"
                        >
                            <div className="flex flex-wrap items-start justify-between gap-3">
                                <div>
                                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-cyan-300">Video Generation Model</p>
                                    <h3 className="mt-2 text-lg font-semibold text-white">{selectedVideoModel.label}</h3>
                                    <p className="mt-2 text-sm text-gray-400">{selectedVideoModel.summary}</p>
                                </div>
                                <span className={`rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] ${
                                    selectedVideoModel.tier === 'elite'
                                        ? 'border border-fuchsia-500/30 bg-fuchsia-500/10 text-fuchsia-200'
                                        : selectedVideoModel.tier === 'premium'
                                            ? 'border border-amber-500/30 bg-amber-500/10 text-amber-200'
                                            : 'border border-emerald-500/30 bg-emerald-500/10 text-emerald-200'
                                }`}>
                                    {formatModelTierLabel(selectedVideoModel)}
                                </span>
                            </div>
                            <div className="mt-4 flex flex-wrap items-center justify-between gap-3 text-sm text-gray-300">
                                <span>{formatModelSpendLabel(selectedVideoModel, 'video')}</span>
                                <span>{selectedVideoModel.speed}</span>
                            </div>
                            <p className="mt-4 text-xs text-gray-500">Kling 2.1 Standard stays on the base animation lane. Premium video lanes multiply Catalyst credit burn by scene count.</p>
                        </button>
                        <div className="rounded-2xl border border-white/[0.08] bg-black/20 p-5">
                            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500">Render Spend Snapshot</p>
                            <p className="mt-2 text-lg font-semibold text-white">
                                {effectiveAnimationEnabled
                                    ? `${animationCreditsRequired} total Catalyst credits on ${selectedVideoModel.label}`
                                    : 'Slideshow mode keeps animation credit burn at zero'}
                            </p>
                            <p className="mt-2 text-sm text-gray-400">
                                {effectiveAnimationEnabled
                                    ? `${Math.max(1, promptSceneCount || creativeScenes.length || 1)} animated scene${Math.max(1, promptSceneCount || creativeScenes.length || 1) === 1 ? '' : 's'} × ${selectedVideoCreditMultiplier}x multiplier.`
                                    : 'Switch animation back on if you want motion render instead of a slideshow export.'}
                            </p>
                        </div>
                    </div>
                )}

                {workspaceStage === 'scenes' && showGenerateScenes && (
                    <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 px-4 py-3">
                        <p className="text-sm font-semibold text-amber-300">
                            {creativeMode === 'script_to_short' ? 'Script to Short' : 'Creative Scene Builder'}
                        </p>
                        <p className="text-xs text-amber-200/80 mt-1">
                            {creativeMode === 'script_to_short'
                                ? 'Generate prompt beats from the exact narration in order. Studio keeps every prompt editable, but it no longer starts from a blank scene list.'
                                : 'Start in Scenes, build the scene list, then generate images one by one across the full set before you render.'}
                        </p>
                        <div className="mt-3 flex flex-wrap items-center gap-3">
                            <button
                                onClick={handleGenerateScriptToShortScenes}
                                disabled={sceneBuildLoading || (!creativeNarration.trim() && !prompt.trim())}
                                className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-amber-600 hover:bg-amber-500 disabled:opacity-50 text-white transition"
                            >
                                {sceneBuildLoading ? "Generating scene prompts..." : (scriptScenesReady ? "Regenerate Prompts" : "Generate Scene Prompts")}
                            </button>
                            <button
                                onClick={handleGenerateSceneImageBatch}
                                disabled={bulkImageGenRunning || !sessionId || sceneBuildLoading || promptSceneCount === 0}
                                className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-cyan-600 hover:bg-cyan-500 disabled:opacity-50 text-white transition"
                            >
                                {bulkImageGenRunning ? "Generating images..." : "Generate Images"}
                            </button>
                            {!sessionId && (
                                <p className="text-xs text-amber-100/90">Generate scenes first.</p>
                            )}
                            {(bulkImageGenRunning || bulkImageGenTotal > 0) && (
                                <p className="text-xs text-amber-100/90">
                                    {bulkImageGenRunning
                                        ? `Batch progress: ${bulkImageGenDone}/${bulkImageGenTotal}`
                                        : `Images ready: ${imageReadyCount}/${promptSceneCount}`}
                                </p>
                            )}
                            {sceneBuildError && (
                                <p className="text-xs text-red-300">{sceneBuildError}</p>
                            )}
                        </div>
                    </div>
                )}

                {workspaceStage === 'script' && sceneBuildLoading && (
                    <div className="rounded-xl border border-cyan-500/30 bg-cyan-500/10 px-5 py-4 flex items-center gap-3">
                        <Loader2 className="w-4 h-4 text-cyan-300 animate-spin" />
                        <p className="text-sm text-cyan-100">Generating scene prompts from your script... this can take 10-30 seconds.</p>
                    </div>
                )}

                {workspaceStage === 'script' && (
                    <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-5 space-y-2">
                        <label className="text-xs text-gray-500 uppercase tracking-wider font-semibold block">Script / Narration (voiceover for the entire short)</label>
                        <textarea
                            value={creativeNarration}
                            onChange={(e) => setCreativeNarration(e.target.value)}
                            rows={4}
                            placeholder="Write the full voiceover script for your short here. This narration will play across all scenes..."
                            className="w-full bg-black/30 border border-white/[0.08] rounded-lg px-4 py-3 text-sm text-white placeholder:text-gray-600 focus:outline-none focus:ring-1 focus:ring-violet-500/50 resize-y"
                        />
                        <p className="text-xs text-gray-600">This script is for the entire video. The scenes below control what visuals appear.</p>
                    </div>
                )}

                {workspaceStage === 'script' && creativeMode === 'script_to_short' && (
                    <div className="rounded-xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3">
                        <div className="flex flex-wrap items-center justify-between gap-3">
                            <div>
                                <p className="text-sm font-semibold text-cyan-100">Next step: open the Scenes tab</p>
                                <p className="mt-1 text-xs text-cyan-100/80">
                                    Scene prompts are now generated from this exact narration in order, then you can edit every prompt before images are created.
                                </p>
                            </div>
                            <button
                                type="button"
                                onClick={() => setWorkspaceStage('scenes')}
                                disabled={!canAdvanceToScenes}
                                className="inline-flex items-center gap-2 rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-cyan-500 disabled:opacity-40"
                            >
                                Next: Scenes
                                <ArrowRight className="h-4 w-4" />
                            </button>
                        </div>
                    </div>
                )}

                {workspaceStage === 'script' && supportsArtStyle && (
                    <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4 space-y-3">
                        <p className="text-sm font-semibold text-white">Art Style ({storyArtStyleOptions.length} verified looks)</p>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                            {storyArtStyleOptions.map((style) => (
                                <button
                                    key={style.id}
                                    type="button"
                                    onClick={() => setArtStyle(style.id)}
                                    className={`rounded-lg p-3 text-left transition border ${
                                        artStyle === style.id
                                            ? 'border-cyan-400/70 bg-cyan-500/10'
                                            : 'border-white/[0.08] bg-white/[0.02] hover:border-white/20'
                                    }`}
                                >
                                    <p className="text-sm font-semibold text-white">{style.label}</p>
                                    <p className="text-xs text-gray-400 mt-1">{style.desc}</p>
                                </button>
                            ))}
                        </div>
                    </div>
                )}

                {workspaceStage === 'script' && (
                    <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4 space-y-3">
                        <div>
                            <p className="text-sm font-semibold text-white">Sound References</p>
                            <p className="mt-1 text-xs text-gray-500">
                                Pick the intended sound profile now so the finale audio stack knows whether this short should feel clean, cinematic, or high-pressure.
                            </p>
                        </div>
                        <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
                            {soundReferenceOptions.map((option) => (
                                <button
                                    key={option.id}
                                    type="button"
                                    onClick={() => setSoundReferencePreset(option.id)}
                                    className={`rounded-xl border p-3 text-left transition ${
                                        soundReferencePreset === option.id
                                            ? 'border-cyan-400/70 bg-cyan-500/10'
                                            : 'border-white/[0.08] bg-black/20 hover:border-white/20'
                                    }`}
                                >
                                    <p className="text-sm font-semibold text-white">{option.label}</p>
                                    <p className="mt-1 text-[11px] text-gray-400">{option.desc}</p>
                                </button>
                            ))}
                        </div>
                    </div>
                )}

                {workspaceStage === 'audio' && renderCustomVoiceLibraryCard()}

                {workspaceStage === 'audio' && templateSupportsVoiceControls && (
                    <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-5 space-y-5">
                        <div className="flex items-center justify-between gap-3">
                            <div>
                                <p className="text-sm font-semibold text-white">Voice &amp; Pacing</p>
                                <p className="text-[11px] text-gray-500 mt-0.5">These settings lock before render — preview a line to double-check before shipping.</p>
                            </div>
                            <button
                                onClick={() => { void previewStoryVoice(); }}
                                disabled={!storyVoiceId || storyPreviewLoading || storyVoicesLoading}
                                className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-cyan-600 text-white hover:bg-cyan-500 disabled:opacity-40 disabled:cursor-not-allowed transition whitespace-nowrap"
                            >
                                {storyPreviewLoading ? "Previewing..." : "Preview Voice"}
                            </button>
                        </div>

                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <div className="space-y-1.5">
                                <label className="text-[10px] uppercase tracking-[0.18em] text-gray-500">Voice</label>
                                <select
                                    value={storyVoiceId}
                                    onChange={(e) => setStoryVoiceId(e.target.value)}
                                    className="w-full bg-black/30 border border-white/[0.08] rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-cyan-500/40"
                                >
                                    {storyVoicesLoading ? (
                                        <option value="">Loading voices...</option>
                                    ) : storyVoices.length > 0 ? (
                                        storyVoices.map((v: any) => (
                                            <option key={String(v.voice_id || v.name || Math.random())} value={String(v.voice_id || "")}>
                                                {String(v.name || v.voice_id || "Voice")}
                                            </option>
                                        ))
                                    ) : (
                                        <option value="">Default voice</option>
                                    )}
                                </select>
                                {storyVoicesWarning ? (
                                    <p className="text-[11px] text-amber-300">{storyVoicesWarning}</p>
                                ) : storyVoicesSource === 'fallback' ? (
                                    <p className="text-[11px] text-gray-500">Using fallback voice catalog.</p>
                                ) : null}
                                {storyPreviewError ? (
                                    <p className="text-[11px] text-red-300">{storyPreviewError}</p>
                                ) : null}
                            </div>

                            <div className="space-y-1.5">
                                <div className="flex items-center justify-between">
                                    <label className="text-[10px] uppercase tracking-[0.18em] text-gray-500">Speed</label>
                                    <span className="text-[11px] text-gray-400 font-mono">{storyVoiceSpeed.toFixed(2)}x</span>
                                </div>
                                <input
                                    type="range"
                                    min={0.8}
                                    max={1.35}
                                    step={0.05}
                                    value={storyVoiceSpeed}
                                    onChange={(e) => setStoryVoiceSpeed(Number(e.target.value))}
                                    className="w-full accent-cyan-500"
                                />
                                <div className="flex justify-between text-[10px] text-gray-600 font-mono">
                                    <span>0.80x</span>
                                    <span>1.00x</span>
                                    <span>1.35x</span>
                                </div>
                            </div>
                        </div>

                        <div className="space-y-1.5">
                            <label className="text-[10px] uppercase tracking-[0.18em] text-gray-500">Pacing</label>
                            <div className="grid grid-cols-3 gap-2">
                                {[
                                    { id: 'standard', label: 'Standard', desc: 'Conversational' },
                                    { id: 'fast', label: 'Fast', desc: 'Energetic' },
                                    { id: 'very_fast', label: 'Very Fast', desc: 'High-velocity' },
                                ].map((p) => (
                                    <button
                                        key={p.id}
                                        type="button"
                                        onClick={() => setStoryPacingMode(p.id as 'standard' | 'fast' | 'very_fast')}
                                        className={`px-3 py-2 rounded-lg text-xs font-semibold transition border ${
                                            storyPacingMode === p.id
                                                ? 'bg-cyan-600 text-white border-cyan-500/40 shadow-md shadow-cyan-600/20'
                                                : 'bg-white/[0.03] text-gray-300 border-white/[0.06] hover:bg-white/[0.06] hover:border-white/[0.1]'
                                        }`}
                                    >
                                        <div>{p.label}</div>
                                        <div className={`text-[10px] mt-0.5 ${storyPacingMode === p.id ? 'text-cyan-100/80' : 'text-gray-500'}`}>{p.desc}</div>
                                    </button>
                                ))}
                            </div>
                        </div>

                        <div className="rounded-lg border border-white/[0.06] bg-black/20 px-4 py-3 flex items-center justify-between">
                            <div>
                                <p className="text-sm text-white font-medium">Burn subtitles into video</p>
                                <p className="text-[11px] text-gray-500 mt-0.5">Recommended — most short-form plays muted at first.</p>
                            </div>
                            <label className="relative inline-flex items-center cursor-pointer">
                                <input
                                    type="checkbox"
                                    checked={subtitlesEnabled}
                                    onChange={(e) => setSubtitlesEnabled(e.target.checked)}
                                    className="sr-only peer"
                                />
                                <div className="w-11 h-6 bg-white/[0.08] rounded-full peer-checked:bg-cyan-600 transition-colors">
                                    <div className={`w-5 h-5 bg-white rounded-full shadow-md transform transition-transform mt-0.5 ml-0.5 ${subtitlesEnabled ? 'translate-x-5' : ''}`} />
                                </div>
                            </label>
                        </div>
                    </div>
                )}

                {workspaceStage === 'scenes' && (
                <div className="space-y-4">
                    {creativeScenes.map((scene, idx) => (
                        <div key={idx} className="rounded-xl border border-white/[0.08] bg-white/[0.02] overflow-hidden">
                            <div className="p-4 border-b border-white/[0.06] flex items-center justify-between">
                                <span className="text-sm font-bold text-violet-400">Scene {idx + 1}</span>
                                <div className="flex items-center gap-3">
                                    <div className="flex items-center gap-1.5">
                                        <label className="text-xs text-gray-600">Duration</label>
                                        <select
                                            value={scene.duration_sec}
                                            onChange={(e) => handleUpdateScene(idx, 'duration_sec', Number(e.target.value))}
                                            className="bg-black/30 border border-white/[0.08] rounded px-2 py-1 text-xs text-white focus:outline-none">
                                            <option value={3}>3s</option>
                                            <option value={4}>4s</option>
                                            <option value={5}>5s</option>
                                            <option value={6}>6s</option>
                                            <option value={7}>7s</option>
                                            <option value={8}>8s</option>
                                            <option value={10}>10s</option>
                                        </select>
                                    </div>
                                    {creativeScenes.length > 1 && (
                                        <button onClick={() => handleRemoveScene(idx)} className="p-1 hover:bg-red-500/20 rounded transition" title="Remove scene">
                                            <Trash2 className="w-4 h-4 text-red-400" />
                                        </button>
                                    )}
                                </div>
                            </div>
                            <div className="p-4 space-y-3">
                                {creativeMode === 'script_to_short' && (
                                    <div className="rounded-lg border border-cyan-500/20 bg-cyan-500/10 px-3 py-3">
                                        <label className="mb-1 block text-xs uppercase tracking-wider text-cyan-200">Scene Beat From Script</label>
                                        <p className="text-sm leading-6 text-cyan-50">
                                            {scene.narration || 'No narration beat generated yet. Click Generate Scene Prompts above.'}
                                        </p>
                                    </div>
                                )}
                                <div>
                                    <label className="text-xs text-gray-500 uppercase tracking-wider mb-1 block">Image Prompt (what this scene looks like)</label>
                                    <textarea
                                        value={scene.visual_description}
                                        onChange={(e) => handleUpdateScene(idx, 'visual_description', e.target.value)}
                                        rows={2}
                                        placeholder={selectedTemplate === 'daytrading'
                                            ? "Describe the visual for this scene, e.g. 'A cinematic 3D trading desk with red candlestick collapse, floating risk overlays, and a trader reacting to the loss'"
                                            : "Describe the visual for this scene, e.g. 'A 3D skeleton wearing a doctor's coat in a hospital setting, dark moody lighting'"}
                                        className="w-full bg-black/30 border border-white/[0.08] rounded-lg px-3 py-2 text-sm text-white placeholder:text-gray-600 focus:outline-none focus:ring-1 focus:ring-violet-500/50 resize-none"
                                    />
                                </div>
                                <div>
                                    <label className="text-xs text-gray-500 uppercase tracking-wider mb-1 block">Negative Prompt (optional)</label>
                                    <textarea
                                        value={scene.negative_prompt || ""}
                                        onChange={(e) => handleUpdateScene(idx, 'negative_prompt', e.target.value)}
                                        rows={2}
                                        placeholder="Only add what you want to avoid (this is fully user-controlled in Creative Control)."
                                        className="w-full bg-black/30 border border-white/[0.08] rounded-lg px-3 py-2 text-sm text-white placeholder:text-gray-600 focus:outline-none focus:ring-1 focus:ring-violet-500/50 resize-none"
                                    />
                                </div>
                                <div className="flex items-center gap-3">
                                    <button
                                        onClick={() => handleGenerateSceneImage(idx)}
                                        disabled={scene.imageLoading || !scene.visual_description.trim() || !sessionId || sceneBuildLoading}
                                        className="px-4 py-2 bg-violet-600 hover:bg-violet-500 disabled:opacity-50 text-white text-sm font-medium rounded-lg transition flex items-center gap-2">
                                        {scene.imageLoading ? (
                                            <><Loader2 className="w-4 h-4 animate-spin" /> Generating...</>
                                        ) : scene.imageData ? (
                                            <><Sparkles className="w-4 h-4" /> Regenerate</>
                                        ) : (
                                            <><Image className="w-4 h-4" /> Generate Image</>
                                        )}
                                    </button>
                                    {scene.imageData && (
                                        scene.qa_ok === false ? (
                                            <span className="text-xs text-amber-300 flex items-center gap-1" title={summarizeSceneQaWarning(scene) || "Prompt match is weak. Regenerate for better match."}>
                                                <CheckCircle2 className="w-3 h-3" /> Image ready (weak match)
                                            </span>
                                        ) : (
                                            <span className="text-xs text-emerald-400 flex items-center gap-1">
                                                <CheckCircle2 className="w-3 h-3" /> Image ready
                                            </span>
                                        )
                                    )}
                                    {scene.imageData && (
                                        <button
                                            type="button"
                                            onClick={() => setScenePromptEditorIndex(idx)}
                                            className="px-3 py-2 rounded-lg border border-white/[0.08] bg-white/[0.03] text-xs font-semibold text-gray-200 transition hover:bg-white/[0.08]"
                                        >
                                            Edit Prompt
                                        </button>
                                    )}
                                </div>
                                {scene.imageError && (
                                    <p
                                        className="text-xs text-red-400 bg-red-500/10 rounded-lg px-3 py-2"
                                        title={scene.imageError}
                                    >
                                        Error: {summarizeSceneError(scene.imageError)}
                                    </p>
                                )}
                                {scene.imageData && scene.qa_ok === false && (
                                    <p className="text-xs text-amber-300 bg-amber-500/10 rounded-lg px-3 py-2">
                                        {summarizeSceneQaWarning(scene) || "Prompt match is weak. Regenerate this scene for better adherence."}
                                    </p>
                                )}
                                {scene.imageLoading && !scene.imageData && (
                                    <SceneImageLoadingCard template={selectedTemplate} />
                                )}
                                {scene.imageData && (
                                    <div className="rounded-lg bg-black/40 p-2">
                                        <button
                                            type="button"
                                            onClick={() => setScenePromptEditorIndex(idx)}
                                            className="block w-full cursor-pointer"
                                            title="Open prompt editor"
                                        >
                                            <img
                                                src={scene.imageData}
                                                alt={`Scene ${idx + 1}`}
                                                onClick={() => setScenePromptEditorIndex(idx)}
                                                draggable={false}
                                                className="mx-auto h-[360px] max-h-[55vh] w-auto max-w-full rounded-md object-contain"
                                            />
                                        </button>
                                        <p className="mt-2 text-center text-[11px] text-gray-400">
                                            Click preview to edit the exact prompt and regenerate this scene.
                                        </p>
                                    </div>
                                )}
                            </div>
                        </div>
                    ))}
                </div>
                )}

                {workspaceStage === 'scenes' && (
                    <button onClick={handleAddScene}
                        className="w-full py-3 border-2 border-dashed border-white/[0.1] hover:border-violet-500/40 rounded-xl text-gray-500 hover:text-violet-400 font-medium transition flex items-center justify-center gap-2">
                        <Plus className="w-4 h-4" /> Add Scene
                    </button>
                )}

                {workspaceStage === 'audio' && finalizeError && (
                    <div className="rounded-xl border border-red-500/30 bg-red-500/10 px-5 py-4 flex items-start gap-3">
                        <X className="w-5 h-5 text-red-400 flex-shrink-0 mt-0.5 cursor-pointer" onClick={() => setFinalizeError(null)} />
                        <p className="text-sm text-red-300">{finalizeError}</p>
                    </div>
                )}

                {workspaceStage === 'scenes' && (!allPromptedImagesReady ? (
                    <button
                        onClick={handleGenerateSceneImageBatch}
                        disabled={bulkImageGenRunning || sceneBuildLoading || !sessionId || promptSceneCount === 0}
                        className="w-full py-4 bg-cyan-600 hover:bg-cyan-500 disabled:opacity-40 text-white font-bold rounded-xl text-lg transition-all flex items-center justify-center gap-3 shadow-lg shadow-cyan-600/20">
                        {bulkImageGenRunning ? (
                            <><Loader2 className="w-5 h-5 animate-spin" /> Generating images...</>
                        ) : (
                            <><Image className="w-5 h-5" /> Generate Images</>
                        )}
                    </button>
                ) : (
                    <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-5 py-4 text-sm text-emerald-100">
                        All scene images are ready. Move to <span className="font-semibold">Finale</span> to render the full short.
                    </div>
                ))}

                {workspaceStage === 'audio' && (
                    <button
                        onClick={() => {
                            if (animationCreditsShort) {
                                openAnimationCreditPrompt(animationCreditsRequired, 'video');
                                return;
                            }
                            void handleFinalize();
                        }}
                        disabled={loading || !hasNarration || !sessionId}
                        className="w-full py-4 bg-emerald-600 hover:bg-emerald-500 disabled:opacity-40 text-white font-bold rounded-xl text-lg transition-all flex items-center justify-center gap-3 shadow-lg shadow-emerald-600/20">
                        {loading ? (
                            <><Loader2 className="w-5 h-5 animate-spin" /> Rendering your short...</>
                        ) : (
                            <><Film className="w-5 h-5" /> {effectiveAnimationEnabled ? 'Animate & Render Final Video' : 'Render Slideshow Video'}</>
                        )}
                    </button>
                )}

                {workspaceStage === 'audio' && !hasNarration && (
                    <p className="text-center text-sm text-gray-600">Write your script above to render.</p>
                )}
                {workspaceStage === 'audio' && !sessionId && (
                    <p className="text-center text-sm text-amber-300">Generate scenes first, then render.</p>
                )}
                {workspaceStage === 'audio' && sessionId && promptSceneCount > 0 && !allPromptedImagesReady && (
                    <p className="text-center text-sm text-cyan-300">Generate images for all scene prompts before rendering.</p>
                )}

                {bulkImageGenRunning && (
                    <div className="fixed inset-0 z-50 bg-black/70 backdrop-blur-[1px] flex items-center justify-center px-6">
                        <div className="w-full max-w-md rounded-2xl border border-cyan-400/30 bg-slate-950/95 p-6 text-center space-y-3">
                            <div className="flex items-center justify-center gap-2 text-cyan-200">
                                <Loader2 className="w-5 h-5 animate-spin" />
                                <p className="font-semibold">Generating images in batch...</p>
                            </div>
                            <p className="text-sm text-cyan-100/90">{bulkImageGenDone}/{bulkImageGenTotal} complete</p>
                            <div className="h-2 w-full bg-white/10 rounded-full overflow-hidden">
                                <div
                                    className="h-full bg-cyan-500 transition-all"
                                    style={{ width: `${bulkImageGenTotal > 0 ? Math.min(100, (bulkImageGenDone / bulkImageGenTotal) * 100) : 0}%` }}
                                />
                            </div>
                        </div>
                    </div>
                )}

                {jobStatus && (
                    <div className={`rounded-2xl border transition-all overflow-hidden ${
                        jobStatus.status === 'complete' ? 'border-emerald-500/30 bg-emerald-500/[0.03]' :
                        jobStatus.status === 'error' ? 'border-red-500/30 bg-red-500/[0.03]' :
                        'border-violet-500/20 bg-violet-500/[0.02]'
                    }`}>
                        {jobStatus.status === 'error' ? (
                            <div className="p-8 text-center">
                                <p className="text-red-400 font-bold text-lg mb-2">Generation Failed</p>
                                <p className="text-gray-500 text-sm">{jobStatus.error}</p>
                                <button onClick={() => { setJobStatus(null); setJobId(null); setLoading(false); setCreativeStep('edit'); }}
                                    className="mt-4 px-6 py-2 bg-white/5 hover:bg-white/10 rounded-lg text-sm transition">
                                    Back to Editor
                                </button>
                            </div>
                        ) : jobStatus.status === 'complete' ? (
                            <div>
                                <video controls autoPlay className="w-full max-h-[500px] bg-black" src={`${GENERATION_API}/api/download/${jobStatus.output_file}`} />
                                <div className="p-6 space-y-4">
                                    <div className="flex items-center justify-between">
                                        <div>
                                            <h3 className="font-bold text-lg text-emerald-400">{jobStatus.metadata?.title}</h3>
                                            <p className="text-gray-500 text-sm">
                                                {jobStatus.resolution && <span className="text-violet-400 mr-2">{jobStatus.resolution}</span>}
                                                {jobStatus.metadata?.tags?.map((t: string) => `#${t}`).join(' ')}
                                            </p>
                                        </div>
                                        <CheckCircle2 className="w-8 h-8 text-emerald-400" />
                                    </div>
                                    <a href={`${GENERATION_API}/api/download/${jobStatus.output_file}`} download
                                        className="flex items-center justify-center gap-2 w-full py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl transition-all">
                                        <Download className="w-5 h-5" /> Download MP4
                                    </a>
                                    <button onClick={handleResetCreative}
                                        className="w-full py-3 bg-white/5 hover:bg-white/10 text-gray-300 font-medium rounded-xl transition-all">
                                        Create Another
                                    </button>
                                    <FeedbackWidget jobId={jobId || ''} template={selectedTemplate} feature="creative" language={language} />
                                </div>
                            </div>
                        ) : (
                            <div className="p-8 space-y-4">
                                <ProgressBar progress={jobStatus.progress || 0} status={jobStatus.status} />
                                {jobStatus.current_scene && jobStatus.total_scenes && (
                                    <p className="text-center text-sm text-gray-600">
                                        Rendering scene {jobStatus.current_scene} of {jobStatus.total_scenes}
                                    </p>
                                )}
                                <JobDiagnostics jobStatus={jobStatus} />
                            </div>
                        )}
                    </div>
                )}
                    </div>
                {quickStartCard}
                {templateChooserModal}
                {subscriptionPromptModal}
                {imageModelPickerModal}
                {videoModelPickerModal}
                {animationCreditPromptModal}
                {globalRenderProgressWindow}
            </div>
        );
    }

    return (
            <div className="w-full max-w-none pb-10 space-y-6">
                {renderWorkspaceChrome({
                    subtitle: createSubTab === 'projects'
                        ? 'Open saved drafts or finished renders. Switch back to Create any time to open the template picker.'
                        : 'Create is the only live workflow in the rail right now. Use it to open AI Stories, Motivation, Skeleton AI, or Chat Story without sacrificing workspace width.',
                })}
                <div className="space-y-5 min-w-0">

                {createSubTab === 'projects' && (
                    <div className="space-y-6">
                        <div className="flex items-center justify-between">
                            <h2 className="text-lg font-bold text-white">Your Projects</h2>
                            <button onClick={loadProjects} className="px-3 py-1.5 rounded-lg bg-white/5 hover:bg-white/10 text-sm text-gray-300">Refresh</button>
                        </div>
                        {projectsError && (
                            <div className="rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-300">
                                {projectsError}
                            </div>
                        )}
                        {projectsLoading && <p className="text-sm text-gray-500">Loading projects...</p>}
                        {!projectsLoading && (
                            <>
                                <div className="space-y-3">
                                    <h3 className="text-sm font-semibold text-amber-300 uppercase tracking-wider">Drafts</h3>
                                    {projectDrafts.length === 0 && <p className="text-sm text-gray-500">No drafts yet.</p>}
                                    {projectDrafts.map((p) => (
                                        <div key={p.project_id} className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4 flex items-center justify-between gap-4">
                                            <div>
                                                <p className="text-sm font-semibold text-white">{p.topic || 'Untitled'}</p>
                                                <p className="text-xs text-gray-500 mt-1">{p.template} • {p.mode} • {p.status} • {p.scene_count || 0} scenes</p>
                                            </div>
                                            <button onClick={() => openDraftProject(p.project_id)} className="px-3 py-2 rounded-lg bg-violet-600 hover:bg-violet-500 text-xs font-semibold text-white">
                                                Open
                                            </button>
                                        </div>
                                    ))}
                                </div>
                                <div className="space-y-3">
                                    <h3 className="text-sm font-semibold text-emerald-300 uppercase tracking-wider">Renders</h3>
                                    {projectRenders.length === 0 && <p className="text-sm text-gray-500">No renders yet.</p>}
                                    {projectRenders.map((p) => (
                                        <div key={p.project_id} className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4 flex items-center justify-between gap-4">
                                            <div>
                                                <p className="text-sm font-semibold text-white">{p.title || p.topic || 'Untitled Render'}</p>
                                                <p className="text-xs text-gray-500 mt-1">{p.template} • {p.status} • {p.resolution || '720p'}</p>
                                            </div>
                                            {p.output_file ? (
                                                <a href={`${GENERATION_API}/api/download/${p.output_file}`} className="px-3 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-xs font-semibold text-white">Download</a>
                                            ) : (
                                                <span className="text-xs text-red-400">{p.error || 'No output file'}</span>
                                            )}
                                        </div>
                                    ))}
                                </div>
                            </>
                        )}
                    </div>
                )}

                {createSubTab !== 'builder' ? null : (
                <>
                <div className="space-y-5">
                    <div className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-5">
                        <div className="flex flex-wrap items-start justify-between gap-4">
                            <div>
                                <p className="text-[10px] font-semibold uppercase tracking-[0.24em] text-violet-300">Template Workspace</p>
                                <h2 className="mt-2 text-2xl font-bold text-white">{currentTemplateMeta.title}</h2>
                                <p className="mt-2 text-sm text-gray-400">
                                    {selectedTemplate === 'chatstory'
                                        ? 'Chat Story runs in a dedicated fullscreen-style editor on the same Catalyst render path as the rest of Studio.'
                                        : 'Auto, Creative Control, and Script to Short are all live build paths for the sellable short-form templates.'}
                                </p>
                            </div>
                            <div className="flex flex-wrap gap-2">
                                <button
                                    type="button"
                                    onClick={openTemplateChooser}
                                    className="rounded-xl border border-white/[0.08] bg-black/20 px-4 py-2 text-sm font-semibold text-white transition hover:border-violet-500/40 hover:bg-violet-500/10"
                                >
                                    Choose Template
                                </button>
                                {selectedTemplate === 'chatstory' ? (
                                    <span className="rounded-xl border border-violet-500/30 bg-violet-500/10 px-4 py-2 text-xs font-semibold uppercase tracking-[0.18em] text-violet-200">
                                        Catalyst Lane
                                    </span>
                                ) : (
                                    <span className="rounded-xl border border-emerald-500/30 bg-emerald-500/10 px-4 py-2 text-xs font-semibold uppercase tracking-[0.18em] text-emerald-200">
                                        Live Template
                                    </span>
                                )}
                            </div>
                        </div>
                    </div>

                    <div className="space-y-5">
                {selectedTemplate === 'chatstory' ? (
                    <ChatStoryPanel />
                ) : (
                <>
                {renderWorkspaceStageTabs()}
                {workspaceStage === 'script' && (
                <>
                {/* MODE TOGGLE */}
                <div>
                    <h2 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-2">Creation Mode</h2>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
                        <button onClick={() => !loading && setCreativeMode('auto')}
                            className={`flex-1 p-3 rounded-lg text-center transition-all border ${
                                creativeMode === 'auto' ? 'border-violet-500 bg-violet-500/10' : 'border-white/[0.06] bg-white/[0.02] hover:border-white/20'
                            }`}>
                            <Wand2 className="w-4 h-4 mx-auto mb-1 text-violet-400" />
                            <div className="text-xs font-bold">Auto</div>
                            <div className="text-[11px] text-gray-500 mt-0.5">
                                AI handles everything
                            </div>
                        </button>
                        <button onClick={() => !loading && setCreativeMode('creative')}
                            className={`flex-1 p-3 rounded-lg text-center transition-all border ${
                                creativeMode === 'creative' ? 'border-amber-500 bg-amber-500/10' : 'border-white/[0.06] bg-white/[0.02] hover:border-white/20'
                            }`}>
                            <Sliders className="w-4 h-4 mx-auto mb-1 text-amber-400" />
                            <div className="text-xs font-bold">Creative Control</div>
                            <div className="text-[11px] text-gray-500 mt-0.5">Edit prompts &amp; preview images</div>
                        </button>
                        <button onClick={() => !loading && setCreativeMode('script_to_short')}
                            className={`flex-1 p-3 rounded-lg text-center transition-all border ${
                                creativeMode === 'script_to_short' ? 'border-cyan-500 bg-cyan-500/10' : 'border-white/[0.06] bg-white/[0.02] hover:border-white/20'
                            }`}>
                            <Clapperboard className="w-4 h-4 mx-auto mb-1 text-cyan-400" />
                            <div className="text-xs font-bold flex items-center justify-center gap-1">
                                Script to Short
                                <span className="rounded border border-cyan-500/40 bg-cyan-500/10 px-1 py-0.5 text-[9px] uppercase tracking-wider text-cyan-200">Open Beta</span>
                            </div>
                            <div className="text-[11px] text-cyan-300 mt-0.5">Paste a script, get editable scenes</div>
                        </button>
                    </div>
                </div>

                {/* RESOLUTION PICKER */}
                <div>
                    <h2 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-2">Resolution</h2>
                    <div className="flex gap-2">
                        <button onClick={() => !loading && setResolution('720p')}
                            className={`flex-1 p-3 rounded-lg text-center transition-all border ${
                                resolution === '720p' ? 'border-violet-500 bg-violet-500/10' : 'border-white/[0.06] bg-white/[0.02] hover:border-white/20'
                            } ${loading ? 'opacity-50' : ''}`}>
                            <div className="text-base font-bold">720p</div>
                            <div className="text-[11px] text-gray-500 mt-0.5">Faster generation</div>
                        </button>
                        <button
                            onClick={() => {
                                if (!canUse1080p) return;
                                if (!loading) setResolution('1080p');
                            }}
                            className={`flex-1 p-3 rounded-lg text-center transition-all border relative ${
                                !canUse1080p ? 'opacity-50 cursor-not-allowed border-white/[0.04] bg-white/[0.01]' :
                                resolution === '1080p' ? 'border-violet-500 bg-violet-500/10' : 'border-white/[0.06] bg-white/[0.02] hover:border-white/20'
                            } ${loading ? 'opacity-50' : ''}`}>
                            {!canUse1080p && (
                                <div className="absolute top-2 right-2">
                                    <Lock className="w-3.5 h-3.5 text-gray-600" />
                                </div>
                            )}
                            <div className="text-base font-bold">1080p</div>
                            <div className="text-[11px] text-gray-500 mt-0.5">
                                {canUse1080p ? 'Best quality' : 'Temporarily unavailable'}
                            </div>
                        </button>
                    </div>
                </div>

                {/* LANGUAGE */}
                {languages.length > 0 && (
                    <div>
                        <h2 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-2">Language</h2>
                        <div className="flex flex-wrap gap-2">
                            {languages.map(l => (
                                <button key={l.code} onClick={() => !loading && setLanguage(l.code)}
                                    className={`px-2.5 py-1 rounded-md text-[11px] font-medium transition-all border ${
                                        language === l.code
                                            ? 'border-violet-500 bg-violet-500/10 text-violet-300'
                                            : 'border-white/[0.06] text-gray-500 hover:border-white/20'
                                    } ${loading ? 'opacity-50' : ''}`}>
                                    {l.name}
                                </button>
                            ))}
                        </div>
                        {language !== 'en' && (
                            <p className="text-xs text-violet-400 mt-2">Script and voiceover will be generated in {languages.find(l => l.code === language)?.name}</p>
                        )}
                    </div>
                )}

                {session && (
                    <div className="rounded-xl border border-cyan-500/20 bg-cyan-500/5 p-4 space-y-3">
                        <div className="flex flex-wrap items-start justify-between gap-3">
                            <div>
                                <h2 className="text-xs font-medium text-cyan-200 uppercase tracking-wider">Catalyst Channel Context</h2>
                                <p className="mt-1 text-xs text-gray-400">
                                    Connect a YouTube channel so Catalyst can use recent winners, title patterns, and retention lessons while building the next short.
                                </p>
                            </div>
                            <div className="flex flex-wrap gap-2">
                                <button
                                    type="button"
                                    onClick={() => { void startYouTubeConnect(); }}
                                    disabled={youtubeConnecting}
                                    className="rounded-lg border border-cyan-400/30 bg-cyan-500/10 px-3 py-2 text-xs font-semibold text-cyan-100 transition hover:bg-cyan-500/20 disabled:opacity-60"
                                >
                                    {youtubeConnecting ? 'Opening Google...' : 'Connect YouTube'}
                                </button>
                                <button
                                    type="button"
                                    onClick={() => { void loadYouTubeChannels(false); }}
                                    disabled={youtubeLoading}
                                    className="rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 py-2 text-xs font-semibold text-gray-200 transition hover:bg-white/[0.06] disabled:opacity-60"
                                >
                                    {youtubeLoading ? 'Refreshing...' : 'Refresh Channels'}
                                </button>
                            </div>
                        </div>
                        <select
                            value={youtubeChannelId}
                            onChange={(e) => setYoutubeChannelId(e.target.value)}
                            className="w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white focus:outline-none"
                        >
                            <option value="">No connected channel selected</option>
                            {youtubeChannels.map((channel) => (
                                <option key={channel.channel_id} value={channel.channel_id}>
                                    {channel.title}{channel.channel_handle ? ` (${channel.channel_handle})` : ''}
                                </option>
                            ))}
                        </select>
                        {supportsTrendHunt ? (
                            <label className="flex items-start gap-3 rounded-lg border border-emerald-400/20 bg-emerald-500/5 px-3 py-3 text-sm text-emerald-50">
                                <input
                                    type="checkbox"
                                    checked={trendHuntEnabled}
                                    onChange={(e) => setTrendHuntEnabled(e.target.checked)}
                                    className="mt-1 h-4 w-4 rounded border-white/20 bg-black/30 text-emerald-400 focus:ring-emerald-400/50"
                                />
                                <span>
                                    <span className="block font-semibold text-emerald-100">Trend Hunt</span>
                                    <span className="mt-1 block text-xs text-emerald-100/70">
                                        Use your connected channel plus fresh public YouTube trend signals to bias Skeleton AI toward newer breakout angles instead of recycling stale comparison hooks.
                                    </span>
                                </span>
                            </label>
                        ) : null}
                        {youtubeError ? <p className="text-xs text-red-400">{youtubeError}</p> : null}
                        {selectedYouTubeChannel ? (
                            <div className="rounded-lg border border-cyan-400/20 bg-black/20 p-3 text-xs text-gray-300">
                                <p className="font-semibold text-white">{selectedYouTubeChannel.title}</p>
                                {selectedYouTubeChannel.analytics_snapshot?.channel_summary ? (
                                    <p className="mt-2">{selectedYouTubeChannel.analytics_snapshot.channel_summary}</p>
                                ) : (
                                    <p className="mt-2">Catalyst will use this channel’s saved winners and packaging memory on the next short-generation pass.</p>
                                )}
                                {supportsTrendHunt && trendHuntEnabled ? (
                                    <p className="mt-2 text-[11px] text-emerald-200/80">
                                        Trend Hunt is active for this Skeleton run. Catalyst will bias toward fresher public angle clusters and more breakout-friendly hooks.
                                    </p>
                                ) : null}
                            </div>
                        ) : null}
                    </div>
                )}

                {/* REMIX SCRIPT — pull a transcript from a TikTok/YouTube/IG URL */}
                {creativeMode === 'script_to_short' && (
                    <div className="rounded-xl border border-white/[0.08] bg-gradient-to-br from-violet-500/[0.03] to-cyan-500/[0.03] p-4 space-y-3">
                        <div className="flex items-start justify-between gap-3">
                            <div>
                                <p className="text-sm font-semibold text-white flex items-center gap-2">
                                    <Sparkles className="h-4 w-4 text-cyan-300" />
                                    Remix From URL
                                </p>
                                <p className="mt-1 text-xs text-gray-400">
                                    Paste a TikTok, YouTube, or Instagram Reel URL. We'll pull the captions so you can remix the script in seconds.
                                </p>
                            </div>
                        </div>
                        <div className="flex flex-col gap-2 sm:flex-row">
                            <input
                                type="url"
                                value={remixUrl}
                                onChange={(e) => setRemixUrl(e.target.value)}
                                disabled={remixLoading || loading || scriptLoading}
                                placeholder="https://www.tiktok.com/@creator/video/..."
                                className="flex-1 bg-black/30 border border-white/[0.08] rounded-lg px-3 py-2 text-sm text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-cyan-500/40 disabled:opacity-50"
                                onKeyDown={(e) => { if (e.key === 'Enter' && !remixLoading && remixUrl.trim()) { void handleRemixIngest(); } }}
                            />
                            <button
                                type="button"
                                onClick={() => void handleRemixIngest()}
                                disabled={remixLoading || !remixUrl.trim() || loading || scriptLoading}
                                className="inline-flex items-center justify-center gap-2 rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-cyan-500 disabled:opacity-40 disabled:cursor-not-allowed whitespace-nowrap"
                            >
                                {remixLoading ? <><Loader2 className="h-4 w-4 animate-spin" /> Pulling…</> : <>Pull Transcript <ArrowRight className="h-4 w-4" /></>}
                            </button>
                        </div>
                        {remixError && (
                            <p className="text-xs text-red-300">{remixError}</p>
                        )}
                        {remixSourceTitle && !remixError && (
                            <p className="text-[11px] text-emerald-300">
                                Pulled from <span className="font-semibold">{remixSourceTitle}</span>
                                {remixWarning ? <> — <span className="text-amber-300">{remixWarning}</span></> : null}
                            </p>
                        )}
                    </div>
                )}

                {/* PROMPT */}
                <div>
                    <h2 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-2">
                        {creativeMode === 'script_to_short' ? 'Script' : 'Topic'}
                    </h2>
                    <div className="relative">
                        {creativeMode === 'script_to_short' ? (
                            <textarea
                                value={prompt}
                                onChange={(e) => setPrompt(e.target.value)}
                                disabled={loading || scriptLoading}
                                rows={6}
                                placeholder="Paste your full script here. Studio will turn it into editable scene prompts, then you can tune prompts or regenerate previews scene by scene before rendering."
                                className="w-full bg-white/[0.03] border border-white/[0.08] rounded-lg px-4 py-3 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-cyan-500/50 focus:border-cyan-500/50 transition-all disabled:opacity-50 text-sm resize-y"
                            />
                        ) : (
                            <input
                                type="text"
                                value={prompt}
                                onChange={(e) => setPrompt(e.target.value)}
                                disabled={loading || scriptLoading}
                                placeholder={selectedTemplate === 'skeleton' ? "e.g., Software Engineer vs Doctor salary comparison"
                                    : selectedTemplate === 'daytrading' ? "e.g., The day trading mistake that wipes beginners in the first 15 minutes"
                                    : selectedTemplate === 'dilemma' ? "e.g., Save one stranger's life or stop five crimes you'll never know about"
                                    : selectedTemplate === 'business' ? "e.g., Why most startups fail before product-market fit"
                                    : selectedTemplate === 'finance' ? "e.g., How compound interest turns small savings into wealth"
                                    : selectedTemplate === 'tech' ? "e.g., The AI tool stack every solo founder should know"
                                    : selectedTemplate === 'crypto' ? "e.g., Why token utility matters more than hype in 2026"
                                    : selectedTemplate === 'scary' ? "e.g., The disappearance at Cecil Hotel"
                                    : selectedTemplate === 'history' ? "e.g., The fall of the Roman Empire"
                                    : "Enter your video topic..."}
                                className="w-full bg-white/[0.03] border border-white/[0.08] rounded-lg px-4 py-3 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 focus:border-violet-500/50 transition-all disabled:opacity-50 text-sm"
                                onKeyDown={(e) => e.key === 'Enter' && !loading && !scriptLoading && handleGenerate()}
                            />
                        )}
                    </div>
                </div>
                </>
                )}

                {workspaceStage === 'scenes' && (
                <>
                {supportsArtStyle && (
                    <div>
                        <h2 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-2">Art Style ({storyArtStyleOptions.length} verified looks)</h2>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                            {storyArtStyleOptions.map((style) => (
                                <button
                                    key={style.id}
                                    type="button"
                                    onClick={() => !loading && !scriptLoading && setArtStyle(style.id)}
                                    className={`rounded-lg p-2.5 text-left transition border ${
                                        artStyle === style.id
                                            ? 'border-cyan-400/70 bg-cyan-500/10'
                                            : 'border-white/[0.08] bg-white/[0.02] hover:border-white/20'
                                    }`}
                                >
                                    <p className="text-xs font-semibold text-white">{style.label}</p>
                                    <p className="text-[11px] text-gray-400 mt-1">{style.desc}</p>
                                </button>
                            ))}
                        </div>
                        <p className="text-xs text-gray-500 mt-2">Available in Auto, Creative Control, and Script to Short. Skeleton AI uses its dedicated style system.</p>
                    </div>
                )}

                <div>
                    <div>
                        <h2 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-2">
                            {selectedTemplate === 'skeleton' ? 'Style Lock (Default On)' : 'Style Reference (Optional)'}
                        </h2>
                        <div className="mb-2 grid grid-cols-2 gap-2">
                            <button
                                type="button"
                                onClick={() => !loading && !scriptLoading && setCreativeReferenceLockMode('strict')}
                                className={`rounded-lg px-2.5 py-1.5 text-[11px] font-semibold transition ${
                                    creativeReferenceLockMode === 'strict'
                                        ? 'border border-violet-400/70 bg-violet-500/15 text-violet-200'
                                        : 'border border-white/[0.08] bg-white/[0.02] text-gray-300 hover:border-white/20'
                                }`}
                            >
                                Strict Reference Lock
                            </button>
                            <button
                                type="button"
                                onClick={() => !loading && !scriptLoading && setCreativeReferenceLockMode('inspired')}
                                className={`rounded-lg px-2.5 py-1.5 text-[11px] font-semibold transition ${
                                    creativeReferenceLockMode === 'inspired'
                                        ? 'border border-amber-400/70 bg-amber-500/15 text-amber-200'
                                        : 'border border-white/[0.08] bg-white/[0.02] text-gray-300 hover:border-white/20'
                                }`}
                            >
                                Style Inspired
                            </button>
                        </div>
                        <label className="block rounded-lg border border-dashed border-white/[0.12] hover:border-violet-500/40 bg-white/[0.02] p-3 cursor-pointer transition">
                            <input
                                type="file"
                                accept="image/*"
                                className="hidden"
                                onChange={(e) => {
                                    const f = e.target.files?.[0] || null;
                                    setCreativeReferenceImage(f);
                                    setCreativeReferenceStatus(f ? 'ready' : 'idle');
                                    if (f) setCreativeReferenceAttached(false);
                                }}
                            />
                            <div className="flex items-center justify-between gap-4">
                                <div>
                                    <p className="text-xs text-white font-medium">
                                        {creativeReferenceImage
                                            ? creativeReferenceImage.name
                                            : effectiveReferenceAttached
                                                ? (selectedTemplate === 'skeleton'
                                                    ? 'Default Skeleton style lock active for this project'
                                                    : 'Reference image already attached for this project')
                                                : 'Upload reference style image'}
                                    </p>
                                    <p className="text-[11px] text-gray-500 mt-1">
                                        {selectedTemplate === 'skeleton'
                                            ? `The built-in Skeleton style lock stays active unless you upload your own override. Mode: ${creativeReferenceLockMode === 'strict' ? 'Strict lock for maximum continuity' : 'Inspired lock for more variation'}.`
                                            : `Applied across this short in Auto, Creative Control, or Script to Short. Mode: ${creativeReferenceLockMode === 'strict' ? 'Strict lock for maximum continuity' : 'Inspired lock for more variation'}.`}
                                    </p>
                                </div>
                                <span className="px-2.5 py-1 rounded-md bg-violet-600/20 text-violet-300 text-[11px] font-semibold">
                                    {creativeReferenceImage || effectiveReferenceAttached ? 'Attached' : 'Recommended'}
                                </span>
                            </div>
                        </label>
                    </div>
                </div>

                <div className="rounded-lg border border-white/[0.08] bg-white/[0.02] p-3">
                    <div className="flex items-center justify-between gap-3">
                        <div>
                            <p className="text-xs font-semibold text-white">Output Type</p>
                            <p className="text-[11px] text-gray-500 mt-1">
                                Animated uses Kling/FAL scene motion. Slideshow uses image-based camera motion only.
                            </p>
                        </div>
                        <button
                            onClick={globalToggleAnimationMode}
                            disabled={loading || scriptLoading}
                            className={`px-2.5 py-1 rounded-md text-[11px] font-semibold transition ${
                                effectiveAnimationEnabled ? "bg-emerald-600/80 text-white" : "bg-white/10 text-gray-300 hover:bg-white/15"
                            } disabled:opacity-50`}
                        >
                            {effectiveAnimationEnabled ? "Animation ON" : "Slideshow Mode"}
                        </button>
                    </div>
                    {animationCreditExhausted && (
                        <p className="text-[11px] text-amber-300 mt-2">
                            Catalyst animation credits are not available yet. Click the toggle to buy credits with PayPal or stay in slideshow mode.
                        </p>
                    )}
                </div>

                <div className="rounded-lg border border-white/[0.08] bg-white/[0.02] p-3">
                    <div className="flex items-center justify-between gap-3">
                        <div>
                            <p className="text-xs font-semibold text-white">Cinematic Boost</p>
                            <p className="text-[11px] text-gray-500 mt-1">
                                Premium continuity profile is locked on for launch quality.
                            </p>
                        </div>
                        <button
                            type="button"
                            disabled
                            className="px-2.5 py-1 rounded-md text-[11px] font-semibold transition bg-cyan-600/80 text-white disabled:opacity-100 cursor-default"
                        >
                            Always ON
                        </button>
                    </div>
                </div>

                {creativeMode === 'creative' && templateSupportsVoiceControls && (canUse1080p ? resolution : '720p') === '720p' && (
                    <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4">
                        <div className="flex items-center justify-between gap-3">
                            <div>
                                <p className="text-sm font-semibold text-white">{selectedTemplate === 'daytrading' ? 'Day Trading Animation' : 'AI Stories Animation'}</p>
                                <p className="text-xs text-gray-500 mt-1">
                                    Turn OFF to render with image-based camera motion only (no Kling scene animation).
                                </p>
                            </div>
                            <button
                                onClick={globalToggleAnimationMode}
                                disabled={loading || scriptLoading}
                                className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition ${
                                    effectiveAnimationEnabled ? "bg-emerald-600/80 text-white" : "bg-white/10 text-gray-300 hover:bg-white/15"
                                } disabled:opacity-50`}
                            >
                                {effectiveAnimationEnabled ? "Animation ON" : "Animation OFF"}
                            </button>
                        </div>
                    </div>
                )}
                </>
                )}

                {workspaceStage === 'audio' && (
                <>
                <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4">
                    <div className="space-y-3">
                        <div>
                            <p className="text-sm font-semibold text-white">Output Type</p>
                            <p className="text-xs text-gray-500 mt-1">
                                Switch to animation here if you forgot earlier. Slideshow stays free, animation uses Catalyst render credits.
                            </p>
                        </div>
                        <div className="inline-flex rounded-xl border border-white/[0.08] bg-black/30 p-1">
                            <button
                                type="button"
                                onClick={() => globalSetRenderMode('slideshow')}
                                disabled={loading || scriptLoading}
                                className={`rounded-lg px-3 py-1.5 text-xs font-semibold transition ${
                                    !effectiveAnimationEnabled
                                        ? "bg-white text-black"
                                        : "text-gray-300 hover:bg-white/[0.08]"
                                } disabled:opacity-50`}
                            >
                                Slideshow
                            </button>
                            <button
                                type="button"
                                onClick={() => globalSetRenderMode('animation')}
                                disabled={loading || scriptLoading}
                                className={`rounded-lg px-3 py-1.5 text-xs font-semibold transition ${
                                    effectiveAnimationEnabled
                                        ? "bg-emerald-600/80 text-white"
                                        : "text-cyan-200 hover:bg-cyan-500/15"
                                } disabled:opacity-50`}
                            >
                                Animation
                            </button>
                        </div>
                        <p className="text-[11px] text-gray-400">
                            Current mode: <span className="font-semibold text-white">{effectiveAnimationEnabled ? 'Animation' : 'Slideshow'}</span>
                        </p>
                    </div>
                </div>
                {renderCustomVoiceLibraryCard()}
                {templateSupportsVoiceControls && (
                    <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4 space-y-4">
                        <div className="flex items-center justify-between gap-3">
                            <div>
                                <p className="text-sm font-semibold text-white">Voice + Pacing</p>
                                <p className="text-xs text-gray-500 mt-1">Choose the render voice, tune speed, and set pacing before render.</p>
                            </div>
                            <button
                                onClick={() => { void previewStoryVoice(); }}
                                disabled={!storyVoiceId || storyPreviewLoading || storyVoicesLoading}
                                className="px-3 py-1.5 rounded-lg text-xs font-semibold bg-white/10 text-gray-200 hover:bg-white/15 disabled:opacity-50"
                            >
                                {storyPreviewLoading ? "Previewing..." : "Preview Voice"}
                            </button>
                        </div>
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                            <div>
                                <label className="text-xs text-gray-500 uppercase tracking-wider mb-1 block">Voice</label>
                                <select
                                    value={storyVoiceId}
                                    onChange={(e) => setStoryVoiceId(e.target.value)}
                                    className="w-full bg-black/30 border border-white/[0.08] rounded-lg px-3 py-2 text-sm text-white focus:outline-none"
                                >
                                    {storyVoicesLoading ? (
                                        <option value="">Loading voices...</option>
                                    ) : storyVoices.length > 0 ? (
                                        storyVoices.map((v: any) => (
                                            <option key={String(v.voice_id || v.name || Math.random())} value={String(v.voice_id || "")}>
                                                {String(v.name || v.voice_id || "Voice")}
                                            </option>
                                        ))
                                    ) : (
                                        <option value="">Default voice</option>
                                    )}
                                </select>
                                {storyVoicesWarning ? (
                                    <p className="text-[11px] text-amber-300 mt-1">{storyVoicesWarning}</p>
                                ) : storyVoicesSource === 'fallback' ? (
                                    <p className="text-[11px] text-gray-400 mt-1">Using fallback voice catalog.</p>
                                ) : null}
                                {storyPreviewError ? (
                                    <p className="text-[11px] text-red-300 mt-1">{storyPreviewError}</p>
                                ) : null}
                            </div>
                            <div>
                                <label className="text-xs text-gray-500 uppercase tracking-wider mb-1 block">Voice Speed ({storyVoiceSpeed.toFixed(2)}x)</label>
                                <input
                                    type="range"
                                    min={0.8}
                                    max={1.35}
                                    step={0.05}
                                    value={storyVoiceSpeed}
                                    onChange={(e) => setStoryVoiceSpeed(Number(e.target.value))}
                                    className="w-full accent-cyan-500"
                                />
                            </div>
                            <div>
                                <label className="text-xs text-gray-500 uppercase tracking-wider mb-1 block">Pacing</label>
                                <div className="grid grid-cols-3 gap-1">
                                    {[
                                        { id: 'standard', label: 'Standard' },
                                        { id: 'fast', label: 'Fast' },
                                        { id: 'very_fast', label: 'Very Fast' },
                                    ].map((p) => (
                                        <button
                                            key={p.id}
                                            type="button"
                                            onClick={() => setStoryPacingMode(p.id as 'standard' | 'fast' | 'very_fast')}
                                            className={`px-2 py-1.5 rounded-md text-xs font-semibold transition ${
                                                storyPacingMode === p.id ? 'bg-cyan-600 text-white' : 'bg-white/10 text-gray-300 hover:bg-white/15'
                                            }`}
                                        >
                                            {p.label}
                                        </button>
                                    ))}
                                </div>
                            </div>
                        </div>
                    </div>
                )}
                <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 p-4 text-sm text-amber-100">
                    Finale is where voice, captions, and music get locked. Generate scenes first, then use the scene editor to tune prompts before final render.
                </div>
                </>
                )}

                {/* GENERATE BUTTON */}
                <div className="flex flex-wrap items-center justify-between gap-3 border-t border-white/[0.06] pt-4">
                    <div className="flex gap-2">
                        {workspaceStage !== 'script' && (
                            <button
                                type="button"
                                onClick={() => setWorkspaceStage(workspaceStage === 'audio' ? 'scenes' : 'script')}
                                className="rounded-lg border border-white/[0.08] bg-white/[0.03] px-4 py-2 text-sm text-gray-300 transition hover:bg-white/[0.06]"
                            >
                                Back
                            </button>
                        )}
                        {workspaceStage !== 'audio' && (
                            <button
                                type="button"
                                onClick={() => setWorkspaceStage(workspaceStage === 'script' ? 'scenes' : 'audio')}
                                className="inline-flex items-center gap-2 rounded-lg border border-white/[0.08] bg-white/[0.03] px-4 py-2 text-sm text-gray-200 transition hover:bg-white/[0.06]"
                            >
                                Next
                                <ArrowRight className="h-4 w-4" />
                            </button>
                        )}
                    </div>
                    {workspaceStage !== 'audio' && (
                        <button
                            onClick={handleGenerate}
                            disabled={loading || scriptLoading || !prompt.trim()}
                            className={`py-3 px-5 ${creativeMode === 'creative' ? 'bg-amber-600 hover:bg-amber-500 shadow-amber-600/20' : creativeMode === 'script_to_short' ? 'bg-cyan-600 hover:bg-cyan-500 shadow-cyan-600/20' : 'bg-violet-600 hover:bg-violet-500 shadow-violet-600/20'} disabled:opacity-40 text-white font-bold rounded-lg text-base transition-all flex items-center justify-center gap-2 shadow-lg active:scale-[0.99]`}
                        >
                            {scriptLoading ? (
                                <><Loader2 className="w-5 h-5 animate-spin" /> {creativeReferenceStatus === 'uploading' ? 'Uploading reference style...' : 'Setting up...'}</>
                            ) : loading ? (
                                <><Loader2 className="w-5 h-5 animate-spin" /> Generating your short...</>
                            ) : creativeMode === 'creative' ? (
                                <><Sliders className="w-5 h-5" /> Start Building</>
                            ) : creativeMode === 'script_to_short' ? (
                                <><Clapperboard className="w-5 h-5" /> Build Scene Plan</>
                            ) : (
                                <><Wand2 className="w-5 h-5" /> {effectiveAnimationEnabled ? 'Generate Animated Short' : 'Generate Slideshow Short'} at {canUse1080p ? resolution : '720p'}</>
                            )}
                        </button>
                    )}
                </div>
                {generateError && (
                    <div className="mt-3 rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-300">
                        {generateError}
                    </div>
                )}
                {quickStartCard}
                {templateChooserModal}
                {subscriptionPromptModal}
                {imageModelPickerModal}
                {videoModelPickerModal}
                {animationCreditPromptModal}
                {globalRenderProgressWindow}

                {activePromptEditorScene && scenePromptEditorIndex !== null && (
                    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4">
                        <div className="w-full max-w-3xl rounded-2xl border border-white/[0.08] bg-[#0d0d11] shadow-2xl">
                            <div className="flex items-center justify-between border-b border-white/[0.08] px-5 py-4">
                                <div>
                                    <h3 className="text-base font-semibold text-white">Scene {scenePromptEditorIndex + 1} Prompt Editor</h3>
                                    <p className="mt-1 text-xs text-gray-400">Edit the exact prompt used for this scene, then regenerate from here.</p>
                                </div>
                                <button
                                    type="button"
                                    onClick={() => setScenePromptEditorIndex(null)}
                                    className="rounded-lg p-2 text-gray-400 transition hover:bg-white/[0.05] hover:text-white"
                                    title="Close"
                                >
                                    <X className="h-4 w-4" />
                                </button>
                            </div>
                            <div className="grid gap-5 p-5 md:grid-cols-[300px,1fr]">
                                <div className="rounded-xl bg-black/40 p-2">
                                    {activePromptEditorScene.imageData ? (
                                        <img
                                            src={activePromptEditorScene.imageData}
                                            alt={`Scene ${scenePromptEditorIndex + 1}`}
                                            className="mx-auto max-h-[420px] w-auto max-w-full rounded-lg object-contain"
                                        />
                                    ) : (
                                        <div className="flex h-[320px] items-center justify-center rounded-lg border border-dashed border-white/[0.08] text-sm text-gray-500">
                                            No preview generated yet
                                        </div>
                                    )}
                                </div>
                                <div className="space-y-4">
                                    <div>
                                        <label className="mb-1 block text-xs uppercase tracking-wider text-gray-500">Image Prompt</label>
                                        <textarea
                                            value={activePromptEditorScene.visual_description}
                                            onChange={(e) => handleUpdateScene(scenePromptEditorIndex, 'visual_description', e.target.value)}
                                            rows={7}
                                            placeholder={selectedTemplate === 'daytrading'
                                                ? "Describe the trading/investing visual, e.g. 'A premium 3D trading terminal with glowing order-flow ribbons, aggressive red sell pressure, and a shocked trader in silhouette'"
                                                : undefined}
                                            className="w-full resize-none rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white placeholder:text-gray-600 focus:outline-none focus:ring-1 focus:ring-violet-500/50"
                                        />
                                    </div>
                                    <div>
                                        <label className="mb-1 block text-xs uppercase tracking-wider text-gray-500">Negative Prompt</label>
                                        <textarea
                                            value={activePromptEditorScene.negative_prompt || ""}
                                            onChange={(e) => handleUpdateScene(scenePromptEditorIndex, 'negative_prompt', e.target.value)}
                                            rows={4}
                                            placeholder="Only add what you want to avoid."
                                            className="w-full resize-none rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white placeholder:text-gray-600 focus:outline-none focus:ring-1 focus:ring-violet-500/50"
                                        />
                                    </div>
                                    <div className="flex flex-wrap gap-3">
                                        <button
                                            type="button"
                                            onClick={() => handleGenerateSceneImage(scenePromptEditorIndex)}
                                            disabled={activePromptEditorScene.imageLoading || !activePromptEditorScene.visual_description.trim() || !sessionId || sceneBuildLoading}
                                            className="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-violet-500 disabled:opacity-50"
                                        >
                                            {activePromptEditorScene.imageLoading ? 'Regenerating...' : 'Save & Regenerate'}
                                        </button>
                                        <button
                                            type="button"
                                            onClick={() => setScenePromptEditorIndex(null)}
                                            className="rounded-lg border border-white/[0.08] bg-white/[0.03] px-4 py-2 text-sm text-gray-300 transition hover:bg-white/[0.06]"
                                        >
                                            Done
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                )}

                {/* JOB STATUS (auto mode) */}
                {jobStatus && (
                    <div className={`rounded-2xl border transition-all overflow-hidden ${
                        jobStatus.status === 'complete' ? 'border-emerald-500/30 bg-emerald-500/[0.03]' :
                        jobStatus.status === 'error' ? 'border-red-500/30 bg-red-500/[0.03]' :
                        'border-violet-500/20 bg-violet-500/[0.02]'
                    }`}>
                        {jobStatus.status === 'error' ? (
                            <div className="p-8 text-center">
                                <p className="text-red-400 font-bold text-lg mb-2">Generation Failed</p>
                                <p className="text-gray-500 text-sm">{jobStatus.error}</p>
                                <button onClick={() => { setJobStatus(null); setJobId(null); setLoading(false); }}
                                    className="mt-4 px-6 py-2 bg-white/5 hover:bg-white/10 rounded-lg text-sm transition">
                                    Try Again
                                </button>
                            </div>
                        ) : jobStatus.status === 'complete' ? (
                            <div>
                                <video controls autoPlay
                                    className="w-full max-h-[500px] bg-black"
                                    src={`${GENERATION_API}/api/download/${jobStatus.output_file}`}
                                />
                                <div className="p-6 space-y-4">
                                    <div className="flex items-center justify-between">
                                        <div>
                                            <h3 className="font-bold text-lg text-emerald-400">{jobStatus.metadata?.title}</h3>
                                            <p className="text-gray-500 text-sm">
                                                {jobStatus.resolution && <span className="text-violet-400 mr-2">{jobStatus.resolution}</span>}
                                                {jobStatus.metadata?.tags?.map((t: string) => `#${t}`).join(' ')}
                                            </p>
                                        </div>
                                        <CheckCircle2 className="w-8 h-8 text-emerald-400" />
                                    </div>
                                    <a href={`${GENERATION_API}/api/download/${jobStatus.output_file}`} download
                                        className="flex items-center justify-center gap-2 w-full py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl transition-all">
                                        <Download className="w-5 h-5" />
                                        Download MP4
                                    </a>
                                    {jobStatus.resolution === '720p' && Array.isArray(jobStatus.scene_images) && jobStatus.scene_images.length > 0 && (
                                        <div className="rounded-xl border border-white/[0.08] bg-black/20 p-4 space-y-3">
                                            <p className="text-xs uppercase tracking-wider text-gray-400 font-semibold">
                                                Regenerate Scene Images (720p Training Data)
                                            </p>
                                            <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                                                {jobStatus.scene_images.map((sceneImg: any, idx: number) => {
                                                    const imgUrl = String(sceneImg?.image_url || "");
                                                    const src = imgUrl.startsWith("http") ? imgUrl : `${GENERATION_API}${imgUrl}`;
                                                    const busy = !!regeneratingAutoScenes[idx];
                                                    return (
                                                        <div key={`auto-scene-${idx}`} className="rounded-lg border border-white/[0.08] bg-white/[0.02] p-2 space-y-2">
                                                            <div className="text-[10px] text-gray-500">Scene {idx + 1}</div>
                                                            {imgUrl ? (
                                                                <img src={src} alt={`Auto scene ${idx + 1}`} className="w-full h-28 object-cover rounded bg-black/40" />
                                                            ) : (
                                                                <div className="w-full h-28 rounded bg-black/40 flex items-center justify-center text-[10px] text-gray-600">No image</div>
                                                            )}
                                                            <button
                                                                onClick={() => handleRegenerateAutoScene(idx)}
                                                                disabled={busy}
                                                                className="w-full py-1.5 rounded bg-violet-600 hover:bg-violet-500 disabled:opacity-50 text-[11px] font-semibold text-white"
                                                            >
                                                                {busy ? "Regenerating..." : "Regenerate"}
                                                            </button>
                                                        </div>
                                                    );
                                                })}
                                            </div>
                                        </div>
                                    )}
                                    <button onClick={() => { setJobStatus(null); setJobId(null); }}
                                        className="w-full py-3 bg-white/5 hover:bg-white/10 text-gray-300 font-medium rounded-xl transition-all">
                                        Create Another
                                    </button>
                                    <FeedbackWidget jobId={jobId || ''} template={selectedTemplate} feature="create" language={language} />
                                </div>
                            </div>
                        ) : (
                            <div className="p-8 space-y-4">
                                <ProgressBar progress={jobStatus.progress || 0} status={jobStatus.status} />
                                {jobStatus.queue_position > 0 && jobStatus.status === 'queued' && (
                                    <div className="flex items-center justify-center gap-2 text-sm">
                                        <Clock className="w-4 h-4 text-violet-400" />
                                        <p className="text-gray-400">
                                            Position <span className="text-violet-400 font-bold">{jobStatus.queue_position}</span> of {jobStatus.queue_total} in queue
                                        </p>
                                    </div>
                                )}
                                {jobStatus.current_scene && jobStatus.total_scenes && (
                                    <p className="text-center text-sm text-gray-600">
                                        Rendering scene {jobStatus.current_scene} of {jobStatus.total_scenes}
                                        {jobStatus.resolution && <span className="ml-1 text-violet-400">({jobStatus.resolution})</span>}
                                    </p>
                                )}
                                {jobStatus.status === 'error' && (
                                    <p className="text-center text-sm text-red-400">{jobStatus.error || 'Generation failed'}</p>
                                )}
                                <JobDiagnostics jobStatus={jobStatus} />
                            </div>
                        )}
                    </div>
                )}
                </>
                )}
                    </div>
                    </div>
                </>
                )}
                </div>
            </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════════════
   CLONE PANEL (inside Dashboard)
   ═══════════════════════════════════════════════════════════════════════════ */



