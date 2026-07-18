/**
 * Studio Agent — full-screen chat (Anthropic Claude + Rookcast skills).
 */
import { useCallback, useContext, useEffect, useMemo, useRef, useState, type ClipboardEvent } from 'react';
import {
    ArrowLeft, ArrowUp, BookOpen, Brain, Check, ChevronUp, ChevronsLeft, ChevronsRight, Clapperboard, History, ImageIcon, Loader2,
    MessageSquarePlus, Mic, MicOff, Palette, Paperclip, Play, RefreshCw, RotateCcw, Search, Shield,
    ShieldOff, Sparkles, Trash2, Users, Video, X, Zap,
} from 'lucide-react';
import AgentActivityTimeline, {
    completeRunningSteps,
    newThinkingStep,
    type ActivityChild,
    type ActivityStep,
} from '../components/agent/AgentActivityTimeline';
import AgentConceptCard, { type ConceptPlan } from '../components/agent/AgentConceptCard';
import AgentJobDeliverable, { type SceneReplyPreset } from '../components/agent/AgentJobDeliverable';
import { type ThumbnailReview } from '../components/agent/ThumbnailReviewCard';
import AgentMessageBody from '../components/agent/AgentMessageBody';
import AgentProductionRail from '../components/agent/AgentProductionRail';
import AgentProgressBubble from '../components/agent/AgentProgressBubble';
import AgentRenderDock from '../components/agent/AgentRenderDock';
import AgentYouTubeConnect, { type ChannelRow } from '../components/agent/AgentYouTubeConnect';
import DictationWaveform from '../components/agent/DictationWaveform';
import { useAgentProductionJobs } from '../hooks/useAgentProductionJobs';
import { useAuthenticatedMediaUrl, useAuthenticatedMediaUrls } from '../hooks/useAuthenticatedMedia';
import {
    type AgentJobSnapshot,
    type AgentJobTrack,
    type ProductionProgressUpdate,
    agentJobPollUrl,
    cancelJob,
    collectTracksFromTranscript,
    collectTracksToRefresh,
    isStaleDeadLongformPoll,
    isStaleIdleLongformFailure,
    isStaleLongformChapterFailure,
    isGhostJobPollFailure,
    isBlockedJobId,
    isImplicitProductionCancel,
    isTerminalJob,
    shouldHideJobDeliverable,
    stripStaleProductionArtifacts,
    lastSessionStorageKey,
    loadPersistedJobs,
    mergeJobTracks,
    normalizeAgentJobKind,
    persistJobs,
    pruneOrphanShortformTracks,
    rehydrateJobSnapshots,
    stripGhostJobDeliverables,
} from '../lib/agentProduction';
import {
    streamAgentChat,
    toolActivityLabel,
    toolLabel,
    type AgentChatAttachment,
    type AgentStreamEvent,
    type AgentToolActivitySummary,
} from '../lib/streamAgentChat';
import { useSpeechDictation } from '../hooks/useSpeechDictation';
import { applyPendingStudioBundleReload, ensureStudioFresh } from '../lib/studioClientSync';
import { loadImageModelPref, saveImageModelPref } from '../lib/productionModelPrefs';
import { AuthContext, resolveStudioBackendUrl } from '../shared';
import { loadStudioHubState } from '../lib/studioHubState';
import AgentModelPicker, { type AgentModelOption } from './AgentModelPicker';

type ApprovalMode = 'auto' | 'confirm';

/** Large chats + job reconcile can exceed 60s on Fly; allow retries on timeout and blips. */
const SESSION_LOAD_TIMEOUT_MS = 120_000;
const SESSION_LOAD_RETRIES = 2;
const NETWORK_BLIP_RETRY_MS = 1800;
const THINKING_RECOVER_MS = 90_000;
const isNetworkBlip = (message: string) =>
    /failed to fetch|networkerror|load failed|fetch resource|could not reach the backend/i.test(message);
const SESSION_MESSAGE_TAIL = 120;
const sessionResumePath = (sessionId: string, syncPending: boolean) =>
    `/api/studio-agent/sessions/${sessionId}?sync_pending=${syncPending ? 'true' : 'false'}&message_tail=${SESSION_MESSAGE_TAIL}`;
const sessionSyncPath = (sessionId: string) =>
    `/api/studio-agent/sessions/${sessionId}/sync?message_tail=${SESSION_MESSAGE_TAIL}`;

interface PendingAction {
    id: string;
    tool: string;
    summary?: string;
    arguments?: Record<string, unknown>;
}

interface ChatMessage {
    role: 'user' | 'assistant' | 'system';
    content: string;
    jobDeliverable?: AgentJobSnapshot;
    thumbnailReview?: ThumbnailReview;
    productionUpdate?: ProductionProgressUpdate;
}

type DeliverableSource = 'stream' | 'poll' | 'rehydrate' | 'action';

const REPAIR_STALE_SNAPSHOT_GRACE_MS = 30_000;
const REPAIR_ACTIVE_GUARD_MAX_MS = 10 * 60_000;

function jobIdsMatch(left?: string | null, right?: string | null): boolean {
    const a = String(left || '').trim();
    const b = String(right || '').trim();
    if (!a || !b) return false;
    return a === b || a.slice(0, 8) === b.slice(0, 8);
}

function isRepairableShortformFailure(snap?: AgentJobSnapshot | null): boolean {
    return Boolean(
        snap
        && snap.kind === 'shortform'
        && snap.status === 'failed'
        && snap.scenes?.length,
    );
}

function deliverableDisplayText(messageText: string, snap?: AgentJobSnapshot) {
    if (!snap || snap.kind !== 'cliplab') return messageText;
    const jobType = String(snap.job_type || '').toLowerCase();
    const isIngest = jobType === 'cliplab_ingest' || snap.job_id.startsWith('clipi_');
    const isAnalyze = jobType === 'cliplab_analyze' || snap.job_id.startsWith('clipa_');
    const isRender = jobType === 'cliplab_render' || snap.job_id.startsWith('clipr_');
    if (snap.status === 'running') return 'ClipLab is working - track progress in the dock.';
    if (snap.status === 'failed') return `ClipLab failed: ${snap.error || 'unknown error'}`;
    if (isIngest) {
        return 'ClipLab ingest is ready. The source video is loaded; continue to analyze and pick clip moments.';
    }
    if (isAnalyze) {
        const count = snap.segment_count || snap.segments?.length || 0;
        return `ClipLab analysis is ready. ${count} candidate segment(s) were found; continue to render the strongest 9:16 clips.`;
    }
    if (isRender) {
        const clips = snap.clip_count || snap.clips?.length || 0;
        const packages = snap.upload_package_count || snap.upload_packages?.length || 0;
        return `ClipLab clips are ready. ${clips} clip(s) and ${packages} upload package(s) are available.`;
    }
    return 'ClipLab step is ready. Continue to the next ClipLab step.';
}

type VerificationStepStatus = 'pending' | 'running' | 'done' | 'error' | 'skipped';

interface AgentVerificationStep {
    id: string;
    label: string;
    detail: string;
    status: VerificationStepStatus;
    required: boolean;
    at?: number;
}

interface SessionUiCache {
    messages: ChatMessage[];
    pending: PendingAction[];
    jobTracks: AgentJobTrack[];
    dockDismissed: boolean;
}

const SESSION_UI_CACHE_VERSION = 2;
const MAX_SESSION_UI_CACHE_ENTRIES = 30;
const MAX_SESSION_UI_CACHE_MESSAGES = 160;
const MAX_SESSION_UI_CACHE_MESSAGE_CHARS = 24000;
const DEFAULT_VERIFICATION_STEPS: AgentVerificationStep[] = [
    {
        id: 'request_scope',
        label: 'Understand the request',
        detail: 'Classify whether this needs channel analytics, public YouTube data, production tools, or a direct answer.',
        status: 'pending',
        required: true,
    },
    {
        id: 'source_plan',
        label: 'Decide required data sources',
        detail: 'Choose the exact evidence needed before answering.',
        status: 'pending',
        required: true,
    },
    {
        id: 'tool_evidence',
        label: 'Run required data tools',
        detail: 'Execute the needed Studio/YouTube tools, or mark the exact blocker.',
        status: 'pending',
        required: true,
    },
    {
        id: 'source_integrity',
        label: 'Verify tool results before answer',
        detail: 'Check whether tool results are usable, stale, empty, or errored.',
        status: 'pending',
        required: true,
    },
    {
        id: 'final_audit',
        label: 'Audit final answer before replying',
        detail: 'Block unsupported claims and force the answer to match the tool evidence.',
        status: 'pending',
        required: true,
    },
];

function sessionUiCacheStorageKey(userKey?: string) {
    return `studio_agent_ui_cache_v${SESSION_UI_CACHE_VERSION}:${userKey || 'anon'}`;
}

function trimCachedMessage(msg: ChatMessage): ChatMessage {
    const content = String(msg.content || '');
    return {
        ...msg,
        content: content.length > MAX_SESSION_UI_CACHE_MESSAGE_CHARS
            ? `${content.slice(0, MAX_SESSION_UI_CACHE_MESSAGE_CHARS)}\n\n[local cache truncated]`
            : content,
    };
}

function stripHeavyDeliverableCache(msg: ChatMessage, keepJobIds: Set<string>): ChatMessage {
    const snap = msg.jobDeliverable;
    if (!snap?.job_id || keepJobIds.has(snap.job_id)) return trimCachedMessage(msg);
    return trimCachedMessage({
        ...msg,
        jobDeliverable: {
            job_id: snap.job_id,
            kind: snap.kind,
            status: snap.status,
            title: snap.title,
            progress: snap.progress,
            mp4_url: snap.mp4_url,
            total_scenes: snap.total_scenes,
            current_scene: snap.current_scene,
            still_preview_urls: snap.still_preview_urls,
            scenes: snap.scenes?.map((scene) => ({
                index: scene.index,
                duration_sec: scene.duration_sec,
                still_preview_url: scene.still_preview_url,
                has_clip: scene.has_clip,
                approved_for_video: scene.approved_for_video,
                approved_for_animation: scene.approved_for_animation,
                animate: scene.animate,
            })),
            animation_pending_count: snap.animation_pending_count,
            animation_complete_count: snap.animation_complete_count,
        },
    });
}

function reattachJobDeliverables(
    messages: ChatMessage[],
    deliverables: Map<string, AgentJobSnapshot>,
): ChatMessage[] {
    if (!deliverables.size) return messages;
    const next = messages.map((msg) => ({ ...msg }));
    for (const [jobId, snap] of deliverables) {
        if (shouldHideJobDeliverable(snap, messages)) continue;
        const existingIdx = next.findIndex((msg) => msg.jobDeliverable?.job_id === jobId);
        if (existingIdx >= 0) {
            next[existingIdx] = { ...next[existingIdx], jobDeliverable: snap };
            continue;
        }
        for (let i = next.length - 1; i >= 0; i -= 1) {
            const content = String(next[i].content || '');
            const isExpansionQuestion = /How long do you want the finished short|Before I build the remaining scenes|motion.graphics\/effects, pacing/i.test(content);
            if (next[i].role === 'assistant' && !isExpansionQuestion) {
                next[i] = { ...next[i], jobDeliverable: snap };
                break;
            }
        }
    }
    return next;
}

function sanitizeSessionUiCacheEntry(entry: SessionUiCache): SessionUiCache {
    const trimmed = (entry.messages || []).slice(-MAX_SESSION_UI_CACHE_MESSAGES);
    const keepJobIds = new Set<string>();
    for (let i = trimmed.length - 1; i >= 0; i -= 1) {
        const jobId = trimmed[i]?.jobDeliverable?.job_id;
        if (jobId && !keepJobIds.has(jobId)) keepJobIds.add(jobId);
    }
    return {
        messages: stripGhostJobDeliverables(
            trimmed.map((msg) => stripHeavyDeliverableCache(msg, keepJobIds)),
        ),
        pending: [],
        jobTracks: entry.jobTracks || [],
        dockDismissed: Boolean(entry.dockDismissed),
    };
}

function loadStoredSessionUiCache(userKey?: string): Map<string, SessionUiCache> {
    if (typeof window === 'undefined') return new Map();
    try {
        const raw = localStorage.getItem(sessionUiCacheStorageKey(userKey));
        if (!raw) return new Map();
        const parsed = JSON.parse(raw) as Record<string, SessionUiCache>;
        const entries = Object.entries(parsed || {})
            .filter(([sid, entry]) => sid && entry && Array.isArray(entry.messages))
            .slice(-MAX_SESSION_UI_CACHE_ENTRIES)
            .map(([sid, entry]) => [sid, sanitizeSessionUiCacheEntry(entry)] as const);
        return new Map(entries);
    } catch {
        return new Map();
    }
}

function persistStoredSessionUiCache(userKey: string | undefined, cache: Map<string, SessionUiCache>) {
    if (typeof window === 'undefined') return;
    try {
        const entries = Array.from(cache.entries()).slice(-MAX_SESSION_UI_CACHE_ENTRIES);
        const payload = Object.fromEntries(
            entries.map(([sid, entry]) => [sid, sanitizeSessionUiCacheEntry(entry)]),
        );
        localStorage.setItem(sessionUiCacheStorageKey(userKey), JSON.stringify(payload));
    } catch {
        /* local cache is best-effort only */
    }
}

interface AttachedFile {
    id: string;
    name: string;
    size: number;
    mimeType: string;
    kind: 'image' | 'text' | 'video' | 'binary';
}

interface AttachmentPayload {
    name: string;
    mime_type: string;
    size: number;
    kind: 'image' | 'text' | 'video' | 'binary';
    text?: string;
    data_url?: string;
    server_path?: string;
}

const MAX_AGENT_IMAGE_ATTACHMENTS = 4;
const MAX_AGENT_IMAGE_BYTES = 8 * 1024 * 1024;
const MAX_AGENT_VIDEO_BYTES = 3 * 1024 * 1024 * 1024;

function readFileAsDataUrl(file: File): Promise<string> {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onerror = () => reject(reader.error || new Error('Could not read file'));
        reader.onload = () => resolve(String(reader.result || ''));
        reader.readAsDataURL(file);
    });
}

function normalizeAgentMessage(raw: unknown): ChatMessage | null {
    if (!raw || typeof raw !== 'object') return null;
    const row = raw as Record<string, unknown>;
    const role = row.role === 'user' || row.role === 'assistant' || row.role === 'system' ? row.role : null;
    if (!role) return null;
    const content = row.content;
    if (Array.isArray(content)) {
        const text = content
            .map((part) => {
                if (!part || typeof part !== 'object') return '';
                const p = part as Record<string, unknown>;
                return p.type === 'text' ? String(p.text || '') : '';
            })
            .filter(Boolean)
            .join('\n\n');
        const imageCount = content.filter((part) => (
            Boolean(part && typeof part === 'object' && (part as Record<string, unknown>).type === 'image_url')
        )).length;
        return {
            ...(row as Partial<ChatMessage>),
            role,
            content: `${text}${imageCount ? `\n\n[${imageCount} attached image${imageCount === 1 ? '' : 's'}]` : ''}`.trim(),
        } as ChatMessage;
    }
    return { ...(row as Partial<ChatMessage>), role, content: String(content || '') } as ChatMessage;
}

const FALLBACK_MODELS: AgentModelOption[] = [
    {
        id: 'claude-sonnet-4-6',
        name: 'Claude Sonnet 4.6',
        provider: 'Anthropic',
        recommended: true,
        intelligence: 5,
        speed: 4,
        prompt_price_per_m: 3.0,
        completion_price_per_m: 15.0,
        est_cost_10k_2k: 0.06,
        context_length: 200_000,
        description: 'Default Studio runner for tool use and production planning.',
    },
    {
        id: 'claude-opus-4-8',
        name: 'Claude Opus 4.8',
        provider: 'Anthropic',
        recommended: true,
        intelligence: 5,
        speed: 2,
        prompt_price_per_m: 15.0,
        completion_price_per_m: 75.0,
        est_cost_10k_2k: 0.3,
        context_length: 200_000,
        description: 'Highest-depth Claude runner for complex production sessions.',
    },
    {
        id: 'claude-haiku-4-5-20251001',
        name: 'Claude Haiku 4.5',
        provider: 'Anthropic',
        recommended: true,
        intelligence: 4,
        speed: 5,
        prompt_price_per_m: 1.0,
        completion_price_per_m: 5.0,
        est_cost_10k_2k: 0.02,
        context_length: 200_000,
        description: 'Fast, lower-cost Claude runner for status checks and lightweight tool loops.',
    },
    // xAI Grok chat models (same key as speech dictation) — official docs.x.ai pricing
    {
        id: 'grok-4.5',
        name: 'Grok 4.5',
        provider: 'xAI',
        recommended: true,
        intelligence: 5,
        speed: 4,
        prompt_price_per_m: 2.0,
        completion_price_per_m: 6.0,
        est_cost_10k_2k: 0.032,
        context_length: 500_000,
        description: 'xAI flagship for code, agentic tool calling, and low-hallucination Studio runs.',
    },
    {
        id: 'grok-4.3',
        name: 'Grok 4.3',
        provider: 'xAI',
        recommended: true,
        intelligence: 5,
        speed: 4,
        prompt_price_per_m: 1.25,
        completion_price_per_m: 2.5,
        est_cost_10k_2k: 0.0175,
        context_length: 1_000_000,
        description: 'Strong general-purpose Grok runner with 1M context and solid tool use.',
    },
    {
        id: 'grok-4.20-0309-reasoning',
        name: 'Grok 4.20 Reasoning',
        provider: 'xAI',
        recommended: true,
        intelligence: 5,
        speed: 3,
        prompt_price_per_m: 1.25,
        completion_price_per_m: 2.5,
        est_cost_10k_2k: 0.0175,
        context_length: 1_000_000,
        description: 'Reasoning-optimized Grok 4.20 for deep planning and multi-step tool loops.',
    },
    {
        id: 'grok-4.20-0309-non-reasoning',
        name: 'Grok 4.20 Non-Reasoning',
        provider: 'xAI',
        recommended: true,
        intelligence: 4,
        speed: 5,
        prompt_price_per_m: 1.25,
        completion_price_per_m: 2.5,
        est_cost_10k_2k: 0.0175,
        context_length: 1_000_000,
        description: 'Fast Grok 4.20 without extra reasoning overhead — good for light orchestration.',
    },
    {
        id: 'grok-4.20-multi-agent-0309',
        name: 'Grok 4.20 Multi-Agent',
        provider: 'xAI',
        intelligence: 5,
        speed: 3,
        prompt_price_per_m: 1.25,
        completion_price_per_m: 2.5,
        est_cost_10k_2k: 0.0175,
        context_length: 1_000_000,
        description: 'Multi-agent orchestration SKU for long context and agent loops.',
    },
    {
        id: 'grok-build-0.1',
        name: 'Grok Build 0.1',
        provider: 'xAI',
        selectable: false,
        disabled: true,
        disabled_reason: 'Grok Build 0.1 is not available as a Studio Agent runner.',
        intelligence: 4,
        speed: 4,
        prompt_price_per_m: 1.0,
        completion_price_per_m: 2.0,
        est_cost_10k_2k: 0.014,
        context_length: 256_000,
        description: 'xAI code-focused model. Shown for catalog completeness but unavailable in Studio Agent.',
    },
];

type ContentFormat = 'short' | 'long' | 'both';
type ReasoningDepth = 'fast' | 'balanced' | 'deep';
type CaptionMode = 'word' | 'off';
type AgentMode = 'plan' | 'studio' | 'cliplab';

function AgentModeMenu({
    mode,
    onSelect,
    isAdmin = false,
}: {
    mode: AgentMode;
    onSelect: (mode: AgentMode) => void;
    isAdmin?: boolean;
}) {
    const [open, setOpen] = useState(false);
    const options: Array<{ id: AgentMode; label: string; detail: string; icon: typeof Sparkles; tone: string }> = [
        { id: 'plan', label: 'Plan & conversation', detail: 'Think, research, and refine. No production spend.', icon: BookOpen, tone: 'text-cyan-200' },
        { id: 'studio', label: 'Production', detail: 'Generate, edit, animate, package, and export.', icon: Sparkles, tone: 'text-violet-200' },
        ...(isAdmin ? [{ id: 'cliplab' as const, label: 'ClipLab', detail: 'Turn uploaded long videos into short clips.', icon: Clapperboard, tone: 'text-rose-200' }] : []),
    ];
    const active = options.find((item) => item.id === mode) || options[0];
    const ActiveIcon = active.icon;
    return (
        <div className="relative">
            {open && <button type="button" aria-label="Close mode menu" className="fixed inset-0 z-40" onClick={() => setOpen(false)} />}
            {open && (
                <div className="absolute bottom-full left-0 z-50 mb-2 w-64 overflow-hidden rounded-xl border border-white/10 bg-[#101015] p-1.5 shadow-2xl shadow-black/70">
                    {options.map((item) => {
                        const Icon = item.icon;
                        return (
                            <button
                                key={item.id}
                                type="button"
                                onClick={() => { onSelect(item.id); setOpen(false); }}
                                className={`flex w-full items-start gap-2 rounded-lg px-2.5 py-2 text-left transition hover:bg-white/[0.06] ${mode === item.id ? 'bg-white/[0.05]' : ''}`}
                            >
                                <Icon className={`mt-0.5 h-3.5 w-3.5 shrink-0 ${item.tone}`} />
                                <span className="min-w-0">
                                    <span className="block text-[11px] font-semibold text-white">{item.label}</span>
                                    <span className="block text-[10px] leading-snug text-gray-500">{item.detail}</span>
                                </span>
                                {mode === item.id && <Check className="ml-auto mt-0.5 h-3.5 w-3.5 shrink-0 text-emerald-300" />}
                            </button>
                        );
                    })}
                </div>
            )}
            <button
                type="button"
                onClick={() => setOpen((value) => !value)}
                className={`inline-flex items-center gap-1 rounded-lg border border-white/[0.07] bg-black/20 px-2 py-1 text-[10px] font-semibold transition hover:bg-white/[0.06] ${active.tone}`}
                title="Choose planning, production, or ClipLab mode"
            >
                <ActiveIcon className="h-3 w-3" />
                {mode === 'plan' ? 'Plan' : mode === 'studio' ? 'Production' : 'ClipLab'}
                <ChevronUp className={`h-3 w-3 text-gray-500 transition ${open ? 'rotate-180' : ''}`} />
            </button>
        </div>
    );
}
type CreativeModelProfile = {
    id?: string;
    label?: string;
    provider?: string;
    tier?: string;
    summary?: string;
    speed?: string;
    estimated_unit_usd?: number;
    billing_unit?: string;
    pricing_source?: string;
    pricing_fetched_at?: number;
    pricing_live?: boolean;
    input_image_usd?: number;
    pricing_assumptions?: string;
    enabled?: boolean;
};

const REASONING_OPTIONS: { id: ReasoningDepth; label: string; hint: string }[] = [
    { id: 'fast', label: 'Fast', hint: 'Quick answers, less deliberation' },
    { id: 'balanced', label: 'Balanced', hint: 'Default depth' },
    { id: 'deep', label: 'Deep', hint: 'Thorough analysis before recommendations' },
];

const DEFAULT_IMAGE_MODEL = 'ernie_image';
const DEFAULT_VIDEO_MODEL = 'seedance';

const FALLBACK_IMAGE_MODELS: AgentModelOption[] = [
    { id: 'seedream_edit', name: 'Seedream 4.5 Edit', provider: 'fal', recommended: true, intelligence: 5, speed: 4, estimated_unit_usd: 0.04, billing_unit: 'image', description: 'Canonical reference editing and high-fidelity stills.' },
    { id: 'seedream_v5_lite', name: 'Seedream 5.0 Lite', provider: 'fal', intelligence: 5, speed: 5, estimated_unit_usd: 0.035, billing_unit: 'image', description: 'Fast latest-generation Seedream stills with reference-aware editing.' },
    { id: 'seedream_v4', name: 'Seedream 4.0', provider: 'fal', intelligence: 4, speed: 5, estimated_unit_usd: 0.03, billing_unit: 'image', description: 'Lower-cost Seedream generation and reference editing.' },
    { id: 'grok_imagine', name: 'Grok Imagine Quality', provider: 'xAI', intelligence: 5, speed: 5, estimated_unit_usd: 0.05, billing_unit: '1K image', description: '$0.05 per 1K output; $0.07 at 2K. Premium history still lane.' },
    { id: 'grok_imagine_standard', name: 'Grok Imagine', provider: 'xAI', intelligence: 4, speed: 5, estimated_unit_usd: 0.02, billing_unit: 'image', description: '$0.02 per 1K or 2K output. Lower-cost Grok still lane.' },
    { id: 'ernie_image', name: 'ERNIE-Image', provider: 'fal', intelligence: 4, speed: 5, estimated_unit_usd: 0.03, billing_unit: 'megapixel', description: '$0.03 per megapixel. Cost scales with output resolution.' },
];

const FALLBACK_VIDEO_MODELS: AgentModelOption[] = [
    { id: 'grok_imagine_video', name: 'Grok Imagine Video', provider: 'xAI', recommended: true, intelligence: 4, speed: 5, estimated_unit_usd: 0.07, billing_unit: 'second', input_image_usd: 0.002, pricing_source: 'xai_published', pricing_assumptions: '720p', description: 'Current published 720p Grok I2V rate.' },
    { id: 'grok_imagine_video_15', name: 'Grok Imagine Video 1.5', provider: 'xAI', intelligence: 5, speed: 4, estimated_unit_usd: 0.14, billing_unit: 'second', input_image_usd: 0.01, pricing_source: 'xai_published', pricing_assumptions: '720p', description: 'Current published 720p Grok I2V 1.5 rate.' },
    { id: 'grok_imagine_video_15_1080p', name: 'Grok Imagine Video 1.5 1080p', provider: 'xAI', intelligence: 5, speed: 2, estimated_unit_usd: 0.25, billing_unit: 'second', input_image_usd: 0.01, pricing_source: 'xai_published', pricing_assumptions: '1080p', description: 'Current published 1080p Grok I2V 1.5 rate.' },
    { id: 'seedance', name: 'Seedance 2.0', provider: 'fal', intelligence: 5, speed: 4, estimated_unit_usd: 0.3024, billing_unit: 'second', pricing_source: 'fallback', pricing_assumptions: '720p, standard, no audio', description: 'Premium cinematic motion. Live provider base pricing is converted to Studio effective render cost.' },
    { id: 'pixverse', name: 'PixVerse V6', provider: 'fal', intelligence: 4, speed: 4, estimated_unit_usd: 0.045, billing_unit: 'second', description: '~$0.225 per 5s at 720p. Strong value and moderation fallback.' },
    { id: 'kling_pro', name: 'Kling 2.1 Pro', provider: 'fal', intelligence: 5, speed: 3, estimated_unit_usd: 0.098, billing_unit: 'second', description: '~$0.49 per 5s at 720p. Premium hero-scene motion with model-specific prompt adapter.' },
    { id: 'ltx_budget', name: 'LTX 13B Budget', provider: 'fal', intelligence: 3, speed: 5, estimated_unit_usd: 0.02, billing_unit: 'second', pricing_source: 'fallback', description: 'Lowest-cost full-motion lane. Live provider pricing replaces this fallback when available.' },
];

// These are the model keys with a real Studio Agent adapter: prompt compiler,
// provider request shape, pricing ledger, fallback behavior, and QA. Do not
// surface a catalog-only model that the short-form renderer cannot actually run.
const SUPPORTED_AGENT_IMAGE_MODEL_IDS = new Set(['seedream_edit', 'seedream_v5_lite', 'seedream_v4', 'seedream_v5_lite_modal', 'grok_imagine', 'grok_imagine_standard', 'ernie_image']);
const SUPPORTED_AGENT_VIDEO_MODEL_IDS = new Set(FALLBACK_VIDEO_MODELS.map((item) => item.id));

const VIDEO_MODEL_OPTIONS = FALLBACK_VIDEO_MODELS.map((model) => ({
    id: model.id,
    label: model.name,
    price: String(model.provider || ''),
}));

function normalizeVideoModel(value: unknown): string {
    return String(value || '').trim() || DEFAULT_VIDEO_MODEL;
}

function isPersistedAgentSessionId(id: string | null | undefined): id is string {
    return Boolean(id && id.startsWith('sa_'));
}

function speedStars(speed?: string): number {
    const raw = String(speed || '').toLowerCase();
    if (raw.includes('very')) return 5;
    if (raw.includes('fast')) return 5;
    if (raw.includes('balanced')) return 4;
    if (raw.includes('medium')) return 3;
    if (raw.includes('slow')) return 2;
    return 3;
}

function tierStars(tier?: string): number {
    const raw = String(tier || '').toLowerCase();
    if (raw.includes('elite')) return 5;
    if (raw.includes('premium')) return 4;
    return 3;
}

function creativeModelOption(profile: CreativeModelProfile): AgentModelOption | null {
    const id = String(profile.id || '').trim();
    if (!id || profile.enabled === false) return null;
    const cost = typeof profile.estimated_unit_usd === 'number'
        ? `$${profile.estimated_unit_usd.toFixed(3).replace(/0+$/, '').replace(/\.$/, '')}/${profile.billing_unit || 'unit'}`
        : '';
    return {
        id,
        name: String(profile.label || id),
        provider: String(profile.provider || 'Other').toUpperCase() === 'XAI' ? 'xAI' : String(profile.provider || 'Other'),
        recommended: id.includes('grok') || id === DEFAULT_IMAGE_MODEL || id === 'kling21_standard',
        intelligence: tierStars(profile.tier),
        speed: speedStars(profile.speed),
        estimated_unit_usd: typeof profile.estimated_unit_usd === 'number' ? profile.estimated_unit_usd : undefined,
        billing_unit: String(profile.billing_unit || '').trim() || undefined,
        pricing_source: String(profile.pricing_source || '').trim() || undefined,
        pricing_fetched_at: typeof profile.pricing_fetched_at === 'number' ? profile.pricing_fetched_at : undefined,
        pricing_live: profile.pricing_live === true,
        input_image_usd: typeof profile.input_image_usd === 'number' ? profile.input_image_usd : undefined,
        pricing_assumptions: String(profile.pricing_assumptions || '').trim() || undefined,
        description: [cost, profile.summary].filter(Boolean).join('. '),
    };
}

function mergeCreativeModelOptions(
    fallback: AgentModelOption[],
    providerModels: AgentModelOption[],
): AgentModelOption[] {
    const merged = new Map(fallback.map((model) => [model.id, { ...model }]));
    for (const model of providerModels) {
        merged.set(model.id, { ...(merged.get(model.id) || {}), ...model });
    }
    return Array.from(merged.values());
}

function selectedModelLabel(models: AgentModelOption[], selectedId: string, fallback: string): string {
    return models.find((m) => m.id === selectedId)?.name || selectedId || fallback;
}

interface RenderStyleOption {
    key: string;
    label: string;
    group: string;
    pipeline?: string;
    description?: string;
    preview_url?: string;
    preview_video_url?: string;
    preview_ready?: boolean;
    preview_video_ready?: boolean;
}

const FALLBACK_RENDER_STYLES: RenderStyleOption[] = [
    { key: 'cinematic', label: 'Cinematic', group: 'Realism' },
    { key: 'ultra_realism', label: 'Ultra realism', group: 'Realism' },
    { key: 'historical_18th_century', label: '18th century historical', group: 'Realism' },
    { key: 'comic_book', label: 'Comic book (color)', group: 'Comic' },
    { key: 'bw_comic', label: 'B&W comic', group: 'Comic' },
    { key: 'studio_ghibli', label: 'Studio Ghibli', group: 'Animation' },
    { key: 'pixar', label: 'Pixar', group: 'Animation' },
    { key: 'claymation', label: 'Claymation', group: 'Animation' },
    { key: 'skeleton_host', label: 'Skeleton (NYPTID mascot)', group: 'Niche' },
];

function formatPendingArgs(
    args?: Record<string, unknown>,
    styleCatalog?: RenderStyleOption[],
): string {
    if (!args || !Object.keys(args).length) return '';
    const styleKey = String(args.render_style || '');
    const styleLabel =
        styleCatalog?.find((s) => s.key === styleKey)?.label
        || styleKey.replace(/_/g, ' ');
    const topic = String(args.topic || args.title || args.video_title || '');
    const sceneCount = args.scene_count ?? (args.visual_proof_only ? 1 : null);
    const parts: string[] = [];
    if (styleKey) parts.push(`Art style: ${styleLabel}`);
    if (topic) parts.push(`Video: ${topic}`);
    if (sceneCount === 1 || args.visual_proof_only) parts.push('Mode: one still for approval');
    return parts.join(' · ') || JSON.stringify(args);
}

interface SessionSummary {
    session_id: string;
    title: string;
    updated_at?: number;
    message_count?: number;
    pending_count?: number;
    content_format?: string;
    reasoning_depth?: string;
    image_model?: string;
    video_model?: string;
    caption_mode?: CaptionMode;
    captions_enabled?: boolean;
    channel_id?: string;
    registry_key?: string;
    channel_title?: string;
    active_runs?: { run_id: string; status: string; last_event?: { data?: { message?: string } } | null }[];
}

const STARTER_PROMPTS = [
    "I don't know what to film — audit my channel, rank 5 topics, and pick the best short vs long path.",
    'Reference: paste a MrBeast / Magnates URL — analyze pacing, blueprint scenes, then start a render.',
    'Long-form: outline a 12-min documentary, start render, and walk me through stills → finalize → download.',
    'Skeleton short: outcast + Seedance, teen in black hoodie — script, render, and deliver MP4 in this chat.',
];

const CLIPLAB_STARTER_PROMPT =
    'ClipLab: use my uploaded long video, study my channel + public demand, cut the best 9:16 clips with upload packages.';

function mentionsClipLab(text: string) {
    return /\bcliplab\b/i.test(String(text || '').trim());
}

function looksLikeIdeation(text: string) {
    const low = String(text || '').trim().toLowerCase();
    if (!low) return false;
    if (/\b(?:render|generate|start|animate|finalize|export)\b.+\b(?:short|video|production)\b/i.test(low)) {
        return false;
    }
    if (/\b(?:make|create|generate|render|produce|build)\b.+\b(?:short|short-form|shortform)\b/i.test(low)) {
        return false;
    }
    const ideationSignals = [
        'how would i',
        'how should i',
        'how could i',
        'market research',
        'niche research',
        'channel strategy',
        'youtube channel for',
        'art style',
        'brainstorm',
        'ideation',
        'using this video as reference',
        'using this as reference',
        'content like this',
        'public youtube data',
        'what niche',
        'topic ideas',
        "don't know what to make",
        "dont know what to make",
    ];
    if (ideationSignals.some((phrase) => low.includes(phrase))) return true;
    if (/\?/.test(low) && /\b(?:channel|niche|topic|style|market|research|audience)\b/i.test(low)) {
        return true;
    }
    return false;
}

function formatSessionAge(updatedAt?: number) {
    if (!updatedAt) return '';
    const sec = Math.max(0, Math.floor(Date.now() / 1000 - updatedAt));
    if (sec < 60) return 'just now';
    if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
    if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
    return `${Math.floor(sec / 86400)}d ago`;
}

function displayModelName(models: AgentModelOption[], id: string) {
    return models.find((m) => m.id === id)?.name || id.split('/').pop()?.replace(/-/g, ' ') || id;
}

function activeRunLabel(summary: Pick<SessionSummary, 'active_runs'>): string {
    const run = summary.active_runs?.find((r) => ['queued', 'running', 'stream_disconnected'].includes(r.status));
    return String(run?.last_event?.data?.message || (run ? 'Running' : '')).trim();
}

function channelRegistryKey(channel?: ChannelRow | null): string {
    const raw = String(channel?.registry_key || channel?.channel_handle || channel?.title || channel?.channel_id || '').trim();
    return raw
        .replace(/^@+/, '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');
}

function normalizeChannelLookup(value?: string | null): string {
    return String(value || '')
        .trim()
        .replace(/^@+/, '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');
}

function channelLookupKeys(channel?: ChannelRow | null): Set<string> {
    const keys = [
        channel?.channel_id,
        channel?.registry_key,
        channel?.channel_handle,
        channel?.title,
        channelRegistryKey(channel),
    ]
        .map(normalizeChannelLookup)
        .filter(Boolean);
    return new Set(keys);
}

function channelMatchesSelection(channel: ChannelRow, selectedId: string, fallback?: ChannelRow | null): boolean {
    const selectedKeys = new Set([
        normalizeChannelLookup(selectedId),
        ...Array.from(channelLookupKeys(fallback)),
    ].filter(Boolean));
    if (!selectedKeys.size) return false;
    for (const key of channelLookupKeys(channel)) {
        if (selectedKeys.has(key)) return true;
    }
    return false;
}

function pendingActionLabel(tool: string) {
    const labels: Record<string, string> = {
        start_shortform_generate: 'Start short-form video',
        start_longform_render: 'Start long-form render',
        finalize_longform_render: 'Finalize long-form MP4',
        run_build_script: 'Run build script',
        write_project_file: 'Write project file',
    };
    return labels[tool] || tool.replace(/_/g, ' ');
}

const PENDING_ACTION_ID_RE = /act_[a-f0-9]{12}/gi;
const SINGLETON_PRODUCTION_APPROVAL_TOOLS = new Set([
    'start_shortform_generate',
    'start_longform_render',
]);
const TITLE_STOPWORDS = new Set([
    'the', 'and', 'for', 'with', 'that', 'this', 'when', 'they', 'them', 'you', 'your',
    'into', 'from', 'short', 'video', 'scene', 'test', 'make', 'making', 'going', 'title',
    'lets', 'let', 'will', 'we', 'one', 'exactly',
]);

function normalizeQuoteChars(text: string): string {
    return String(text || '')
        .replace(/\u201c/g, '"')
        .replace(/\u201d/g, '"')
        .replace(/\u2018/g, "'")
        .replace(/\u2019/g, "'");
}

function titleKeywords(value: string): Set<string> {
    return new Set(
        String(value || '')
            .toLowerCase()
            .match(/[a-z0-9]+/g)
            ?.filter((word) => word.length > 2 && !TITLE_STOPWORDS.has(word)) || [],
    );
}

function cleanTitleCandidate(value: string): string {
    return String(value || '')
        .trim()
        .replace(/^[-:,. ]+|[-:,. ]+$/g, '')
        .replace(/^(?:yes\.?|okay\.?|ok\.?|sure\.?|let'?s\s+see|let\s+us\s+see|maybe)\s*,?\s*/i, '')
        .replace(/^(?:make|do|start)\s+/i, '')
        .replace(/^[-:,. ]+|[-:,. ]+$/g, '');
}

function explicitTitleCandidate(text: string): string {
    const value = normalizeQuoteChars(String(text || ''));
    const quoted = Array.from(value.matchAll(/"([^"\n]{8,140})"/g))
        .map((match) => cleanTitleCandidate(match[1] || ''))
        .filter((candidate) => titleKeywords(candidate).size >= 2);
    if (quoted.length) return quoted[quoted.length - 1];

    // "yes make Why Men Suddenly… but only 30 seconds" — match backend extract_user_locked_title
    const hard = value.match(
        /\b(?:yes|yeah|yep|sure|ok(?:ay)?|go ahead)[,.]?\s+(?:make|render|produce)\s+(?!it\b|this\b|that\b)(.+?)(?:\s+but\s+only|\s+only\s+\d|\s*$)/i,
    );
    if (hard?.[1]) {
        const cand = cleanTitleCandidate(hard[1].replace(/\s+but\s+only[\s\S]*$/i, ''));
        if (titleKeywords(cand).size >= 2) return cand;
    }

    const patterns = [
        /(?:one\s+still\s+for|still\s+for|short\s+for|video\s+for|make(?:\s+exactly)?\s+one\s+still\s+for)\s+(.{8,140}?)(?:\s+using|\s+with|\.|$)/gi,
        /(?:title\s+(?:we'?re\s+going\s+to\s+go\s+with|is|it)\s*[:,]?\s*)([^.\n]{8,140})/gi,
        /(?:we\s+will\s+do|we'?ll\s+do|let'?s\s+do|lets\s+do|if we are making|we are making|we're making)\s+([^.\n]{8,140})/gi,
    ];
    for (const pattern of patterns) {
        const matches = Array.from(value.matchAll(pattern))
            .map((match) => cleanTitleCandidate(match[1] || ''))
            .filter((candidate) => titleKeywords(candidate).size >= 2);
        if (matches.length) return matches[matches.length - 1];
    }
    return '';
}

function titleOverlapScore(left: string, right: string): number {
    const leftWords = titleKeywords(left);
    const rightWords = titleKeywords(right);
    if (!leftWords.size || !rightWords.size) return 0;
    let intersection = 0;
    leftWords.forEach((word) => {
        if (rightWords.has(word)) intersection += 1;
    });
    return intersection / Math.max(1, Math.min(leftWords.size, rightWords.size));
}

function canonicalProductionTopic(
    messages: Array<{ role?: string; content?: string }>,
    _actions: PendingAction[],
): string {
    // Only the latest user-requested title counts — never self-justify from pending.
    return explicitTitleCandidate(latestUserText(messages));
}

function latestUserText(messages: Array<{ role?: string; content?: string }>, limit = 4): string {
    return messages
        .filter((m) => m.role === 'user')
        .slice(-limit)
        .map((m) => String(m.content || ''))
        .join('\n');
}

/** Only the newest user turn — used for Approve-card freshness (never poison with prior research). */
function latestSingleUserText(messages: Array<{ role?: string; content?: string }>): string {
    const users = messages.filter((m) => m.role === 'user');
    return String(users[users.length - 1]?.content || '');
}

function isStatusOrStatsQuery(text: string): boolean {
    const compact = String(text || '').trim().toLowerCase().replace(/[?!.]+$/g, '');
    if (['status', 'stats', 'stat', 'statistics', 'channel stats', 'my stats', 'check status', 'any update'].includes(compact)) {
        return true;
    }
    return /\bstats?\b/i.test(compact) && compact.split(/\s+/).length <= 3;
}

function isProductionDiagnosticText(text: string): boolean {
    const value = String(text || '').toLowerCase();
    const startsNewProduction = /\b(?:let'?s|lets)\s+(?:do|make|produce|create|start|generate|render)\b/i.test(value)
        || /\b(?:start|go ahead|do it|render|generate|make|begin)\b.*\b(?:it|this|video|render|production)\b/i.test(value);
    if (startsNewProduction) return false;
    return [
        'wrong short',
        'wrong video',
        'wrong one',
        'previous short',
        'previous video',
        'old short',
        'old video',
        'same video',
        'same short',
        'already made',
        'already been made',
        'why are you',
        'why is it',
        'what is causing',
        "what's causing",
        'causing it',
        'stuck',
        'do i need to start a new chat',
        'need to start a new chat',
        'trying to build',
        'keeps trying',
        'keep getting stuck',
    ].some((term) => value.includes(term));
}

/** Research / demand turns must not keep a production Approve card. */
function isResearchOnlyUserText(text: string): boolean {
    const value = String(text || '').toLowerCase();
    if (!value.trim()) return false;
    // Hard commit escapes research-only filtering.
    if (/\b(?:yes|yeah|yep)\b.+\b(?:make|render|generate|produce)\b/i.test(value)
        || /\brender that plan\b/i.test(value)
        || /\b(?:go ahead and|please)\s+(?:make|render|generate|produce)\b/i.test(value)
        || /\b(?:render|generate|start production)\b.+\bnow\b/i.test(value)
        || /\bapprove and run\b/i.test(value)) {
        return false;
    }
    if (/\b(?:look at|pull|check)\b.+\b(?:data|stats|analytics|post|video)\b/i.test(value)) {
        return true;
    }
    if (/\b(?:compare|balance)\b.+\b(?:short[- ]form|shorts|men|women|audience|niche)\b/i.test(value)) {
        return true;
    }
    if (/\b(?:public (?:youtube )?data|live demand|what(?:'s| is) (?:working|performing|viral|trending)|niche (?:data|performance|demand)|search trends|view counts?|how (?:can|do) we make it better)\b/i.test(value)) {
        return true;
    }
    if (/\b(?:research|analyze|analysis|competitor|channel (?:stats|analytics|data))\b/i.test(value)
        && !/\b(?:render|approve|start production)\b.+\bnow\b/i.test(value)) {
        return true;
    }
    // Soft "let's make … hows that?" is planning, not Approve.
    if (/\b(?:let'?s|lets|maybe|what if|could we|should we)\b.+\b(?:make|create|do)\b.+\b(?:short|video|ad)\b/i.test(value)
        && (/\?/.test(value) || /\b(?:how(?:'s|s)? that|how about|thoughts|instead|maybe|plan|concept)\b/i.test(value))) {
        return true;
    }
    return false;
}

function latestAssistantText(messages: Array<{ role?: string; content?: string }>): string {
    for (let i = messages.length - 1; i >= 0; i -= 1) {
        if (messages[i]?.role === 'assistant') return String(messages[i].content || '');
    }
    return '';
}

function pendingActionTitle(action: PendingAction): string {
    const args = action.arguments || {};
    return String(args.title || args.video_title || args.topic || '').trim();
}

/** Production Approve only valid after a hard commit on the latest user message. */
function latestUserAllowsProductionPending(text: string): boolean {
    const value = String(text || '').toLowerCase();
    if (!value.trim()) return false;
    if (/\b(?:yes|yeah|yep|sure|ok(?:ay)?|do it|go ahead)\b.+\b(?:make|render|generate|produce|start|build)\b/i.test(value)
        || /\brender that plan\b/i.test(value)
        || /\blooks good[,.]?\s*(?:make|render)\b/i.test(value)
        || /\b(?:go ahead and|please)\s+(?:make|render|generate|produce|start)\b/i.test(value)
        || /\b(?:render|generate|start production)\b.+\bnow\b/i.test(value)
        || /\bapprove and run\b/i.test(value)
        || /\bstart_shortform_generate\b/i.test(value)
        || /\bstart_longform_render\b/i.test(value)
        || /\b(?:just\s+)?(?:make|render|start|build)\s+it\b/i.test(value)) {
        if (!/\b(?:how|what if|maybe|better|should we|could we)\b/i.test(value)) {
            return true;
        }
    }
    if (
        /\bmake\s+(?:exactly\s+)?(?:one|1|a|single)\s+(?:still|scene|image|frame)\b/i.test(value)
        || /\b(?:make|render|start|build|produce|generate)\s+(?:the\s+)?(?:very\s+)?(?:first|1st)\s+scene\b/i.test(value)
        || /\b(?:make|render|start|build|produce|generate)\s+scene\s*(?:#?\s*1|one)\b/i.test(value)
        || /\bvisual\s+proof\b/i.test(value)
        || /\bproof\s+(?:still|image)\b/i.test(value)
    ) {
        return true;
    }
    return false;
}

function isResearchAssistantReply(text: string): boolean {
    const value = String(text || '').toLowerCase();
    const hits = ['public data', 'views', 'traction', 'performing', 'demand', 'niche', 'outperform', 'hits ', ' lands at ']
        .filter((term) => value.includes(term)).length;
    return hits >= 2;
}

function isStaleShortformPendingAction(
    action: PendingAction,
    messages: Array<{ role?: string; content?: string }>,
    siblingPending: PendingAction[],
    actionIndex: number,
): boolean {
    if (action.tool !== 'start_shortform_generate' && action.tool !== 'start_longform_render') return false;
    // Critical: use ONLY the latest user turn. Joining prior research turns was
    // marking fresh Approve cards stale and wiping them before click.
    const latestText = latestSingleUserText(messages);
    if (isStatusOrStatsQuery(latestText)) return true;
    if (isProductionDiagnosticText(latestText)) return true;
    // Absolute: no hard production commit on latest user turn → hide Approve.
    if (!latestUserAllowsProductionPending(latestText)) return true;
    if (isResearchOnlyUserText(latestText)) return true;
    const asst = latestAssistantText(messages);
    if (isResearchAssistantReply(asst) && !latestUserAllowsProductionPending(latestText)) return true;
    // Research assistant that proposes one title but pending has a different competitor title.
    if (isResearchAssistantReply(asst)) {
        const proposed = asst.match(/let'?s make\s+[“"']([^”"']{8,140})[”"']/i)?.[1]
            || asst.match(/let'?s make\s+"([^"]{8,140})"/i)?.[1]
            || '';
        const actionTitle = pendingActionTitle(action);
        if (proposed && actionTitle && titleOverlapScore(proposed, actionTitle) < 0.5) {
            return true;
        }
    }
    const actionTitle = pendingActionTitle(action);
    const args = action.arguments || {};
    // One-still proof jobs are stale unless the latest user message asked for a single still.
    if (args.visual_proof_only === true || Number(args.scene_count || 0) === 1) {
        if (!/\b(?:one|1|single|first)\s+(?:still|scene|image|frame)\b|\bvisual\s+proof\b|\bproof\s+(?:still|image)\b/i.test(latestText)) {
            return true;
        }
    }
    // If the assistant just asked for approval, never hide the card on this turn.
    if (/approve when you'?re ready|approval required|prepared production/i.test(asst)) {
        return false;
    }
    if (!actionTitle) return false;
    for (let i = actionIndex + 1; i < siblingPending.length; i += 1) {
        const other = siblingPending[i];
        if (!SINGLETON_PRODUCTION_APPROVAL_TOOLS.has(other.tool)) continue;
        const otherTitle = pendingActionTitle(other);
        if (otherTitle && titleOverlapScore(actionTitle, otherTitle) < 0.34) {
            return true;
        }
    }
    const canonicalTitle = explicitTitleCandidate(latestText);
    // No user-requested title → after hard commit keep the prepared card.
    if (!canonicalTitle) return false;
    return titleOverlapScore(actionTitle, canonicalTitle) < 0.34;
}

function isNewProductionRequest(text: string): boolean {
    const low = String(text || '').toLowerCase();
    // Scene repairs are continuations of the existing production. Without
    // this guard, ordinary phrasing such as "fix scenes 2-6 and make sure the
    // video follows the script" matched the broad `make ... video` fallback,
    // marked the repaired job stale, and hid its persisted scene gallery.
    if (
        /\b(?:audit|fix|repair|correct|redo|regenerate|rerender|re-render|reanimate|re-animate|edit|revise|restage|re-stage)\b[\s\S]{0,220}\b(?:scenes?|stills?|clips?|animations?|shots?)\b/i.test(low)
        || /\b(?:scenes?|stills?|clips?|animations?|shots?)\b[\s\S]{0,220}\b(?:fix|repair|correct|redo|regenerate|rerender|re-render|reanimate|re-animate|edit|revise|restage|re-stage)\b/i.test(low)
    ) {
        return false;
    }
    if (/\b(?:next video|next short|next one|the next short|new video|new short|plan the next|make a new|lets make a|let's make a)\b/.test(low)) {
        return true;
    }
    return /\b(?:make|create|generate|render|produce|build)\b.+\b(?:short|video)\b/.test(low);
}

function priorProductionTitleFromMessages(
    messages: Array<{ role?: string; content?: string; jobDeliverable?: AgentJobSnapshot }>,
): string {
    for (let i = messages.length - 1; i >= 0; i -= 1) {
        const snap = messages[i]?.jobDeliverable;
        const title = String(snap?.title || '').trim();
        if (title) return title;
    }
    for (let i = messages.length - 1; i >= 0; i -= 1) {
        const row = messages[i];
        if (row?.role !== 'user') continue;
        const explicit = explicitTitleCandidate(String(row.content || ''));
        if (explicit) return explicit;
    }
    return '';
}

function userAffirmsAssistantTopic(text: string): boolean {
    const low = String(text || '').toLowerCase();
    return /\b(?:that topic|this topic|do that topic|we(?:'|')?ll do (?:that|this)|i like it|sounds good|go ahead and make|make the (?:very )?first scene|(?:the )?first scene|let'?s do (?:that|this))\b/.test(low);
}

function cleanOutlineTitleCandidate(raw: string): string {
    const s = String(raw || '').replace(/\s+/g, ' ').trim().replace(/^[-:,.?! ]+|[-:,. ]+$/g, '');
    const low = s.toLowerCase();
    if (
        low.includes('hook (')
        || low.includes('main beat')
        || low.includes('twist/close')
        || low.includes('catalyst note')
        || low.includes('0-3 sec')
        || low.includes('25-45 sec')
        || low.includes('skeleton outline')
        || low.includes('concept plan')
        || low.includes('not rendering yet')
        || low.includes('render style')
        || low.includes('seedream')
        || low.includes('grok_imagine')
        || low.includes('skeleton_host')
    ) {
        return '';
    }
    if (/\bscene\s*#?\s*\d+\b/i.test(low)) return '';
    if (/^(?:yes make it|render that|make the first|go ahead and render)\b/i.test(low)) return '';
    if (s.length < 10 || (s.match(/[A-Za-z0-9']+/g) || []).length < 3) return '';
    return s.slice(0, 120);
}

function extractTitleFromAssistantText(text: string): string {
    const body = String(text || '');
    if (!body.trim()) return '';
    const working = body.match(/\*\*Working title:\*\*\s*(.+)/i);
    if (working?.[1]) {
        const cleaned = cleanOutlineTitleCandidate(working[1]);
        if (cleaned) return cleaned;
    }
    const skeleton = body.match(/Skeleton outline for \*\*([^*\n]{10,140})\*\*/i);
    if (skeleton?.[1]) {
        const cleaned = cleanOutlineTitleCandidate(skeleton[1]);
        if (cleaned) return cleaned;
    }
    const hook = body.match(/\*\*Hook:\*\*\s*(.+)/i);
    if (hook?.[1]) {
        const hookLead = String(hook[1]).split(/[.!?]\s+/)[0] || '';
        const cleaned = cleanOutlineTitleCandidate(hookLead) || hookLead.trim().slice(0, 120);
        if (cleaned.length >= 10) return cleaned;
    }
    const bold = Array.from(body.matchAll(/\*\*([^*\n]{10,140})\*\*/g))
        .map((match) => cleanOutlineTitleCandidate(match[1] || ''))
        .filter(Boolean);
    if (bold.length) return bold[bold.length - 1];
    const patterns = [
        /(?:working title|title|topic|outline)[:\s]+["“']?([^"'\n]{10,140})/i,
        /(?:let'?s make|make)\s+["“']([^"'\n]{10,140})/i,
    ];
    for (const pattern of patterns) {
        const match = body.match(pattern);
        if (match?.[1]) {
            const cleaned = cleanOutlineTitleCandidate(match[1]);
            if (cleaned) return cleaned;
        }
    }
    const block = body.match(/Skeleton outline[^\n]*\n+([^\n]{12,160})/i);
    if (block?.[1]) {
        const cleaned = cleanOutlineTitleCandidate(block[1]);
        if (cleaned) return cleaned;
    }
    return '';
}

function extractTitleFromLatestAssistant(
    messages: Array<{ role?: string; content?: string }>,
): string {
    for (let i = messages.length - 1; i >= 0; i -= 1) {
        const row = messages[i];
        if (row?.role !== 'assistant') continue;
        const title = extractTitleFromAssistantText(String(row.content || ''));
        if (title) return title;
    }
    return '';
}

function resolveCurrentProductionTarget(
    messages: Array<{ role?: string; content?: string }>,
    lockedTitle = '',
): string {
    const latestText = latestUserText(messages);
    if (userAffirmsAssistantTopic(latestText) || latestUserAllowsProductionPending(latestText)) {
        const fromOutline = extractTitleFromLatestAssistant(messages);
        if (fromOutline) return fromOutline;
        const prior = priorProductionTitleFromMessages(messages);
        const locked = String(lockedTitle || '').trim();
        if (
            latestUserAllowsProductionPending(latestText)
            && locked
            && prior
            && titleOverlapScore(locked, prior) >= 0.75
        ) {
            return '';
        }
    }
    const explicit = explicitTitleCandidate(latestText) || canonicalProductionTopic(messages, []);
    if (explicit) return explicit;
    return String(lockedTitle || '').trim();
}

function isStaleProductionJob(
    title: string,
    messages: Array<{ role?: string; content?: string; jobDeliverable?: AgentJobSnapshot }>,
    lockedTitle = '',
): boolean {
    const jobTitle = String(title || '').trim();
    if (!jobTitle) return false;
    const latestText = latestUserText(messages);
    if (isProductionDiagnosticText(latestText)) return true;
    if (
        /\b(?:make|render|start|build)\s+(?:the\s+)?(?:very\s+)?(?:first|1st)\s+scene\b/i.test(latestText)
    ) {
        const prior = priorProductionTitleFromMessages(messages);
        if (prior && titleOverlapScore(jobTitle, prior) >= 0.75) {
            const outline = extractTitleFromLatestAssistant(messages);
            if (!outline || titleOverlapScore(jobTitle, outline) < 0.75) return true;
        }
    }
    const target = resolveCurrentProductionTarget(messages, lockedTitle);
    if (!target && latestUserAllowsProductionPending(latestText)) {
        const priorTitle = priorProductionTitleFromMessages(messages);
        if (priorTitle && titleOverlapScore(jobTitle, priorTitle) >= 0.75) return true;
    }
    if (target && titleOverlapScore(jobTitle, target) < 0.34) return true;
    if (
        target
        && (latestUserAllowsProductionPending(latestText) || userAffirmsAssistantTopic(latestText))
        && titleOverlapScore(jobTitle, target) < 0.75
    ) {
        return true;
    }
    const prior = priorProductionTitleFromMessages(messages);
    if (isNewProductionRequest(latestText)) {
        if (prior && titleOverlapScore(jobTitle, prior) >= 0.75) return true;
    }
    const canonical = canonicalProductionTopic(messages, []);
    if (canonical && titleOverlapScore(jobTitle, canonical) < 0.34) return true;
    if (
        prior
        && (latestUserAllowsProductionPending(latestText) || userAffirmsAssistantTopic(latestText))
        && /\b(?:make|render|start|build)\s+(?:the\s+)?(?:very\s+)?(?:first|1st)\s+scene\b/i.test(latestText)
        && titleOverlapScore(jobTitle, prior) >= 0.75
        && (!target || titleOverlapScore(jobTitle, target) < 0.75)
    ) {
        return true;
    }
    if (!latestUserAllowsProductionPending(latestText)) {
        if (prior && titleOverlapScore(jobTitle, prior) >= 0.75) {
            if (
                isResearchOnlyUserText(latestText)
                || /\b(?:plan(?:ning)?|next(?:\s+one)?|similar to|retention|views|analytics|stats|what worked)\b/i.test(latestText)
            ) {
                return true;
            }
        }
    }
    return false;
}

function isStaleAwaitingSnapshot(
    snap: AgentJobSnapshot | undefined,
    messages: Array<{ role?: string; content?: string; jobDeliverable?: AgentJobSnapshot }>,
): boolean {
    if (!snap) return false;
    if (snap.status !== 'awaiting_approval' && snap.status !== 'running') return false;
    return isStaleProductionJob(String(snap.title || ''), messages);
}

function resolveAwaitingDeliverableSnap(
    messages: Array<{ role?: string; content?: string; jobDeliverable?: AgentJobSnapshot }>,
    snapshots: Record<string, AgentJobSnapshot>,
): AgentJobSnapshot | undefined {
    const byId = new Map<string, AgentJobSnapshot>();
    for (const snap of Object.values(snapshots)) {
        if (snap?.status === 'awaiting_approval' && snap.job_id) byId.set(snap.job_id, snap);
    }
    for (let i = messages.length - 1; i >= 0; i -= 1) {
        const snap = messages[i]?.jobDeliverable;
        if (snap?.status === 'awaiting_approval' && snap.job_id && !byId.has(snap.job_id)) {
            byId.set(snap.job_id, snap);
        }
    }
    const fresh = [...byId.values()].filter((snap) => !isStaleAwaitingSnapshot(snap, messages));
    if (!fresh.length) return undefined;
    const latestComplete = [...messages].reverse().find((row) => {
        const snap = row.jobDeliverable;
        return snap?.status === 'complete' && Boolean(snap.mp4_url || snap.download_url);
    })?.jobDeliverable;
    if (latestComplete?.job_id) {
        return fresh.find((snap) => snap.job_id === latestComplete.job_id) || undefined;
    }
    return fresh[0];
}

function pruneStaleJobTracks(
    tracks: AgentJobTrack[],
    messages: Array<{ role?: string; content?: string; jobDeliverable?: AgentJobSnapshot }>,
    blockedJobIds: string[] = [],
): AgentJobTrack[] {
    return tracks.filter((track) => (
        !isBlockedJobId(track.job_id, blockedJobIds)
        && !isStaleProductionJob(String(track.title || ''), messages)
    ));
}

function shouldSuppressProductionJob(
    jobId: string,
    title: string | undefined,
    messages: Array<{ role?: string; content?: string; jobDeliverable?: AgentJobSnapshot }>,
    blockedJobIds: string[] = [],
    snapStatus?: string,
): boolean {
    if (isBlockedJobId(jobId, blockedJobIds)) {
        return true;
    }
    const latestText = latestUserText(messages);
    const sceneOneCommit = /\b(?:make|render|start|build|produce|generate)\s+(?:the\s+)?(?:very\s+)?(?:first|1st)\s+scene\b/i.test(latestText);
    const hardCommit = latestUserAllowsProductionPending(latestText)
        && !/\b(?:expand|rest of|remaining|full short|build the rest|render the rest)\b/i.test(latestText);
    if (
        snapStatus === 'complete'
        && (sceneOneCommit || hardCommit)
    ) {
        return true;
    }
    return isStaleProductionJob(String(title || ''), messages);
}

function collectKnownProductionJobIds(
    messages: Array<{ jobDeliverable?: AgentJobSnapshot }>,
    tracks: AgentJobTrack[],
    deliverables: Iterable<string>,
): string[] {
    const ids = new Set<string>();
    for (const track of tracks) {
        const id = String(track.job_id || '').trim();
        if (id) ids.add(id);
    }
    for (const msg of messages) {
        const id = String(msg.jobDeliverable?.job_id || '').trim();
        if (id) ids.add(id);
    }
    for (const id of deliverables) {
        const trimmed = String(id || '').trim();
        if (trimmed) ids.add(trimmed);
    }
    return [...ids];
}

function keepSingleProductionPending(actions: PendingAction[], messages: Array<{ role?: string; content?: string }>): PendingAction[] {
    const productionIndices = actions
        .map((action, index) => ({ action, index }))
        .filter(({ action }) => SINGLETON_PRODUCTION_APPROVAL_TOOLS.has(action.tool));
    if (productionIndices.length <= 1) return actions;
    const canonicalTitle = canonicalProductionTopic(messages, actions);
    let keepIndex = productionIndices[productionIndices.length - 1].index;
    if (canonicalTitle) {
        keepIndex = productionIndices.reduce((best, row) => (
            titleOverlapScore(pendingActionTitle(row.action), canonicalTitle)
                > titleOverlapScore(pendingActionTitle(actions[best]), canonicalTitle)
                ? row.index
                : best
        ), keepIndex);
    }
    return actions.filter((action, index) => {
        if (!SINGLETON_PRODUCTION_APPROVAL_TOOLS.has(action.tool)) return true;
        return index === keepIndex;
    });
}

const OWNER_ONLY_PENDING_TOOLS = new Set([
    'ingest_cliplab_attachment',
    'analyze_cliplab_video',
    'render_cliplab_segments',
    'remix_cliplab_short',
    'poll_cliplab_job',
]);

function filterOwnerOnlyPending(actions: PendingAction[], isAdmin: boolean): PendingAction[] {
    if (isAdmin) return actions;
    return actions.filter((action) => !OWNER_ONLY_PENDING_TOOLS.has(String(action.tool || '')));
}

function filterStalePendingActions(
    actions: PendingAction[],
    messages: Array<{ role?: string; content?: string }>,
): PendingAction[] {
    const fresh = actions.filter(
        (action, index) => !isStaleShortformPendingAction(action, messages, actions, index),
    );
    return keepSingleProductionPending(fresh, messages);
}

function mergePendingFromTranscript(
    messages: Array<{ role?: string; content?: string; tool_call_id?: string }>,
    serverPending: PendingAction[],
): PendingAction[] {
    const filteredServerPending = filterStalePendingActions(serverPending, messages);
    if (filteredServerPending.length > 0) return filteredServerPending;
    // Do not resurrect approval cards from raw transcript JSON. The backend is
    // the only safe source of runnable approval state because users can change
    // direction after a tool approval was originally prepared.
    return [];
}

function transcriptMentionsPendingAction(messages: ChatMessage[]): boolean {
    const last = [...messages].reverse().find((m) => m.role === 'assistant');
    if (!last) return false;
    return PENDING_ACTION_ID_RE.test(String(last.content || ''));
}

function friendlyApiError(status: number, data: Record<string, unknown>, fallback: string) {
    const rawDetail = data?.detail ?? data?.error ?? fallback;
    const detail = typeof rawDetail === 'string'
        ? rawDetail
        : Array.isArray(rawDetail)
          ? rawDetail
              .map((item: unknown) => {
                  if (typeof item === 'string') return item;
                  if (item && typeof item === 'object' && 'msg' in (item as object)) {
                      return String((item as { msg?: string }).msg || '');
                  }
                  return '';
              })
              .filter(Boolean)
              .join('; ')
          : rawDetail && typeof rawDetail === 'object'
            ? JSON.stringify(rawDetail)
            : String(rawDetail ?? fallback);
    if (status === 401 || status === 403) {
        return detail || 'Sign in required. Studio Agent needs Studio or Studio Pro (owners have unlimited access).';
    }
    if (status === 404) {
        if (detail && !detail.startsWith('Not Found')) {
            return detail;
        }
        return (
            'Studio Agent returned 404. If you were approving an action, refresh the chat and '
            + 'ask the agent to propose the step again (the pending action may have expired). '
            + 'Otherwise redeploy nyptid-studio on Fly and the api-studio Cloudflare worker.'
        );
    }
    if (status === 503) {
        if (/queue/i.test(detail)) {
            return (
                `${detail} — Try Sync chat, then Approve & run. Owner accounts skip the agent queue after deploy; `
                + 'if this persists, Roll over or start a new chat.'
            );
        }
        return detail || 'Studio is at capacity (Claude + fal). Your request is queued — try again shortly.';
    }
    if (status === 524 || status === 504) {
        return (
            detail
            || 'Studio Agent timed out at the edge proxy before Fly responded. Your chat is preserved server-side — press Resume in a few seconds.'
        );
    }
    if (status === 429) {
        if (/queue/i.test(detail)) {
            return (
                `${detail} — Chat runs on the Fly control plane and does not enter the RunPod production queue. Use Sync chat, then retry.`
            );
        }
        return detail || 'Too many requests — wait a moment and retry.';
    }
    return detail;
}

export default function AgentPanel({ onBack }: { onBack?: () => void }) {
    const { session, ownerOverride, studioLaneAccess, supabase } = useContext(AuthContext);
    const isAdminUser = Boolean(ownerOverride);
    const canUseLongform = isAdminUser || Boolean(studioLaneAccess.longform);
    const userCacheKey = String((session as any)?.user?.id || (session as any)?.user?.email || 'anon');
    const [sessionId, setSessionId] = useState<string | null>(null);
    const [model, setModel] = useState(FALLBACK_MODELS[0].id);
    const [modelCatalog, setModelCatalog] = useState<AgentModelOption[]>(FALLBACK_MODELS);
    const [modelPickerOpen, setModelPickerOpen] = useState(false);
    const [imageModel, setImageModel] = useState(() => loadImageModelPref(DEFAULT_IMAGE_MODEL));
    const [imageModelCatalog, setImageModelCatalog] = useState<AgentModelOption[]>(FALLBACK_IMAGE_MODELS);
    const [imageModelPickerOpen, setImageModelPickerOpen] = useState(false);
    const [videoModelCatalog, setVideoModelCatalog] = useState<AgentModelOption[]>(FALLBACK_VIDEO_MODELS);
    const [videoModelPickerOpen, setVideoModelPickerOpen] = useState(false);
    const [approvalMode, setApprovalMode] = useState<ApprovalMode>('confirm');
    const [messages, setMessages] = useState<ChatMessage[]>([]);
    const [pending, setPending] = useState<PendingAction[]>([]);
    const [conceptPlan, setConceptPlan] = useState<ConceptPlan | null>(null);
    const [draftsBySession, setDraftsBySession] = useState<Record<string, string>>({});
    const [attachments, setAttachments] = useState<AttachedFile[]>([]);
    const [attachmentPayload, setAttachmentPayload] = useState<Record<string, AttachmentPayload>>({});
    const [runningBySession, setRunningBySession] = useState<Record<string, string>>({});
    const [resuming, setResuming] = useState(false);
    const [syncing, setSyncing] = useState(false);
    const [toolActivity, setToolActivity] = useState('');
    const [activitySteps, setActivitySteps] = useState<ActivityStep[]>([]);
    const [, setVerificationSteps] = useState<AgentVerificationStep[]>([]);
    const activityStepRef = useRef(0);
    const [booting, setBooting] = useState(true);
    const [creatingSession, setCreatingSession] = useState(false);
    const [history, setHistory] = useState<SessionSummary[]>([]);
    const [historyQuery, setHistoryQuery] = useState('');
    const [historyOpen, setHistoryOpen] = useState(true);

    useEffect(() => {
        const narrowWindow = window.matchMedia('(max-width: 767px)');
        const collapseForNarrowWindow = () => {
            if (narrowWindow.matches) setHistoryOpen(false);
        };
        collapseForNarrowWindow();
        narrowWindow.addEventListener('change', collapseForNarrowWindow);
        return () => narrowWindow.removeEventListener('change', collapseForNarrowWindow);
    }, []);
    const [productWebsite, setProductWebsite] = useState('');
    const [contentFormat, setContentFormat] = useState<ContentFormat>('short');
    const [reasoningDepth, setReasoningDepth] = useState<ReasoningDepth>('balanced');
    const [agentMode, setAgentMode] = useState<AgentMode>('plan');
    const [renderStyle, setRenderStyle] = useState('cinematic');
    const [videoModel, setVideoModel] = useState(DEFAULT_VIDEO_MODEL);
    const [captionMode, setCaptionMode] = useState<CaptionMode>('word');
    const [, setAnimate] = useState(true); // internal for session compat / patch; UI toggle removed per request
    const [showStyleGrid, setShowStyleGrid] = useState(false);
    const [channelsOpen, setChannelsOpen] = useState(false);
    const [youtubeChannels, setYoutubeChannels] = useState<ChannelRow[]>([]);
    const [selectedChannelId, setSelectedChannelId] = useState('');
    const [sessionChannel, setSessionChannel] = useState<ChannelRow | null>(null);
    const [replyingTo, setReplyingTo] = useState<(AgentJobSnapshot & { scene_index?: number }) | null>(null);
    const [renderStyleCatalog, setRenderStyleCatalog] = useState<RenderStyleOption[]>(FALLBACK_RENDER_STYLES);
    const [activeStylePreview, setActiveStylePreview] = useState('');
    const styleStillMedia = useAuthenticatedMediaUrls(
        renderStyleCatalog.map((style) => style.preview_url || ''),
        session?.access_token || '',
        showStyleGrid,
    );
    const activeStyleVideoPath = useMemo(
        () => renderStyleCatalog.find((style) => style.key === activeStylePreview)?.preview_video_url || '',
        [activeStylePreview, renderStyleCatalog],
    );
    const activeStyleVideoMedia = useAuthenticatedMediaUrl(
        activeStyleVideoPath,
        session?.access_token || '',
        showStyleGrid && Boolean(activeStyleVideoPath),
    );
    const styleStillUrlByKey = useMemo(
        () => new Map(renderStyleCatalog.map((style, index) => [style.key, styleStillMedia.urls[index] || ''])),
        [renderStyleCatalog, styleStillMedia.urls],
    );

    // Art Style picker is live session state — retarget waiting Approve cards
    // immediately so the user never approves a stale style.
    useEffect(() => {
        if (!renderStyle) return;
        setPending((prev) => prev.map((action) => {
            if (action.tool !== 'start_longform_render' && action.tool !== 'start_shortform_generate') {
                return action;
            }
            const args = (action.arguments || {}) as Record<string, unknown>;
            if (String(args.render_style || '') === renderStyle) return action;
            return { ...action, arguments: { ...args, render_style: renderStyle } };
        }));
        setConceptPlan((prev) => (
            prev && String(prev.visual_style || '') !== renderStyle
                ? { ...prev, visual_style: renderStyle }
                : prev
        ));
    }, [renderStyle]);

    const [error, setError] = useState('');
    const [queueHint, setQueueHint] = useState('');
    const [dictationPreview, setDictationPreview] = useState('');
    const scrollRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLTextAreaElement>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);
    const messagesRef = useRef<ChatMessage[]>([]);
    const blockedJobIdsRef = useRef<string[]>([]);
    const productionEpochRef = useRef(1);
    const stickToBottomRef = useRef(true);
    const sessionIdRef = useRef<string | null>(null);
    const jobSessionRef = useRef<Map<string, string>>(new Map());
    const deliverablesByJobRef = useRef<Map<string, AgentJobSnapshot>>(new Map());
    const repairSnapshotGuardRef = useRef<Map<string, number>>(new Map());
    const repairingJobIdRef = useRef('');
    const repairActiveRunSeenRef = useRef<Set<string>>(new Set());
    const dismissedDockJobIdsRef = useRef<Set<string>>(new Set());
    const sessionLoadSeqRef = useRef(0);
    const autoSyncTimerRef = useRef<number | null>(null);
    const sessionUiCacheRef = useRef<Map<string, SessionUiCache>>(new Map());
    const [jobTracks, setJobTracks] = useState<AgentJobTrack[]>([]);
    const [dockDismissed, setDockDismissed] = useState(false);
    const [pollResetKey, setPollResetKey] = useState(0);
    const [retryingProduction, setRetryingProduction] = useState(false);
    const [repairingJobId, setRepairingJobId] = useState('');
    const [cancellingProduction, setCancellingProduction] = useState(false);
    const [deleteTarget, setDeleteTarget] = useState<SessionSummary | null>(null);
    const userCancelledJobsRef = useRef<Set<string>>(new Set());
    const currentSessionRunning = Boolean(sessionId && runningBySession[sessionId]);
    const chatSessionReady = isPersistedAgentSessionId(sessionId) && !creatingSession;
    const latestVideoPreviewJobId = useMemo(() => {
        for (let i = messages.length - 1; i >= 0; i -= 1) {
            const jobId = messages[i]?.jobDeliverable?.job_id;
            if (jobId) return jobId;
        }
        return null;
    }, [messages]);
    const visibleChatMessages = useMemo(
        () => messages.filter((m) => m.role === 'user' || m.role === 'assistant'),
        [messages],
    );
    const input = sessionId ? draftsBySession[sessionId] || '' : '';
    const selectedChannel =
        youtubeChannels.find((ch) => channelMatchesSelection(ch, selectedChannelId, sessionChannel))
        || sessionChannel;
    const filteredHistory = history.filter((item) => (
        !historyQuery.trim()
        || String(item.title || '').toLowerCase().includes(historyQuery.trim().toLowerCase())
        || String(item.channel_title || '').toLowerCase().includes(historyQuery.trim().toLowerCase())
    ));
    const hasReadableAttachment = attachments.some((f) => {
        const payload = attachmentPayload[f.id];
        return Boolean(payload && (payload.data_url || payload.text));
    });

    useEffect(() => {
        messagesRef.current = messages;
    }, [messages]);

    const setInput = useCallback((next: string | ((prev: string) => string)) => {
        const sid = sessionIdRef.current;
        if (!sid) return;
        setDraftsBySession((prev) => {
            const old = prev[sid] || '';
            const value = typeof next === 'function' ? next(old) : next;
            return { ...prev, [sid]: value };
        });
    }, []);

    const resetVerificationChecklist = useCallback(() => {
        setVerificationSteps(DEFAULT_VERIFICATION_STEPS.map((step) => ({ ...step })));
    }, []);

    const updateVerificationStep = useCallback((
        id: string,
        patch: Partial<Omit<AgentVerificationStep, 'id'>>,
    ) => {
        setVerificationSteps((rows) => {
            const base = rows.length ? rows : DEFAULT_VERIFICATION_STEPS.map((step) => ({ ...step }));
            let found = false;
            const next = base.map((step) => {
                if (step.id !== id) return step;
                found = true;
                return {
                    ...step,
                    ...patch,
                    status: (patch.status || step.status) as VerificationStepStatus,
                    at: Date.now(),
                };
            });
            if (found) return next;
            return [
                ...next,
                {
                    id,
                    label: patch.label || id.replace(/_/g, ' '),
                    detail: patch.detail || '',
                    status: (patch.status || 'running') as VerificationStepStatus,
                    required: patch.required ?? true,
                    at: Date.now(),
                },
            ];
        });
    }, []);

    const markSessionRunning = useCallback((sid: string, label = 'Thinking...') => {
        setRunningBySession((prev) => ({ ...prev, [sid]: label }));
    }, []);

    const clearSessionRunning = useCallback((sid: string) => {
        setRunningBySession((prev) => {
            if (!prev[sid]) return prev;
            const next = { ...prev };
            delete next[sid];
            return next;
        });
    }, []);

    const isImplicitCancelFailure = useCallback((snap: AgentJobSnapshot) => (
        isImplicitProductionCancel(snap)
        && !userCancelledJobsRef.current.has(snap.job_id)
    ), []);

    const dropGhostShortformTrack = useCallback((jobId?: string) => {
        if (jobId) {
            deliverablesByJobRef.current.delete(jobId);
        } else {
            for (const [id, snap] of [...deliverablesByJobRef.current.entries()]) {
                if (isGhostJobPollFailure(snap)) {
                    deliverablesByJobRef.current.delete(id);
                }
            }
        }
        setJobTracks((prev) => {
            const next = jobId
                ? prev.filter((j) => j.job_id !== jobId)
                : prev.filter((j) => j.kind !== 'shortform');
            const sid = sessionIdRef.current;
            if (sid) persistJobs(sid, next);
            return next;
        });
        setMessages((rows) => {
            if (jobId) {
                return rows.map((row): ChatMessage => (
                    row.jobDeliverable?.job_id === jobId
                        ? { role: row.role, content: row.content, productionUpdate: row.productionUpdate }
                        : row
                ));
            }
            return stripGhostJobDeliverables(rows);
        });
        setError((prev) => (
            /Expecting value: line 1 column 1/i.test(prev)
            || /Production result file was empty or invalid/i.test(prev)
                ? ''
                : prev
        ));
    }, []);

    useEffect(() => {
        const hasStale = messages.some((msg) => shouldHideJobDeliverable(msg.jobDeliverable, messages));
        if (!hasStale) return;
        setMessages((rows) => stripGhostJobDeliverables(rows));
        dropGhostShortformTrack();
    }, [messages, dropGhostShortformTrack]);

    useEffect(() => {
        sessionIdRef.current = sessionId;
    }, [sessionId]);

    useEffect(() => {
        const token = session?.access_token;
        if (!token) return;
        let cancelled = false;
        loadStudioHubState(token)
            .then((state) => {
                if (!cancelled) setProductWebsite(String(state.profile?.website || '').trim());
            })
            .catch(() => {});
        return () => {
            cancelled = true;
        };
    }, [session?.access_token]);

    useEffect(() => {
        const stored = loadStoredSessionUiCache(userCacheKey);
        for (const [sid, entry] of sessionUiCacheRef.current.entries()) {
            stored.set(sid, entry);
        }
        sessionUiCacheRef.current = stored;
    }, [userCacheKey]);

    useEffect(() => {
        if (!sessionId) return;
        sessionUiCacheRef.current.set(sessionId, {
            messages,
            pending,
            jobTracks,
            dockDismissed,
        });
        persistStoredSessionUiCache(userCacheKey, sessionUiCacheRef.current);
    }, [dockDismissed, jobTracks, messages, pending, sessionId, userCacheKey]);

    useEffect(() => {
        setPending((current) => {
            const filtered = filterStalePendingActions(current, messages);
            if (
                filtered.length === current.length
                && filtered.every((action, index) => action.id === current[index]?.id)
            ) {
                return current;
            }
            return filtered;
        });
    }, [messages]);

    // First-time Studio Agent greeting (only once per user, only for non-owners, and only if no channels connected).
    // This runs on initial mount of the agent experience.
    useEffect(() => {
        const userEmail = (session as any)?.user?.email || '';
        const isOwnerUser = ownerOverride || userEmail.toLowerCase().includes('omatic') || userEmail.toLowerCase().includes('hauntinghd');
        if (isOwnerUser) return;

        const seenKey = `studio_agent_first_greeting_seen_${(session as any)?.user?.id || userEmail || 'anon'}`;
        const alreadySeen = typeof window !== 'undefined' && localStorage.getItem(seenKey) === 'true';
        if (alreadySeen) return;

        // Only trigger if we believe they have no channels connected yet.
        // We do a lightweight check via the connect component status or just show — the message itself invites connecting.
        // To avoid spamming every new chat, we mark as seen immediately.
        const hasAnyHistory = history.length > 0 || messages.length > 0;
        if (hasAnyHistory) {
            // They've used it before in this session load — don't greet.
            return;
        }

        // Inject the one-time greeting as the very first assistant message.
        setMessages((prev) => {
            if (prev.length > 0) return prev;
            return [
                {
                    role: 'assistant' as const,
                    content: "I notice you don't have any of your YouTube channels connected. Would you like to connect to YouTube channels or brainstorm video ideas?",
                },
            ];
        });

        if (typeof window !== 'undefined') {
            localStorage.setItem(seenKey, 'true');
        }
    }, [session, ownerOverride, history.length, messages.length]);

    const getToken = useCallback(async () => {
        // Supabase access_token JWTs expire (~1h). UI can still show you signed in via
        // user profile while the JWT is stale — mic/API must refresh first.
        try {
            if (supabase?.auth) {
                const { data, error } = await supabase.auth.getSession();
                if (!error) {
                    let next = data.session;
                    const expiresAt = Number(next?.expires_at || 0);
                    const nowSec = Math.floor(Date.now() / 1000);
                    // Refresh when missing, expired, or expiring within 2 minutes.
                    if (!next?.access_token || (expiresAt > 0 && expiresAt <= nowSec + 120)) {
                        const refreshed = await supabase.auth.refreshSession();
                        if (!refreshed.error && refreshed.data.session?.access_token) {
                            next = refreshed.data.session;
                        }
                    }
                    const fresh = String(next?.access_token || '').trim();
                    if (fresh) return fresh;
                }
            }
        } catch {
            /* fall through to context snapshot */
        }
        const tok = String(session?.access_token || '').trim();
        if (!tok) {
            throw new Error('Sign in required. Refresh Studio, then try again.');
        }
        return tok;
    }, [session?.access_token, supabase]);

    const ingestActiveJobs = useCallback(
        (raw: unknown, sid: string | null) => {
            const list = Array.isArray(raw) ? (raw as AgentJobTrack[]) : [];
            const normalized = list
                .map((j) => ({
                    job_id: String(j.job_id || ''),
                    kind: (j.kind || 'shortform') as AgentJobTrack['kind'],
                    title: String(j.title || ''),
                    started_at: Number(j.started_at || Date.now()),
                }))
                .filter((j) => j.job_id)
                .filter((j) => !shouldSuppressProductionJob(j.job_id, j.title, messagesRef.current, blockedJobIdsRef.current));
            if (!normalized.length) {
                if (sid) {
                    setJobTracks((prev) => {
                        const pruned = pruneStaleJobTracks(prev, messagesRef.current, blockedJobIdsRef.current);
                        persistJobs(sid, pruned);
                        return pruned;
                    });
                }
                return;
            }
            const hasReferenceJob = normalized.some((j) => j.kind === 'competitor');
            if (sid) {
                for (const job of normalized) {
                    jobSessionRef.current.set(job.job_id, sid);
                }
            }
            setJobTracks((prev) => {
                const pruned = hasReferenceJob
                    ? prev.filter((j) => j.kind !== 'shortform')
                    : prev;
                const merged = mergeJobTracks(pruned, normalized);
                if (sid) persistJobs(sid, merged);
                return merged;
            });
            if (hasReferenceJob) {
                setMessages((rows) => stripGhostJobDeliverables(rows));
            }
            setDockDismissed(false);
        },
        [],
    );

    const mergeThumbnailReviewIntoDeliverable = useCallback((review: ThumbnailReview) => {
        if (!review.review_id || !(review.candidate_urls || []).length) return;
        const urls = review.candidate_urls || [];
        setMessages((rows) => {
            const cleaned = rows.filter((row) => row.thumbnailReview?.review_id !== review.review_id);
            const findTarget = (predicate: (snap: AgentJobSnapshot) => boolean) => (
                [...cleaned].map((row, index) => ({ row, index }))
                    .reverse()
                    .find(({ row }) => row.jobDeliverable && predicate(row.jobDeliverable))
                    ?.index ?? -1
            );
            let targetIdx = -1;
            if (review.job_id) {
                targetIdx = findTarget((snap) => (
                    snap.job_id === review.job_id
                    || snap.job_id.slice(0, 8) === review.job_id!.slice(0, 8)
                ));
            }
            if (targetIdx < 0) {
                targetIdx = findTarget((snap) => (
                    snap.status === 'complete'
                    && Boolean(snap.mp4_url || snap.download_url)
                ));
            }
            if (targetIdx < 0) {
                targetIdx = findTarget((snap) => snap.kind === 'longform' || snap.kind === 'shortform');
            }
            if (targetIdx < 0) return cleaned;
            const copy = [...cleaned];
            const existing = copy[targetIdx].jobDeliverable!;
            copy[targetIdx] = {
                ...copy[targetIdx],
                jobDeliverable: {
                    ...existing,
                    thumbnail_urls: urls,
                    title: existing.title || review.title,
                },
            };
            deliverablesByJobRef.current.set(existing.job_id, copy[targetIdx].jobDeliverable!);
            return copy;
        });
        stickToBottomRef.current = true;
    }, []);

    const absorbThumbnailSnapshot = useCallback((snap: AgentJobSnapshot): boolean => {
        const urls = snap.thumbnail_urls || [];
        if (!snap.thumbnail_only || !urls.length) return false;
        mergeThumbnailReviewIntoDeliverable({
            review_id: snap.job_id,
            job_id: snap.job_id,
            title: snap.title,
            candidate_urls: urls,
        });
        deliverablesByJobRef.current.delete(snap.job_id);
        setJobTracks((prev) => {
            const next = prev.filter((job) => job.job_id !== snap.job_id);
            const sid = sessionIdRef.current;
            if (sid) persistJobs(sid, next);
            return next;
        });
        return true;
    }, [mergeThumbnailReviewIntoDeliverable]);

    const appendJobDeliverable = useCallback((
        snap: AgentJobSnapshot,
        options?: { source?: DeliverableSource; pinToLatest?: boolean },
    ) => {
        const source = options?.source || 'action';
        const guardedUntil = repairSnapshotGuardRef.current.get(snap.job_id) || 0;
        if (
            (source === 'poll' || source === 'rehydrate')
            && snap.status === 'failed'
            && guardedUntil > Date.now()
        ) {
            return;
        }
        if (absorbThumbnailSnapshot(snap)) return;
        if (shouldSuppressProductionJob(snap.job_id, snap.title, messagesRef.current, blockedJobIdsRef.current)) {
            dropGhostShortformTrack(snap.job_id);
            return;
        }
        const ownerSession = jobSessionRef.current.get(snap.job_id);
        if (ownerSession && ownerSession !== sessionIdRef.current) return;
        if (isGhostJobPollFailure(snap)) {
            dropGhostShortformTrack(snap.job_id);
            return;
        }
        if (shouldHideJobDeliverable(snap, messagesRef.current)) {
            return;
        }
        if (shouldSuppressProductionJob(
            snap.job_id,
            snap.title,
            messagesRef.current,
            blockedJobIdsRef.current,
        )) {
            return;
        }
        if (
            (snap.status === 'awaiting_approval' || snap.status === 'running' || snap.status === 'complete')
            && snap.kind === 'longform'
        ) {
            setMessages((rows) => rows.map((row) => {
                const other = row.jobDeliverable;
                if (!other || other.job_id === snap.job_id) return row;
                if (other.kind !== 'longform' || other.status !== 'failed') return row;
                const { jobDeliverable: _drop, ...rest } = row;
                return rest;
            }));
            if (snap.status === 'awaiting_approval') setError('');
        }
        if (isImplicitCancelFailure(snap)) return;
        if (snap.kind === 'competitor' && snap.status === 'complete') {
            for (const [id, cached] of [...deliverablesByJobRef.current.entries()]) {
                if (cached.status === 'failed' && (cached.kind === 'competitor' || cached.kind === 'shortform' || isGhostJobPollFailure(cached))) {
                    deliverablesByJobRef.current.delete(id);
                }
            }
            setMessages((rows) => stripGhostJobDeliverables(rows));
        }
        const referenceFacts = (() => {
            if (snap.kind !== 'competitor' || snap.status !== 'complete') return '';
            const facts: string[] = [];
            const visual = String(snap.visual_summary || '').trim();
            if (visual) facts.push(`observed look: ${visual.slice(0, 220)}`);
            if (snap.pacing?.avg_shot_sec != null) facts.push(`avg shot ${snap.pacing.avg_shot_sec}s`);
            if (snap.pacing?.cut_count != null) facts.push(`${snap.pacing.cut_count} cuts`);
            if (snap.pacing?.duration_sec != null) facts.push(`${snap.pacing.duration_sec}s duration`);
            if (snap.frame_count != null) facts.push(`${snap.frame_count} keyframes`);
            const warnings = snap.pacing_warnings || [];
            if (warnings.length) facts.push(String(warnings[0]).slice(0, 180));
            const evidence = facts.length ? ` Extracted: ${facts.join('; ')}.` : '';
            return `${evidence} Conclusion: use the observed visual look plus pacing metrics as reference evidence — do not infer art style from the session Art Style picker.`;
        })();
        const label =
            snap.kind === 'competitor'
                ? snap.status === 'complete'
                    ? `Reference analysis finished — format-specific pacing and blueprint signals are ready.${referenceFacts}`
                    : snap.status === 'failed'
                      ? `Reference analysis failed: ${snap.error || 'the analysis workspace could not be read.'}`
                      : `Reference analysis in progress — ${String(snap.stage_label || snap.stage || 'working').replace(/_/g, ' ')} (${Math.max(0, Number(snap.progress || 0))}%).`
                : snap.status === 'failed'
                  ? snap.error
                      ? `Production failed: ${snap.error}`
                      : 'Production failed. Ask the agent to retry or adjust the brief.'
                  : snap.status === 'running'
                    ? 'Production is in progress — track progress in the render dock.'
                  : snap.status === 'awaiting_approval'
                    ? approvalMode === 'auto'
                      ? 'Stills are ready — auto-finalize is exporting voice, sound, and MP4.'
                      : snap.kind === 'longform'
                        ? 'Your long-form stills are ready. Review the grid, then tap Finalize & export MP4.'
                        : Number(snap.animation_complete_count || 0) > 0
                          ? 'Animation is ready in the card below — review the clip, then export MP4.'
                          : Number(snap.animation_pending_count || 0) > 0
                            ? 'Scenes approved — animation is queued. The clip will appear in this card when ready.'
                            : 'Your short-form scenes are ready. Review the grid, then approve animation or export MP4.'
                    : snap.status === 'running' && /animat/i.test(String(snap.stage || snap.stage_label || ''))
                      ? 'Animating approved scenes — the clip will appear in this card when ready.'
                    : 'Your video is ready.';
        deliverablesByJobRef.current.set(snap.job_id, snap);
        setMessages((m) => {
            const nextRow: ChatMessage = {
                role: 'assistant' as const,
                content: deliverableDisplayText(label, snap),
                jobDeliverable: snap,
            };
            let replaced = false;
            const withoutProgress = m.filter((row) => row.productionUpdate?.job_id !== snap.job_id);
            const jobMatches = (existing?: AgentJobSnapshot) => {
                if (!existing?.job_id || !snap.job_id) return false;
                if (existing.job_id === snap.job_id) return true;
                return existing.job_id.slice(0, 8) === snap.job_id.slice(0, 8);
            };
            const pinReviewCard = Boolean(options?.pinToLatest) || (
                snap.kind === 'shortform'
                && snap.status === 'awaiting_approval'
            );
            // For in-progress competitor analysis, update the latest running bubble
            // in place so we don't stack "still running" clones every poll.
            // Shortform review cards are re-pinned to the end so Scene 1 stays
            // visible after later assistant text (audit / expand prompts).
            const next = withoutProgress.map((row) => {
                if (!jobMatches(row.jobDeliverable)) return row;
                replaced = true;
                if (pinReviewCard) {
                    const { jobDeliverable: _drop, ...rest } = row;
                    return rest as ChatMessage;
                }
                return nextRow;
            });
            if (replaced) {
                if (pinReviewCard) return [...next, nextRow];
                return next;
            }
            if (
                snap.kind === 'competitor'
                && snap.status === 'running'
            ) {
                const runningIdx = [...next]
                    .map((row, i) => ({ row, i }))
                    .reverse()
                    .find(({ row }) =>
                        row.jobDeliverable?.kind === 'competitor'
                        && row.jobDeliverable?.status === 'running',
                    )?.i;
                if (typeof runningIdx === 'number') {
                    const copy = [...next];
                    copy[runningIdx] = nextRow;
                    return copy;
                }
            }
            return [...next, nextRow];
        });
        setJobTracks((prev) => {
            const sid = sessionIdRef.current;
            // Shortform review stays pollable so i2v clips land in the chat card.
            const keepShortformReview = (
                snap.kind === 'shortform'
                && snap.status === 'awaiting_approval'
            );
            if (isTerminalJob(snap) && !keepShortformReview) {
                let next = prev.filter((j) => j.job_id !== snap.job_id);
                if (snap.kind === 'competitor' && snap.status === 'complete') {
                    next = pruneOrphanShortformTracks(next, []);
                }
                if (sid) persistJobs(sid, next);
                return next;
            }
            const merged = mergeJobTracks(prev, [{
                job_id: snap.job_id,
                kind: normalizeAgentJobKind(snap.job_id, snap.kind, snap.title),
                title: snap.title,
                started_at: Date.now(),
            }]);
            if (sid) persistJobs(sid, merged);
            return merged;
        });
        stickToBottomRef.current = true;
    }, [absorbThumbnailSnapshot, approvalMode, dropGhostShortformTrack, isImplicitCancelFailure]);

    const upsertProgressLine = useCallback((update: ProductionProgressUpdate) => {
        const ownerSession = jobSessionRef.current.get(update.job_id);
        if (ownerSession && ownerSession !== sessionIdRef.current) return;
        if (
            isImplicitProductionCancel({
                status: 'failed',
                stage_label: update.stage_label,
                stage: update.stage_label,
                error: '',
            })
            && !userCancelledJobsRef.current.has(update.job_id)
        ) {
            setMessages((rows) => rows.filter((row) => row.productionUpdate?.job_id !== update.job_id));
            return;
        }
        if (shouldSuppressProductionJob(update.job_id, update.title, messagesRef.current, blockedJobIdsRef.current)) {
            setMessages((rows) => rows.filter((row) => row.productionUpdate?.job_id !== update.job_id));
            return;
        }
        setMessages((m) => {
            const idx = m.findIndex((row) => row.productionUpdate?.job_id === update.job_id);
            const row: ChatMessage = {
                role: 'assistant',
                content: '',
                productionUpdate: update,
            };
            if (idx >= 0) {
                const next = [...m];
                next[idx] = row;
                return next;
            }
            return [...m, row];
        });
        if (stickToBottomRef.current) {
            requestAnimationFrame(() => {
                scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' });
            });
        }
    }, []);

    const handleFinalizeStarted = useCallback(
        (jobId: string, activeJobs?: unknown) => {
            setDockDismissed(false);
            if (activeJobs) ingestActiveJobs(activeJobs, sessionId);
            else {
                setJobTracks((prev) => {
                    const merged = mergeJobTracks(prev, [
                        {
                            job_id: jobId,
                            kind: 'longform',
                            title: 'Finalizing long-form',
                            started_at: Date.now(),
                        },
                    ]);
                    if (sessionId) persistJobs(sessionId, merged);
                    return merged;
                });
            }
        },
        [ingestActiveJobs, sessionId],
    );

    const handleReplyToJob = useCallback(
        (snapshot: AgentJobSnapshot, sceneIndex?: number, preset: SceneReplyPreset = 'edit') => {
            setReplyingTo(typeof sceneIndex === 'number' ? { ...snapshot, scene_index: sceneIndex } : snapshot);
            const kindLabel = snapshot.kind === 'longform' ? 'long-form' : 'short-form';
            const suggested = typeof sceneIndex === 'number'
                ? preset === 'regenerate'
                    ? `Regenerate scene ${sceneIndex + 1} in this ${kindLabel} video from scratch. Keep the same canonical character identity and channel style, but rebuild the still so it has no artifacts and matches what I describe.`
                    : `Please edit scene ${sceneIndex + 1} in this ${kindLabel} video. Keep the same character identity, then change only what I describe.`
                : `Please re-edit this ${kindLabel} video and make sure it has proper editing, pacing, storytelling, and packaging + a CTA at the end to get people to subscribe.`;
            setInput(suggested);
            // focus the input so user can edit the instruction or just hit Enter
            setTimeout(() => {
                inputRef.current?.focus();
                // move cursor to end
                const el = inputRef.current;
                if (el) {
                    const len = el.value.length;
                    el.setSelectionRange(len, len);
                }
            }, 0);
        },
        [],
    );

    const shouldAcceptPolledSnapshot = useCallback((snap: AgentJobSnapshot) => {
        const guardedUntil = repairSnapshotGuardRef.current.get(snap.job_id) || 0;
        return !(
            snap.status === 'failed'
            && guardedUntil > Date.now()
        );
    }, []);

    const {
        snapshots,
        primary: dockTrack,
        primarySnap: dockSnap,
        clearSnapshot: clearPolledSnapshot,
    } = useAgentProductionJobs({
        sessionId,
        tracks: jobTracks,
        pollResetKey,
        getToken,
        shouldPollJobTrack: (track) => (
            !shouldSuppressProductionJob(track.job_id, track.title, messagesRef.current, blockedJobIdsRef.current)
        ),
        shouldAcceptSnapshot: shouldAcceptPolledSnapshot,
        onProgress: upsertProgressLine,
        onRunningPreview: (snap) => appendJobDeliverable(snap, { source: 'poll' }),
        onGhostJobDropped: (track) => {
            dropGhostShortformTrack(track.job_id);
        },
        onJobComplete: (snap: AgentJobSnapshot) => {
            const ownerSession = jobSessionRef.current.get(snap.job_id);
            if (ownerSession && ownerSession !== sessionIdRef.current) return;
            setDockDismissed(true);
            setJobTracks((prev) => {
                const next = prev.filter((j) => j.job_id !== snap.job_id);
                if (sessionId) persistJobs(sessionId, next);
                return next;
            });
            setMessages((rows) => rows.filter((row) => row.productionUpdate?.job_id !== snap.job_id));
            appendJobDeliverable(snap, { source: 'poll' });
        },
        onJobFailed: (snap: AgentJobSnapshot) => {
            const ownerSession = jobSessionRef.current.get(snap.job_id);
            if (ownerSession && ownerSession !== sessionIdRef.current) return;
            if (isGhostJobPollFailure(snap) || shouldHideJobDeliverable(snap, messagesRef.current)) {
                dropGhostShortformTrack(snap.job_id);
                return;
            }
            if (isStaleLongformChapterFailure(snap) || isStaleIdleLongformFailure(snap)) {
                setJobTracks((prev) => {
                    const next = prev.filter((j) => j.job_id !== snap.job_id);
                    if (sessionId) persistJobs(sessionId, next);
                    return next;
                });
                setMessages((rows) => rows.filter((row) => row.productionUpdate?.job_id !== snap.job_id));
                setError((prev) => (
                    snap.error && prev.includes(snap.error) ? '' : prev
                ));
                return;
            }
            if (isImplicitCancelFailure(snap)) {
                setJobTracks((prev) => {
                    const next = prev.filter((j) => j.job_id !== snap.job_id);
                    if (sessionId) persistJobs(sessionId, next);
                    return next;
                });
                setMessages((rows) => rows.filter((row) => row.productionUpdate?.job_id !== snap.job_id));
                setDockDismissed(true);
                setError((prev) => (
                    snap.error && prev.includes(snap.error) ? '' : prev
                ));
                return;
            }
            setPending([]);
            setDockDismissed(false);
            setError(snap.error || 'Production failed');
            appendJobDeliverable(snap, { source: 'poll' });
        },
        onAwaitingApproval: (snap) => appendJobDeliverable(snap, { source: 'poll' }),
        autoFinalizeLongform: approvalMode === 'auto',
        onAutoFinalizeStarted: handleFinalizeStarted,
    });

    const awaitingDeliverableSnap = useMemo(
        () => resolveAwaitingDeliverableSnap(messages, snapshots),
        [messages, snapshots],
    );

    const failedDeliverableSnap = useMemo(() => {
        for (const snap of Object.values(snapshots)) {
            if (snap?.status === 'failed' && !isImplicitCancelFailure(snap)) return snap;
        }
        for (let i = messages.length - 1; i >= 0; i -= 1) {
            const snap = messages[i]?.jobDeliverable;
            if (snap?.status === 'failed' && !isImplicitCancelFailure(snap)) return snap;
        }
        return undefined;
    }, [isImplicitCancelFailure, messages, snapshots]);

    const repairingDockSnap = useMemo<AgentJobSnapshot | undefined>(() => {
        if (!repairingJobId) return undefined;
        const cached = deliverablesByJobRef.current.get(repairingJobId)
            || [...messages].reverse().find((row) => jobIdsMatch(row.jobDeliverable?.job_id, repairingJobId))?.jobDeliverable;
        return {
            ...(cached || {}),
            job_id: repairingJobId,
            kind: 'shortform',
            status: 'running',
            running: true,
            progress: Number(cached?.progress || 0),
            stage: 'repairing_scenes',
            stage_label: 'Repairing selected scenes',
            stage_detail: 'Auditing and repairing only the requested scenes. Existing good assets stay preserved.',
            error: null,
            client_updated_at: Date.now(),
        };
    }, [messages, repairingJobId]);

    const resolvedDockSnap = repairingDockSnap
        || (awaitingDeliverableSnap?.status === 'awaiting_approval'
        ? awaitingDeliverableSnap
        : dockSnap?.status === 'failed'
        ? dockSnap
        : failedDeliverableSnap?.status === 'failed'
          ? failedDeliverableSnap
          : dockSnap);

    const resolvedDockTrack = (
        dockTrack
        && resolvedDockSnap
        && jobIdsMatch(dockTrack.job_id, resolvedDockSnap.job_id)
    ) ? dockTrack : (resolvedDockSnap ? {
            job_id: resolvedDockSnap.job_id,
            kind: (resolvedDockSnap.kind || 'longform') as AgentJobTrack['kind'],
            title: resolvedDockSnap.title,
            started_at: Date.now(),
        } : undefined);

    const showRenderDock = Boolean(
        resolvedDockTrack
        && resolvedDockSnap
        && !isImplicitCancelFailure(resolvedDockSnap)
        && !isStaleAwaitingSnapshot(resolvedDockSnap, messages)
        && !isStaleDeadLongformPoll(resolvedDockSnap)
        && !isStaleIdleLongformFailure(resolvedDockSnap)
        && !dismissedDockJobIdsRef.current.has(resolvedDockSnap.job_id)
        && !dockDismissed,
    );
    const dockRepairableFailedSnap = isRepairableShortformFailure(resolvedDockSnap)
        ? resolvedDockSnap
        : undefined;
    // `/retry-production` repeats the production-start action. Never expose it
    // for a scene-bearing failed short; use the existing chat command compiler
    // until the backend provides a dedicated scene-repair endpoint.


    const appendDictation = useCallback((text: string) => {
        const chunk = text.trim();
        if (!chunk) return;
        setInput((prev) => (prev.trim() ? `${prev.trimEnd()} ${chunk}` : chunk));
        setDictationPreview('');
    }, []);

    const dictation = useSpeechDictation({
        getAccessToken: getToken,
        onFinalText: appendDictation,
        onInterimText: setDictationPreview,
    });

    const authFetch = useCallback(
        async (path: string, init?: RequestInit & { timeoutMs?: number; retries?: number }) => {
            const tok = await getToken();
            const url = resolveStudioBackendUrl(path);
            const timeoutMs = init?.timeoutMs ?? 0;
            const retries = Math.max(0, Number(init?.retries ?? 0));
            const { timeoutMs: _omitTimeout, retries: _omitRetries, ...fetchInit } = init || {};
            let lastError: Error | null = null;
            for (let attempt = 0; attempt <= retries; attempt++) {
                const ctrl = new AbortController();
                const timer =
                    timeoutMs > 0
                        ? window.setTimeout(() => ctrl.abort(), timeoutMs)
                        : undefined;
                try {
                    const res = await fetch(url, {
                        ...fetchInit,
                        signal: ctrl.signal,
                        headers: {
                            'Content-Type': 'application/json',
                            Authorization: `Bearer ${tok}`,
                            ...(fetchInit.headers || {}),
                        },
                    });
                    const data = (await res.json().catch(() => ({}))) as Record<string, unknown>;
                    if (!res.ok) {
                        throw new Error(friendlyApiError(res.status, data, res.statusText));
                    }
                    return data;
                } catch (e) {
                    lastError = e as Error;
                    const message = String((e as Error)?.message || e || '');
                    if ((e as Error).name === 'AbortError' && attempt < retries) {
                        await new Promise((r) => window.setTimeout(r, NETWORK_BLIP_RETRY_MS));
                        continue;
                    }
                    if ((e as Error).name === 'AbortError') {
                        throw new Error(
                            `Studio Agent timed out after ${Math.round(timeoutMs / 1000)}s — retry Resume or open the chat from History.`,
                        );
                    }
                    if (isNetworkBlip(message) && attempt < retries) {
                        await new Promise((r) => window.setTimeout(r, NETWORK_BLIP_RETRY_MS * (attempt + 1)));
                        continue;
                    }
                    if (isNetworkBlip(message)) {
                        throw new Error(
                            'Studio Agent could not reach the backend from this browser tab. Your chat is preserved; wait a moment and press Resume.',
                        );
                    }
                    throw e;
                } finally {
                    if (timer) window.clearTimeout(timer);
                }
            }
            throw lastError || new Error('Studio Agent request failed');
        },
        [getToken],
    );

    const scrollToBottom = useCallback((behavior: ScrollBehavior = 'smooth') => {
        const el = scrollRef.current;
        if (!el) return;
        el.scrollTo({ top: el.scrollHeight, behavior });
    }, []);

    const styleAssetUrl = useCallback((path?: string) => {
        if (!path) return '';
        return path.startsWith('http') ? path : resolveStudioBackendUrl(path.startsWith('/') ? path : `/${path}`);
    }, []);

    const handleScroll = useCallback(() => {
        const el = scrollRef.current;
        if (!el) return;
        const distance = el.scrollHeight - el.scrollTop - el.clientHeight;
        stickToBottomRef.current = distance < 96;
    }, []);

    useEffect(() => {
        if (stickToBottomRef.current) {
            scrollToBottom(messages.length <= 1 ? 'auto' : 'smooth');
        }
    }, [messages, pending, currentSessionRunning, scrollToBottom]);

    useEffect(() => {
        if (pending.length > 0) {
            stickToBottomRef.current = true;
            requestAnimationFrame(() => scrollToBottom('smooth'));
        }
    }, [pending.length, scrollToBottom]);

    const lastSessionKey = lastSessionStorageKey(session?.user?.id);

    const applyProductionLedger = useCallback((raw: Record<string, unknown>) => {
        const prevBlocked = [...blockedJobIdsRef.current];
        const prevEpoch = productionEpochRef.current;
        if (Array.isArray(raw.blocked_job_ids)) {
            blockedJobIdsRef.current = (raw.blocked_job_ids as unknown[])
                .map((value) => String(value || '').trim())
                .filter(Boolean);
        }
        const prodState = raw.production_state as { epoch?: number } | undefined;
        const nextEpoch = Math.max(1, Number(prodState?.epoch || 1));
        const sid = String(raw.session_id || sessionIdRef.current || '').trim();
        const blockedChanged = blockedJobIdsRef.current.length !== prevBlocked.length
            || blockedJobIdsRef.current.some((id, index) => id !== prevBlocked[index]);
        const epochBumped = nextEpoch > prevEpoch;
        productionEpochRef.current = nextEpoch;
        if (epochBumped) {
            for (const jobId of [...deliverablesByJobRef.current.keys()]) {
                if (isBlockedJobId(jobId, blockedJobIdsRef.current)) {
                    deliverablesByJobRef.current.delete(jobId);
                }
            }
            if (sid) {
                persistJobs(sid, []);
                setJobTracks([]);
                setDockDismissed(true);
            }
            setConceptPlan((prev) => {
                if (!prev || prev.status !== 'started') return prev;
                return { ...prev, status: 'confirmed' };
            });
        }
        if (epochBumped || blockedChanged) {
            setMessages((rows) => {
                const stripped = stripStaleProductionArtifacts(
                    rows,
                    (jobId, title) => shouldSuppressProductionJob(
                        jobId,
                        title,
                        rows,
                        blockedJobIdsRef.current,
                    ),
                );
                messagesRef.current = stripped;
                return stripped;
            });
        }
    }, []);

    const applySessionPayload = useCallback((raw: Record<string, unknown>, opts?: { forceServer?: boolean }) => {
        const forceServer = Boolean(opts?.forceServer);
        const sid = String(raw.session_id || '');
        if (sid) {
            sessionIdRef.current = sid;
            setSessionId(sid);
            try {
                localStorage.setItem(lastSessionKey, sid);
            } catch {
                /* ignore */
            }
        }
        applyProductionLedger(raw);
        const rawMessages = raw.messages;
        const hasServerMessages = Array.isArray(rawMessages);
        let effectiveMessages: ChatMessage[] | null = null;
        if (hasServerMessages) {
            const msgs = rawMessages
                .map(normalizeAgentMessage)
                .filter((msg): msg is ChatMessage => Boolean(msg));
            const cached = sid ? sessionUiCacheRef.current.get(sid) : null;
            const serverMessageCount = Number(raw.message_count ?? msgs.length);
            // Force server sync never falls back to local cache for transcript.
            effectiveMessages =
                !forceServer && msgs.length === 0 && serverMessageCount > 0 && cached?.messages?.length
                    ? cached.messages
                    : msgs;
            const skipGlobalDeliverables = Boolean(
                raw.forked_from
                || raw.context_ingested
                || (Boolean(raw.skip_job_recovery) && Array.isArray(raw.active_jobs) && raw.active_jobs.length === 0 && msgs.length > 0),
            );
            const deliverableContext = [
                ...(effectiveMessages || msgs),
                ...messagesRef.current,
                ...(cached?.messages || []),
            ];
            const preservedDeliverables = new Map<string, AgentJobSnapshot>();
            const shouldPreserveDeliverable = (snap: AgentJobSnapshot) => {
                if (forceServer && snap.status === 'running') return false;
                if (shouldSuppressProductionJob(
                    snap.job_id,
                    snap.title,
                    deliverableContext,
                    blockedJobIdsRef.current,
                )) {
                    return false;
                }
                return !shouldHideJobDeliverable(snap, deliverableContext);
            };
            if (!skipGlobalDeliverables) {
                for (const [jobId, snap] of deliverablesByJobRef.current.entries()) {
                    if (shouldPreserveDeliverable(snap)) {
                        preservedDeliverables.set(jobId, snap);
                    }
                }
            }
            for (const msg of messagesRef.current) {
                const snap = msg.jobDeliverable;
                if (snap?.job_id && shouldPreserveDeliverable(snap)) {
                    preservedDeliverables.set(snap.job_id, snap);
                }
            }
            for (const msg of cached?.messages || []) {
                const snap = msg.jobDeliverable;
                if (snap?.job_id && shouldPreserveDeliverable(snap)) {
                    preservedDeliverables.set(snap.job_id, snap);
                }
            }
            for (const msg of msgs) {
                const snap = msg.jobDeliverable;
                if (snap?.job_id && shouldPreserveDeliverable(snap)) {
                    preservedDeliverables.set(snap.job_id, snap);
                }
            }
            const transcriptForStale = effectiveMessages || msgs;
            effectiveMessages = stripStaleProductionArtifacts(
                stripGhostJobDeliverables(
                    reattachJobDeliverables(effectiveMessages, preservedDeliverables),
                ),
                (jobId, title) => shouldSuppressProductionJob(
                    jobId,
                    title,
                    transcriptForStale,
                    blockedJobIdsRef.current,
                ),
            );
            messagesRef.current = effectiveMessages;
            setMessages(effectiveMessages);
        } else if (sid && !forceServer) {
            const cached = sessionUiCacheRef.current.get(sid);
            if (cached?.messages?.length) {
                effectiveMessages = cached.messages;
                messagesRef.current = effectiveMessages;
                setMessages(cached.messages);
            }
        }
        const serverPending = (raw.pending_actions as PendingAction[]) || [];
        // Server is the only authority for pending when force-syncing.
        const resolvedPending = forceServer
            ? filterOwnerOnlyPending(
                filterStalePendingActions(serverPending, effectiveMessages || messagesRef.current),
                isAdminUser,
            )
            : effectiveMessages
                ? filterOwnerOnlyPending(
                    mergePendingFromTranscript(effectiveMessages, serverPending),
                    isAdminUser,
                )
                : sid
                    ? filterOwnerOnlyPending(
                        filterStalePendingActions(
                            sessionUiCacheRef.current.get(sid)?.pending || [],
                            sessionUiCacheRef.current.get(sid)?.messages || [],
                        ),
                        isAdminUser,
                    )
                    : [];
        setPending(resolvedPending);
        if (forceServer && sid) {
            sessionUiCacheRef.current.set(sid, {
                messages: effectiveMessages || messagesRef.current,
                pending: resolvedPending,
                jobTracks: sessionUiCacheRef.current.get(sid)?.jobTracks || [],
                dockDismissed: sessionUiCacheRef.current.get(sid)?.dockDismissed ?? true,
            });
            persistStoredSessionUiCache(userCacheKey, sessionUiCacheRef.current);
        }
        const rawConcept = (raw.pending_concept || raw.concept_plan) as ConceptPlan | null | undefined;
        if (rawConcept && typeof rawConcept === 'object' && (rawConcept.title || rawConcept.id || rawConcept.format)) {
            setConceptPlan(rawConcept);
        } else if (!rawConcept || forceServer) {
            setConceptPlan(null);
        }
        if (raw.model) setModel(String(raw.model));
        if (raw.agent_mode === 'plan' || raw.agent_mode === 'studio' || raw.agent_mode === 'cliplab') {
            const mode = raw.agent_mode as AgentMode;
            setAgentMode(!isAdminUser && mode === 'cliplab' ? 'plan' : mode);
        }
        if (raw.approval_mode === 'auto' || raw.approval_mode === 'confirm') {
            setApprovalMode(raw.approval_mode);
        }
        const fmt = raw.content_format as ContentFormat | undefined;
        if (fmt === 'short' || fmt === 'long' || fmt === 'both') {
            setContentFormat(!canUseLongform && fmt !== 'short' ? 'short' : fmt);
        }
        const depth = raw.reasoning_depth as ReasoningDepth | undefined;
        if (depth === 'fast' || depth === 'balanced' || depth === 'deep') {
            setReasoningDepth(depth);
        }
        const rs = String(raw.render_style || '').trim();
        if (rs) setRenderStyle(rs);
        setImageModel(String(raw.image_model || raw.image_model_id || DEFAULT_IMAGE_MODEL).trim() || DEFAULT_IMAGE_MODEL);
        setVideoModel(String(raw.video_model || DEFAULT_VIDEO_MODEL).trim() || DEFAULT_VIDEO_MODEL);
        const rawCaptionMode = String(raw.caption_mode || '').trim();
        if (rawCaptionMode === 'word' || rawCaptionMode === 'off') {
            setCaptionMode(rawCaptionMode);
        } else if (raw.captions_enabled === false) {
            setCaptionMode('off');
        } else if (raw.captions_enabled === true) {
            setCaptionMode('word');
        }
        if (typeof raw.animate === 'boolean') setAnimate(raw.animate);
        const channelId = String(raw.channel_id || '').trim();
        const channelTitle = String(raw.channel_title || '').trim();
        const registryKey = String(raw.registry_key || '').trim();
        setSelectedChannelId(channelId);
        setSessionChannel(channelId || channelTitle || registryKey ? {
            channel_id: channelId,
            title: channelTitle || registryKey || channelId,
            registry_key: registryKey,
        } : null);
        const activeRuns = Array.isArray(raw.active_runs) ? raw.active_runs as SessionSummary['active_runs'] : [];
        const runLabel = activeRunLabel({ active_runs: activeRuns });
        if (sid) {
            // Critical: clear stuck "running" when server has no active runs.
            // Old code only set running=true and never cleared → Sync looked like Resume stuck.
            setRunningBySession((prev) => {
                if (runLabel) return { ...prev, [sid]: runLabel };
                if (!prev[sid]) return prev;
                const next = { ...prev };
                delete next[sid];
                return next;
            });
        }
        if (forceServer) {
            setToolActivity('');
            setActivitySteps([]);
            setQueueHint('');
        }
    }, [applyProductionLedger, canUseLongform, isAdminUser, lastSessionKey, userCacheKey]);

    const resumeSession = useCallback(
        async (raw: Record<string, unknown>, opts?: { rehydrateJobs?: boolean; forceServer?: boolean }) => {
            applySessionPayload(raw, { forceServer: opts?.forceServer });
            const review = raw.thumbnail_review as ThumbnailReview | null | undefined;
            if (review?.review_id && (review.candidate_urls || []).length) {
                mergeThumbnailReviewIntoDeliverable(review);
            }
            const sid = String(raw.session_id || '');
            if (!sid) return;
            const activeRuns = Array.isArray(raw.active_runs)
                ? raw.active_runs as SessionSummary['active_runs']
                : [];
            const repairJobId = repairingJobIdRef.current;
            if (repairJobId) {
                if (activeRunLabel({ active_runs: activeRuns })) {
                    repairActiveRunSeenRef.current.add(repairJobId);
                } else if (repairActiveRunSeenRef.current.has(repairJobId)) {
                    // A disconnected stream may finish entirely server-side.
                    // Once Sync observes the run transition from active to
                    // terminal, release the synthetic repair state and allow a
                    // fresh status read after the short stale-result grace.
                    repairActiveRunSeenRef.current.delete(repairJobId);
                    repairSnapshotGuardRef.current.set(
                        repairJobId,
                        Date.now() + REPAIR_STALE_SNAPSHOT_GRACE_MS,
                    );
                    repairingJobIdRef.current = '';
                    setRepairingJobId((current) => (
                        jobIdsMatch(current, repairJobId) ? '' : current
                    ));
                    setPollResetKey((key) => key + 1);
                }
            }
            const serverJobs = Array.isArray(raw.active_jobs) ? (raw.active_jobs as AgentJobTrack[]) : [];
            const persisted = opts?.forceServer
                ? []
                : pruneOrphanShortformTracks(loadPersistedJobs(sid), serverJobs);
            const merged = pruneOrphanShortformTracks(
                opts?.forceServer
                    ? mergeJobTracks(serverJobs, collectTracksToRefresh([], messagesRef.current, blockedJobIdsRef.current))
                    : mergeJobTracks(persisted, serverJobs),
                serverJobs,
            );
            const tracksForSession = pruneStaleJobTracks(
                merged.length
                    ? merged
                    : collectTracksToRefresh(
                        collectTracksFromTranscript(messagesRef.current),
                        messagesRef.current,
                        blockedJobIdsRef.current,
                    ),
                messagesRef.current,
                blockedJobIdsRef.current,
            );
            for (const job of tracksForSession) {
                if (job.job_id) jobSessionRef.current.set(job.job_id, sid);
            }
            // Always apply server job list (including empty) so Sync clears ghosts.
            setJobTracks(tracksForSession);
            persistJobs(sid, tracksForSession);
            if (!tracksForSession.length) {
                if (opts?.forceServer) setDockDismissed(true);
                if (opts?.rehydrateJobs === false) return;
            } else {
                setDockDismissed(false);
            }
            if (opts?.rehydrateJobs === false) return;
            try {
                const tok = await getToken();
                const tracksToRefresh = collectTracksToRefresh(tracksForSession, messagesRef.current, blockedJobIdsRef.current);
                const { deliverables } = await rehydrateJobSnapshots(sid, tracksToRefresh, tok);
                if (sessionIdRef.current !== sid) return;
                const review = raw.thumbnail_review as ThumbnailReview | null | undefined;
                const hasThumbnailReview = Boolean(review?.candidate_urls?.length);
                const prunedTracks = tracksForSession.filter((track) => {
                    const snap = deliverables.find((row) => row.job_id === track.job_id);
                    if (isStaleLongformChapterFailure(snap)) return false;
                    if (isStaleIdleLongformFailure(snap)) return false;
                    if (isStaleDeadLongformPoll(snap)) return false;
                    if (snap?.thumbnail_only) return false;
                    if (hasThumbnailReview && snap?.status === 'failed' && snap.kind === 'longform') return false;
                    return true;
                });
                if (prunedTracks.length !== tracksForSession.length) {
                    setJobTracks(prunedTracks);
                    persistJobs(sid, prunedTracks);
                }
                for (const snap of deliverables) {
                    if (isStaleLongformChapterFailure(snap)) continue;
                    if (shouldSuppressProductionJob(
                        snap.job_id,
                        snap.title,
                        messagesRef.current,
                        blockedJobIdsRef.current,
                    )) continue;
                    if (snap.status === 'failed' && snap.kind === 'longform' && hasThumbnailReview) continue;
                    appendJobDeliverable(snap, { source: 'rehydrate' });
                }
                setPollResetKey((k) => k + 1);
                const cachedDeliverableJobs = new Map<string, AgentJobTrack>();
                for (const msg of messagesRef.current) {
                    const jobId = msg.jobDeliverable?.job_id;
                    if (!jobId) continue;
                    cachedDeliverableJobs.set(jobId, {
                        job_id: jobId,
                        kind: msg.jobDeliverable?.kind || 'shortform',
                        title: msg.jobDeliverable?.title,
                    });
                }
                const missingMp4 = [...cachedDeliverableJobs.values()].filter((track) => {
                    const snap = messagesRef.current.find((msg) => msg.jobDeliverable?.job_id === track.job_id)?.jobDeliverable;
                    return snap?.status === 'complete' && !snap?.mp4_url;
                });
                if (missingMp4.length) {
                    const refreshed = await rehydrateJobSnapshots(sid, missingMp4, tok);
                    for (const snap of refreshed.deliverables) {
                        appendJobDeliverable(snap, { source: 'rehydrate' });
                    }
                }
            } catch {
                /* polling optional on resume */
            }
        },
        [applySessionPayload, appendJobDeliverable, mergeThumbnailReviewIntoDeliverable, getToken],
    );

    /**
     * True when the deployed frontend build differs from the one running.
     * index.html is served no-cache, so fetching it always reflects the
     * latest deploy; the hashed bundle name is the build identity.
     */
    const newerBuildAvailable = useCallback(async (): Promise<boolean> => {
        try {
            const current = document.querySelector<HTMLScriptElement>('script[src*="assets/index-"]');
            const currentSrc = current?.src || '';
            const match = currentSrc.match(/assets\/index-[\w-]+\.js/);
            if (!match) return false; // dev server or unexpected layout
            const res = await fetch(`/?sync=${Date.now()}`, { cache: 'no-store' });
            if (!res.ok) return false;
            const html = await res.text();
            const deployed = html.match(/assets\/index-[\w-]+\.js/);
            return Boolean(deployed && deployed[0] !== match[0]);
        } catch {
            return false;
        }
    }, []);

    const syncSessionFromServer = useCallback(async (opts?: { quiet?: boolean }) => {
        if (!sessionId) return;
        if (!opts?.quiet) setSyncing(true);
        setError('');
        // Manual Sync doubles as a hard refresh: when a newer frontend build is
        // deployed, reload the page so the user never needs Ctrl+Shift+R. Chat
        // state lives server-side and rehydrates after the reload.
        if (!opts?.quiet && (await newerBuildAvailable())) {
            window.location.reload();
            return;
        }
        try {
            // Dedicated sync endpoint — prunes stale Approves, never rebuilds production pending.
            let data: Record<string, unknown>;
            try {
                data = await authFetch(sessionSyncPath(sessionId), {
                    method: 'POST',
                    timeoutMs: SESSION_LOAD_TIMEOUT_MS,
                    retries: SESSION_LOAD_RETRIES,
                    headers: { 'Cache-Control': 'no-store' },
                });
            } catch {
                // Fallback for older backend until POST /sync is live.
                data = await authFetch(sessionResumePath(sessionId, true), {
                    timeoutMs: SESSION_LOAD_TIMEOUT_MS,
                    retries: SESSION_LOAD_RETRIES,
                    headers: { 'Cache-Control': 'no-store' },
                });
            }
            await resumeSession((data?.session as Record<string, unknown>) || {}, {
                rehydrateJobs: true,
                forceServer: true,
            });
        } finally {
            if (!opts?.quiet) setSyncing(false);
        }
    }, [authFetch, newerBuildAvailable, resumeSession, sessionId]);

    const scheduleAutoSync = useCallback((opts?: { delayMs?: number; rehydrateJobs?: boolean }) => {
        if (!sessionId) return;
        if (autoSyncTimerRef.current) window.clearTimeout(autoSyncTimerRef.current);
        autoSyncTimerRef.current = window.setTimeout(() => {
            autoSyncTimerRef.current = null;
            void (async () => {
                try {
                    await syncSessionFromServer({ quiet: true });
                } catch {
                    /* background sync is best-effort */
                }
            })();
        }, opts?.delayMs ?? 300);
    }, [sessionId, syncSessionFromServer]);

    useEffect(() => () => {
        if (autoSyncTimerRef.current) window.clearTimeout(autoSyncTimerRef.current);
    }, []);

    const refreshHistory = useCallback(async () => {
        const data = await authFetch('/api/studio-agent/sessions?limit=50');
        const sessions = (data?.sessions as SessionSummary[]) || [];
        setHistory(sessions);
        setRunningBySession((prev) => {
            const next = { ...prev };
            const seen = new Set(sessions.map((s) => s.session_id));
            for (const s of sessions) {
                const label = activeRunLabel(s);
                if (label) next[s.session_id] = label;
                else delete next[s.session_id];
            }
            for (const sid of Object.keys(next)) {
                if (!seen.has(sid) && !sessionIdRef.current) {
                    delete next[sid];
                }
            }
            return next;
        });
    }, [authFetch]);

    const createNewSession = useCallback(
        async (pickModel: string) => {
            if (creatingSession) return;
            const priorSessionId = isPersistedAgentSessionId(sessionIdRef.current)
                ? sessionIdRef.current
                : null;
            setCreatingSession(true);
            sessionIdRef.current = null;
            setSessionId(null);
            setError('');
            setQueueHint('');
            setToolActivity('');
            setPending([]);
            setConceptPlan(null);
            setMessages([]);
            deliverablesByJobRef.current.clear();
            blockedJobIdsRef.current = [];
            productionEpochRef.current = 1;
            setJobTracks([]);
            setDockDismissed(true);
            setReplyingTo(null);
            setAttachments([]);
            setAttachmentPayload({});
            try {
                const created = await authFetch('/api/studio-agent/sessions', {
                    method: 'POST',
                    body: JSON.stringify({
                        model: pickModel,
                        approval_mode: approvalMode,
                        content_format: canUseLongform ? contentFormat : 'short',
                        reasoning_depth: reasoningDepth,
                        render_style: renderStyle,
                        image_model: imageModel,
                        video_model: videoModel,
                        caption_mode: captionMode,
                        captions_enabled: captionMode !== 'off',
                        channel_id: selectedChannel?.channel_id || '',
                        registry_key: channelRegistryKey(selectedChannel),
                        channel_title: selectedChannel?.title || '',
                        product_website: productWebsite,
                    }),
                });
                const sid = String((created.session as Record<string, unknown>)?.session_id || '');
                if (!isPersistedAgentSessionId(sid)) {
                    throw new Error('Studio Agent could not create a chat session. Try again.');
                }
                applySessionPayload((created.session as Record<string, unknown>) || {});
                sessionIdRef.current = sid;
                setSessionId(sid);
                setJobTracks([]);
                setPollResetKey((k) => k + 1);
                setDockDismissed(true);
                persistJobs(sid, []);
                setDraftsBySession((prev) => ({ ...prev, [sid]: '' }));
                await refreshHistory();
            } catch (e) {
                setError((e as Error).message);
                if (priorSessionId) {
                    sessionIdRef.current = priorSessionId;
                    setSessionId(priorSessionId);
                    const cached = sessionUiCacheRef.current.get(priorSessionId);
                    if (cached) {
                        messagesRef.current = cached.messages;
                        setMessages(cached.messages);
                        setPending(filterStalePendingActions(cached.pending, cached.messages));
                        setJobTracks(cached.jobTracks);
                        setDockDismissed(cached.dockDismissed);
                    }
                }
            } finally {
                setCreatingSession(false);
            }
        },
        [applySessionPayload, approvalMode, authFetch, canUseLongform, captionMode, contentFormat, creatingSession, imageModel, productWebsite, reasoningDepth, renderStyle, refreshHistory, selectedChannel, videoModel],
    );

    const openSession = useCallback(
        async (id: string) => {
            if (!id) return;
            const loadSeq = ++sessionLoadSeqRef.current;
            sessionIdRef.current = id;
            setSessionId(id);
            const cached = sessionUiCacheRef.current.get(id);
            if (cached) {
                messagesRef.current = cached.messages;
                setMessages(cached.messages);
                setPending([]);
                setJobTracks(cached.jobTracks);
                setDockDismissed(cached.dockDismissed);
            } else {
                setDockDismissed(true);
            }
            setReplyingTo(null);
            setToolActivity('');
            setResuming(true);
            setError('');
            try {
                const data = await authFetch(sessionResumePath(id, false), {
                    timeoutMs: SESSION_LOAD_TIMEOUT_MS,
                    retries: SESSION_LOAD_RETRIES,
                });
                if (sessionLoadSeqRef.current !== loadSeq || sessionIdRef.current !== id) return;
                await resumeSession((data?.session as Record<string, unknown>) || {}, {
                    rehydrateJobs: true,
                });
                setDockDismissed(false);
            } catch (e) {
                if (sessionLoadSeqRef.current !== loadSeq || sessionIdRef.current !== id) return;
                const msg = (e as Error).message || '';
                if (cached && (msg.includes('timed out after') || isNetworkBlip(msg))) {
                    setQueueHint('Connection blip — showing your saved chat while Studio reconnects. Press Sync when back online.');
                    scheduleAutoSync({ delayMs: 2500, rehydrateJobs: true });
                    return;
                }
                setError(msg);
            } finally {
                if (sessionLoadSeqRef.current === loadSeq) setResuming(false);
            }
        },
        [authFetch, resumeSession],
    );

    const reloadCurrentSession = useCallback(async () => {
        // Cold resume / history reopen — separate from Sync chat (which uses force sync).
        const loadSeq = ++sessionLoadSeqRef.current;
        setResuming(true);
        setError('');
        setQueueHint('');
        try {
            let id = sessionId || '';
            if (!id) {
                try {
                    id = localStorage.getItem(lastSessionKey) || '';
                } catch {
                    id = '';
                }
            }
            if (!id && history.length > 0) {
                id = history[0].session_id;
            }
            if (!id) {
                setError(
                    'No chat to resume — pick your thread from History on the left, or start a new chat.',
                );
                return;
            }
            // Prefer authoritative sync when we already have the session open.
            let data: Record<string, unknown>;
            if (sessionId && id === sessionId) {
                try {
                    data = await authFetch(sessionSyncPath(id), {
                        method: 'POST',
                        timeoutMs: SESSION_LOAD_TIMEOUT_MS,
                        retries: SESSION_LOAD_RETRIES,
                        headers: { 'Cache-Control': 'no-store' },
                    });
                } catch {
                    data = await authFetch(sessionResumePath(id, true), {
                        timeoutMs: SESSION_LOAD_TIMEOUT_MS,
                        retries: SESSION_LOAD_RETRIES,
                        headers: { 'Cache-Control': 'no-store' },
                    });
                }
            } else {
                data = await authFetch(sessionResumePath(id, true), {
                    timeoutMs: SESSION_LOAD_TIMEOUT_MS,
                    retries: SESSION_LOAD_RETRIES,
                    headers: { 'Cache-Control': 'no-store' },
                });
            }
            if (sessionLoadSeqRef.current !== loadSeq) return;
            await resumeSession((data?.session as Record<string, unknown>) || {}, {
                rehydrateJobs: true,
                forceServer: true,
            });
        } catch (e) {
            if (sessionLoadSeqRef.current !== loadSeq) return;
            const msg = (e as Error).message || '';
            if (messagesRef.current.length > 0 && (msg.includes('timed out after') || isNetworkBlip(msg))) {
                setQueueHint('Still syncing your chat in the background — production rails update automatically.');
                scheduleAutoSync({ delayMs: 1500, rehydrateJobs: true });
                return;
            }
            setError(msg);
        } finally {
            if (sessionLoadSeqRef.current === loadSeq) setResuming(false);
        }
    }, [authFetch, history, lastSessionKey, resumeSession, scheduleAutoSync, sessionId]);

    const rolloverSession = useCallback(async () => {
        if (!sessionId || currentSessionRunning) return;
        if (
            !window.confirm(
                'Roll this chat into a new session? Your full transcript, pending approvals, and active renders carry over.',
            )
        ) {
            return;
        }
        markSessionRunning(sessionId, 'Rolling over chat...');
        setError('');
        try {
            const data = await authFetch(`/api/studio-agent/sessions/${sessionId}/rollover`, {
                method: 'POST',
            });
            await resumeSession((data?.session as Record<string, unknown>) || {});
            await refreshHistory();
        } catch (e) {
            setError((e as Error).message);
        } finally {
            clearSessionRunning(sessionId);
        }
    }, [authFetch, clearSessionRunning, currentSessionRunning, markSessionRunning, refreshHistory, resumeSession, sessionId]);

    const resetProductionInPlace = useCallback(async () => {
        if (!sessionId || currentSessionRunning) return;
        if (
            !window.confirm(
                'Reset production for this chat? Old renders and approve cards are cleared; your transcript and concept plan stay.',
            )
        ) {
            return;
        }
        markSessionRunning(sessionId, 'Resetting production...');
        setError('');
        try {
            const data = await authFetch(
                `/api/studio-agent/sessions/${sessionId}/reset-production?message_tail=${SESSION_MESSAGE_TAIL}`,
                { method: 'POST', body: JSON.stringify({}) },
            );
            deliverablesByJobRef.current.clear();
            await resumeSession((data?.session as Record<string, unknown>) || {}, {
                rehydrateJobs: false,
                forceServer: true,
            });
            setPending([]);
            setPollResetKey((k) => k + 1);
        } catch (e) {
            setError((e as Error).message);
        } finally {
            clearSessionRunning(sessionId);
        }
    }, [authFetch, clearSessionRunning, currentSessionRunning, markSessionRunning, resumeSession, sessionId]);

    const forkSessionWithContext = useCallback(async (sourceSessionId?: string) => {
        const fromId = String(sourceSessionId || sessionId || '').trim();
        if (!fromId || currentSessionRunning) return;
        if (
            !window.confirm(
                'Start a fresh chat with context from this thread? Channel setup and prior decisions carry over, but old renders and scene-review jobs do not.',
            )
        ) {
            return;
        }
        markSessionRunning(fromId, 'Starting chat with context...');
        setError('');
        try {
            const data = await authFetch(`/api/studio-agent/sessions/${fromId}/fork`, {
                method: 'POST',
            });
            deliverablesByJobRef.current.clear();
            await resumeSession((data?.session as Record<string, unknown>) || {}, {
                rehydrateJobs: false,
            });
            await refreshHistory();
        } catch (e) {
            setError((e as Error).message);
        } finally {
            clearSessionRunning(fromId);
        }
    }, [authFetch, clearSessionRunning, currentSessionRunning, markSessionRunning, refreshHistory, resumeSession, sessionId]);

    useEffect(() => {
        if (!sessionId) return;
        const onVisible = () => {
            if (document.visibilityState !== 'visible') return;
            const needsPendingSync =
                pending.length === 0 && transcriptMentionsPendingAction(messages);
            if (pending.length === 0 && jobTracks.length === 0 && !needsPendingSync) return;
            // Quiet server sync — never flip the Resume button to "Resuming…".
            void syncSessionFromServer({ quiet: true }).catch(() => {});
        };
        document.addEventListener('visibilitychange', onVisible);
        return () => document.removeEventListener('visibilitychange', onVisible);
    }, [sessionId, pending.length, jobTracks.length, messages, syncSessionFromServer]);

    const confirmDeleteSession = useCallback(
        async (id: string) => {
            if (!id || currentSessionRunning) return;
            markSessionRunning(id, 'Deleting chat...');
            setError('');
            try {
                await authFetch(`/api/studio-agent/sessions/${id}`, { method: 'DELETE' });
                const listData = await authFetch('/api/studio-agent/sessions?limit=50');
                const sessions = (listData?.sessions as SessionSummary[]) || [];
                setHistory(sessions);
                if (id === sessionId) {
                    try {
                        localStorage.removeItem(lastSessionKey);
                    } catch {
                        /* ignore */
                    }
                    if (sessions[0]?.session_id) {
                        await openSession(sessions[0].session_id);
                    } else {
                        await createNewSession(model);
                    }
                }
            } catch (e) {
                setError((e as Error).message);
            } finally {
                clearSessionRunning(id);
                setDeleteTarget(null);
            }
        },
        [authFetch, clearSessionRunning, createNewSession, currentSessionRunning, lastSessionKey, markSessionRunning, model, openSession, sessionId],
    );

    useEffect(() => {
        let cancelled = false;
        (async () => {
            setBooting(true);
            setError('');
            try {
                // Do not serially block the conversation on three unrelated
                // fetches. The general config endpoint is optional for Agent
                // boot and may be slower than the Fly-hosted Agent API.
                // The picker already has a complete baked-in catalog. A slow
                // provider catalog refresh must never fail or block Agent boot.
                const modelsRequest = authFetch('/api/studio-agent/models', { timeoutMs: 12_000 })
                    .catch(() => ({} as Record<string, unknown>));
                const sessionsRequest = authFetch('/api/studio-agent/sessions?limit=50', { timeoutMs: 45_000, retries: SESSION_LOAD_RETRIES });
                void authFetch('/api/config', { timeoutMs: 3500 })
                    .then((configData) => {
                        const creative = (configData?.creative_model_catalog || {}) as {
                            image_models?: CreativeModelProfile[];
                            video_models?: CreativeModelProfile[];
                        };
                    const imageOptions = (creative.image_models || [])
                        .map(creativeModelOption)
                        .filter((m): m is AgentModelOption => Boolean(m))
                        .filter((m) => SUPPORTED_AGENT_IMAGE_MODEL_IDS.has(m.id));
                    const videoOptions = (creative.video_models || [])
                        .map(creativeModelOption)
                        .filter((m): m is AgentModelOption => Boolean(m))
                        .filter((m) => SUPPORTED_AGENT_VIDEO_MODEL_IDS.has(m.id));
                        if (!cancelled) {
                            // The remote creative catalog is intentionally partial: it only
                            // advertises profiles supplied by this deployment.  Replacing the
                            // baked-in list with it hid direct xAI Grok Imagine whenever the
                            // catalog happened to contain only a FAL profile.  Merge instead,
                            // so every model with a real Agent adapter remains selectable.
                            if (imageOptions.length) setImageModelCatalog(mergeCreativeModelOptions(FALLBACK_IMAGE_MODELS, imageOptions));
                            if (videoOptions.length) setVideoModelCatalog(mergeCreativeModelOptions(FALLBACK_VIDEO_MODELS, videoOptions));
                        }
                    })
                    .catch(() => { /* baked picker catalog is already ready */ });
                const [modelData, listData] = await Promise.all([modelsRequest, sessionsRequest]);
                const catalog = (modelData?.models as AgentModelOption[]) || [];
                const modelDataImageOptions = ((modelData?.image_models as CreativeModelProfile[]) || [])
                    .map(creativeModelOption)
                    .filter((m): m is AgentModelOption => Boolean(m))
                    .filter((m) => SUPPORTED_AGENT_IMAGE_MODEL_IDS.has(m.id));
                const modelDataVideoOptions = ((modelData?.video_models as CreativeModelProfile[]) || [])
                    .map(creativeModelOption)
                    .filter((m): m is AgentModelOption => Boolean(m))
                    .filter((m) => SUPPORTED_AGENT_VIDEO_MODEL_IDS.has(m.id));
                const rec = (modelData?.recommended as string[]) || [];
                let pickModel = FALLBACK_MODELS[0].id;
                if (!cancelled) {
                    if (modelDataImageOptions.length) setImageModelCatalog(mergeCreativeModelOptions(FALLBACK_IMAGE_MODELS, modelDataImageOptions));
                    if (modelDataVideoOptions.length) setVideoModelCatalog(mergeCreativeModelOptions(FALLBACK_VIDEO_MODELS, modelDataVideoOptions));
                    if (catalog.length) {
                        setModelCatalog(catalog);
                        pickModel = catalog.find((m) => m.recommended)?.id || catalog[0].id;
                        setModel(pickModel);
                    } else if (rec.length) {
                        setModelCatalog(
                            rec.map((id) => FALLBACK_MODELS.find((m) => m.id === id) || {
                                id,
                                name: displayModelName(FALLBACK_MODELS, id),
                                provider: 'Anthropic',
                            }),
                        );
                        pickModel = rec[0];
                        setModel(pickModel);
                    }
                }
                const sessions = (listData?.sessions as SessionSummary[]) || [];
                if (!cancelled) setHistory(sessions);

                let lastId = '';
                try {
                    lastId = localStorage.getItem(lastSessionStorageKey(session?.user?.id)) || '';
                } catch {
                    lastId = '';
                }
                const resume = sessions.find((s) => s.session_id === lastId) || sessions[0];
                if (!cancelled && resume?.session_id) {
                    // Show the shell immediately; hydrate transcript in the background.
                    if (!cancelled) setBooting(false);
                    const data = await authFetch(sessionResumePath(resume.session_id, false), {
                        timeoutMs: SESSION_LOAD_TIMEOUT_MS,
                        retries: SESSION_LOAD_RETRIES,
                    });
                    if (!cancelled) {
                        await resumeSession((data?.session as Record<string, unknown>) || {});
                    }
                    return;
                }
                if (!cancelled) setBooting(false);
                if (!cancelled) {
                    const created = await authFetch('/api/studio-agent/sessions', {
                        method: 'POST',
                        body: JSON.stringify({
                            model: pickModel,
                            approval_mode: 'confirm',
                            content_format: canUseLongform ? 'both' : 'short',
                            image_model: loadImageModelPref(DEFAULT_IMAGE_MODEL),
                            video_model: DEFAULT_VIDEO_MODEL,
                        }),
                    });
                    const bootSid = String((created.session as Record<string, unknown>)?.session_id || '');
                    if (!isPersistedAgentSessionId(bootSid)) {
                        throw new Error('Studio Agent could not start a chat session. Try again.');
                    }
                    applySessionPayload((created.session as Record<string, unknown>) || {});
                    setHistory([]);
                }
            } catch (e) {
                if (!cancelled) setError((e as Error).message);
            } finally {
                if (!cancelled) setBooting(false);
            }
        })();
        return () => { cancelled = true; };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    useEffect(() => {
        if (!booting) inputRef.current?.focus();
    }, [booting]);

    useEffect(() => {
        const onOnline = () => {
            setError((prev) => (isNetworkBlip(prev) ? '' : prev));
            if (sessionId) {
                setQueueHint('Back online — syncing your chat…');
                scheduleAutoSync({ delayMs: 400, rehydrateJobs: true });
            }
        };
        window.addEventListener('online', onOnline);
        return () => window.removeEventListener('online', onOnline);
    }, [scheduleAutoSync, sessionId]);

    useEffect(() => {
        let cancelled = false;
        (async () => {
            try {
                const data = await authFetch('/api/studio-agent/render-styles');
                const styles = (data?.styles as RenderStyleOption[]) || [];
                if (!cancelled && styles.length) {
                    setRenderStyleCatalog(
                        styles.map((s) => ({
                            ...s,
                            preview_url: styleAssetUrl(s.preview_url),
                            preview_video_url: styleAssetUrl(s.preview_video_url),
                        })),
                    );
                }
            } catch {
                /* fallback catalog */
            }
        })();
        return () => { cancelled = true; };
    }, [authFetch]);

    const patchSession = useCallback(
        async (patch: {
            model?: string;
            agent_mode?: AgentMode;
            approval_mode?: ApprovalMode;
            content_format?: ContentFormat;
            reasoning_depth?: ReasoningDepth;
            render_style?: string;
            image_model?: string;
            video_model?: string;
            caption_mode?: CaptionMode;
            captions_enabled?: boolean;
            channel_id?: string;
            registry_key?: string;
            channel_title?: string;
            web_search?: boolean;
            animate?: boolean;
            product_website?: string;
        }) => {
            if (!isPersistedAgentSessionId(sessionId)) return;
            try {
                await authFetch(`/api/studio-agent/sessions/${sessionId}`, {
                    method: 'PATCH',
                    body: JSON.stringify(patch),
                });
                if (patch.render_style || patch.image_model || patch.video_model) {
                    await syncSessionFromServer({ quiet: true });
                } else {
                    await refreshHistory();
                }
            } catch (e) {
                setError((e as Error).message);
            }
        },
        [authFetch, refreshHistory, sessionId, syncSessionFromServer],
    );

    useEffect(() => {
        if (canUseLongform) return;
        if (agentMode === 'cliplab') setAgentMode('plan');
        if (contentFormat !== 'short') setContentFormat('short');
    }, [agentMode, canUseLongform, contentFormat]);

    useEffect(() => {
        if (!sessionId || !productWebsite) return;
        void patchSession({ product_website: productWebsite });
    }, [patchSession, productWebsite, sessionId]);

    const selectChannelForChat = useCallback(
        (channel: ChannelRow | null) => {
            setSelectedChannelId(channel?.channel_id || '');
            setSessionChannel(channel);
            const patch = {
                channel_id: channel?.channel_id || '',
                registry_key: channelRegistryKey(channel),
                channel_title: channel?.title || '',
            };
            if (sessionId) {
                void patchSession(patch);
            }
        },
        [patchSession, sessionId],
    );

    const handleChannelsLoaded = useCallback((channels: ChannelRow[]) => {
        setYoutubeChannels(channels);
        const match = channels.find((ch) => channelMatchesSelection(ch, selectedChannelId, sessionChannel));
        if (match) {
            setSelectedChannelId(match.channel_id || '');
            setSessionChannel(match);
            return;
        }
        // A newly connected creator with exactly one channel should never be
        // left in a "connected, but no channel selected" state.  Selecting it
        // here persists the id/registry on the chat so Catalyst can use it on
        // the very next natural-language request.
        if (channels.length === 1) {
            const onlyChannel = channels[0];
            setSelectedChannelId(onlyChannel.channel_id || '');
            setSessionChannel(onlyChannel);
            if (sessionId) {
                void patchSession({
                    channel_id: onlyChannel.channel_id || '',
                    registry_key: channelRegistryKey(onlyChannel),
                    channel_title: onlyChannel.title || '',
                });
            }
        }
    }, [patchSession, selectedChannelId, sessionChannel, sessionId]);

    const buildOutboundMessage = useCallback(
        (text: string) => {
            const trimmed = text.trim();
            const clipLabTurn = agentMode === 'cliplab' || mentionsClipLab(trimmed);
            const ideationTurn = !clipLabTurn && looksLikeIdeation(trimmed);
            const hasImages = attachments.some((f) => f.kind === 'image' && attachmentPayload[f.id]?.data_url);
            const hasVideo = attachments.some((f) => f.kind === 'video' && attachmentPayload[f.id]?.server_path);
            const defaultPrompt = hasImages
                ? 'Please analyze the attached image(s).'
                : hasVideo
                    ? (clipLabTurn
                        ? 'Use the attached video with ClipLab.'
                        : ideationTurn
                            ? 'I attached a reference video for planning context.'
                            : '')
                    : '';
            const parts = [trimmed || defaultPrompt];
            for (const f of attachments) {
                const payload = attachmentPayload[f.id];
                if (!payload) continue;
                if (payload.kind === 'text' && payload.text) {
                    parts.push(`\n\n[Attachment: ${f.name}]\n${payload.text.slice(0, 12000)}`);
                } else if (payload.kind === 'video' && payload.server_path) {
                    if (clipLabTurn) {
                        parts.push(
                            `\n\n[Video attachment ready for ClipLab: ${f.name}]\n`
                            + 'Use ingest_cliplab_attachment, then analyze_cliplab_video for the selected channel and render the strongest clips with upload packages.',
                        );
                    } else {
                        parts.push(
                            `\n\n[Uploaded reference video: ${f.name}]\n`
                            + `local_path: ${payload.server_path}`,
                        );
                    }
                } else if (payload.kind === 'binary') {
                    parts.push(`\n\n[Attachment: ${f.name}]\n[Binary file, ${Math.round(f.size / 1024)}KB. Ask the user for a supported image or text file if visual/text access is required.]`);
                }
            }
            return parts.join('');
        },
        [agentMode, attachmentPayload, attachments],
    );

    const buildOutboundAttachments = useCallback((): AgentChatAttachment[] => {
        return attachments
            .map((f) => attachmentPayload[f.id])
            .filter((payload): payload is AttachmentPayload & { data_url: string } => (
                Boolean(payload && payload.kind === 'image' && payload.data_url)
            ))
            .map((payload) => ({
                name: payload.name,
                mime_type: payload.mime_type,
                size: payload.size,
                data_url: payload.data_url,
            }));
    }, [attachmentPayload, attachments]);

    const sendText = useCallback(
        async (text: string, modeOverride?: AgentMode) => {
            const trimmed = buildOutboundMessage(text);
            if (!trimmed || !chatSessionReady || runningBySession[sessionId!]) return;
            const activeSessionId = sessionId;
            setInput('');
            setAttachments([]);
            setAttachmentPayload({});
            setPending([]);
            setDictationPreview('');
            markSessionRunning(activeSessionId, 'Thinking...');
            setError('');
            setQueueHint('');
            setToolActivity('');
            setActivitySteps([newThinkingStep('Thinking about your request')]);
            activityStepRef.current = 0;
            stickToBottomRef.current = true;
            const readableAttachments = buildOutboundAttachments();
            const visibleUserText = text.trim()
                || (readableAttachments.length ? `Please analyze the attached image${readableAttachments.length === 1 ? '' : 's'}.` : '');
            let effectiveMode = modeOverride || agentMode;
            if (
                effectiveMode === 'plan'
                && (latestUserAllowsProductionPending(visibleUserText) || userAffirmsAssistantTopic(visibleUserText))
            ) {
                effectiveMode = 'studio';
                setAgentMode('studio');
                void patchSession({ agent_mode: 'studio' });
            }
            setMessages((m) => {
                const next = [...m, { role: 'user' as const, content: visibleUserText }];
                messagesRef.current = next;
                return next;
            });
            if (
                effectiveMode === 'plan'
                || isNewProductionRequest(visibleUserText)
                || isResearchOnlyUserText(visibleUserText)
                || latestUserAllowsProductionPending(visibleUserText)
                || userAffirmsAssistantTopic(visibleUserText)
            ) {
                const optimisticBlocked = collectKnownProductionJobIds(
                    messagesRef.current,
                    jobTracks,
                    deliverablesByJobRef.current.keys(),
                );
                if (optimisticBlocked.length) {
                    blockedJobIdsRef.current = [...new Set([
                        ...blockedJobIdsRef.current,
                        ...optimisticBlocked,
                    ])];
                }
                const suppress = (jobId: string, title?: string, status?: string) => (
                    shouldSuppressProductionJob(
                        jobId,
                        title,
                        messagesRef.current,
                        blockedJobIdsRef.current,
                        status,
                    )
                );
                setMessages((rows) => {
                    const stripped = stripStaleProductionArtifacts(
                        rows,
                        (jobId, title) => suppress(jobId, title),
                    );
                    messagesRef.current = stripped.map((row) => {
                        const snap = row.jobDeliverable;
                        if (!snap?.job_id) return row;
                        if (!suppress(snap.job_id, snap.title, snap.status)) return row;
                        const { jobDeliverable: _drop, ...rest } = row;
                        return rest as ChatMessage;
                    });
                    return messagesRef.current;
                });
                setJobTracks((prev) => {
                    const pruned = pruneStaleJobTracks(prev, messagesRef.current, blockedJobIdsRef.current);
                    if (activeSessionId) persistJobs(activeSessionId, pruned);
                    if (!pruned.length) setDockDismissed(true);
                    return pruned;
                });
                setPollResetKey((k) => k + 1);
            }
            let completedCleanly = false;
            let backendRunStillActive = false;
            let latestTurnJobSnapshot: AgentJobSnapshot | undefined;
            let turnRepairJobId = '';
            const thinkingRecover = window.setTimeout(() => {
                if (sessionIdRef.current !== activeSessionId) return;
                void (async () => {
                    try {
                        const refreshed = await authFetch(sessionResumePath(activeSessionId, false), {
                            timeoutMs: 60_000,
                            retries: SESSION_LOAD_RETRIES,
                        });
                        const recoveredSession = (refreshed?.session as Record<string, unknown>) || {};
                        const recoveredRuns = Array.isArray(recoveredSession.active_runs)
                            ? recoveredSession.active_runs as SessionSummary['active_runs']
                            : [];
                        const recoveredRunLabel = activeRunLabel({ active_runs: recoveredRuns });
                        backendRunStillActive = Boolean(recoveredRunLabel);
                        await resumeSession(recoveredSession, {
                            rehydrateJobs: true,
                        });
                        if (recoveredRunLabel) {
                            markSessionRunning(activeSessionId, recoveredRunLabel);
                            setQueueHint(`${recoveredRunLabel} — Studio is still working; no need to resend.`);
                        } else {
                            setQueueHint('Recovered your saved run after a slow connection — no need to resend.');
                            clearSessionRunning(activeSessionId);
                            setActivitySteps((prev) => completeRunningSteps(prev));
                        }
                        setError('');
                    } catch {
                        backendRunStillActive = true;
                        markSessionRunning(activeSessionId, 'Still working...');
                        setQueueHint('Still working server-side — press Sync if this hangs past 2 minutes.');
                    }
                })();
            }, THINKING_RECOVER_MS);
            try {
                const tok = await getToken();
                void ensureStudioFresh(tok);
                const onStreamEvent = (ev: AgentStreamEvent) => {
                    if (sessionIdRef.current !== activeSessionId) return;
                    if (ev.event === 'verification_step' && ev.step) {
                        updateVerificationStep(String(ev.step), {
                            label: String(ev.label || ev.step),
                            detail: String(ev.detail || ''),
                            status: (String(ev.status || 'running') as VerificationStepStatus),
                            required: ev.required !== false,
                        });
                    } else if (ev.event === 'tool_start' && ev.tool) {
                        const toolName = String(ev.tool);
                        if (toolName === 'audit_and_repair_production_scenes') {
                            const requestedJobId = String(ev.args?.job_id || '').trim();
                            const replyJobId = String(replyingTo?.job_id || '').trim();
                            const latestRepairable = [...messagesRef.current]
                                .reverse()
                                .map((row) => row.jobDeliverable)
                                .find((snap) => snap?.kind === 'shortform' && Boolean(snap.scenes?.length));
                            turnRepairJobId = requestedJobId || replyJobId || latestRepairable?.job_id || '';
                            if (turnRepairJobId) {
                                const guardedRepairJobId = turnRepairJobId;
                                const guardExpiresAt = Date.now() + REPAIR_ACTIVE_GUARD_MAX_MS;
                                repairSnapshotGuardRef.current.set(guardedRepairJobId, guardExpiresAt);
                                repairingJobIdRef.current = guardedRepairJobId;
                                repairActiveRunSeenRef.current.delete(guardedRepairJobId);
                                dismissedDockJobIdsRef.current.delete(guardedRepairJobId);
                                clearPolledSnapshot(guardedRepairJobId);
                                setRepairingJobId(guardedRepairJobId);
                                setDockDismissed(false);
                                window.setTimeout(() => {
                                    const currentDeadline = repairSnapshotGuardRef.current.get(guardedRepairJobId);
                                    if (currentDeadline !== guardExpiresAt || currentDeadline > Date.now()) return;
                                    repairSnapshotGuardRef.current.delete(guardedRepairJobId);
                                    repairActiveRunSeenRef.current.delete(guardedRepairJobId);
                                    if (jobIdsMatch(repairingJobIdRef.current, guardedRepairJobId)) {
                                        repairingJobIdRef.current = '';
                                        setRepairingJobId('');
                                    }
                                    if (sessionIdRef.current === activeSessionId) {
                                        setPollResetKey((key) => key + 1);
                                        scheduleAutoSync({ delayMs: 0, rehydrateJobs: true });
                                    }
                                }, REPAIR_ACTIVE_GUARD_MAX_MS + 250);
                            }
                        }
                        // Production starts are only shown if they succeed; blocked research turns stay quiet.
                        if (/^start_(shortform|longform)/i.test(toolName)) {
                            return;
                        }
                        const label = toolActivityLabel(
                            toolName,
                            String(ev.query || ''),
                            String(ev.label || ''),
                        );
                        activityStepRef.current += 1;
                        setToolActivity(
                            ev.awaiting_approval
                                ? `Queued for approval — ${toolLabel(toolName)}`
                                : label,
                        );
                        setActivitySteps((prev) => {
                            // Keep a single running poll/analyze step instead of stacking clones.
                            const sameRunning = prev.find(
                                (s) => s.status === 'running' && s.tool === toolName,
                            );
                            if (sameRunning) {
                                return prev.map((s) =>
                                    s.id === sameRunning.id ? { ...s, label } : s,
                                );
                            }
                            const closed = completeRunningSteps(prev);
                            return [
                                ...closed,
                                {
                                    id: `tool-${toolName}-${Date.now()}-${activityStepRef.current}`,
                                    kind: 'tool' as const,
                                    label: ev.awaiting_approval
                                        ? `Queued for approval — ${toolLabel(toolName)}`
                                        : label,
                                    tool: toolName,
                                    startedAt: Date.now(),
                                    status: 'running' as const,
                                },
                            ];
                        });
                    } else if (ev.event === 'tool_end' && ev.tool) {
                        const toolName = String(ev.tool);
                        const errCode = String(ev.error || '');
                        const skipped =
                            ev.status === 'skipped'
                            || errCode.startsWith('blocked_')
                            || errCode === 'blocked_channel_analysis_turn';
                        if (skipped) {
                            // Drop intentional blocks from the timeline (not user-facing failures).
                            setActivitySteps((prev) =>
                                prev.filter((s) => !(s.tool === toolName && s.status === 'running')),
                            );
                            return;
                        }
                        const failed = ev.status === 'error';
                        const summary = (ev.summary || {}) as AgentToolActivitySummary;
                        const label = toolActivityLabel(
                            toolName,
                            String(ev.query || summary.query || ''),
                            String(ev.label || summary.label || ''),
                        );
                        setToolActivity(
                            failed
                                ? `${label} — error`
                                : `${label} — done`,
                        );
                        setActivitySteps((prev) => {
                            const now = Date.now();
                            let matched = false;
                            const next = prev.map((step) => {
                                if (matched) return step;
                                if (step.status === 'running' && (step.tool === toolName || step.kind === 'tool')) {
                                    matched = true;
                                    const children: ActivityChild[] = [...(step.children || [])];
                                    const q = String(summary.query || ev.query || '').trim();
                                    const count = summary.result_count;
                                    if (q || typeof count === 'number' || summary.title) {
                                        children.push({
                                            id: `child-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
                                            title: String(summary.title || 'Searched'),
                                            query: q || undefined,
                                            resultCount: typeof count === 'number' ? count : undefined,
                                            source: String(summary.source || ''),
                                        });
                                    }
                                    return {
                                        ...step,
                                        label: label || step.label,
                                        status: failed ? ('error' as const) : ('done' as const),
                                        endedAt: now,
                                        children: children.length ? children : step.children,
                                        detail: failed
                                            ? (errCode === 'error' ? 'Deep analysis incomplete — retrying or partial results used' : errCode)
                                            : step.detail,
                                    };
                                }
                                return step;
                            });
                            if (!matched && !failed) {
                                next.push({
                                    id: `tool-end-${toolName}-${Date.now()}`,
                                    kind: 'tool',
                                    label,
                                    tool: toolName,
                                    startedAt: now - 1000,
                                    endedAt: now,
                                    status: 'done',
                                });
                            }
                            return next;
                        });
                    } else if (ev.event === 'status' && ev.message) {
                        const msg = String(ev.message);
                        setToolActivity(msg);
                        if (/thinking/i.test(msg)) {
                            setActivitySteps((prev) => {
                                const hasRunningThink = prev.some(
                                    (s) => s.kind === 'thinking' && s.status === 'running',
                                );
                                if (hasRunningThink) return prev;
                                return [...completeRunningSteps(prev), newThinkingStep('Thinking about your request')];
                            });
                            updateVerificationStep('final_audit', {
                                status: 'running',
                                label: 'Audit final answer before replying',
                                detail: 'Drafting from available context and preparing evidence audit.',
                            });
                        } else if (/pulling|search|demand|live demand/i.test(msg)) {
                            setActivitySteps((prev) => {
                                const closed = completeRunningSteps(prev);
                                return [
                                    ...closed,
                                    {
                                        id: `status-${Date.now()}`,
                                        kind: 'status' as const,
                                        label: msg.replace(/\.\.\.$/, '').slice(0, 120),
                                        startedAt: Date.now(),
                                        status: 'running' as const,
                                    },
                                ];
                            });
                            updateVerificationStep('tool_evidence', {
                                status: 'running',
                                label: 'Run required data tools',
                                detail: msg,
                            });
                        }
                    } else if (ev.event === 'error') {
                        setActivitySteps((prev) =>
                            completeRunningSteps(prev).map((s, i, arr) =>
                                i === arr.length - 1
                                    ? { ...s, status: 'error' as const, detail: String(ev.message || 'error') }
                                    : s,
                            ),
                        );
                        updateVerificationStep('final_audit', {
                            status: 'error',
                            label: 'Audit final answer before replying',
                            detail: String(ev.message || 'Studio Agent stream error'),
                        });
                    } else if (ev.event === 'session_state') {
                        applyProductionLedger(ev as Record<string, unknown>);
                        if (Array.isArray(ev.active_jobs)) {
                            ingestActiveJobs(ev.active_jobs, activeSessionId);
                        }
                    } else if (ev.event === 'active_jobs' && Array.isArray(ev.jobs)) {
                        ingestActiveJobs(ev.jobs, activeSessionId);
                        const jobs = ev.jobs as AgentJobTrack[];
                        const shouldRehydrate = jobs.length > 0;
                        scheduleAutoSync({ delayMs: 400, rehydrateJobs: shouldRehydrate });
                    } else if (ev.event === 'job_snapshot' && ev.snapshot && typeof ev.snapshot === 'object') {
                        const snap = ev.snapshot as AgentJobSnapshot;
                        if (!shouldSuppressProductionJob(
                            snap.job_id,
                            snap.title,
                            messagesRef.current,
                            blockedJobIdsRef.current,
                        )) {
                            latestTurnJobSnapshot = snap;
                            appendJobDeliverable(snap, {
                                source: 'stream',
                                pinToLatest: Boolean(turnRepairJobId && jobIdsMatch(snap.job_id, turnRepairJobId)),
                            });
                        }
                    } else if (ev.event === 'thumbnail_review' && ev.review && typeof ev.review === 'object') {
                        mergeThumbnailReviewIntoDeliverable(ev.review as ThumbnailReview);
                    } else if (ev.event === 'pending_actions' && Array.isArray(ev.actions)) {
                        setPending(filterStalePendingActions(ev.actions as PendingAction[], messagesRef.current));
                        // Do NOT auto-sync immediately — sync was pruning Approve before click.
                    } else if (ev.event === 'concept_plan' && ev.plan && typeof ev.plan === 'object') {
                        setConceptPlan(ev.plan as ConceptPlan);
                    }
                };

                let data: Record<string, unknown>;
                try {
                    // Always push live picker state on every turn so the backend cannot
                    // estimate against stale session defaults.
                    data = await streamAgentChat(sessionId, trimmed, tok, {
                        onEvent: onStreamEvent,
                        replyTo: replyingTo ? { job_id: replyingTo.job_id, kind: replyingTo.kind, scene_index: replyingTo.scene_index } : undefined,
                        attachments: readableAttachments,
                        captions_enabled: captionMode !== 'off',
                        caption_mode: captionMode,
                        render_style: renderStyle,
                        image_model: imageModel,
                        image_model_id: imageModel,
                        video_model: videoModel,
                        agent_mode: effectiveMode,
                        channel: selectedChannel ? {
                            channel_id: selectedChannel.channel_id || '',
                            registry_key: channelRegistryKey(selectedChannel),
                            channel_title: selectedChannel.title || '',
                        } : null,
                    });
                    backendRunStillActive = false;
                } catch (streamError) {
                    const recoverSessionAfterStreamDrop = async () => {
                        try {
                            return await authFetch(sessionResumePath(activeSessionId, false), {
                                timeoutMs: 60_000,
                                retries: SESSION_LOAD_RETRIES,
                            });
                        } catch {
                            return await authFetch(sessionResumePath(activeSessionId, true), {
                                timeoutMs: SESSION_LOAD_TIMEOUT_MS,
                                retries: SESSION_LOAD_RETRIES,
                            });
                        }
                    };
                    try {
                        const refreshed = await recoverSessionAfterStreamDrop();
                        const recoveredSession = (refreshed?.session as Record<string, unknown>) || {};
                        const recoveredRuns = Array.isArray(recoveredSession.active_runs)
                            ? recoveredSession.active_runs as SessionSummary['active_runs']
                            : [];
                        const recoveredRunLabel = activeRunLabel({ active_runs: recoveredRuns });
                        backendRunStillActive = Boolean(recoveredRunLabel);
                        await resumeSession(recoveredSession, {
                            rehydrateJobs: true,
                        });
                        if (recoveredRunLabel) {
                            markSessionRunning(activeSessionId, recoveredRunLabel);
                        }
                    } catch (refreshError) {
                        throw new Error(
                            `Studio Agent connection dropped and the recovery refresh could not reach the backend. Your chat is preserved; press Resume in a few seconds. ${String((streamError as Error).message || (refreshError as Error).message || '')}`,
                        );
                    }
                    // The stream can be interrupted while the server-side run
                    // continues (deploys, proxy rebalancing, mobile network).
                    // We already recovered the authoritative session above, so
                    // do not turn a successful recovery into a red error banner
                    // or force the creator to press Resume manually.
                    setQueueHint(
                        backendRunStillActive
                            ? 'Reconnected — Studio is still working from the saved run.'
                            : 'Reconnected — the saved run is synchronized.',
                    );
                    scheduleAutoSync({ delayMs: 1200, rehydrateJobs: true });
                    completedCleanly = true;
                    return;
                }
                setReplyingTo(null); // clear reply after send
                const q = data?.queue as { waited_sec?: number; queue_position?: number } | undefined;
                if (q && Number(q.waited_sec || 0) > 2) {
                    setQueueHint(
                        `Started after ${Math.round(Number(q.waited_sec))}s in queue`
                        + (q.queue_position ? ` (position ~${q.queue_position})` : ''),
                    );
                } else {
                    setQueueHint('');
                }
                if (sessionIdRef.current !== activeSessionId) {
                    await refreshHistory();
                    return;
                }
                const reply = String(data?.assistant_message || '').trim();
                const nextPending = filterStalePendingActions(
                    (data?.pending_actions as PendingAction[]) || [],
                    messagesRef.current,
                );
                updateVerificationStep('final_audit', {
                    status: 'done',
                    label: 'Audit final answer before replying',
                    detail: reply
                        ? 'Final answer passed the backend evidence gate and was returned.'
                        : nextPending.length > 0
                            ? 'Final response is gated on your approval before paid/writing tools run.'
                            : 'Backend completed without a usable assistant message.',
                });
                if (reply) {
                    setMessages((m) => {
                        const replyDeliverable = latestTurnJobSnapshot;
                        const withoutPriorCard = replyDeliverable
                            ? m.map((row) => {
                                if (!jobIdsMatch(row.jobDeliverable?.job_id, replyDeliverable.job_id)) return row;
                                const { jobDeliverable: _drop, ...rest } = row;
                                return rest as ChatMessage;
                            })
                            : m;
                        const next = [
                            ...withoutPriorCard,
                            {
                                role: 'assistant' as const,
                                content: reply,
                                jobDeliverable: replyDeliverable,
                            },
                        ];
                        messagesRef.current = next;
                        return next;
                    });
                } else if (nextPending.length > 0) {
                    setMessages((m) => [
                        ...m,
                        {
                            role: 'assistant',
                            content:
                                'I prepared the next steps — review and approve the actions below to continue.',
                        },
                    ]);
                } else {
                    setError('Agent returned an empty reply. Try again or pick a different model.');
                }
                setPending(nextPending);
                const nextConcept = (data?.pending_concept || data?.concept_plan) as ConceptPlan | null | undefined;
                if (nextConcept && typeof nextConcept === 'object') {
                    setConceptPlan(nextConcept);
                } else if (nextPending.length === 0 && !nextConcept && data && 'concept_plan' in data && !data.concept_plan) {
                    // keep existing card unless server cleared it
                }
                applyProductionLedger(data);
                ingestActiveJobs(data?.active_jobs, activeSessionId);
                await refreshHistory();
                // If Approve is waiting, skip auto-sync so force_sync cannot wipe the card.
                if (nextPending.length === 0) {
                    scheduleAutoSync({ delayMs: 0, rehydrateJobs: true });
                }
                completedCleanly = Boolean(reply || nextPending.length > 0 || nextConcept);
            } catch (e) {
                if (sessionIdRef.current !== activeSessionId) return;
                updateVerificationStep('final_audit', {
                    status: 'error',
                    label: 'Audit final answer before replying',
                    detail: (e as Error).message,
                });
                setError((e as Error).message);
                setQueueHint('');
            } finally {
                window.clearTimeout(thinkingRecover);
                if (turnRepairJobId && !backendRunStillActive) {
                    repairSnapshotGuardRef.current.set(
                        turnRepairJobId,
                        Date.now() + REPAIR_STALE_SNAPSHOT_GRACE_MS,
                    );
                    repairActiveRunSeenRef.current.delete(turnRepairJobId);
                    if (jobIdsMatch(repairingJobIdRef.current, turnRepairJobId)) {
                        repairingJobIdRef.current = '';
                    }
                    setRepairingJobId((current) => (
                        jobIdsMatch(current, turnRepairJobId) ? '' : current
                    ));
                }
                if (!backendRunStillActive) {
                    clearSessionRunning(activeSessionId);
                }
                if (sessionIdRef.current === activeSessionId) {
                    if (!backendRunStillActive) {
                        setToolActivity('');
                        setActivitySteps((prev) => completeRunningSteps(prev));
                    }
                    // Keep the professional timeline visible briefly after the reply lands.
                    window.setTimeout(() => {
                        if (sessionIdRef.current === activeSessionId) {
                            setActivitySteps([]);
                        }
                    }, 5000);
                    if (completedCleanly) {
                        setVerificationSteps([]);
                        applyPendingStudioBundleReload();
                    }
                }
            }
        },
        [
            authFetch,
            buildOutboundAttachments,
            buildOutboundMessage,
            captionMode,
            clearSessionRunning,
            clearPolledSnapshot,
            getToken,
            ingestActiveJobs,
            markSessionRunning,
            refreshHistory,
            replyingTo,
            resetVerificationChecklist,
            resumeSession,
            chatSessionReady,
            runningBySession,
            scheduleAutoSync,
            agentMode,
            selectedChannel,
            sessionId,
            appendJobDeliverable,
            updateVerificationStep,
        ],
    );

    const sendMessage = useCallback(() => sendText(input), [input, sendText]);

    const commitConceptPlan = useCallback(() => {
        const title = String(conceptPlan?.title || '').trim();
        const dur = Number(conceptPlan?.duration_sec || 0);
        const durationHint = dur > 0 ? `, only ${dur} seconds` : '';
        const msg = title
            ? `yes make it — render that plan for "${title}"${durationHint}`
            : 'yes make it — render that plan';
        // A click is explicit concept approval. Collapse immediately while the
        // production approval/result arrives rather than leaving a giant card.
        setConceptPlan((current) => current ? { ...current, status: 'confirmed' } : current);
        setAgentMode('studio');
        void patchSession({ agent_mode: 'studio' });
        void sendText(msg, 'studio');
    }, [conceptPlan, patchSession, sendText]);

    const onPickFiles = useCallback(async (files: FileList | File[] | null) => {
        if (!files?.length) return;
        const next: AttachedFile[] = [];
        const payload: Record<string, AttachmentPayload> = { ...attachmentPayload };
        let imageCount = attachments.filter((f) => f.kind === 'image').length;
        for (const file of Array.from(files)) {
            const id = `f_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
            const isImage = file.type.startsWith('image/');
            const isVideo = file.type.startsWith('video/')
                || /\.(mp4|mov|mkv|webm|m4v)$/i.test(file.name);
            const isText = file.type.startsWith('text/')
                || /\.(md|txt|json|csv|py|ts|tsx|js|jsx|yaml|yml)$/i.test(file.name);
            const kind: AttachedFile['kind'] = isImage ? 'image' : isVideo ? 'video' : isText ? 'text' : 'binary';
            const name = file.name || 'pasted-image.png';
            if (isImage) {
                if (imageCount >= MAX_AGENT_IMAGE_ATTACHMENTS) {
                    setError(`Studio Agent can read up to ${MAX_AGENT_IMAGE_ATTACHMENTS} images per message.`);
                    continue;
                }
                if (file.size > MAX_AGENT_IMAGE_BYTES) {
                    setError(`${name} is too large. Keep image attachments under 8MB.`);
                    continue;
                }
                payload[id] = {
                    name,
                    mime_type: file.type || 'image/png',
                    size: file.size,
                    kind,
                    data_url: await readFileAsDataUrl(file),
                };
                imageCount += 1;
            } else if (isText) {
                payload[id] = {
                    name,
                    mime_type: file.type || 'text/plain',
                    size: file.size,
                    kind,
                    text: await file.text(),
                };
            } else if (isVideo) {
                if (!isPersistedAgentSessionId(sessionId)) {
                    setError('Open or create a Studio Agent chat before attaching a video.');
                    continue;
                }
                if (file.size > MAX_AGENT_VIDEO_BYTES) {
                    setError(`${name} is too large. Keep Studio Agent video attachments under 3GB for now.`);
                    continue;
                }
                const tok = await getToken();
                const form = new FormData();
                form.append('file', file);
                const res = await fetch(resolveStudioBackendUrl(`/api/studio-agent/sessions/${sessionId}/attachments/video`), {
                    method: 'POST',
                    headers: { Authorization: `Bearer ${tok}` },
                    body: form,
                });
                const data = await res.json().catch(() => ({}));
                if (!res.ok) {
                    setError(typeof data.detail === 'string' ? data.detail : `Video upload failed (${res.status})`);
                    continue;
                }
                payload[id] = {
                    name,
                    mime_type: file.type || 'video/mp4',
                    size: file.size,
                    kind,
                    server_path: String(data.path || ''),
                };
            } else {
                payload[id] = {
                    name,
                    mime_type: file.type || 'application/octet-stream',
                    size: file.size,
                    kind,
                };
            }
            next.push({ id, name, size: file.size, mimeType: file.type, kind });
        }
        setAttachments((a) => [...a, ...next]);
        setAttachmentPayload(payload);
        if (fileInputRef.current) fileInputRef.current.value = '';
    }, [attachmentPayload, attachments, getToken, sessionId]);

    const onPasteIntoInput = useCallback((e: ClipboardEvent<HTMLTextAreaElement>) => {
        const clipboard = e.clipboardData;
        const fromFiles = Array.from(clipboard?.files || []).filter((file) => file.type.startsWith('image/'));
        const fromItems = Array.from(clipboard?.items || [])
            .filter((item) => item.kind === 'file' && item.type.startsWith('image/'))
            .map((item, idx) => {
                const file = item.getAsFile();
                if (!file) return null;
                const ext = item.type.includes('jpeg') || item.type.includes('jpg') ? 'jpg' : 'png';
                return new File([file], file.name || `pasted-screenshot-${Date.now()}-${idx}.${ext}`, {
                    type: file.type || item.type || 'image/png',
                    lastModified: Date.now(),
                });
            })
            .filter((file): file is File => Boolean(file));
        const files = [...fromFiles, ...fromItems].filter((file, idx, all) => (
            idx === all.findIndex((candidate) => (
                candidate.name === file.name
                && candidate.size === file.size
                && candidate.type === file.type
            ))
        ));
        if (!files.length) return;
        e.preventDefault();
        void onPickFiles(files);
    }, [onPickFiles]);

    const approveAction = useCallback(
        async (actionId: string) => {
            if (!sessionId || currentSessionRunning) return;
            const approved = pending.find((a) => a.id === actionId);
            const approvedIndex = pending.findIndex((a) => a.id === actionId);
            if (
                approved
                && isStaleShortformPendingAction(approved, messagesRef.current, pending, approvedIndex)
            ) {
                setPending((p) => p.filter((a) => a.id !== actionId));
                setError('Blocked stale production approval. Sync chat, then approve the current title.');
                return;
            }
            markSessionRunning(sessionId, 'Approving action...');
            setError('');
            stickToBottomRef.current = true;
            try {
                // Do NOT sync-before-approve. force_sync was pruning the pending action
                // (or racing it) so Approve never reached start_shortform_generate.
                const data = await authFetch(`/api/studio-agent/sessions/${sessionId}/approve`, {
                    method: 'POST',
                    body: JSON.stringify({ action_id: actionId }),
                    timeoutMs: 120_000,
                });
                setPending((p) => p.filter((a) => a.id !== actionId));
                const approvedAction = data?.approved_action as {
                    tool?: string;
                    error?: string;
                    result_preview?: string;
                } | undefined;
                if (approvedAction?.error) {
                    setError(`${approvedAction.tool || 'Action'} failed: ${approvedAction.error}`);
                }
                const reply = String(data?.assistant_message || '').trim();
                if (reply) {
                    setMessages((m) => {
                        const next = [...m, { role: 'assistant' as const, content: reply }];
                        messagesRef.current = next;
                        return next;
                    });
                } else if (approvedAction?.error) {
                    setMessages((m) => [
                        ...m,
                        {
                            role: 'assistant',
                            content: `Could not run ${approvedAction.tool || 'action'}: ${approvedAction.error}`,
                        },
                    ]);
                } else if (Array.isArray(data?.active_jobs) && (data.active_jobs as unknown[]).length > 0) {
                    setMessages((m) => [
                        ...m,
                        {
                            role: 'assistant',
                            content: `Started ${approved?.tool || 'production'} — watch the render dock for live progress.`,
                        },
                    ]);
                }
                setPending(filterStalePendingActions(
                    (data?.pending_actions as PendingAction[]) || [],
                    messagesRef.current,
                ));
                ingestActiveJobs(data?.active_jobs, sessionId);
                setDockDismissed(false);
                // Refresh AFTER start so the new job is on the session — not before.
                scheduleAutoSync({ delayMs: 400, rehydrateJobs: true });
            } catch (e) {
                setError((e as Error).message);
                if (
                    approved
                    && !isStaleShortformPendingAction(approved, messagesRef.current, pending, approvedIndex)
                ) {
                    setPending((p) => filterStalePendingActions([...p, approved], messagesRef.current));
                }
            } finally {
                clearSessionRunning(sessionId);
            }
        },
        [authFetch, clearSessionRunning, currentSessionRunning, ingestActiveJobs, markSessionRunning, pending, scheduleAutoSync, sessionId],
    );

    const rejectAction = useCallback(
        async (actionId: string) => {
            if (!sessionId || currentSessionRunning) return;
            markSessionRunning(sessionId, 'Rejecting action...');
            try {
                await authFetch(`/api/studio-agent/sessions/${sessionId}/reject`, {
                    method: 'POST',
                    body: JSON.stringify({ action_id: actionId, reason: 'Rejected by user' }),
                });
                setPending((p) => p.filter((a) => a.id !== actionId));
                dropGhostShortformTrack();
                setMessages((rows) => stripGhostJobDeliverables(rows));
                setError('');
                scheduleAutoSync({ delayMs: 0, rehydrateJobs: false });
            } catch (e) {
                setError((e as Error).message);
                scheduleAutoSync({ delayMs: 0 });
            } finally {
                clearSessionRunning(sessionId);
            }
        },
        [authFetch, clearSessionRunning, currentSessionRunning, dropGhostShortformTrack, markSessionRunning, scheduleAutoSync, sessionId],
    );

    const handleRetryProduction = useCallback(async () => {
        if (!sessionId || retryingProduction) return;
        if (currentSessionRunning && dockSnap?.status === 'running') return;
        setRetryingProduction(true);
        setError('');
        setToolActivity('Retrying production…');
        try {
            const data = (await authFetch(`/api/studio-agent/sessions/${sessionId}/retry-production`, {
                method: 'POST',
                timeoutMs: 120000,
            })) as { active_jobs?: unknown[]; assistant_message?: string };
            const list = Array.isArray(data?.active_jobs) ? (data.active_jobs as AgentJobTrack[]) : [];
            const normalized = list
                .map((j) => ({
                    job_id: String(j.job_id || ''),
                    kind: (j.kind || 'shortform') as AgentJobTrack['kind'],
                    title: String(j.title || ''),
                    started_at: Number(j.started_at || Date.now()),
                }))
                .filter((j) => j.job_id);
            if (!normalized.length) {
                throw new Error('Retry did not start a new job — ask the agent to run start_shortform_generate again.');
            }
            setJobTracks(normalized);
            if (sessionId) persistJobs(sessionId, normalized);
            setPollResetKey((k) => k + 1);
            setDockDismissed(false);
            if (data?.assistant_message) {
                setMessages((m) => [
                    ...m,
                    { role: 'assistant' as const, content: String(data.assistant_message) },
                ]);
            }
        } catch (e) {
            setError((e as Error).message);
        } finally {
            setRetryingProduction(false);
            setToolActivity('');
        }
    }, [authFetch, currentSessionRunning, dockSnap?.status, retryingProduction, sessionId]);

    const handlePrepareSceneRepair = useCallback((snapshot: AgentJobSnapshot) => {
        const repairSceneNumbers = (snapshot.scenes || [])
            .filter((scene) => {
                const status = String(scene.status || '').toLowerCase();
                return /fail|error|blocked|missing/.test(status)
                    || scene.still_qa?.pass === false
                    || (scene.animate === true && scene.has_clip !== true);
            })
            .map((scene) => Number(scene.index) + 1)
            .filter((sceneNumber) => Number.isInteger(sceneNumber) && sceneNumber > 0);
        const totalScenes = Math.max(
            1,
            Number(snapshot.total_scenes || snapshot.scenes?.length || 1),
        );
        const explicitScope = repairSceneNumbers.length
            ? `Scenes ${repairSceneNumbers.join(', ')}`
            : `Scenes 1 through ${totalScenes}`;
        setReplyingTo(snapshot);
        setAgentMode('studio');
        setInput(
            `Audit and repair ${explicitScope} in this existing video. Preserve every passing scene and approved asset, `
            + 'regenerate only what fails script, prompt, continuity, or artifact QA, then reanimate only the scenes whose still changed.',
        );
        setDockDismissed(true);
        window.setTimeout(() => inputRef.current?.focus(), 0);
    }, []);

    const handleCancelProduction = useCallback(async (jobId?: string, kind: string = 'shortform') => {
        const targetId = jobId || dockTrack?.job_id;
        const targetKind = jobId ? kind : (dockTrack?.kind || 'shortform');
        if (!targetId || cancellingProduction) return;
        userCancelledJobsRef.current.add(targetId);
        setCancellingProduction(true);
        try {
            const tok = await getToken();
            await cancelJob(targetId, targetKind, tok, sessionId);
            setJobTracks((prev) => {
                const merged = mergeJobTracks(prev, [{
                    job_id: targetId,
                    kind: normalizeAgentJobKind(targetId, targetKind),
                    started_at: Date.now(),
                }]);
                if (sessionId) persistJobs(sessionId, merged);
                return merged;
            });
            const pollCancel = async (attempt = 0): Promise<void> => {
                if (!sessionId || attempt > 12) return;
                const pollTok = await getToken();
                const res = await fetch(agentJobPollUrl(targetId, targetKind as AgentJobTrack['kind'], sessionId), {
                    headers: { Authorization: `Bearer ${pollTok}` },
                });
                const data = (await res.json().catch(() => ({}))) as AgentJobSnapshot;
                if (!res.ok) return;
                appendJobDeliverable(data);
                if (!isTerminalJob(data)) {
                    await new Promise((resolve) => window.setTimeout(resolve, 2000));
                    return pollCancel(attempt + 1);
                }
            };
            await pollCancel();
            setMessages((m) => [
                ...m,
                { role: 'assistant' as const, content: 'Cancelling the render — it will stop at the next scene. No further provider spend.' },
            ]);
        } catch (e) {
            setError((e as Error).message);
        } finally {
            setCancellingProduction(false);
        }
    }, [appendJobDeliverable, cancellingProduction, dockTrack, getToken, sessionId]);

    const visiblePending = filterOwnerOnlyPending(
        filterStalePendingActions(pending, messages),
        isAdminUser,
    );

    if (booting) {
        return (
            <div className="flex flex-1 items-center justify-center gap-2 text-sm text-gray-400">
                <Loader2 className="h-4 w-4 animate-spin" /> Starting Studio Agent…
            </div>
        );
    }

    return (
        <>
            <AgentModelPicker
                open={modelPickerOpen}
                models={modelCatalog}
                selectedId={model}
                title="Choose a runner model"
                subtitle="Used for planning, tool calls, and production orchestration. Claude via Anthropic · Grok via xAI."
                searchPlaceholder="Search Claude, Grok, providers, or costs..."
                onSelect={(id) => {
                    setModel(id);
                    patchSession({ model: id });
                }}
                onClose={() => setModelPickerOpen(false)}
            />
            <AgentModelPicker
                open={imageModelPickerOpen}
                models={imageModelCatalog}
                selectedId={imageModel}
                title="Choose an image model"
                subtitle="Used for stills, scene images, and thumbnails on short-form and long-form renders. Your pick is saved on this chat and sent on every turn."
                statusText={`${imageModelCatalog.length} image models available through Studio providers`}
                searchPlaceholder="Search image models, providers, or costs..."
                onSelect={(id) => {
                    setImageModel(id);
                    patchSession({ image_model: id });
                    saveImageModelPref(id);
                }}
                onClose={() => setImageModelPickerOpen(false)}
            />
            <AgentModelPicker
                open={videoModelPickerOpen}
                models={videoModelCatalog}
                selectedId={videoModel}
                title="Choose an image-to-video model"
                subtitle="Used when approved stills animate into motion clips (short-form and long-form). Your pick is saved on this chat."
                statusText={`${videoModelCatalog.length} video models available through Studio providers`}
                searchPlaceholder="Search video models, providers, or costs..."
                onSelect={(id) => {
                    setVideoModel(id);
                    patchSession({ video_model: id });
                }}
                onClose={() => setVideoModelPickerOpen(false)}
            />
            {deleteTarget && (
                <div className="fixed inset-0 z-[80] flex items-center justify-center bg-black/65 px-4 backdrop-blur-sm">
                    <button
                        type="button"
                        className="absolute inset-0"
                        aria-label="Close delete confirmation"
                        onClick={() => setDeleteTarget(null)}
                    />
                    <div className="relative w-full max-w-sm rounded-2xl border border-rose-500/25 bg-[#111116] p-5 shadow-2xl shadow-black/60">
                        <div className="flex items-start gap-3">
                            <div className="grid h-10 w-10 shrink-0 place-items-center rounded-xl bg-rose-500/10 text-rose-200">
                                <Trash2 className="h-4 w-4" />
                            </div>
                            <div className="min-w-0">
                                <h2 className="text-base font-semibold text-white">Delete chat?</h2>
                                <p className="mt-1 line-clamp-2 text-sm text-gray-400">
                                    {deleteTarget.title || 'New chat'} will be removed from Studio Agent history.
                                </p>
                            </div>
                        </div>
                        <div className="mt-5 flex justify-end gap-2">
                            <button
                                type="button"
                                onClick={() => setDeleteTarget(null)}
                                className="rounded-xl border border-white/10 px-4 py-2 text-sm font-semibold text-gray-300 transition hover:bg-white/[0.06] hover:text-white"
                            >
                                Keep
                            </button>
                            <button
                                type="button"
                                disabled={currentSessionRunning}
                                onClick={() => void confirmDeleteSession(deleteTarget.session_id)}
                                className="rounded-xl bg-rose-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-rose-500 disabled:opacity-50"
                            >
                                Delete
                            </button>
                        </div>
                    </div>
                </div>
            )}

            <div className="relative flex min-h-0 flex-1 overflow-hidden bg-black">
                {historyOpen && (
                    <button
                        type="button"
                        aria-label="Close chat history"
                        className="absolute inset-0 z-30 bg-black/60 sm:hidden"
                        onClick={() => setHistoryOpen(false)}
                    />
                )}
                <aside
                    className={`shrink-0 flex-col border-r border-white/[0.07] bg-[#050505] transition-all duration-200 ${
                        historyOpen
                            ? 'absolute inset-y-0 left-0 z-40 flex w-[min(244px,calc(100vw-3rem))] sm:relative sm:z-auto sm:w-[244px]'
                            : 'hidden sm:flex sm:w-[56px]'
                    }`}
                >
                    <div className={`border-b border-white/[0.06] p-2.5 ${historyOpen ? 'block' : 'hidden'}`}>
                        <div className="mb-2 flex items-center justify-between px-1">
                            <span className="text-sm font-semibold text-white">Studio</span>
                            <button
                                type="button"
                                onClick={() => setHistoryOpen(false)}
                                className="grid h-8 w-8 place-items-center rounded-lg text-gray-500 transition hover:bg-white/[0.07] hover:text-white"
                                title="Collapse sidebar"
                            >
                                <ChevronsLeft className="h-4 w-4" />
                            </button>
                        </div>
                        <button
                            type="button"
                            disabled={creatingSession}
                            onClick={() => void createNewSession(model)}
                            className="mb-2 flex h-10 w-full items-center gap-2 rounded-xl bg-white/[0.08] px-3 text-sm font-semibold text-white transition hover:bg-white/[0.12] disabled:opacity-40"
                        >
                            {creatingSession ? <Loader2 className="h-4 w-4 animate-spin" /> : <MessageSquarePlus className="h-4 w-4" />}
                            {creatingSession ? 'Starting chat…' : 'New chat'}
                        </button>
                        <label className="flex h-9 items-center gap-2 rounded-xl border border-white/[0.06] bg-black/30 px-3 text-gray-500 focus-within:border-white/15 focus-within:text-gray-300">
                            <Search className="h-3.5 w-3.5" />
                            <input
                                value={historyQuery}
                                onChange={(event) => setHistoryQuery(event.target.value)}
                                placeholder="Search chats"
                                className="min-w-0 flex-1 bg-transparent text-xs text-white outline-none placeholder:text-gray-600"
                            />
                        </label>
                    </div>
                    <div className={`min-h-0 flex-1 overflow-y-auto p-1.5 ${historyOpen ? '' : 'hidden'}`}>
                        {history.length === 0 && (
                            <p className="px-2 py-3 text-[10px] text-gray-600">No chats yet — start one.</p>
                        )}
                        {filteredHistory.map((s) => {
                            const active = s.session_id === sessionId;
                            const runningLabel = runningBySession[s.session_id];
                            return (
                                <div
                                    key={s.session_id}
                                    className={`group mb-1 flex items-stretch gap-0.5 rounded-xl transition ${
                                        active ? 'bg-white/[0.09]' : 'hover:bg-white/[0.05]'
                                    }`}
                                >
                                    <button
                                        type="button"
                                        onClick={() => openSession(s.session_id)}
                                        className={`min-w-0 flex-1 rounded-lg px-2.5 py-2 text-left ${
                                            active ? 'text-white' : 'text-gray-300'
                                        }`}
                                    >
                                        <p className="line-clamp-2 text-xs font-medium leading-snug">
                                            {s.title || 'New chat'}
                                        </p>
                                        <p className="mt-0.5 text-[9px] text-gray-500">
                                            {runningLabel ? 'Running' : formatSessionAge(s.updated_at)}
                                            {(s.pending_count || 0) > 0
                                                ? ` · ${s.pending_count} pending`
                                                : ''}
                                        </p>
                                        {s.channel_title && (
                                            <p className="mt-0.5 truncate text-[9px] text-cyan-300/70">
                                                {s.channel_title}
                                            </p>
                                        )}
                                    </button>
                                    <button
                                        type="button"
                                        title="New chat with prior context (no old renders)"
                                        onClick={() => void forkSessionWithContext(s.session_id)}
                                        className="shrink-0 rounded-lg px-1.5 py-2 text-gray-500 opacity-0 transition hover:bg-violet-500/15 hover:text-violet-200 group-hover:opacity-100"
                                    >
                                        <RotateCcw className="h-3.5 w-3.5" />
                                    </button>
                                    <button
                                        type="button"
                                        title="Delete chat"
                                        onClick={() => setDeleteTarget(s)}
                                        className="shrink-0 rounded-lg px-1.5 py-2 text-gray-500 opacity-70 transition hover:bg-rose-500/15 hover:text-rose-300 group-hover:opacity-100"
                                    >
                                        <Trash2 className="h-3.5 w-3.5" />
                                    </button>
                                </div>
                            );
                        })}
                    </div>
                    {!historyOpen && (
                        <div className="flex h-full flex-col items-center gap-2 py-2">
                            <button
                                type="button"
                                onClick={() => setHistoryOpen(true)}
                                className="grid h-10 w-10 place-items-center rounded-xl bg-white/[0.08] text-gray-200 transition hover:bg-white/[0.12] hover:text-white"
                                title="Expand"
                            >
                                <ChevronsRight className="h-4 w-4" />
                            </button>
                            <button
                                type="button"
                                title="New chat"
                                disabled={creatingSession}
                                onClick={() => void createNewSession(model)}
                                className="grid h-10 w-10 place-items-center rounded-xl text-gray-400 transition hover:bg-violet-500/15 hover:text-violet-200 disabled:opacity-40"
                            >
                                {creatingSession ? <Loader2 className="h-4 w-4 animate-spin" /> : <MessageSquarePlus className="h-4 w-4" />}
                            </button>
                            <button
                                type="button"
                                title="History"
                                onClick={() => setHistoryOpen(true)}
                                className="grid h-10 w-10 place-items-center rounded-xl text-gray-400 transition hover:bg-white/[0.08] hover:text-white"
                            >
                                <History className="h-4 w-4" />
                            </button>
                        </div>
                    )}
                </aside>

            <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
                <header className="flex min-h-14 shrink-0 flex-wrap items-center gap-2 border-b border-white/[0.07] px-3 py-2 sm:px-5">
                    {onBack && (
                        <button
                            type="button"
                            onClick={onBack}
                            className="inline-flex items-center gap-1.5 rounded-lg px-2 py-1.5 text-xs text-gray-400 transition hover:bg-white/[0.06] hover:text-white"
                        >
                            <ArrowLeft className="h-3.5 w-3.5" />
                            Studio
                        </button>
                    )}
                    <button
                        type="button"
                        onClick={() => setHistoryOpen((o) => !o)}
                        className="rounded-lg p-1.5 text-gray-400 transition hover:bg-white/[0.06] hover:text-white"
                        title={historyOpen ? 'Hide history' : 'Show history'}
                    >
                        <History className="h-4 w-4" />
                    </button>
                    <div className="flex items-center gap-2">
                        {/* Clean branding — no literal robot logo per user request */}
                        <h1 className="text-sm font-semibold text-white">Studio Agent</h1>
                        <span className="hidden text-xs text-gray-600 sm:inline">Create, edit, and ship from one conversation</span>
                    </div>
                    <div className="ml-auto flex flex-wrap items-center gap-2">
                        <button
                            type="button"
                            title={sessionId
                                ? 'Sync this chat and pull the latest Studio update (auto hard-refresh when a new build is live).'
                                : 'Reopen the last Studio Agent chat from the server.'}
                            disabled={resuming || syncing || booting}
                            onClick={() => {
                                if (sessionId) void syncSessionFromServer();
                                else void reloadCurrentSession();
                            }}
                            className="inline-flex items-center gap-1 rounded-lg border border-white/[0.06] px-2 py-1 text-[9px] font-semibold uppercase text-gray-400 transition hover:bg-white/[0.06] hover:text-white disabled:opacity-40"
                        >
                            <RefreshCw className={`h-3 w-3 ${syncing || resuming ? 'animate-spin' : ''}`} />
                            {syncing ? 'Syncing…' : resuming ? 'Loading…' : sessionId ? 'Sync' : 'Reload'}
                        </button>
                        <button
                            type="button"
                            title="Fresh chat with prior context — no old renders"
                            disabled={currentSessionRunning || !chatSessionReady}
                            onClick={() => void forkSessionWithContext()}
                            className="inline-flex items-center gap-1 rounded-lg border border-white/[0.06] px-2 py-1 text-[9px] font-semibold uppercase text-gray-400 transition hover:bg-violet-500/15 hover:text-violet-200 disabled:opacity-40"
                        >
                            <RotateCcw className="h-3 w-3" />
                            With context
                        </button>
                        <button
                            type="button"
                            title="Clear stuck renders and approve cards for this chat"
                            disabled={currentSessionRunning || !chatSessionReady || !sessionId}
                            onClick={() => void resetProductionInPlace()}
                            className="inline-flex items-center gap-1 rounded-lg border border-amber-500/25 px-2 py-1 text-[9px] font-semibold uppercase text-amber-200/90 transition hover:bg-amber-500/10 disabled:opacity-40"
                        >
                            <RotateCcw className="h-3 w-3" />
                            Reset prod
                        </button>
                        <button
                            type="button"
                            title="Roll transcript into a new session"
                            disabled={currentSessionRunning || !chatSessionReady}
                            onClick={() => void rolloverSession()}
                            className="inline-flex items-center gap-1 rounded-lg border border-white/[0.06] px-2 py-1 text-[9px] font-semibold uppercase text-gray-400 transition hover:bg-white/[0.06] hover:text-white disabled:opacity-40"
                        >
                            <History className="h-3 w-3" />
                            Roll over
                        </button>
                        {/* Workflow mode lives in the composer so this header stays focused on session controls. */}
                        <div
                            className="flex items-center gap-0.5 rounded-lg border border-white/[0.06] bg-white/[0.02] p-0.5"
                            title="How deeply Claude reasons"
                        >
                            {REASONING_OPTIONS.map((opt) => (
                                <button
                                    key={opt.id}
                                    type="button"
                                    title={opt.hint}
                                    onClick={() => {
                                        setReasoningDepth(opt.id);
                                        patchSession({ reasoning_depth: opt.id });
                                    }}
                                    className={`inline-flex items-center gap-1 rounded-md px-2 py-0.5 text-[9px] font-semibold uppercase transition ${
                                        reasoningDepth === opt.id
                                            ? 'bg-sky-600/25 text-sky-200'
                                            : 'text-gray-500 hover:text-gray-300'
                                    }`}
                                >
                                    {opt.id === 'fast' && <Zap className="h-2.5 w-2.5" />}
                                    {opt.id === 'deep' && <Brain className="h-2.5 w-2.5" />}
                                    {opt.label}
                                </button>
                            ))}
                        </div>
                        {agentMode === 'studio' && (
                            <>
                        {/* Visual Style Grid - Seedream stills + on-demand i2v motion previews */}
                        <div className="relative">
                            <button
                                type="button"
                                onClick={() => {
                                    setShowStyleGrid(!showStyleGrid);
                                    if (showStyleGrid) setActiveStylePreview('');
                                }}
                                className="flex items-center gap-1.5 rounded-lg border border-white/[0.06] bg-white/[0.02] px-2 py-0.5 text-[9px] font-semibold uppercase text-violet-200 hover:bg-white/5"
                                title="Art style previews"
                            >
                                <Palette className="h-3 w-3 text-violet-300" />
                                {renderStyleCatalog.find(s => s.key === renderStyle)?.label || renderStyle}
                            </button>
                            {showStyleGrid && (
                                <div className="absolute right-0 z-[60] mt-2 w-[640px] max-h-[560px] overflow-auto rounded-2xl border border-white/10 bg-[#07070a]/95 p-3 text-xs shadow-[0_24px_80px_rgba(0,0,0,0.72)] backdrop-blur-xl">
                                    <div className="mb-3 flex items-center justify-between gap-3 border-b border-white/[0.06] pb-2">
                                        <div>
                                            <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-violet-300">Choose art style</div>
                                            <div className="mt-0.5 text-[10px] text-white/40">Seedream 4.5 stills; hover a card for its separate motion preview.</div>
                                        </div>
                                        <button
                                            type="button"
                                            onClick={() => {
                                                setShowStyleGrid(false);
                                                setActiveStylePreview('');
                                            }}
                                            className="rounded-lg border border-white/10 px-2 py-1 text-[10px] font-semibold text-white/50 transition hover:bg-white/[0.06] hover:text-white"
                                        >
                                            Close
                                        </button>
                                    </div>
                                    {['Realism', 'Comic', 'Animation', 'Specialty', 'Niche'].map(group => {
                                        const items = renderStyleCatalog.filter(s => s.group === group);
                                        if (!items.length) return null;
                                        return (
                                            <div key={group} className="mb-4 last:mb-0">
                                                <div className="mb-2 flex items-center gap-2">
                                                    <div className="text-[9px] font-semibold uppercase tracking-[0.18em] text-white/45">{group}</div>
                                                    <div className="h-px flex-1 bg-white/[0.06]" />
                                                </div>
                                                <div className="grid grid-cols-4 gap-2.5">
                                                    {items.map(s => {
                                                        const isSelected = renderStyle === s.key;
                                                        const isActive = activeStylePreview === s.key;
                                                        const stillPreviewUrl = styleStillUrlByKey.get(s.key) || '';
                                                        const videoPreviewUrl = isActive ? activeStyleVideoMedia.url : '';
                                                        const showVideo = Boolean(videoPreviewUrl);
                                                        return (
                                                            <button
                                                                key={s.key}
                                                                type="button"
                                                                onMouseEnter={() => setActiveStylePreview(s.key)}
                                                                onFocus={() => setActiveStylePreview(s.key)}
                                                                onMouseLeave={() => setActiveStylePreview('')}
                                                                onClick={() => {
                                                                    setRenderStyle(s.key);
                                                                    if (s.key === 'skeleton_host') {
                                                                        setImageModel('grok_imagine');
                                                                        setVideoModel('grok_imagine_video');
                                                                        void patchSession({
                                                                            render_style: s.key,
                                                                            image_model: 'grok_imagine',
                                                                            video_model: 'grok_imagine_video',
                                                                        });
                                                                    } else {
                                                                        void patchSession({ render_style: s.key });
                                                                    }
                                                                    setShowStyleGrid(false);
                                                                    setActiveStylePreview('');
                                                                }}
                                                                className={`group overflow-hidden rounded-xl border bg-white/[0.025] text-left transition ${
                                                                    isSelected
                                                                        ? 'border-violet-400 shadow-[0_0_0_1px_rgba(167,139,250,0.35),0_14px_42px_rgba(109,40,217,0.22)]'
                                                                        : 'border-white/[0.08] hover:border-white/25 hover:bg-white/[0.045]'
                                                                }`}
                                                            >
                                                                <div className="relative aspect-[9/12] overflow-hidden bg-[#050507]">
                                                                    {showVideo ? (
                                                                        <video
                                                                            key={s.key}
                                                                            src={videoPreviewUrl}
                                                                            poster={stillPreviewUrl || undefined}
                                                                            className="h-full w-full object-cover"
                                                                            autoPlay
                                                                            muted
                                                                            loop
                                                                            playsInline
                                                                            preload="none"
                                                                        />
                                                                    ) : stillPreviewUrl ? (
                                                                        <img
                                                                            src={stillPreviewUrl}
                                                                            alt={s.label}
                                                                            className="h-full w-full object-cover transition duration-300 group-hover:scale-[1.035]"
                                                                            loading="lazy"
                                                                        />
                                                                    ) : (
                                                                        <div className="h-full w-full bg-white/[0.04]" />
                                                                    )}
                                                                    <div className="absolute inset-x-0 bottom-0 bg-gradient-to-t from-black/80 via-black/15 to-transparent px-2 pb-2 pt-8">
                                                                        <div className="truncate text-[10px] font-semibold text-white">{s.label}</div>
                                                                    </div>
                                                                    <div className="absolute left-2 top-2 rounded-full border border-white/10 bg-black/55 px-1.5 py-0.5 text-[8px] font-semibold uppercase tracking-wide text-white/70">
                                                                        {showVideo ? 'Motion' : 'Still'}
                                                                    </div>
                                                                </div>
                                                                <div className="min-h-[44px] px-2 py-1.5 leading-tight">
                                                                    {s.description ? (
                                                                        <div className="line-clamp-2 text-[9px] leading-[12px] text-white/45">{s.description}</div>
                                                                    ) : null}
                                                                </div>
                                                            </button>
                                                        );
                                                    })}
                                                </div>
                                            </div>
                                        );
                                    })}
                                </div>
                            )}
                        </div>
                        <label
                            className="hidden"
                            title="Image-to-video model. Stills remain locked to Seedream 4.5."
                        >
                            <Video className="h-3 w-3 text-cyan-300" />
                            <select
                                value={videoModel}
                                onChange={(event) => {
                                    const next = normalizeVideoModel(event.target.value);
                                    setVideoModel(next);
                                    void patchSession({ video_model: next });
                                }}
                                className="max-w-[120px] bg-transparent text-[9px] font-semibold uppercase text-cyan-100 outline-none"
                            >
                                {VIDEO_MODEL_OPTIONS.map((opt) => (
                                    <option key={opt.id} value={opt.id} className="bg-black text-white">
                                        {opt.label} · {opt.price}
                                    </option>
                                ))}
                            </select>
                        </label>
                            </>
                        )}
                        <button
                            type="button"
                            onClick={() => {
                                const next: CaptionMode = captionMode === 'off' ? 'word' : 'off';
                                setCaptionMode(next);
                                void patchSession({
                                    caption_mode: next,
                                    captions_enabled: next !== 'off',
                                });
                            }}
                            className={`inline-flex items-center gap-1.5 rounded-lg border px-2 py-1 text-[9px] font-semibold uppercase transition ${
                                captionMode === 'off'
                                    ? 'border-white/[0.06] bg-white/[0.02] text-gray-400 hover:bg-white/[0.06] hover:text-white'
                                    : 'border-cyan-500/35 bg-cyan-500/10 text-cyan-100'
                            }`}
                            title={captionMode === 'off' ? 'Captions are disabled for new renders' : 'One-word captions are enabled for new renders'}
                        >
                            CC {captionMode === 'off' ? 'Off' : 'Word'}
                        </button>
                        <div className="relative">
                            <button
                                type="button"
                                onClick={() => setChannelsOpen((o) => !o)}
                                className={`inline-flex items-center gap-1.5 rounded-lg border px-2 py-1 text-[9px] font-semibold uppercase transition ${
                                    channelsOpen
                                        ? 'border-violet-500/40 bg-violet-500/10 text-violet-200'
                                        : 'border-white/[0.06] bg-white/[0.02] text-gray-400 hover:bg-white/[0.06] hover:text-white'
                                }`}
                                title="Connected YouTube channels"
                            >
                                <Users className="h-3 w-3" />
                                {selectedChannel?.title || 'Select channel'}
                            </button>
                            {channelsOpen && (
                                <div className="absolute right-0 top-[calc(100%+8px)] z-50 w-[min(92vw,420px)] rounded-2xl border border-white/10 bg-[#08080b]/95 p-3 shadow-2xl shadow-black/50 backdrop-blur-md">
                                    <div className="mb-2 flex items-center justify-between">
                                        <div className="text-[10px] font-semibold uppercase tracking-wider text-gray-400">Chat channel memory</div>
                                        <button
                                            type="button"
                                            onClick={() => setChannelsOpen(false)}
                                            className="rounded-lg p-1 text-gray-500 hover:bg-white/5 hover:text-white"
                                            title="Close channels"
                                        >
                                            <X className="h-3.5 w-3.5" />
                                        </button>
                                    </div>
                                    <div className="mb-2 space-y-1">
                                        <button
                                            type="button"
                                            onClick={() => selectChannelForChat(null)}
                                            className={`flex w-full items-center justify-between rounded-lg border px-2.5 py-2 text-left text-xs transition ${
                                                !selectedChannelId
                                                    ? 'border-cyan-400/35 bg-cyan-500/10 text-cyan-100'
                                                    : 'border-white/[0.06] bg-white/[0.025] text-gray-300 hover:bg-white/[0.05]'
                                            }`}
                                        >
                                            <span>No channel selected</span>
                                            {!selectedChannelId && <Check className="h-3.5 w-3.5" />}
                                        </button>
                                        {youtubeChannels.map((ch) => {
                                            const active = channelMatchesSelection(ch, selectedChannelId, sessionChannel);
                                            return (
                                                <button
                                                    key={ch.channel_id}
                                                    type="button"
                                                    onClick={() => selectChannelForChat(ch)}
                                                    className={`flex w-full items-center justify-between gap-2 rounded-lg border px-2.5 py-2 text-left transition ${
                                                        active
                                                            ? 'border-cyan-400/35 bg-cyan-500/10 text-cyan-100'
                                                            : 'border-white/[0.06] bg-white/[0.025] text-gray-300 hover:bg-white/[0.05]'
                                                    }`}
                                                >
                                                    <span className="min-w-0">
                                                        <span className="block truncate text-xs font-semibold">{ch.title}</span>
                                                        <span className="block truncate text-[10px] text-gray-500">
                                                            {ch.channel_handle || ch.registry_key || ch.channel_id}
                                                        </span>
                                                    </span>
                                                    {active && <Check className="h-3.5 w-3.5 shrink-0" />}
                                                </button>
                                            );
                                        })}
                                    </div>
                                    <AgentYouTubeConnect onChannelsLoaded={handleChannelsLoaded} />
                                </div>
                            )}
                        </div>
                    </div>
                </header>

                {/* Plan is a strict no-production workspace. Existing render
                    cards remain durable server-side, but cannot be presented
                    as a new action while the user is planning. */}
                <AgentProductionRail
                    tracks={agentMode === 'plan'
                        ? jobTracks.filter((track) => track.kind !== 'longform' && track.kind !== 'shortform')
                        : jobTracks}
                    snapshots={snapshots}
                />

                {false && messages.length === 0 && !currentSessionRunning && (
                    <div className="flex flex-1 items-center justify-center">
                        <div className="text-center">
                            <div className="mx-auto mb-4 h-9 w-9 rounded-full bg-white/90" />
                            <div className="text-2xl font-semibold tracking-tight text-white">Studio Agent</div>
                            <div className="mt-1 text-sm text-white/50">Your personal production partner</div>
                            <div className="mt-6 text-xs text-white/40">Type anything to begin — or use the mic.</div>
                        </div>
                    </div>
                )}

                {/* Channel connect now opens as a header popover. */}
                {false && channelsOpen && (
                    <div className="mx-auto mb-2 w-full max-w-3xl shrink-0 rounded-2xl border border-white/10 bg-white/[0.015] p-3">
                        <div className="mb-2 flex items-center justify-between">
                            <div className="text-[10px] font-semibold uppercase tracking-wider text-gray-400">YouTube Channels</div>
                            <button
                                type="button"
                                onClick={() => setChannelsOpen(false)}
                                className="text-gray-500 hover:text-white"
                                title="Close channels"
                            >
                                <X className="h-3.5 w-3.5" />
                            </button>
                        </div>
                        <AgentYouTubeConnect />
                        <p className="mt-2 text-[10px] leading-relaxed text-gray-500">
                            Studio logs production signals (chat + renders) to improve NYPTID models — never sold to advertisers.
                            Connect YouTube so Catalyst can learn what works on your channel and for better topic/CTA suggestions.
                        </p>
                    </div>
                )}

                {error && (
                    <div className="mx-auto mb-2 flex w-full max-w-3xl shrink-0 items-start gap-2 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2">
                        <p className="flex-1 text-xs leading-relaxed text-red-200">{error}</p>
                        {dockRepairableFailedSnap ? (
                            <button
                                type="button"
                                onClick={() => handlePrepareSceneRepair(dockRepairableFailedSnap)}
                                className="shrink-0 rounded-md border border-cyan-400/35 bg-cyan-400/10 px-2 py-1 text-[10px] font-semibold text-cyan-100 hover:bg-cyan-400/20"
                                title="Prepare a scene-scoped repair request; do not restart the whole production"
                            >
                                Repair scenes
                            </button>
                        ) : /production failed|LFRenderError|start_longform_render failed/i.test(error) ? (
                            <button
                                type="button"
                                disabled={retryingProduction}
                                onClick={() => void handleRetryProduction()}
                                className="shrink-0 rounded-md border border-red-400/35 bg-red-500/15 px-2 py-1 text-[10px] font-semibold text-red-100 hover:bg-red-500/25 disabled:opacity-50"
                            >
                                {retryingProduction ? 'Retrying…' : 'Retry'}
                            </button>
                        ) : null}
                        <button
                            type="button"
                            onClick={() => setError('')}
                            className="shrink-0 text-[10px] text-red-300 hover:text-white"
                        >
                            Dismiss
                        </button>
                    </div>
                )}
                {dictation.error && (
                    <p className="mb-2 shrink-0 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs leading-relaxed text-amber-100">
                        {dictation.error}
                    </p>
                )}

                <div
                    ref={scrollRef}
                    onScroll={handleScroll}
                    className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-2 sm:px-3"
                >
                    <div className="mx-auto max-w-4xl space-y-4 pb-8 pt-4">
                        {visibleChatMessages.length === 0 && (
                            <div className="flex min-h-[56vh] flex-col items-center justify-center px-4 text-center">
                                {/* Ultra-premium Hero */}
                                <div className="mb-4 flex h-20 w-20 items-center justify-center rounded-[28px] bg-gradient-to-br from-violet-500/50 via-cyan-400/40 to-violet-500/50 ring-[6px] ring-white/10 shadow-[0_0_80px_rgba(139,92,246,0.25)]">
                                    <Sparkles className="h-10 w-10 text-white drop-shadow-lg" />
                                </div>
                                <div className="mb-2 inline-flex items-center gap-2.5 rounded-full border border-white/20 bg-white/5 py-1 pl-2.5 pr-4 text-[10px] font-semibold uppercase tracking-[2.5px] text-white/70">
                                    <div className="flex items-center gap-1.5 rounded-full bg-emerald-500/20 px-2 py-px text-emerald-400">
                                        <div className="h-1 w-1 rounded-full bg-emerald-400 animate-pulse" /> LIVE
                                    </div>
                                    PREMIUM REAL-TIME VIDEO STUDIO
                                </div>
                                <h1 className="mb-3 text-[42px] font-semibold leading-none tracking-[-2.2px] text-white sm:text-[48px]">What are we shipping today?</h1>
                                <p className="mb-1 max-w-[620px] text-[15px] leading-relaxed text-gray-400">
                                    This is the experience your plan unlocks. The agent doesn&apos;t just generate — it <span className="font-medium text-white">builds your video live inside this chat</span>. You watch every decision, every still, every motion clip, every audio layer appear in real time. Full transparency. Full control. Premium quality, delivered visibly.
                                </p>
                                <div className="mb-5 mt-1 flex items-center gap-3 text-[10px] text-white/50">
                                    <div>Sub-second updates</div>
                                    <div className="h-px w-3 bg-white/20" />
                                    <div>Per-scene creative control</div>
                                    <div className="h-px w-3 bg-white/20" />
                                    <div>Production-grade output</div>
                                </div>

                                {/* Premium, luxurious starter journeys */}
                                <div className="grid w-full max-w-[720px] grid-cols-1 gap-2 sm:grid-cols-2">
                                    {(agentMode === 'cliplab' ? [CLIPLAB_STARTER_PROMPT, ...STARTER_PROMPTS] : STARTER_PROMPTS).map((p, index) => {
                                        const icons = [Video, BookOpen, Users, Zap, Sparkles];
                                        const labels = agentMode === 'cliplab'
                                            ? ['CLIPLAB', 'VIRAL SHORTS', 'REFERENCE-LED', 'LONG-FORM DOCS', 'SIGNATURE STYLE']
                                            : ['VIRAL SHORTS', 'REFERENCE-LED', 'LONG-FORM DOCS', 'SIGNATURE STYLE'];
                                        const sub = agentMode === 'cliplab'
                                            ? [
                                                'Upload long video → clips + packaging',
                                                'Audit → rank → script → render live',
                                                'Paste any video → blueprint + build',
                                                'Outline → chapter stills → finalize',
                                                'Outcast + Seedance in one flow',
                                            ]
                                            : [
                                                'Audit → rank → script → render live',
                                                'Paste any video → blueprint + build',
                                                'Outline → chapter stills → finalize',
                                                'Outcast + Seedance in one flow',
                                            ];
                                        const Icon = icons[index % icons.length];
                                        return (
                                            <button
                                                key={index}
                                                type="button"
                                                disabled={!chatSessionReady}
                                                onClick={() => sendText(p)}
                                                className="group relative flex min-h-[132px] flex-col items-start gap-2 overflow-hidden rounded-2xl border border-white/10 bg-[#0a0a0f] p-4 text-left transition-all hover:border-white/25 hover:bg-[#111117] active:scale-[0.985] disabled:opacity-40"
                                            >
                                                <div className="flex w-full items-center justify-between">
                                                    <div className="rounded-xl bg-white/5 p-2 text-violet-400 transition group-hover:bg-violet-500/10">
                                                        <Icon className="h-4 w-4" />
                                                    </div>
                                                    <div className="text-[9px] font-mono uppercase tracking-[2px] text-violet-400/70 group-hover:text-violet-400">
                                                        {labels[index]}
                                                    </div>
                                                </div>
                                                <div className="pr-4">
                                                    <p className="text-[13px] font-semibold leading-tight text-white">{p}</p>
                                                    <p className="mt-1.5 text-[11px] text-gray-500 group-hover:text-gray-400">{sub[index]}</p>
                                                </div>
                                                <div className="mt-auto pt-3 text-[10px] text-emerald-400/80 flex items-center gap-1.5 group-hover:gap-2 transition-all">
                                                    <Play className="h-3 w-3" /> Watch real-time in chat
                                                </div>
                                                <div className="absolute -right-6 -top-6 h-24 w-24 rounded-full bg-gradient-to-br from-violet-500/10 to-transparent opacity-0 group-hover:opacity-100 transition" />
                                            </button>
                                        );
                                    })}
                                </div>

                            </div>
                        )}
                        {visibleChatMessages.map((m, i) => {
                                if (m.productionUpdate) {
                                    const update = m.productionUpdate;
                                    if (
                                        isImplicitProductionCancel({
                                            status: 'failed',
                                            stage_label: update.stage_label,
                                            stage: update.stage_label,
                                            error: '',
                                        })
                                        || shouldSuppressProductionJob(
                                            update.job_id,
                                            update.title,
                                            messages,
                                            blockedJobIdsRef.current,
                                        )
                                    ) {
                                        return null;
                                    }
                                    return (
                                        <AgentProgressBubble
                                            key={`progress-${update.job_id}-${i}`}
                                            update={update}
                                        />
                                    );
                                }
                                const text = deliverableDisplayText(String(m.content ?? ''), m.jobDeliverable);
                                if (!text.trim() && !m.jobDeliverable) return null;

                                const isUser = m.role === 'user';

                                return (
                                    <div
                                        key={`${m.role}-${i}`}
                                        className={`flex ${isUser ? 'justify-end' : 'justify-start'} group`}
                                    >
                                        <div
                                            className={`max-w-[92%] rounded-3xl px-4 py-3 text-[14px] leading-relaxed transition-all duration-200 sm:max-w-[82%] ${
                                                isUser
                                                    ? 'bg-white text-black rounded-br-lg'
                                                    : 'bg-white/[0.035] text-gray-100 border border-white/[0.07] rounded-bl-lg'
                                            }`}
                                        >
                                            {text.trim() ? (
                                                isUser ? (
                                                    <p className="whitespace-pre-wrap">{text}</p>
                                                ) : (
                                                    <AgentMessageBody content={text} />
                                                )
                                            ) : null}

                                            {/* The magic: rich live production card appears right inside the chat thread */}
                                            {m.jobDeliverable
                                                && !shouldHideJobDeliverable(m.jobDeliverable, messages)
                                                && !shouldSuppressProductionJob(
                                                    m.jobDeliverable.job_id,
                                                    m.jobDeliverable.title,
                                                    messages,
                                                    blockedJobIdsRef.current,
                                                    m.jobDeliverable.status,
                                                )
                                                && !(agentMode === 'plan'
                                                    && (m.jobDeliverable.kind === 'longform' || m.jobDeliverable.kind === 'shortform')
                                                    && !m.jobDeliverable.thumbnail_only) && (
                                                <div className="mt-2 -mx-1">
                                                    <AgentJobDeliverable
                                                        snapshot={m.jobDeliverable}
                                                        sessionId={sessionId}
                                                        enableVideoPreview={
                                                            m.jobDeliverable.job_id === latestVideoPreviewJobId
                                                            || m.jobDeliverable.status === 'running'
                                                            || m.jobDeliverable.status === 'awaiting_approval'
                                                            || (
                                                                m.jobDeliverable.status === 'complete'
                                                                && Boolean(m.jobDeliverable.mp4_url)
                                                            )
                                                        }
                                                        captionsEnabled={captionMode !== 'off'}
                                                        onCancel={
                                                            m.jobDeliverable.kind === 'shortform'
                                                            && m.jobDeliverable.status === 'running'
                                                            && m.jobDeliverable.running !== false
                                                                ? () => void handleCancelProduction(
                                                                    m.jobDeliverable!.job_id,
                                                                    m.jobDeliverable!.kind,
                                                                )
                                                                : undefined
                                                        }
                                                        cancelling={cancellingProduction}
                                                        onFinalizeStarted={(jid, jobs) =>
                                                            handleFinalizeStarted(jid, jobs)
                                                        }
                                                        onReply={handleReplyToJob}
                                                        onSnapshotUpdate={appendJobDeliverable}
                                                        onRetry={
                                                            m.jobDeliverable.status === 'failed'
                                                            && !isRepairableShortformFailure(m.jobDeliverable)
                                                                ? () => void handleRetryProduction()
                                                                : undefined
                                                        }
                                                        retrying={retryingProduction}
                                                    />
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                );
                            })}
                        {(currentSessionRunning || activitySteps.length > 0) && (
                            <div
                                className="max-w-[92%] space-y-3 rounded-2xl border border-white/[0.06] bg-white/[0.02] px-4 py-3 sm:max-w-[82%]"
                                title={toolActivity || undefined}
                            >
                                {queueHint && (
                                    <p className="text-[11px] text-amber-200/80">{queueHint}</p>
                                )}
                                {activitySteps.length > 0 && <AgentActivityTimeline steps={activitySteps} />}
                                {currentSessionRunning && activitySteps.length === 0 && (
                                    <div className="flex items-center gap-2 text-[13px] text-gray-400">
                                        <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                        <span>{toolActivity || 'Thinking about your request'}</span>
                                    </div>
                                )}
                                {dockSnap && dockSnap.running !== false && !dockDismissed ? (
                                    <p className="text-[12px] text-cyan-300/90">
                                        {dockSnap.stage_label} · {dockSnap.progress}%
                                        {dockSnap.total_scenes
                                            ? ` · scene ${dockSnap.current_scene || 0}/${dockSnap.total_scenes}`
                                            : ''}
                                    </p>
                                ) : null}
                            </div>
                        )}
                    </div>
                </div>

                {conceptPlan
                    && (agentMode !== 'plan' || conceptPlan.status === 'ready_for_review')
                    && (conceptPlan.title || conceptPlan.format || conceptPlan.id) && (
                    <div className="relative z-30 mx-auto mb-1 w-full max-w-3xl shrink-0 px-3">
                        <AgentConceptCard
                            plan={conceptPlan}
                            disabled={Boolean(currentSessionRunning)}
                            onCommit={commitConceptPlan}
                            onDismiss={() => setConceptPlan(null)}
                        />
                    </div>
                )}

                {visiblePending.length > 0 && (
                    <div className="relative z-30 mx-auto mb-1 w-full max-w-3xl shrink-0 px-3">
                        <div className="rounded-xl border border-amber-500/35 bg-[#140f08]/95 px-2.5 py-2 shadow-lg shadow-black/30">
                        <div className="mb-1.5 flex items-center justify-between gap-2">
                            <p className="flex items-center gap-1.5 text-[11px] font-semibold text-amber-100">
                                <Zap className="h-3.5 w-3.5 text-amber-400" />
                                Approval required
                            </p>
                            <button
                                type="button"
                                disabled={syncing || resuming || !sessionId}
                                onClick={() => void syncSessionFromServer()}
                                className="shrink-0 rounded-md border border-amber-500/25 px-2 py-0.5 text-[10px] font-semibold text-amber-200 hover:bg-amber-500/15 disabled:opacity-50"
                            >
                                {syncing ? 'Syncing…' : 'Sync chat'}
                            </button>
                        </div>
                        <div className="space-y-1.5">
                            {visiblePending.map((a) => (
                                <div
                                    key={a.id}
                                    className="rounded-lg border border-amber-500/25 bg-black/30 p-2"
                                >
                                    <p className="text-xs font-semibold text-white">
                                        {pendingActionLabel(a.tool)}
                                    </p>
                                    <p className="mt-0.5 line-clamp-1 text-[10px] text-gray-400">
                                        {formatPendingArgs(a.arguments, renderStyleCatalog) || a.summary || JSON.stringify(a.arguments)}
                                    </p>
                                    <div className="mt-2 flex gap-2">
                                        <button
                                            type="button"
                                            disabled={currentSessionRunning}
                                            onClick={() => approveAction(a.id)}
                                            className="inline-flex min-h-9 flex-1 items-center justify-center gap-1.5 rounded-lg bg-emerald-600 px-3 py-1.5 text-xs font-bold text-white transition hover:bg-emerald-500 disabled:opacity-50"
                                        >
                                            {currentSessionRunning ? (
                                                <Loader2 className="h-4 w-4 animate-spin" />
                                            ) : (
                                                <Check className="h-4 w-4 stroke-[3]" />
                                            )}
                                            Approve & run
                                        </button>
                                        <button
                                            type="button"
                                            disabled={currentSessionRunning}
                                            onClick={() => rejectAction(a.id)}
                                            className="inline-flex min-h-9 items-center justify-center gap-1 rounded-lg border border-white/15 px-3 py-1.5 text-xs font-semibold text-gray-300 hover:bg-white/[0.06]"
                                        >
                                            <X className="h-4 w-4" />
                                            Reject
                                        </button>
                                    </div>
                                </div>
                            ))}
                        </div>
                        </div>
                        <p className="mt-2 text-center text-[10px] text-amber-200/70">
                            Not the shield toggle below — use Approve & run above.
                        </p>
                    </div>
                )}

                <div className="mx-auto w-full max-w-3xl shrink-0 px-3 pb-3 pt-2">
                    {attachments.length > 0 && (
                        <div className="mb-2 flex flex-wrap gap-2">
                            {attachments.map((f) => (
                                <span
                                    key={f.id}
                                    className="inline-flex items-center gap-2 rounded-xl border border-white/10 bg-white/[0.04] px-2 py-1.5 text-[10px] text-gray-300"
                                >
                                    {f.kind === 'image' && attachmentPayload[f.id]?.data_url && (
                                        <img
                                            src={attachmentPayload[f.id].data_url}
                                            alt={f.name}
                                            className="h-9 w-9 rounded-lg border border-white/10 object-cover"
                                        />
                                    )}
                                    {f.kind === 'image' ? 'Image: ' : f.kind === 'text' ? 'File: ' : f.kind === 'video' ? 'Video: ' : 'Unsupported: '}
                                    {f.name}
                                    <button
                                        type="button"
                                        onClick={() => {
                                            setAttachments((a) => a.filter((x) => x.id !== f.id));
                                            setAttachmentPayload((p) => {
                                                const next = { ...p };
                                                delete next[f.id];
                                                return next;
                                            });
                                        }}
                                        className="text-gray-500 hover:text-white"
                                    >
                                        <X className="h-3 w-3" />
                                    </button>
                                </span>
                            ))}
                        </div>
                    )}

                    {/* Reply context bar - like Discord reply, lets user re-edit specific video in same chat */}
                    {replyingTo && (
                        <div className="mb-2 flex items-center gap-2 rounded-lg border-l-4 border-violet-500 bg-white/[0.03] px-3 py-1.5 text-xs">
                            <ArrowLeft className="h-3.5 w-3.5 text-violet-400" />
                            <span className="font-semibold text-violet-300">Replying to:</span>
                            <span className="truncate text-gray-300">{replyingTo.title || replyingTo.kind + ' video'}</span>
                            {typeof replyingTo.scene_index === 'number' && (
                                <span className="rounded-full border border-cyan-400/25 bg-cyan-500/10 px-2 py-0.5 text-[10px] font-semibold text-cyan-200">
                                    Scene {replyingTo.scene_index + 1}
                                </span>
                            )}
                            <button
                                type="button"
                                onClick={() => setReplyingTo(null)}
                                className="ml-auto text-gray-500 hover:text-white"
                                title="Cancel reply"
                            >
                                <X className="h-3 w-3" />
                            </button>
                        </div>
                    )}

                    <div className="rounded-[26px] border border-white/[0.12] bg-[#111113] shadow-2xl shadow-black/50">
                        {(dictation.listening || dictation.transcribing) && (
                            <div className="flex items-center gap-3 border-b border-white/[0.05] px-3 pt-2.5 pb-1.5">
                                <DictationWaveform
                                    active={dictation.listening}
                                    levels={dictation.levels}
                                    className="min-w-0 flex-1"
                                />
                                <span className="shrink-0 text-[10px] font-medium uppercase tracking-wide text-rose-300/80">
                                    {dictation.transcribing ? 'Transcribing' : 'Live'}
                                </span>
                            </div>
                        )}
                        {dictationPreview && (dictation.listening || dictation.transcribing) && (
                            <p className="line-clamp-2 px-4 pt-2 text-[12px] leading-relaxed text-gray-400">
                                <span className="text-gray-600">Transcript · </span>
                                {dictationPreview}
                            </p>
                        )}
                        <textarea
                            ref={inputRef}
                            className="max-h-36 min-h-[52px] w-full resize-none bg-transparent px-4 pt-3.5 text-sm text-white placeholder:text-gray-600 focus:outline-none"
                            placeholder={
                                dictation.listening
                                    ? 'Speak naturally — waves show the mic is hearing you'
                                    : dictation.transcribing
                                      ? 'Finishing transcript…'
                                      : 'Talk to the agent — type, dictate (mic), or kick off a video'
                            }
                            rows={1}
                            value={input}
                            disabled={!chatSessionReady || dictation.transcribing}
                            onChange={(e) => setInput(e.target.value)}
                            onPaste={onPasteIntoInput}
                            onKeyDown={(e) => {
                                if (e.key === 'Enter' && !e.shiftKey) {
                                    e.preventDefault();
                                    sendMessage();
                                }
                            }}
                        />
                        <div className="flex items-center gap-2 border-t border-white/[0.06] px-2 py-2">
                            <input
                                ref={fileInputRef}
                                type="file"
                                multiple
                                accept="image/*,video/*,.mp4,.mov,.mkv,.webm,.m4v,.txt,.md,.json,.csv,.py,.ts,.tsx,.js,.jsx,.yaml,.yml"
                                className="hidden"
                                onChange={(e) => onPickFiles(e.target.files)}
                            />
                            <button
                                type="button"
                                onClick={() => fileInputRef.current?.click()}
                                className="rounded-lg p-2 text-gray-400 transition hover:bg-white/[0.06] hover:text-white"
                                title="Attach files"
                            >
                                <Paperclip className="h-4 w-4" />
                            </button>
                            <AgentModeMenu
                                mode={agentMode}
                                isAdmin={isAdminUser}
                                onSelect={(next) => {
                                    setAgentMode(next);
                                    void patchSession({ agent_mode: next });
                                }}
                            />
                            <button
                                type="button"
                                disabled={!chatSessionReady || !dictation.supported || dictation.transcribing || currentSessionRunning}
                                onClick={() => dictation.toggle()}
                                className={`relative rounded-lg p-2 transition ${
                                    dictation.listening
                                        ? 'bg-rose-600/25 text-rose-200 ring-1 ring-rose-500/50 shadow-[0_0_18px_rgba(244,63,94,0.25)]'
                                        : 'text-gray-400 hover:bg-white/[0.06] hover:text-white'
                                } disabled:opacity-40`}
                                title={
                                    dictation.engine === 'live-xai'
                                        ? 'Voice dictation (live xAI Grok STT)'
                                        : dictation.engine === 'record'
                                          ? 'Voice dictation (record then xAI transcribe)'
                                          : dictation.engine === 'webspeech'
                                            ? 'Voice dictation (browser speech)'
                                            : 'Voice dictation not supported'
                                }
                            >
                                {dictation.listening ? (
                                    <MicOff className="h-4 w-4" />
                                ) : dictation.transcribing ? (
                                    <Loader2 className="h-4 w-4 animate-spin" />
                                ) : (
                                    <Mic className="h-4 w-4" />
                                )}
                            </button>
                            {dictation.listening && (
                                <DictationWaveform
                                    active
                                    levels={dictation.levels}
                                    className="hidden h-6 max-w-[88px] sm:flex"
                                />
                            )}
                            <button
                                type="button"
                                onClick={() => setModelPickerOpen(true)}
                                className="inline-flex max-w-[45%] items-center gap-1.5 truncate rounded-lg px-2 py-1.5 text-[11px] font-semibold uppercase tracking-wide text-gray-200 transition hover:bg-white/[0.06] sm:max-w-none"
                            >
                                <Sparkles className="h-3.5 w-3.5 shrink-0 text-sky-400" />
                                <span className="truncate">{displayModelName(modelCatalog, model)}</span>
                            </button>
                            <button
                                type="button"
                                onClick={() => {
                                    const next = approvalMode === 'confirm' ? 'auto' : 'confirm';
                                    setApprovalMode(next);
                                    patchSession({ approval_mode: next });
                                }}
                                className={`inline-flex items-center gap-1.5 rounded-lg px-2 py-1.5 text-[11px] font-medium transition ${
                                    approvalMode === 'confirm'
                                        ? 'text-amber-200/90 hover:bg-amber-500/10'
                                        : 'text-emerald-200/90 hover:bg-emerald-500/10'
                                }`}
                            >
                                {approvalMode === 'confirm' ? (
                                    <>
                                        <Shield className="h-3.5 w-3.5" />
                                        Confirm calls
                                    </>
                                ) : (
                                    <>
                                        <ShieldOff className="h-3.5 w-3.5" />
                                        Auto-approve
                                    </>
                                )}
                            </button>
                            {(agentMode === 'studio' || agentMode === 'plan') && (
                                <>
                                    <button
                                        type="button"
                                        onClick={() => setImageModelPickerOpen(true)}
                                        className="inline-flex max-w-[150px] items-center gap-1.5 rounded-lg px-2 py-1.5 text-[11px] font-medium text-cyan-100 transition hover:bg-cyan-500/10"
                                        title="Image model for stills, scenes, and thumbnails (short-form and long-form)."
                                    >
                                        <ImageIcon className="h-3.5 w-3.5 shrink-0 text-cyan-300" />
                                        <span className="truncate">
                                            {selectedModelLabel(imageModelCatalog, imageModel, 'Image')}
                                        </span>
                                    </button>
                                    <button
                                        type="button"
                                        onClick={() => setVideoModelPickerOpen(true)}
                                        className="inline-flex max-w-[150px] items-center gap-1.5 rounded-lg px-2 py-1.5 text-[11px] font-medium text-cyan-100 transition hover:bg-cyan-500/10"
                                        title="Video model for image-to-video animation (short-form and long-form motion)."
                                    >
                                        <Video className="h-3.5 w-3.5 shrink-0 text-cyan-300" />
                                        <span className="truncate">
                                            {selectedModelLabel(videoModelCatalog, videoModel, 'Video')}
                                        </span>
                                    </button>
                                </>
                            )}
                            <div className="flex-1" />
                            <button
                                type="button"
                                disabled={currentSessionRunning || (!input.trim() && !hasReadableAttachment) || !chatSessionReady}
                                onClick={sendMessage}
                                className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-emerald-600 text-black transition hover:bg-emerald-500 disabled:opacity-40"
                            >
                                <ArrowUp className="h-4 w-4 stroke-[2.5]" />
                            </button>
                        </div>
                    </div>
                </div>
            </div>
            </div>
            {showRenderDock ? (
                <AgentRenderDock
                    track={resolvedDockTrack ?? null}
                    snapshot={resolvedDockSnap}
                    accessToken={session?.access_token}
                    onRetry={dockRepairableFailedSnap ? undefined : handleRetryProduction}
                    onRepair={dockRepairableFailedSnap
                        ? () => handlePrepareSceneRepair(dockRepairableFailedSnap)
                        : undefined}
                    retrying={retryingProduction}
                    onCancel={handleCancelProduction}
                    cancelling={cancellingProduction}
                    onDismiss={() => {
                        if (resolvedDockSnap?.job_id) {
                            dismissedDockJobIdsRef.current.add(resolvedDockSnap.job_id);
                        }
                        setDockDismissed(true);
                    }}
                />
            ) : null}
        </>
    );
}
