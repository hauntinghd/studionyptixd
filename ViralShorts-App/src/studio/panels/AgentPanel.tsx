/**
 * Studio Agent — full-screen chat (Anthropic Claude + Rookcast skills).
 */
import { useCallback, useContext, useEffect, useRef, useState, type ClipboardEvent } from 'react';
import {
    ArrowLeft, ArrowUp, BookOpen, Brain, Check, ChevronsLeft, ChevronsRight, History, Loader2,
    MessageSquarePlus, Mic, MicOff, Palette, Paperclip, Play, RefreshCw, RotateCcw, Search, Shield,
    ShieldOff, Sparkles, Trash2, Users, Video, X, Zap,
} from 'lucide-react';
import AgentJobDeliverable, { type SceneReplyPreset } from '../components/agent/AgentJobDeliverable';
import AgentMessageBody from '../components/agent/AgentMessageBody';
import AgentProductionRail from '../components/agent/AgentProductionRail';
import AgentProgressBubble from '../components/agent/AgentProgressBubble';
import AgentRenderDock from '../components/agent/AgentRenderDock';
import AgentYouTubeConnect, { type ChannelRow } from '../components/agent/AgentYouTubeConnect';
import { useAgentProductionJobs } from '../hooks/useAgentProductionJobs';
import {
    type AgentJobSnapshot,
    type AgentJobTrack,
    type ProductionProgressUpdate,
    cancelJob,
    lastSessionStorageKey,
    loadPersistedJobs,
    mergeJobTracks,
    persistJobs,
    rehydrateJobSnapshots,
} from '../lib/agentProduction';
import { streamAgentChat, toolLabel, type AgentChatAttachment, type AgentStreamEvent } from '../lib/streamAgentChat';
import { useSpeechDictation } from '../hooks/useSpeechDictation';
import { AuthContext, resolveStudioBackendUrl } from '../shared';
import { loadStudioHubState } from '../lib/studioHubState';
import AgentModelPicker, { type AgentModelOption } from './AgentModelPicker';

type ApprovalMode = 'auto' | 'confirm';

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
    productionUpdate?: ProductionProgressUpdate;
}

interface SessionUiCache {
    messages: ChatMessage[];
    pending: PendingAction[];
    jobTracks: AgentJobTrack[];
    dockDismissed: boolean;
}

const SESSION_UI_CACHE_VERSION = 1;
const MAX_SESSION_UI_CACHE_ENTRIES = 30;
const MAX_SESSION_UI_CACHE_MESSAGES = 160;
const MAX_SESSION_UI_CACHE_MESSAGE_CHARS = 24000;

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

function sanitizeSessionUiCacheEntry(entry: SessionUiCache): SessionUiCache {
    return {
        messages: (entry.messages || []).slice(-MAX_SESSION_UI_CACHE_MESSAGES).map(trimCachedMessage),
        pending: entry.pending || [],
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
    kind: 'image' | 'text' | 'binary';
}

interface AttachmentPayload {
    name: string;
    mime_type: string;
    size: number;
    kind: 'image' | 'text' | 'binary';
    text?: string;
    data_url?: string;
}

const MAX_AGENT_IMAGE_ATTACHMENTS = 4;
const MAX_AGENT_IMAGE_BYTES = 8 * 1024 * 1024;

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
        description: 'Default Studio runner for tool use and production planning.',
    },
    {
        id: 'claude-opus-4-8',
        name: 'Claude Opus 4.8',
        provider: 'Anthropic',
        recommended: true,
        intelligence: 5,
        speed: 2,
        description: 'Highest-depth Claude runner for complex production sessions.',
    },
    {
        id: 'claude-haiku-4-5-20251001',
        name: 'Claude Haiku 4.5',
        provider: 'Anthropic',
        recommended: true,
        intelligence: 4,
        speed: 5,
        description: 'Fast, lower-cost Claude runner for status checks and lightweight tool loops.',
    },
];

type ContentFormat = 'short' | 'long' | 'both';
type ReasoningDepth = 'fast' | 'balanced' | 'deep';
type CaptionMode = 'word' | 'off';

const REASONING_OPTIONS: { id: ReasoningDepth; label: string; hint: string }[] = [
    { id: 'fast', label: 'Fast', hint: 'Quick answers, less deliberation' },
    { id: 'balanced', label: 'Balanced', hint: 'Default depth' },
    { id: 'deep', label: 'Deep', hint: 'Thorough analysis before recommendations' },
];

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
    { key: 'comic_book', label: 'Comic book (color)', group: 'Comic' },
    { key: 'bw_comic', label: 'B&W comic', group: 'Comic' },
    { key: 'studio_ghibli', label: 'Studio Ghibli', group: 'Animation' },
    { key: 'pixar', label: 'Pixar', group: 'Animation' },
    { key: 'claymation', label: 'Claymation', group: 'Animation' },
    { key: 'skeleton_host', label: 'Skeleton (NYPTID mascot)', group: 'Niche' },
];

function formatPendingArgs(args?: Record<string, unknown>): string {
    if (!args || !Object.keys(args).length) return '';
    const styleKey = String(args.render_style || '');
    const topic = String(args.topic || '');
    const brief = String(args.visual_brief || '');
    const parts: string[] = [];
    if (styleKey) parts.push(`Art style: ${styleKey}`);
    if (topic) parts.push(`Topic: ${topic}`);
    if (brief) parts.push(`Brief: ${brief.slice(0, 160)}`);
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

function productionAlreadyApproved(
    messages: Array<{ role?: string; content?: string }>,
): boolean {
    return messages.some(
        (m) =>
            m.role === 'user'
            && (String(m.content || '').startsWith('[User approved start_shortform_generate]')
                || String(m.content || '').startsWith('[User approved start_longform_render]')),
    );
}

function mergePendingFromTranscript(
    messages: Array<{ role?: string; content?: string; tool_call_id?: string }>,
    serverPending: PendingAction[],
): PendingAction[] {
    if (serverPending.length > 0) return serverPending;
    if (productionAlreadyApproved(messages)) return [];

    const recovered = new Map<string, PendingAction>();
    for (const m of messages) {
        const text = String(m.content || '');
        if (!text.includes('awaiting_user_approval')) continue;
        try {
            const parsed = JSON.parse(text) as { action_id?: string; status?: string };
            const aid = String(parsed.action_id || '').trim();
            if (!aid || parsed.status !== 'awaiting_user_approval') continue;
            recovered.set(aid, {
                id: aid,
                tool: 'start_shortform_generate',
                summary: 'Tap Sync chat first so the server loads full args, then Approve & run',
            });
        } catch {
            /* ignore */
        }
    }
    return Array.from(recovered.values());
}

function transcriptMentionsPendingAction(messages: ChatMessage[]): boolean {
    const last = [...messages].reverse().find((m) => m.role === 'assistant');
    if (!last) return false;
    return PENDING_ACTION_ID_RE.test(String(last.content || ''));
}

function friendlyApiError(status: number, data: Record<string, unknown>, fallback: string) {
    const detail = String(data?.detail || data?.error || fallback);
    if (status === 401 || status === 403) {
        return detail || 'Sign in required. Studio Agent needs Creator or Studio plan (owners have full access).';
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
    if (status === 429) {
        if (/queue/i.test(detail)) {
            return (
                `${detail} — This is usually the RunPod API bridge (not your approve step). `
                + 'Agent chat/approve uses Fly directly; use Sync chat and Approve & run.'
            );
        }
        return detail || 'Too many requests — wait a moment and retry.';
    }
    return detail;
}

export default function AgentPanel({ onBack }: { onBack?: () => void }) {
    const { session, ownerOverride } = useContext(AuthContext);
    const userCacheKey = String((session as any)?.user?.id || (session as any)?.user?.email || 'anon');
    const [sessionId, setSessionId] = useState<string | null>(null);
    const [model, setModel] = useState(FALLBACK_MODELS[0].id);
    const [modelCatalog, setModelCatalog] = useState<AgentModelOption[]>(FALLBACK_MODELS);
    const [modelPickerOpen, setModelPickerOpen] = useState(false);
    const [approvalMode, setApprovalMode] = useState<ApprovalMode>('confirm');
    const [messages, setMessages] = useState<ChatMessage[]>([]);
    const [pending, setPending] = useState<PendingAction[]>([]);
    const [draftsBySession, setDraftsBySession] = useState<Record<string, string>>({});
    const [attachments, setAttachments] = useState<AttachedFile[]>([]);
    const [attachmentPayload, setAttachmentPayload] = useState<Record<string, AttachmentPayload>>({});
    const [runningBySession, setRunningBySession] = useState<Record<string, string>>({});
    const [resuming, setResuming] = useState(false);
    const [toolActivity, setToolActivity] = useState('');
    const [booting, setBooting] = useState(true);
    const [history, setHistory] = useState<SessionSummary[]>([]);
    const [historyQuery, setHistoryQuery] = useState('');
    const [historyOpen, setHistoryOpen] = useState(true);
    const [productWebsite, setProductWebsite] = useState('');
    const [contentFormat, setContentFormat] = useState<ContentFormat>('both');
    const [reasoningDepth, setReasoningDepth] = useState<ReasoningDepth>('balanced');
    const [renderStyle, setRenderStyle] = useState('cinematic');
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
    const [error, setError] = useState('');
    const [queueHint, setQueueHint] = useState('');
    const [dictationPreview, setDictationPreview] = useState('');
    const scrollRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLTextAreaElement>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);
    const stickToBottomRef = useRef(true);
    const sessionIdRef = useRef<string | null>(null);
    const jobSessionRef = useRef<Map<string, string>>(new Map());
    const queuePollInFlightRef = useRef(false);
    const sessionLoadSeqRef = useRef(0);
    const sessionUiCacheRef = useRef<Map<string, SessionUiCache>>(new Map());
    const [jobTracks, setJobTracks] = useState<AgentJobTrack[]>([]);
    const [dockDismissed, setDockDismissed] = useState(false);
    const [pollResetKey, setPollResetKey] = useState(0);
    const [retryingProduction, setRetryingProduction] = useState(false);
    const [cancellingProduction, setCancellingProduction] = useState(false);
    const [deleteTarget, setDeleteTarget] = useState<SessionSummary | null>(null);
    const userCancelledJobsRef = useRef<Set<string>>(new Set());
    const currentSessionRunning = Boolean(sessionId && runningBySession[sessionId]);
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

    const setInput = useCallback((next: string | ((prev: string) => string)) => {
        const sid = sessionIdRef.current;
        if (!sid) return;
        setDraftsBySession((prev) => {
            const old = prev[sid] || '';
            const value = typeof next === 'function' ? next(old) : next;
            return { ...prev, [sid]: value };
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
        snap.status === 'failed'
        && /cancel+ed by user/i.test(String(snap.error || snap.stage_label || snap.stage || ''))
        && !userCancelledJobsRef.current.has(snap.job_id)
    ), []);

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
        const tok = session?.access_token;
        if (!tok) throw new Error('Not signed in');
        return tok;
    }, [session?.access_token]);

    const ingestActiveJobs = useCallback(
        (raw: unknown, sid: string | null) => {
            const list = Array.isArray(raw) ? (raw as AgentJobTrack[]) : [];
            const normalized = list
                .map((j) => ({
                    job_id: String(j.job_id || ''),
                    kind: (j.kind || 'longform') as AgentJobTrack['kind'],
                    title: String(j.title || ''),
                    started_at: Number(j.started_at || Date.now()),
                }))
                .filter((j) => j.job_id);
            if (!normalized.length) return;
            if (sid) {
                for (const job of normalized) {
                    jobSessionRef.current.set(job.job_id, sid);
                }
            }
            setJobTracks((prev) => {
                const merged = mergeJobTracks(prev, normalized);
                if (sid) persistJobs(sid, merged);
                return merged;
            });
            setDockDismissed(false);
        },
        [],
    );

    const appendJobDeliverable = useCallback((snap: AgentJobSnapshot) => {
        const ownerSession = jobSessionRef.current.get(snap.job_id);
        if (ownerSession && ownerSession !== sessionIdRef.current) return;
        if (isImplicitCancelFailure(snap)) return;
        const label =
            snap.kind === 'competitor'
                ? 'Reference analysis finished — pacing and blueprint signals are in the card below.'
                : snap.status === 'failed'
                  ? snap.error
                      ? `Production failed: ${snap.error}`
                      : 'Production failed. Ask the agent to retry or adjust the brief.'
                  : snap.status === 'running'
                    ? 'Production is in progress — track progress in the render dock.'
                  : snap.status === 'awaiting_approval'
                    ? approvalMode === 'auto'
                      ? 'Stills are ready — auto-finalize is exporting voice, sound, and MP4.'
                      : 'Your long-form stills are ready. Review the grid, then tap Finalize & export MP4.'
                    : 'Your video is ready.';
        setMessages((m) => {
            const nextRow: ChatMessage = {
                role: 'assistant' as const,
                content: label,
                jobDeliverable: snap,
            };
            let replaced = false;
            const withoutProgress = m.filter((row) => row.productionUpdate?.job_id !== snap.job_id);
            const next = withoutProgress.map((row) => {
                if (row.jobDeliverable?.job_id !== snap.job_id) return row;
                replaced = true;
                return nextRow;
            });
            return replaced ? next : [...next, nextRow];
        });
        setJobTracks((prev) => {
            const next = prev.filter((j) => j.job_id !== snap.job_id);
            const sid = sessionIdRef.current;
            if (sid) persistJobs(sid, next);
            return next;
        });
        stickToBottomRef.current = true;
    }, [approvalMode, isImplicitCancelFailure]);

    const upsertProgressLine = useCallback((update: ProductionProgressUpdate) => {
        const ownerSession = jobSessionRef.current.get(update.job_id);
        if (ownerSession && ownerSession !== sessionIdRef.current) return;
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

    const { snapshots, primary: dockTrack, primarySnap: dockSnap } = useAgentProductionJobs({
        sessionId,
        tracks: jobTracks,
        pollResetKey,
        getToken,
        onProgress: upsertProgressLine,
        onJobComplete: (snap: AgentJobSnapshot) => {
            const ownerSession = jobSessionRef.current.get(snap.job_id);
            if (ownerSession && ownerSession !== sessionIdRef.current) return;
            setDockDismissed(false);
            appendJobDeliverable(snap);
        },
        onJobFailed: (snap: AgentJobSnapshot) => {
            const ownerSession = jobSessionRef.current.get(snap.job_id);
            if (ownerSession && ownerSession !== sessionIdRef.current) return;
            if (isImplicitCancelFailure(snap)) {
                setJobTracks((prev) => {
                    const next = prev.filter((j) => j.job_id !== snap.job_id);
                    if (sessionId) persistJobs(sessionId, next);
                    return next;
                });
                setDockDismissed(true);
                return;
            }
            setPending([]);
            setDockDismissed(false);
            setError(snap.error || 'Production failed');
            appendJobDeliverable(snap);
        },
        onAwaitingApproval: appendJobDeliverable,
        autoFinalizeLongform: approvalMode === 'auto',
        onAutoFinalizeStarted: handleFinalizeStarted,
    });


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
        async (path: string, init?: RequestInit & { timeoutMs?: number }) => {
            const tok = await getToken();
            const url = resolveStudioBackendUrl(path);
            const timeoutMs = init?.timeoutMs ?? 0;
            const { timeoutMs: _omitTimeout, ...fetchInit } = init || {};
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
                if ((e as Error).name === 'AbortError') {
                    throw new Error(
                        `Studio Agent timed out after ${Math.round(timeoutMs / 1000)}s — retry Resume or open the chat from History.`,
                    );
                }
                const message = String((e as Error)?.message || e || '');
                if (/failed to fetch|networkerror|load failed|fetch resource/i.test(message)) {
                    throw new Error(
                        'Studio Agent could not reach the backend from this browser tab. Your chat is preserved; wait a moment and press Resume.',
                    );
                }
                throw e;
            } finally {
                if (timer) window.clearTimeout(timer);
            }
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

    const applySessionPayload = useCallback((raw: Record<string, unknown>) => {
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
        const rawMessages = raw.messages;
        const hasServerMessages = Array.isArray(rawMessages);
        let effectiveMessages: ChatMessage[] | null = null;
        if (hasServerMessages) {
            const msgs = rawMessages
                .map(normalizeAgentMessage)
                .filter((msg): msg is ChatMessage => Boolean(msg));
            const cached = sid ? sessionUiCacheRef.current.get(sid) : null;
            const serverMessageCount = Number(raw.message_count ?? msgs.length);
            effectiveMessages =
                msgs.length === 0 && serverMessageCount > 0 && cached?.messages?.length
                    ? cached.messages
                    : msgs;
            setMessages(effectiveMessages);
        } else if (sid) {
            const cached = sessionUiCacheRef.current.get(sid);
            if (cached?.messages?.length) {
                effectiveMessages = cached.messages;
                setMessages(cached.messages);
            }
        }
        const serverPending = (raw.pending_actions as PendingAction[]) || [];
        if (effectiveMessages) {
            setPending(mergePendingFromTranscript(effectiveMessages, serverPending));
        } else if (sid) {
            const cached = sessionUiCacheRef.current.get(sid);
            if (cached) setPending(cached.pending);
        }
        if (raw.model) setModel(String(raw.model));
        if (raw.approval_mode === 'auto' || raw.approval_mode === 'confirm') {
            setApprovalMode(raw.approval_mode);
        }
        const fmt = raw.content_format as ContentFormat | undefined;
        if (fmt === 'short' || fmt === 'long' || fmt === 'both') setContentFormat(fmt);
        const depth = raw.reasoning_depth as ReasoningDepth | undefined;
        if (depth === 'fast' || depth === 'balanced' || depth === 'deep') {
            setReasoningDepth(depth);
        }
        const rs = String(raw.render_style || '').trim();
        if (rs) setRenderStyle(rs);
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
        if (sid && runLabel) {
            setRunningBySession((prev) => ({ ...prev, [sid]: runLabel }));
        }
    }, [lastSessionKey]);

    const resumeSession = useCallback(
        async (raw: Record<string, unknown>, opts?: { rehydrateJobs?: boolean }) => {
            applySessionPayload(raw);
            const sid = String(raw.session_id || '');
            if (!sid) return;
            const serverJobs = Array.isArray(raw.active_jobs) ? (raw.active_jobs as AgentJobTrack[]) : [];
            const merged = mergeJobTracks(loadPersistedJobs(sid), serverJobs);
            for (const job of merged) {
                if (job.job_id) jobSessionRef.current.set(job.job_id, sid);
            }
            if (!merged.length) return;
            setJobTracks(merged);
            persistJobs(sid, merged);
            setDockDismissed(false);
            if (opts?.rehydrateJobs === false) return;
            try {
                const tok = await getToken();
                const { deliverables } = await rehydrateJobSnapshots(sid, merged, tok);
                if (sessionIdRef.current !== sid) return;
                for (const snap of deliverables) appendJobDeliverable(snap);
                const terminal = new Set(deliverables.map((snap) => snap.job_id));
                if (terminal.size) {
                    setJobTracks((prev) => {
                        const next = prev.filter((job) => !terminal.has(job.job_id));
                        persistJobs(sid, next);
                        return next;
                    });
                }
            } catch {
                /* polling optional on resume */
            }
        },
        [applySessionPayload, appendJobDeliverable, getToken],
    );

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
            const tempSid = `local_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`;
            sessionIdRef.current = tempSid;
            setSessionId(tempSid);
            setError('');
            setQueueHint('');
            setToolActivity('');
            setPending([]);
            setMessages([]);
            setJobTracks([]);
            setDockDismissed(true);
            setReplyingTo(null);
            setAttachments([]);
            setAttachmentPayload({});
            const created = await authFetch('/api/studio-agent/sessions', {
                method: 'POST',
                body: JSON.stringify({
                    model: pickModel,
                    approval_mode: approvalMode,
                    content_format: contentFormat,
                    reasoning_depth: reasoningDepth,
                    render_style: renderStyle,
                    caption_mode: captionMode,
                    captions_enabled: captionMode !== 'off',
                    channel_id: selectedChannel?.channel_id || '',
                    registry_key: channelRegistryKey(selectedChannel),
                    channel_title: selectedChannel?.title || '',
                    product_website: productWebsite,
                }),
            });
            applySessionPayload((created.session as Record<string, unknown>) || {});
            setJobTracks([]);
            setPollResetKey((k) => k + 1);
            setDockDismissed(true);
            const sid = String((created.session as Record<string, unknown>)?.session_id || '');
            if (sid) sessionIdRef.current = sid;
            if (sid) persistJobs(sid, []);
            if (sid) setDraftsBySession((prev) => ({ ...prev, [sid]: '' }));
            await refreshHistory();
        },
        [applySessionPayload, approvalMode, authFetch, captionMode, contentFormat, productWebsite, reasoningDepth, renderStyle, refreshHistory, selectedChannel],
    );

    const openSession = useCallback(
        async (id: string) => {
            if (!id) return;
            const loadSeq = ++sessionLoadSeqRef.current;
            sessionIdRef.current = id;
            setSessionId(id);
            const cached = sessionUiCacheRef.current.get(id);
            if (cached) {
                setMessages(cached.messages);
                setPending(cached.pending);
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
                const data = await authFetch(`/api/studio-agent/sessions/${id}?sync_pending=false`, {
                    timeoutMs: 60_000,
                });
                if (sessionLoadSeqRef.current !== loadSeq || sessionIdRef.current !== id) return;
                await resumeSession((data?.session as Record<string, unknown>) || {}, {
                    rehydrateJobs: false,
                });
            } catch (e) {
                if (sessionLoadSeqRef.current !== loadSeq || sessionIdRef.current !== id) return;
                const msg = (e as Error).message || '';
                if (cached && msg.includes('timed out after')) {
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
                    'No chat to resume — pick your Wally West thread from History on the left, or start a new chat.',
                );
                return;
            }
            const data = await authFetch(`/api/studio-agent/sessions/${id}?sync_pending=true`, {
                timeoutMs: 45_000,
            });
            if (sessionLoadSeqRef.current !== loadSeq) return;
            await resumeSession((data?.session as Record<string, unknown>) || {}, {
                rehydrateJobs: true,
            });
        } catch (e) {
            if (sessionLoadSeqRef.current !== loadSeq) return;
            setError((e as Error).message);
        } finally {
            if (sessionLoadSeqRef.current === loadSeq) setResuming(false);
        }
    }, [authFetch, history, lastSessionKey, resumeSession, sessionId]);

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

    useEffect(() => {
        if (!sessionId) return;
        const onVisible = () => {
            if (document.visibilityState !== 'visible') return;
            const needsPendingSync =
                pending.length === 0 && transcriptMentionsPendingAction(messages);
            if (pending.length === 0 && jobTracks.length === 0 && !needsPendingSync) return;
            void reloadCurrentSession();
        };
        document.addEventListener('visibilitychange', onVisible);
        return () => document.removeEventListener('visibilitychange', onVisible);
    }, [sessionId, pending.length, jobTracks.length, messages, reloadCurrentSession]);

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
                const modelData = await authFetch('/api/studio-agent/models');
                const catalog = (modelData?.models as AgentModelOption[]) || [];
                const rec = (modelData?.recommended as string[]) || [];
                let pickModel = FALLBACK_MODELS[0].id;
                if (!cancelled) {
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
                const listData = await authFetch('/api/studio-agent/sessions?limit=50');
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
                    const data = await authFetch(`/api/studio-agent/sessions/${resume.session_id}?sync_pending=false`, {
                        timeoutMs: 60_000,
                    });
                    if (!cancelled) {
                        await resumeSession((data?.session as Record<string, unknown>) || {});
                    }
                } else if (!cancelled) {
                    const created = await authFetch('/api/studio-agent/sessions', {
                        method: 'POST',
                        body: JSON.stringify({
                            model: pickModel,
                            approval_mode: 'confirm',
                            content_format: 'both',
                        }),
                    });
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
            approval_mode?: ApprovalMode;
            content_format?: ContentFormat;
            reasoning_depth?: ReasoningDepth;
            render_style?: string;
            caption_mode?: CaptionMode;
            captions_enabled?: boolean;
            channel_id?: string;
            registry_key?: string;
            channel_title?: string;
            web_search?: boolean;
            animate?: boolean;
            product_website?: string;
        }) => {
            if (!sessionId) return;
            try {
                await authFetch(`/api/studio-agent/sessions/${sessionId}`, {
                    method: 'PATCH',
                    body: JSON.stringify(patch),
                });
                await refreshHistory();
            } catch (e) {
                setError((e as Error).message);
            }
        },
        [authFetch, refreshHistory, sessionId],
    );

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
        }
    }, [selectedChannelId, sessionChannel]);

    const buildOutboundMessage = useCallback(
        (text: string) => {
            const hasImages = attachments.some((f) => f.kind === 'image' && attachmentPayload[f.id]?.data_url);
            const parts = [text.trim() || (hasImages ? 'Please analyze the attached image(s).' : '')];
            for (const f of attachments) {
                const payload = attachmentPayload[f.id];
                if (!payload) continue;
                if (payload.kind === 'text' && payload.text) {
                    parts.push(`\n\n[Attachment: ${f.name}]\n${payload.text.slice(0, 12000)}`);
                } else if (payload.kind === 'binary') {
                    parts.push(`\n\n[Attachment: ${f.name}]\n[Binary file, ${Math.round(f.size / 1024)}KB. Ask the user for a supported image or text file if visual/text access is required.]`);
                }
            }
            return parts.join('');
        },
        [attachmentPayload, attachments],
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

    const pollQueueWhileLoading = useCallback(async () => {
        if (queuePollInFlightRef.current) return;
        queuePollInFlightRef.current = true;
        try {
            const snap = (await authFetch('/api/studio-agent/queue')) as {
                queued?: boolean;
                waiting?: number;
                active_sessions?: number;
                max_concurrent?: number;
            };
            if (snap?.queued) {
                const wait = Number(snap.waiting || 0);
                const active = Number(snap.active_sessions || 0);
                const max = Number(snap.max_concurrent || 250);
                setQueueHint(
                    wait > 0
                        ? `High load — ~${wait} ahead of you (${active}/${max} active). Claude + fal are queued…`
                        : `High load — ${active}/${max} concurrent sessions. Waiting for a slot…`,
                );
            } else {
                setQueueHint('');
            }
        } catch {
            setQueueHint('');
        } finally {
            queuePollInFlightRef.current = false;
        }
    }, [authFetch]);

    const sendText = useCallback(
        async (text: string) => {
            const trimmed = buildOutboundMessage(text);
            if (!trimmed || !sessionId || runningBySession[sessionId]) return;
            const activeSessionId = sessionId;
            setInput('');
            setAttachments([]);
            setAttachmentPayload({});
            markSessionRunning(activeSessionId, 'Thinking...');
            setError('');
            setQueueHint('');
            setToolActivity('');
            stickToBottomRef.current = true;
            const readableAttachments = buildOutboundAttachments();
            const visibleUserText = text.trim()
                || (readableAttachments.length ? `Please analyze the attached image${readableAttachments.length === 1 ? '' : 's'}.` : '');
            setMessages((m) => [...m, { role: 'user', content: visibleUserText }]);
            const queuePoll = window.setInterval(() => {
                if (sessionIdRef.current !== activeSessionId) return;
                void pollQueueWhileLoading();
            }, 2500);
            void pollQueueWhileLoading();
            try {
                const tok = await getToken();
                const onStreamEvent = (ev: AgentStreamEvent) => {
                    if (sessionIdRef.current !== activeSessionId) return;
                    if (ev.event === 'tool_start' && ev.tool) {
                        setToolActivity(
                            ev.awaiting_approval
                                ? `Queued for approval: ${toolLabel(ev.tool)}`
                                : toolLabel(ev.tool),
                        );
                    } else if (ev.event === 'tool_end' && ev.tool) {
                        setToolActivity(
                            ev.status === 'error'
                                ? `${toolLabel(ev.tool)} — error`
                                : `${toolLabel(ev.tool)} — done`,
                        );
                    } else if (ev.event === 'status' && ev.message) {
                        setToolActivity(ev.message);
                    } else if (ev.event === 'active_jobs' && Array.isArray(ev.jobs)) {
                        ingestActiveJobs(ev.jobs, activeSessionId);
                    } else if (ev.event === 'pending_actions' && Array.isArray(ev.actions)) {
                        setPending(ev.actions as PendingAction[]);
                    }
                };

                let data: Record<string, unknown>;
                try {
                    data = await streamAgentChat(sessionId, trimmed, tok, {
                        onEvent: onStreamEvent,
                        replyTo: replyingTo ? { job_id: replyingTo.job_id, kind: replyingTo.kind, scene_index: replyingTo.scene_index } : undefined,
                        attachments: readableAttachments,
                        captions_enabled: captionMode !== 'off',
                        caption_mode: captionMode,
                        channel: selectedChannel ? {
                            channel_id: selectedChannel.channel_id || '',
                            registry_key: channelRegistryKey(selectedChannel),
                            channel_title: selectedChannel.title || '',
                        } : null,
                    });
                } catch (streamError) {
                    try {
                        const refreshed = await authFetch(`/api/studio-agent/sessions/${activeSessionId}?sync_pending=true`, {
                            timeoutMs: 120_000,
                        });
                        await resumeSession((refreshed?.session as Record<string, unknown>) || {}, {
                            rehydrateJobs: true,
                        });
                    } catch (refreshError) {
                        throw new Error(
                            `Studio Agent connection dropped and the recovery refresh could not reach the backend. Your chat is preserved; press Resume in a few seconds. ${String((streamError as Error).message || (refreshError as Error).message || '')}`,
                        );
                    }
                    throw new Error(
                        `Studio Agent connection dropped, but the backend kept working and I reloaded the saved chat. Press Resume in a few seconds if the answer is still running. ${String((streamError as Error).message || '')}`,
                    );
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
                const nextPending = (data?.pending_actions as PendingAction[]) || [];
                if (reply) {
                    setMessages((m) => [...m, { role: 'assistant', content: reply }]);
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
                ingestActiveJobs(data?.active_jobs, activeSessionId);
                await refreshHistory();
            } catch (e) {
                if (sessionIdRef.current !== activeSessionId) return;
                setError((e as Error).message);
                setQueueHint('');
            } finally {
                window.clearInterval(queuePoll);
                clearSessionRunning(activeSessionId);
                if (sessionIdRef.current === activeSessionId) {
                    setToolActivity('');
                }
            }
        },
        [
            authFetch,
            buildOutboundAttachments,
            buildOutboundMessage,
            captionMode,
            clearSessionRunning,
            getToken,
            ingestActiveJobs,
            markSessionRunning,
            pollQueueWhileLoading,
            refreshHistory,
            resumeSession,
            runningBySession,
            selectedChannel,
            sessionId,
        ],
    );

    const sendMessage = useCallback(() => sendText(input), [input, sendText]);

    const onPickFiles = useCallback(async (files: FileList | File[] | null) => {
        if (!files?.length) return;
        const next: AttachedFile[] = [];
        const payload: Record<string, AttachmentPayload> = { ...attachmentPayload };
        let imageCount = attachments.filter((f) => f.kind === 'image').length;
        for (const file of Array.from(files)) {
            const id = `f_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
            const isImage = file.type.startsWith('image/');
            const isText = file.type.startsWith('text/')
                || /\.(md|txt|json|csv|py|ts|tsx|js|jsx|yaml|yml)$/i.test(file.name);
            const kind: AttachedFile['kind'] = isImage ? 'image' : isText ? 'text' : 'binary';
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
    }, [attachmentPayload, attachments]);

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

    const syncSessionFromServer = useCallback(async () => {
        if (!sessionId) return;
        const data = await authFetch(`/api/studio-agent/sessions/${sessionId}?sync_pending=true`, {
            timeoutMs: 120_000,
        });
        await resumeSession((data?.session as Record<string, unknown>) || {});
    }, [authFetch, resumeSession, sessionId]);

    const approveAction = useCallback(
        async (actionId: string) => {
            if (!sessionId || currentSessionRunning) return;
            const approved = pending.find((a) => a.id === actionId);
            markSessionRunning(sessionId, 'Approving action...');
            setError('');
            stickToBottomRef.current = true;
            try {
                await syncSessionFromServer();
                const data = await authFetch(`/api/studio-agent/sessions/${sessionId}/approve`, {
                    method: 'POST',
                    body: JSON.stringify({ action_id: actionId }),
                    timeoutMs: 120_000,
                });
                setPending((p) => p.filter((a) => a.id !== actionId));
                const approvedAction = data?.approved_action as {
                    tool?: string;
                    error?: string;
                } | undefined;
                if (approvedAction?.error) {
                    setError(`${approvedAction.tool || 'Action'} failed: ${approvedAction.error}`);
                }
                const reply = String(data?.assistant_message || '').trim();
                if (reply) {
                    setMessages((m) => [...m, { role: 'assistant', content: reply }]);
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
                setPending((data?.pending_actions as PendingAction[]) || []);
                ingestActiveJobs(data?.active_jobs, sessionId);
                setDockDismissed(false);
            } catch (e) {
                setError((e as Error).message);
                if (approved) setPending((p) => [...p, approved]);
            } finally {
                clearSessionRunning(sessionId);
            }
        },
        [authFetch, clearSessionRunning, currentSessionRunning, ingestActiveJobs, markSessionRunning, pending, sessionId, syncSessionFromServer],
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
            } catch (e) {
                setError((e as Error).message);
            } finally {
                clearSessionRunning(sessionId);
            }
        },
        [authFetch, clearSessionRunning, currentSessionRunning, markSessionRunning, sessionId],
    );

    const handleRetryProduction = useCallback(async () => {
        if (!sessionId || currentSessionRunning || retryingProduction) return;
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
    }, [authFetch, currentSessionRunning, retryingProduction, sessionId]);

    const handleCancelProduction = useCallback(async () => {
        if (!dockTrack?.job_id || cancellingProduction) return;
        userCancelledJobsRef.current.add(dockTrack.job_id);
        setCancellingProduction(true);
        try {
            const tok = await getToken();
            await cancelJob(dockTrack.job_id, dockTrack.kind, tok, sessionId);
            setMessages((m) => [
                ...m,
                { role: 'assistant' as const, content: 'Cancelling the render — it will stop at the next scene. No further fal spend.' },
            ]);
        } catch (e) {
            setError((e as Error).message);
        } finally {
            setCancellingProduction(false);
        }
    }, [cancellingProduction, dockTrack, getToken, sessionId]);

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
                onSelect={(id) => {
                    setModel(id);
                    patchSession({ model: id });
                }}
                onClose={() => setModelPickerOpen(false)}
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

            <div className="flex min-h-0 flex-1 overflow-hidden bg-black">
                <aside
                    className={`flex shrink-0 flex-col border-r border-white/[0.07] bg-[#050505] transition-all duration-200 ${
                        historyOpen ? 'w-[244px]' : 'w-[56px]'
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
                            onClick={() => createNewSession(model)}
                            className="mb-2 flex h-10 w-full items-center gap-2 rounded-xl bg-white/[0.08] px-3 text-sm font-semibold text-white transition hover:bg-white/[0.12]"
                        >
                            <MessageSquarePlus className="h-4 w-4" />
                            New chat
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
                                onClick={() => createNewSession(model)}
                                className="grid h-10 w-10 place-items-center rounded-xl text-gray-400 transition hover:bg-violet-500/15 hover:text-violet-200"
                            >
                                <MessageSquarePlus className="h-4 w-4" />
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
                            title="Reload chat from server (transcript, pending approvals, renders). Works even if the agent was stuck loading."
                            disabled={resuming || booting}
                            onClick={() => void reloadCurrentSession()}
                            className="inline-flex items-center gap-1 rounded-lg border border-white/[0.06] px-2 py-1 text-[9px] font-semibold uppercase text-gray-400 transition hover:bg-white/[0.06] hover:text-white disabled:opacity-40"
                        >
                            <RefreshCw className={`h-3 w-3 ${resuming ? 'animate-spin' : ''}`} />
                            {resuming ? 'Resuming…' : 'Resume'}
                        </button>
                        <button
                            type="button"
                            title="Roll transcript into a new session"
                            disabled={currentSessionRunning || !sessionId}
                            onClick={() => void rolloverSession()}
                            className="inline-flex items-center gap-1 rounded-lg border border-white/[0.06] px-2 py-1 text-[9px] font-semibold uppercase text-gray-400 transition hover:bg-violet-500/15 hover:text-violet-200 disabled:opacity-40"
                        >
                            <RotateCcw className="h-3 w-3" />
                            Roll over
                        </button>
                        {/* Decluttered: removed Short/Long/Auto tabs (user request). Agent now infers from chat text ("make a 45s short about X" or "12-min documentary"). This makes the header much cleaner. */}
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
                                                        const showVideo = isActive && Boolean(s.preview_video_url);
                                                        return (
                                                            <button
                                                                key={s.key}
                                                                type="button"
                                                                onMouseEnter={() => setActiveStylePreview(s.key)}
                                                                onFocus={() => setActiveStylePreview(s.key)}
                                                                onMouseLeave={() => setActiveStylePreview('')}
                                                                onClick={() => {
                                                                    setRenderStyle(s.key);
                                                                    void patchSession({ render_style: s.key });
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
                                                                            src={s.preview_video_url}
                                                                            poster={s.preview_url}
                                                                            className="h-full w-full object-cover"
                                                                            autoPlay
                                                                            muted
                                                                            loop
                                                                            playsInline
                                                                            preload="none"
                                                                        />
                                                                    ) : s.preview_url ? (
                                                                        <img
                                                                            src={s.preview_url}
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

                <AgentProductionRail tracks={jobTracks} snapshots={snapshots} />

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
                        <button
                            type="button"
                            onClick={() => setError('')}
                            className="shrink-0 text-[10px] text-red-300 hover:text-white"
                        >
                            Dismiss
                        </button>
                    </div>
                )}
                {(dictation.error || dictationPreview) && (
                    <p
                        className={`mb-2 shrink-0 rounded-lg border px-3 py-2 text-xs leading-relaxed ${
                            dictation.error
                                ? 'border-amber-500/30 bg-amber-500/10 text-amber-100'
                                : 'border-violet-500/20 bg-violet-500/5 text-violet-100'
                        }`}
                    >
                        {dictation.error || dictationPreview}
                    </p>
                )}

                <div
                    ref={scrollRef}
                    onScroll={handleScroll}
                    className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-2 sm:px-3"
                >
                    <div className="mx-auto max-w-4xl space-y-4 pb-8 pt-4">
                        {messages.length === 0 && (
                            <div className="flex min-h-[56vh] flex-col items-center justify-center px-4 text-center">
                                {/* Ultra-premium Hero */}
                                <div className="mb-5 flex h-14 w-14 items-center justify-center rounded-2xl border border-white/10 bg-white/[0.06]">
                                    <Sparkles className="h-7 w-7 text-white" />
                                </div>
                                <div className="mb-1.5 inline-flex items-center gap-2 rounded-full border border-white/20 bg-white/5 py-1 pl-2 pr-3 text-[9px] font-semibold uppercase tracking-[1.8px] text-white/70">
                                    <div className="flex items-center gap-1.5 rounded-full bg-emerald-500/20 px-2 py-px text-emerald-400">
                                        <div className="h-1 w-1 rounded-full bg-emerald-400 animate-pulse" /> LIVE
                                    </div>
                                    PREMIUM REAL-TIME VIDEO STUDIO
                                </div>
                                <h1 className="text-4xl font-semibold tracking-tight text-white sm:text-5xl">What do you want to create?</h1>
                                <p className="mb-1 max-w-[620px] text-[12px] leading-relaxed text-gray-400 sm:text-[13px]">
                                    This is the experience your plan unlocks. The agent doesn&apos;t just generate — it <span className="text-white font-medium">builds your video live inside this chat</span>. You watch every decision, every still, every motion clip, every audio layer appear in real time. Full transparency. Full control. Premium quality, delivered visibly.
                                </p>
                                <div className="mb-3 mt-1 flex items-center gap-2 text-[9px] text-white/50 sm:gap-3">
                                    <div>Sub-second updates</div>
                                    <div className="h-px w-3 bg-white/20" />
                                    <div>Per-scene creative control</div>
                                    <div className="h-px w-3 bg-white/20" />
                                    <div>Production-grade output</div>
                                </div>

                                {/* Premium, luxurious starter journeys */}
                                <div className="grid w-full max-w-[720px] grid-cols-1 gap-2 sm:grid-cols-2">
                                    {STARTER_PROMPTS.map((p, index) => {
                                        const icons = [Video, BookOpen, Users, Zap];
                                        const labels = ["VIRAL SHORTS", "REFERENCE-LED", "LONG-FORM DOCS", "SIGNATURE STYLE"];
                                        const sub = [
                                            "Audit → rank → script → render live",
                                            "Paste any video → blueprint + build",
                                            "Outline → chapter stills → finalize",
                                            "Outcast + Seedance in one flow"
                                        ];
                                        const Icon = icons[index % icons.length];
                                        return (
                                            <button
                                                key={index}
                                                type="button"
                                                disabled={!sessionId}
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
                        {messages
                            .filter((m) => m.role === 'user' || m.role === 'assistant')
                            .map((m, i) => {
                                if (m.productionUpdate) {
                                    return (
                                        <AgentProgressBubble
                                            key={`progress-${m.productionUpdate.job_id}-${i}`}
                                            update={m.productionUpdate}
                                        />
                                    );
                                }
                                const text = String(m.content ?? '');
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
                                            {m.jobDeliverable && (
                                                <div className="mt-2 -mx-1">
                                                    <AgentJobDeliverable
                                                        snapshot={m.jobDeliverable}
                                                        onFinalizeStarted={(jid, jobs) =>
                                                            handleFinalizeStarted(jid, jobs)
                                                        }
                                                        onReply={handleReplyToJob}
                                                        onSnapshotUpdate={appendJobDeliverable}
                                                    />
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                );
                            })}
                        {currentSessionRunning && (
                            <div className="space-y-1 text-xs text-gray-500">
                                <div className="flex items-center gap-2">
                                    <Loader2 className="h-3 w-3 animate-spin" />
                                    {queueHint || toolActivity || 'Thinking…'}
                                </div>
                                {dockSnap && dockSnap.running !== false && !dockDismissed ? (
                                    <p className="text-cyan-300/90">
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

                {pending.length > 0 && (
                    <div className="relative z-30 mx-auto w-full max-w-3xl shrink-0 border-t-2 border-amber-500/60 bg-gradient-to-t from-[#1a1408] to-[#12100a] px-3 py-3 shadow-[0_-12px_40px_rgba(0,0,0,0.65)]">
                        <div className="mb-2 flex items-center justify-between gap-2">
                            <p className="flex items-center gap-1.5 text-xs font-semibold text-amber-100">
                                <Zap className="h-4 w-4 text-amber-400" />
                                Approval required — tap the green button to run production
                            </p>
                            <button
                                type="button"
                                disabled={resuming}
                                onClick={() => void reloadCurrentSession()}
                                className="shrink-0 rounded-lg border border-amber-500/30 px-2 py-1 text-[10px] font-semibold text-amber-200 hover:bg-amber-500/15 disabled:opacity-50"
                            >
                                {resuming ? 'Syncing…' : 'Sync chat'}
                            </button>
                        </div>
                        <div className="space-y-2">
                            {pending.map((a) => (
                                <div
                                    key={a.id}
                                    className="rounded-xl border border-amber-500/35 bg-black/40 p-3"
                                >
                                    <p className="text-sm font-semibold text-white">
                                        {pendingActionLabel(a.tool)}
                                    </p>
                                    <p className="mt-1 line-clamp-3 text-[11px] text-gray-400">
                                        {formatPendingArgs(a.arguments) || a.summary || JSON.stringify(a.arguments)}
                                    </p>
                                    <div className="mt-3 flex flex-wrap gap-2">
                                        <button
                                            type="button"
                                            disabled={currentSessionRunning}
                                            onClick={() => approveAction(a.id)}
                                            className="inline-flex min-h-11 flex-1 items-center justify-center gap-2 rounded-xl bg-emerald-600 px-4 py-2.5 text-sm font-bold text-white shadow-lg shadow-emerald-900/40 transition hover:bg-emerald-500 disabled:opacity-50"
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
                                            className="inline-flex min-h-11 items-center justify-center gap-1 rounded-xl border border-white/15 px-4 py-2.5 text-xs font-semibold text-gray-300 hover:bg-white/[0.06]"
                                        >
                                            <X className="h-4 w-4" />
                                            Reject
                                        </button>
                                    </div>
                                </div>
                            ))}
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
                                    {f.kind === 'image' ? 'Image: ' : f.kind === 'text' ? 'File: ' : 'Unsupported: '}
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
                        <textarea
                            ref={inputRef}
                            className="max-h-36 min-h-[52px] w-full resize-none bg-transparent px-4 pt-3.5 text-sm text-white placeholder:text-gray-600 focus:outline-none"
                            placeholder={
                                dictation.listening
                                    ? 'Listening… tap mic to stop'
                                    : dictation.transcribing
                                      ? 'Transcribing your voice…'
                                      : 'Talk to the agent — type, dictate (mic), or kick off a video'
                            }
                            rows={1}
                            value={input}
                            disabled={!sessionId || dictation.transcribing}
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
                                accept="image/*,.txt,.md,.json,.csv,.py,.ts,.tsx,.js,.jsx,.yaml,.yml"
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
                            <button
                                type="button"
                                disabled={!sessionId || !dictation.supported || dictation.transcribing || currentSessionRunning}
                                onClick={() => dictation.toggle()}
                                className={`rounded-lg p-2 transition ${
                                    dictation.listening
                                        ? 'bg-rose-600/20 text-rose-300 ring-1 ring-rose-500/40'
                                        : 'text-gray-400 hover:bg-white/[0.06] hover:text-white'
                                } disabled:opacity-40`}
                                title={
                                    dictation.engine === 'record'
                                        ? 'Voice dictation (record then transcribe — Firefox)'
                                        : dictation.engine === 'webspeech'
                                          ? 'Voice dictation (live)'
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
                            <div className="flex-1" />
                            <button
                                type="button"
                                disabled={currentSessionRunning || (!input.trim() && !hasReadableAttachment) || !sessionId}
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
            {!dockDismissed ? (
                <AgentRenderDock
                    track={dockTrack ?? null}
                    snapshot={dockSnap}
                    accessToken={session?.access_token}
                    onRetry={handleRetryProduction}
                    retrying={retryingProduction}
                    onCancel={handleCancelProduction}
                    cancelling={cancellingProduction}
                    onDismiss={() => setDockDismissed(true)}
                />
            ) : null}
        </>
    );
}
