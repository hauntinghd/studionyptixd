/**
 * Studio Agent — full-screen chat (OpenRouter + Rookcast skills).
 */
import { useCallback, useContext, useEffect, useRef, useState } from 'react';
import {
    ArrowLeft, ArrowUp, BookOpen, Bot, Brain, Check, History, Loader2, MessageSquarePlus, Mic, MicOff,
    Palette, Paperclip, Play, RefreshCw, RotateCcw, Shield, ShieldOff, Sparkles, Trash2, Users, Video, X, Zap,
} from 'lucide-react';
import AgentJobDeliverable from '../components/agent/AgentJobDeliverable';
import AgentMessageBody from '../components/agent/AgentMessageBody';
import AgentProductionRail from '../components/agent/AgentProductionRail';
import AgentProgressBubble from '../components/agent/AgentProgressBubble';
import AgentRenderDock from '../components/agent/AgentRenderDock';
import AgentYouTubeConnect from '../components/agent/AgentYouTubeConnect';
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
import { streamAgentChat, toolLabel, type AgentStreamEvent } from '../lib/streamAgentChat';
import { useSpeechDictation } from '../hooks/useSpeechDictation';
// @ts-ignore - TS module resolution issue with shared exports in Vercel tsc build; exports exist at runtime
import * as Shared from '../shared.tsx';
// @ts-expect-error - module resolution quirk in build env, exports are present
const { AuthContext, resolveStudioBackendUrl } = Shared as any;
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

interface AttachedFile {
    id: string;
    name: string;
    size: number;
}

const FALLBACK_MODELS: AgentModelOption[] = [
    {
        id: 'anthropic/claude-sonnet-4',
        name: 'Claude Sonnet 4',
        provider: 'Anthropic',
        recommended: true,
        intelligence: 5,
        speed: 4,
        description: 'Strong tool use and production planning.',
    },
    {
        id: 'google/gemini-2.0-flash-001',
        name: 'Gemini 2.0 Flash',
        provider: 'Google',
        recommended: true,
        intelligence: 4,
        speed: 5,
        description: 'Fast, cheap runner for high-volume iteration.',
    },
    {
        id: 'openai/gpt-4o',
        name: 'GPT-4o',
        provider: 'OpenAI',
        recommended: true,
        intelligence: 5,
        speed: 4,
    },
    {
        id: 'deepseek/deepseek-chat',
        name: 'DeepSeek Chat',
        provider: 'DeepSeek',
        recommended: true,
        intelligence: 4,
        speed: 5,
    },
];

type ContentFormat = 'short' | 'long' | 'both';
type ReasoningDepth = 'fast' | 'balanced' | 'deep';

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
}

const FALLBACK_RENDER_STYLES: RenderStyleOption[] = [
    { key: 'cinematic', label: 'Cinematic', group: 'Realism' },
    { key: 'ultra_realism', label: 'Ultra realism', group: 'Realism' },
    { key: 'comic_book', label: 'Comic book (color)', group: 'Comic' },
    { key: 'bw_comic', label: 'B&W comic', group: 'Comic' },
    { key: 'studio_ghibli', label: 'Studio Ghibli', group: 'Animation' },
    { key: 'pixar', label: 'Pixar', group: 'Animation' },
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
        return detail || 'Studio is at capacity (OpenRouter + fal). Your request is queued — try again shortly.';
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
    const [accountBadge, setAccountBadge] = useState('');
    const [sessionId, setSessionId] = useState<string | null>(null);
    const [model, setModel] = useState(FALLBACK_MODELS[0].id);
    const [modelCatalog, setModelCatalog] = useState<AgentModelOption[]>(FALLBACK_MODELS);
    const [modelPickerOpen, setModelPickerOpen] = useState(false);
    const [approvalMode, setApprovalMode] = useState<ApprovalMode>('confirm');
    const [messages, setMessages] = useState<ChatMessage[]>([]);
    const [pending, setPending] = useState<PendingAction[]>([]);
    const [input, setInput] = useState('');
    const [attachments, setAttachments] = useState<AttachedFile[]>([]);
    const [attachmentPayload, setAttachmentPayload] = useState<Record<string, string>>({});
    const [loading, setLoading] = useState(false);
    const [resuming, setResuming] = useState(false);
    const [toolActivity, setToolActivity] = useState('');
    const [booting, setBooting] = useState(true);
    const [history, setHistory] = useState<SessionSummary[]>([]);
    const [historyOpen, setHistoryOpen] = useState(true);
    const [contentFormat, setContentFormat] = useState<ContentFormat>('both');
    const [reasoningDepth, setReasoningDepth] = useState<ReasoningDepth>('balanced');
    const [renderStyle, setRenderStyle] = useState('cinematic');
    const [animate, setAnimate] = useState(true);
    const [showStyleGrid, setShowStyleGrid] = useState(false);
    const [channelsOpen, setChannelsOpen] = useState(false);
    const [replyingTo, setReplyingTo] = useState<AgentJobSnapshot | null>(null);
    const [renderStyleCatalog, setRenderStyleCatalog] = useState<RenderStyleOption[]>(FALLBACK_RENDER_STYLES);
    const [error, setError] = useState('');
    const [queueHint, setQueueHint] = useState('');
    const [dictationPreview, setDictationPreview] = useState('');
    const scrollRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLTextAreaElement>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);
    const stickToBottomRef = useRef(true);
    const [jobTracks, setJobTracks] = useState<AgentJobTrack[]>([]);
    const [dockDismissed, setDockDismissed] = useState(false);
    const [pollResetKey, setPollResetKey] = useState(0);
    const [retryingProduction, setRetryingProduction] = useState(false);
    const [cancellingProduction, setCancellingProduction] = useState(false);

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
            const dup = m.some(
                (row) =>
                    row.jobDeliverable?.job_id === snap.job_id
                    && row.jobDeliverable?.status === snap.status,
            );
            if (dup) return m;
            return [
                ...m,
                {
                    role: 'assistant' as const,
                    content: label,
                    jobDeliverable: snap,
                },
            ];
        });
        stickToBottomRef.current = true;
    }, [approvalMode]);

    const upsertProgressLine = useCallback((update: ProductionProgressUpdate) => {
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
        (snapshot: AgentJobSnapshot) => {
            setReplyingTo(snapshot);
            const kindLabel = snapshot.kind === 'longform' ? 'long-form' : 'short-form';
            const suggested =
                `Please re-edit this ${kindLabel} video and make sure it has proper editing, pacing, storytelling, and packaging + a CTA at the end to get people to subscribe.`;
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
            setDockDismissed(false);
            appendJobDeliverable(snap);
        },
        onJobFailed: (snap: AgentJobSnapshot) => {
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
    }, [messages, pending, loading, scrollToBottom]);

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
            setSessionId(sid);
            try {
                localStorage.setItem(lastSessionKey, sid);
            } catch {
                /* ignore */
            }
        }
        const msgs = (raw.messages as ChatMessage[]) || [];
        setMessages(msgs);
        const serverPending = (raw.pending_actions as PendingAction[]) || [];
        setPending(mergePendingFromTranscript(msgs, serverPending));
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
        if (typeof raw.animate === 'boolean') setAnimate(raw.animate);
    }, [lastSessionKey]);

    const resumeSession = useCallback(
        async (raw: Record<string, unknown>, opts?: { rehydrateJobs?: boolean }) => {
            applySessionPayload(raw);
            const sid = String(raw.session_id || '');
            if (!sid) return;
            const serverJobs = Array.isArray(raw.active_jobs) ? (raw.active_jobs as AgentJobTrack[]) : [];
            const merged = mergeJobTracks(loadPersistedJobs(sid), serverJobs);
            if (!merged.length) return;
            setJobTracks(merged);
            persistJobs(sid, merged);
            setDockDismissed(false);
            if (opts?.rehydrateJobs === false) return;
            try {
                const tok = await getToken();
                const { deliverables } = await rehydrateJobSnapshots(sid, merged, tok);
                for (const snap of deliverables) appendJobDeliverable(snap);
            } catch {
                /* polling optional on resume */
            }
        },
        [applySessionPayload, appendJobDeliverable, getToken],
    );

    const refreshHistory = useCallback(async () => {
        const data = await authFetch('/api/studio-agent/sessions?limit=50');
        setHistory((data?.sessions as SessionSummary[]) || []);
    }, [authFetch]);

    const createNewSession = useCallback(
        async (pickModel: string) => {
            const created = await authFetch('/api/studio-agent/sessions', {
                method: 'POST',
                body: JSON.stringify({
                    model: pickModel,
                    approval_mode: approvalMode,
                    content_format: contentFormat,
                    reasoning_depth: reasoningDepth,
                    render_style: renderStyle,
                }),
            });
            applySessionPayload((created.session as Record<string, unknown>) || {});
            setJobTracks([]);
            setDockDismissed(true);
            await refreshHistory();
        },
        [applySessionPayload, approvalMode, authFetch, contentFormat, reasoningDepth, renderStyle, refreshHistory],
    );

    const openSession = useCallback(
        async (id: string) => {
            if (!id || resuming) return;
            setResuming(true);
            setError('');
            try {
                const data = await authFetch(`/api/studio-agent/sessions/${id}`, {
                    timeoutMs: 30_000,
                });
                await resumeSession((data?.session as Record<string, unknown>) || {});
            } catch (e) {
                setError((e as Error).message);
            } finally {
                setResuming(false);
                setLoading(false);
            }
        },
        [authFetch, resumeSession, resuming],
    );

    const reloadCurrentSession = useCallback(async () => {
        if (resuming) return;
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
            const data = await authFetch(`/api/studio-agent/sessions/${id}`, {
                timeoutMs: 30_000,
            });
            await resumeSession((data?.session as Record<string, unknown>) || {}, {
                rehydrateJobs: true,
            });
        } catch (e) {
            setError((e as Error).message);
        } finally {
            setResuming(false);
            setLoading(false);
        }
    }, [authFetch, history, lastSessionKey, resumeSession, resuming, sessionId]);

    const rolloverSession = useCallback(async () => {
        if (!sessionId || loading) return;
        if (
            !window.confirm(
                'Roll this chat into a new session? Your full transcript, pending approvals, and active renders carry over.',
            )
        ) {
            return;
        }
        setLoading(true);
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
            setLoading(false);
        }
    }, [authFetch, loading, refreshHistory, resumeSession, sessionId]);

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

    const deleteSession = useCallback(
        async (id: string) => {
            if (!id || loading) return;
            if (!window.confirm('Delete this chat? This cannot be undone.')) return;
            setLoading(true);
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
                setLoading(false);
            }
        },
        [authFetch, createNewSession, lastSessionKey, loading, model, openSession, sessionId],
    );

    useEffect(() => {
        if (ownerOverride) {
            setAccountBadge('Owner — unmetered');
            return;
        }
        let cancelled = false;
        (async () => {
            try {
                const cred = await authFetch('/api/studio-agent/credits');
                if (cancelled) return;
                if (cred?.unlimited || cred?.tier === 'owner') {
                    setAccountBadge('Owner — unmetered');
                } else if (cred?.plan_name) {
                    setAccountBadge(`${cred.plan_name} · ${Number(cred.balance || 0).toLocaleString()} cr`);
                }
            } catch {
                if (!cancelled) setAccountBadge('');
            }
        })();
        return () => { cancelled = true; };
    }, [authFetch, ownerOverride]);

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
                                provider: id.split('/')[0] || 'OpenRouter',
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
                    const data = await authFetch(`/api/studio-agent/sessions/${resume.session_id}`);
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
                if (!cancelled && styles.length) setRenderStyleCatalog(styles);
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
            web_search?: boolean;
            animate?: boolean;
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

    const buildOutboundMessage = useCallback(
        (text: string) => {
            const parts = [text.trim()];
            for (const f of attachments) {
                const body = attachmentPayload[f.id];
                if (!body) continue;
                parts.push(`\n\n[Attachment: ${f.name}]\n${body.slice(0, 12000)}`);
            }
            return parts.join('');
        },
        [attachmentPayload, attachments],
    );

    const pollQueueWhileLoading = useCallback(async () => {
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
                        ? `High load — ~${wait} ahead of you (${active}/${max} active). OpenRouter + fal are queued…`
                        : `High load — ${active}/${max} concurrent sessions. Waiting for a slot…`,
                );
            } else {
                setQueueHint('');
            }
        } catch {
            setQueueHint('');
        }
    }, [authFetch]);

    const sendText = useCallback(
        async (text: string) => {
            const trimmed = buildOutboundMessage(text);
            if (!trimmed || !sessionId || loading) return;
            setInput('');
            setAttachments([]);
            setAttachmentPayload({});
            setLoading(true);
            setError('');
            setQueueHint('');
            setToolActivity('');
            stickToBottomRef.current = true;
            setMessages((m) => [...m, { role: 'user', content: text.trim() }]);
            const queuePoll = window.setInterval(() => {
                void pollQueueWhileLoading();
            }, 2500);
            void pollQueueWhileLoading();
            try {
                const tok = await getToken();
                const onStreamEvent = (ev: AgentStreamEvent) => {
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
                        ingestActiveJobs(ev.jobs, sessionId);
                    } else if (ev.event === 'pending_actions' && Array.isArray(ev.actions)) {
                        setPending(ev.actions as PendingAction[]);
                    }
                };

                let data: Record<string, unknown>;
                try {
                    data = await streamAgentChat(sessionId, trimmed, tok, { onEvent: onStreamEvent, replyTo: replyingTo ? { job_id: replyingTo.job_id, kind: replyingTo.kind } : undefined });
                } catch {
                    data = await authFetch(`/api/studio-agent/sessions/${sessionId}/chat`, {
                        method: 'POST',
                        body: JSON.stringify({ 
                            message: trimmed,
                            reply_to: replyingTo ? { job_id: replyingTo.job_id, kind: replyingTo.kind } : undefined 
                        }),
                    });
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
                ingestActiveJobs(data?.active_jobs, sessionId);
                await refreshHistory();
            } catch (e) {
                setError((e as Error).message);
                setQueueHint('');
            } finally {
                window.clearInterval(queuePoll);
                setLoading(false);
                setToolActivity('');
            }
        },
        [
            authFetch,
            buildOutboundMessage,
            getToken,
            ingestActiveJobs,
            loading,
            pollQueueWhileLoading,
            refreshHistory,
            sessionId,
        ],
    );

    const sendMessage = useCallback(() => sendText(input), [input, sendText]);

    const onPickFiles = useCallback(async (files: FileList | null) => {
        if (!files?.length) return;
        const next: AttachedFile[] = [];
        const payload: Record<string, string> = { ...attachmentPayload };
        for (const file of Array.from(files)) {
            const id = `f_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
            next.push({ id, name: file.name, size: file.size });
            const isText = file.type.startsWith('text/')
                || /\.(md|txt|json|csv|py|ts|tsx|js|jsx|yaml|yml)$/i.test(file.name);
            if (isText) {
                payload[id] = await file.text();
            } else {
                payload[id] = `[Binary file ${file.name}, ${Math.round(file.size / 1024)}KB — describe how to use it]`;
            }
        }
        setAttachments((a) => [...a, ...next]);
        setAttachmentPayload(payload);
        if (fileInputRef.current) fileInputRef.current.value = '';
    }, [attachmentPayload]);

    const syncSessionFromServer = useCallback(async () => {
        if (!sessionId) return;
        const data = await authFetch(`/api/studio-agent/sessions/${sessionId}`, {
            timeoutMs: 30_000,
        });
        await resumeSession((data?.session as Record<string, unknown>) || {});
    }, [authFetch, resumeSession, sessionId]);

    const approveAction = useCallback(
        async (actionId: string) => {
            if (!sessionId || loading) return;
            const approved = pending.find((a) => a.id === actionId);
            setLoading(true);
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
                setLoading(false);
            }
        },
        [authFetch, ingestActiveJobs, loading, pending, sessionId, syncSessionFromServer],
    );

    const rejectAction = useCallback(
        async (actionId: string) => {
            if (!sessionId || loading) return;
            setLoading(true);
            try {
                await authFetch(`/api/studio-agent/sessions/${sessionId}/reject`, {
                    method: 'POST',
                    body: JSON.stringify({ action_id: actionId, reason: 'Rejected by user' }),
                });
                setPending((p) => p.filter((a) => a.id !== actionId));
            } catch (e) {
                setError((e as Error).message);
            } finally {
                setLoading(false);
            }
        },
        [authFetch, loading, sessionId],
    );

    const handleRetryProduction = useCallback(async () => {
        if (!sessionId || loading || retryingProduction) return;
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
    }, [authFetch, loading, retryingProduction, sessionId]);

    const handleCancelProduction = useCallback(async () => {
        if (!dockTrack?.job_id || cancellingProduction) return;
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

            <div className="flex min-h-0 flex-1 overflow-hidden">
                <aside
                    className={`flex shrink-0 flex-col border-r border-white/[0.06] bg-[#08080a] transition-all ${
                        historyOpen ? 'w-56' : 'w-0 overflow-hidden border-r-0'
                    }`}
                >
                    <div className="flex items-center justify-between gap-1 border-b border-white/[0.06] p-2">
                        <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                            History
                        </span>
                        <button
                            type="button"
                            title="New chat"
                            disabled={loading}
                            onClick={() => createNewSession(model)}
                            className="rounded-lg p-1.5 text-gray-400 transition hover:bg-violet-500/15 hover:text-violet-200 disabled:opacity-40"
                        >
                            <MessageSquarePlus className="h-4 w-4" />
                        </button>
                    </div>
                    <div className="min-h-0 flex-1 overflow-y-auto p-1.5">
                        {history.length === 0 && (
                            <p className="px-2 py-3 text-[10px] text-gray-600">No chats yet — start one.</p>
                        )}
                        {history.map((s) => {
                            const active = s.session_id === sessionId;
                            return (
                                <div
                                    key={s.session_id}
                                    className={`group mb-1 flex items-stretch gap-0.5 rounded-lg transition ${
                                        active ? 'bg-violet-500/20' : 'hover:bg-white/[0.05]'
                                    }`}
                                >
                                    <button
                                        type="button"
                                        disabled={loading}
                                        onClick={() => openSession(s.session_id)}
                                        className={`min-w-0 flex-1 rounded-lg px-2.5 py-2 text-left ${
                                            active ? 'text-white' : 'text-gray-300'
                                        }`}
                                    >
                                        <p className="line-clamp-2 text-xs font-medium leading-snug">
                                            {s.title || 'New chat'}
                                        </p>
                                        <p className="mt-0.5 text-[9px] text-gray-500">
                                            {formatSessionAge(s.updated_at)}
                                            {(s.pending_count || 0) > 0
                                                ? ` · ${s.pending_count} pending`
                                                : ''}
                                        </p>
                                    </button>
                                    <button
                                        type="button"
                                        title="Delete chat"
                                        disabled={loading}
                                        onClick={() => deleteSession(s.session_id)}
                                        className="shrink-0 rounded-lg px-1.5 py-2 text-gray-500 opacity-70 transition hover:bg-rose-500/15 hover:text-rose-300 group-hover:opacity-100 disabled:opacity-40"
                                    >
                                        <Trash2 className="h-3.5 w-3.5" />
                                    </button>
                                </div>
                            );
                        })}
                    </div>
                </aside>

            <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
                <header className="mb-2 flex shrink-0 flex-wrap items-center gap-2 border-b border-white/[0.06] pb-2">
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
                        <Bot className="h-5 w-5 text-violet-400" />
                        <h1 className="text-sm font-semibold text-white">Studio Agent</h1>
                        <span className="rounded bg-violet-500/15 px-1.5 py-0.5 text-[9px] font-bold uppercase text-violet-300">
                            Beta
                        </span>
                        {accountBadge && (
                            <span
                                className={`rounded px-1.5 py-0.5 text-[9px] font-semibold uppercase ${
                                    accountBadge.startsWith('Owner')
                                        ? 'bg-amber-500/15 text-amber-200'
                                        : 'bg-cyan-500/10 text-cyan-200'
                                }`}
                            >
                                {accountBadge}
                            </span>
                        )}
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
                            disabled={loading || !sessionId}
                            onClick={() => void rolloverSession()}
                            className="inline-flex items-center gap-1 rounded-lg border border-white/[0.06] px-2 py-1 text-[9px] font-semibold uppercase text-gray-400 transition hover:bg-violet-500/15 hover:text-violet-200 disabled:opacity-40"
                        >
                            <RotateCcw className="h-3 w-3" />
                            Roll over
                        </button>
                        {/* Decluttered: removed Short/Long/Auto tabs (user request). Agent now infers from chat text ("make a 45s short about X" or "12-min documentary"). This makes the header much cleaner. */}
                        <div
                            className="flex items-center gap-0.5 rounded-lg border border-white/[0.06] bg-white/[0.02] p-0.5"
                            title="How deeply the model reasons (OpenRouter)"
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
                        {/* Visual Style Grid - now looks like the premium reference galleries (distinct previews per style, cheap Seedream) */}
                        <div className="relative">
                            <button
                                type="button"
                                onClick={() => setShowStyleGrid(!showStyleGrid)}
                                className="flex items-center gap-1.5 rounded-lg border border-white/[0.06] bg-white/[0.02] px-2 py-0.5 text-[9px] font-semibold uppercase text-violet-200 hover:bg-white/5"
                                title="Art style (visual gallery - click to pick, uses Seedream v4.5 previews)"
                            >
                                <Palette className="h-3 w-3 text-violet-300" />
                                {renderStyleCatalog.find(s => s.key === renderStyle)?.label || renderStyle}
                            </button>
                            {showStyleGrid && (
                                <div className="absolute right-0 z-[60] mt-1 w-[520px] max-h-[420px] overflow-auto rounded-2xl border border-white/10 bg-[#0b0b11] p-3 shadow-2xl text-xs">
                                    <div className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-violet-400/80">Choose Art Style (visual previews - 1 cheap Seedream per style)</div>
                                    {['Realism', 'Comic', 'Animation', 'Specialty', 'Niche'].map(group => {
                                        const items = renderStyleCatalog.filter(s => s.group === group);
                                        if (!items.length) return null;
                                        return (
                                            <div key={group} className="mb-3">
                                                <div className="mb-1 text-[9px] font-semibold uppercase tracking-wider text-white/50">{group}</div>
                                                <div className="grid grid-cols-4 gap-2">
                                                    {items.map(s => (
                                                        <button
                                                            key={s.key}
                                                            onClick={() => {
                                                                setRenderStyle(s.key);
                                                                void patchSession({ render_style: s.key });
                                                                setShowStyleGrid(false);
                                                            }}
                                                            className={`group overflow-hidden rounded-xl border text-left transition ${renderStyle === s.key ? 'border-violet-500 ring-1 ring-violet-500/40' : 'border-white/10 hover:border-white/30'}`}
                                                        >
                                                            {s.preview_url ? (
                                                                <img src={s.preview_url} alt={s.label} className="h-14 w-full object-cover bg-black/40" />
                                                            ) : (
                                                                <div className="h-14 w-full bg-white/5" />
                                                            )}
                                                            <div className="px-1 py-0.5 text-[9px] text-white/90 group-hover:text-white truncate">{s.label}</div>
                                                        </button>
                                                    ))}
                                                </div>
                                            </div>
                                        );
                                    })}
                                </div>
                            )}
                        </div>
                        <div
                            className="flex items-center gap-0.5 rounded-lg border border-white/[0.06] bg-white/[0.02] p-0.5"
                            title="Animate: i2v motion per scene. Stills: still images with a gentle Ken Burns push (cheaper, no motion)."
                        >
                            {([['animate', 'Animate'], ['stills', 'Stills']] as const).map(([id, label]) => {
                                const on = (id === 'animate') === animate;
                                return (
                                    <button
                                        key={id}
                                        type="button"
                                        onClick={() => {
                                            const next = id === 'animate';
                                            setAnimate(next);
                                            void patchSession({ animate: next });
                                        }}
                                        className={`rounded-md px-2 py-0.5 text-[9px] font-semibold uppercase transition ${
                                            on ? 'bg-emerald-600/25 text-emerald-200' : 'text-gray-500 hover:text-gray-300'
                                        }`}
                                    >
                                        {label}
                                    </button>
                                );
                            })}
                        </div>
                        {/* Channels as a "window" (click to open, like model picker). Not always taking space in the main UI. */}
                        <button
                            type="button"
                            onClick={() => setChannelsOpen((o) => !o)}
                            className={`inline-flex items-center gap-1.5 rounded-lg border px-2 py-1 text-[9px] font-semibold uppercase transition ${
                                channelsOpen
                                    ? 'border-violet-500/40 bg-violet-500/10 text-violet-200'
                                    : 'border-white/[0.06] bg-white/[0.02] text-gray-400 hover:bg-white/[0.06] hover:text-white'
                            }`}
                            title="Connected YouTube channels (click to manage / add)"
                        >
                            <Users className="h-3 w-3" />
                            Channels
                        </button>
                    </div>
                </header>

                <AgentProductionRail tracks={jobTracks} snapshots={snapshots} />

                {/* Channel connect as a toggleable "window" (not always visible clutter). Matches the model picker pattern. */}
                {channelsOpen && (
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
                    <div className="mx-auto max-w-3xl space-y-3 pb-6 pt-2">
                        {messages.length === 0 && (
                            <div className="flex flex-col items-center justify-center py-6 text-center sm:py-10">
                                {/* Ultra-premium Hero */}
                                <div className="mb-4 flex h-20 w-20 items-center justify-center rounded-[28px] bg-gradient-to-br from-violet-500/50 via-cyan-400/40 to-violet-500/50 ring-[6px] ring-white/10 shadow-[0_0_80px_rgba(139,92,246,0.25)]">
                                    <Sparkles className="h-10 w-10 text-white drop-shadow-lg" />
                                </div>
                                <div className="inline-flex items-center gap-2.5 rounded-full border border-white/20 bg-white/5 pl-2.5 pr-4 py-1 text-[10px] font-semibold uppercase tracking-[2.5px] text-white/70 mb-2">
                                    <div className="flex items-center gap-1.5 rounded-full bg-emerald-500/20 px-2 py-px text-emerald-400">
                                        <div className="h-1 w-1 rounded-full bg-emerald-400 animate-pulse" /> LIVE
                                    </div>
                                    PREMIUM REAL-TIME VIDEO STUDIO
                                </div>
                                <h1 className="text-[42px] sm:text-[48px] font-semibold tracking-[-2.2px] text-white leading-none mb-3">What are we shipping today?</h1>
                                <p className="max-w-[620px] text-[15px] text-gray-400 leading-relaxed mb-1">
                                    This is the experience your plan unlocks. The agent doesn&apos;t just generate — it <span className="text-white font-medium">builds your video live inside this chat</span>. You watch every decision, every still, every motion clip, every audio layer appear in real time. Full transparency. Full control. Premium quality, delivered visibly.
                                </p>
                                <div className="flex items-center gap-3 text-[10px] text-white/50 mt-1 mb-5">
                                    <div>Sub-second updates</div>
                                    <div className="h-px w-3 bg-white/20" />
                                    <div>Per-scene creative control</div>
                                    <div className="h-px w-3 bg-white/20" />
                                    <div>Production-grade output</div>
                                </div>

                                {/* Premium, luxurious starter journeys */}
                                <div className="w-full max-w-[720px] grid grid-cols-1 sm:grid-cols-2 gap-3">
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
                                                className="group relative overflow-hidden flex flex-col items-start gap-3 rounded-3xl border border-white/10 bg-[#0a0a0f] p-5 text-left transition-all hover:border-white/25 hover:bg-[#111117] active:scale-[0.985] disabled:opacity-40"
                                            >
                                                <div className="flex w-full items-center justify-between">
                                                    <div className="rounded-2xl bg-white/5 p-2.5 text-violet-400 group-hover:bg-violet-500/10 transition">
                                                        <Icon className="h-5 w-5" />
                                                    </div>
                                                    <div className="text-[9px] font-mono uppercase tracking-[2px] text-violet-400/70 group-hover:text-violet-400">
                                                        {labels[index]}
                                                    </div>
                                                </div>
                                                <div className="pr-4">
                                                    <p className="text-[14px] font-semibold text-white leading-tight tracking-[-0.2px]">{p}</p>
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

                                <div className="mt-7 text-[10px] font-medium tracking-[1px] text-white/40 flex items-center gap-3">
                                    <div className="h-px flex-1 bg-white/10" />
                                    CREATOR $60 • STUDIO $200 • OWNER UNMETERED
                                    <div className="h-px flex-1 bg-white/10" />
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
                                            className={`max-w-[88%] rounded-3xl px-4 py-2.5 text-[13.5px] leading-relaxed shadow-sm transition-all duration-200 sm:max-w-[78%] ${
                                                isUser
                                                    ? 'bg-gradient-to-br from-violet-600 to-violet-500 text-white rounded-br-lg'
                                                    : 'bg-white/[0.035] text-gray-100 border border-white/[0.06] rounded-bl-lg'
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
                                                    />
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                );
                            })}
                        {loading && (
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
                                            disabled={loading}
                                            onClick={() => approveAction(a.id)}
                                            className="inline-flex min-h-11 flex-1 items-center justify-center gap-2 rounded-xl bg-emerald-600 px-4 py-2.5 text-sm font-bold text-white shadow-lg shadow-emerald-900/40 transition hover:bg-emerald-500 disabled:opacity-50"
                                        >
                                            {loading ? (
                                                <Loader2 className="h-4 w-4 animate-spin" />
                                            ) : (
                                                <Check className="h-4 w-4 stroke-[3]" />
                                            )}
                                            Approve & run
                                        </button>
                                        <button
                                            type="button"
                                            disabled={loading}
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

                <div className="mx-auto w-full max-w-3xl shrink-0 pb-1 pt-2">
                    {attachments.length > 0 && (
                        <div className="mb-2 flex flex-wrap gap-2">
                            {attachments.map((f) => (
                                <span
                                    key={f.id}
                                    className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-white/[0.04] px-2 py-1 text-[10px] text-gray-300"
                                >
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

                    <div className="rounded-2xl border border-white/[0.1] bg-[#0c0c0e] shadow-lg shadow-black/40">
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
                                disabled={!sessionId || !dictation.supported || dictation.transcribing || loading}
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
                                disabled={loading || !input.trim() || !sessionId}
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
