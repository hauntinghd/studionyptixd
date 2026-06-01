/**
 * Studio Agent — full-screen chat (OpenRouter + Rookcast skills).
 */
import { useCallback, useContext, useEffect, useRef, useState } from 'react';
import {
    ArrowLeft, ArrowUp, Bot, Check, Loader2, Paperclip, Shield, ShieldOff, Sparkles, X, Zap,
} from 'lucide-react';
import { API, AuthContext } from '../shared';
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

const STARTER_PROMPTS = [
    'Plan a CrypticScience verified long-form on CTR + SS deposits — load compliance-preflight first.',
    'Outline a 60s ZeroTier short using outlier-mining and thumbnail-design skills.',
    'Pull channel analytics for cryptic_science and suggest 3 topics from public search trends.',
];

function displayModelName(models: AgentModelOption[], id: string) {
    return models.find((m) => m.id === id)?.name || id.split('/').pop()?.replace(/-/g, ' ') || id;
}

function friendlyApiError(status: number, data: Record<string, unknown>, fallback: string) {
    const detail = String(data?.detail || data?.error || fallback);
    if (status === 404 && /not found/i.test(detail)) {
        return (
            'Studio Agent API is not available on the backend yet. '
            + 'The frontend is deployed, but api-studio needs the latest Docker image on Fly '
            + '(studio-agent routes return 404 until then).'
        );
    }
    if (status === 503) return detail;
    return detail;
}

export default function AgentPanel({ onBack }: { onBack?: () => void }) {
    const { session } = useContext(AuthContext);
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
    const [booting, setBooting] = useState(true);
    const [error, setError] = useState('');
    const scrollRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLTextAreaElement>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);
    const stickToBottomRef = useRef(true);

    const getToken = useCallback(async () => {
        const tok = session?.access_token;
        if (!tok) throw new Error('Not signed in');
        return tok;
    }, [session]);

    const authFetch = useCallback(
        async (path: string, init?: RequestInit) => {
            const tok = await getToken();
            const res = await fetch(`${API}${path}`, {
                ...init,
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${tok}`,
                    ...(init?.headers || {}),
                },
            });
            const data = (await res.json().catch(() => ({}))) as Record<string, unknown>;
            if (!res.ok) {
                throw new Error(friendlyApiError(res.status, data, res.statusText));
            }
            return data;
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
                const created = await authFetch('/api/studio-agent/sessions', {
                    method: 'POST',
                    body: JSON.stringify({
                        model: pickModel,
                        approval_mode: approvalMode,
                        content_format: 'both',
                    }),
                });
                if (!cancelled) {
                    setSessionId((created.session as { session_id?: string })?.session_id || null);
                    setPending((created.session as { pending_actions?: PendingAction[] })?.pending_actions || []);
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

    const patchSession = useCallback(
        async (patch: { model?: string; approval_mode?: ApprovalMode }) => {
            if (!sessionId) return;
            try {
                await authFetch(`/api/studio-agent/sessions/${sessionId}`, {
                    method: 'PATCH',
                    body: JSON.stringify(patch),
                });
            } catch (e) {
                setError((e as Error).message);
            }
        },
        [authFetch, sessionId],
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

    const sendText = useCallback(
        async (text: string) => {
            const trimmed = buildOutboundMessage(text);
            if (!trimmed || !sessionId || loading) return;
            setInput('');
            setAttachments([]);
            setAttachmentPayload({});
            setLoading(true);
            setError('');
            stickToBottomRef.current = true;
            setMessages((m) => [...m, { role: 'user', content: text.trim() }]);
            try {
                const data = await authFetch(`/api/studio-agent/sessions/${sessionId}/chat`, {
                    method: 'POST',
                    body: JSON.stringify({ message: trimmed }),
                });
                const reply = String(data?.assistant_message || '').trim();
                if (reply) {
                    setMessages((m) => [...m, { role: 'assistant', content: reply }]);
                }
                setPending((data?.pending_actions as PendingAction[]) || []);
            } catch (e) {
                setError((e as Error).message);
            } finally {
                setLoading(false);
            }
        },
        [authFetch, buildOutboundMessage, loading, sessionId],
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

    const approveAction = useCallback(
        async (actionId: string) => {
            if (!sessionId || loading) return;
            setLoading(true);
            setError('');
            stickToBottomRef.current = true;
            try {
                const data = await authFetch(`/api/studio-agent/sessions/${sessionId}/approve`, {
                    method: 'POST',
                    body: JSON.stringify({ action_id: actionId }),
                });
                const reply = String(data?.assistant_message || '').trim();
                if (reply) {
                    setMessages((m) => [...m, { role: 'assistant', content: reply }]);
                }
                setPending((data?.pending_actions as PendingAction[]) || []);
            } catch (e) {
                setError((e as Error).message);
            } finally {
                setLoading(false);
            }
        },
        [authFetch, loading, sessionId],
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

            <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
                <header className="mb-2 flex shrink-0 items-center gap-2 border-b border-white/[0.06] pb-2">
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
                    <div className="flex items-center gap-2">
                        <Bot className="h-5 w-5 text-violet-400" />
                        <h1 className="text-sm font-semibold text-white">Studio Agent</h1>
                        <span className="rounded bg-violet-500/15 px-1.5 py-0.5 text-[9px] font-bold uppercase text-violet-300">
                            Beta
                        </span>
                    </div>
                </header>

                {error && (
                    <p className="mb-2 shrink-0 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs leading-relaxed text-red-200">
                        {error}
                    </p>
                )}

                <div
                    ref={scrollRef}
                    onScroll={handleScroll}
                    className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-1"
                >
                    <div className="mx-auto max-w-3xl space-y-4 pb-4">
                        {messages.length === 0 && (
                            <div className="flex flex-col items-center justify-center py-12 text-center sm:py-16">
                                <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-2xl bg-violet-500/15">
                                    <Sparkles className="h-6 w-6 text-violet-400" />
                                </div>
                                <h2 className="text-lg font-semibold text-white">What are we shipping?</h2>
                                <p className="mt-2 max-w-md text-sm text-gray-500">
                                    Long-form, shorts, analytics, renders — OpenRouter + 26 Rookcast skills.
                                </p>
                                <div className="mt-6 grid w-full max-w-lg gap-2">
                                    {STARTER_PROMPTS.map((p) => (
                                        <button
                                            key={p}
                                            type="button"
                                            disabled={!sessionId}
                                            onClick={() => sendText(p)}
                                            className="rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-3 text-left text-xs text-gray-300 transition hover:border-violet-500/30 hover:bg-violet-500/5 hover:text-white disabled:opacity-40"
                                        >
                                            {p}
                                        </button>
                                    ))}
                                </div>
                            </div>
                        )}
                        {messages.map((m, i) => (
                            <div
                                key={`${m.role}-${i}`}
                                className={`flex ${m.role === 'user' ? 'justify-end' : 'justify-start'}`}
                            >
                                <div
                                    className={`max-w-[92%] rounded-2xl px-4 py-2.5 text-sm leading-relaxed sm:max-w-[85%] ${
                                        m.role === 'user'
                                            ? 'bg-violet-600 text-white'
                                            : 'bg-white/[0.05] text-gray-100'
                                    }`}
                                >
                                    <pre className="whitespace-pre-wrap font-sans">{m.content}</pre>
                                </div>
                            </div>
                        ))}
                        {loading && (
                            <div className="flex items-center gap-2 text-xs text-gray-500">
                                <Loader2 className="h-3 w-3 animate-spin" /> Thinking…
                            </div>
                        )}
                    </div>
                </div>

                {pending.length > 0 && (
                    <div className="mx-auto mb-2 w-full max-w-3xl shrink-0 rounded-xl border border-amber-500/25 bg-amber-500/5 p-3">
                        <p className="mb-2 flex items-center gap-1 text-[10px] font-semibold uppercase tracking-wider text-amber-200">
                            <Zap className="h-3 w-3" /> Approve to run
                        </p>
                        <div className="space-y-2">
                            {pending.map((a) => (
                                <div
                                    key={a.id}
                                    className="flex items-start gap-2 rounded-lg border border-amber-500/20 bg-black/30 p-2"
                                >
                                    <div className="min-w-0 flex-1">
                                        <p className="text-xs font-medium text-white">{a.tool}</p>
                                        <p className="truncate text-[10px] text-gray-500">
                                            {a.summary || JSON.stringify(a.arguments)}
                                        </p>
                                    </div>
                                    <button
                                        type="button"
                                        disabled={loading}
                                        onClick={() => approveAction(a.id)}
                                        className="rounded bg-emerald-600 px-2 py-1 text-[10px] font-semibold text-white"
                                    >
                                        <Check className="h-3 w-3" />
                                    </button>
                                    <button
                                        type="button"
                                        disabled={loading}
                                        onClick={() => rejectAction(a.id)}
                                        className="rounded bg-white/10 px-2 py-1 text-[10px] text-gray-300"
                                    >
                                        <X className="h-3 w-3" />
                                    </button>
                                </div>
                            ))}
                        </div>
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

                    <div className="rounded-2xl border border-white/[0.1] bg-[#0c0c0e] shadow-lg shadow-black/40">
                        <textarea
                            ref={inputRef}
                            className="max-h-36 min-h-[52px] w-full resize-none bg-transparent px-4 pt-3.5 text-sm text-white placeholder:text-gray-600 focus:outline-none"
                            placeholder="Talk to the agent — ask, edit, or kick off a video"
                            rows={1}
                            value={input}
                            disabled={!sessionId}
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
        </>
    );
}
