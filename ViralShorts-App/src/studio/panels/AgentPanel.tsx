/**
 * Studio Agent — full-screen chat (OpenRouter + Rookcast skills).
 */
import { useCallback, useContext, useEffect, useRef, useState } from 'react';
import {
    ArrowLeft, Bot, Check, ChevronDown, Loader2, Send, Settings2, Shield, ShieldOff, Sparkles, X, Zap,
} from 'lucide-react';
import { API, AuthContext } from '../shared';

type ApprovalMode = 'auto' | 'confirm';
type ContentFormat = 'short' | 'long' | 'both';

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

const DEFAULT_MODELS = [
    'anthropic/claude-sonnet-4',
    'openai/gpt-4o',
    'google/gemini-2.0-flash-001',
    'deepseek/deepseek-chat',
];

const STARTER_PROMPTS = [
    'Plan a CrypticScience verified long-form on CTR + SS deposits — load compliance-preflight first.',
    'Outline a 60s ZeroTier short using outlier-mining and thumbnail-design skills.',
    'Pull channel analytics for cryptic_science and suggest 3 topics from public search trends.',
];

export default function AgentPanel({ onBack }: { onBack?: () => void }) {
    const { session } = useContext(AuthContext);
    const [sessionId, setSessionId] = useState<string | null>(null);
    const [model, setModel] = useState(DEFAULT_MODELS[0]);
    const [models, setModels] = useState<string[]>(DEFAULT_MODELS);
    const [approvalMode, setApprovalMode] = useState<ApprovalMode>('confirm');
    const [contentFormat, setContentFormat] = useState<ContentFormat>('both');
    const [messages, setMessages] = useState<ChatMessage[]>([]);
    const [pending, setPending] = useState<PendingAction[]>([]);
    const [input, setInput] = useState('');
    const [loading, setLoading] = useState(false);
    const [booting, setBooting] = useState(true);
    const [error, setError] = useState('');
    const [settingsOpen, setSettingsOpen] = useState(false);
    const scrollRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLTextAreaElement>(null);

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
            const data = await res.json().catch(() => ({}));
            if (!res.ok) {
                throw new Error(String(data?.detail || data?.error || res.statusText));
            }
            return data;
        },
        [getToken],
    );

    useEffect(() => {
        scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' });
    }, [messages, pending]);

    useEffect(() => {
        let cancelled = false;
        (async () => {
            setBooting(true);
            setError('');
            try {
                const modelData = await authFetch('/api/studio-agent/models');
                const rec = (modelData?.recommended as string[]) || DEFAULT_MODELS;
                if (!cancelled && rec.length) setModels(rec);
                const created = await authFetch('/api/studio-agent/sessions', {
                    method: 'POST',
                    body: JSON.stringify({
                        model,
                        approval_mode: approvalMode,
                        content_format: contentFormat,
                    }),
                });
                if (!cancelled) {
                    setSessionId(created.session?.session_id || null);
                    setPending(created.session?.pending_actions || []);
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
        async (patch: { model?: string; approval_mode?: ApprovalMode; content_format?: ContentFormat }) => {
            if (!sessionId) return;
            await authFetch(`/api/studio-agent/sessions/${sessionId}`, {
                method: 'PATCH',
                body: JSON.stringify(patch),
            });
        },
        [authFetch, sessionId],
    );

    const sendText = useCallback(
        async (text: string) => {
            const trimmed = text.trim();
            if (!trimmed || !sessionId || loading) return;
            setInput('');
            setLoading(true);
            setError('');
            setMessages((m) => [...m, { role: 'user', content: trimmed }]);
            try {
                const data = await authFetch(`/api/studio-agent/sessions/${sessionId}/chat`, {
                    method: 'POST',
                    body: JSON.stringify({ message: trimmed }),
                });
                const reply = String(data?.assistant_message || '').trim();
                if (reply) {
                    setMessages((m) => [...m, { role: 'assistant', content: reply }]);
                }
                setPending(data?.pending_actions || []);
            } catch (e) {
                setError((e as Error).message);
            } finally {
                setLoading(false);
            }
        },
        [authFetch, loading, sessionId],
    );

    const sendMessage = useCallback(() => sendText(input), [input, sendText]);

    const approveAction = useCallback(
        async (actionId: string) => {
            if (!sessionId || loading) return;
            setLoading(true);
            setError('');
            try {
                const data = await authFetch(`/api/studio-agent/sessions/${sessionId}/approve`, {
                    method: 'POST',
                    body: JSON.stringify({ action_id: actionId }),
                });
                const reply = String(data?.assistant_message || '').trim();
                if (reply) {
                    setMessages((m) => [...m, { role: 'assistant', content: reply }]);
                }
                setPending(data?.pending_actions || []);
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
        <div className="flex min-h-0 flex-1 flex-col">
            {/* Top bar */}
            <header className="mb-3 flex shrink-0 items-center gap-2 border-b border-white/[0.06] pb-3">
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
                <div className="ml-auto flex items-center gap-2">
                    <span className="hidden text-[10px] text-gray-500 sm:inline">
                        {approvalMode === 'confirm' ? 'Confirm mode' : 'Auto-accept'}
                    </span>
                    <button
                        type="button"
                        onClick={() => setSettingsOpen((v) => !v)}
                        className="inline-flex items-center gap-1 rounded-lg border border-white/10 px-2.5 py-1.5 text-xs text-gray-300 hover:border-violet-500/40"
                    >
                        <Settings2 className="h-3.5 w-3.5" />
                        Settings
                        <ChevronDown className={`h-3 w-3 transition ${settingsOpen ? 'rotate-180' : ''}`} />
                    </button>
                </div>
            </header>

            {settingsOpen && (
                <div className="mb-3 grid shrink-0 gap-3 rounded-xl border border-white/[0.08] bg-black/30 p-3 sm:grid-cols-3">
                    <label className="block text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                        Model
                        <select
                            className="mt-1 w-full rounded-lg border border-white/10 bg-black/40 px-2 py-2 text-xs text-white"
                            value={model}
                            onChange={(e) => {
                                setModel(e.target.value);
                                patchSession({ model: e.target.value });
                            }}
                        >
                            {models.map((m) => (
                                <option key={m} value={m}>{m}</option>
                            ))}
                        </select>
                    </label>
                    <label className="block text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                        Content focus
                        <select
                            className="mt-1 w-full rounded-lg border border-white/10 bg-black/40 px-2 py-2 text-xs text-white"
                            value={contentFormat}
                            onChange={(e) => {
                                const v = e.target.value as ContentFormat;
                                setContentFormat(v);
                                patchSession({ content_format: v });
                            }}
                        >
                            <option value="both">Short + Long</option>
                            <option value="long">Long-form only</option>
                            <option value="short">Short-form only</option>
                        </select>
                    </label>
                    <div className="space-y-1.5">
                        <p className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Tool approval</p>
                        <div className="flex gap-2">
                            <button
                                type="button"
                                onClick={() => {
                                    setApprovalMode('confirm');
                                    patchSession({ approval_mode: 'confirm' });
                                }}
                                className={`flex flex-1 items-center justify-center gap-1 rounded-lg border px-2 py-2 text-[10px] ${
                                    approvalMode === 'confirm'
                                        ? 'border-amber-500/40 bg-amber-500/10 text-amber-100'
                                        : 'border-white/10 text-gray-400'
                                }`}
                            >
                                <Shield className="h-3 w-3" /> Confirm
                            </button>
                            <button
                                type="button"
                                onClick={() => {
                                    setApprovalMode('auto');
                                    patchSession({ approval_mode: 'auto' });
                                }}
                                className={`flex flex-1 items-center justify-center gap-1 rounded-lg border px-2 py-2 text-[10px] ${
                                    approvalMode === 'auto'
                                        ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-100'
                                        : 'border-white/10 text-gray-400'
                                }`}
                            >
                                <ShieldOff className="h-3 w-3" /> Auto
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {error && (
                <p className="mb-2 shrink-0 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-200">
                    {error}
                </p>
            )}

            {/* Messages */}
            <div ref={scrollRef} className="min-h-0 flex-1 overflow-y-auto px-1">
                <div className="mx-auto max-w-3xl space-y-4 pb-4">
                    {messages.length === 0 && (
                        <div className="flex flex-col items-center justify-center py-16 text-center">
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
                                        onClick={() => sendText(p)}
                                        className="rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-3 text-left text-xs text-gray-300 transition hover:border-violet-500/30 hover:bg-violet-500/5 hover:text-white"
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

            {/* Composer */}
            <div className="mx-auto w-full max-w-3xl shrink-0 pb-2 pt-2">
                <div className="flex items-end gap-2 rounded-2xl border border-white/[0.1] bg-[#0c0c0e] p-2 shadow-lg shadow-black/40">
                    <textarea
                        ref={inputRef}
                        className="max-h-40 min-h-[48px] flex-1 resize-none bg-transparent px-2 py-2.5 text-sm text-white placeholder:text-gray-600 focus:outline-none"
                        placeholder="Message Studio Agent…"
                        rows={1}
                        value={input}
                        onChange={(e) => setInput(e.target.value)}
                        onKeyDown={(e) => {
                            if (e.key === 'Enter' && !e.shiftKey) {
                                e.preventDefault();
                                sendMessage();
                            }
                        }}
                    />
                    <button
                        type="button"
                        disabled={loading || !input.trim()}
                        onClick={sendMessage}
                        className="mb-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-violet-600 text-white disabled:opacity-40"
                    >
                        <Send className="h-4 w-4" />
                    </button>
                </div>
                <p className="mt-2 text-center text-[10px] text-gray-600">
                    Renders and file writes require approval in Confirm mode.
                </p>
            </div>
        </div>
    );
}
