import { resolveStudioBackendUrl } from './backend';

export type AgentToolActivitySummary = {
    title?: string;
    query?: string;
    result_count?: number;
    source?: string;
    label?: string;
};

export type AgentStreamEvent =
    | { event: 'status'; message?: string }
    | { event: 'verification_step'; step?: string; status?: string; label?: string; detail?: string; required?: boolean }
    | { event: 'model_round'; round?: number }
    | {
        event: 'tool_start';
        tool?: string;
        round?: number;
        awaiting_approval?: boolean;
        label?: string;
        query?: string;
        args?: Record<string, unknown>;
    }
    | {
        event: 'tool_end';
        tool?: string;
        status?: string;
        error?: string | null;
        summary?: AgentToolActivitySummary;
        label?: string;
        query?: string;
    }
    | { event: 'active_jobs'; jobs?: unknown[] }
    | {
        event: 'session_state';
        blocked_job_ids?: unknown[];
        production_state?: { epoch?: number; target_title?: string };
        active_jobs?: unknown[];
    }
    | { event: 'job_snapshot'; snapshot?: unknown }
    | { event: 'thumbnail_review'; review?: unknown }
    | { event: 'pending_actions'; actions?: unknown[] }
    | { event: 'concept_plan'; plan?: unknown }
    | { event: 'done'; [key: string]: unknown }
    | { event: 'error'; message?: string; queue?: boolean };

export type AgentChatResult = {
    assistant_message?: string;
    pending_actions?: unknown[];
    pending_concept?: unknown;
    concept_plan?: unknown;
    active_jobs?: unknown[];
    queue?: { waited_sec?: number; queue_position?: number };
    [key: string]: unknown;
};

export type AgentChatAttachment = {
    name: string;
    mime_type: string;
    size: number;
    data_url: string;
};

class AgentSseError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'AgentSseError';
    }
}

function parseSseBlock(block: string): AgentStreamEvent | null {
    let eventName = 'status';
    let dataLine = '';
    for (const line of block.split('\n')) {
        if (line.startsWith('event:')) eventName = line.slice(6).trim();
        if (line.startsWith('data:')) dataLine += line.slice(5).trim();
    }
    if (!dataLine) return null;
    try {
        const payload = JSON.parse(dataLine) as Record<string, unknown>;
        return { event: eventName, ...payload } as AgentStreamEvent;
    } catch {
        return null;
    }
}

export async function streamAgentChat(
    sessionId: string,
    message: string,
    accessToken: string,
    handlers: {
        onEvent?: (ev: AgentStreamEvent) => void;
        replyTo?: { job_id: string; kind: string; scene_index?: number } | null;
        attachments?: AgentChatAttachment[];
        channel?: {
            channel_id?: string;
            registry_key?: string;
            channel_title?: string;
        } | null;
        captions_enabled?: boolean;
        caption_mode?: 'word' | 'off';
        render_style?: string;
        image_model?: string;
        image_model_id?: string;
        video_model?: string;
        agent_mode?: 'plan' | 'studio' | 'cliplab';
    },
): Promise<AgentChatResult> {
    const url = resolveStudioBackendUrl(`/api/studio-agent/sessions/${sessionId}/chat/stream`);
    // One ID represents one logical send. Keep it outside the HTTP retry loop so
    // an edge timeout cannot turn the same user message into a second model run.
    const requestId = globalThis.crypto?.randomUUID?.()
        || `chat_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 12)}`;
    const payload = JSON.stringify({
        request_id: requestId,
        message,
        reply_to: handlers.replyTo || undefined,
        attachments: handlers.attachments?.length ? handlers.attachments : undefined,
        channel_id: handlers.channel?.channel_id || undefined,
        registry_key: handlers.channel?.registry_key || undefined,
        channel_title: handlers.channel?.channel_title || undefined,
        captions_enabled: typeof handlers.captions_enabled === 'boolean' ? handlers.captions_enabled : undefined,
        caption_mode: handlers.caption_mode || undefined,
        render_style: handlers.render_style || undefined,
        image_model: handlers.image_model || handlers.image_model_id || undefined,
        image_model_id: handlers.image_model_id || handlers.image_model || undefined,
        video_model: handlers.video_model || undefined,
        agent_mode: handlers.agent_mode || 'plan',
    });
    const retryableStatuses = new Set([524, 502, 503, 504]);
    let res: Response | null = null;
    let lastConnectError = '';
    for (let attempt = 0; attempt < 2; attempt++) {
        try {
            res = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${accessToken}`,
                    Accept: 'text/event-stream',
                    'X-Idempotency-Key': requestId,
                },
                body: payload,
            });
        } catch (e) {
            lastConnectError = String((e as Error)?.message || e || '');
            if (attempt < 1) {
                await new Promise((r) => setTimeout(r, 2000));
                continue;
            }
            throw new Error(
                `Studio Agent stream could not connect. The saved chat may still continue server-side; press Resume in a few seconds. ${lastConnectError}`,
            );
        }
        if (res.ok || !retryableStatuses.has(res.status) || attempt >= 1) {
            break;
        }
        await new Promise((r) => setTimeout(r, 2000));
    }
    if (!res) {
        throw new Error(
            `Studio Agent stream could not connect. The saved chat may still continue server-side; press Resume in a few seconds. ${lastConnectError}`,
        );
    }

    if (!res.ok) {
        const errBody = (await res.json().catch(() => ({}))) as { detail?: string };
        const statusHint = res.status === 524
            ? 'Proxy timed out before Studio responded — your run may still be saved server-side; press Resume.'
            : '';
        throw new Error(
            typeof errBody.detail === 'string'
                ? errBody.detail
                : `Agent stream failed (${res.status})${statusHint ? `. ${statusHint}` : ''}`,
        );
    }

    const reader = res.body?.getReader();
    if (!reader) throw new Error('No response body from agent stream');

    const decoder = new TextDecoder();
    let buffer = '';
    let result: AgentChatResult | null = null;

    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            const parts = buffer.split('\n\n');
            buffer = parts.pop() || '';
            for (const part of parts) {
                const ev = parseSseBlock(part.trim());
                if (!ev) continue;
                handlers.onEvent?.(ev);
                if (ev.event === 'done') {
                    const { event: _e, ...rest } = ev as AgentStreamEvent & Record<string, unknown>;
                    result = rest as AgentChatResult;
                }
                if (ev.event === 'error') {
                    throw new AgentSseError(String((ev as { message?: string }).message || 'Agent turn failed'));
                }
            }
        }
    } catch (e) {
        if (e instanceof AgentSseError) {
            throw e;
        }
        const message = String((e as Error)?.message || e || '');
        throw new Error(
            `Studio Agent stream disconnected before the final event. The run was saved server-side; press Resume to reload it. ${message}`,
        );
    }

    if (!result) throw new Error('Agent stream ended without a result');
    return result;
}

export function toolLabel(tool?: string) {
    if (!tool) return 'Running tool…';
    return tool
        .replace(/_/g, ' ')
        .replace(/\b\w/g, (c) => c.toUpperCase());
}

/** Human-friendly activity sentence for the thinking timeline. */
export function toolActivityLabel(tool?: string, query?: string, explicit?: string) {
    if (explicit?.trim()) return explicit.trim();
    const name = String(tool || '').trim();
    const q = String(query || '').trim();
    const low = name.toLowerCase();
    // poll_render_job is used for reference/competitor analysis progress too — not production.
    if (low.includes('poll_render') || low === 'poll_render_job') {
        return 'Deep-analyzing video (download + transcript + visuals)';
    }
    if (low.includes('retry_reference')) return 'Retrying deep analysis stages';
    if (
        low.includes('search')
        || low.includes('trend')
        || low.includes('demand')
        || low.includes('youtube')
        || low.includes('public')
        || low.includes('web_')
    ) {
        return q ? `Searching for information on ${q.slice(0, 80)}` : 'Searching for information';
    }
    if (low.includes('fetch_competitor') || (low.includes('competitor') && low.includes('channel'))) {
        return 'Fetching channel uploads';
    }
    if (low.includes('analytics') || (low.includes('channel') && !low.includes('competitor'))) {
        return q ? `Checking analytics for ${q.slice(0, 60)}` : 'Checking channel analytics';
    }
    if (low.includes('competitor')) return 'Reviewing competitor content';
    if (low.includes('memory')) return 'Updating session memory';
    if (low.includes('reference') || low.includes('analyze')) return 'Analyzing reference media';
    if (
        low.includes('shortform')
        || low.includes('longform')
        || low.includes('start_shortform')
        || low.includes('start_longform')
        || low.includes('finalize_production')
        || low.includes('animate_production')
    ) {
        return 'Starting production';
    }
    if (low.includes('cliplab')) return 'Running ClipLab';
    return toolLabel(tool);
}
