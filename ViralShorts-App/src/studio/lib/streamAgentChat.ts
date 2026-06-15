import { resolveStudioBackendUrl } from './backend';

export type AgentStreamEvent =
    | { event: 'status'; message?: string }
    | { event: 'model_round'; round?: number }
    | { event: 'tool_start'; tool?: string; round?: number; awaiting_approval?: boolean }
    | { event: 'tool_end'; tool?: string; status?: string; error?: string | null }
    | { event: 'active_jobs'; jobs?: unknown[] }
    | { event: 'pending_actions'; actions?: unknown[] }
    | { event: 'done'; [key: string]: unknown }
    | { event: 'error'; message?: string; queue?: boolean };

export type AgentChatResult = {
    assistant_message?: string;
    pending_actions?: unknown[];
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
    },
): Promise<AgentChatResult> {
    const url = resolveStudioBackendUrl(`/api/studio-agent/sessions/${sessionId}/chat/stream`);
    let res: Response;
    try {
        res = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${accessToken}`,
                Accept: 'text/event-stream',
            },
            body: JSON.stringify({
                message,
                reply_to: handlers.replyTo || undefined,
                attachments: handlers.attachments?.length ? handlers.attachments : undefined,
                channel_id: handlers.channel?.channel_id || undefined,
                registry_key: handlers.channel?.registry_key || undefined,
                channel_title: handlers.channel?.channel_title || undefined,
                captions_enabled: typeof handlers.captions_enabled === 'boolean' ? handlers.captions_enabled : undefined,
                caption_mode: handlers.caption_mode || undefined,
            }),
        });
    } catch (e) {
        const message = String((e as Error)?.message || e || '');
        throw new Error(
            `Studio Agent stream could not connect. The saved chat may still continue server-side; press Resume in a few seconds. ${message}`,
        );
    }

    if (!res.ok) {
        const errBody = (await res.json().catch(() => ({}))) as { detail?: string };
        throw new Error(
            typeof errBody.detail === 'string'
                ? errBody.detail
                : `Agent stream failed (${res.status})`,
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
