/**
 * Renders assistant markdown (headings, bold, lists) — avoids raw ** and ## in the chat UI.
 */
import type { ReactNode } from 'react';

function parseInline(text: string, keyPrefix: string): ReactNode[] {
    const out: ReactNode[] = [];
    const re = /\*\*([^*]+)\*\*/g;
    let last = 0;
    let match: RegExpExecArray | null;
    let idx = 0;
    while ((match = re.exec(text)) !== null) {
        if (match.index > last) {
            out.push(text.slice(last, match.index));
        }
        out.push(
            <strong key={`${keyPrefix}-b-${idx}`} className="font-semibold text-white">
                {match[1]}
            </strong>,
        );
        last = match.index + match[0].length;
        idx += 1;
    }
    if (last < text.length) {
        out.push(text.slice(last));
    }
    return out.length ? out : [text];
}

function stripHeadingDecor(title: string): string {
    return title.replace(/^\*+\s*|\s*\*+$/g, '').trim();
}

type Block =
    | { type: 'h'; level: 3 | 4; text: string }
    | { type: 'p'; text: string }
    | { type: 'ul'; items: string[] }
    | { type: 'ol'; items: string[] };

function parseBlocks(content: string): Block[] {
    const blocks: Block[] = [];
    let list: { ordered: boolean; items: string[] } | null = null;

    const flushList = () => {
        if (!list || !list.items.length) {
            list = null;
            return;
        }
        blocks.push(
            list.ordered
                ? { type: 'ol', items: [...list.items] }
                : { type: 'ul', items: [...list.items] },
        );
        list = null;
    };

    for (const raw of content.split('\n')) {
        const line = raw.trimEnd();
        if (!line.trim()) {
            flushList();
            continue;
        }
        const h3 = line.match(/^###\s+(.+)$/);
        if (h3) {
            flushList();
            blocks.push({ type: 'h', level: 4, text: stripHeadingDecor(h3[1]) });
            continue;
        }
        const h2 = line.match(/^##\s+(.+)$/);
        if (h2) {
            flushList();
            blocks.push({ type: 'h', level: 3, text: stripHeadingDecor(h2[1]) });
            continue;
        }
        const bullet = line.match(/^[-*]\s+(.+)$/);
        if (bullet) {
            if (!list || list.ordered) {
                flushList();
                list = { ordered: false, items: [] };
            }
            list.items.push(bullet[1]);
            continue;
        }
        const num = line.match(/^\d+\.\s+(.+)$/);
        if (num) {
            if (!list || !list.ordered) {
                flushList();
                list = { ordered: true, items: [] };
            }
            list.items.push(num[1]);
            continue;
        }
        flushList();
        blocks.push({ type: 'p', text: line });
    }
    flushList();
    return blocks;
}

export default function AgentMessageBody({ content }: { content: string }) {
    const safe = typeof content === 'string' ? content : String(content ?? '');
    const blocks = parseBlocks(safe);
    return (
        <div className="space-y-2.5 text-sm leading-relaxed text-gray-100">
            {blocks.map((block, i) => {
                const key = `b-${i}`;
                if (block.type === 'h') {
                    const Tag = block.level === 3 ? 'h3' : 'h4';
                    return (
                        <Tag key={key} className="mt-1 text-sm font-semibold tracking-tight text-white">
                            {parseInline(block.text, key)}
                        </Tag>
                    );
                }
                if (block.type === 'ul') {
                    return (
                        <ul key={key} className="list-disc space-y-1 pl-5 text-gray-100">
                            {block.items.map((item, j) => (
                                <li key={`${key}-${j}`}>{parseInline(item, `${key}-${j}`)}</li>
                            ))}
                        </ul>
                    );
                }
                if (block.type === 'ol') {
                    return (
                        <ol key={key} className="list-decimal space-y-1 pl-5 text-gray-100">
                            {block.items.map((item, j) => (
                                <li key={`${key}-${j}`}>{parseInline(item, `${key}-${j}`)}</li>
                            ))}
                        </ol>
                    );
                }
                return (
                    <p key={key} className="text-gray-100">
                        {parseInline(block.text, key)}
                    </p>
                );
            })}
        </div>
    );
}
