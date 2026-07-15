/**
 * Renders assistant markdown (headings, bold, lists) — avoids raw ** and ## in the chat UI.
 */
import { useContext, type ReactNode } from 'react';
import { AuthContext } from '../../shared';
import { mediaUrl } from '../../lib/agentProduction';

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
    const { session } = useContext(AuthContext);
    // Older Agent messages printed raw thumbnail routes instead of emitting a
    // visual deliverable. Render them as an actual review grid too, and map
    // the now-retired /api/long-form route onto the authenticated Agent route.
    const thumbnailUrls = [...new Set(
        [...safe.matchAll(/\/api\/(?:long-form|studio-agent)\/jobs\/[^\s/)]+\/thumbnail\/\d+/g)]
            .map((match) => match[0].replace('/api/long-form/jobs/', '/api/studio-agent/jobs/')),
    )];
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
            {thumbnailUrls.length > 0 && session?.access_token && (
                <div className="grid grid-cols-1 gap-2 pt-1 sm:grid-cols-3">
                    {thumbnailUrls.map((url, index) => (
                        <a
                            key={url}
                            href={mediaUrl(url, session.access_token)}
                            target="_blank"
                            rel="noreferrer"
                            className="group overflow-hidden rounded-lg border border-violet-400/25 bg-black/30"
                            title={`Open thumbnail ${index + 1}`}
                        >
                            <img
                                src={mediaUrl(url, session.access_token)}
                                alt={`Thumbnail option ${index + 1}`}
                                className="aspect-video w-full object-cover transition group-hover:scale-[1.02]"
                                loading="lazy"
                            />
                            <div className="px-2 py-1.5 text-[10px] font-medium text-violet-100">Thumbnail {index + 1}</div>
                        </a>
                    ))}
                </div>
            )}
        </div>
    );
}
