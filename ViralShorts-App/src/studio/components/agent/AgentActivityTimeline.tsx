/**
 * Professional activity timeline for Studio Agent turns:
 * "Thinking about your request · 3s" / "Searching for … · 10s" with nested result rows.
 */
import { useEffect, useState } from 'react';
import { Check, Globe, Loader2, Search, Youtube } from 'lucide-react';

export type ActivityChild = {
    id: string;
    title: string;
    query?: string;
    resultCount?: number;
    source?: string;
};

export type ActivityStep = {
    id: string;
    kind: 'thinking' | 'tool' | 'status';
    label: string;
    tool?: string;
    startedAt: number;
    endedAt?: number;
    status: 'running' | 'done' | 'error';
    detail?: string;
    children?: ActivityChild[];
};

function elapsedSeconds(step: ActivityStep, now: number): number {
    const end = step.endedAt ?? now;
    return Math.max(0, Math.floor((end - step.startedAt) / 1000));
}

function SourceIcon({ source }: { source?: string }) {
    const s = String(source || '').toLowerCase();
    if (s.includes('youtube') || s === 'yt') {
        return <Youtube className="h-3 w-3 shrink-0 text-red-400/90" />;
    }
    if (s.includes('instagram') || s === 'ig') {
        return (
            <span className="inline-flex h-3 w-3 shrink-0 items-center justify-center rounded-sm bg-gradient-to-br from-amber-400 via-rose-500 to-violet-600 text-[7px] font-bold text-white">
                ig
            </span>
        );
    }
    return <Globe className="h-3 w-3 shrink-0 text-sky-400/80" />;
}

function ThinkingDots() {
    return (
        <span className="inline-flex w-4 shrink-0 justify-center gap-[2px]" aria-hidden>
            <span className="h-1 w-1 animate-pulse rounded-full bg-gray-400 [animation-delay:0ms]" />
            <span className="h-1 w-1 animate-pulse rounded-full bg-gray-400 [animation-delay:150ms]" />
            <span className="h-1 w-1 animate-pulse rounded-full bg-gray-400 [animation-delay:300ms]" />
        </span>
    );
}

export default function AgentActivityTimeline({
    steps,
    className = '',
}: {
    steps: ActivityStep[];
    className?: string;
}) {
    const [now, setNow] = useState(() => Date.now());
    const hasRunning = steps.some((s) => s.status === 'running');

    useEffect(() => {
        if (!hasRunning) return undefined;
        const id = window.setInterval(() => setNow(Date.now()), 250);
        return () => window.clearInterval(id);
    }, [hasRunning]);

    if (!steps.length) return null;

    return (
        <div className={`space-y-2.5 text-sm text-gray-300 ${className}`}>
            {steps.map((step) => {
                const secs = elapsedSeconds(step, now);
                const running = step.status === 'running';
                return (
                    <div key={step.id} className="min-w-0">
                        <div className="flex items-center gap-2 text-[13px] leading-snug text-gray-400">
                            {running ? (
                                step.kind === 'thinking' ? (
                                    <ThinkingDots />
                                ) : (
                                    <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin text-gray-400" />
                                )
                            ) : step.status === 'error' ? (
                                <span className="inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center text-[10px] text-rose-400">
                                    !
                                </span>
                            ) : (
                                <Check className="h-3.5 w-3.5 shrink-0 text-gray-500" strokeWidth={2.5} />
                            )}
                            <span className={running ? 'text-gray-300' : 'text-gray-400'}>
                                {step.label}
                            </span>
                            <span className="shrink-0 tabular-nums text-gray-600">· {secs}s</span>
                        </div>
                        {step.children && step.children.length > 0 && (
                            <div className="mt-1.5 space-y-1.5 pl-6">
                                {step.children.map((child) => (
                                    <div
                                        key={child.id}
                                        className="flex min-w-0 items-center gap-2 text-[12px] text-gray-500"
                                    >
                                        <SourceIcon source={child.source} />
                                        <span className="shrink-0 text-gray-400">
                                            {child.title || 'Searched'}
                                        </span>
                                        {child.query && (
                                            <span className="min-w-0 truncate text-gray-500" title={child.query}>
                                                {child.query}
                                            </span>
                                        )}
                                        {typeof child.resultCount === 'number' && (
                                            <span className="ml-auto shrink-0 tabular-nums text-gray-500">
                                                {child.resultCount} result{child.resultCount === 1 ? '' : 's'}
                                            </span>
                                        )}
                                        <SourceIcon source={child.source} />
                                    </div>
                                ))}
                            </div>
                        )}
                        {step.detail && !step.children?.length && (
                            <p className="mt-1 pl-6 text-[11px] text-gray-600">{step.detail}</p>
                        )}
                    </div>
                );
            })}
            {hasRunning && (
                <div className="flex items-center gap-2 pl-0.5 text-[11px] text-gray-600">
                    <Search className="h-3 w-3 opacity-50" />
                    <span>Working…</span>
                </div>
            )}
        </div>
    );
}

export function activityLabelForTool(
    tool?: string,
    args?: Record<string, unknown> | null,
    explicitLabel?: string,
): string {
    if (explicitLabel?.trim()) return explicitLabel.trim();
    const name = String(tool || '').trim();
    if (!name) return 'Working on your request';
    const q = String(
        args?.query
            ?? args?.search_query
            ?? args?.topic
            ?? args?.niche
            ?? args?.channel_title
            ?? '',
    ).trim();
    const low = name.toLowerCase();
    if (
        low.includes('search')
        || low.includes('trend')
        || low.includes('demand')
        || low.includes('youtube')
        || low.includes('public')
        || low.includes('web_')
    ) {
        return q
            ? `Searching for information on ${q.slice(0, 80)}`
            : 'Searching for information';
    }
    if (low.includes('analytics') || low.includes('channel')) {
        return q ? `Checking analytics for ${q.slice(0, 60)}` : 'Checking channel analytics';
    }
    if (low.includes('competitor')) {
        return 'Reviewing competitor content';
    }
    if (low.includes('memory')) {
        return 'Updating session memory';
    }
    if (low.includes('poll_render')) {
        return 'Deep-analyzing video (download + transcript + visuals)';
    }
    if (low.includes('reference') || low.includes('analyze')) {
        return 'Analyzing reference media';
    }
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
    if (low.includes('cliplab')) {
        return 'Running ClipLab';
    }
    return name
        .replace(/_/g, ' ')
        .replace(/\b\w/g, (c) => c.toUpperCase());
}

export function newThinkingStep(label = 'Thinking about your request'): ActivityStep {
    return {
        id: `think-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
        kind: 'thinking',
        label,
        startedAt: Date.now(),
        status: 'running',
    };
}

export function completeRunningSteps(steps: ActivityStep[], now = Date.now()): ActivityStep[] {
    return steps.map((s) =>
        s.status === 'running'
            ? { ...s, status: 'done' as const, endedAt: s.endedAt ?? now }
            : s,
    );
}
