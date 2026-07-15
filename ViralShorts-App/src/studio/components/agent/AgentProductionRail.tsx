import { Clapperboard, Loader2 } from 'lucide-react';
import type { AgentJobSnapshot, AgentJobTrack } from '../../lib/agentProduction';
import { isStaleDeadLongformPoll, isTerminalJob, isStaleIdleLongformFailure } from '../../lib/agentProduction';

export default function AgentProductionRail({
    tracks,
    snapshots,
}: {
    tracks: AgentJobTrack[];
    snapshots: Record<string, AgentJobSnapshot>;
}) {
    const active = tracks
        .map((t) => ({ track: t, snap: snapshots[t.job_id] }))
        .filter(({ snap }) => snap
            && !snap.thumbnail_only
            && snap.running !== false
            && !isTerminalJob(snap)
            && !isStaleDeadLongformPoll(snap)
            && !isStaleIdleLongformFailure(snap));

    if (!active.length) return null;

    return (
        <div className="mx-auto mb-2 w-full max-w-3xl shrink-0 space-y-2">
            {active.map(({ track, snap }) => {
                if (!snap) return null;
                const pct = Math.max(0, Math.min(100, Number(snap.progress || 0)));
                return (
                    <div
                        key={track.job_id}
                        className="rounded-xl border border-cyan-500/20 bg-cyan-950/20 px-3 py-2.5"
                    >
                        <div className="mb-1.5 flex items-center justify-between gap-2">
                            <div className="flex min-w-0 items-center gap-2">
                                <Clapperboard className="h-3.5 w-3.5 shrink-0 text-cyan-300" />
                                <p className="truncate text-xs font-semibold text-white">
                                    {track.title || snap.title || track.kind}
                                </p>
                            </div>
                            <span className="shrink-0 text-[10px] tabular-nums text-cyan-200">{pct}%</span>
                        </div>
                        <div className="h-1.5 overflow-hidden rounded-full bg-white/[0.06]">
                            <div
                                className="h-full rounded-full bg-gradient-to-r from-cyan-500 to-violet-500 transition-all duration-700"
                                style={{ width: `${pct}%` }}
                            />
                        </div>
                        <p className="mt-1.5 flex items-center gap-1.5 text-[10px] text-gray-400">
                            <Loader2 className="h-3 w-3 animate-spin text-cyan-400" />
                            <span className="truncate">{snap.stage_label}</span>
                            {snap.total_scenes ? (
                                <span className="shrink-0 text-gray-500">
                                    · {snap.current_scene || 0}/{snap.total_scenes} scenes
                                </span>
                            ) : null}
                        </p>
                    </div>
                );
            })}
        </div>
    );
}
