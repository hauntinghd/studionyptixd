import { Clapperboard } from 'lucide-react';
import type { ProductionProgressUpdate } from '../../lib/agentProduction';

export default function AgentProgressBubble({ update }: { update: ProductionProgressUpdate }) {
    const pct = Math.max(0, Math.min(100, update.progress));
    return (
        <div className="flex justify-start">
            <div className="max-w-[92%] rounded-2xl border border-cyan-500/15 bg-cyan-950/25 px-3 py-2 sm:max-w-[85%]">
                <div className="flex items-center gap-2 text-[11px] text-cyan-100/90">
                    <Clapperboard className="h-3.5 w-3.5 shrink-0 text-cyan-300" />
                    <span className="font-medium">{update.title || update.kind}</span>
                    <span className="text-gray-500">·</span>
                    <span className="truncate">{update.stage_label}</span>
                    <span className="ml-auto tabular-nums text-cyan-200">{pct}%</span>
                </div>
                <div className="mt-1.5 h-1 overflow-hidden rounded-full bg-white/[0.06]">
                    <div
                        className="h-full bg-cyan-500/80 transition-all duration-500"
                        style={{ width: `${pct}%` }}
                    />
                </div>
            </div>
        </div>
    );
}
