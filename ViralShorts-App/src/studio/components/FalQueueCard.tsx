import { useEffect, useState } from 'react';
import { Users } from 'lucide-react';
import { GENERATION_API } from '../shared';

interface QueueStatus {
    in_flight: number;
    waiting: number;
    cap: number;
    slots_free: number;
    eta_sec: number;
    saturated: boolean;
}

interface Props {
    active: boolean;                // only poll while a render is in flight client-side
    className?: string;
}

// Polls /api/studio/queue/status every 4s while the user has an active
// render. Renders a compact "You're #N in line — ~2 min" card when the
// fal.ai concurrency gate is saturated. Silent when the gate is idle.
//
// Built for Reddit-promo-class traffic: when 10k users hit Studio, the
// fal_gate semaphore (capped at 16 of fal.ai's 20-limit) becomes the
// bottleneck. This card gives users a visible feedback loop instead of
// an invisible stall.
export default function FalQueueCard({ active, className = "" }: Props) {
    const [status, setStatus] = useState<QueueStatus | null>(null);

    useEffect(() => {
        if (!active) return;
        let cancelled = false;
        const tick = async () => {
            try {
                const res = await fetch(`${GENERATION_API}/api/studio/queue/status`);
                if (!res.ok) return;
                const data = (await res.json()) as QueueStatus;
                if (!cancelled) setStatus(data);
            } catch {
                // silent — queue UI is ambient, never block UX
            }
        };
        void tick();
        const handle = setInterval(() => { void tick(); }, 4000);
        return () => {
            cancelled = true;
            clearInterval(handle);
        };
    }, [active]);

    if (!active || !status || !status.saturated) return null;

    const totalAhead = status.waiting + status.in_flight;
    const position = Math.max(1, status.waiting + 1);
    const etaMin = Math.ceil(status.eta_sec / 60);

    return (
        <div
            className={`rounded-xl border border-amber-500/30 bg-amber-500/[0.06] p-3 ${className}`}
            role="status"
            aria-live="polite"
        >
            <div className="flex items-center gap-3">
                <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-amber-500/20 text-amber-200">
                    <Users className="h-4 w-4" />
                </div>
                <div className="min-w-0 flex-1">
                    <p className="text-sm font-semibold text-white">
                        Heavy traffic — you're #{position} in line
                    </p>
                    <p className="mt-0.5 text-[11px] text-amber-100/80">
                        {totalAhead} Studio users rendering right now · estimated {etaMin <= 0 ? 'under a minute' : `~${etaMin} min`} wait. Rendering will continue automatically.
                    </p>
                </div>
            </div>
            <div className="mt-2 h-1 w-full overflow-hidden rounded-full bg-white/[0.05]">
                <div
                    className="h-full rounded-full bg-gradient-to-r from-amber-500 to-orange-500 transition-all"
                    style={{ width: `${Math.min(100, (status.in_flight / Math.max(1, status.cap)) * 100)}%` }}
                />
            </div>
        </div>
    );
}
