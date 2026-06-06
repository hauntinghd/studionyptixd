import { useContext, useEffect, useState } from 'react';
import { Sparkles } from 'lucide-react';
import { AuthContext, resolveStudioBackendUrl } from '../../shared';

type CreditState = {
    balance: number;
    plan?: string;
    plan_name?: string;
    monthly_credits?: number;
    unlimited?: boolean;
    tier?: string;
    label?: string;
};

// Single unified wallet pill. Replaces the old split "Render fuel" + "Shorts left"
// readout — credits are now one balance debited from real OpenRouter / fal /
// ElevenLabs usage (see unified_credits.py). Falls back to the legacy `totalAc`
// prop only until the live balance loads.
export default function CreditFuelBar({
    totalAc,
    onTopUp,
}: {
    totalAc?: number;
    onTopUp?: () => void;
}) {
    const { session } = useContext(AuthContext);
    const [state, setState] = useState<CreditState | null>(null);

    useEffect(() => {
        const tok = session?.access_token;
        if (!tok) return;
        let cancelled = false;
        (async () => {
            try {
                const res = await fetch(resolveStudioBackendUrl('/api/studio-agent/credits'), {
                    headers: { Authorization: `Bearer ${tok}` },
                });
                if (!res.ok) return;
                const data = (await res.json()) as CreditState;
                if (!cancelled) setState(data);
            } catch {
                /* keep fallback */
            }
        })();
        return () => {
            cancelled = true;
        };
    }, [session?.access_token]);

    const unlimited = Boolean(state?.unlimited) || state?.tier === 'owner';
    const balance = state ? Number(state.balance || 0) : Number(totalAc ?? 0);
    const display = unlimited ? '∞' : balance.toLocaleString();
    const label = unlimited
        ? 'Owner — unmetered'
        : state?.plan_name
          ? `${state.plan_name} credits`
          : 'Credits';

    return (
        <div className="flex items-center gap-2 rounded-xl border border-white/[0.08] bg-black/40 px-2 py-1.5">
            <div className="flex items-center gap-2 rounded-lg bg-gradient-to-br from-violet-600/30 to-cyan-600/20 px-3 py-1.5">
                <Sparkles className="h-3.5 w-3.5 shrink-0 text-cyan-300" />
                <div>
                    <p className="text-[9px] font-semibold uppercase tracking-[0.2em] text-gray-400">{label}</p>
                    <p className="text-lg font-bold leading-tight text-white tabular-nums">{display}</p>
                </div>
            </div>
            {onTopUp && !unlimited && (
                <button
                    type="button"
                    onClick={onTopUp}
                    className="ml-1 rounded-lg bg-cyan-600 px-3 py-2 text-xs font-semibold text-white transition hover:bg-cyan-500"
                >
                    Top up
                </button>
            )}
        </div>
    );
}
