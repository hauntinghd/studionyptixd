import { useContext, useEffect, useState } from 'react';
import { Sparkles } from 'lucide-react';
import { AuthContext, isOwnerEmail, resolveStudioBackendUrl } from '../../shared';

type CreditState = {
    balance: number;
    plan?: string;
    plan_name?: string;
    monthly_credits?: number;
    unlimited?: boolean;
    tier?: string;
    label?: string;
};

/** Matches Discord / notification pills in StudioTopBar — no wrapper box. */
const topBarPill =
    'inline-flex items-center gap-2 rounded-lg border border-white/[0.08] px-3 py-1.5 text-xs font-medium text-gray-300 transition hover:border-indigo-500/30 hover:text-white';

export default function CreditFuelBar({
    totalAc,
    onTopUp,
}: {
    totalAc?: number;
    onTopUp?: () => void;
}) {
    const { session, ownerOverride, role } = useContext(AuthContext);
    const [state, setState] = useState<CreditState | null>(null);

    const ownerAccount = ownerOverride
        || role === 'admin'
        || isOwnerEmail(session?.user?.email);

    useEffect(() => {
        const tok = session?.access_token;
        if (!tok || ownerAccount) return;
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
    }, [session?.access_token, ownerAccount]);

    const unlimited = ownerAccount
        || Boolean(state?.unlimited)
        || state?.tier === 'owner';
    const balance = unlimited
        ? 0
        : state
          ? Number(state.balance || 0)
          : Number(totalAc ?? 0);
    const display = unlimited ? '∞' : balance.toLocaleString();

    return (
        <>
            <span className={topBarPill} title={unlimited ? 'Owner — unmetered' : 'Studio credits'}>
                <Sparkles className="h-3.5 w-3.5 shrink-0 text-cyan-300" />
                <span className="hidden uppercase tracking-[0.14em] text-gray-500 sm:inline">Credits</span>
                <span className="text-sm font-bold tabular-nums text-white">{display}</span>
            </span>
            {onTopUp && !unlimited && (
                <button type="button" onClick={onTopUp} className={topBarPill}>
                    Top up
                </button>
            )}
        </>
    );
}
