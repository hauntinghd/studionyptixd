import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { CheckCircle2, Loader2, Mail, RefreshCw, XCircle } from 'lucide-react';
import { AuthContext, GENERATION_API } from '../shared';

interface WaitlistRow {
    id?: string;
    email: string;
    plan?: string;
    price_usd?: number | null;
    paid?: boolean;
    stripe_session_id?: string | null;
    created_at?: string;
}

interface WaitlistSummary {
    total?: number;
    by_plan?: Record<string, number>;
    paid_revenue_monthly_usd?: number;
}

const PLAN_COLORS: Record<string, string> = {
    starter: 'border-sky-500/30 bg-sky-500/10 text-sky-200',
    creator: 'border-violet-500/30 bg-violet-500/10 text-violet-200',
    pro: 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200',
    elite: 'border-amber-500/30 bg-amber-500/10 text-amber-200',
};

export default function WaitlistPanel() {
    const { session } = useContext(AuthContext);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [rows, setRows] = useState<WaitlistRow[]>([]);
    const [summary, setSummary] = useState<WaitlistSummary>({});
    const [filter, setFilter] = useState<'all' | 'paid' | 'unpaid'>('all');

    const fetchAll = useCallback(async () => {
        if (!session) return;
        setLoading(true);
        setError(null);
        try {
            const res = await fetch(`${GENERATION_API}/api/admin/waiting-list`, {
                headers: { Authorization: `Bearer ${session.access_token}` },
            });
            if (!res.ok) {
                throw new Error(`HTTP ${res.status}`);
            }
            const data = await res.json();
            setRows(Array.isArray(data?.rows) ? data.rows : []);
            setSummary(data?.summary && typeof data.summary === 'object' ? data.summary : {});
        } catch (e: any) {
            setError(e?.message || 'Failed to load waitlist');
        } finally {
            setLoading(false);
        }
    }, [session]);

    useEffect(() => { void fetchAll(); }, [fetchAll]);

    const filtered = useMemo(() => {
        if (filter === 'paid') return rows.filter((r) => Boolean(r.paid));
        if (filter === 'unpaid') return rows.filter((r) => !r.paid);
        return rows;
    }, [rows, filter]);

    const total = summary.total ?? rows.length;
    const byPlan = summary.by_plan ?? {};
    const paidRevenue = summary.paid_revenue_monthly_usd ?? 0;
    const paidCount = rows.filter((r) => Boolean(r.paid)).length;

    return (
        <div className="space-y-6 p-6">
            <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                    <h1 className="text-2xl font-bold text-white">Open-Beta Waitlist</h1>
                    <p className="text-sm text-gray-500">Reservations from studio.nyptidindustries.com/waitlist — paid first-month deposits via Stripe or PayPal.</p>
                </div>
                <button
                    type="button"
                    onClick={() => void fetchAll()}
                    disabled={loading}
                    className="inline-flex items-center gap-1.5 rounded-lg border border-white/[0.08] bg-white/[0.02] px-3 py-1.5 text-xs font-semibold text-gray-300 transition hover:bg-white/[0.05] disabled:opacity-50"
                >
                    <RefreshCw className={`h-3.5 w-3.5 ${loading ? 'animate-spin' : ''}`} />
                    Refresh
                </button>
            </div>

            {error && (
                <div className="rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-200">
                    {error}
                </div>
            )}

            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
                <StatCard label="Total signups" value={String(total)} tone="violet" />
                <StatCard label="Paid deposits" value={String(paidCount)} tone="emerald" />
                <StatCard label="Monthly paid revenue" value={`$${paidRevenue.toFixed(2)}`} tone="cyan" />
                <StatCard
                    label="By plan"
                    value={['starter', 'creator', 'pro', 'elite']
                        .map((p) => `${p[0].toUpperCase()}${byPlan[p] ?? 0}`)
                        .join(' · ')}
                    tone="amber"
                />
            </div>

            <div className="flex gap-1 border-b border-white/[0.06]">
                <TabButton active={filter === 'all'} onClick={() => setFilter('all')}>
                    All ({rows.length})
                </TabButton>
                <TabButton active={filter === 'paid'} onClick={() => setFilter('paid')}>
                    Paid ({paidCount})
                </TabButton>
                <TabButton active={filter === 'unpaid'} onClick={() => setFilter('unpaid')}>
                    Unpaid ({rows.length - paidCount})
                </TabButton>
            </div>

            {loading && rows.length === 0 && (
                <div className="flex items-center gap-2 rounded-xl border border-white/[0.06] bg-white/[0.02] px-4 py-6 text-sm text-gray-400">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Loading waitlist…
                </div>
            )}
            {!loading && filtered.length === 0 && (
                <div className="rounded-xl border border-white/[0.06] bg-white/[0.02] px-4 py-6 text-center text-sm text-gray-500">
                    No waitlist entries {filter !== 'all' ? `(${filter})` : 'yet'}.
                </div>
            )}

            <div className="overflow-hidden rounded-xl border border-white/[0.06] bg-white/[0.02]">
                <table className="min-w-full text-sm">
                    <thead className="bg-black/30 text-[11px] uppercase tracking-wider text-gray-500">
                        <tr>
                            <th className="px-4 py-2 text-left">Email</th>
                            <th className="px-4 py-2 text-left">Plan</th>
                            <th className="px-4 py-2 text-right">Deposit</th>
                            <th className="px-4 py-2 text-left">Paid</th>
                            <th className="px-4 py-2 text-left">Joined</th>
                        </tr>
                    </thead>
                    <tbody>
                        {filtered.map((row, idx) => {
                            const plan = String(row.plan || '').toLowerCase();
                            const planCls = PLAN_COLORS[plan] || 'border-white/[0.08] bg-black/30 text-gray-300';
                            return (
                                <tr
                                    key={String(row.id || row.email || idx)}
                                    className="border-t border-white/[0.04]"
                                >
                                    <td className="px-4 py-2">
                                        <span className="inline-flex items-center gap-2 text-white">
                                            <Mail className="h-3.5 w-3.5 text-gray-500" />
                                            {row.email || '—'}
                                        </span>
                                    </td>
                                    <td className="px-4 py-2">
                                        <span className={`rounded-full border px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider ${planCls}`}>
                                            {plan || '—'}
                                        </span>
                                    </td>
                                    <td className="px-4 py-2 text-right font-mono text-[12px] text-gray-300">
                                        {typeof row.price_usd === 'number' ? `$${row.price_usd.toFixed(2)}` : '—'}
                                    </td>
                                    <td className="px-4 py-2">
                                        {row.paid ? (
                                            <span className="inline-flex items-center gap-1 text-[11px] font-semibold text-emerald-300">
                                                <CheckCircle2 className="h-3.5 w-3.5" />
                                                Paid
                                            </span>
                                        ) : (
                                            <span className="inline-flex items-center gap-1 text-[11px] font-semibold text-gray-500">
                                                <XCircle className="h-3.5 w-3.5" />
                                                Unpaid
                                            </span>
                                        )}
                                    </td>
                                    <td className="px-4 py-2 text-[11px] text-gray-500">
                                        {row.created_at ? new Date(row.created_at).toLocaleString() : '—'}
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

function StatCard({ label, value, tone }: { label: string; value: string; tone: 'amber' | 'emerald' | 'cyan' | 'violet' }) {
    const toneClass =
        tone === 'amber' ? 'border-amber-500/30 bg-amber-500/[0.06] text-amber-200' :
        tone === 'emerald' ? 'border-emerald-500/30 bg-emerald-500/[0.06] text-emerald-200' :
        tone === 'cyan' ? 'border-cyan-500/30 bg-cyan-500/[0.06] text-cyan-200' :
        'border-violet-500/30 bg-violet-500/[0.06] text-violet-200';
    return (
        <div className={`rounded-xl border p-3 ${toneClass}`}>
            <div className="text-[10px] font-semibold uppercase tracking-wider opacity-80">{label}</div>
            <div className="mt-1 text-lg font-bold tabular-nums">{value}</div>
        </div>
    );
}

function TabButton({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
    return (
        <button
            type="button"
            onClick={onClick}
            className={`px-4 py-2 text-sm font-semibold transition ${
                active ? 'text-white border-b-2 border-violet-500' : 'text-gray-400 hover:text-white'
            }`}
        >
            {children}
        </button>
    );
}
