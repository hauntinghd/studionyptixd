import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { CheckCircle2, Loader2, Mail, RefreshCw, ShieldCheck, XCircle } from 'lucide-react';
import { AuthContext, GENERATION_API } from '../shared';

interface RefundRequest {
    id?: string;
    user_id: string;
    email: string;
    reason: string;
    amount_usd?: number | null;
    payment_reference?: string | null;
    status?: string;
    admin_note?: string | null;
    created_at?: string;
    updated_at?: string;
}

interface VerifiedUser {
    id: string;
    email: string;
    plan?: string;
    created_at?: string;
}

const STATUS_COLORS: Record<string, string> = {
    pending: 'border-amber-500/30 bg-amber-500/10 text-amber-200',
    approved: 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200',
    denied: 'border-red-500/30 bg-red-500/10 text-red-200',
    refunded: 'border-violet-500/30 bg-violet-500/10 text-violet-200',
};

export default function RefundsPanel() {
    const { session } = useContext(AuthContext);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [refunds, setRefunds] = useState<RefundRequest[]>([]);
    const [verifiedUsers, setVerifiedUsers] = useState<VerifiedUser[]>([]);
    const [activeTab, setActiveTab] = useState<'requests' | 'users'>('requests');
    const [updatingId, setUpdatingId] = useState<string | null>(null);

    const fetchAll = useCallback(async () => {
        if (!session) return;
        setLoading(true);
        setError(null);
        try {
            const res = await fetch(`${GENERATION_API}/api/admin/refunds`, {
                headers: { Authorization: `Bearer ${session.access_token}` },
            });
            if (!res.ok) {
                throw new Error(`HTTP ${res.status}`);
            }
            const data = await res.json();
            setRefunds(Array.isArray(data?.refunds) ? data.refunds : []);
            setVerifiedUsers(Array.isArray(data?.verified_users) ? data.verified_users : []);
        } catch (e: any) {
            setError(e?.message || 'Failed to load refunds');
        } finally {
            setLoading(false);
        }
    }, [session]);

    useEffect(() => { void fetchAll(); }, [fetchAll]);

    const updateStatus = async (refund: RefundRequest, status: string) => {
        if (!session || !refund.id) return;
        setUpdatingId(refund.id);
        try {
            const res = await fetch(`${GENERATION_API}/api/admin/refunds/${refund.id}`, {
                method: 'PATCH',
                headers: {
                    Authorization: `Bearer ${session.access_token}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ status }),
            });
            if (!res.ok) {
                throw new Error(`HTTP ${res.status}`);
            }
            await fetchAll();
        } catch (e: any) {
            setError(e?.message || 'Update failed');
        } finally {
            setUpdatingId(null);
        }
    };

    const counts = useMemo(() => {
        const c = { pending: 0, approved: 0, denied: 0, refunded: 0 };
        for (const r of refunds) {
            const s = String(r.status || 'pending').toLowerCase();
            if (s in c) (c as any)[s]++;
        }
        return c;
    }, [refunds]);

    return (
        <div className="space-y-6 p-6">
            <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                    <h1 className="text-2xl font-bold text-white">Refunds</h1>
                    <p className="text-sm text-gray-500">User-submitted refund requests and verified-email roster.</p>
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
                <StatCard label="Pending" value={counts.pending} tone="amber" />
                <StatCard label="Approved" value={counts.approved} tone="emerald" />
                <StatCard label="Denied" value={counts.denied} tone="red" />
                <StatCard label="Refunded" value={counts.refunded} tone="violet" />
            </div>

            <div className="flex gap-1 border-b border-white/[0.06]">
                <TabButton active={activeTab === 'requests'} onClick={() => setActiveTab('requests')}>
                    Refund Requests ({refunds.length})
                </TabButton>
                <TabButton active={activeTab === 'users'} onClick={() => setActiveTab('users')}>
                    Verified Users ({verifiedUsers.length})
                </TabButton>
            </div>

            {activeTab === 'requests' ? (
                <div className="space-y-2">
                    {loading && refunds.length === 0 && (
                        <div className="flex items-center gap-2 rounded-xl border border-white/[0.06] bg-white/[0.02] px-4 py-6 text-sm text-gray-400">
                            <Loader2 className="h-4 w-4 animate-spin" />
                            Loading refund requests...
                        </div>
                    )}
                    {!loading && refunds.length === 0 && (
                        <div className="rounded-xl border border-white/[0.06] bg-white/[0.02] px-4 py-6 text-center text-sm text-gray-500">
                            No refund requests yet.
                        </div>
                    )}
                    {refunds.map((refund, idx) => (
                        <div
                            key={String(refund.id || `${refund.user_id}-${idx}`)}
                            className="rounded-xl border border-white/[0.06] bg-white/[0.02] p-4"
                        >
                            <div className="flex flex-wrap items-start justify-between gap-3">
                                <div className="min-w-0 flex-1">
                                    <div className="flex flex-wrap items-center gap-2">
                                        <span className="text-sm font-semibold text-white">{refund.email || refund.user_id}</span>
                                        <span className={`rounded-full border px-2 py-0.5 text-[9px] font-bold uppercase tracking-wider ${STATUS_COLORS[String(refund.status || 'pending').toLowerCase()] || STATUS_COLORS.pending}`}>
                                            {String(refund.status || 'pending').toUpperCase()}
                                        </span>
                                        {typeof refund.amount_usd === 'number' && (
                                            <span className="text-[11px] font-semibold text-violet-300">
                                                ${refund.amount_usd.toFixed(2)}
                                            </span>
                                        )}
                                        {refund.payment_reference && (
                                            <span className="rounded border border-white/[0.08] bg-black/30 px-1.5 py-0.5 font-mono text-[10px] text-gray-400">
                                                {refund.payment_reference}
                                            </span>
                                        )}
                                    </div>
                                    <p className="mt-2 text-[13px] leading-snug text-gray-300">{refund.reason}</p>
                                    {refund.admin_note && (
                                        <p className="mt-1 text-[11px] italic text-violet-300/70">Admin: {refund.admin_note}</p>
                                    )}
                                    {refund.created_at && (
                                        <p className="mt-2 text-[10px] text-gray-600">
                                            {new Date(refund.created_at).toLocaleString()}
                                        </p>
                                    )}
                                </div>
                                {refund.id && (
                                    <div className="flex shrink-0 flex-wrap items-center gap-1.5">
                                        <ActionButton
                                            icon={CheckCircle2}
                                            label="Approve"
                                            color="emerald"
                                            loading={updatingId === refund.id}
                                            onClick={() => void updateStatus(refund, 'approved')}
                                        />
                                        <ActionButton
                                            icon={ShieldCheck}
                                            label="Refunded"
                                            color="violet"
                                            loading={updatingId === refund.id}
                                            onClick={() => void updateStatus(refund, 'refunded')}
                                        />
                                        <ActionButton
                                            icon={XCircle}
                                            label="Deny"
                                            color="red"
                                            loading={updatingId === refund.id}
                                            onClick={() => void updateStatus(refund, 'denied')}
                                        />
                                    </div>
                                )}
                            </div>
                        </div>
                    ))}
                </div>
            ) : (
                <div className="space-y-2">
                    {verifiedUsers.length === 0 && !loading && (
                        <div className="rounded-xl border border-white/[0.06] bg-white/[0.02] px-4 py-6 text-center text-sm text-gray-500">
                            No verified users loaded.
                        </div>
                    )}
                    {verifiedUsers.map((user) => (
                        <div
                            key={user.id}
                            className="flex items-center justify-between gap-3 rounded-xl border border-white/[0.06] bg-white/[0.02] px-4 py-3"
                        >
                            <div className="flex min-w-0 items-center gap-2">
                                <Mail className="h-4 w-4 shrink-0 text-gray-500" />
                                <span className="truncate text-sm text-white">{user.email}</span>
                                {user.plan && (
                                    <span className="rounded-full border border-white/[0.08] bg-black/30 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-gray-400">
                                        {user.plan}
                                    </span>
                                )}
                            </div>
                            {user.created_at && (
                                <span className="shrink-0 text-[11px] text-gray-500">
                                    since {new Date(user.created_at).toLocaleDateString()}
                                </span>
                            )}
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}

function StatCard({ label, value, tone }: { label: string; value: number; tone: 'amber' | 'emerald' | 'red' | 'violet' }) {
    const toneClass =
        tone === 'amber' ? 'border-amber-500/30 bg-amber-500/[0.06] text-amber-200' :
        tone === 'emerald' ? 'border-emerald-500/30 bg-emerald-500/[0.06] text-emerald-200' :
        tone === 'red' ? 'border-red-500/30 bg-red-500/[0.06] text-red-200' :
        'border-violet-500/30 bg-violet-500/[0.06] text-violet-200';
    return (
        <div className={`rounded-xl border p-3 ${toneClass}`}>
            <div className="text-[10px] font-semibold uppercase tracking-wider opacity-80">{label}</div>
            <div className="mt-1 text-2xl font-bold tabular-nums">{value}</div>
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

function ActionButton({
    icon: Icon,
    label,
    color,
    loading,
    onClick,
}: {
    icon: React.ComponentType<{ className?: string }>;
    label: string;
    color: 'emerald' | 'violet' | 'red';
    loading?: boolean;
    onClick: () => void;
}) {
    const cls =
        color === 'emerald' ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200 hover:bg-emerald-500/20' :
        color === 'violet' ? 'border-violet-500/40 bg-violet-500/10 text-violet-200 hover:bg-violet-500/20' :
        'border-red-500/40 bg-red-500/10 text-red-200 hover:bg-red-500/20';
    return (
        <button
            type="button"
            onClick={onClick}
            disabled={loading}
            className={`inline-flex items-center gap-1 rounded-md border px-2 py-1 text-[10px] font-semibold uppercase tracking-wider transition disabled:opacity-40 ${cls}`}
        >
            {loading ? <Loader2 className="h-3 w-3 animate-spin" /> : <Icon className="h-3 w-3" />}
            {label}
        </button>
    );
}
