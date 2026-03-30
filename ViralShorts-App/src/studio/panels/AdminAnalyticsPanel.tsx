import { useCallback, useContext, useEffect, useState } from 'react';
import {
    API,
    AuthContext,
} from '../shared';

export default function AdminAnalyticsPanel() {
    const { session, backendOffline } = useContext(AuthContext);
    const [data, setData] = useState<any>(null);
    const [billingAuditRows, setBillingAuditRows] = useState<any[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");
    const [bannerEnabled, setBannerEnabled] = useState(false);
    const [bannerMessage, setBannerMessage] = useState("Studio is under high load. Queue times may be longer than usual while we scale capacity.");
    const [savingBanner, setSavingBanner] = useState(false);

    const readJsonResponse = async <T = any>(res: Response): Promise<{ data: T | null; raw: string }> => {
        const raw = await res.text().catch(() => "");
        if (!raw) return { data: null, raw: "" };
        try {
            return { data: JSON.parse(raw) as T, raw };
        } catch {
            return { data: null, raw };
        }
    };

    const loadAnalytics = useCallback(async () => {
        if (!session) return;
        if (backendOffline) {
            setLoading(false);
            return;
        }
        setLoading(true);
        setError("");
        try {
            const res = await fetch(`${API}/api/admin/analytics`, {
                headers: { Authorization: `Bearer ${session.access_token}` },
            });
            if (!res.ok) throw new Error(`Failed to load analytics (${res.status})`);
            const { data: payload } = await readJsonResponse<any>(res);
            if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
                setData(null);
                setBillingAuditRows([]);
                setError("Backend analytics unavailable.");
                return;
            }
            setData(payload);

            const auditRes = await fetch(`${API}/api/admin/billing-audit`, {
                headers: { Authorization: `Bearer ${session.access_token}` },
            });
            if (auditRes.ok) {
                const { data: audit } = await readJsonResponse<any>(auditRes);
                setBillingAuditRows(Array.isArray(audit?.rows) ? audit.rows : []);
            } else {
                setBillingAuditRows([]);
            }

            setBannerEnabled(Boolean(payload.maintenance_banner_enabled));
            setBannerMessage((payload.maintenance_banner_message || "").trim() || "Studio is under high load. Queue times may be longer than usual while we scale capacity.");
        } catch (e: any) {
            setError(e?.message || "Failed to load analytics");
        } finally {
            setLoading(false);
        }
    }, [session, backendOffline]);

    useEffect(() => {
        loadAnalytics();
        const id = setInterval(() => {
            loadAnalytics();
        }, 15000);
        return () => clearInterval(id);
    }, [loadAnalytics]);

    const saveBanner = useCallback(async () => {
        if (!session) return;
        setSavingBanner(true);
        setError("");
        try {
            const res = await fetch(`${API}/api/admin/maintenance-banner`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Authorization: `Bearer ${session.access_token}`,
                },
                body: JSON.stringify({
                    enabled: bannerEnabled,
                    message: bannerMessage,
                }),
            });
            if (!res.ok) throw new Error(`Failed to save banner (${res.status})`);
            const { data: updated } = await readJsonResponse<any>(res);
            if (!updated || typeof updated !== 'object') throw new Error("Invalid maintenance banner response");
            setData((prev: any) => ({
                ...(prev || {}),
                maintenance_banner_enabled: Boolean(updated.maintenance_banner_enabled),
                maintenance_banner_message: updated.maintenance_banner_message || "",
            }));
        } catch (e: any) {
            setError(e?.message || "Failed to save banner settings");
        } finally {
            setSavingBanner(false);
        }
    }, [session, bannerEnabled, bannerMessage]);

    const subscribers = data?.subscribers_by_tier || {};
    const formatUsd = (v: number) => `$${Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    const queueUtilization = Number(data?.queue_utilization_pct || 0);
    const highLoadDetected = Boolean(data?.high_load_detected);

    return (
        <div className="max-w-5xl mx-auto px-6 pb-10 space-y-6">
            <div className="flex items-center justify-between">
                <div>
                    <h2 className="text-xl font-bold text-white">Product Analytics</h2>
                    <p className="text-sm text-gray-500">
                        {backendOffline ? 'Backend offline - metrics are temporarily unavailable.' : 'Live admin metrics for usage, queue load, and billing status.'}
                    </p>
                </div>
                <button onClick={() => { loadAnalytics(); }} className="px-4 py-2 rounded-lg bg-white/5 hover:bg-white/10 text-sm text-gray-300 transition">
                    Refresh
                </button>
            </div>

            {loading && <p className="text-gray-500 text-sm">Loading analytics...</p>}
            {error && <p className="text-red-400 text-sm">{error}</p>}

            <div className="rounded-xl border border-emerald-500/30 bg-emerald-500/[0.06] p-4">
                <h3 className="font-semibold text-emerald-200">Waitlist Removed</h3>
                <p className="text-sm text-emerald-100 mt-1">
                    Studio now uses free slideshow access plus usage-based animation credits. Monthly waitlist/subscription onboarding has been retired.
                </p>
            </div>

            {data && !backendOffline && (
                <>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                        <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4">
                            <p className="text-xs text-gray-500 uppercase tracking-wider">Active Users (est.)</p>
                            <p className="text-2xl font-bold text-white mt-1">{data.active_users_estimate || 0}</p>
                            <p className="text-xs text-gray-500 mt-1">Sign-ins (15m): {data.active_users_signins_15m || 0}</p>
                        </div>
                        <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4">
                            <p className="text-xs text-gray-500 uppercase tracking-wider">Active Generations</p>
                            <p className="text-2xl font-bold text-white mt-1">{data.active_generations || 0}</p>
                            <p className="text-xs text-gray-500 mt-1">
                                Queue depth: {data.queue_depth || 0} / {data.queue_max_depth || 0} | Workers: {data.queue_workers || 0}
                            </p>
                        </div>
                        <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-4">
                            <p className="text-xs text-gray-500 uppercase tracking-wider">Monthly Profit (proxy)</p>
                            <p className="text-2xl font-bold text-emerald-400 mt-1">{formatUsd(data.monthly_profit_usd || 0)}</p>
                            <p className="text-xs text-gray-500 mt-1">Source: {data.revenue_source || 'none'}</p>
                            <p className="text-xs text-gray-500 mt-1">
                                Voices: {data.voice_provider_ok ? "ElevenLabs" : "Fallback"} ({data.voice_catalog_count || 0})
                            </p>
                        </div>
                    </div>
                    {data.voice_catalog_warning && (
                        <div className="rounded-xl border border-amber-400/30 bg-amber-500/10 p-4">
                            <p className="text-xs text-amber-200 uppercase tracking-wider">Voice Provider Warning</p>
                            <p className="text-sm text-amber-100 mt-1">{data.voice_catalog_warning}</p>
                        </div>
                    )}

                    <div className={`rounded-xl border p-4 ${highLoadDetected ? "border-amber-400/40 bg-amber-500/10" : "border-white/[0.08] bg-white/[0.02]"}`}>
                        <div className="flex items-center justify-between gap-4">
                            <div>
                                <p className="text-xs text-gray-500 uppercase tracking-wider">Load Status</p>
                                <p className={`text-lg font-bold mt-1 ${highLoadDetected ? "text-amber-300" : "text-emerald-300"}`}>
                                    {highLoadDetected ? "High Load Detected" : "Normal Load"}
                                </p>
                                <p className="text-xs text-gray-400 mt-1">
                                    Queue utilization: {queueUtilization.toFixed(1)}% | Active per worker: {Number(data.active_generations_per_worker || 0).toFixed(2)}
                                </p>
                            </div>
                        </div>
                    </div>

                    <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-5 space-y-3">
                        <h3 className="font-semibold text-white">High Load Banner (Admin)</h3>
                        <div className="flex items-center justify-between gap-3 rounded-lg border border-white/[0.08] bg-black/20 p-3">
                            <div>
                                <p className="text-sm text-white font-medium">Show warning banner</p>
                                <p className="text-xs text-gray-500">Students see longer queue warning during heavy load.</p>
                            </div>
                            <button
                                onClick={() => setBannerEnabled(v => !v)}
                                className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition ${bannerEnabled ? "bg-emerald-600/80 text-white" : "bg-white/10 text-gray-300 hover:bg-white/15"}`}
                            >
                                {bannerEnabled ? "ON" : "OFF"}
                            </button>
                        </div>
                        <textarea
                            value={bannerMessage}
                            onChange={(e) => setBannerMessage(e.target.value)}
                            rows={2}
                            className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-4 py-2.5 text-sm text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all resize-none"
                            placeholder="Studio is under high load. Queue times may be longer than usual while we scale capacity."
                        />
                        <div className="flex items-center gap-3">
                            <button
                                onClick={saveBanner}
                                disabled={savingBanner}
                                className="px-4 py-2 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-sm font-medium transition disabled:opacity-60"
                            >
                                {savingBanner ? "Saving..." : "Save Banner"}
                            </button>
                            <p className="text-xs text-gray-500">Current: {data.maintenance_banner_enabled ? "Visible" : "Hidden"}</p>
                        </div>
                    </div>

                    <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-5 space-y-3">
                        <h3 className="font-semibold text-white">Paid Tiers</h3>
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                            <div className="rounded-lg bg-black/30 border border-white/[0.06] p-3">
                                <p className="text-xs text-gray-500">Starter</p>
                                <p className="text-lg font-bold text-white">{subscribers.starter || 0}</p>
                            </div>
                            <div className="rounded-lg bg-black/30 border border-white/[0.06] p-3">
                                <p className="text-xs text-gray-500">Creator</p>
                                <p className="text-lg font-bold text-white">{subscribers.creator || 0}</p>
                            </div>
                            <div className="rounded-lg bg-black/30 border border-white/[0.06] p-3">
                                <p className="text-xs text-gray-500">Pro</p>
                                <p className="text-lg font-bold text-white">{subscribers.pro || 0}</p>
                            </div>
                            <div className="rounded-lg bg-black/30 border border-white/[0.06] p-3">
                                <p className="text-xs text-gray-500">Demo Pro</p>
                                <p className="text-lg font-bold text-white">{subscribers.demo_pro || 0}</p>
                            </div>
                        </div>
                        <p className="text-sm text-gray-400">
                            Total paid subscribers: <span className="text-violet-300 font-semibold">{data.total_paid_subscribers || 0}</span>
                            {' '}| Monthly revenue: <span className="text-emerald-300 font-semibold">{formatUsd(data.monthly_revenue_usd || 0)}</span>
                        </p>
                    </div>

                    <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-5 space-y-3">
                        <h3 className="font-semibold text-white">Billing Audit (status source)</h3>
                        <p className="text-xs text-gray-500">Tracks who paid, current status, renewal timestamp, and whether status comes from Stripe or profile fallback.</p>
                        <div className="overflow-x-auto">
                            <table className="min-w-full text-xs">
                                <thead>
                                    <tr className="text-gray-500 border-b border-white/[0.08]">
                                        <th className="text-left py-2 pr-3">Email</th>
                                        <th className="text-left py-2 pr-3">Plan</th>
                                        <th className="text-left py-2 pr-3">Stripe</th>
                                        <th className="text-left py-2 pr-3">Source</th>
                                        <th className="text-left py-2">Next Renewal</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {billingAuditRows.length === 0 ? (
                                        <tr>
                                            <td className="py-3 text-gray-500" colSpan={5}>No paid billing rows found.</td>
                                        </tr>
                                    ) : billingAuditRows.map((row, idx) => {
                                        const renewalUnix = Number(row.next_renewal_unix || 0);
                                        const renewalSource = String(row.next_renewal_source || "");
                                        const cancelAtPeriodEnd = Boolean(row.cancel_at_period_end);
                                        return (
                                            <tr key={`${row.email || 'row'}-${idx}`} className="border-b border-white/[0.05] text-gray-300">
                                                <td className="py-2 pr-3">{row.email || '-'}</td>
                                                <td className="py-2 pr-3">{row.plan || '-'}</td>
                                                <td className="py-2 pr-3">{row.stripe_status || '-'}</td>
                                                <td className="py-2 pr-3">{row.status_source || '-'}</td>
                                                <td className="py-2">
                                                    <div>
                                                        {cancelAtPeriodEnd
                                                            ? (renewalUnix > 0
                                                                ? `Scheduled cancel: ${new Date(renewalUnix * 1000).toLocaleString()}`
                                                                : 'Scheduled cancel at period end')
                                                            : (renewalUnix > 0 ? new Date(renewalUnix * 1000).toLocaleString() : '-')}
                                                    </div>
                                                    {renewalSource ? (
                                                        <div className="text-[10px] text-gray-500 mt-0.5">{renewalSource}</div>
                                                    ) : null}
                                                </td>
                                            </tr>
                                        );
                                    })}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </>
            )}
        </div>
    );
}
