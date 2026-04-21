import { useContext, useState } from 'react';
import { AuthContext, API } from '../shared';
import type { PageNav } from '../components/NavBar';

interface Props {
    onNavigate: PageNav;
}

type Plan = 'starter' | 'creator' | 'pro';
type Provider = 'stripe' | 'paypal';

const PLAN_DETAILS: Record<Plan, { label: string; price: number; blurb: string }> = {
    starter: { label: 'Starter', price: 14, blurb: '~10 animated renders/month + basic niches' },
    creator: { label: 'Creator', price: 24, blurb: '~20 animated renders/month + priority queue' },
    pro: { label: 'Pro', price: 39, blurb: '~40 animated renders/month + operator headroom' },
};

const DONATION_URL = 'https://paypal.me/YtItsOmatic';

export default function WaitlistPage({ onNavigate }: Props) {
    const { session } = useContext(AuthContext);
    const defaultEmail = session?.user?.email || '';
    const [email, setEmail] = useState(defaultEmail);
    const [name, setName] = useState('');
    const [plan, setPlan] = useState<Plan>('starter');
    const [provider, setProvider] = useState<Provider>('paypal');
    const [submitting, setSubmitting] = useState(false);
    const [error, setError] = useState('');

    const cancelled = typeof window !== 'undefined'
        && new URLSearchParams(window.location.search).get('status') === 'cancelled';

    const handleSubmit = async (e?: React.FormEvent) => {
        if (e) e.preventDefault();
        setError('');
        const trimmedEmail = email.trim().toLowerCase();
        if (!trimmedEmail || !trimmedEmail.includes('@')) {
            setError('Please enter a valid email address.');
            return;
        }
        setSubmitting(true);
        try {
            const headers: Record<string, string> = { 'Content-Type': 'application/json' };
            if (session?.access_token) headers['Authorization'] = `Bearer ${session.access_token}`;
            const res = await fetch(`${API}/api/waitlist/join`, {
                method: 'POST',
                headers,
                body: JSON.stringify({
                    plan,
                    email: trimmedEmail,
                    name: name.trim(),
                    provider,
                }),
            });
            if (!res.ok) {
                const body = await res.json().catch(() => ({ detail: 'Something went wrong' }));
                throw new Error(body?.detail || `Request failed (${res.status})`);
            }
            const data = await res.json();
            if (data?.checkout_url) {
                window.location.href = data.checkout_url;
                return;
            }
            throw new Error('No checkout URL returned');
        } catch (err: any) {
            setError(err?.message || 'Could not reach the waitlist service.');
            setSubmitting(false);
        }
    };

    return (
        <div className="min-h-screen bg-[#09090b] text-gray-100 px-4 py-10 sm:py-16">
            <div className="mx-auto max-w-2xl">
                <div className="mb-8 text-center">
                    <button
                        type="button"
                        onClick={() => onNavigate('landing')}
                        className="text-xs uppercase tracking-widest text-gray-500 hover:text-gray-300 transition"
                    >
                        ← Back to Studio
                    </button>
                </div>

                <div className="rounded-2xl border border-violet-500/20 bg-gradient-to-br from-violet-500/[0.04] to-cyan-500/[0.04] p-6 sm:p-10">
                    <h1 className="text-3xl sm:text-4xl font-bold text-white">Studio Open-Beta Waitlist</h1>
                    <p className="mt-3 text-gray-400 leading-relaxed">
                        Studio is in closed beta while we polish short-form rendering. Reserve your open-beta slot —
                        first month's subscription is charged up front. If development extends, we'll email you
                        before each monthly renewal.
                    </p>

                    {cancelled && (
                        <div className="mt-5 rounded-lg border border-amber-500/30 bg-amber-500/[0.08] px-4 py-3 text-sm text-amber-200">
                            Payment cancelled. Your slot is still held — try again when you're ready.
                        </div>
                    )}

                    <form onSubmit={handleSubmit} className="mt-8 space-y-6">
                        {/* Email */}
                        <div>
                            <label className="block text-xs uppercase tracking-wider text-gray-500 mb-2">Email</label>
                            <input
                                type="email"
                                value={email}
                                onChange={(e) => setEmail(e.target.value)}
                                disabled={submitting}
                                required
                                placeholder="you@example.com"
                                className="w-full rounded-lg border border-white/[0.08] bg-black/40 px-4 py-3 text-sm text-white placeholder:text-gray-600 focus:border-violet-400 focus:outline-none disabled:opacity-50"
                            />
                        </div>

                        {/* Name */}
                        <div>
                            <label className="block text-xs uppercase tracking-wider text-gray-500 mb-2">
                                Name <span className="text-gray-600 normal-case">(optional)</span>
                            </label>
                            <input
                                type="text"
                                value={name}
                                onChange={(e) => setName(e.target.value)}
                                disabled={submitting}
                                placeholder="First name or channel name"
                                className="w-full rounded-lg border border-white/[0.08] bg-black/40 px-4 py-3 text-sm text-white placeholder:text-gray-600 focus:border-violet-400 focus:outline-none disabled:opacity-50"
                            />
                        </div>

                        {/* Plan selector */}
                        <div>
                            <label className="block text-xs uppercase tracking-wider text-gray-500 mb-3">Pick a plan</label>
                            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                                {(Object.keys(PLAN_DETAILS) as Plan[]).map((p) => {
                                    const info = PLAN_DETAILS[p];
                                    const active = plan === p;
                                    return (
                                        <button
                                            key={p}
                                            type="button"
                                            onClick={() => setPlan(p)}
                                            disabled={submitting}
                                            className={`rounded-lg border p-4 text-left transition ${
                                                active
                                                    ? 'border-violet-400 bg-violet-500/[0.08]'
                                                    : 'border-white/[0.08] bg-white/[0.02] hover:border-white/20'
                                            }`}
                                        >
                                            <div className="flex items-baseline justify-between">
                                                <span className="text-sm font-semibold text-white">{info.label}</span>
                                                <span className="text-lg font-bold text-violet-200">${info.price}<span className="text-xs text-gray-500">/mo</span></span>
                                            </div>
                                            <p className="mt-2 text-[11px] text-gray-400 leading-snug">{info.blurb}</p>
                                        </button>
                                    );
                                })}
                            </div>
                        </div>

                        {/* Provider */}
                        <div>
                            <label className="block text-xs uppercase tracking-wider text-gray-500 mb-3">Payment method</label>
                            <div className="grid grid-cols-2 gap-3">
                                {(['stripe', 'paypal'] as Provider[]).map((p) => {
                                    const active = provider === p;
                                    const isStripe = p === 'stripe';
                                    return (
                                        <button
                                            key={p}
                                            type="button"
                                            onClick={() => !isStripe && setProvider(p)}
                                            disabled={submitting || isStripe}
                                            aria-disabled={isStripe}
                                            title={isStripe ? 'Card checkout is temporarily unavailable while we recover our Stripe account.' : undefined}
                                            className={`relative rounded-lg border p-4 text-sm font-semibold transition ${
                                                isStripe
                                                    ? 'border-white/[0.06] bg-white/[0.015] text-gray-500 cursor-not-allowed'
                                                    : active
                                                        ? 'border-cyan-400 bg-cyan-500/[0.08] text-white'
                                                        : 'border-white/[0.08] bg-white/[0.02] text-gray-300 hover:border-white/20'
                                            }`}
                                        >
                                            {isStripe ? (
                                                <span className="flex items-center justify-center gap-2">
                                                    <span className="line-through opacity-70">Pay with card (Stripe)</span>
                                                    <span className="rounded-full border border-amber-400/40 bg-amber-500/[0.12] px-2 py-0.5 text-[10px] uppercase tracking-wider text-amber-200">Coming soon</span>
                                                </span>
                                            ) : (
                                                'Pay with PayPal'
                                            )}
                                        </button>
                                    );
                                })}
                            </div>
                            <p className="mt-2 text-[11px] text-gray-500">
                                Card checkout is temporarily paused while we recover our Stripe account. PayPal works now and supports cards at checkout.
                            </p>
                        </div>

                        {error && (
                            <div className="rounded-lg border border-red-500/30 bg-red-500/[0.08] px-4 py-3 text-sm text-red-300">
                                {error}
                            </div>
                        )}

                        <button
                            type="submit"
                            disabled={submitting}
                            className="w-full rounded-lg bg-gradient-to-r from-violet-600 via-fuchsia-600 to-cyan-600 px-6 py-3 text-sm font-semibold text-white shadow-lg shadow-violet-900/30 transition hover:opacity-95 disabled:opacity-40"
                        >
                            {submitting ? 'Redirecting to checkout…' : `Reserve my ${PLAN_DETAILS[plan].label} slot — $${PLAN_DETAILS[plan].price}`}
                        </button>
                    </form>

                    <div className="mt-10 pt-6 border-t border-white/[0.06] text-center">
                        <p className="text-xs text-gray-500 mb-3">
                            Want to help development directly? Donate any amount via PayPal.
                        </p>
                        <a
                            href={DONATION_URL}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="inline-flex items-center gap-2 rounded-lg border border-blue-400/40 bg-blue-500/[0.08] px-4 py-2 text-sm font-semibold text-blue-200 transition hover:border-blue-300 hover:bg-blue-500/[0.12]"
                        >
                            ❤️ Donate to Studio development
                        </a>
                    </div>
                </div>
            </div>
        </div>
    );
}
