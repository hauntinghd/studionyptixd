import { useState, useEffect, createContext, useContext, useCallback } from 'react';
import { Wand2, UploadCloud, FileVideo, CheckCircle2, Loader2, Download, Zap, Shield, ArrowRight, LogOut, User, Crown, Monitor, Lock, Clock, Film, Layers, Sliders, Clapperboard, Globe, Image, Palette, Camera, Trash2, Plus, Sparkles, Eye, X } from 'lucide-react';
import { createClient, Session, SupabaseClient } from '@supabase/supabase-js';

const API = "";
const Logo = ({ size = 24 }: { size?: number }) => (
    <img src="/logo.png" alt="NYPTID" width={size} height={size} className="rounded-full" />
);

type Plan = 'free' | 'starter' | 'creator' | 'pro';
const PRICE_IDS: Record<string, string> = {
    starter: "price_1T4eT7BL8lRmwao2hHcUbcny",
    creator: "price_1T4eTUBL8lRmwao2EK3JDOpy",
    pro: "price_1T4eTjBL8lRmwao2q6WkoZLH",
};

interface AuthContextType {
    session: Session | null;
    supabase: SupabaseClient | null;
    plan: Plan;
    role: string;
    loading: boolean;
    signIn: (email: string, password: string) => Promise<string | null>;
    signUp: (email: string, password: string) => Promise<string | null>;
    signOut: () => Promise<void>;
    checkout: (plan: string) => Promise<void>;
}

const AuthContext = createContext<AuthContextType>({
    session: null, supabase: null, plan: 'free', role: 'user', loading: true,
    signIn: async () => null, signUp: async () => null, signOut: async () => {},
    checkout: async () => {},
});

function AuthProvider({ children }: { children: React.ReactNode }) {
    const [supabase, setSupabase] = useState<SupabaseClient | null>(null);
    const [session, setSession] = useState<Session | null>(null);
    const [plan, setPlan] = useState<Plan>('free');
    const [role, setRole] = useState<string>('user');
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        (async () => {
            try {
                const res = await fetch(`${API}/api/config`);
                const cfg = await res.json();
                if (cfg.supabase_url && cfg.supabase_anon_key) {
                    const sb = createClient(cfg.supabase_url, cfg.supabase_anon_key);
                    setSupabase(sb);
                    const { data: { session: s } } = await sb.auth.getSession();
                    setSession(s);
                    sb.auth.onAuthStateChange((_e, s) => setSession(s));
                }
            } catch { /* no supabase config yet -- free mode */ }
            setLoading(false);
        })();
    }, []);

    useEffect(() => {
        if (!session) { setPlan('free'); setRole('user'); return; }
        (async () => {
            try {
                const res = await fetch(`${API}/api/me`, {
                    headers: { Authorization: `Bearer ${session.access_token}` },
                });
                if (res.ok) {
                    const data = await res.json();
                    setPlan(data.plan || 'free');
                    setRole(data.role || 'user');
                }
            } catch { setPlan('free'); setRole('user'); }
        })();
    }, [session]);

    const signIn = useCallback(async (email: string, password: string): Promise<string | null> => {
        if (!supabase) return "Auth not configured yet";
        const { error } = await supabase.auth.signInWithPassword({ email, password });
        return error ? error.message : null;
    }, [supabase]);

    const signUp = useCallback(async (email: string, password: string): Promise<string | null> => {
        if (!supabase) return "Auth not configured yet";
        const { error } = await supabase.auth.signUp({ email, password });
        return error ? error.message : null;
    }, [supabase]);

    const signOut = useCallback(async () => {
        if (supabase) await supabase.auth.signOut();
        setSession(null);
        setPlan('free');
    }, [supabase]);

    const checkout = useCallback(async (planName: string) => {
        const priceId = PRICE_IDS[planName];
        if (!priceId || !session) return;
        try {
            const res = await fetch(`${API}/api/checkout`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Authorization: `Bearer ${session.access_token}`,
                },
                body: JSON.stringify({ price_id: priceId }),
            });
            const data = await res.json();
            if (data.checkout_url) window.location.href = data.checkout_url;
        } catch (e) { console.error("Checkout failed", e); }
    }, [session]);

    return (
        <AuthContext.Provider value={{ session, supabase, plan, role, loading, signIn, signUp, signOut, checkout }}>
            {children}
        </AuthContext.Provider>
    );
}

function App() {
    const [page, setPage] = useState<'landing' | 'dashboard' | 'auth' | 'account'>('landing');

    return (
        <AuthProvider>
            <div className="min-h-screen bg-[#09090b] text-gray-100 font-sans selection:bg-violet-500/30">
                {page === 'landing' && <LandingPage onNavigate={setPage} />}
                {page === 'dashboard' && <DashboardPage onNavigate={setPage} />}
                {page === 'auth' && <AuthPage onNavigate={setPage} />}
                {page === 'account' && <AccountPage onNavigate={setPage} />}
            </div>
        </AuthProvider>
    );
}

export default App;

type PageNav = (p: 'landing' | 'dashboard' | 'auth' | 'account') => void;

function NavBar({ onNavigate, active }: { onNavigate: PageNav; active?: string }) {
    const { session, signOut } = useContext(AuthContext);

    return (
        <nav className="fixed top-0 w-full z-50 backdrop-blur-xl bg-[#09090b]/80 border-b border-white/5">
            <div className="max-w-7xl mx-auto px-6 h-16 flex items-center justify-between">
                <button onClick={() => onNavigate('landing')} className="flex items-center gap-2.5 hover:opacity-80 transition">
                    <Logo size={32} />
                    <span className="text-xl font-bold tracking-tight">NYPTID Studio</span>
                </button>
                <div className="flex items-center gap-3">
                    <button onClick={() => onNavigate('dashboard')}
                        className={`px-4 py-2 text-sm font-medium rounded-lg transition ${active === 'dashboard' ? 'text-white bg-violet-600/20 border border-violet-500/30' : 'text-gray-400 hover:text-white hover:bg-white/5'}`}>
                        Dashboard
                    </button>
                    <div className="w-px h-6 bg-white/10 mx-1" />
                    {session ? (
                        <div className="flex items-center gap-2">
                            <button onClick={() => onNavigate('account')}
                                className="flex items-center gap-2 px-3 py-2 text-sm text-gray-300 hover:text-white hover:bg-white/5 rounded-lg transition">
                                <User className="w-4 h-4" />
                                <span className="hidden sm:inline max-w-[120px] truncate">{session.user.email}</span>
                            </button>
                            <button onClick={signOut}
                                className="p-2 text-gray-500 hover:text-red-400 hover:bg-red-500/10 rounded-lg transition" title="Sign Out">
                                <LogOut className="w-4 h-4" />
                            </button>
                        </div>
                    ) : (
                        <button onClick={() => onNavigate('auth')}
                            className="px-5 py-2 text-sm font-medium text-white bg-violet-600 hover:bg-violet-500 rounded-lg transition-all shadow-lg shadow-violet-600/20">
                            Sign In
                        </button>
                    )}
                </div>
            </div>
        </nav>
    );
}


/* ═══════════════════════════════════════════════════════════════════════════
   AUTH PAGE
   ═══════════════════════════════════════════════════════════════════════════ */

function AuthPage({ onNavigate }: { onNavigate: PageNav }) {
    const { signIn, signUp, session } = useContext(AuthContext);
    const [mode, setMode] = useState<'login' | 'signup'>('login');
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const [error, setError] = useState('');
    const [loading, setLoading] = useState(false);
    const [success, setSuccess] = useState('');

    useEffect(() => {
        if (session) onNavigate('dashboard');
    }, [session, onNavigate]);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setError('');
        setSuccess('');
        setLoading(true);

        const result = mode === 'login'
            ? await signIn(email, password)
            : await signUp(email, password);

        if (result) {
            setError(result);
        } else if (mode === 'signup') {
            setSuccess('Account created! Check your email to confirm, then sign in.');
        }
        setLoading(false);
    };

    return (
        <div className="min-h-screen flex items-center justify-center px-6">
            <div className="w-full max-w-md">
                <div className="text-center mb-8">
                    <button onClick={() => onNavigate('landing')} className="inline-flex items-center gap-2.5 mb-6 hover:opacity-80 transition">
                        <Logo size={40} />
                    </button>
                    <h1 className="text-3xl font-bold mb-2">{mode === 'login' ? 'Welcome Back' : 'Create Account'}</h1>
                    <p className="text-gray-500">
                        {mode === 'login' ? 'Sign in to start generating' : 'Join NYPTID Studio today'}
                    </p>
                </div>

                <form onSubmit={handleSubmit} className="space-y-4">
                    <div>
                        <label className="block text-sm text-gray-400 mb-1.5">Email</label>
                        <input type="email" value={email} onChange={e => setEmail(e.target.value)}
                            required autoFocus
                            className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-4 py-3 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition" />
                    </div>
                    <div>
                        <label className="block text-sm text-gray-400 mb-1.5">Password</label>
                        <input type="password" value={password} onChange={e => setPassword(e.target.value)}
                            required minLength={6}
                            className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-4 py-3 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition" />
                    </div>

                    {error && (
                        <div className="p-3 rounded-lg bg-red-500/10 border border-red-500/20 text-red-400 text-sm">{error}</div>
                    )}
                    {success && (
                        <div className="p-3 rounded-lg bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-sm">{success}</div>
                    )}

                    <button type="submit" disabled={loading}
                        className="w-full py-3.5 bg-violet-600 hover:bg-violet-500 disabled:opacity-50 text-white font-bold rounded-xl transition-all flex items-center justify-center gap-2 shadow-lg shadow-violet-600/20">
                        {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : null}
                        {mode === 'login' ? 'Sign In' : 'Create Account'}
                    </button>
                </form>

                <p className="text-center text-sm text-gray-500 mt-6">
                    {mode === 'login' ? "Don't have an account? " : 'Already have an account? '}
                    <button onClick={() => { setMode(mode === 'login' ? 'signup' : 'login'); setError(''); setSuccess(''); }}
                        className="text-violet-400 hover:text-violet-300 font-medium transition">
                        {mode === 'login' ? 'Sign up' : 'Sign in'}
                    </button>
                </p>
            </div>
        </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════════════
   ACCOUNT PAGE
   ═══════════════════════════════════════════════════════════════════════════ */

function AccountPage({ onNavigate }: { onNavigate: PageNav }) {
    const { session, plan, signOut, checkout } = useContext(AuthContext);

    useEffect(() => {
        if (!session) onNavigate('auth');
    }, [session, onNavigate]);

    const planNames: Record<Plan, string> = { free: 'Free', starter: 'Starter', creator: 'Creator', pro: 'Pro' };
    const planColors: Record<Plan, string> = { free: 'text-gray-400', starter: 'text-blue-400', creator: 'text-violet-400', pro: 'text-amber-400' };

    return (
        <>
            <NavBar onNavigate={onNavigate} active="account" />
            <div className="pt-24 max-w-2xl mx-auto px-6">
                <h1 className="text-3xl font-bold mb-8">Your Account</h1>

                <div className="space-y-6">
                    <div className="p-6 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                        <div className="flex items-center gap-4 mb-4">
                            <div className="w-14 h-14 rounded-full bg-violet-500/10 flex items-center justify-center">
                                <User className="w-7 h-7 text-violet-400" />
                            </div>
                            <div>
                                <p className="font-bold text-lg">{session?.user.email}</p>
                                <div className="flex items-center gap-2 mt-0.5">
                                    <Crown className={`w-4 h-4 ${planColors[plan]}`} />
                                    <span className={`text-sm font-medium ${planColors[plan]}`}>{planNames[plan]} Plan</span>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div className="p-6 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                        <h3 className="font-bold text-lg mb-4">Plan Features</h3>
                        <div className="grid grid-cols-2 gap-4 text-sm">
                            <div>
                                <p className="text-gray-500">Videos/month</p>
                                <p className="font-bold text-white">{plan === 'pro' ? 'Unlimited' : plan === 'creator' ? '150' : plan === 'starter' ? '50' : '3'}</p>
                            </div>
                            <div>
                                <p className="text-gray-500">Max Resolution</p>
                                <p className="font-bold text-white">{plan === 'creator' || plan === 'pro' ? '1080p' : '720p'}</p>
                            </div>
                            <div>
                                <p className="text-gray-500">Clone Feature</p>
                                <p className={`font-bold ${plan === 'creator' || plan === 'pro' ? 'text-emerald-400' : 'text-gray-600'}`}>
                                    {plan === 'creator' || plan === 'pro' ? 'Enabled' : 'Locked'}
                                </p>
                            </div>
                            <div>
                                <p className="text-gray-500">Priority Queue</p>
                                <p className={`font-bold ${plan === 'creator' || plan === 'pro' ? 'text-emerald-400' : 'text-gray-600'}`}>
                                    {plan === 'creator' || plan === 'pro' ? 'Yes' : 'No'}
                                </p>
                            </div>
                        </div>
                    </div>

                    {plan !== 'pro' && (
                        <div className="p-6 rounded-2xl bg-gradient-to-br from-violet-500/5 to-purple-500/5 border border-violet-500/20">
                            <h3 className="font-bold text-lg mb-2">Upgrade Your Plan</h3>
                            <p className="text-gray-400 text-sm mb-4">
                                Unlock 1080p output, clone viral shorts, priority rendering, and more.
                            </p>
                            <div className="flex gap-3">
                                {plan === 'free' && (
                                    <button onClick={() => checkout('starter')}
                                        className="px-5 py-2.5 bg-white/5 hover:bg-white/10 text-white font-medium rounded-xl transition border border-white/10 text-sm">
                                        Starter $14/mo
                                    </button>
                                )}
                                {(plan === 'free' || plan === 'starter') && (
                                    <button onClick={() => checkout('creator')}
                                        className="px-5 py-2.5 bg-violet-600 hover:bg-violet-500 text-white font-bold rounded-xl transition-all shadow-lg shadow-violet-600/20 text-sm">
                                        Creator $24/mo
                                    </button>
                                )}
                                <button onClick={() => checkout('pro')}
                                    className="px-5 py-2.5 bg-white/5 hover:bg-white/10 text-white font-medium rounded-xl transition border border-white/10 text-sm">
                                    Pro $39/mo
                                </button>
                            </div>
                        </div>
                    )}

                    <button onClick={signOut}
                        className="w-full py-3 bg-red-500/10 hover:bg-red-500/20 text-red-400 font-medium rounded-xl transition border border-red-500/20">
                        Sign Out
                    </button>
                </div>
            </div>
        </>
    );
}

/* ═══════════════════════════════════════════════════════════════════════════
   LANDING PAGE
   ═══════════════════════════════════════════════════════════════════════════ */

function LandingPage({ onNavigate }: { onNavigate: PageNav }) {
    const { session, checkout } = useContext(AuthContext);

    return (
        <>
            <NavBar onNavigate={onNavigate} />

            {/* HERO */}
            <section className="relative pt-32 pb-24 overflow-hidden">
                <div className="absolute inset-0 bg-gradient-to-b from-violet-600/10 via-transparent to-transparent" />
                <div className="absolute top-20 left-1/2 -translate-x-1/2 w-[800px] h-[800px] bg-violet-600/5 rounded-full blur-[120px]" />

                <div className="relative max-w-5xl mx-auto px-6 text-center">
                    <div className="inline-flex items-center gap-2 px-4 py-1.5 rounded-full bg-violet-500/10 border border-violet-500/20 text-violet-300 text-sm font-medium mb-8">
                        <Zap className="w-4 h-4" />
                        NVIDIA A40 GPU-Powered Generation
                    </div>

                    <h1 className="text-6xl md:text-7xl font-extrabold tracking-tight leading-[1.1] mb-6">
                        Create Viral Shorts<br />
                        <span className="text-transparent bg-clip-text bg-gradient-to-r from-violet-400 via-purple-400 to-indigo-400">
                            Like Zach D Films
                        </span>
                    </h1>

                    <p className="text-xl text-gray-400 max-w-2xl mx-auto mb-10 leading-relaxed">
                        Pixar-quality 3D animated shorts, cinematic AI stories, viral debates, and more.
                        Generate professional short-form videos in minutes, not hours. Up to 1080p.
                    </p>

                    <div className="flex flex-col sm:flex-row items-center justify-center gap-4 mb-16">
                        <button onClick={() => onNavigate(session ? 'dashboard' : 'auth')}
                            className="group px-8 py-4 bg-violet-600 hover:bg-violet-500 text-white font-bold rounded-xl text-lg transition-all flex items-center gap-2 shadow-lg shadow-violet-600/25">
                            {session ? 'Open Studio' : 'Get Started Free'}
                            <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
                        </button>
                        <button onClick={() => onNavigate(session ? 'dashboard' : 'auth')}
                            className="px-8 py-4 bg-white/5 hover:bg-white/10 text-white font-medium rounded-xl text-lg transition-all border border-white/10">
                            Clone a Viral Short
                        </button>
                    </div>

                    <div className="grid grid-cols-4 gap-6 max-w-xl mx-auto">
                        {[
                            { val: '10', label: 'Templates' },
                            { val: '1080p', label: 'Max Quality' },
                            { val: '48GB', label: 'GPU VRAM' },
                            { val: '<5 min', label: 'Generation' },
                        ].map((s, i) => (
                            <div key={i}>
                                <div className="text-2xl md:text-3xl font-bold text-white">{s.val}</div>
                                <div className="text-xs md:text-sm text-gray-500">{s.label}</div>
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            {/* TEMPLATES SHOWCASE */}
            <section className="py-24 border-t border-white/5">
                <div className="max-w-6xl mx-auto px-6">
                    <div className="text-center mb-16">
                        <h2 className="text-4xl font-bold mb-4">Choose Your Style</h2>
                        <p className="text-gray-400 text-lg">Every template is engineered for maximum retention and virality</p>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                        {[
                            { title: 'Skeleton AI', desc: '3D skeleton characters in outfits. Career salary comparisons, dark humor, viral breakdowns.', icon: '💀', color: 'from-gray-600 to-gray-800', tag: 'TRENDING' },
                            { title: 'Objects Explain', desc: 'Everyday objects come to life and explain how they work. Educational + hilarious.', icon: '🔌', color: 'from-teal-600 to-teal-800', tag: 'TRENDING' },
                            { title: 'Would You Rather', desc: 'Impossible dilemmas that everyone needs to answer. Insane engagement.', icon: '🤔', color: 'from-purple-600 to-purple-800', tag: 'HOT' },
                            { title: 'Scary Stories', desc: 'Bone-chilling true crime and horror stories with Fincher-level atmosphere.', icon: '👻', color: 'from-zinc-700 to-zinc-900', tag: 'POPULAR' },
                            { title: 'Historical Epic', desc: 'Cinematic historical content. Battles, empires, dramatic reveals.', icon: '⚔️', color: 'from-amber-700 to-amber-900', tag: 'NEW' },
                            { title: 'Argument Debate', desc: 'Two sides debate hot topics. Viewers pick a side in comments.', icon: '🗣️', color: 'from-rose-600 to-rose-800', tag: 'HOT' },
                            { title: 'Motivation', desc: 'Powerful life advice with epic cinematic landscapes. Screenshot-worthy.', icon: '🔥', color: 'from-amber-600 to-amber-800', tag: null },
                            { title: 'What If', desc: 'Mind-bending hypotheticals with real science. Maximum curiosity gap.', icon: '🌍', color: 'from-indigo-600 to-indigo-800', tag: null },
                            { title: 'Top 5 Lists', desc: 'Countdown videos with dramatic reveals and bold visuals.', icon: '🏆', color: 'from-yellow-700 to-yellow-900', tag: null },
                            { title: 'Chaos Mode', desc: 'Maximum retention brain content. Fast, unpredictable, pure viral.', icon: '🌀', color: 'from-emerald-700 to-emerald-900', tag: null },
                        ].map((t, i) => (
                            <div key={i} onClick={() => onNavigate(session ? 'dashboard' : 'auth')}
                                className="group relative p-6 rounded-2xl bg-white/[0.03] border border-white/[0.06] hover:border-violet-500/30 hover:bg-violet-500/[0.03] transition-all cursor-pointer">
                                {t.tag && (
                                    <span className="absolute top-4 right-4 text-[10px] font-bold tracking-wider px-2 py-0.5 rounded-full bg-violet-500/20 text-violet-300 border border-violet-500/30">
                                        {t.tag}
                                    </span>
                                )}
                                <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${t.color} flex items-center justify-center text-xl mb-4`}>
                                    {t.icon}
                                </div>
                                <h3 className="text-lg font-bold mb-2 group-hover:text-violet-300 transition-colors">{t.title}</h3>
                                <p className="text-gray-500 text-sm leading-relaxed">{t.desc}</p>
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            {/* HOW IT WORKS */}
            <section className="py-24 border-t border-white/5">
                <div className="max-w-5xl mx-auto px-6">
                    <div className="text-center mb-16">
                        <h2 className="text-4xl font-bold mb-4">How It Works</h2>
                        <p className="text-gray-400 text-lg">From idea to viral short in 3 steps</p>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
                        {[
                            { step: '01', title: 'Choose Template & Topic', desc: 'Pick a template style and enter your topic. Our AI scriptwriter crafts hook-optimized scenes instantly.' },
                            { step: '02', title: 'AI Generates Everything', desc: 'Self-hosted NVIDIA A40 GPU renders 3D scenes. ElevenLabs creates the voiceover. All fully automated.' },
                            { step: '03', title: 'Download & Post', desc: 'Get a production-ready MP4 with text overlays, voiceover, and optimized SEO metadata. Upload and watch it go viral.' },
                        ].map((s, i) => (
                            <div key={i} className="relative p-8 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                                <div className="text-5xl font-black text-white/[0.04] absolute top-4 right-6">{s.step}</div>
                                <h3 className="text-xl font-bold mb-3">{s.title}</h3>
                                <p className="text-gray-500 leading-relaxed">{s.desc}</p>
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            {/* WHY US vs COMPETITION */}
            <section className="py-24 border-t border-white/5">
                <div className="max-w-6xl mx-auto px-6">
                    <div className="text-center mb-16">
                        <h2 className="text-4xl font-bold mb-4">Why NYPTID Studio</h2>
                        <p className="text-gray-400 text-lg">We're not another credit-burning API wrapper. Here's the difference.</p>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-12">
                        <div className="p-8 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                            <div className="flex items-center gap-3 mb-6">
                                <div className="w-10 h-10 rounded-lg bg-red-500/10 flex items-center justify-center text-sm font-bold text-red-400">X</div>
                                <h3 className="text-lg font-bold text-gray-400">Other Platforms</h3>
                            </div>
                            <ul className="space-y-3">
                                {[
                                    'Image slideshow with zoom = "video"',
                                    'Credit-based -- costs add up fast',
                                    'Shared API backends -- slow at peak',
                                    '720p max on most plans',
                                    'Generic templates, no customization',
                                ].map((item, i) => (
                                    <li key={i} className="flex items-start gap-2 text-sm text-gray-500">
                                        <span className="text-red-400 mt-0.5">-</span>{item}
                                    </li>
                                ))}
                            </ul>
                        </div>

                        <div className="p-8 rounded-2xl bg-violet-500/[0.03] border border-violet-500/20">
                            <div className="flex items-center gap-3 mb-6">
                                <Logo size={28} />
                                <h3 className="text-lg font-bold text-violet-300">NYPTID Studio</h3>
                            </div>
                            <ul className="space-y-3">
                                {[
                                    'Real 3D rendered scenes via SDXL on dedicated GPU',
                                    'Flat monthly pricing -- no credit anxiety',
                                    'Self-hosted NVIDIA A40 (48GB VRAM) -- consistently fast',
                                    'Up to 1080p output with latent upscaling',
                                    'Clone any viral short and remake it on your topic',
                                ].map((item, i) => (
                                    <li key={i} className="flex items-start gap-2 text-sm text-gray-300">
                                        <CheckCircle2 className="w-4 h-4 text-violet-400 shrink-0 mt-0.5" />{item}
                                    </li>
                                ))}
                            </ul>
                        </div>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
                        <div className="p-8 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                            <div className="w-12 h-12 rounded-xl bg-violet-500/10 flex items-center justify-center mb-5">
                                <Zap className="w-6 h-6 text-violet-400" />
                            </div>
                            <h3 className="text-xl font-bold mb-3">Dedicated GPU Power</h3>
                            <p className="text-gray-500 leading-relaxed">
                                NVIDIA A40 with 48GB VRAM renders every frame. No shared infrastructure, no throttling. Your generation gets the full GPU.
                            </p>
                        </div>
                        <div className="p-8 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                            <div className="w-12 h-12 rounded-xl bg-emerald-500/10 flex items-center justify-center mb-5">
                                <Shield className="w-6 h-6 text-emerald-400" />
                            </div>
                            <h3 className="text-xl font-bold mb-3">Clone Any Viral Short</h3>
                            <p className="text-gray-500 leading-relaxed">
                                Upload a viral short that hit 100K+ views. AI detects the template, reverse-engineers the formula, and generates a new one on your topic.
                            </p>
                        </div>
                        <div className="p-8 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                            <div className="w-12 h-12 rounded-xl bg-amber-500/10 flex items-center justify-center mb-5">
                                <Monitor className="w-6 h-6 text-amber-400" />
                            </div>
                            <h3 className="text-xl font-bold mb-3">Up to 1080p Output</h3>
                            <p className="text-gray-500 leading-relaxed">
                                Generate at native SDXL resolution with AI upscaling to full 1080p. Crisp, professional shorts that stand out in any feed.
                            </p>
                        </div>
                    </div>
                </div>
            </section>

            {/* COMING SOON */}
            <section className="py-24 border-t border-white/5">
                <div className="max-w-6xl mx-auto px-6">
                    <div className="text-center mb-16">
                        <div className="inline-flex items-center gap-2 px-4 py-1.5 rounded-full bg-amber-500/10 border border-amber-500/20 text-amber-300 text-sm font-medium mb-6">
                            <Clock className="w-4 h-4" />
                            On the Roadmap
                        </div>
                        <h2 className="text-4xl font-bold mb-4">Coming Soon</h2>
                        <p className="text-gray-400 text-lg max-w-2xl mx-auto">We're building the most advanced AI video platform on the planet. Here's what's next.</p>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                        {[
                            {
                                icon: <Film className="w-6 h-6 text-rose-400" />,
                                iconBg: 'bg-rose-500/10',
                                title: 'Long-Form Documentaries',
                                desc: '10-20 minute AI-generated documentaries with full script, scene breakdown, voiceover, and cinematic 3D animation. YouTube-ready.',
                                tag: 'Q2 2026',
                            },
                            {
                                icon: <Clapperboard className="w-6 h-6 text-cyan-400" />,
                                iconBg: 'bg-cyan-500/10',
                                title: '3D Character Animation',
                                desc: 'Consistent 3D characters across every scene with smooth animation, cinematic camera work, and studio-quality lighting.',
                                tag: 'Q2 2026',
                            },
                            {
                                icon: <Layers className="w-6 h-6 text-emerald-400" />,
                                iconBg: 'bg-emerald-500/10',
                                title: 'Scene-by-Scene Editor',
                                desc: 'Pick, swap, and regenerate individual images per scene before final render. Full creative control over every frame.',
                                tag: 'Q2 2026',
                            },
                            {
                                icon: <Globe className="w-6 h-6 text-violet-400" />,
                                iconBg: 'bg-violet-500/10',
                                title: 'Multi-Niche Templates',
                                desc: 'Business, Tech, Crypto, Science, Entertainment, Education -- purpose-built templates with niche-optimized hooks and visuals.',
                                tag: 'Q3 2026',
                            },
                            {
                                icon: <Sliders className="w-6 h-6 text-amber-400" />,
                                iconBg: 'bg-amber-500/10',
                                title: 'Custom Video Length',
                                desc: 'Slider from 15-second reels to 20+ minute long-form. The AI adapts pacing, scene count, and narration depth automatically.',
                                tag: 'Q3 2026',
                            },
                            {
                                icon: <Wand2 className="w-6 h-6 text-pink-400" />,
                                iconBg: 'bg-pink-500/10',
                                title: 'Full Sentient Pipeline',
                                desc: 'End-to-end generation: topic research, SEO-optimized titles, thumbnails, descriptions, hashtags, and scheduled auto-posting.',
                                tag: 'Q4 2026',
                            },
                        ].map((item, i) => (
                            <div key={i} className="group relative p-6 rounded-2xl bg-white/[0.02] border border-white/[0.06] hover:border-white/[0.12] transition-all">
                                <span className="absolute top-4 right-4 text-[10px] font-bold tracking-wider px-2 py-0.5 rounded-full bg-amber-500/10 text-amber-300 border border-amber-500/20">
                                    {item.tag}
                                </span>
                                <div className={`w-12 h-12 rounded-xl ${item.iconBg} flex items-center justify-center mb-4`}>
                                    {item.icon}
                                </div>
                                <h3 className="text-lg font-bold mb-2">{item.title}</h3>
                                <p className="text-gray-500 text-sm leading-relaxed">{item.desc}</p>
                            </div>
                        ))}
                    </div>
                </div>
            </section>

            {/* PRICING */}
            <section className="py-24 border-t border-white/5" id="pricing">
                <div className="max-w-5xl mx-auto px-6">
                    <div className="text-center mb-16">
                        <h2 className="text-4xl font-bold mb-4">Simple, Honest Pricing</h2>
                        <p className="text-gray-400 text-lg">Start free. No credit card required. Upgrade when you're ready to scale.</p>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-4 gap-5">
                        {/* FREE */}
                        <div className="p-6 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                            <h3 className="text-lg font-bold mb-1">Free</h3>
                            <p className="text-gray-500 text-xs mb-5">Try it out</p>
                            <div className="flex items-baseline gap-1 mb-5">
                                <span className="text-3xl font-extrabold">$0</span>
                            </div>
                            <ul className="space-y-2.5 mb-6">
                                {['3 videos/month', 'All 10 templates', '30s max', '720p output'].map((f, i) => (
                                    <li key={i} className="flex items-center gap-2 text-xs text-gray-400">
                                        <CheckCircle2 className="w-3.5 h-3.5 text-gray-600 shrink-0" />{f}
                                    </li>
                                ))}
                            </ul>
                            <button onClick={() => onNavigate(session ? 'dashboard' : 'auth')}
                                className="w-full py-2.5 rounded-lg bg-white/5 hover:bg-white/10 text-white text-sm font-medium transition-all border border-white/10">
                                {session ? 'Open Studio' : 'Sign Up Free'}
                            </button>
                        </div>

                        {/* STARTER */}
                        <div className="p-6 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                            <h3 className="text-lg font-bold mb-1">Starter</h3>
                            <p className="text-gray-500 text-xs mb-5">Getting started</p>
                            <div className="flex items-baseline gap-1 mb-5">
                                <span className="text-3xl font-extrabold">$14</span>
                                <span className="text-gray-500 text-sm">/mo</span>
                            </div>
                            <ul className="space-y-2.5 mb-6">
                                {['50 videos/month', 'All 10 templates', '60s per video', '720p output', 'Standard speed'].map((f, i) => (
                                    <li key={i} className="flex items-center gap-2 text-xs text-gray-400">
                                        <CheckCircle2 className="w-3.5 h-3.5 text-violet-400 shrink-0" />{f}
                                    </li>
                                ))}
                            </ul>
                            <button onClick={() => session ? checkout('starter') : onNavigate('auth')}
                                className="w-full py-2.5 rounded-lg bg-white/5 hover:bg-white/10 text-white text-sm font-medium transition-all border border-white/10">
                                {session ? 'Choose Starter' : 'Sign Up to Subscribe'}
                            </button>
                        </div>

                        {/* CREATOR */}
                        <div className="relative p-6 rounded-2xl bg-violet-500/[0.05] border-2 border-violet-500/30 shadow-xl shadow-violet-500/5">
                            <span className="absolute -top-3 left-1/2 -translate-x-1/2 px-3 py-1 bg-violet-600 text-white text-[10px] font-bold rounded-full tracking-wide">
                                MOST POPULAR
                            </span>
                            <h3 className="text-lg font-bold mb-1">Creator</h3>
                            <p className="text-gray-500 text-xs mb-5">For serious creators</p>
                            <div className="flex items-baseline gap-1 mb-5">
                                <span className="text-3xl font-extrabold">$24</span>
                                <span className="text-gray-500 text-sm">/mo</span>
                            </div>
                            <ul className="space-y-2.5 mb-6">
                                {['150 videos/month', 'All 10 templates', '3 min per video', '1080p output', 'Priority speed', 'Clone viral shorts'].map((f, i) => (
                                    <li key={i} className="flex items-center gap-2 text-xs text-gray-300">
                                        <CheckCircle2 className="w-3.5 h-3.5 text-violet-400 shrink-0" />{f}
                                    </li>
                                ))}
                            </ul>
                            <button onClick={() => session ? checkout('creator') : onNavigate('auth')}
                                className="w-full py-2.5 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-sm font-bold transition-all shadow-lg shadow-violet-600/25">
                                {session ? 'Choose Creator' : 'Sign Up to Subscribe'}
                            </button>
                        </div>

                        {/* PRO */}
                        <div className="p-6 rounded-2xl bg-white/[0.02] border border-white/[0.06]">
                            <h3 className="text-lg font-bold mb-1">Pro</h3>
                            <p className="text-gray-500 text-xs mb-5">Agencies &amp; power users</p>
                            <div className="flex items-baseline gap-1 mb-5">
                                <span className="text-3xl font-extrabold">$39</span>
                                <span className="text-gray-500 text-sm">/mo</span>
                            </div>
                            <ul className="space-y-2.5 mb-6">
                                {['Unlimited videos', 'All 10 templates', '5 min per video', '1080p output', 'Max priority speed', 'Clone viral shorts', 'Priority support'].map((f, i) => (
                                    <li key={i} className="flex items-center gap-2 text-xs text-gray-400">
                                        <CheckCircle2 className="w-3.5 h-3.5 text-violet-400 shrink-0" />{f}
                                    </li>
                                ))}
                            </ul>
                            <button onClick={() => session ? checkout('pro') : onNavigate('auth')}
                                className="w-full py-2.5 rounded-lg bg-white/5 hover:bg-white/10 text-white text-sm font-medium transition-all border border-white/10">
                                {session ? 'Choose Pro' : 'Sign Up to Subscribe'}
                            </button>
                        </div>
                    </div>
                </div>
            </section>

            {/* FOOTER */}
            <footer className="py-12 border-t border-white/5">
                <div className="max-w-6xl mx-auto px-6 flex flex-col md:flex-row items-center justify-between gap-4">
                    <div className="flex items-center gap-2">
                        <Logo size={22} />
                        <span className="font-bold">NYPTID Studio</span>
                        <span className="text-gray-600 text-sm ml-2">by NYPTID Industries</span>
                    </div>
                    <p className="text-gray-600 text-sm">&copy; 2026 NYPTID Industries. All rights reserved.</p>
                </div>
            </footer>
        </>
    );
}

/* ═══════════════════════════════════════════════════════════════════════════
   DASHBOARD PAGE (Gated Hub for Create + Clone)
   ═══════════════════════════════════════════════════════════════════════════ */

function DashboardPage({ onNavigate }: { onNavigate: PageNav }) {
    const { session, plan, role } = useContext(AuthContext);
    const isAdmin = role === 'admin';
    const [tab, setTab] = useState<'create' | 'clone' | 'thumbnails' | 'demo'>('create');

    useEffect(() => {
        if (!session) onNavigate('auth');
    }, [session, onNavigate]);

    if (!session) return null;

    return (
        <div className="min-h-screen">
            <NavBar onNavigate={onNavigate} active="dashboard" />

            <div className="max-w-5xl mx-auto px-6 pt-24 pb-6">
                <div className="flex items-center justify-between mb-8">
                    <div>
                        <h1 className="text-2xl font-bold">Welcome back</h1>
                        <p className="text-gray-500 text-sm mt-1">
                            <span className="capitalize text-violet-400 font-medium">{plan}</span> plan
                            {plan === 'free' && <span className="text-gray-600"> -- <button onClick={() => onNavigate('account')} className="text-violet-400 hover:text-violet-300 transition">Upgrade</button> for more</span>}
                        </p>
                    </div>
                    <button onClick={() => onNavigate('account')}
                        className="flex items-center gap-2 px-4 py-2 bg-white/[0.03] border border-white/[0.06] rounded-xl text-sm text-gray-400 hover:text-white hover:border-white/[0.12] transition">
                        <Crown className="w-4 h-4" />
                        Account
                    </button>
                </div>

                <div className="flex gap-1 p-1 bg-white/[0.03] border border-white/[0.06] rounded-xl mb-8">
                    <button onClick={() => setTab('create')}
                        className={`flex-1 flex items-center justify-center gap-2 py-3 rounded-lg text-sm font-medium transition-all ${
                            tab === 'create'
                                ? 'bg-violet-600 text-white shadow-lg shadow-violet-600/20'
                                : 'text-gray-400 hover:text-white hover:bg-white/[0.03]'
                        }`}>
                        <Wand2 className="w-4 h-4" />
                        Create
                    </button>
                    <button onClick={() => setTab('clone')}
                        className={`flex-1 flex items-center justify-center gap-2 py-3 rounded-lg text-sm font-medium transition-all ${
                            tab === 'clone'
                                ? 'bg-violet-600 text-white shadow-lg shadow-violet-600/20'
                                : 'text-gray-400 hover:text-white hover:bg-white/[0.03]'
                        }`}>
                        <FileVideo className="w-4 h-4" />
                        Clone
                        {plan !== 'creator' && plan !== 'pro' && (
                            <Lock className="w-3 h-3 text-gray-500" />
                        )}
                    </button>
                    {isAdmin && (
                    <button onClick={() => setTab('thumbnails')}
                        className={`flex-1 flex items-center justify-center gap-2 py-3 rounded-lg text-sm font-medium transition-all ${
                            tab === 'thumbnails'
                                ? 'bg-violet-600 text-white shadow-lg shadow-violet-600/20'
                                : 'text-gray-400 hover:text-white hover:bg-white/[0.03]'
                        }`}>
                        <Image className="w-4 h-4" />
                        Thumbnails
                    </button>
                    )}
                    <button onClick={() => setTab('demo')}
                        className={`flex-1 flex items-center justify-center gap-2 py-3 rounded-lg text-sm font-medium transition-all ${
                            tab === 'demo'
                                ? 'bg-violet-600 text-white shadow-lg shadow-violet-600/20'
                                : 'text-gray-400 hover:text-white hover:bg-white/[0.03]'
                        }`}>
                        <Monitor className="w-4 h-4" />
                        Product Demo
                    </button>
                </div>
            </div>

            {tab === 'create' ? <CreatePanel /> : tab === 'clone' ? <ClonePanel /> : tab === 'demo' ? <DemoPanel /> : <ThumbnailPanel />}
        </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════════════
   CREATE PANEL (inside Dashboard)
   ═══════════════════════════════════════════════════════════════════════════ */

function CreatePanel() {
    const { session, plan } = useContext(AuthContext);
    const [prompt, setPrompt] = useState("");
    const [selectedTemplate, setSelectedTemplate] = useState('skeleton');
    const [resolution, setResolution] = useState<'720p' | '1080p'>('720p');
    const [jobId, setJobId] = useState<string | null>(null);
    const [jobStatus, setJobStatus] = useState<any>(null);
    const [loading, setLoading] = useState(false);

    const canUse1080p = plan === 'creator' || plan === 'pro';

    const templates = [
        { id: 'skeleton', title: 'Skeleton AI', desc: '3D skeleton comparisons', icon: '💀' },
        { id: 'objects', title: 'Objects Explain', desc: 'Talking objects', icon: '🔌' },
        { id: 'wouldyourather', title: 'Would You Rather', desc: 'Impossible dilemmas', icon: '🤔' },
        { id: 'scary', title: 'Scary Stories', desc: 'Horror & true crime', icon: '👻' },
        { id: 'history', title: 'Historical Epic', desc: 'Cinematic history', icon: '⚔️' },
        { id: 'argument', title: 'Argument Debate', desc: 'Two sides debate', icon: '🗣️' },
        { id: 'motivation', title: 'Motivation', desc: 'Powerful life advice', icon: '🔥' },
        { id: 'whatif', title: 'What If', desc: 'Hypothetical scenarios', icon: '🌍' },
        { id: 'top5', title: 'Top 5', desc: 'Countdown lists', icon: '🏆' },
        { id: 'random', title: 'Chaos Mode', desc: 'Maximum retention', icon: '🌀' },
    ];

    useEffect(() => {
        if (!jobId) return;
        const interval = setInterval(async () => {
            try {
                const res = await fetch(`${API}/api/status/${jobId}`);
                const data = await res.json();
                setJobStatus(data);
                if (data.status === "complete" || data.status === "error") {
                    clearInterval(interval);
                    setLoading(false);
                }
            } catch { /* retry */ }
        }, 2000);
        return () => clearInterval(interval);
    }, [jobId]);

    const handleGenerate = async () => {
        if (!prompt) return;
        setLoading(true);
        setJobStatus(null);
        setJobId(null);

        const headers: Record<string, string> = { "Content-Type": "application/json" };
        if (session) headers["Authorization"] = `Bearer ${session.access_token}`;

        try {
            const res = await fetch(`${API}/api/generate`, {
                method: "POST",
                headers,
                body: JSON.stringify({
                    template: selectedTemplate,
                    prompt,
                    resolution: canUse1080p ? resolution : '720p',
                }),
            });
            const data = await res.json();
            if (data.job_id) setJobId(data.job_id);
            else { setLoading(false); }
        } catch { setLoading(false); }
    };

    return (
            <div className="max-w-4xl mx-auto px-6 pb-10 space-y-8">
                {/* TEMPLATE PICKER */}
                <div>
                    <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-4">Template</h2>
                    <div className="grid grid-cols-3 md:grid-cols-6 gap-3">
                        {templates.map(t => (
                            <button key={t.id} onClick={() => !loading && setSelectedTemplate(t.id)}
                                className={`p-3 rounded-xl text-center transition-all border-2 ${
                                    selectedTemplate === t.id
                                        ? 'border-violet-500 bg-violet-500/10'
                                        : 'border-white/[0.06] bg-white/[0.02] hover:border-white/20'
                                } ${loading ? 'opacity-50' : ''}`}>
                                <div className="text-2xl mb-1">{t.icon}</div>
                                <div className="text-xs font-medium truncate">{t.title}</div>
                            </button>
                        ))}
                    </div>
                </div>

                {/* RESOLUTION PICKER */}
                <div>
                    <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-4">Resolution</h2>
                    <div className="flex gap-3">
                        <button onClick={() => !loading && setResolution('720p')}
                            className={`flex-1 p-4 rounded-xl text-center transition-all border-2 ${
                                resolution === '720p' ? 'border-violet-500 bg-violet-500/10' : 'border-white/[0.06] bg-white/[0.02] hover:border-white/20'
                            } ${loading ? 'opacity-50' : ''}`}>
                            <div className="text-lg font-bold">720p</div>
                            <div className="text-xs text-gray-500 mt-0.5">Faster generation</div>
                        </button>
                        <button
                            onClick={() => {
                                if (!canUse1080p) return;
                                if (!loading) setResolution('1080p');
                            }}
                            className={`flex-1 p-4 rounded-xl text-center transition-all border-2 relative ${
                                !canUse1080p ? 'opacity-50 cursor-not-allowed border-white/[0.04] bg-white/[0.01]' :
                                resolution === '1080p' ? 'border-violet-500 bg-violet-500/10' : 'border-white/[0.06] bg-white/[0.02] hover:border-white/20'
                            } ${loading ? 'opacity-50' : ''}`}>
                            {!canUse1080p && (
                                <div className="absolute top-2 right-2">
                                    <Lock className="w-3.5 h-3.5 text-gray-600" />
                                </div>
                            )}
                            <div className="text-lg font-bold">1080p</div>
                            <div className="text-xs text-gray-500 mt-0.5">
                                {canUse1080p ? 'Best quality' : 'Creator plan+'}
                            </div>
                        </button>
                    </div>
                </div>

                {/* PROMPT */}
                <div>
                    <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-4">Topic</h2>
                    <div className="relative">
                        <input
                            type="text"
                            value={prompt}
                            onChange={(e) => setPrompt(e.target.value)}
                            disabled={loading}
                            placeholder={selectedTemplate === 'skeleton' ? "e.g., Software Engineer vs Doctor salary comparison"
                                : selectedTemplate === 'objects' ? "e.g., Your microwave explains how it works"
                                : selectedTemplate === 'wouldyourather' ? "e.g., Would you rather have unlimited money or unlimited time?"
                                : selectedTemplate === 'scary' ? "e.g., The disappearance at Cecil Hotel"
                                : selectedTemplate === 'history' ? "e.g., The fall of the Roman Empire"
                                : selectedTemplate === 'argument' ? "e.g., Is college worth it in 2026?"
                                : selectedTemplate === 'motivation' ? "e.g., Why most people quit right before success"
                                : selectedTemplate === 'whatif' ? "e.g., What if Earth stopped spinning for 1 second?"
                                : selectedTemplate === 'top5' ? "e.g., Top 5 most powerful ancient civilizations"
                                : "Enter your video topic..."}
                            className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-5 py-4 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 focus:border-violet-500/50 transition-all disabled:opacity-50 text-lg"
                            onKeyDown={(e) => e.key === 'Enter' && !loading && handleGenerate()}
                        />
                    </div>
                </div>

                {/* GENERATE BUTTON */}
                <button onClick={handleGenerate} disabled={loading || !prompt}
                    className="w-full py-4 bg-violet-600 hover:bg-violet-500 disabled:opacity-40 disabled:hover:bg-violet-600 text-white font-bold rounded-xl text-lg transition-all flex items-center justify-center gap-3 shadow-lg shadow-violet-600/20 active:scale-[0.99]">
                    {loading ? (
                        <><Loader2 className="w-5 h-5 animate-spin" /> Generating your short...</>
                    ) : (
                        <><Wand2 className="w-5 h-5" /> Generate at {canUse1080p ? resolution : '720p'}</>
                    )}
                </button>

                {plan === 'free' && (
                    <p className="text-center text-sm text-gray-600">
                        You're on the free plan (3 videos/month, 720p). Upgrade for more.
                    </p>
                )}

                {/* JOB STATUS */}
                {jobStatus && (
                    <div className={`rounded-2xl border transition-all overflow-hidden ${
                        jobStatus.status === 'complete' ? 'border-emerald-500/30 bg-emerald-500/[0.03]' :
                        jobStatus.status === 'error' ? 'border-red-500/30 bg-red-500/[0.03]' :
                        'border-violet-500/20 bg-violet-500/[0.02]'
                    }`}>
                        {jobStatus.status === 'error' ? (
                            <div className="p-8 text-center">
                                <p className="text-red-400 font-bold text-lg mb-2">Generation Failed</p>
                                <p className="text-gray-500 text-sm">{jobStatus.error}</p>
                                <button onClick={() => { setJobStatus(null); setJobId(null); setLoading(false); }}
                                    className="mt-4 px-6 py-2 bg-white/5 hover:bg-white/10 rounded-lg text-sm transition">
                                    Try Again
                                </button>
                            </div>
                        ) : jobStatus.status === 'complete' ? (
                            <div>
                                <video controls autoPlay
                                    className="w-full max-h-[500px] bg-black"
                                    src={`${API}/api/download/${jobStatus.output_file}`}
                                />
                                <div className="p-6 space-y-4">
                                    <div className="flex items-center justify-between">
                                        <div>
                                            <h3 className="font-bold text-lg text-emerald-400">{jobStatus.metadata?.title}</h3>
                                            <p className="text-gray-500 text-sm">
                                                {jobStatus.resolution && <span className="text-violet-400 mr-2">{jobStatus.resolution}</span>}
                                                {jobStatus.metadata?.tags?.map((t: string) => `#${t}`).join(' ')}
                                            </p>
                                        </div>
                                        <CheckCircle2 className="w-8 h-8 text-emerald-400" />
                                    </div>
                                    <a href={`${API}/api/download/${jobStatus.output_file}`} download
                                        className="flex items-center justify-center gap-2 w-full py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl transition-all">
                                        <Download className="w-5 h-5" />
                                        Download MP4
                                    </a>
                                    <button onClick={() => { setJobStatus(null); setJobId(null); }}
                                        className="w-full py-3 bg-white/5 hover:bg-white/10 text-gray-300 font-medium rounded-xl transition-all">
                                        Create Another
                                    </button>
                                </div>
                            </div>
                        ) : (
                            <div className="p-8 space-y-4">
                                <ProgressBar progress={jobStatus.progress || 0} status={jobStatus.status} />
                                {jobStatus.queue_position > 0 && jobStatus.status === 'queued' && (
                                    <div className="flex items-center justify-center gap-2 text-sm">
                                        <Clock className="w-4 h-4 text-violet-400" />
                                        <p className="text-gray-400">
                                            Position <span className="text-violet-400 font-bold">{jobStatus.queue_position}</span> of {jobStatus.queue_total} in queue
                                        </p>
                                    </div>
                                )}
                                {jobStatus.current_scene && jobStatus.total_scenes && (
                                    <p className="text-center text-sm text-gray-600">
                                        Rendering scene {jobStatus.current_scene} of {jobStatus.total_scenes}
                                        {jobStatus.resolution && <span className="ml-1 text-violet-400">({jobStatus.resolution})</span>}
                                    </p>
                                )}
                                {jobStatus.status === 'error' && (
                                    <p className="text-center text-sm text-red-400">{jobStatus.error || 'Generation failed'}</p>
                                )}
                            </div>
                        )}
                    </div>
                )}
            </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════════════
   CLONE PANEL (inside Dashboard)
   ═══════════════════════════════════════════════════════════════════════════ */

function ClonePanel() {
    const { session, plan } = useContext(AuthContext);
    const [viralFile, setViralFile] = useState<File | null>(null);
    const [topic, setTopic] = useState("");
    const [viralUrl, setViralUrl] = useState("");
    const [showSource, setShowSource] = useState(false);
    const [resolution, setResolution] = useState<'720p' | '1080p'>('720p');
    const [jobId, setJobId] = useState<string | null>(null);
    const [jobStatus, setJobStatus] = useState<any>(null);
    const [loading, setLoading] = useState(false);

    const canClone = plan === 'creator' || plan === 'pro';
    const canUse1080p = plan === 'creator' || plan === 'pro';

    useEffect(() => {
        if (!jobId) return;
        const interval = setInterval(async () => {
            try {
                const res = await fetch(`${API}/api/status/${jobId}`);
                const data = await res.json();
                setJobStatus(data);
                if (data.status === "complete" || data.status === "error") {
                    clearInterval(interval);
                    setLoading(false);
                }
            } catch { /* retry */ }
        }, 2000);
        return () => clearInterval(interval);
    }, [jobId]);

    const handleClone = async () => {
        if (!topic) return;
        setLoading(true);
        setJobStatus(null);
        setJobId(null);

        const fullTopic = viralUrl ? `${topic} [Source: ${viralUrl}]` : topic;
        const formData = new FormData();
        formData.append("topic", fullTopic);
        formData.append("resolution", canUse1080p ? resolution : '720p');
        if (viralFile) formData.append("file", viralFile);

        const headers: Record<string, string> = {};
        if (session) headers["Authorization"] = `Bearer ${session.access_token}`;

        try {
            const res = await fetch(`${API}/api/clone`, { method: "POST", headers, body: formData });
            const data = await res.json();
            if (data.job_id) setJobId(data.job_id);
            else setLoading(false);
        } catch { setLoading(false); }
    };

    return (
            <div className="max-w-3xl mx-auto px-6 pb-10 space-y-8">
                <div className="text-center mb-4">
                    <h1 className="text-2xl font-bold mb-2">Clone a Viral Short</h1>
                    <p className="text-gray-500 text-sm max-w-lg mx-auto">Just tell us the new topic. AI auto-detects the best template, reverse-engineers what makes content go viral, and generates a new short for you.</p>
                </div>

                {!canClone && (
                    <div className="p-4 rounded-xl bg-amber-500/10 border border-amber-500/20 flex items-center gap-3">
                        <Lock className="w-5 h-5 text-amber-400 shrink-0" />
                        <div>
                            <p className="text-amber-300 text-sm font-medium">Clone requires Creator plan or higher</p>
                            <p className="text-gray-500 text-xs mt-0.5">Upgrade to access the viral cloning engine.</p>
                        </div>
                    </div>
                )}

                <div>
                    <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">New Topic</h2>
                    <input
                        type="text"
                        value={topic}
                        onChange={(e) => setTopic(e.target.value)}
                        disabled={loading || !canClone}
                        placeholder="e.g., Why F1 drivers earn more than NFL players"
                        className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-5 py-4 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 focus:border-violet-500/50 transition-all disabled:opacity-50 text-lg"
                        onKeyDown={(e) => e.key === 'Enter' && !loading && canClone && handleClone()}
                    />
                </div>

                <button
                    type="button"
                    onClick={() => setShowSource(!showSource)}
                    className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-300 transition"
                >
                    <Plus className={`w-4 h-4 transition-transform ${showSource ? 'rotate-45' : ''}`} />
                    {showSource ? 'Hide source reference' : 'Add source video (optional)'}
                </button>

                {showSource && (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 animate-in fade-in">
                        <label className={`block border-2 border-dashed rounded-2xl p-6 text-center transition-all ${
                            canClone ? 'border-white/[0.08] hover:border-violet-500/30 hover:bg-violet-500/[0.02] cursor-pointer' : 'border-white/[0.04] opacity-50 cursor-not-allowed'
                        }`}>
                            {viralFile ? (
                                <div className="flex flex-col items-center gap-2">
                                    <FileVideo className="w-7 h-7 text-violet-400" />
                                    <p className="text-violet-300 font-medium text-xs">{viralFile.name}</p>
                                    <p className="text-gray-600 text-[10px]">Click to change</p>
                                </div>
                            ) : (
                                <div className="flex flex-col items-center gap-2">
                                    <UploadCloud className="w-7 h-7 text-gray-600" />
                                    <p className="text-gray-400 font-medium text-xs">Upload MP4</p>
                                </div>
                            )}
                            <input type="file" className="hidden" accept="video/mp4" disabled={!canClone}
                                onChange={e => { if (e.target.files) setViralFile(e.target.files[0]); }} />
                        </label>

                        <div className={`border-2 border-dashed rounded-2xl p-6 flex flex-col justify-center ${
                            canClone ? 'border-white/[0.08]' : 'border-white/[0.04] opacity-50'
                        }`}>
                            <p className="text-gray-500 text-[10px] uppercase tracking-wider mb-2 text-center">Or paste a link</p>
                            <input
                                type="url"
                                value={viralUrl}
                                onChange={e => setViralUrl(e.target.value)}
                                disabled={!canClone || loading}
                                placeholder="https://tiktok.com/... or youtube.com/shorts/..."
                                className="w-full bg-white/[0.03] border border-white/[0.06] rounded-lg px-3 py-2 text-white placeholder:text-gray-600 text-xs focus:outline-none focus:ring-2 focus:ring-violet-500/50 disabled:opacity-50"
                            />
                        </div>
                    </div>
                )}

                <div className="flex gap-3">
                    <button onClick={() => !loading && setResolution('720p')}
                        className={`flex-1 p-3 rounded-xl text-center transition-all border-2 ${
                            resolution === '720p' ? 'border-violet-500 bg-violet-500/10' : 'border-white/[0.06] bg-white/[0.02]'
                        }`}>
                        <div className="text-sm font-bold">720p</div>
                        <div className="text-[10px] text-gray-500">Faster</div>
                    </button>
                    <button onClick={() => canUse1080p && !loading && setResolution('1080p')}
                        className={`flex-1 p-3 rounded-xl text-center transition-all border-2 ${
                            resolution === '1080p' ? 'border-violet-500 bg-violet-500/10' : 'border-white/[0.06] bg-white/[0.02]'
                        } ${!canUse1080p ? 'opacity-40 cursor-not-allowed' : ''}`}>
                        <div className="text-sm font-bold">1080p</div>
                        <div className="text-[10px] text-gray-500">{canUse1080p ? 'Max quality' : 'Creator+'}</div>
                    </button>
                </div>

                <button onClick={handleClone} disabled={loading || !topic || !canClone}
                    className="w-full py-4 bg-violet-600 hover:bg-violet-500 disabled:opacity-40 text-white font-bold rounded-xl text-lg transition-all flex items-center justify-center gap-3 shadow-lg shadow-violet-600/20 active:scale-[0.99]">
                    {loading ? (
                        <><Loader2 className="w-5 h-5 animate-spin" /> Analyzing &amp; Generating...</>
                    ) : (
                        <><Wand2 className="w-5 h-5" /> Clone Viral Formula</>
                    )}
                </button>

                {jobStatus && (
                    <div className={`rounded-2xl border transition-all overflow-hidden ${
                        jobStatus.status === 'complete' ? 'border-emerald-500/30 bg-emerald-500/[0.03]' :
                        jobStatus.status === 'error' ? 'border-red-500/30 bg-red-500/[0.03]' :
                        'border-violet-500/20 bg-violet-500/[0.02]'
                    }`}>
                        {jobStatus.viral_analysis && (
                            <div className="px-6 pt-5 pb-3 border-b border-white/5">
                                <p className="text-xs text-gray-500 uppercase tracking-wider mb-2">Viral Analysis</p>
                                <div className="flex flex-wrap gap-2">
                                    {jobStatus.viral_analysis.hook_type && (
                                        <span className="px-2 py-1 bg-violet-500/10 text-violet-300 text-xs rounded-lg">Hook: {jobStatus.viral_analysis.hook_type}</span>
                                    )}
                                    {jobStatus.template && jobStatus.template !== 'analyzing...' && (
                                        <span className="px-2 py-1 bg-cyan-500/10 text-cyan-300 text-xs rounded-lg">Template: {jobStatus.template}</span>
                                    )}
                                    {jobStatus.viral_analysis.pacing && (
                                        <span className="px-2 py-1 bg-amber-500/10 text-amber-300 text-xs rounded-lg">Pacing: {jobStatus.viral_analysis.pacing}</span>
                                    )}
                                </div>
                                {jobStatus.viral_analysis.what_made_it_viral && (
                                    <p className="text-gray-400 text-xs mt-2 italic">{jobStatus.viral_analysis.what_made_it_viral}</p>
                                )}
                            </div>
                        )}
                        {jobStatus.status === 'error' ? (
                            <div className="p-8 text-center">
                                <p className="text-red-400 font-bold">Generation Failed</p>
                                <p className="text-gray-500 text-sm mt-1">{jobStatus.error}</p>
                                <button onClick={() => { setJobStatus(null); setJobId(null); setLoading(false); }}
                                    className="mt-4 px-6 py-2 bg-white/5 hover:bg-white/10 rounded-lg text-sm transition">
                                    Try Again
                                </button>
                            </div>
                        ) : jobStatus.status === 'complete' ? (
                            <div>
                                <video controls autoPlay className="w-full max-h-[500px] bg-black"
                                    src={`${API}/api/download/${jobStatus.output_file}`} />
                                <div className="p-6 space-y-4">
                                    <div className="flex items-center justify-between">
                                        <h3 className="font-bold text-lg text-emerald-400">{jobStatus.metadata?.title}</h3>
                                        <CheckCircle2 className="w-6 h-6 text-emerald-400 shrink-0" />
                                    </div>
                                    {jobStatus.metadata?.description && (
                                        <p className="text-gray-500 text-xs">{jobStatus.metadata.description}</p>
                                    )}
                                    <a href={`${API}/api/download/${jobStatus.output_file}`} download
                                        className="flex items-center justify-center gap-2 w-full py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl transition-all">
                                        <Download className="w-5 h-5" /> Download MP4
                                    </a>
                                    <button onClick={() => { setJobStatus(null); setJobId(null); }}
                                        className="w-full py-3 bg-white/5 hover:bg-white/10 text-gray-300 font-medium rounded-xl transition-all">
                                        Clone Another
                                    </button>
                                </div>
                            </div>
                        ) : (
                            <div className="p-8 space-y-4">
                                <ProgressBar progress={jobStatus.progress || 0} status={jobStatus.status} />
                                {jobStatus.queue_position > 0 && jobStatus.status === 'queued' && (
                                    <div className="flex items-center justify-center gap-2 text-sm">
                                        <Clock className="w-4 h-4 text-violet-400" />
                                        <p className="text-gray-400">
                                            Position <span className="text-violet-400 font-bold">{jobStatus.queue_position}</span> of {jobStatus.queue_total} in queue
                                        </p>
                                    </div>
                                )}
                                {jobStatus.current_scene && jobStatus.total_scenes && (
                                    <p className="text-center text-sm text-gray-600">
                                        Rendering scene {jobStatus.current_scene} of {jobStatus.total_scenes}
                                        {jobStatus.resolution && <span className="ml-1 text-violet-400">({jobStatus.resolution})</span>}
                                    </p>
                                )}
                                {jobStatus.status === 'error' && (
                                    <p className="text-center text-sm text-red-400">{jobStatus.error || 'Generation failed'}</p>
                                )}
                            </div>
                        )}
                    </div>
                )}
            </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════════════
   THUMBNAIL PANEL (inside Dashboard)
   ═══════════════════════════════════════════════════════════════════════════ */

interface ThumbFile { id: string; name: string; size: number; url: string; created_at?: number; }

interface TrainingStatus {
    lora_available: boolean;
    is_training: boolean;
    total_images: number;
    trained_images: number;
    version: number;
    last_train: number;
}

function DemoPanel() {
    const { session } = useContext(AuthContext);
    const [referenceFile, setReferenceFile] = useState<File | null>(null);
    const [demoFile, setDemoFile] = useState<File | null>(null);
    const [faceFile, setFaceFile] = useState<File | null>(null);
    const [autoFace, setAutoFace] = useState(true);
    const [productName, setProductName] = useState('');
    const [referenceNotes, setReferenceNotes] = useState('');
    const [pipPosition, setPipPosition] = useState('bottom-right');
    const [loading, setLoading] = useState(false);
    const [jobId, setJobId] = useState<string | null>(null);
    const [jobStatus, setJobStatus] = useState<any>(null);

    useEffect(() => {
        if (!jobId) return;
        const interval = setInterval(async () => {
            try {
                const res = await fetch(`${API}/api/status/${jobId}`);
                const data = await res.json();
                setJobStatus(data);
                if (data.status === "complete" || data.status === "error") {
                    clearInterval(interval);
                    setLoading(false);
                }
            } catch { /* retry */ }
        }, 2000);
        return () => clearInterval(interval);
    }, [jobId]);

    const [demoError, setDemoError] = useState<string | null>(null);
    const [uploadProgress, setUploadProgress] = useState<number | null>(null);

    const handleGenerate = async () => {
        if (!demoFile) return;
        setLoading(true);
        setJobStatus(null);
        setJobId(null);
        setDemoError(null);
        setUploadProgress(0);

        const formData = new FormData();
        formData.append('demo_video', demoFile);
        if (referenceFile) formData.append('reference_video', referenceFile);
        if (!autoFace && faceFile) formData.append('face_image', faceFile);
        formData.append('product_name', productName);
        formData.append('reference_notes', referenceNotes);
        formData.append('pip_position', pipPosition);

        const totalSize = (demoFile?.size || 0) + (referenceFile?.size || 0) + (faceFile?.size || 0);
        const totalMB = (totalSize / (1024 * 1024)).toFixed(0);

        try {
            const result = await new Promise<any>((resolve, reject) => {
                const xhr = new XMLHttpRequest();
                xhr.open('POST', `${API}/api/demo`);
                if (session) xhr.setRequestHeader('Authorization', `Bearer ${session.access_token}`);

                xhr.upload.onprogress = (e) => {
                    if (e.lengthComputable) {
                        setUploadProgress(Math.round((e.loaded / e.total) * 100));
                    }
                };

                xhr.onload = () => {
                    setUploadProgress(null);
                    if (xhr.status >= 200 && xhr.status < 300) {
                        try { resolve(JSON.parse(xhr.responseText)); }
                        catch { reject(new Error('Invalid response from server')); }
                    } else {
                        try {
                            const err = JSON.parse(xhr.responseText);
                            reject(new Error(err.detail || `Server error: ${xhr.status}`));
                        } catch { reject(new Error(`Upload failed (${xhr.status})`)); }
                    }
                };

                xhr.onerror = () => {
                    setUploadProgress(null);
                    reject(new Error('Network error. Connection lost during upload.'));
                };

                xhr.ontimeout = () => {
                    setUploadProgress(null);
                    reject(new Error(`Upload timed out (${totalMB}MB is large -- try a shorter clip)`));
                };

                xhr.timeout = 600000;
                xhr.send(formData);
            });

            if (result.job_id) setJobId(result.job_id);
            else {
                setDemoError('No job ID returned -- server may have rejected the request');
                setLoading(false);
            }
        } catch (e: any) {
            setDemoError(e?.message || 'Upload failed');
            setLoading(false);
            setUploadProgress(null);
        }
    };

    const statusLabels: Record<string, string> = {
        queued: 'Starting...',
        compressing: 'Auto-compressing large video files...',
        compressing_demo: 'Compressing demo video to 720p...',
        compressing_reference: 'Compressing reference video to 720p...',
        analyzing_reference: 'Analyzing reference video style...',
        analyzing: 'Analyzing screen recording frame-by-frame...',
        scripting: 'Writing voiceover script with AI...',
        generating_voice: 'Generating voiceover with ElevenLabs...',
        generating_face: 'Generating AI presenter face...',
        compositing: 'Compositing final demo video...',
        complete: 'Done!',
        error: 'Generation failed'
    };

    return (
        <div className="max-w-4xl mx-auto px-6 pb-10 space-y-8">
            <div>
                <h2 className="text-xl font-bold mb-2">AI Product Demo Generator</h2>
                <p className="text-gray-500 text-sm">Upload a screen recording of your software + a face photo. AI writes the script, generates a talking head with lip-sync, and composites a professional product demo video.</p>
            </div>

            <div className="space-y-6">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div>
                        <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">Reference Video <span className="text-gray-600 normal-case">(style guide)</span></h3>
                        <label className={`flex flex-col items-center justify-center p-8 rounded-xl border-2 border-dashed transition-all cursor-pointer ${
                            referenceFile ? 'border-violet-500 bg-violet-500/5' : 'border-white/[0.08] hover:border-violet-500/30 bg-white/[0.02]'
                        }`}>
                            <input type="file" accept="video/*" className="hidden" onChange={(e) => setReferenceFile(e.target.files?.[0] || null)} disabled={loading} />
                            {referenceFile ? (
                                <div className="text-center">
                                    <Eye className="w-8 h-8 text-violet-400 mx-auto mb-2" />
                                    <p className="text-sm text-violet-300 font-medium truncate max-w-[180px]">{referenceFile.name}</p>
                                    <p className="text-xs text-gray-500 mt-1">{(referenceFile.size / 1024 / 1024).toFixed(1)} MB</p>
                                </div>
                            ) : (
                                <div className="text-center">
                                    <Eye className="w-8 h-8 text-gray-500 mx-auto mb-2" />
                                    <p className="text-sm text-gray-400">Upload reference video</p>
                                    <p className="text-xs text-gray-600 mt-1">The style you want to match</p>
                                </div>
                            )}
                        </label>
                    </div>

                    <div>
                        <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">Your Demo Video <span className="text-red-400">*</span></h3>
                        <label className={`flex flex-col items-center justify-center p-8 rounded-xl border-2 border-dashed transition-all cursor-pointer ${
                            demoFile ? 'border-violet-500 bg-violet-500/5' : 'border-white/[0.08] hover:border-violet-500/30 bg-white/[0.02]'
                        }`}>
                            <input type="file" accept="video/*" className="hidden" onChange={(e) => setDemoFile(e.target.files?.[0] || null)} disabled={loading} />
                            {demoFile ? (
                                <div className="text-center">
                                    <Film className="w-8 h-8 text-violet-400 mx-auto mb-2" />
                                    <p className="text-sm text-violet-300 font-medium truncate max-w-[180px]">{demoFile.name}</p>
                                    <p className="text-xs text-gray-500 mt-1">{(demoFile.size / 1024 / 1024).toFixed(1)} MB</p>
                                </div>
                            ) : (
                                <div className="text-center">
                                    <UploadCloud className="w-8 h-8 text-gray-500 mx-auto mb-2" />
                                    <p className="text-sm text-gray-400">Upload raw screen recording</p>
                                    <p className="text-xs text-gray-600 mt-1">The software demo to edit</p>
                                </div>
                            )}
                        </label>
                    </div>
                </div>

                <div>
                    <div className="flex items-center justify-between mb-3">
                        <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wider">AI Presenter</h3>
                        <button onClick={() => !loading && setAutoFace(!autoFace)}
                            className={`text-xs px-3 py-1 rounded-full transition-all ${
                                autoFace ? 'bg-violet-500/20 text-violet-300 border border-violet-500/30' : 'bg-white/[0.03] text-gray-500 border border-white/[0.08]'
                            }`}>
                            {autoFace ? 'Auto-Generate Face' : 'Upload Custom Face'}
                        </button>
                    </div>
                    {autoFace ? (
                        <div className="flex flex-col items-center justify-center p-8 rounded-xl border-2 border-dashed border-violet-500/30 bg-violet-500/5">
                            <Sparkles className="w-8 h-8 text-violet-400 mx-auto mb-2" />
                            <p className="text-sm text-violet-300 font-medium">AI-Generated Male Face</p>
                            <p className="text-xs text-gray-500 mt-1">A unique, realistic face will be auto-generated</p>
                        </div>
                    ) : (
                        <label className={`flex flex-col items-center justify-center p-8 rounded-xl border-2 border-dashed transition-all cursor-pointer ${
                            faceFile ? 'border-violet-500 bg-violet-500/5' : 'border-white/[0.08] hover:border-violet-500/30 bg-white/[0.02]'
                        }`}>
                            <input type="file" accept="image/*" className="hidden" onChange={(e) => setFaceFile(e.target.files?.[0] || null)} disabled={loading} />
                            {faceFile ? (
                                <div className="text-center">
                                    <User className="w-8 h-8 text-violet-400 mx-auto mb-2" />
                                    <p className="text-sm text-violet-300 font-medium">{faceFile.name}</p>
                                    <p className="text-xs text-gray-500 mt-1">{(faceFile.size / 1024 / 1024).toFixed(1)} MB</p>
                                </div>
                            ) : (
                                <div className="text-center">
                                    <User className="w-8 h-8 text-gray-500 mx-auto mb-2" />
                                    <p className="text-sm text-gray-400">Upload face photo</p>
                                    <p className="text-xs text-gray-600 mt-1">Clear, front-facing portrait</p>
                                </div>
                            )}
                        </label>
                    )}
                </div>
            </div>

            <div className="space-y-4">
                <div>
                    <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-2">Product Name</h3>
                    <input type="text" value={productName} onChange={(e) => setProductName(e.target.value)}
                        disabled={loading} placeholder="e.g., BrayneAI, Notion, Stripe Dashboard"
                        className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-4 py-3 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all disabled:opacity-50" />
                </div>

                <div>
                    <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-2">Style Notes (optional)</h3>
                    <textarea value={referenceNotes} onChange={(e) => setReferenceNotes(e.target.value)}
                        disabled={loading} placeholder="Describe the style you want: e.g., 'Energetic and fast-paced like a YC demo day pitch' or 'Calm and professional like an Apple keynote'"
                        rows={2}
                        className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-4 py-3 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all disabled:opacity-50 resize-none" />
                </div>

                <div>
                    <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-2">Face Position</h3>
                    <div className="grid grid-cols-4 gap-2">
                        {[
                            { id: 'bottom-right', label: 'Bottom Right' },
                            { id: 'bottom-left', label: 'Bottom Left' },
                            { id: 'top-right', label: 'Top Right' },
                            { id: 'top-left', label: 'Top Left' },
                        ].map(pos => (
                            <button key={pos.id} onClick={() => !loading && setPipPosition(pos.id)}
                                className={`p-2 rounded-lg text-xs font-medium transition-all border ${
                                    pipPosition === pos.id ? 'border-violet-500 bg-violet-500/10 text-violet-300' : 'border-white/[0.06] text-gray-500 hover:border-white/20'
                                } ${loading ? 'opacity-50' : ''}`}>
                                {pos.label}
                            </button>
                        ))}
                    </div>
                </div>
            </div>

            <button onClick={handleGenerate}
                disabled={loading || !demoFile || (!autoFace && !faceFile)}
                className={`w-full py-4 rounded-xl font-semibold text-lg transition-all flex items-center justify-center gap-3 ${
                    loading || !demoFile || (!autoFace && !faceFile)
                        ? 'bg-gray-700/50 text-gray-500 cursor-not-allowed'
                        : 'bg-gradient-to-r from-violet-600 to-purple-600 text-white hover:shadow-lg hover:shadow-violet-600/20 hover:-translate-y-0.5'
                }`}>
                {loading ? (
                    <><Loader2 className="w-5 h-5 animate-spin" /> Generating Demo...</>
                ) : (
                    <><Monitor className="w-5 h-5" /> Generate Product Demo</>
                )}
            </button>

            {demoError && !jobStatus && (
                <div className="bg-red-500/5 border border-red-500/20 rounded-xl px-5 py-4">
                    <p className="text-red-400 text-sm font-medium">{demoError}</p>
                </div>
            )}

            {uploadProgress !== null && !jobStatus && (
                <div className="bg-white/[0.02] border border-white/[0.06] rounded-2xl overflow-hidden">
                    <div className="px-6 pt-5 pb-4">
                        <div className="flex items-center justify-between mb-3">
                            <p className="text-sm font-medium flex items-center gap-2">
                                <Loader2 className="w-4 h-4 animate-spin text-violet-400" />
                                Uploading files to server...
                            </p>
                            <span className="text-xs text-gray-500">{uploadProgress}%</span>
                        </div>
                        <div className="w-full bg-white/[0.05] rounded-full h-2">
                            <div className="bg-gradient-to-r from-blue-500 to-violet-500 h-2 rounded-full transition-all duration-300"
                                style={{ width: `${uploadProgress}%` }} />
                        </div>
                        <p className="text-xs text-gray-600 mt-2">
                            {uploadProgress < 100
                                ? `Uploading ${((demoFile?.size || 0) / (1024*1024)).toFixed(0)}MB${referenceFile ? ` + ${((referenceFile.size) / (1024*1024)).toFixed(0)}MB` : ''} to server...`
                                : 'Upload complete, server is processing...'}
                        </p>
                    </div>
                </div>
            )}

            {jobStatus && (
                <div className="bg-white/[0.02] border border-white/[0.06] rounded-2xl overflow-hidden">
                    <div className="px-6 pt-5 pb-4">
                        <div className="flex items-center justify-between mb-3">
                            <p className="text-sm font-medium">{statusLabels[jobStatus.status] || jobStatus.status}</p>
                            <span className="text-xs text-gray-500">{jobStatus.progress || 0}%</span>
                        </div>
                        <div className="w-full bg-white/[0.05] rounded-full h-2">
                            <div className="bg-gradient-to-r from-violet-500 to-purple-500 h-2 rounded-full transition-all duration-500"
                                style={{ width: `${jobStatus.progress || 0}%` }} />
                        </div>
                    </div>

                    {jobStatus.compress_info && (jobStatus.status === 'compressing' || jobStatus.status === 'compressing_demo' || jobStatus.status === 'compressing_reference') && (
                        <div className="px-6 py-3 border-t border-white/[0.05]">
                            <div className="flex items-center gap-2 text-xs text-amber-300">
                                <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                <span>Compressing {jobStatus.compress_info.label} video: {jobStatus.compress_info.original_size_mb}MB → 720p</span>
                            </div>
                        </div>
                    )}

                    {jobStatus.compress_info && jobStatus.compress_info.compressed_size_mb && jobStatus.status !== 'compressing' && jobStatus.status !== 'compressing_demo' && jobStatus.status !== 'compressing_reference' && (
                        <div className="px-6 py-2 border-t border-white/[0.05]">
                            <p className="text-xs text-emerald-400">Compressed: {jobStatus.compress_info.original_size_mb}MB → {jobStatus.compress_info.compressed_size_mb}MB</p>
                        </div>
                    )}

                    {jobStatus.script && (
                        <div className="px-6 py-3 border-t border-white/[0.05]">
                            <p className="text-xs text-gray-500 uppercase tracking-wider mb-1">Script Preview</p>
                            <p className="text-xs text-gray-400 line-clamp-3">
                                {jobStatus.script.segments?.slice(0, 3).map((s: any) => s.text || s.narration).join(' ')}
                            </p>
                        </div>
                    )}

                    {jobStatus.status === 'complete' && jobStatus.output_url && (
                        <div className="px-6 py-4 border-t border-white/[0.05] space-y-3">
                            <video controls className="w-full rounded-xl" src={`${API}${jobStatus.output_url}`} />
                            <a href={`${API}${jobStatus.output_url}`} download
                                className="flex items-center justify-center gap-2 w-full py-3 bg-violet-600 hover:bg-violet-500 text-white rounded-xl font-medium transition-all">
                                <Download className="w-4 h-4" /> Download Demo Video
                            </a>
                        </div>
                    )}

                    {jobStatus.status === 'error' && (
                        <div className="px-6 py-4 border-t border-red-500/20">
                            <p className="text-red-400 text-sm">{jobStatus.error || 'Generation failed. Please try again.'}</p>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}


function ThumbnailPanel() {
    const { session } = useContext(AuthContext);
    const [subTab, setSubTab] = useState<'generate' | 'library'>('generate');
    const [mode, setMode] = useState<'describe' | 'style_transfer' | 'screenshot_analysis'>('describe');
    const [description, setDescription] = useState('');
    const [styleDesc, setStyleDesc] = useState('');
    const [selectedStyleRef, setSelectedStyleRef] = useState<string>('');
    const [screenshotDesc, setScreenshotDesc] = useState('');
    const [library, setLibrary] = useState<ThumbFile[]>([]);
    const [uploading, setUploading] = useState(false);
    const [jobId, setJobId] = useState<string | null>(null);
    const [jobStatus, setJobStatus] = useState<any>(null);
    const [loading, setLoading] = useState(false);
    const [previewUrl, setPreviewUrl] = useState<string | null>(null);
    const [trainingStatus, setTrainingStatus] = useState<TrainingStatus | null>(null);

    const fetchLibrary = useCallback(async () => {
        try {
            const res = await fetch(`${API}/api/thumbnails/library`);
            if (res.ok) { const data = await res.json(); setLibrary(data.files || []); }
        } catch { /* ignore */ }
    }, []);

    const fetchTrainingStatus = useCallback(async () => {
        try {
            const res = await fetch(`${API}/api/thumbnails/training-status`);
            if (res.ok) { const data = await res.json(); setTrainingStatus(data); }
        } catch { /* ignore */ }
    }, []);

    useEffect(() => { fetchLibrary(); fetchTrainingStatus(); }, [fetchLibrary, fetchTrainingStatus]);

    useEffect(() => {
        const interval = setInterval(fetchTrainingStatus, 15000);
        return () => clearInterval(interval);
    }, [fetchTrainingStatus]);

    useEffect(() => {
        if (!jobId) return;
        const interval = setInterval(async () => {
            try {
                const res = await fetch(`${API}/api/status/${jobId}`);
                const data = await res.json();
                setJobStatus(data);
                if (data.status === 'complete' || data.status === 'error') {
                    clearInterval(interval);
                    setLoading(false);
                }
            } catch { /* retry */ }
        }, 2000);
        return () => clearInterval(interval);
    }, [jobId]);

    const handleUpload = async (files: FileList) => {
        setUploading(true);
        const formData = new FormData();
        Array.from(files).forEach(f => formData.append('files', f));
        try {
            const res = await fetch(`${API}/api/thumbnails/upload`, { method: 'POST', body: formData });
            if (res.ok) await fetchLibrary();
        } catch { /* ignore */ }
        setUploading(false);
    };

    const handleDelete = async (id: string) => {
        await fetch(`${API}/api/thumbnails/library/${id}`, { method: 'DELETE' });
        setLibrary(prev => prev.filter(f => f.id !== id));
        if (selectedStyleRef === id) setSelectedStyleRef('');
    };

    const handleGenerate = async () => {
        if (!description && mode === 'describe') return;
        setLoading(true);
        setJobStatus(null);
        setJobId(null);

        const headers: Record<string, string> = { 'Content-Type': 'application/json' };
        if (session) headers['Authorization'] = `Bearer ${session.access_token}`;

        try {
            const body: any = { mode, description };
            if (mode === 'style_transfer') {
                body.style_reference_id = selectedStyleRef;
                body.screenshot_description = styleDesc;
            } else if (mode === 'screenshot_analysis') {
                body.screenshot_description = screenshotDesc;
            }

            const res = await fetch(`${API}/api/thumbnails/generate`, {
                method: 'POST', headers, body: JSON.stringify(body),
            });
            const data = await res.json();
            if (data.job_id) setJobId(data.job_id);
            else setLoading(false);
        } catch { setLoading(false); }
    };

    const modes = [
        { id: 'describe' as const, icon: <Sparkles className="w-4 h-4" />, title: 'Describe', desc: 'Describe your video and get a pro thumbnail' },
        { id: 'style_transfer' as const, icon: <Palette className="w-4 h-4" />, title: 'Style Transfer', desc: 'Copy a thumbnail style you like' },
        { id: 'screenshot_analysis' as const, icon: <Camera className="w-4 h-4" />, title: 'Channel Analysis', desc: 'AI learns from what works for you' },
    ];

    return (
        <div className="max-w-4xl mx-auto px-6 pb-10 space-y-8">
            <div className="text-center mb-2">
                <h1 className="text-2xl font-bold mb-2">AI Thumbnail Engine</h1>
                <p className="text-gray-500 text-sm max-w-xl mx-auto">Generate click-worthy thumbnails that outperform human designers. Upload your proven winners to train the AI on your style.</p>
            </div>

            <div className="flex gap-1 p-1 bg-white/[0.03] border border-white/[0.06] rounded-xl">
                <button onClick={() => setSubTab('generate')}
                    className={`flex-1 py-2.5 rounded-lg text-sm font-medium transition-all ${
                        subTab === 'generate' ? 'bg-violet-600 text-white shadow-lg shadow-violet-600/20' : 'text-gray-400 hover:text-white'
                    }`}>
                    <Sparkles className="w-4 h-4 inline mr-1.5" />Generate
                </button>
                <button onClick={() => setSubTab('library')}
                    className={`flex-1 py-2.5 rounded-lg text-sm font-medium transition-all ${
                        subTab === 'library' ? 'bg-violet-600 text-white shadow-lg shadow-violet-600/20' : 'text-gray-400 hover:text-white'
                    }`}>
                    <Image className="w-4 h-4 inline mr-1.5" />Library ({library.length})
                </button>
            </div>

            {subTab === 'library' ? (
                <div className="space-y-6">
                    {trainingStatus && (
                        <div className={`p-4 rounded-xl border flex items-center gap-3 ${
                            trainingStatus.is_training
                                ? 'bg-amber-500/5 border-amber-500/20'
                                : trainingStatus.lora_available
                                    ? 'bg-emerald-500/5 border-emerald-500/20'
                                    : 'bg-white/[0.02] border-white/[0.06]'
                        }`}>
                            <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${
                                trainingStatus.is_training ? 'bg-amber-500/10' : trainingStatus.lora_available ? 'bg-emerald-500/10' : 'bg-white/[0.05]'
                            }`}>
                                {trainingStatus.is_training
                                    ? <Loader2 className="w-5 h-5 text-amber-400 animate-spin" />
                                    : trainingStatus.lora_available
                                        ? <Sparkles className="w-5 h-5 text-emerald-400" />
                                        : <Eye className="w-5 h-5 text-gray-500" />
                                }
                            </div>
                            <div className="flex-1">
                                <p className={`text-sm font-medium ${
                                    trainingStatus.is_training ? 'text-amber-300' : trainingStatus.lora_available ? 'text-emerald-300' : 'text-gray-400'
                                }`}>
                                    {trainingStatus.is_training
                                        ? 'AI is training on your thumbnails...'
                                        : trainingStatus.lora_available
                                            ? `Thumbnail AI trained (v${trainingStatus.version}, ${trainingStatus.trained_images} images)`
                                            : `Upload ${Math.max(0, 5 - trainingStatus.total_images)} more thumbnails to start training`
                                    }
                                </p>
                                <p className="text-gray-600 text-xs mt-0.5">
                                    {trainingStatus.total_images} images in training set
                                    {trainingStatus.lora_available && trainingStatus.total_images > trainingStatus.trained_images &&
                                        ` (${trainingStatus.total_images - trainingStatus.trained_images} new, will retrain soon)`
                                    }
                                </p>
                            </div>
                        </div>
                    )}

                    <label className="block border-2 border-dashed border-white/[0.08] hover:border-violet-500/30 hover:bg-violet-500/[0.02] rounded-2xl p-8 text-center cursor-pointer transition-all">
                        <UploadCloud className={`w-10 h-10 mx-auto mb-3 ${uploading ? 'text-violet-400 animate-pulse' : 'text-gray-600'}`} />
                        <p className="text-gray-300 font-medium">{uploading ? 'Uploading...' : 'Upload Thumbnails'}</p>
                        <p className="text-gray-600 text-xs mt-1">PNG, JPG, WebP -- drag and drop or click. Upload as many as you want.</p>
                        <input type="file" className="hidden" accept="image/png,image/jpeg,image/webp" multiple
                            onChange={e => { if (e.target.files?.length) handleUpload(e.target.files); }} />
                    </label>

                    {library.length === 0 ? (
                        <div className="text-center py-12">
                            <Image className="w-12 h-12 mx-auto text-gray-700 mb-3" />
                            <p className="text-gray-500">No thumbnails yet</p>
                            <p className="text-gray-600 text-xs mt-1">Upload your best-performing thumbnails to train the AI on your style</p>
                        </div>
                    ) : (
                        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
                            {library.map(f => (
                                <div key={f.id} className="group relative rounded-xl overflow-hidden border border-white/[0.06] bg-white/[0.02] hover:border-violet-500/30 transition-all">
                                    <img src={`${API}${f.url}`} alt={f.name}
                                        className="w-full aspect-video object-cover cursor-pointer"
                                        onClick={() => setPreviewUrl(`${API}${f.url}`)} />
                                    <div className="absolute inset-0 bg-black/0 group-hover:bg-black/40 transition-all flex items-center justify-center opacity-0 group-hover:opacity-100">
                                        <button onClick={() => setPreviewUrl(`${API}${f.url}`)}
                                            className="p-2 bg-white/10 rounded-lg mr-2 hover:bg-white/20 transition">
                                            <Eye className="w-4 h-4" />
                                        </button>
                                        <button onClick={() => handleDelete(f.id)}
                                            className="p-2 bg-red-500/20 rounded-lg hover:bg-red-500/40 transition">
                                            <Trash2 className="w-4 h-4 text-red-400" />
                                        </button>
                                    </div>
                                    <div className="p-2">
                                        <p className="text-[10px] text-gray-500 truncate">{f.name}</p>
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            ) : (
                <div className="space-y-6">
                    {trainingStatus?.lora_available && (
                        <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-emerald-500/5 border border-emerald-500/20 text-xs">
                            <Sparkles className="w-3.5 h-3.5 text-emerald-400" />
                            <span className="text-emerald-300 font-medium">AI trained on {trainingStatus.trained_images} of your thumbnails</span>
                            <span className="text-gray-600">v{trainingStatus.version}</span>
                        </div>
                    )}

                    <div className="grid grid-cols-3 gap-3">
                        {modes.map(m => (
                            <button key={m.id} onClick={() => setMode(m.id)}
                                className={`p-4 rounded-xl text-left transition-all border-2 ${
                                    mode === m.id
                                        ? 'border-violet-500 bg-violet-500/10'
                                        : 'border-white/[0.06] bg-white/[0.02] hover:border-white/20'
                                }`}>
                                <div className={`mb-2 ${mode === m.id ? 'text-violet-400' : 'text-gray-500'}`}>{m.icon}</div>
                                <div className="text-sm font-bold">{m.title}</div>
                                <div className="text-[10px] text-gray-500 mt-0.5">{m.desc}</div>
                            </button>
                        ))}
                    </div>

                    {mode === 'describe' && (
                        <div>
                            <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">Describe Your Video</h2>
                            <textarea
                                value={description}
                                onChange={e => setDescription(e.target.value)}
                                disabled={loading}
                                placeholder={"Describe your video in detail. The AI will design a click-optimized thumbnail.\ne.g., \"A comparison video about why software engineers earn more than doctors, shocking statistics, aimed at 18-30 year olds\""}
                                rows={4}
                                className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-5 py-4 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all disabled:opacity-50 resize-none"
                            />
                        </div>
                    )}

                    {mode === 'style_transfer' && (
                        <div className="space-y-4">
                            <div>
                                <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">
                                    Select Style Reference
                                    {library.length === 0 && <span className="text-amber-400 ml-2">(upload thumbnails to library first)</span>}
                                </h2>
                                {library.length > 0 ? (
                                    <div className="grid grid-cols-4 md:grid-cols-6 gap-2">
                                        {library.map(f => (
                                            <button key={f.id} onClick={() => setSelectedStyleRef(f.id)}
                                                className={`rounded-lg overflow-hidden border-2 transition-all ${
                                                    selectedStyleRef === f.id ? 'border-violet-500 ring-2 ring-violet-500/30' : 'border-white/[0.06] hover:border-white/20'
                                                }`}>
                                                <img src={`${API}${f.url}`} alt={f.name} className="w-full aspect-video object-cover" />
                                            </button>
                                        ))}
                                    </div>
                                ) : (
                                    <div className="p-4 rounded-xl bg-white/[0.02] border border-white/[0.06] text-center">
                                        <p className="text-gray-500 text-sm">Go to Library tab and upload thumbnail styles you like</p>
                                    </div>
                                )}
                            </div>
                            <div>
                                <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">Describe Your New Thumbnail</h2>
                                <textarea
                                    value={styleDesc}
                                    onChange={e => setStyleDesc(e.target.value)}
                                    disabled={loading}
                                    placeholder="Describe what your new thumbnail should show, using the selected style as a reference..."
                                    rows={3}
                                    className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-5 py-4 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all disabled:opacity-50 resize-none"
                                />
                            </div>
                            <input type="hidden" value={description} />
                            {!description && styleDesc && (
                                <p className="text-amber-400 text-xs">Also fill in a brief overall description above for best results, or this field will be used.</p>
                            )}
                        </div>
                    )}

                    {mode === 'screenshot_analysis' && (
                        <div className="space-y-4">
                            <div>
                                <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">Describe Your Channel's Thumbnails</h2>
                                <textarea
                                    value={screenshotDesc}
                                    onChange={e => setScreenshotDesc(e.target.value)}
                                    disabled={loading}
                                    placeholder={"Paste a screenshot description of your YouTube channel, or describe what your thumbnails typically look like:\ne.g., \"My thumbnails use bold red/yellow text, shocked face reactions, dark backgrounds, and always show a comparison split screen. My best performing ones have numbers in them.\""}
                                    rows={4}
                                    className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-5 py-4 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all disabled:opacity-50 resize-none"
                                />
                            </div>
                            <div>
                                <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">New Video to Make Thumbnail For</h2>
                                <input
                                    type="text"
                                    value={description}
                                    onChange={e => setDescription(e.target.value)}
                                    disabled={loading}
                                    placeholder="e.g., Top 5 richest YouTubers of 2026"
                                    className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-5 py-4 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all disabled:opacity-50"
                                />
                            </div>
                        </div>
                    )}

                    <button onClick={handleGenerate}
                        disabled={loading || (!description && mode !== 'style_transfer') || (mode === 'style_transfer' && !styleDesc && !description)}
                        className="w-full py-4 bg-violet-600 hover:bg-violet-500 disabled:opacity-40 text-white font-bold rounded-xl text-lg transition-all flex items-center justify-center gap-3 shadow-lg shadow-violet-600/20 active:scale-[0.99]">
                        {loading ? (
                            <><Loader2 className="w-5 h-5 animate-spin" /> Generating Thumbnail...</>
                        ) : (
                            <><Sparkles className="w-5 h-5" /> Generate Thumbnail</>
                        )}
                    </button>

                    {jobStatus && (
                        <div className={`rounded-2xl border transition-all overflow-hidden ${
                            jobStatus.status === 'complete' ? 'border-emerald-500/30 bg-emerald-500/[0.03]' :
                            jobStatus.status === 'error' ? 'border-red-500/30 bg-red-500/[0.03]' :
                            'border-violet-500/20 bg-violet-500/[0.02]'
                        }`}>
                            {jobStatus.ai_analysis && (
                                <div className="px-6 pt-5 pb-3 border-b border-white/5">
                                    <p className="text-xs text-gray-500 uppercase tracking-wider mb-2">AI Design Strategy</p>
                                    {jobStatus.ai_analysis.style_notes && (
                                        <p className="text-gray-400 text-xs italic">{jobStatus.ai_analysis.style_notes}</p>
                                    )}
                                    {jobStatus.ai_analysis.patterns?.length > 0 && (
                                        <div className="flex flex-wrap gap-1.5 mt-2">
                                            {jobStatus.ai_analysis.patterns.map((p: string, i: number) => (
                                                <span key={i} className="px-2 py-0.5 bg-violet-500/10 text-violet-300 text-[10px] rounded-lg">{p}</span>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            )}
                            {jobStatus.status === 'error' ? (
                                <div className="p-8 text-center">
                                    <p className="text-red-400 font-bold">Generation Failed</p>
                                    <p className="text-gray-500 text-sm mt-1">{jobStatus.error}</p>
                                    <button onClick={() => { setJobStatus(null); setJobId(null); setLoading(false); }}
                                        className="mt-4 px-6 py-2 bg-white/5 hover:bg-white/10 rounded-lg text-sm transition">
                                        Try Again
                                    </button>
                                </div>
                            ) : jobStatus.status === 'complete' ? (
                                <div>
                                    <img src={`${API}${jobStatus.output_url}`} alt="Generated Thumbnail"
                                        className="w-full cursor-pointer"
                                        onClick={() => setPreviewUrl(`${API}${jobStatus.output_url}`)} />
                                    <div className="p-6 space-y-3">
                                        <div className="flex items-center gap-2">
                                            <CheckCircle2 className="w-5 h-5 text-emerald-400" />
                                            <span className="text-emerald-400 font-bold">Thumbnail Ready</span>
                                        </div>
                                        <div className="flex gap-3">
                                            <a href={`${API}${jobStatus.output_url}`} download
                                                className="flex-1 flex items-center justify-center gap-2 py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl transition-all">
                                                <Download className="w-5 h-5" /> Download PNG
                                            </a>
                                            <button onClick={() => { setJobStatus(null); setJobId(null); }}
                                                className="flex-1 py-3 bg-white/5 hover:bg-white/10 text-gray-300 font-medium rounded-xl transition-all">
                                                Generate Another
                                            </button>
                                        </div>
                                    </div>
                                </div>
                            ) : (
                                <div className="p-8">
                                    <ThumbProgressBar progress={jobStatus.progress || 0} status={jobStatus.status} />
                                </div>
                            )}
                        </div>
                    )}
                </div>
            )}

            {previewUrl && (
                <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-sm flex items-center justify-center p-6"
                    onClick={() => setPreviewUrl(null)}>
                    <div className="relative max-w-4xl w-full">
                        <button onClick={() => setPreviewUrl(null)}
                            className="absolute -top-10 right-0 p-2 text-gray-400 hover:text-white transition">
                            <X className="w-6 h-6" />
                        </button>
                        <img src={previewUrl} alt="Preview" className="w-full rounded-xl" />
                    </div>
                </div>
            )}
        </div>
    );
}

function ThumbProgressBar({ progress, status }: { progress: number; status: string }) {
    const labels: Record<string, string> = {
        queued: 'In queue...',
        analyzing: 'AI designing your thumbnail...',
        generating: 'Rendering on GPU...',
        complete: 'Done!',
        error: 'Error occurred',
    };
    return (
        <div>
            <div className="flex justify-between text-sm mb-3">
                <span className="text-violet-300 font-medium">{labels[status] || status}</span>
                <span className="text-gray-600 tabular-nums">{progress}%</span>
            </div>
            <div className="w-full bg-white/[0.05] rounded-full h-2.5 overflow-hidden">
                <div className="h-full rounded-full bg-gradient-to-r from-violet-600 to-purple-500 transition-all duration-700 ease-out"
                    style={{ width: `${progress}%` }} />
            </div>
        </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════════════
   SHARED COMPONENTS
   ═══════════════════════════════════════════════════════════════════════════ */

function ProgressBar({ progress, status }: { progress: number; status: string }) {
    const labels: Record<string, string> = {
        queued: "In queue...",
        analyzing: "Reverse-engineering viral formula...",
        generating_script: "AI is writing the script...",
        generating_images: "Generating scenes with Grok Imagine...",
        animating_scenes: "Animating with Kling 2.1 AI Video...",
        generating_voice: "Creating AI voiceover...",
        compositing: "Compositing final video...",
        complete: "Done!",
        error: "Error occurred",
    };

    return (
        <div>
            <div className="flex justify-between text-sm mb-3">
                <span className="text-violet-300 font-medium">{labels[status] || status}</span>
                <span className="text-gray-600 tabular-nums">{progress}%</span>
            </div>
            <div className="w-full bg-white/[0.05] rounded-full h-2.5 overflow-hidden">
                <div className="h-full rounded-full bg-gradient-to-r from-violet-600 to-purple-500 transition-all duration-700 ease-out"
                    style={{ width: `${progress}%` }} />
            </div>
        </div>
    );
}
