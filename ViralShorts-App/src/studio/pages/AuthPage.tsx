import { useContext, useEffect, useState } from 'react';
import { Lock, Mail } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, Logo } from '../shared';

export default function AuthPage({ onNavigate }: { onNavigate: PageNav }) {
    const { signIn, signInWithGoogle, signUp, session, loading, supabase } = useContext(AuthContext);
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const [mode, setMode] = useState<'signin' | 'signup'>('signin');
    const [error, setError] = useState('');
    const [info, setInfo] = useState('');
    const [submitLoading, setSubmitLoading] = useState(false);
    const [googleLoading, setGoogleLoading] = useState(false);

    useEffect(() => {
        if (session) onNavigate('dashboard');
    }, [onNavigate, session]);

    if (session) return null;
    const authBooting = loading || !supabase;

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        if (authBooting) {
            setError('');
            setInfo('Connecting auth. Try again in a second.');
            return;
        }
        setError('');
        setInfo('');
        setSubmitLoading(true);
        if (mode === 'signup') {
            const err = await signUp(email, password);
            setSubmitLoading(false);
            if (err) { setError(err); return; }
            setInfo('Check your email to confirm your account, then sign in.');
        } else {
            const err = await signIn(email, password);
            setSubmitLoading(false);
            if (err) { setError(err); return; }
        }
    };

    const handleGoogleSignIn = async () => {
        setError('');
        setInfo('');
        setGoogleLoading(true);
        const err = await signInWithGoogle();
        setGoogleLoading(false);
        if (err) setError(err);
    };

    return (
        <>
            <NavBar onNavigate={onNavigate} />
            <div className="pt-32 max-w-md mx-auto px-6">
                <div className="text-center mb-8">
                    <Logo size={48} />
                    <h1 className="text-3xl font-bold mt-4">{mode === 'signin' ? 'Welcome Back' : 'Create Account'}</h1>
                    <p className="text-gray-500 text-sm mt-2">
                        {mode === 'signin'
                            ? 'Email + password login is fully supported. Google stays optional when its OAuth path is healthy.'
                            : (
                                <>
                                    Create an email account below, then verify it to unlock Studio even if Google sign-in is down.
                                    <br />1) Create an account below.
                                    <br />2) Verify your email.
                                    <br />3) Open your dashboard and start creating.
                                </>
                            )}
                    </p>
                </div>

                <form onSubmit={handleSubmit} className="space-y-4">
                    <div className="relative">
                        <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" />
                        <input
                            type="email"
                            value={email}
                            onChange={e => setEmail(e.target.value)}
                            placeholder="Email"
                            required
                            className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl pl-10 pr-4 py-3 text-sm text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all"
                        />
                    </div>
                    <div className="relative">
                        <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" />
                        <input
                            type="password"
                            value={password}
                            onChange={e => setPassword(e.target.value)}
                            placeholder="Password"
                            required
                            minLength={6}
                            className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl pl-10 pr-4 py-3 text-sm text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all"
                        />
                    </div>

                    {error && <p className="text-red-400 text-sm">{error}</p>}
                    {info && <p className="text-emerald-400 text-sm">{info}</p>}
                    {authBooting && !error && !info && <p className="text-gray-400 text-sm">Connecting auth...</p>}

                    <button
                        type="submit"
                        disabled={submitLoading || authBooting}
                        className="w-full py-3 bg-violet-600 hover:bg-violet-500 disabled:opacity-60 text-white font-bold rounded-xl transition-all shadow-lg shadow-violet-600/25"
                    >
                        {submitLoading ? 'Please wait...' : (authBooting ? 'Connecting...' : (mode === 'signin' ? 'Sign In' : 'Create Account'))}
                    </button>
                </form>

                <div className="mb-4 mt-6 flex items-center gap-3 text-xs uppercase tracking-[0.18em] text-gray-500">
                    <div className="h-px flex-1 bg-white/[0.08]" />
                    <span>Optional Google</span>
                    <div className="h-px flex-1 bg-white/[0.08]" />
                </div>

                <button
                    type="button"
                    onClick={() => void handleGoogleSignIn()}
                    disabled={googleLoading || submitLoading || authBooting}
                    className="flex w-full items-center justify-center gap-3 rounded-xl border border-white/[0.08] bg-white/[0.04] px-4 py-3 text-sm font-semibold text-white transition hover:border-white/[0.14] hover:bg-white/[0.08] disabled:opacity-60"
                >
                    <GoogleMark />
                    {googleLoading ? 'Redirecting to Google...' : (authBooting ? 'Connecting auth...' : 'Continue with Google')}
                </button>

                <p className="text-center text-sm text-gray-500 mt-6">
                    {mode === 'signin' ? "Don't have an account? " : 'Already have an account? '}
                    <button onClick={() => { setMode(mode === 'signin' ? 'signup' : 'signin'); setError(''); setInfo(''); }}
                        className="text-violet-400 hover:text-violet-300 font-medium transition">
                        {mode === 'signin' ? 'Sign Up' : 'Sign In'}
                    </button>
                </p>
            </div>
        </>
    );
}

function GoogleMark() {
    return (
        <svg viewBox="0 0 24 24" aria-hidden="true" className="h-5 w-5">
            <path fill="#EA4335" d="M12 10.2v3.9h5.5c-.2 1.3-1.5 3.9-5.5 3.9-3.3 0-6-2.8-6-6.2s2.7-6.2 6-6.2c1.9 0 3.2.8 3.9 1.5l2.7-2.6C17 2.8 14.8 2 12 2 6.9 2 2.8 6.4 2.8 11.8S6.9 21.6 12 21.6c6.1 0 9.1-4.3 9.1-6.5 0-.4 0-.7-.1-.9H12Z" />
            <path fill="#4285F4" d="M21.1 15.1c0-.4 0-.7-.1-.9H12v3.9h5.5c-.3 1.5-1.5 2.8-3.1 3.6l3 2.4c2.7-2.5 3.7-6.1 3.7-9Z" opacity=".001" />
            <path fill="#FBBC05" d="M6.5 14.2c-.2-.7-.4-1.5-.4-2.4s.1-1.6.4-2.4L3.4 7C2.9 8.3 2.6 9.9 2.6 11.8s.3 3.4.8 4.8l3.1-2.4Z" />
            <path fill="#34A853" d="M12 21.6c2.8 0 5.1-.9 6.9-2.5l-3-2.4c-.8.6-2 1.1-3.9 1.1-2.5 0-4.7-1.7-5.5-4l-3.1 2.4c1.5 3 4.5 5.4 8.6 5.4Z" />
            <path fill="#4285F4" d="M6.5 9.4c.8-2.3 3-4 5.5-4 1.9 0 3.2.8 3.9 1.5l2.9-2.8C17 2.8 14.8 2 12 2 7.9 2 4.9 4.4 3.4 7l3.1 2.4Z" />
        </svg>
    );
}
