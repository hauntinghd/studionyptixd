import { useContext, useState } from 'react';
import { Lock, Mail } from 'lucide-react';
import NavBar, { type PageNav } from '../components/NavBar';
import { AuthContext, Logo } from '../shared';

export default function AuthPage({ onNavigate }: { onNavigate: PageNav }) {
    const { signIn, signUp, session } = useContext(AuthContext);
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const [mode, setMode] = useState<'signin' | 'signup'>('signin');
    const [error, setError] = useState('');
    const [info, setInfo] = useState('');
    const [loading, setLoading] = useState(false);

    if (session) {
        onNavigate('landing');
        return null;
    }

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setError('');
        setInfo('');
        setLoading(true);
        if (mode === 'signup') {
            const err = await signUp(email, password);
            setLoading(false);
            if (err) { setError(err); return; }
            setInfo('Check your email to confirm your account, then sign in.');
        } else {
            const err = await signIn(email, password);
            setLoading(false);
            if (err) { setError(err); return; }
        }
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
                            ? 'Sign in to access your Studio dashboard.'
                            : (
                                <>
                                    Sign up to start using NYPTID Studio.
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

                    <button
                        type="submit"
                        disabled={loading}
                        className="w-full py-3 bg-violet-600 hover:bg-violet-500 disabled:opacity-60 text-white font-bold rounded-xl transition-all shadow-lg shadow-violet-600/25"
                    >
                        {loading ? 'Please wait...' : (mode === 'signin' ? 'Sign In' : 'Create Account')}
                    </button>
                </form>

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
