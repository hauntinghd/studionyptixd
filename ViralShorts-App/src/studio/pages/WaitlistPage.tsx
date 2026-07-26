import type { PageNav } from '../components/NavBar';

interface Props {
    onNavigate: PageNav;
}

export default function WaitlistPage({ onNavigate }: Props) {
    return (
        <div className="flex min-h-screen items-center bg-[#09090b] px-4 py-16 text-gray-100">
            <div className="mx-auto w-full max-w-2xl rounded-2xl border border-violet-500/20 bg-gradient-to-br from-violet-500/[0.06] to-cyan-500/[0.05] p-8 text-center sm:p-12">
                <p className="text-xs font-semibold uppercase tracking-[0.24em] text-cyan-300">Public access</p>
                <h1 className="mt-4 text-4xl font-bold text-white">Studio is open.</h1>
                <p className="mx-auto mt-4 max-w-xl leading-relaxed text-gray-400">
                    The waitlist is retired. Create an account, choose a Studio Pro plan, and build long-form,
                    short-form, and product videos from the same workspace. All payments are processed securely by Stripe.
                </p>
                <div className="mt-8 flex flex-col justify-center gap-3 sm:flex-row">
                    <button
                        type="button"
                        onClick={() => onNavigate('auth')}
                        className="rounded-xl bg-gradient-to-r from-violet-600 to-cyan-600 px-6 py-3 text-sm font-semibold text-white transition hover:opacity-95"
                    >
                        Create account
                    </button>
                    <button
                        type="button"
                        onClick={() => onNavigate('subscription')}
                        className="rounded-xl border border-white/[0.1] bg-white/[0.04] px-6 py-3 text-sm font-semibold text-gray-200 transition hover:bg-white/[0.08]"
                    >
                        View Studio Pro
                    </button>
                </div>
            </div>
        </div>
    );
}
