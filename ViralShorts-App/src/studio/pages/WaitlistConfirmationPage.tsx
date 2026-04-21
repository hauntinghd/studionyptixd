import type { PageNav } from '../components/NavBar';

interface Props {
    onNavigate: PageNav;
}

export default function WaitlistConfirmationPage({ onNavigate }: Props) {
    const search = typeof window !== 'undefined' ? new URLSearchParams(window.location.search) : null;
    const plan = search?.get('plan') || '';
    const provider = search?.get('provider') || '';

    return (
        <div className="min-h-screen bg-[#09090b] text-gray-100 px-4 py-16 flex items-center justify-center">
            <div className="mx-auto max-w-xl text-center">
                <div className="mb-8 text-6xl">🎉</div>
                <h1 className="text-3xl sm:text-4xl font-bold text-white">
                    Welcome to Studio.
                </h1>
                <p className="mt-4 text-lg text-violet-200">
                    You're now on the list of open-beta testers.
                </p>

                <div className="mt-8 rounded-2xl border border-white/[0.08] bg-white/[0.02] p-6 text-left">
                    <p className="text-sm text-gray-300 leading-relaxed">
                        Thanks for reserving a slot. We'll email <span className="font-semibold text-white">you</span> as soon
                        as the open beta opens up with access instructions and login details.
                    </p>
                    {plan && (
                        <p className="mt-3 text-xs text-gray-500">
                            Plan reserved: <span className="text-gray-300 capitalize">{plan}</span>
                            {provider ? <> · Paid via <span className="text-gray-300">{provider}</span></> : null}
                        </p>
                    )}
                    <p className="mt-4 text-xs text-gray-500 leading-relaxed">
                        If development extends beyond the first month, we'll email you before each renewal so you can
                        opt out or continue at the same rate. No surprise charges.
                    </p>
                </div>

                <div className="mt-8 flex flex-col sm:flex-row items-center justify-center gap-3">
                    <button
                        type="button"
                        onClick={() => onNavigate('landing')}
                        className="rounded-lg border border-white/[0.08] bg-white/[0.03] px-5 py-2.5 text-sm font-semibold text-gray-200 transition hover:bg-white/[0.06]"
                    >
                        Back to Studio
                    </button>
                    <a
                        href="https://paypal.me/YtItsOmatic"
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex items-center gap-2 rounded-lg border border-blue-400/40 bg-blue-500/[0.08] px-4 py-2.5 text-sm font-semibold text-blue-200 transition hover:border-blue-300 hover:bg-blue-500/[0.12]"
                    >
                        ❤️ Donate to speed up development
                    </a>
                </div>
            </div>
        </div>
    );
}
