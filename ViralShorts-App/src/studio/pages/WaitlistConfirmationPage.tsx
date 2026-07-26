import type { PageNav } from '../components/NavBar';

interface Props {
    onNavigate: PageNav;
}

export default function WaitlistConfirmationPage({ onNavigate }: Props) {
    return (
        <div className="flex min-h-screen items-center justify-center bg-[#09090b] px-4 py-16 text-gray-100">
            <div className="mx-auto max-w-xl text-center">
                <div className="text-5xl">✓</div>
                <h1 className="mt-5 text-3xl font-bold text-white sm:text-4xl">Welcome to Studio.</h1>
                <p className="mt-4 text-lg text-violet-200">
                    Stripe checkout returned successfully. Studio will grant access only after its signed webhook confirms payment.
                </p>
                <p className="mt-4 text-sm leading-relaxed text-gray-400">
                    Open Studio to see your confirmed plan and credit balance. If confirmation is still pending, your account
                    will refresh automatically without creating a duplicate charge.
                </p>
                <button
                    type="button"
                    onClick={() => onNavigate('dashboard')}
                    className="mt-8 rounded-xl bg-gradient-to-r from-violet-600 to-cyan-600 px-6 py-3 text-sm font-semibold text-white transition hover:opacity-95"
                >
                    Open Studio
                </button>
            </div>
        </div>
    );
}
