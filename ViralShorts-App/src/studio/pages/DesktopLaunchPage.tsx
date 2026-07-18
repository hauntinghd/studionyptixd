import { Download, ExternalLink, MonitorDown } from 'lucide-react';
import { Logo } from '../shared';
import {
    STUDIO_DESKTOP_DOWNLOAD_URL,
    STUDIO_DESKTOP_OPEN_URL,
} from '../lib/desktopRelease';

export default function DesktopLaunchPage({ onContinueWeb }: { onContinueWeb: () => void }) {
    return (
        <main className="flex min-h-[100dvh] items-center justify-center bg-[#07080a] px-5 py-12 text-gray-100">
            <section className="w-full max-w-xl rounded-2xl border border-white/[0.08] bg-[#0c0d10] p-6 shadow-2xl sm:p-9">
                <div className="flex items-center gap-3">
                    <Logo size={42} />
                    <div>
                        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-cyan-300">Studio desktop</p>
                        <h1 className="mt-1 text-2xl font-bold text-white sm:text-3xl">Create inside the Studio app</h1>
                    </div>
                </div>

                <p className="mt-5 text-sm leading-6 text-gray-300 sm:text-base">
                    The desktop app is the primary Studio workspace. It opens directly in Studio Agent and keeps
                    production, updates, billing refreshes, and connected-channel work together in one window.
                </p>

                <div className="mt-7 grid gap-3">
                    <a
                        href={STUDIO_DESKTOP_OPEN_URL}
                        className="inline-flex items-center justify-center gap-2 rounded-xl bg-cyan-400 px-5 py-3 text-sm font-bold text-black transition hover:bg-cyan-300"
                    >
                        <ExternalLink className="h-4 w-4" />
                        Open installed Studio
                    </a>
                    <a
                        href={STUDIO_DESKTOP_DOWNLOAD_URL}
                        className="inline-flex items-center justify-center gap-2 rounded-xl border border-cyan-400/30 bg-cyan-400/10 px-5 py-3 text-sm font-semibold text-cyan-50 transition hover:border-cyan-300/50 hover:bg-cyan-400/20"
                    >
                        <Download className="h-4 w-4" />
                        Download the latest Studio
                    </a>
                </div>

                <div className="mt-6 rounded-xl border border-white/[0.07] bg-black/20 p-4">
                    <div className="flex items-start gap-3">
                        <MonitorDown className="mt-0.5 h-4 w-4 shrink-0 text-gray-500" />
                        <div>
                            <p className="text-xs leading-5 text-gray-400">
                                Cannot install or open the app on this device? Studio Web remains available as a
                                recovery path for this browser session.
                            </p>
                            <button
                                type="button"
                                onClick={onContinueWeb}
                                className="mt-2 text-xs font-semibold text-gray-300 underline decoration-white/20 underline-offset-4 transition hover:text-white"
                            >
                                Continue on Studio Web for this session
                            </button>
                        </div>
                    </div>
                </div>
            </section>
        </main>
    );
}
