import { useContext, useMemo, useRef, useState, useEffect } from 'react';
import { ChevronDown, Download, Loader2, LogOut, MessageSquarePlus, Settings, User } from 'lucide-react';
import { AuthContext, Logo, isTauriDesktopApp } from '../../shared';
import {
    STUDIO_DESKTOP_DOWNLOAD_URL,
    fetchDesktopRelease,
    installDesktopUpdate,
    isDesktopUpdate,
    type DesktopRelease,
    type DesktopUpdateProgress,
} from '../../lib/desktopRelease';
import type { PageNav } from '../NavBar';
import CreditFuelBar from './CreditFuelBar';
import NotificationBell from './NotificationBell';

export default function StudioTopBar({ onNavigate }: { onNavigate: PageNav }) {
    const {
        session,
        role,
        signOut,
        topupCreditsRemaining,
        monthlyCreditsRemaining,
        creditsTotalRemaining,
    } = useContext(AuthContext);
    const [menuOpen, setMenuOpen] = useState(false);
    const [desktopUpdate, setDesktopUpdate] = useState<DesktopRelease | null>(null);
    const [updateProgress, setUpdateProgress] = useState<DesktopUpdateProgress | null>(null);
    const [updateError, setUpdateError] = useState('');
    const menuRef = useRef<HTMLDivElement | null>(null);
    const discordUrl = 'https://discord.gg/zMZxRRu7BS';

    const accountLabel = useMemo(() => {
        const email = String(session?.user?.email || '').trim();
        if (!email) return 'Account';
        return email.split('@')[0] || email;
    }, [session]);

    const totalAc = Number(creditsTotalRemaining ?? 0) || (
        Number(topupCreditsRemaining || 0) + Number(monthlyCreditsRemaining || 0)
    );

    useEffect(() => {
        const onPointerDown = (e: MouseEvent) => {
            if (!menuRef.current?.contains(e.target as Node)) setMenuOpen(false);
        };
        window.addEventListener('mousedown', onPointerDown);
        return () => window.removeEventListener('mousedown', onPointerDown);
    }, []);

    useEffect(() => {
        let cancelled = false;
        const checkForUpdate = async () => {
            const release = await fetchDesktopRelease();
            const updateAvailable = await isDesktopUpdate(release);
            if (!cancelled) setDesktopUpdate(updateAvailable ? release : null);
        };
        void checkForUpdate();
        const interval = window.setInterval(checkForUpdate, 30 * 60 * 1000);
        const onFocus = () => { void checkForUpdate(); };
        window.addEventListener('focus', onFocus);
        return () => {
            cancelled = true;
            window.clearInterval(interval);
            window.removeEventListener('focus', onFocus);
        };
    }, []);

    const goBilling = () => onNavigate('billing');

    const runDesktopUpdate = async () => {
        if (!desktopUpdate || updateProgress) return;
        setUpdateError('');
        setUpdateProgress({ phase: 'checking' });
        try {
            const result = await installDesktopUpdate(desktopUpdate, setUpdateProgress);
            if (result === 'manual-download') {
                setUpdateProgress(null);
                setUpdateError(
                    `Studio ${desktopUpdate.version} opened in your browser. Run that installer once; later updates install here automatically.`,
                );
            }
        } catch (error) {
            setUpdateProgress(null);
            setUpdateError((error as Error).message || 'Studio update failed. Try again.');
        }
    };

    const updateTitle = updateProgress
        ? updateProgress.phase === 'downloading'
            ? `Downloading Studio ${desktopUpdate?.version || ''}${typeof updateProgress.percent === 'number' ? ` (${updateProgress.percent}%)` : ''}`
            : updateProgress.phase === 'installing'
                ? `Installing Studio ${desktopUpdate?.version || ''}`
                : `Checking Studio ${desktopUpdate?.version || ''}`
        : updateError || `Install Studio ${desktopUpdate?.version || ''}`;

    return (
        <header className="sticky top-0 z-30 flex h-14 items-center justify-between gap-2 border-b border-white/[0.06] bg-[#09090b]/95 px-2 backdrop-blur-md sm:gap-3 sm:px-6">
            <div className="flex shrink-0 items-center gap-1.5">
                <button type="button" onClick={() => onNavigate('dashboard')} className="flex items-center gap-2">
                    <Logo size={28} />
                </button>
                {!isTauriDesktopApp && (
                    <a
                        href={STUDIO_DESKTOP_DOWNLOAD_URL}
                        title="Download Studio for desktop"
                        className="inline-flex h-8 items-center gap-1.5 rounded-lg border border-cyan-400/25 bg-cyan-400/10 px-2 text-xs font-semibold text-cyan-100 transition hover:border-cyan-300/50 hover:bg-cyan-400/20 hover:text-white"
                    >
                        <Download className="h-4 w-4" />
                        <span className="hidden sm:inline">Download Studio</span>
                    </a>
                )}
                {desktopUpdate && (
                    <button
                        type="button"
                        onClick={() => { void runDesktopUpdate(); }}
                        disabled={Boolean(updateProgress)}
                        title={updateTitle}
                        aria-label={updateTitle}
                        className="grid h-8 w-8 place-items-center rounded-lg border border-cyan-400/25 bg-cyan-400/10 text-cyan-200 transition hover:border-cyan-300/50 hover:bg-cyan-400/20 hover:text-white disabled:cursor-wait disabled:opacity-70"
                    >
                        {updateProgress
                            ? <Loader2 className="h-4 w-4 animate-spin" />
                            : <Download className="h-4 w-4" />}
                    </button>
                )}
                {desktopUpdate && updateError && (
                    <div
                        role="status"
                        className="absolute left-2 top-12 z-50 flex max-w-[min(92vw,420px)] items-start gap-2 rounded-lg border border-cyan-400/25 bg-[#071014] px-3 py-2 text-xs leading-relaxed text-cyan-50 shadow-2xl"
                    >
                        <span>{updateError}</span>
                        <button
                            type="button"
                            onClick={() => setUpdateError('')}
                            className="shrink-0 text-cyan-300/70 hover:text-white"
                            aria-label="Dismiss update message"
                        >
                            ×
                        </button>
                    </div>
                )}
            </div>

            <div className="flex flex-1 items-center justify-end gap-2 sm:gap-3">
                <a
                    href={discordUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="hidden rounded-lg border border-white/[0.08] px-3 py-1.5 text-xs font-medium text-gray-300 transition hover:border-indigo-500/30 hover:text-white md:inline-flex"
                >
                    Discord
                </a>

                <CreditFuelBar totalAc={totalAc} onTopUp={goBilling} />
                <NotificationBell />

                <div className="relative" ref={menuRef}>
                    <button
                        type="button"
                        onClick={() => setMenuOpen((v) => !v)}
                        className="flex items-center gap-2 rounded-lg border border-white/[0.08] bg-white/[0.03] px-2 py-1.5 text-sm text-gray-200 transition hover:border-violet-500/30"
                    >
                        <span className="hidden max-w-[120px] truncate sm:inline">{accountLabel}</span>
                        <ChevronDown className="h-4 w-4 text-gray-500" />
                    </button>
                    {menuOpen && (
                        <div className="absolute right-0 z-50 mt-2 w-52 overflow-hidden rounded-xl border border-white/[0.1] bg-[#0c0c10] py-1 shadow-xl">
                            <MenuLink icon={User} label="Account" onClick={() => { setMenuOpen(false); onNavigate('account'); }} />
                            <MenuLink icon={Settings} label="Settings" onClick={() => { setMenuOpen(false); onNavigate('settings'); }} />
                            <MenuLink icon={MessageSquarePlus} label="Membership" onClick={() => { setMenuOpen(false); onNavigate('subscription'); }} />
                            {role === 'admin' && (
                                <MenuLink icon={Settings} label="Billing admin" onClick={() => { setMenuOpen(false); onNavigate('billing'); }} />
                            )}
                            <button
                                type="button"
                                onClick={() => { setMenuOpen(false); void signOut(); }}
                                className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-rose-300 hover:bg-white/[0.04]"
                            >
                                <LogOut className="h-4 w-4" />
                                Sign out
                            </button>
                        </div>
                    )}
                </div>
            </div>
        </header>
    );
}

function MenuLink({
    icon: Icon,
    label,
    onClick,
}: {
    icon: typeof User;
    label: string;
    onClick: () => void;
}) {
    return (
        <button
            type="button"
            onClick={onClick}
            className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-gray-200 hover:bg-white/[0.04]"
        >
            <Icon className="h-4 w-4 text-gray-500" />
            {label}
        </button>
    );
}
