import { useContext, useMemo, useRef, useState, useEffect } from 'react';
import { ChevronDown, LogOut, MessageSquarePlus, Settings, User } from 'lucide-react';
import { AuthContext, Logo, STUDIO_SITE_URL } from '../../shared';
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

    const goBilling = () => onNavigate('billing');

    return (
        <header className="sticky top-0 z-30 flex h-14 items-center justify-between gap-3 border-b border-white/[0.06] bg-[#09090b]/95 px-4 backdrop-blur-md sm:px-6">
            <a href={`${STUDIO_SITE_URL}?page=dashboard&tab=agent`} className="flex items-center gap-2 shrink-0">
                <Logo size={28} />
            </a>

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
