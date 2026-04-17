import { useContext, useEffect, useMemo, useRef, useState } from 'react';
import { Bell, ChevronDown, LogOut, MessageSquarePlus, Settings, User, WalletCards } from 'lucide-react';
import { AuthContext, BILLING_SITE_URL, Logo, STUDIO_SITE_URL, isBillingHost } from '../shared';

export type PageNav = (page: 'landing' | 'dashboard' | 'auth' | 'account' | 'settings' | 'billing' | 'subscription') => void;

export default function NavBar({ onNavigate, active }: { onNavigate: PageNav; active?: string }) {
    const { session, role, signInWithGoogle, signOut, topupCreditsRemaining, monthlyCreditsRemaining, membershipActive } = useContext(AuthContext);
    const isAdmin = role === 'admin';
    const billingHost = isBillingHost;
    const discordUrl = 'https://discord.gg/zMZxRRu7BS';
    const [menuOpen, setMenuOpen] = useState(false);
    const [googleLoading, setGoogleLoading] = useState(false);
    const menuRef = useRef<HTMLDivElement | null>(null);
    const accountLabel = useMemo(() => {
        const email = String(session?.user?.email || '').trim();
        if (!email) return 'Account';
        return email.split('@')[0] || email;
    }, [session]);

    useEffect(() => {
        const onPointerDown = (event: MouseEvent) => {
            if (!menuRef.current) return;
            if (!menuRef.current.contains(event.target as Node)) setMenuOpen(false);
        };
        window.addEventListener('mousedown', onPointerDown);
        return () => window.removeEventListener('mousedown', onPointerDown);
    }, []);

    // Build hrefs that work with middle-click / cmd-click / ctrl-click to open in a new tab.
    // The onClick handler only intercepts plain left-click (no modifiers) for SPA routing.
    const buildPageHref = (page: string, extraParams?: Record<string, string>, hash?: string): string => {
        const base = billingHost ? STUDIO_SITE_URL : (typeof window === 'undefined' ? '/' : window.location.origin);
        const url = new URL(base);
        url.searchParams.set('page', page);
        if (extraParams) {
            for (const [k, v] of Object.entries(extraParams)) url.searchParams.set(k, v);
        }
        if (hash) url.hash = hash;
        // Same-origin anchors can use path+search+hash only; cross-origin needs full URL.
        if (typeof window !== 'undefined' && url.origin === window.location.origin) {
            return `${url.pathname}${url.search}${url.hash}`;
        }
        return url.toString();
    };

    const brandHref = (() => {
        if (billingHost) {
            return session ? `${BILLING_SITE_URL}?view=checkout` : BILLING_SITE_URL;
        }
        return session ? buildPageHref('dashboard') : buildPageHref('landing');
    })();
    const topupHref = buildPageHref('billing', { section: 'topups' }, 'topup-packs');
    const subscriptionHref = buildPageHref('subscription');
    const dashboardHref = buildPageHref('dashboard');
    const accountHref = buildPageHref('account');
    const settingsHref = buildPageHref('settings');
    const billingPageHref = buildPageHref('billing');

    // Intercept only plain left-click — let the browser handle middle-click, ctrl-click,
    // cmd-click, shift-click (those are "open in new tab / new window" gestures).
    const isPlainLeftClick = (e: React.MouseEvent) =>
        e.button === 0 && !e.metaKey && !e.ctrlKey && !e.shiftKey && !e.altKey;

    const navigateToUrl = (targetUrl: string) => {
        const target = new URL(targetUrl, window.location.origin);
        const current = new URL(window.location.href);
        if (target.origin === current.origin) {
            const nextHref = `${target.pathname}${target.search}${target.hash}`;
            const currentHref = `${current.pathname}${current.search}${current.hash}`;
            if (nextHref !== currentHref) {
                window.history.pushState({}, '', nextHref);
            } else if (target.hash) {
                window.location.hash = target.hash;
            }
            window.dispatchEvent(new CustomEvent('nyptid:navigation'));
            window.dispatchEvent(new PopStateEvent('popstate'));
            return;
        }
        window.location.assign(target.toString());
    };

    const handleBrandClick = (e: React.MouseEvent) => {
        if (!isPlainLeftClick(e)) return;
        e.preventDefault();
        if (billingHost) {
            window.location.href = brandHref;
            return;
        }
        onNavigate(session ? 'dashboard' : 'landing');
    };

    const handleTopupClick = (e: React.MouseEvent) => {
        if (!isPlainLeftClick(e)) return;
        e.preventDefault();
        navigateToUrl(topupHref);
    };

    const handleSubscriptionClick = (e: React.MouseEvent) => {
        if (!isPlainLeftClick(e)) return;
        e.preventDefault();
        if (billingHost) {
            window.location.href = subscriptionHref;
            return;
        }
        onNavigate('subscription');
    };

    const handleDashboardClick = (e: React.MouseEvent) => {
        if (!isPlainLeftClick(e)) return;
        e.preventDefault();
        if (billingHost) {
            window.location.href = STUDIO_SITE_URL;
            return;
        }
        onNavigate('dashboard');
    };

    const handleAccountClick = (e: React.MouseEvent) => {
        if (!isPlainLeftClick(e)) return;
        e.preventDefault();
        if (billingHost) {
            window.location.href = accountHref;
            return;
        }
        onNavigate('account');
    };

    const handleSettingsClick = (e: React.MouseEvent) => {
        if (!isPlainLeftClick(e)) return;
        e.preventDefault();
        if (billingHost) {
            window.location.href = settingsHref;
            return;
        }
        onNavigate('settings');
    };

    const handleBillingMenuClick = (e: React.MouseEvent) => {
        if (!isPlainLeftClick(e)) return;
        e.preventDefault();
        setMenuOpen(false);
        if (billingHost) {
            window.location.href = `${BILLING_SITE_URL}?page=billing`;
            return;
        }
        onNavigate('billing');
    };

    const handleLogout = async () => {
        setMenuOpen(false);
        await signOut();
    };

    const handleGoogleAuth = async () => {
        setGoogleLoading(true);
        const error = await signInWithGoogle();
        setGoogleLoading(false);
        if (error) onNavigate('auth');
    };

    return (
        <nav className="fixed top-0 left-0 right-0 z-50 border-b border-white/[0.06] bg-[#09090b]/88 backdrop-blur-xl">
            <div className="h-16 px-4 sm:px-6 lg:px-8 flex items-center justify-between gap-4">
                <div className="flex items-center gap-4 min-w-0">
                    <a href={brandHref} onClick={handleBrandClick} className="flex items-center gap-2 font-bold text-white min-w-0">
                        <Logo size={24} />
                        <span className="truncate">NYPTID Studio</span>
                    </a>
                </div>

                {!session ? (
                    <div className="flex items-center gap-3">
                        <a
                            href={discordUrl}
                            target="_blank"
                            rel="noreferrer"
                            className="hidden md:inline-flex items-center gap-2 rounded-lg border border-white/[0.08] bg-white/[0.02] px-3 py-2 text-sm text-gray-300 hover:text-white hover:border-violet-500/40 hover:bg-violet-500/10 transition"
                        >
                            <MessageSquarePlus className="w-4 h-4 text-violet-300" />
                            Join Discord
                        </a>
                        <button
                            type="button"
                            onClick={() => onNavigate('auth')}
                            className="rounded-lg border border-white/[0.08] bg-white/[0.02] px-4 py-2 text-sm font-medium text-gray-200 transition hover:border-white/[0.14] hover:bg-white/[0.06]"
                        >
                            Email Sign In
                        </button>
                        <button
                            onClick={() => void handleGoogleAuth()}
                            disabled={googleLoading}
                            className="rounded-lg bg-violet-600 px-4 py-2 text-sm font-medium text-white transition hover:bg-violet-500"
                        >
                            {googleLoading ? 'Opening Google...' : 'Continue with Google'}
                        </button>
                    </div>
                ) : (
                    <div className="flex items-center gap-2 sm:gap-3">
                        <a
                            href={discordUrl}
                            target="_blank"
                            rel="noreferrer"
                            className="hidden lg:inline-flex items-center gap-2 rounded-lg border border-white/[0.08] bg-white/[0.02] px-3 py-2 text-sm text-gray-300 hover:text-white hover:border-violet-500/40 hover:bg-violet-500/10 transition"
                        >
                            <MessageSquarePlus className="w-4 h-4 text-violet-300" />
                            Join Discord
                        </a>

                        <div className="hidden xl:flex items-center gap-2 rounded-xl border border-cyan-500/20 bg-cyan-500/10 px-3 py-2">
                            <div>
                                <p className="text-[10px] uppercase tracking-[0.18em] text-cyan-200/70">Credit Wallet</p>
                                <p className="text-sm font-semibold text-cyan-100">{Number(topupCreditsRemaining || 0)}</p>
                            </div>
                            <a href={topupHref} onClick={handleTopupClick} className="rounded-lg bg-cyan-500/80 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-cyan-400">
                                Top Up
                            </a>
                        </div>

                        <div className="hidden xl:flex items-center gap-2 rounded-xl border border-violet-500/20 bg-violet-500/10 px-3 py-2">
                            <div>
                                <p className="text-[10px] uppercase tracking-[0.18em] text-violet-200/70">Included Credits</p>
                                <p className="text-sm font-semibold text-violet-100">{Number(monthlyCreditsRemaining || 0)}</p>
                            </div>
                            <a href={topupHref} onClick={handleTopupClick} className="rounded-lg bg-violet-500/80 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-violet-400">
                                Top Up
                            </a>
                        </div>

                        <a
                            href={subscriptionHref}
                            onClick={handleSubscriptionClick}
                            className="hidden lg:inline-flex rounded-lg border border-white/[0.08] bg-white/[0.02] px-4 py-2 text-sm font-medium text-gray-200 transition hover:border-white/[0.14] hover:bg-white/[0.06]"
                        >
                            {membershipActive ? 'Membership' : 'Start Membership'}
                        </a>

                        <button type="button" className="hidden md:inline-flex items-center gap-2 rounded-lg border border-white/[0.08] bg-white/[0.02] px-3 py-2 text-sm text-gray-300">
                            English
                        </button>

                        <button type="button" className="inline-flex h-10 w-10 items-center justify-center rounded-lg border border-white/[0.08] bg-white/[0.02] text-gray-300 transition hover:border-white/[0.14] hover:bg-white/[0.06] hover:text-white">
                            <Bell className="w-4 h-4" />
                        </button>

                        <div className="relative" ref={menuRef}>
                            <button
                                type="button"
                                onClick={() => setMenuOpen((open) => !open)}
                                className={`inline-flex items-center gap-2 rounded-xl border px-3 py-2 text-sm font-medium transition ${
                                    menuOpen || active === 'account' || active === 'settings'
                                        ? 'border-violet-500/50 bg-violet-500/10 text-white'
                                        : 'border-white/[0.08] bg-white/[0.02] text-gray-200 hover:border-white/[0.14] hover:bg-white/[0.06]'
                                }`}
                            >
                                <span className="inline-flex h-6 w-6 items-center justify-center rounded-full bg-cyan-500/20 text-xs font-bold text-cyan-100">
                                    {accountLabel.slice(0, 1).toUpperCase() || 'O'}
                                </span>
                                <span className="hidden sm:inline truncate max-w-[120px]">{accountLabel}</span>
                                <ChevronDown className="w-4 h-4 opacity-70" />
                            </button>

                            {menuOpen && (
                                <div className="absolute right-0 mt-2 w-72 rounded-2xl border border-white/[0.08] bg-[#141416] p-2 shadow-2xl shadow-black/40">
                                    <div className="rounded-xl px-3 py-3">
                                        <p className="text-sm font-semibold text-white">{accountLabel}</p>
                                        <p className="mt-1 text-sm text-gray-400">{session.user.email}</p>
                                        {isAdmin && <p className="mt-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-300">Admin</p>}
                                    </div>
                                    <div className="my-2 border-t border-white/[0.06]" />
                                    <a href={dashboardHref} onClick={(e) => { setMenuOpen(false); handleDashboardClick(e); }} className="flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-sm text-gray-200 transition hover:bg-white/[0.05]">
                                        <User className="w-4 h-4 text-cyan-300" />
                                        Dashboard
                                    </a>
                                    <a href={accountHref} onClick={(e) => { setMenuOpen(false); handleAccountClick(e); }} className="flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-sm text-gray-200 transition hover:bg-white/[0.05]">
                                        <User className="w-4 h-4 text-violet-300" />
                                        Account
                                    </a>
                                    <a href={settingsHref} onClick={(e) => { setMenuOpen(false); handleSettingsClick(e); }} className="flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-sm text-gray-200 transition hover:bg-white/[0.05]">
                                        <Settings className="w-4 h-4 text-amber-300" />
                                        Settings
                                    </a>
                                    <a href={billingPageHref} onClick={handleBillingMenuClick} className="flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-sm text-gray-200 transition hover:bg-white/[0.05]">
                                        <WalletCards className="w-4 h-4 text-cyan-300" />
                                        Billing
                                    </a>
                                    <div className="my-2 border-t border-white/[0.06]" />
                                    <button type="button" onClick={handleLogout} className="flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-sm text-red-300 transition hover:bg-red-500/10">
                                        <LogOut className="w-4 h-4" />
                                        Log out
                                    </button>
                                </div>
                            )}
                        </div>
                    </div>
                )}
            </div>
        </nav>
    );
}
