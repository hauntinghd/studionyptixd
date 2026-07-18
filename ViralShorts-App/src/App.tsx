import { useCallback, useContext, useEffect, useRef, useState } from 'react';
import AccountPage from './studio/pages/AccountPage';
import AuthPage from './studio/pages/AuthPage';
import BillingPage from './studio/pages/BillingPage';
import DashboardPage from './studio/pages/DashboardPage';
import DesktopLaunchPage from './studio/pages/DesktopLaunchPage';
import LandingPage from './studio/pages/LandingPage';
import PrivacyPage from './studio/pages/PrivacyPage';
import SettingsPage from './studio/pages/SettingsPage';
import SubscriptionPage from './studio/pages/SubscriptionPage';
import TermsPage from './studio/pages/TermsPage';
import WaitlistPage from './studio/pages/WaitlistPage';
import WaitlistConfirmationPage from './studio/pages/WaitlistConfirmationPage';
import { AuthContext, AuthProvider, isBillingHost, isTauriDesktopApp } from './studio/shared';
import { trackStudioPageView } from './studio/lib/googleAds';

type StudioPage = 'landing' | 'dashboard' | 'auth' | 'account' | 'settings' | 'billing' | 'subscription' | 'privacy' | 'terms' | 'waitlist' | 'waitlist_confirmation';

const WEB_WORKSPACE_FALLBACK_KEY = 'nyptid:web-workspace-fallback:v1';

const hasPendingAuthRedirectArtifacts = (): boolean => {
    if (typeof window === 'undefined') return false;
    try {
        const url = new URL(window.location.href);
        const hashBody = String(url.hash || '').replace(/^#/, '');
        if (/(^|&)(access_token|refresh_token|expires_in|expires_at|token_type|type|provider_token|provider_refresh_token)=/i.test(hashBody)) {
            return true;
        }
        return ['code', 'error', 'error_code', 'error_description'].some((key) => url.searchParams.has(key));
    } catch {
        return false;
    }
};

function AppShell() {
    const {
        session,
        loading,
        role,
        backendOffline,
        maintenanceBannerEnabled,
        maintenanceBannerMessage,
        signInWithGoogle,
        supabase,
    } = useContext(AuthContext);
    const [desktopAuthError, setDesktopAuthError] = useState('');
    const [webWorkspaceFallback, setWebWorkspaceFallback] = useState(() => {
        if (isTauriDesktopApp || typeof window === 'undefined') return false;
        try {
            return window.sessionStorage.getItem(WEB_WORKSPACE_FALLBACK_KEY) === '1';
        } catch {
            return false;
        }
    });
    const desktopStartupAuthStartedRef = useRef(false);
    const billingHost = isBillingHost;
    const thumblabHost = typeof window !== 'undefined' && window.location.hostname.toLowerCase() === 'thumblab.nyptidindustries.com';
    const resolvePageFromLocation = useCallback((): StudioPage | null => {
        if (typeof window === 'undefined') return null;
        const pathname = String(window.location.pathname || '').replace(/\/+$/, '').toLowerCase();
        if (pathname === '/privacy') return 'privacy';
        if (pathname === '/terms') return 'terms';
        if (pathname === '/waitlist') return 'waitlist';
        if (pathname === '/waitlist/confirmation') return 'waitlist_confirmation';
        const search = new URLSearchParams(window.location.search);
        const urlPage = String(search.get('page') || '').trim().toLowerCase();
        if (urlPage === 'dashboard') return 'dashboard';
        if (urlPage === 'auth') return 'auth';
        if (urlPage === 'landing') return 'landing';
        if (urlPage === 'billing') return 'billing';
        if (urlPage === 'subscription') return 'subscription';
        if (urlPage === 'settings') return 'settings';
        if (urlPage === 'account') return 'account';
        if (urlPage === 'privacy') return 'privacy';
        if (urlPage === 'terms') return 'terms';
        if (urlPage === 'waitlist') return 'waitlist';
        if (urlPage === 'waitlist_confirmation') return 'waitlist_confirmation';
        return billingHost ? 'billing' : null;
    }, [billingHost]);
    const [page, setPage] = useState<StudioPage>(() => {
        // The desktop application is the product workspace, not the marketing
        // website. It boots into auth and moves straight to Studio after the
        // persisted or browser-returned session is available.
        if (isTauriDesktopApp) return 'auth';
        try {
            const locationPage = resolvePageFromLocation();
            if (locationPage) return locationPage;
            const saved = localStorage.getItem('nyptid_page');
            if (
                saved === 'landing' || saved === 'dashboard' || saved === 'auth' || saved === 'account'
                || saved === 'settings' || saved === 'billing' || saved === 'subscription'
                || saved === 'privacy' || saved === 'terms'
            ) {
                return saved;
            }
        } catch {
            // ignore storage errors and fall back
        }
        if (billingHost) return 'billing';
        return 'landing';
    });

    useEffect(() => {
        const syncPageFromLocation = () => {
            const locationPage = resolvePageFromLocation();
            if (!locationPage) return;
            setPage((current) => (current === locationPage ? current : locationPage));
        };
        window.addEventListener('popstate', syncPageFromLocation);
        window.addEventListener('nyptid:navigation', syncPageFromLocation as EventListener);
        return () => {
            window.removeEventListener('popstate', syncPageFromLocation);
            window.removeEventListener('nyptid:navigation', syncPageFromLocation as EventListener);
        };
    }, [resolvePageFromLocation]);

    useEffect(() => {
        if (!thumblabHost) return;
        window.location.replace('https://studio.nyptidindustries.com/?focus=thumbnails');
    }, [thumblabHost]);

    useEffect(() => {
        if (thumblabHost) return;
        try {
            const search = new URLSearchParams(window.location.search);
            if (search.get('focus')) return;
            const referrerHost = new URL(document.referrer || '').hostname.toLowerCase();
            if (referrerHost === 'thumblab.nyptidindustries.com') {
                window.location.replace('https://studio.nyptidindustries.com/?focus=thumbnails');
            }
        } catch {
            // ignore referrer parsing issues
        }
    }, [thumblabHost]);

    useEffect(() => {
        try {
            localStorage.setItem('nyptid_page', page);
        } catch {
            // ignore storage errors
        }
    }, [page]);

    useEffect(() => {
        trackStudioPageView(page);
    }, [page]);

    const continueOnWebForSession = useCallback(() => {
        try {
            window.sessionStorage.setItem(WEB_WORKSPACE_FALLBACK_KEY, '1');
        } catch {
            // The in-memory state still provides the explicit recovery path.
        }
        setWebWorkspaceFallback(true);
    }, []);

    useEffect(() => {
        const handleDesktopAuthError = (event: Event) => {
            const message = String((event as CustomEvent<string>).detail || '').trim();
            setDesktopAuthError(message || 'Google sign-in could not return to Studio.');
        };
        const clearDesktopAuthError = () => setDesktopAuthError('');
        window.addEventListener('nyptid:desktop-auth-error', handleDesktopAuthError);
        window.addEventListener('nyptid:desktop-auth-complete', clearDesktopAuthError);
        return () => {
            window.removeEventListener('nyptid:desktop-auth-error', handleDesktopAuthError);
            window.removeEventListener('nyptid:desktop-auth-complete', clearDesktopAuthError);
        };
    }, []);

    useEffect(() => {
        if (!isTauriDesktopApp || loading || session) return;
        if (page !== 'auth') setPage('auth');
        if (!supabase || desktopStartupAuthStartedRef.current) return;

        desktopStartupAuthStartedRef.current = true;
        void signInWithGoogle().then((error) => {
            if (!error) return;
            window.dispatchEvent(new CustomEvent('nyptid:desktop-auth-error', { detail: error }));
        });
    }, [loading, page, session, signInWithGoogle, supabase]);

    useEffect(() => {
        if (loading) return;
        if (isTauriDesktopApp && !session) {
            if (page !== 'auth') setPage('auth');
            return;
        }
        // Legal pages are always reachable — both when signed in (no forced-dashboard redirect)
        // and when signed out (no forced-auth redirect). Google's OAuth verification flow
        // needs these URLs to load for any visitor, logged in or not.
        if (page === 'privacy' || page === 'terms') return;
        // Waitlist is retired — send visitors to sign-in or Studio.
        if (page === 'waitlist' || page === 'waitlist_confirmation') {
            setPage(session ? 'dashboard' : 'auth');
            return;
        }
        const authRedirectPending = hasPendingAuthRedirectArtifacts();
        if (billingHost) {
            if (!session && !authRedirectPending && (page === 'dashboard' || page === 'account' || page === 'settings')) {
                setPage('auth');
                return;
            }
            if (session && (page === 'landing' || page === 'auth')) {
                setPage('billing');
                return;
            }
            return;
        }
        if (!session && !authRedirectPending && page === 'dashboard') setPage('landing');
        if (!session && !authRedirectPending && (page === 'account' || page === 'settings' || page === 'billing' || page === 'subscription')) setPage('auth');

        const isAdmin = role === 'admin';

        if (backendOffline) {
            if (session && isAdmin && (page === 'landing' || page === 'auth')) {
                setPage('dashboard');
                return;
            }
            if (session && (page === 'landing' || page === 'auth')) setPage('dashboard');
            return;
        }

        if (session && (page === 'landing' || page === 'auth')) {
            setPage('dashboard');
        }
    }, [session, loading, page, role, backendOffline, billingHost]);

    return (
        <div className="min-h-[100dvh] overflow-x-hidden bg-[#09090b] text-gray-100 font-sans selection:bg-violet-500/30">
            {maintenanceBannerEnabled && (
                <div className="sticky top-0 z-50 border-b border-amber-300/20 bg-amber-500/10 px-4 py-2 text-center text-xs sm:text-sm text-amber-100 backdrop-blur">
                    {maintenanceBannerMessage || 'Studio is under high load. Queue times may be longer than usual while we scale capacity.'}
                </div>
            )}
            {desktopAuthError && (
                <div role="alert" className="sticky top-0 z-50 flex items-center justify-center gap-3 border-b border-red-300/20 bg-red-500/10 px-4 py-2 text-center text-xs sm:text-sm text-red-100 backdrop-blur">
                    <span>{desktopAuthError}</span>
                    <button type="button" className="text-red-200 underline underline-offset-2" onClick={() => setDesktopAuthError('')}>
                        Dismiss
                    </button>
                </div>
            )}
            {page === 'landing' && <LandingPage onNavigate={setPage} />}
            {page === 'dashboard' && session && !isTauriDesktopApp && !webWorkspaceFallback
                ? <DesktopLaunchPage onContinueWeb={continueOnWebForSession} />
                : page === 'dashboard' && <DashboardPage onNavigate={setPage} />}
            {page === 'auth' && <AuthPage onNavigate={setPage} />}
            {page === 'account' && <AccountPage onNavigate={setPage} />}
            {page === 'settings' && <SettingsPage onNavigate={setPage} />}
            {page === 'billing' && <BillingPage onNavigate={setPage} />}
            {page === 'subscription' && <SubscriptionPage onNavigate={setPage} />}
            {page === 'privacy' && <PrivacyPage />}
            {page === 'terms' && <TermsPage />}
            {page === 'waitlist' && <WaitlistPage onNavigate={setPage} />}
            {page === 'waitlist_confirmation' && <WaitlistConfirmationPage onNavigate={setPage} />}
        </div>
    );
}

export default function App() {
    return (
        <AuthProvider>
            <AppShell />
        </AuthProvider>
    );
}
