import { useCallback, useContext, useEffect, useState } from 'react';
import AccountPage from './studio/pages/AccountPage';
import AuthPage from './studio/pages/AuthPage';
import BillingPage from './studio/pages/BillingPage';
import DashboardPage from './studio/pages/DashboardPage';
import LandingPage from './studio/pages/LandingPage';
import PrivacyPage from './studio/pages/PrivacyPage';
import SettingsPage from './studio/pages/SettingsPage';
import SubscriptionPage from './studio/pages/SubscriptionPage';
import TermsPage from './studio/pages/TermsPage';
import WaitlistPage from './studio/pages/WaitlistPage';
import WaitlistConfirmationPage from './studio/pages/WaitlistConfirmationPage';
import { AuthContext, AuthProvider, isBillingHost } from './studio/shared';
import { trackStudioPageView } from './studio/lib/googleAds';

type StudioPage = 'landing' | 'dashboard' | 'auth' | 'account' | 'settings' | 'billing' | 'subscription' | 'privacy' | 'terms' | 'waitlist' | 'waitlist_confirmation';

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
    const { session, loading, role, backendOffline, maintenanceBannerEnabled, maintenanceBannerMessage, waitlistOnlyMode } = useContext(AuthContext);
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

    useEffect(() => {
        if (loading) return;
        // Legal pages are always reachable — both when signed in (no forced-dashboard redirect)
        // and when signed out (no forced-auth redirect). Google's OAuth verification flow
        // needs these URLs to load for any visitor, logged in or not.
        if (page === 'privacy' || page === 'terms') return;
        // Waitlist pages are always reachable — that's the whole point.
        if (page === 'waitlist' || page === 'waitlist_confirmation') return;
        // WAITLIST GATE: when the backend flags waitlistOnlyMode=true, every
        // non-admin user (signed in or not) gets redirected to /waitlist for
        // any dashboard/settings/billing route. Admins keep full access so
        // Casey can still debug + test behind the gate.
        if (waitlistOnlyMode && role !== 'admin') {
            const gatedPages: StudioPage[] = ['dashboard', 'account', 'settings', 'billing', 'subscription'];
            if (gatedPages.includes(page)) {
                setPage('waitlist');
                return;
            }
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
    }, [session, loading, page, role, backendOffline, billingHost, waitlistOnlyMode]);

    return (
        <div className="min-h-screen bg-[#09090b] text-gray-100 font-sans selection:bg-violet-500/30">
            {maintenanceBannerEnabled && (
                <div className="sticky top-0 z-50 border-b border-amber-300/20 bg-amber-500/10 px-4 py-2 text-center text-xs sm:text-sm text-amber-100 backdrop-blur">
                    {maintenanceBannerMessage || 'Studio is under high load. Queue times may be longer than usual while we scale capacity.'}
                </div>
            )}
            {page === 'landing' && <LandingPage onNavigate={setPage} />}
            {page === 'dashboard' && <DashboardPage onNavigate={setPage} />}
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
