import { useContext, useEffect, useState } from 'react';
import AccountPage from './studio/pages/AccountPage';
import AuthPage from './studio/pages/AuthPage';
import BillingPage from './studio/pages/BillingPage';
import DashboardPage from './studio/pages/DashboardPage';
import LandingPage from './studio/pages/LandingPage';
import SettingsPage from './studio/pages/SettingsPage';
import SubscriptionPage from './studio/pages/SubscriptionPage';
import { AuthContext, AuthProvider, isBillingHost } from './studio/shared';

type StudioPage = 'landing' | 'dashboard' | 'auth' | 'account' | 'settings' | 'billing' | 'subscription';

function AppShell() {
    const { session, loading, role, backendOffline, maintenanceBannerEnabled, maintenanceBannerMessage } = useContext(AuthContext);
    const billingHost = isBillingHost;
    const thumblabHost = typeof window !== 'undefined' && window.location.hostname.toLowerCase() === 'thumblab.nyptidindustries.com';
    const [page, setPage] = useState<StudioPage>(() => {
        try {
            const urlPage = new URLSearchParams(window.location.search).get('page');
            if (urlPage === 'billing') return 'billing';
            if (urlPage === 'subscription') return 'subscription';
            if (urlPage === 'settings') return 'settings';
            if (urlPage === 'account') return 'account';
            const saved = localStorage.getItem('nyptid_page');
            if (saved === 'landing' || saved === 'dashboard' || saved === 'auth' || saved === 'account' || saved === 'settings' || saved === 'billing' || saved === 'subscription') {
                return saved;
            }
        } catch {
            // ignore storage errors and fall back
        }
        if (billingHost) return 'billing';
        return 'landing';
    });

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
        if (billingHost) {
            if (!session && (page === 'dashboard' || page === 'account' || page === 'settings')) {
                setPage('auth');
                return;
            }
            if (session && (page === 'landing' || page === 'auth')) {
                setPage('billing');
                return;
            }
            return;
        }
        if (!session && page === 'dashboard') setPage('landing');
        if (!session && (page === 'account' || page === 'settings' || page === 'billing' || page === 'subscription')) setPage('auth');
        if (loading) return;

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
