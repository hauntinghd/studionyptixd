export const STUDIO_DESKTOP_AUTH_RELAY_HOST = 'studio.nyptidindustries.com';
export const STUDIO_DESKTOP_AUTH_RELAY_MARKER = 'studio_desktop_auth';
export const STUDIO_DESKTOP_AUTH_RELAY_URL =
    `https://${STUDIO_DESKTOP_AUTH_RELAY_HOST}/?${STUDIO_DESKTOP_AUTH_RELAY_MARKER}=1`;

export const STUDIO_DESKTOP_AUTH_CALLBACK_URL = 'nyptid-studio://auth/callback';
const CALLBACK_QUERY_KEYS = ['code', 'error', 'error_code', 'error_description'] as const;
const TOKEN_PARAMETER_PATTERN = /(^|[&#])(access_token|refresh_token|id_token)=/i;

/**
 * Converts the hosted Supabase PKCE return into the registered Tauri deep link.
 * Only one-time authorization results are forwarded; bearer-token fragments are
 * rejected and the browser URL is scrubbed before the app is opened.
 */
export const buildDesktopAuthDeepLink = (rawUrl: string): string | null => {
    try {
        const parsed = new URL(rawUrl);
        if (
            parsed.protocol !== 'https:'
            || parsed.hostname.toLowerCase() !== STUDIO_DESKTOP_AUTH_RELAY_HOST
            || parsed.username
            || parsed.password
            || parsed.port
            || parsed.pathname !== '/'
            || parsed.searchParams.get(STUDIO_DESKTOP_AUTH_RELAY_MARKER) !== '1'
        ) {
            return null;
        }
        if (parsed.hash && TOKEN_PARAMETER_PATTERN.test(parsed.hash)) return null;

        const code = String(parsed.searchParams.get('code') || '').trim();
        const error = String(
            parsed.searchParams.get('error')
            || parsed.searchParams.get('error_description')
            || '',
        ).trim();
        if ((!code && !error) || (code && error)) return null;

        const callback = new URL(STUDIO_DESKTOP_AUTH_CALLBACK_URL);
        for (const key of CALLBACK_QUERY_KEYS) {
            const value = String(parsed.searchParams.get(key) || '').trim();
            if (value) callback.searchParams.set(key, value);
        }
        return callback.toString();
    } catch {
        return null;
    }
};

export const relayDesktopAuthToTauri = (): boolean => {
    if (typeof window === 'undefined') return false;
    const deepLink = buildDesktopAuthDeepLink(window.location.href);
    if (!deepLink) return false;

    // Remove the one-time code from the browser address/history before opening
    // the desktop app. The app alone retains the matching PKCE verifier.
    window.history.replaceState({}, document.title, '/');
    const root = document.getElementById('root');
    if (root) {
        root.textContent = '';
        const message = document.createElement('p');
        message.textContent = 'Returning your secure Google sign-in to NYPTID Studio…';
        message.style.cssText = 'font: 16px system-ui; color: #fff; padding: 32px; background: #050505; min-height: 100vh; margin: 0;';
        root.appendChild(message);
    }
    window.location.assign(deepLink);
    return true;
};
