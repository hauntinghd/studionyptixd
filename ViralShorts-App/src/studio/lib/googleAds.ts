const env = ((import.meta as any).env || {}) as Record<string, string>;

const GOOGLE_TAG_ID = String(env.VITE_GOOGLE_TAG_ID || '').trim();
const GOOGLE_ADS_ID = String(env.VITE_GOOGLE_ADS_ID || '').trim();
const PRIMARY_TAG_ID = GOOGLE_TAG_ID || GOOGLE_ADS_ID;

const CONVERSION_LABELS = {
    signup: String(env.VITE_GOOGLE_ADS_SIGNUP_LABEL || '').trim(),
    firstProject: String(env.VITE_GOOGLE_ADS_FIRST_PROJECT_LABEL || '').trim(),
    firstRender: String(env.VITE_GOOGLE_ADS_FIRST_RENDER_LABEL || '').trim(),
    topup: String(env.VITE_GOOGLE_ADS_TOPUP_LABEL || '').trim(),
    membership: String(env.VITE_GOOGLE_ADS_MEMBERSHIP_LABEL || '').trim(),
};

declare global {
    interface Window {
        dataLayer?: any[];
        gtag?: (...args: any[]) => void;
        __nyptidGoogleTagLoaded?: boolean;
        __nyptidGoogleTagInit?: boolean;
    }
}

const hasTrackingConfigured = (): boolean => Boolean(PRIMARY_TAG_ID);

const ensureGoogleTag = (): boolean => {
    if (!hasTrackingConfigured()) return false;
    if (typeof window === 'undefined' || typeof document === 'undefined') return false;

    window.dataLayer = window.dataLayer || [];
    window.gtag = window.gtag || function gtag(...args: any[]) {
        window.dataLayer?.push(args);
    };

    if (!window.__nyptidGoogleTagInit) {
        window.gtag('js', new Date());
        const configuredIds = Array.from(new Set([GOOGLE_TAG_ID, GOOGLE_ADS_ID].filter(Boolean)));
        configuredIds.forEach((id) => {
            window.gtag?.('config', id, { send_page_view: false });
        });
        window.__nyptidGoogleTagInit = true;
    }

    if (!window.__nyptidGoogleTagLoaded) {
        const existing = document.querySelector(`script[data-nyptid-gtag="${PRIMARY_TAG_ID}"]`);
        if (!existing) {
            const script = document.createElement('script');
            script.async = true;
            script.src = `https://www.googletagmanager.com/gtag/js?id=${encodeURIComponent(PRIMARY_TAG_ID)}`;
            script.dataset.nyptidGtag = PRIMARY_TAG_ID;
            document.head.appendChild(script);
        }
        window.__nyptidGoogleTagLoaded = true;
    }

    return true;
};

const fireEvent = (name: string, payload: Record<string, any> = {}) => {
    if (!ensureGoogleTag()) return;
    window.gtag?.('event', name, payload);
};

const onceKey = (suffix: string) => `nyptid_ads_once:${suffix}`;

export const trackStudioPageView = (pageName: string) => {
    if (!ensureGoogleTag()) return;
    const safePage = String(pageName || 'unknown').trim().toLowerCase() || 'unknown';
    const pagePath = safePage === 'landing' ? '/' : `/?page=${encodeURIComponent(safePage)}`;
    const payload = {
        page_title: `NYPTID Studio - ${safePage}`,
        page_path: pagePath,
        page_location: typeof window !== 'undefined' ? window.location.href : '',
    };
    if (GOOGLE_TAG_ID) {
        window.gtag?.('event', 'page_view', {
            ...payload,
            send_to: GOOGLE_TAG_ID,
        });
    } else {
        window.gtag?.('event', 'page_view', payload);
    }
};

export const trackStudioEvent = (eventName: string, payload: Record<string, any> = {}) => {
    fireEvent(eventName, payload);
};

export const trackGoogleAdsConversion = (
    conversion: keyof typeof CONVERSION_LABELS,
    payload: {
        value?: number;
        currency?: string;
        transactionId?: string;
    } = {},
) => {
    const label = CONVERSION_LABELS[conversion];
    if (!GOOGLE_ADS_ID || !label) return;
    fireEvent('conversion', {
        send_to: `${GOOGLE_ADS_ID}/${label}`,
        value: Number(payload.value || 0) || 0,
        currency: String(payload.currency || 'USD'),
        transaction_id: String(payload.transactionId || ''),
    });
};

export const trackOnce = (key: string, fn: () => void, scope: 'session' | 'local' = 'session'): boolean => {
    if (typeof window === 'undefined') return false;
    const storage = scope === 'local' ? window.localStorage : window.sessionStorage;
    const storageKey = onceKey(key);
    if (storage.getItem(storageKey) === '1') return false;
    storage.setItem(storageKey, '1');
    fn();
    return true;
};

export const trackAuthCompletion = (intent: 'signup' | 'signin' | 'google', isLikelyNewUser: boolean) => {
    const method = intent === 'google' ? 'google' : 'email';
    const eventName = isLikelyNewUser || intent === 'signup' ? 'sign_up' : 'login';
    trackStudioEvent(eventName, { method });
    if (eventName === 'sign_up') {
        trackGoogleAdsConversion('signup');
    }
};

export const trackShortProjectStarted = (template: string, mode: string, firstOnly = false) => {
    const fire = () => {
        trackStudioEvent('short_project_started', {
            template,
            mode,
        });
        trackGoogleAdsConversion('firstProject');
    };
    if (firstOnly) {
        trackOnce('first_short_project_started', fire, 'local');
        return;
    }
    fire();
};

export const trackShortRenderCompleted = (template: string, outputResolution: string, firstOnly = false) => {
    const fire = () => {
        trackStudioEvent('short_render_completed', {
            template,
            output_resolution: outputResolution,
        });
        trackGoogleAdsConversion('firstRender');
    };
    if (firstOnly) {
        trackOnce('first_short_render_completed', fire, 'local');
        return;
    }
    fire();
};

export const trackTopupPurchaseCompleted = (valueUsd: number, transactionId = '') => {
    trackStudioEvent('topup_purchase_completed', { value: valueUsd, currency: 'USD' });
    trackGoogleAdsConversion('topup', {
        value: valueUsd,
        currency: 'USD',
        transactionId,
    });
};

export const trackMembershipPurchaseCompleted = (planId: string, valueUsd: number, transactionId = '') => {
    trackStudioEvent('membership_purchase_completed', {
        plan_id: planId,
        value: valueUsd,
        currency: 'USD',
    });
    trackGoogleAdsConversion('membership', {
        value: valueUsd,
        currency: 'USD',
        transactionId,
    });
};

