import type { Session } from '@supabase/supabase-js';

const INVOICER_BASE_URL = String(import.meta.env.VITE_INVOICER_BASE_URL || 'https://invoicer.nyptidindustries.com')
    .trim()
    .replace(/\/+$/, '');

const STUDIO_BUSINESS_NAME_KEY = 'nyptid_studio_business_name';

function readCachedBusinessName() {
    if (typeof window === 'undefined') return '';
    return String(window.localStorage.getItem(STUDIO_BUSINESS_NAME_KEY) || '').trim();
}

function writeCachedBusinessName(value: string) {
    if (typeof window === 'undefined') return;
    window.localStorage.setItem(STUDIO_BUSINESS_NAME_KEY, value);
}

function deriveSuggestedBusinessName(session: Session | null) {
    const metadata = (session?.user?.user_metadata || {}) as Record<string, unknown>;
    const candidates = [
        metadata.business_name,
        metadata.company_name,
        metadata.full_name,
        metadata.name,
        readCachedBusinessName(),
        session?.user?.email ? String(session.user.email).split('@')[0] : '',
    ];

    for (const candidate of candidates) {
        const trimmed = String(candidate || '').trim();
        if (trimmed) return trimmed;
    }

    return 'Studio Client';
}

function resolveBusinessName(session: Session | null) {
    const suggestedBusinessName = deriveSuggestedBusinessName(session);
    const response = window.prompt(
        'Business name for this hosted Studio license:',
        suggestedBusinessName,
    );
    const businessName = String(response || '').trim();
    if (!businessName) return '';
    writeCachedBusinessName(businessName);
    return businessName;
}

export async function startHostedStudioCheckout(session: Session | null): Promise<string | null> {
    const customerEmail = String(session?.user?.email || '').trim().toLowerCase();
    if (!customerEmail) {
        return 'Sign in first so the hosted license can be attached to your account email.';
    }

    const businessName = resolveBusinessName(session);
    if (!businessName) {
        return 'A business name is required to start the hosted Studio license checkout.';
    }

    const metadata = {
        source: 'studio-hosted-license',
        origin: 'studio-app',
        studioUserId: session?.user?.id || '',
    };

    const customerName = String(
        (session?.user?.user_metadata || {})?.full_name ||
        (session?.user?.user_metadata || {})?.name ||
        '',
    ).trim();

    const baseUrl = typeof window !== 'undefined'
        ? `${window.location.origin}${window.location.pathname}`
        : 'https://studio.nyptidindustries.com/';

    const successUrl = `${baseUrl}?page=billing&subscription=success`;
    const cancelUrl = `${baseUrl}?page=billing&subscription=cancelled`;

    try {
        const response = await fetch(`${INVOICER_BASE_URL}/api/billing/public/orders`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                productCode: 'studio',
                customerName: customerName || undefined,
                customerEmail,
                businessName,
                successUrl,
                cancelUrl,
                metadata,
            }),
        });

        const payload = await response.json().catch(() => ({})) as {
            checkoutUrl?: string;
            error?: string;
        };

        if (!response.ok) {
            return payload.error || `Hosted checkout failed (${response.status})`;
        }

        const checkoutUrl = String(payload.checkoutUrl || '').trim();
        if (!checkoutUrl) {
            return 'Hosted checkout URL was not returned by Invoicer.';
        }

        window.location.assign(checkoutUrl);
        return null;
    } catch (error) {
        console.error('Hosted Studio checkout failed', error);
        return 'Unable to start the hosted Studio checkout right now.';
    }
}
