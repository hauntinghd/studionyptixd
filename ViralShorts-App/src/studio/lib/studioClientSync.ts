/** Soft client freshness: sync release notes + prefetch new bundles without interrupting chat. */
import { resolveStudioBackendUrl } from '../shared';
import { syncReleaseNotifications } from './studioNotifications';

const MANIFEST_KEY = 'nyptid_studio_client_manifest_v1';
const PENDING_BUNDLE_KEY = 'nyptid_studio_pending_bundle_v1';

export type StudioClientManifest = {
    backend_commit?: string;
    frontend_bundle?: string;
    built_at?: number;
};

function readStoredManifest(): StudioClientManifest {
    if (typeof window === 'undefined') return {};
    try {
        return JSON.parse(localStorage.getItem(MANIFEST_KEY) || '{}') as StudioClientManifest;
    } catch {
        return {};
    }
}

function storeManifest(manifest: StudioClientManifest) {
    if (typeof window === 'undefined') return;
    localStorage.setItem(MANIFEST_KEY, JSON.stringify(manifest));
}

function prefetchBundle(bundleName: string) {
    if (!bundleName || typeof document === 'undefined') return;
    const href = `/assets/${bundleName}`;
    if (document.querySelector(`link[data-studio-prefetch="${bundleName}"]`)) return;
    const link = document.createElement('link');
    link.rel = 'prefetch';
    link.as = 'script';
    link.href = `${href}?prefetch=${Date.now()}`;
    link.setAttribute('data-studio-prefetch', bundleName);
    document.head.appendChild(link);
}

/** Run before each agent message: sync notifications + detect deploy updates. */
export async function ensureStudioFresh(accessToken?: string): Promise<void> {
    if (typeof window === 'undefined') return;
    await syncReleaseNotifications(accessToken);
    try {
        const res = await fetch(resolveStudioBackendUrl('/api/studio/client-manifest'), {
            cache: 'no-store',
            headers: accessToken ? { Authorization: `Bearer ${accessToken}` } : {},
        });
        if (!res.ok) return;
        const manifest = (await res.json().catch(() => ({}))) as StudioClientManifest;
        const prev = readStoredManifest();
        storeManifest(manifest);
        const nextBundle = String(manifest.frontend_bundle || '').trim();
        const prevBundle = String(prev.frontend_bundle || '').trim();
        if (nextBundle && prevBundle && nextBundle !== prevBundle) {
            prefetchBundle(nextBundle);
            sessionStorage.setItem(PENDING_BUNDLE_KEY, nextBundle);
            // This check runs immediately after Send. Never navigate here: a
            // deploy discovered during a repair must not tear down the stream
            // or make a successfully persisted scene card appear to vanish.
            // Manual Sync/reopen remains the explicit bundle-update boundary.
        }
    } catch {
        /* best-effort */
    }
}

/** Apply a prefetched bundle reload only when Studio is idle (no active agent turn). */
export function applyPendingStudioBundleReload(): boolean {
    if (typeof window === 'undefined') return false;
    const pending = String(sessionStorage.getItem(PENDING_BUNDLE_KEY) || '').trim();
    if (!pending) return false;
    sessionStorage.removeItem(PENDING_BUNDLE_KEY);
    const url = new URL(window.location.href);
    url.searchParams.set('_studio_v', pending.slice(0, 24) || String(Date.now()));
    window.location.replace(url.toString());
    return true;
}
