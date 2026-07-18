import { PROD_API_BASE_URL, isTauriDesktopApp } from '../shared';

export const STUDIO_DESKTOP_RELEASE_URL = `${PROD_API_BASE_URL}/api/desktop/releases/latest`;
export const STUDIO_DESKTOP_DOWNLOAD_URL = `${PROD_API_BASE_URL}/api/desktop/download`;

export type DesktopRelease = {
    version: string;
    available: boolean;
    download_url: string;
    sha256: string;
    published_at: string;
    notes?: string;
};

const VERSION_RE = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/;

export function compareVersions(left: string, right: string): number {
    if (!VERSION_RE.test(left) || !VERSION_RE.test(right)) return 0;
    const a = left.split('-', 1)[0].split('.').map(Number);
    const b = right.split('-', 1)[0].split('.').map(Number);
    for (let index = 0; index < 3; index += 1) {
        if (a[index] !== b[index]) return a[index] > b[index] ? 1 : -1;
    }
    return 0;
}

export async function fetchDesktopRelease(): Promise<DesktopRelease | null> {
    try {
        const response = await fetch(STUDIO_DESKTOP_RELEASE_URL, { cache: 'no-store' });
        if (!response.ok) return null;
        const payload = await response.json() as Partial<DesktopRelease>;
        const version = String(payload.version || '').trim();
        const downloadUrl = String(payload.download_url || '').trim();
        const parsed = new URL(downloadUrl);
        if (
            !VERSION_RE.test(version)
            || payload.available !== true
            || parsed.protocol !== 'https:'
            || parsed.hostname !== 'nyptid-studio.fly.dev'
        ) return null;
        return {
            version,
            available: true,
            download_url: downloadUrl,
            sha256: String(payload.sha256 || '').trim(),
            published_at: String(payload.published_at || '').trim(),
            notes: String(payload.notes || '').trim(),
        };
    } catch {
        return null;
    }
}

async function runningDesktopVersion(): Promise<string | null> {
    if (!isTauriDesktopApp) return null;
    try {
        // The desktop shell loads the live web app, so a version baked into the
        // web bundle can be newer or older than the binary that is actually
        // running. Tauri is the only authoritative source here.
        const { getVersion } = await import('@tauri-apps/api/app');
        const version = String(await getVersion()).trim();
        return VERSION_RE.test(version) ? version : null;
    } catch {
        // Fail closed: never advertise an update unless both versions were
        // verified. The public website still exposes the normal download CTA.
        return null;
    }
}

export async function isDesktopUpdate(release: DesktopRelease | null): Promise<boolean> {
    if (!release) return false;
    const currentVersion = await runningDesktopVersion();
    return Boolean(currentVersion && compareVersions(release.version, currentVersion) > 0);
}
