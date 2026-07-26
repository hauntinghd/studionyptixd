import { PROD_API_BASE_URL, isTauriDesktopApp } from '../shared';

export const STUDIO_DESKTOP_RELEASE_URL = `${PROD_API_BASE_URL}/api/desktop/releases/latest`;
export const STUDIO_DESKTOP_DOWNLOAD_URL = `${PROD_API_BASE_URL}/api/desktop/download`;
export const STUDIO_DESKTOP_OPEN_URL = 'nyptid-studio://open/agent';

export type DesktopRelease = {
    version: string;
    available: boolean;
    download_url: string;
    sha256: string;
    published_at: string;
    notes?: string;
};

const VERSION_RE = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/;
const TRUSTED_DESKTOP_DOWNLOAD_ORIGINS = new Set([
    PROD_API_BASE_URL,
    // Signed releases already published by the previous backend remain valid
    // during rollback and updater transition, but new clients never route API
    // traffic here by default.
    'https://nyptid-studio.fly.dev',
]);

function isTrustedDesktopDownloadUrl(parsed: URL): boolean {
    return (
        TRUSTED_DESKTOP_DOWNLOAD_ORIGINS.has(parsed.origin)
        && !parsed.username
        && !parsed.password
        && !parsed.port
        && !parsed.search
        && !parsed.hash
        && (
            parsed.pathname === '/api/desktop/download'
            || parsed.pathname.startsWith('/api/desktop/download/')
        )
    );
}

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
            || !isTrustedDesktopDownloadUrl(parsed)
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

export async function runningDesktopVersion(): Promise<string | null> {
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

export type DesktopUpdateProgress = {
    phase: 'checking' | 'downloading' | 'installing';
    percent?: number;
};

export type DesktopUpdateResult = 'relaunching' | 'manual-download';

/**
 * Install an update through Tauri's signed native updater. Versions older than
 * 1.0.2 trust the retired updater key and/or Fly endpoint, so the deliberate
 * Contabo trust rotation requires one manual installer. Version 1.0.2 and
 * later update in place through the canonical API and relaunch.
 */
export async function installDesktopUpdate(
    release: DesktopRelease,
    onProgress?: (progress: DesktopUpdateProgress) => void,
): Promise<DesktopUpdateResult> {
    const currentVersion = await runningDesktopVersion();
    if (!currentVersion) throw new Error('Could not verify the installed Studio version.');

    if (compareVersions(currentVersion, '1.0.2') < 0) {
        window.location.assign(release.download_url);
        return 'manual-download';
    }

    onProgress?.({ phase: 'checking' });
    const [{ check }, { relaunch }] = await Promise.all([
        import('@tauri-apps/plugin-updater'),
        import('@tauri-apps/plugin-process'),
    ]);
    const update = await check({ timeout: 20_000 });
    if (!update) throw new Error('The update is no longer available. Check again in a moment.');
    if (compareVersions(update.version, currentVersion) <= 0) {
        await update.close();
        throw new Error('Studio is already current.');
    }

    let downloaded = 0;
    let total = 0;
    await update.downloadAndInstall((event) => {
        if (event.event === 'Started') {
            total = Number(event.data.contentLength || 0);
            downloaded = 0;
            onProgress?.({ phase: 'downloading', percent: total > 0 ? 0 : undefined });
            return;
        }
        if (event.event === 'Progress') {
            downloaded += Number(event.data.chunkLength || 0);
            onProgress?.({
                phase: 'downloading',
                percent: total > 0 ? Math.min(99, Math.round((downloaded / total) * 100)) : undefined,
            });
            return;
        }
        onProgress?.({ phase: 'installing', percent: 100 });
    }, { timeout: 10 * 60 * 1000 });
    onProgress?.({ phase: 'installing', percent: 100 });
    await relaunch();
    return 'relaunching';
}
