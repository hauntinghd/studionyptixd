/** One stable command identity per logical browser production request. */
export function productionIdempotencyKey(scope: string, stableEntity?: string): string {
    const cleanedScope = String(scope || 'production').replace(/[^a-zA-Z0-9_-]+/g, '-').slice(0, 80);
    const cleanedEntity = String(stableEntity || '').replace(/[^a-zA-Z0-9_-]+/g, '-').slice(0, 160);
    if (cleanedEntity) return `studio-${cleanedScope}-${cleanedEntity}`;
    const random = globalThis.crypto?.randomUUID?.()
        || `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 14)}`;
    return `studio-${cleanedScope}-${random}`;
}

export type ProductionCommandLease = {
    commandId: string;
    release: () => void;
};

/**
 * Reuse the same command after a lost response or hard refresh.
 *
 * Call `release` only after a definite HTTP response/terminal stream. A
 * network exception intentionally leaves the lease in sessionStorage so the
 * next click replays the backend receipt instead of starting paid work again.
 */
export function acquireProductionCommandLease(
    scope: string,
    stableEntity = '',
): ProductionCommandLease {
    const normalizedScope = String(scope || 'production')
        .replace(/[^a-zA-Z0-9_-]+/g, '-')
        .slice(0, 100);
    const normalizedEntity = String(stableEntity || '')
        .replace(/[^a-zA-Z0-9_-]+/g, '-')
        .slice(0, 120);
    const storageKey = `studio:production-command:${normalizedScope}:${normalizedEntity}`;
    let commandId = '';
    try {
        commandId = window.sessionStorage.getItem(storageKey) || '';
    } catch {
        commandId = '';
    }
    if (!commandId) {
        commandId = productionIdempotencyKey(normalizedScope);
        try {
            window.sessionStorage.setItem(storageKey, commandId);
        } catch {
            // The backend still owns idempotency for this live request.
        }
    }
    return {
        commandId,
        release: () => {
            try {
                window.sessionStorage.removeItem(storageKey);
            } catch {
                // Storage may be unavailable in privacy-restricted browsers.
            }
        },
    };
}
