/** One stable command identity per logical browser production request. */
export function productionIdempotencyKey(scope: string, stableEntity?: string): string {
    const cleanedScope = String(scope || 'production').replace(/[^a-zA-Z0-9_-]+/g, '-').slice(0, 80);
    const cleanedEntity = String(stableEntity || '').replace(/[^a-zA-Z0-9_-]+/g, '-').slice(0, 160);
    if (cleanedEntity) return `studio-${cleanedScope}-${cleanedEntity}`;
    const random = globalThis.crypto?.randomUUID?.()
        || `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 14)}`;
    return `studio-${cleanedScope}-${random}`;
}
