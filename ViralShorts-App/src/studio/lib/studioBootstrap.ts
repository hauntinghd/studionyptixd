export const STUDIO_BOOTSTRAP_RETRY_DELAYS_MS = [3_000, 5_000] as const;

type StatusError = Error & { status?: number };
type StudioStorage = Pick<Storage, 'getItem' | 'setItem'>;

const STUDIO_BOOTSTRAP_KEY_VALUE = 'studio-agent-bootstrap-v2';

export function studioApiError(message: string, status: number): StatusError {
    const error = new Error(message) as StatusError;
    error.status = status;
    return error;
}

export class StudioTransportError extends Error {
    constructor(message: string, cause?: unknown) {
        super(message);
        this.name = 'StudioTransportError';
        if (cause !== undefined) (this as Error & { cause?: unknown }).cause = cause;
    }
}

export function studioTransportError(message: string, cause?: unknown): StudioTransportError {
    return new StudioTransportError(message, cause);
}

export function isStudioNetworkError(value: unknown): boolean {
    if (value instanceof StudioTransportError) return true;
    const status = Number((value as StatusError | null)?.status || 0);
    if (status > 0) return false;
    const message = String(
        value instanceof Error ? value.message : value || '',
    ).trim();
    return (
        /^(?:TypeError:\s*)?(?:failed to fetch|load failed|networkerror(?: when attempting to fetch resource\.?)?|the network connection was lost\.?)$/i.test(message)
        || /^Studio Agent could not reach the backend from this browser tab\./i.test(message)
        || /^Studio Agent timed out after \d+s\b/i.test(message)
        || /^Studio Agent connection dropped and the recovery refresh could not reach the backend\./i.test(message)
        || /^Studio Agent stream (?:could not connect|disconnected before the final event)\./i.test(message)
    );
}

export function clearStudioNetworkError(message: string): string {
    return isStudioNetworkError(message) ? '' : message;
}

export function isRetryableStudioBootstrapError(value: unknown): boolean {
    const status = Number((value as StatusError | null)?.status || 0);
    return (
        isStudioNetworkError(value)
        || status === 408
        || status === 425
        || status === 429
        || status === 502
        || status === 503
        || status === 504
        || status === 524
    );
}

export class StudioBootstrapCancelledError extends Error {
    constructor() {
        super('Studio bootstrap was cancelled.');
        this.name = 'StudioBootstrapCancelledError';
    }
}

export function studioBootstrapStorageKey(userId: string): string {
    return `studio_agent_bootstrap_key_v2:${String(userId || '').trim() || 'anonymous'}`;
}

export function loadPersistedStudioBootstrapKey(
    storage: StudioStorage,
    userId: string,
): string {
    const storageKey = studioBootstrapStorageKey(userId);
    try {
        const stored = String(storage.getItem(storageKey) || '').trim();
        if (stored.length >= 8 && stored.length <= 128) return stored;
        storage.setItem(storageKey, STUDIO_BOOTSTRAP_KEY_VALUE);
    } catch {
        // The deterministic value still converges across tabs when storage is
        // disabled. The backend additionally namespaces and locks by user.
    }
    return STUDIO_BOOTSTRAP_KEY_VALUE;
}

export interface StudioBootstrapResult<T> {
    attempts: number;
    value: T;
}

interface StudioBootstrapOptions<T> {
    bootstrap: () => Promise<T>;
    sleep: (delayMs: number) => Promise<void>;
    shouldRetry?: (error: unknown) => boolean;
    shouldContinue?: () => boolean;
    retryDelaysMs?: readonly number[];
    onRetry?: (details: {
        attempt: number;
        delayMs: number;
        error: unknown;
    }) => void;
}

/**
 * Retry one backend-owned resume-or-create contract.
 *
 * The browser never decides whether a POST retry should create. Every attempt
 * carries the same persisted key to the backend's atomic bootstrap endpoint.
 */
export async function runBoundedStudioBootstrap<T>(
    options: StudioBootstrapOptions<T>,
): Promise<StudioBootstrapResult<T>> {
    const retryDelaysMs = options.retryDelaysMs || STUDIO_BOOTSTRAP_RETRY_DELAYS_MS;
    const shouldRetry = options.shouldRetry || isRetryableStudioBootstrapError;
    const shouldContinue = options.shouldContinue || (() => true);

    for (let attemptIndex = 0; attemptIndex <= retryDelaysMs.length; attemptIndex += 1) {
        if (!shouldContinue()) throw new StudioBootstrapCancelledError();
        try {
            const value = await options.bootstrap();
            if (!shouldContinue()) throw new StudioBootstrapCancelledError();
            return { attempts: attemptIndex + 1, value };
        } catch (error) {
            if (error instanceof StudioBootstrapCancelledError) throw error;
            if (
                attemptIndex >= retryDelaysMs.length
                || !shouldRetry(error)
            ) {
                throw error;
            }
            const delayMs = retryDelaysMs[attemptIndex];
            options.onRetry?.({
                attempt: attemptIndex + 1,
                delayMs,
                error,
            });
            await options.sleep(delayMs);
        }
    }

    throw new Error('Studio bootstrap exhausted its retry budget.');
}

/**
 * React StrictMode and a user clicking Reload can otherwise overlap boot
 * requests. This gate makes all concurrent callers share one authoritative
 * bootstrap, then permits a later explicit retry after it settles.
 */
export class StudioBootstrapSingleFlight {
    private active: Promise<unknown> | null = null;

    run<T>(operation: () => Promise<T>): Promise<T> {
        if (this.active) return this.active as Promise<T>;
        const started = operation();
        this.active = started;
        void started.finally(() => {
            if (this.active === started) this.active = null;
        }).catch(() => {
            // The original promise remains the caller-visible rejection.
        });
        return started;
    }

    invalidate(): void {
        this.active = null;
    }
}
