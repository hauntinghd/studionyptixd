import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import {
    STUDIO_BOOTSTRAP_RETRY_DELAYS_MS,
    StudioBootstrapCancelledError,
    StudioBootstrapSingleFlight,
    clearStudioNetworkError,
    isRetryableStudioBootstrapError,
    isStudioNetworkError,
    loadPersistedStudioBootstrapKey,
    runBoundedStudioBootstrap,
    studioApiError,
    studioTransportError,
} from '../node_modules/.cache/studio-bootstrap-tests/studioBootstrap.js';

const networkFailure = () => studioTransportError(
    'Studio Agent could not reach the backend from this browser tab.',
);

test('cold bootstrap retries one backend-owned contract with the same client key', async () => {
    let calls = 0;
    const keys = [];
    const sleeps = [];
    const bootstrapKey = 'persisted-client-key';

    const result = await runBoundedStudioBootstrap({
        bootstrap: async () => {
            calls += 1;
            keys.push(bootstrapKey);
            if (calls === 1) throw networkFailure();
            return {
                mode: 'resumed',
                session: { session_id: 'sa_existing' },
            };
        },
        sleep: async (delayMs) => {
            sleeps.push(delayMs);
        },
    });

    assert.deepEqual(result, {
        attempts: 2,
        value: {
            mode: 'resumed',
            session: { session_id: 'sa_existing' },
        },
    });
    assert.deepEqual(keys, [bootstrapKey, bootstrapKey]);
    assert.deepEqual(sleeps, [STUDIO_BOOTSTRAP_RETRY_DELAYS_MS[0]]);
});

test('cold bootstrap retries are bounded', async () => {
    let calls = 0;
    const sleeps = [];

    await assert.rejects(
        runBoundedStudioBootstrap({
            bootstrap: async () => {
                calls += 1;
                throw networkFailure();
            },
            sleep: async (delayMs) => {
                sleeps.push(delayMs);
            },
        }),
        /could not reach the backend/i,
    );

    assert.equal(calls, 3);
    assert.deepEqual(sleeps, [...STUDIO_BOOTSTRAP_RETRY_DELAYS_MS]);
});

test('bootstrap cancellation stops retries and prevents a stale result', async () => {
    let current = true;
    let calls = 0;

    await assert.rejects(
        runBoundedStudioBootstrap({
            bootstrap: async () => {
                calls += 1;
                current = false;
                return { session: { session_id: 'sa_stale' } };
            },
            shouldContinue: () => current,
            sleep: async () => {},
        }),
        StudioBootstrapCancelledError,
    );

    assert.equal(calls, 1);
});

test('the bootstrap key is persisted per authenticated user', () => {
    const values = new Map();
    const storage = {
        getItem(key) {
            return values.get(key) ?? null;
        },
        setItem(key, value) {
            values.set(key, value);
        },
    };

    const first = loadPersistedStudioBootstrapKey(storage, 'owner-1');
    const second = loadPersistedStudioBootstrapKey(storage, 'owner-1');
    const otherUser = loadPersistedStudioBootstrapKey(storage, 'owner-2');

    assert.equal(first, second);
    assert.ok(first.length >= 8);
    assert.equal(otherUser, first);
    assert.equal(values.size, 2);
});

test('single-flight collapses concurrent calls and invalidation permits explicit navigation', async () => {
    const gate = new StudioBootstrapSingleFlight();
    let calls = 0;
    let releaseFirst;
    const held = new Promise((resolve) => {
        releaseFirst = resolve;
    });
    const firstOperation = async () => {
        calls += 1;
        await held;
        return 'stale';
    };

    const first = gate.run(firstOperation);
    const duplicate = gate.run(firstOperation);
    assert.strictEqual(first, duplicate);
    assert.equal(calls, 1);

    gate.invalidate();
    const explicit = gate.run(async () => {
        calls += 1;
        return 'explicit';
    });
    assert.notStrictEqual(explicit, first);
    assert.equal(await explicit, 'explicit');
    assert.equal(calls, 2);

    releaseFirst();
    assert.equal(await first, 'stale');
});

test('only typed or anchored transport failures are classified as network errors', () => {
    assert.equal(isStudioNetworkError(networkFailure()), true);
    assert.equal(isStudioNetworkError(new Error('Failed to fetch')), true);
    assert.equal(
        isStudioNetworkError(new Error('Studio Agent timed out after 12s - retry Resume.')),
        true,
    );
    assert.equal(
        isStudioNetworkError(studioApiError('Studio Agent timed out after 12s', 500)),
        false,
    );
    assert.equal(isStudioNetworkError(new Error('Production timed out after 300s')), false);
    assert.equal(isStudioNetworkError(new Error('Production failed visual QA')), false);
    assert.equal(
        clearStudioNetworkError(
            'Studio Agent could not reach the backend from this browser tab. '
            + 'Your chat is preserved; wait a moment and press Resume.',
        ),
        '',
    );
    assert.equal(clearStudioNetworkError('Production failed visual QA'), 'Production failed visual QA');
    assert.equal(isRetryableStudioBootstrapError(studioApiError('Bad gateway', 502)), true);
    assert.equal(isRetryableStudioBootstrapError(studioApiError('Invalid request', 422)), false);
});

test('AgentPanel uses the atomic endpoint and cancels stale bootstrap navigation', () => {
    const source = readFileSync(
        new URL('../src/studio/panels/AgentPanel.tsx', import.meta.url),
        'utf8',
    );
    const bootstrapBlock = source
        .split('const bootstrapStudio = useCallback', 2)[1]
        .split('const reloadCurrentSession = useCallback', 1)[0];
    const createBlock = source
        .split('const createNewSession = useCallback', 2)[1]
        .split('const openSession = useCallback', 1)[0];
    const openBlock = source
        .split('const openSession = useCallback', 2)[1]
        .split('const bootstrapStudio = useCallback', 1)[0];

    assert.match(bootstrapBlock, /\/sessions\/bootstrap\?message_tail=/);
    assert.match(bootstrapBlock, /bootstrap_key:\s*bootstrapKey/);
    assert.match(bootstrapBlock, /preferred_session_id:\s*lastId/);
    assert.doesNotMatch(bootstrapBlock, /authFetch\('\/api\/studio-agent\/sessions',/);
    assert.match(bootstrapBlock, /bootstrapGenerationRef\.current === generation/);
    assert.match(createBlock, /cancelBootstrapRecovery\(\)/);
    assert.match(openBlock, /cancelBootstrapRecovery\(\)/);
});
