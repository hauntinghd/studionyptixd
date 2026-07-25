export type ProductionEffectiveModeV1 = 'plan' | 'studio' | 'cliplab';

export type ProductionCommandLifecycleV1 =
    | 'compiled'
    | 'ready'
    | 'executing'
    | 'verifying'
    | 'completed'
    | 'failed'
    | 'cancelled';

export type ProductionCommandViewV1 = {
    command_id: string;
    action: string;
    lifecycle: ProductionCommandLifecycleV1;
    target_job_id: string;
    progress_percent: number;
    active_step: string;
    error: string;
};

export type ProductionJobViewV1 = {
    job_id: string;
    kind: string;
    title: string;
    status: string;
    stage: string;
    progress_percent: number;
};

export type ProductionCardViewV1 = {
    card_id: string;
    kind: string;
    job_id: string;
    title: string;
    status: string;
    body: string;
    actions: string[];
};

export type ProductionPendingConfirmationV1 = {
    command_id: string;
    prompt: string;
    approve_action: string;
    cancel_action: string;
};

export type ProductionNoticeV1 = {
    notice_id: string;
    level: 'info' | 'success' | 'warning' | 'error';
    message: string;
};

/** The backend-owned, complete production projection for one Studio session. */
export type ProductionViewV1 = {
    schema_version: 'production-view-v1';
    session_id: string;
    view_revision: number;
    /** Opaque backend token. The browser must never parse or increment it. */
    state_revision: string;
    effective_mode: ProductionEffectiveModeV1;
    command: ProductionCommandViewV1 | null;
    jobs: ProductionJobViewV1[];
    cards: ProductionCardViewV1[];
    allowed_actions: string[];
    pending_confirmation: ProductionPendingConfirmationV1 | null;
    notices: ProductionNoticeV1[];
};

export type ProductionViewStateV1 = {
    session_id: string;
    view_revision: number;
    state_revision: string;
    view: ProductionViewV1 | null;
    /** Exact opaque job IDs only. Prefix and normalized matching are forbidden. */
    jobs_by_id: Readonly<Record<string, ProductionJobViewV1>>;
};

export function emptyProductionViewStateV1(sessionId = ''): ProductionViewStateV1 {
    return {
        session_id: sessionId,
        view_revision: -1,
        state_revision: '',
        view: null,
        jobs_by_id: Object.freeze(Object.create(null) as Record<string, ProductionJobViewV1>),
    };
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function finiteNumber(value: unknown, fallback = 0): number {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
}

function stringArray(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    return value.map((item) => String(item || '').trim()).filter(Boolean);
}

function parseCommand(value: unknown): ProductionCommandViewV1 | null {
    if (value == null) return null;
    if (!isRecord(value)) return null;
    const commandId = String(value.command_id || '').trim();
    const lifecycle = String(value.lifecycle || '') as ProductionCommandLifecycleV1;
    const validLifecycles = new Set<ProductionCommandLifecycleV1>([
        'compiled',
        'ready',
        'executing',
        'verifying',
        'completed',
        'failed',
        'cancelled',
    ]);
    if (!commandId || !validLifecycles.has(lifecycle)) return null;
    return {
        command_id: commandId,
        action: String(value.action || '').trim(),
        lifecycle,
        target_job_id: String(value.target_job_id || '').trim(),
        progress_percent: finiteNumber(value.progress_percent),
        active_step: String(value.active_step || '').trim(),
        error: String(value.error || ''),
    };
}

function parseJobs(value: unknown): ProductionJobViewV1[] | null {
    if (!Array.isArray(value)) return null;
    const seen = new Set<string>();
    const jobs: ProductionJobViewV1[] = [];
    for (const raw of value) {
        if (!isRecord(raw)) return null;
        const jobId = String(raw.job_id || '').trim();
        if (!jobId || seen.has(jobId)) return null;
        seen.add(jobId);
        jobs.push({
            job_id: jobId,
            kind: String(raw.kind || '').trim(),
            title: String(raw.title || '').trim(),
            status: String(raw.status || '').trim(),
            stage: String(raw.stage || '').trim(),
            progress_percent: finiteNumber(raw.progress_percent),
        });
    }
    return jobs;
}

function parseCards(value: unknown): ProductionCardViewV1[] | null {
    if (!Array.isArray(value)) return null;
    const seen = new Set<string>();
    const cards: ProductionCardViewV1[] = [];
    for (const raw of value) {
        if (!isRecord(raw)) return null;
        const cardId = String(raw.card_id || '').trim();
        const jobId = String(raw.job_id || '').trim();
        if (!cardId || seen.has(cardId)) return null;
        seen.add(cardId);
        cards.push({
            card_id: cardId,
            kind: String(raw.kind || '').trim(),
            job_id: jobId,
            title: String(raw.title || '').trim(),
            status: String(raw.status || '').trim(),
            body: String(raw.body || '').trim(),
            actions: stringArray(raw.actions),
        });
    }
    return cards;
}

function parsePendingConfirmation(value: unknown): ProductionPendingConfirmationV1 | null {
    if (value == null) return null;
    if (!isRecord(value)) return null;
    const commandId = String(value.command_id || '').trim();
    if (!commandId) return null;
    return {
        command_id: commandId,
        prompt: String(value.prompt || '').trim(),
        approve_action: String(value.approve_action || '').trim(),
        cancel_action: String(value.cancel_action || '').trim(),
    };
}

function parseNotices(value: unknown): ProductionNoticeV1[] | null {
    if (!Array.isArray(value)) return null;
    const levels = new Set<ProductionNoticeV1['level']>(['info', 'success', 'warning', 'error']);
    const seen = new Set<string>();
    const notices: ProductionNoticeV1[] = [];
    for (const raw of value) {
        if (!isRecord(raw)) return null;
        const noticeId = String(raw.notice_id || '').trim();
        const level = String(raw.level || '') as ProductionNoticeV1['level'];
        if (!noticeId || seen.has(noticeId) || !levels.has(level)) return null;
        seen.add(noticeId);
        notices.push({
            notice_id: noticeId,
            level,
            message: String(raw.message || '').trim(),
        });
    }
    return notices;
}

/**
 * Parse either the canonical top-level SSE payload or a temporary `view` wrapper.
 * Invalid canonical projections fail closed instead of partially mutating UI state.
 */
export function productionViewV1FromUnknown(value: unknown): ProductionViewV1 | null {
    if (!isRecord(value)) return null;
    const candidate = isRecord(value.production_view)
        ? value.production_view
        : isRecord(value.view)
            ? value.view
            : value;
    if (candidate.schema_version !== 'production-view-v1') return null;

    const sessionId = String(candidate.session_id || '').trim();
    const stateRevision = String(candidate.state_revision || '').trim();
    const viewRevision = finiteNumber(candidate.view_revision, -1);
    const effectiveMode = String(candidate.effective_mode || '') as ProductionEffectiveModeV1;
    const jobs = parseJobs(candidate.jobs);
    const cards = parseCards(candidate.cards);
    const notices = parseNotices(candidate.notices);
    const command = parseCommand(candidate.command);
    const pendingConfirmation = parsePendingConfirmation(candidate.pending_confirmation);
    if (
        !sessionId
        || !stateRevision
        || !Number.isInteger(viewRevision)
        || viewRevision < 1
        || !['plan', 'studio', 'cliplab'].includes(effectiveMode)
        || jobs == null
        || cards == null
        || notices == null
        || (candidate.command != null && command == null)
        || (candidate.pending_confirmation != null && pendingConfirmation == null)
        || !Array.isArray(candidate.allowed_actions)
    ) {
        return null;
    }

    return {
        schema_version: 'production-view-v1',
        session_id: sessionId,
        view_revision: viewRevision,
        state_revision: stateRevision,
        effective_mode: effectiveMode,
        command,
        jobs,
        cards,
        allowed_actions: stringArray(candidate.allowed_actions),
        pending_confirmation: pendingConfirmation,
        notices,
    };
}

/**
 * Accept only a strictly newer projection for the exact session. Replaying the
 * same revision is idempotent; stale and cross-session events are ignored.
 */
export function reduceProductionViewV1(
    current: ProductionViewStateV1,
    incoming: ProductionViewV1,
    expectedSessionId: string,
): ProductionViewStateV1 {
    if (!expectedSessionId || incoming.session_id !== expectedSessionId) return current;
    if (current.session_id === incoming.session_id && incoming.view_revision <= current.view_revision) {
        return current;
    }

    const jobsById = Object.create(null) as Record<string, ProductionJobViewV1>;
    for (const job of incoming.jobs) {
        jobsById[job.job_id] = job;
    }
    return {
        session_id: incoming.session_id,
        view_revision: incoming.view_revision,
        state_revision: incoming.state_revision,
        view: incoming,
        jobs_by_id: Object.freeze(jobsById),
    };
}
