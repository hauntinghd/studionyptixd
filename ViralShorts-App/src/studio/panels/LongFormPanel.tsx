/**
 * LongFormPanel — Long-form (10-540 minute) generator.
 *
 * 3-tab UX:
 *   1. Channel — pick from registry; Catalyst signals + locked title pattern
 *                surface immediately for the picked channel so the user sees
 *                what works on THIS channel before writing a topic.
 *   2. Outline — topic input + Grok outline (PR #119: enforces channel's
 *                decoded winner pattern via title_template + description_tail).
 *                Editable.
 *   3. Render  — LIVE (PR #120 + #121): kicks a background-job render via
 *                /api/long-form/render-start, polls /jobs/{id}/status, plays
 *                the final MP4 inline. Recent renders gallery + Resume Project
 *                flow mirror the ZT Private pattern.
 *
 * Backend: /api/long-form/{channels,catalyst-insights,outline,render-start,
 *          jobs,jobs/{id},jobs/{id}/{status,state,mp4,thumbnail/{idx},still/{n}}}
 */
import { useCallback, useContext, useEffect, useRef, useState } from 'react';
import {
    BookOpen, BrainCircuit, Briefcase, Building2, Check, Clock, Download, Eye, FileText,
    Film, Headphones, Image as ImageIcon, Loader2, Music, Play, RefreshCw, RotateCcw,
    Search, Sparkles, TrendingUp, Wand2, AlertTriangle,
} from 'lucide-react';
import { API, AuthContext } from '../shared';

type LongFormTab = 'channel' | 'outline' | 'render';

interface ChannelInfo {
    key: string;
    label: string;
    tagline: string;
    icon: string;
    format?: 'long_form' | 'shorts';
    channel_id?: string;
    default_minutes: number;
    fps: number;
    image_model_default: string;
    i2v_model_default: string;
    voice_provider_default: string;
    cost_estimate_usd: number;
    // PR #119: surface decoded winner-pattern fields so the Outline tab can
    // show the user the locked title shape + examples before they hit
    // Generate, and so the Render tab can display the description tail.
    pipeline_kind?: string;
    title_template?: string;
    title_examples?: string[];
    title_avoid?: string[];
    description_tail?: string;
    thumbnail_style_prompt?: string;
}

interface CatalystInsights {
    top_titles: { title: string; views: number; vps: number; video_id: string; likes?: number }[];
    breakout_titles: { title: string; views: number; lift_vs_baseline: number; video_id: string }[];
    hook_patterns: string[];
    thumbnail_signals: string[];
    subscribers?: number;
    videos?: number;
    channel_views?: number;
    harvest_present?: boolean;
}

interface ConnectedChannel {
    channel_id: string;
    channel_title: string;
    channel_handle: string;
    subscriber_count: number;
    video_count: number;
    view_count: number;
    last_synced_at: number | null;
    harvest_present: boolean;
    registry_key: string;
    registry_label: string;
    registry_format: string;
}

interface Chapter {
    index: number;
    title: string;
    minutes: number;
    synopsis: string;
}

interface Outline {
    title: string;
    hook: string;
    chapters: Chapter[];
    tags: string[];
    description?: string;
    _parse_error?: boolean;
}

const CHANNEL_ICON: Record<string, typeof BookOpen> = {
    lacuna: Search,
    hidden_cortex: BrainCircuit,
    zerotier: TrendingUp,
    cryptic_science: Sparkles,
    lexi_manhwa: BookOpen,
    pb_live: Briefcase,
    lofi_radio: Headphones,
    empire_magnates: Building2,
    history_rewind: BookOpen,
};

// ─────────────────────────────────────────────────────────────────────────────
// Render-tab types (PR #121 wires these — backend shapes from PR #120)
// ─────────────────────────────────────────────────────────────────────────────

interface RenderJobStatus {
    job_id: string;
    phase: string;
    percent: number;
    error?: string;
    scene_done?: number;
    scene_total?: number;
    narration_done?: number;
    narration_total?: number;
    chapter_done?: number;
    chapter_total?: number;
    updated_at?: number;
}

interface RecentJobRow {
    job_id: string;
    channel_key: string;
    channel_label?: string;
    pipeline_kind?: string;
    title?: string;
    phase: string;
    percent: number;
    error?: string;
    created_at?: number;
    started_at?: number;
    finished_at?: number;
    mp4_present: boolean;
    mp4_url?: string;
    mp4_duration_sec?: number;
    mp4_size_bytes?: number;
    thumbnail_url?: string;
}

interface JobFullState {
    job_id: string;
    channel_key?: string;
    channel_label?: string;
    pipeline_kind?: string;
    outline?: Outline;
    phase?: string;
    percent?: number;
    error?: string;
    mp4_url?: string;
    mp4_path?: string;
    mp4_duration_sec?: number;
    mp4_size_bytes?: number;
    narration_duration_sec?: number;
    scenes_generated?: number;
    thumbnails_generated?: number;
    thumbnail_urls?: string[];
    started_at?: number;
    finished_at?: number;
    created_at?: number;
    [k: string]: any;
}

const PHASE_LABELS: Record<string, string> = {
    queued: 'Queued',
    starting: 'Starting',
    chapters: 'Writing chapters',
    scenes: 'Generating scenes',
    awaiting_approval: 'Awaiting your approval',
    finalizing: 'Finalizing',
    scene_assembly: 'Assembling scenes (LTX + VO + SFX)',
    narration: 'Recording narration',
    ambient: 'Generating ambient bed',
    thumbnails: 'Generating thumbnails',
    compose: 'Composing final video',
    done: 'Done',
    failed: 'Failed',
    cancelled: 'Cancelled',
    unknown: 'Status unknown',
};

const PHASE_ORDER = [
    'queued', 'starting', 'chapters', 'scenes', 'awaiting_approval',
    'scene_assembly', 'narration', 'ambient', 'thumbnails', 'compose', 'done',
];

interface SceneGridRow {
    scene_idx: number;
    chapter_index: number;
    local_idx: number;
    scene_prompt: string;
    narration?: string;
    still_url: string;
    still_present: boolean;
}

function formatDuration(sec: number): string {
    if (!sec || sec <= 0) return '—';
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = Math.floor(sec % 60);
    if (h > 0) return `${h}h ${m}m ${s}s`;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
}

function formatBytes(b: number): string {
    if (!b || b <= 0) return '—';
    if (b > 1e9) return `${(b / 1e9).toFixed(2)} GB`;
    if (b > 1e6) return `${(b / 1e6).toFixed(1)} MB`;
    if (b > 1e3) return `${(b / 1e3).toFixed(1)} KB`;
    return `${b} B`;
}

function formatRelativeTime(epoch: number): string {
    if (!epoch || epoch <= 0) return '—';
    const ms = Date.now() / 1000 - epoch;
    if (ms < 60) return 'just now';
    if (ms < 3600) return `${Math.floor(ms / 60)}m ago`;
    if (ms < 86400) return `${Math.floor(ms / 3600)}h ago`;
    return `${Math.floor(ms / 86400)}d ago`;
}

export default function LongFormPanel() {
    const { session, supabase } = useContext(AuthContext);
    const accessToken = session?.access_token || '';

    const [tab, setTab] = useState<LongFormTab>('channel');
    const [channels, setChannels] = useState<ChannelInfo[]>([]);
    const [connected, setConnected] = useState<ConnectedChannel[]>([]);
    const [selectedChannel, setSelectedChannel] = useState<string>('');
    const [insights, setInsights] = useState<CatalystInsights | null>(null);
    const [insightsLoading, setInsightsLoading] = useState(false);
    const [catalystPresent, setCatalystPresent] = useState<boolean | null>(null);

    const [topic, setTopic] = useState('');
    const [targetMinutes, setTargetMinutes] = useState<number | ''>('');
    const [useCatalystContext, setUseCatalystContext] = useState(true);
    const [outline, setOutline] = useState<Outline | null>(null);
    const [outliningBusy, setOutliningBusy] = useState(false);
    const [outlineError, setOutlineError] = useState('');

    // PR #121: render-tab state
    const [activeJobId, setActiveJobId] = useState<string>('');
    const [jobStatus, setJobStatus] = useState<RenderJobStatus | null>(null);
    const [jobFullState, setJobFullState] = useState<JobFullState | null>(null);
    const [renderError, setRenderError] = useState('');
    const [renderStarting, setRenderStarting] = useState(false);
    const [recentJobs, setRecentJobs] = useState<RecentJobRow[]>([]);
    const [recentLoading, setRecentLoading] = useState(false);
    const pollAbortRef = useRef<{ cancelled: boolean }>({ cancelled: false });

    // Get a fresh Supabase token for every POST/poll iteration so a token
    // expiring during a multi-hour render doesn't abort the polling loop.
    // Mirrors ZT private's getFreshToken pattern (Phase 4.5d fix).
    const getFreshToken = useCallback(async (): Promise<string> => {
        if (!supabase) return accessToken;
        try {
            const { data } = await supabase.auth.getSession();
            return data?.session?.access_token || accessToken;
        } catch {
            return accessToken;
        }
    }, [supabase, accessToken]);

    // Cold-start resilience helper. Fly machines return HTML 502 during cold
    // starts; r.json() throws unhelpfully. Retry 3× with exponential backoff
    // before surfacing. Used for all single-shot calls (kickoff, /state lookup).
    const fetchJsonResilient = useCallback(async (
        url: string,
        init: RequestInit,
        { retries = 3, retryDelayMs = 2500 }: { retries?: number; retryDelayMs?: number } = {},
    ): Promise<{ ok: boolean; status: number; data: any }> => {
        let lastErr: string = '';
        for (let attempt = 0; attempt <= retries; attempt++) {
            try {
                const r = await fetch(url, init);
                if ((r.status === 502 || r.status === 504) && attempt < retries) {
                    lastErr = `cold-start ${r.status}`;
                    await new Promise((res) => setTimeout(res, retryDelayMs * Math.pow(1.5, attempt)));
                    continue;
                }
                const text = await r.text();
                let data: any = null;
                try {
                    data = text ? JSON.parse(text) : null;
                } catch {
                    if (attempt < retries) {
                        lastErr = `non-JSON response (${r.status}): ${text.slice(0, 80)}`;
                        await new Promise((res) => setTimeout(res, retryDelayMs * Math.pow(1.5, attempt)));
                        continue;
                    }
                    return { ok: false, status: r.status, data: { detail: lastErr || `Server returned non-JSON (${r.status})` } };
                }
                return { ok: r.ok, status: r.status, data };
            } catch (e: any) {
                lastErr = String(e?.message || e || 'Network error');
                if (attempt < retries) {
                    await new Promise((res) => setTimeout(res, retryDelayMs * Math.pow(1.5, attempt)));
                    continue;
                }
                throw e;
            }
        }
        throw new Error(lastErr || 'Request failed after retries');
    }, []);

    // Load channel registry + connected-channel snapshot on auth.
    // PR #126: surface errors instead of silently swallowing them. The prior
    // implementation set channels=[] on any non-array response, which meant
    // a 401/403/cold-start/network failure looked identical to an empty
    // registry — Casey saw "0 long-form · 0 shorts" with no diagnostic.
    const [channelLoadError, setChannelLoadError] = useState('');
    const loadChannels = useCallback(async () => {
        if (!accessToken) return;
        setChannelLoadError('');
        try {
            const tok = await getFreshToken();
            const [chRes, connRes] = await Promise.all([
                fetchJsonResilient(`${API}/api/long-form/channels`, {
                    headers: { Authorization: `Bearer ${tok}` },
                }),
                fetchJsonResilient(`${API}/api/long-form/connected-channels`, {
                    headers: { Authorization: `Bearer ${tok}` },
                }),
            ]);
            if (chRes.ok && Array.isArray(chRes.data?.channels)) {
                setChannels(chRes.data.channels);
            } else if (chRes.status === 401) {
                setChannelLoadError(
                    'Authentication failed loading channels. Sign out and back in.'
                );
            } else if (chRes.status === 403) {
                setChannelLoadError(
                    'Long-Form is admin-only. The signed-in account is not an admin.'
                );
            } else {
                setChannelLoadError(
                    `Could not load channel registry (${chRes.status}): `
                    + (chRes.data?.detail || 'unknown error')
                );
            }
            if (connRes.ok && Array.isArray(connRes.data?.channels)) {
                setConnected(connRes.data.channels);
            }
        } catch (e) {
            setChannelLoadError((e as Error).message || 'Network error loading channels');
        }
    }, [accessToken, getFreshToken, fetchJsonResilient]);

    useEffect(() => {
        if (accessToken) loadChannels();
    }, [accessToken, loadChannels]);

    // When a channel is picked, default targetMinutes + fetch Catalyst insights.
    const onPickChannel = useCallback((key: string) => {
        setSelectedChannel(key);
        const ch = channels.find((c) => c.key === key);
        if (ch) setTargetMinutes(ch.default_minutes);
        setOutline(null);
        setOutlineError('');
        if (!accessToken) return;
        setInsightsLoading(true);
        setInsights(null);
        setCatalystPresent(null);
        fetch('/api/long-form/catalyst-insights', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${accessToken}`,
            },
            body: JSON.stringify({ channel_key: key }),
        })
            .then((r) => r.json())
            .then((d) => {
                setInsights(d.insights || null);
                setCatalystPresent(Boolean(d.catalyst_present));
            })
            .catch(() => { setInsights(null); setCatalystPresent(false); })
            .finally(() => setInsightsLoading(false));
    }, [accessToken, channels]);

    const generateOutline = useCallback(async () => {
        if (!selectedChannel) { alert('Pick a channel first.'); return; }
        if (!topic.trim()) { alert('Enter a topic.'); return; }
        if (!accessToken) { alert('Sign in first.'); return; }
        setOutliningBusy(true);
        setOutlineError('');
        setOutline(null);
        try {
            const r = await fetch('/api/long-form/outline', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${accessToken}`,
                },
                body: JSON.stringify({
                    channel_key: selectedChannel,
                    topic: topic.trim(),
                    target_minutes: targetMinutes || undefined,
                    use_catalyst_context: useCatalystContext,
                }),
            });
            if (!r.ok) {
                const txt = await r.text().catch(() => '');
                throw new Error(`outline failed: ${r.status} ${txt.slice(0, 200)}`);
            }
            const d = await r.json();
            setOutline(d.outline as Outline);
        } catch (e) {
            setOutlineError((e as Error).message);
        } finally {
            setOutliningBusy(false);
        }
    }, [selectedChannel, topic, targetMinutes, useCatalystContext, accessToken]);

    // ── Recent Renders gallery ──────────────────────────────────────────
    const fetchRecentJobs = useCallback(async () => {
        if (!accessToken) return;
        setRecentLoading(true);
        try {
            const tok = await getFreshToken();
            const { ok, data } = await fetchJsonResilient(`${API}/api/long-form/jobs?limit=20`, {
                headers: { Authorization: `Bearer ${tok}` },
            });
            if (ok && Array.isArray(data?.jobs)) {
                setRecentJobs(data.jobs as RecentJobRow[]);
            }
        } catch {
            /* ignore — Recent panel can show empty without blocking the page */
        } finally {
            setRecentLoading(false);
        }
    }, [accessToken, getFreshToken, fetchJsonResilient]);

    useEffect(() => {
        if (accessToken) fetchRecentJobs();
    }, [accessToken, fetchRecentJobs]);

    // ── Poll loop for the active job ────────────────────────────────────
    // Mirrors ZT private's pollJobStatus: 4s interval, getFreshToken per
    // iteration so a Supabase token expiring during a multi-hour render
    // doesn't kill the loop, consecutive401 tolerance, hard cap so we
    // don't spin forever on a stuck pipeline.
    const pollJob = useCallback(async (jobId: string, maxSeconds: number = 36000) => {
        pollAbortRef.current = { cancelled: false };
        const startedAt = Date.now();
        let consecutive401 = 0;
        while (!pollAbortRef.current.cancelled) {
            if ((Date.now() - startedAt) / 1000 > maxSeconds) {
                setRenderError(`Render exceeded ${maxSeconds}s without finishing — check the backend logs.`);
                return;
            }
            try {
                const tok = await getFreshToken();
                const { ok, status, data } = await fetchJsonResilient(
                    `${API}/api/long-form/jobs/${jobId}/status`,
                    { headers: { Authorization: `Bearer ${tok}` } },
                );
                if (status === 401) {
                    consecutive401 += 1;
                    if (consecutive401 >= 5) {
                        setRenderError('Authentication expired during render — sign in again to resume.');
                        return;
                    }
                    await new Promise((res) => setTimeout(res, 4000));
                    continue;
                }
                if (!ok) {
                    // 404 or 500 — keep trying for a few iterations in case
                    // it's a transient backend issue.
                    consecutive401 = 0;
                    await new Promise((res) => setTimeout(res, 4000));
                    continue;
                }
                consecutive401 = 0;
                const live = data as RenderJobStatus;
                setJobStatus(live);
                if (live.phase === 'done') {
                    // Fetch full state for MP4 + thumbnail URLs.
                    try {
                        const stateResp = await fetchJsonResilient(
                            `${API}/api/long-form/jobs/${jobId}/state`,
                            { headers: { Authorization: `Bearer ${tok}` } },
                        );
                        if (stateResp.ok) setJobFullState(stateResp.data as JobFullState);
                    } catch { /* ignore */ }
                    // Also refresh the Recent Renders panel.
                    fetchRecentJobs();
                    return;
                }
                if (live.phase === 'failed') {
                    setRenderError(live.error || 'Render failed — check backend logs.');
                    fetchRecentJobs();
                    return;
                }
                if (live.phase === 'awaiting_approval') {
                    // Stop polling — let the gate UI take over. The user
                    // reviews stills + clicks Approve+Finalize, which kicks
                    // a new poll loop via finalizeJob().
                    fetchRecentJobs();
                    return;
                }
            } catch (e) {
                /* network blip — retry */
            }
            await new Promise((res) => setTimeout(res, 4000));
        }
    }, [getFreshToken, fetchJsonResilient, fetchRecentJobs]);

    // ── Kick a new render ───────────────────────────────────────────────
    const startRender = useCallback(async () => {
        if (!selectedChannel || !outline) {
            alert('Pick a channel + generate an outline first.');
            return;
        }
        setRenderStarting(true);
        setRenderError('');
        setJobStatus(null);
        setJobFullState(null);
        try {
            const tok = await getFreshToken();
            const { ok, status, data } = await fetchJsonResilient(`${API}/api/long-form/render-start`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${tok}` },
                body: JSON.stringify({ channel_key: selectedChannel, outline }),
            });
            if (!ok) {
                throw new Error(
                    `render-start failed (${status}): ${data?.detail || JSON.stringify(data).slice(0, 200)}`,
                );
            }
            const jobId = data?.job_id as string;
            if (!jobId) throw new Error('render-start returned no job_id');
            setActiveJobId(jobId);
            setJobStatus({ job_id: jobId, phase: 'queued', percent: 0 });
            // Refresh the Recent panel + start polling.
            fetchRecentJobs();
            pollJob(jobId, 36000);  // 10h cap (Khmer 9hr render headroom)
        } catch (e) {
            setRenderError((e as Error).message);
        } finally {
            setRenderStarting(false);
        }
    }, [selectedChannel, outline, getFreshToken, fetchJsonResilient, pollJob, fetchRecentJobs]);

    // ── Resume an in-progress / past render from the Recent panel ───────
    const resumeJob = useCallback(async (jobId: string) => {
        if (!jobId) return;
        setActiveJobId(jobId);
        setRenderError('');
        setJobStatus(null);
        setJobFullState(null);
        try {
            const tok = await getFreshToken();
            const { ok, data } = await fetchJsonResilient(
                `${API}/api/long-form/jobs/${jobId}/state`,
                { headers: { Authorization: `Bearer ${tok}` } },
            );
            if (!ok) throw new Error(`resume failed: ${data?.detail || 'unknown'}`);
            const state = data as JobFullState;
            setJobFullState(state);
            setJobStatus({
                job_id: jobId,
                phase: state.phase || 'unknown',
                percent: state.percent || 0,
                error: state.error,
            });
            // Re-hydrate selected channel + outline so the user can edit
            // the outline + kick a follow-up render off the same plan.
            if (state.channel_key) setSelectedChannel(state.channel_key);
            if (state.outline) setOutline(state.outline);
            setTab('render');

            // PR #129: if the job was running but its asyncio task got
            // killed (Fly redeploy / process restart / cancel), the disk
            // state shows phase=scene_assembly but no work is happening.
            // Auto-re-kick finalize so Casey doesn't have to click an
            // extra Resume Finalize button. The backend's allow-list
            // includes scene_assembly / cancelled / failed (PR #128) and
            // every per-helper is idempotent (existing files reused).
            const stuckPhases = [
                'scene_assembly', 'narration', 'ambient', 'thumbnails',
                'compose', 'cancelled', 'failed', 'finalizing', 'starting',
            ];
            if (state.phase && stuckPhases.includes(state.phase)) {
                try {
                    const finalizeResp = await fetchJsonResilient(
                        `${API}/api/long-form/jobs/${jobId}/finalize`,
                        {
                            method: 'POST',
                            headers: { Authorization: `Bearer ${tok}` },
                        },
                    );
                    if (finalizeResp.ok) {
                        setJobStatus({
                            job_id: jobId,
                            phase: 'finalizing',
                            percent: state.percent || 73,
                        });
                    }
                } catch {
                    /* fall through to plain poll */
                }
                pollJob(jobId, 36000);
                return;
            }
            // Other live phases (chapters / scenes) — just poll.
            if (
                state.phase
                && state.phase !== 'done'
                && state.phase !== 'failed'
                && state.phase !== 'awaiting_approval'
            ) {
                pollJob(jobId, 36000);
            }
        } catch (e) {
            setRenderError((e as Error).message);
        }
    }, [getFreshToken, fetchJsonResilient, pollJob]);

    // ── Per-scene approval gate (PR #127) ───────────────────────────────
    const [scenes, setScenes] = useState<SceneGridRow[]>([]);
    const [scenesLoading, setScenesLoading] = useState(false);
    const [regeneratingIdx, setRegeneratingIdx] = useState<number | null>(null);
    const [finalizingBusy, setFinalizingBusy] = useState(false);

    const loadScenes = useCallback(async (jobId: string) => {
        if (!jobId) return;
        setScenesLoading(true);
        try {
            const tok = await getFreshToken();
            const { ok, data } = await fetchJsonResilient(
                `${API}/api/long-form/jobs/${jobId}/scenes`,
                { headers: { Authorization: `Bearer ${tok}` } },
            );
            if (ok && Array.isArray(data?.scenes)) {
                setScenes(data.scenes as SceneGridRow[]);
            } else {
                setScenes([]);
            }
        } catch {
            setScenes([]);
        } finally {
            setScenesLoading(false);
        }
    }, [getFreshToken, fetchJsonResilient]);

    // Auto-load the scene grid as soon as we hit awaiting_approval.
    useEffect(() => {
        if (activeJobId && jobStatus?.phase === 'awaiting_approval') {
            loadScenes(activeJobId);
        }
    }, [activeJobId, jobStatus?.phase, loadScenes]);

    const regenerateScene = useCallback(async (
        sceneIdx: number,
        newPrompt?: string,
    ) => {
        if (!activeJobId) return;
        setRegeneratingIdx(sceneIdx);
        try {
            const tok = await getFreshToken();
            const { ok, data } = await fetchJsonResilient(
                `${API}/api/long-form/jobs/${activeJobId}/regenerate-scene`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        Authorization: `Bearer ${tok}`,
                    },
                    body: JSON.stringify({
                        scene_idx: sceneIdx,
                        new_prompt: newPrompt || undefined,
                    }),
                },
                { retries: 1 },
            );
            if (!ok) throw new Error(data?.detail || 'regenerate failed');
            // Reload the scene row with the new still URL (cache-busted).
            await loadScenes(activeJobId);
        } catch (e) {
            alert((e as Error).message);
        } finally {
            setRegeneratingIdx(null);
        }
    }, [activeJobId, getFreshToken, fetchJsonResilient, loadScenes]);

    const finalizeJob = useCallback(async () => {
        if (!activeJobId) return;
        setFinalizingBusy(true);
        setRenderError('');
        try {
            const tok = await getFreshToken();
            const { ok, data } = await fetchJsonResilient(
                `${API}/api/long-form/jobs/${activeJobId}/finalize`,
                {
                    method: 'POST',
                    headers: { Authorization: `Bearer ${tok}` },
                },
            );
            if (!ok) throw new Error(data?.detail || 'finalize failed');
            // Backend has kicked the finalize background task. Resume polling.
            setJobStatus({
                job_id: activeJobId,
                phase: 'finalizing',
                percent: 46,
            });
            pollJob(activeJobId, 36000);
        } catch (e) {
            setRenderError((e as Error).message);
        } finally {
            setFinalizingBusy(false);
        }
    }, [activeJobId, getFreshToken, fetchJsonResilient, pollJob]);

    // PR #128: cancel an in-flight render. Cooperative — backend cancels
    // the asyncio task at the next per-scene boundary. Already-rendered
    // assets stay on disk so user can resume / re-finalize later.
    const [cancellingBusy, setCancellingBusy] = useState(false);
    const cancelJob = useCallback(async () => {
        if (!activeJobId) return;
        if (!confirm('Cancel this render? Already-rendered scenes stay on disk so you can resume later.')) {
            return;
        }
        setCancellingBusy(true);
        try {
            const tok = await getFreshToken();
            const { ok, data } = await fetchJsonResilient(
                `${API}/api/long-form/jobs/${activeJobId}/cancel`,
                {
                    method: 'POST',
                    headers: { Authorization: `Bearer ${tok}` },
                },
            );
            if (!ok) throw new Error(data?.detail || 'cancel failed');
            pollAbortRef.current.cancelled = true;
            setJobStatus({
                job_id: activeJobId,
                phase: 'cancelled',
                percent: jobStatus?.percent || 0,
                error: 'cancelled by user',
            });
            fetchRecentJobs();
        } catch (e) {
            setRenderError((e as Error).message);
        } finally {
            setCancellingBusy(false);
        }
    }, [activeJobId, getFreshToken, fetchJsonResilient, fetchRecentJobs, jobStatus?.percent]);

    // Stop polling on unmount so the loop doesn't keep running after the
    // user navigates away. The pollAbortRef guards against late setState.
    useEffect(() => {
        return () => { pollAbortRef.current.cancelled = true; };
    }, []);

    const longFormChannels = channels.filter((c) => (c.format || 'long_form') === 'long_form');
    const shortsChannels = channels.filter((c) => c.format === 'shorts');

    return (
        <div className="flex flex-col gap-6 px-6 py-8 max-w-5xl mx-auto">
            <header className="flex items-center justify-between">
                <h1 className="text-2xl font-bold text-white">Long-Form</h1>
                <div className="flex items-center gap-3 text-xs text-zinc-400">
                    <span>
                        {longFormChannels.length} long-form · {shortsChannels.length} shorts · {connected.length} OAuth-connected · Catalyst-fed
                    </span>
                    <button
                        onClick={loadChannels}
                        className="text-violet-400 hover:text-violet-300 flex items-center gap-1"
                        title="Reload channel registry"
                    >
                        <RefreshCw className="h-3 w-3" /> Refresh
                    </button>
                </div>
            </header>

            {channelLoadError && (
                <div className="rounded-md bg-rose-500/10 border border-rose-500/30 px-3 py-2 text-sm text-rose-200 flex items-start gap-2">
                    <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                    <div className="flex-1">
                        <div className="font-semibold">Channels failed to load</div>
                        <div className="text-xs text-rose-300/80 mt-0.5">{channelLoadError}</div>
                    </div>
                </div>
            )}

            <TabRow tab={tab} setTab={setTab} />

            {tab === 'channel' && (
                <ChannelTab
                    channels={channels}
                    longFormChannels={longFormChannels}
                    shortsChannels={shortsChannels}
                    connected={connected}
                    selectedChannel={selectedChannel}
                    onPickChannel={onPickChannel}
                    insights={insights}
                    insightsLoading={insightsLoading}
                    catalystPresent={catalystPresent}
                    onContinue={() => setTab('outline')}
                />
            )}
            {tab === 'outline' && (
                <OutlineTab
                    selectedChannel={selectedChannel}
                    channels={channels}
                    topic={topic}
                    setTopic={setTopic}
                    targetMinutes={targetMinutes}
                    setTargetMinutes={setTargetMinutes}
                    useCatalystContext={useCatalystContext}
                    setUseCatalystContext={setUseCatalystContext}
                    insights={insights}
                    onGenerate={generateOutline}
                    outline={outline}
                    setOutline={setOutline}
                    busy={outliningBusy}
                    error={outlineError}
                    onContinue={() => setTab('render')}
                />
            )}
            {tab === 'render' && (
                <RenderTab
                    selectedChannel={selectedChannel}
                    channels={channels}
                    outline={outline}
                    activeJobId={activeJobId}
                    jobStatus={jobStatus}
                    jobFullState={jobFullState}
                    renderError={renderError}
                    renderStarting={renderStarting}
                    recentJobs={recentJobs}
                    recentLoading={recentLoading}
                    onStart={startRender}
                    onResume={resumeJob}
                    onRefreshRecent={fetchRecentJobs}
                    scenes={scenes}
                    scenesLoading={scenesLoading}
                    regeneratingIdx={regeneratingIdx}
                    finalizingBusy={finalizingBusy}
                    onRegenerateScene={regenerateScene}
                    onFinalize={finalizeJob}
                    onReloadScenes={loadScenes}
                    cancellingBusy={cancellingBusy}
                    onCancel={cancelJob}
                />
            )}
        </div>
    );
}

function TabRow({ tab, setTab }: { tab: LongFormTab; setTab: (t: LongFormTab) => void }) {
    const tabs: { id: LongFormTab; label: string; icon: typeof Wand2 }[] = [
        { id: 'channel', label: 'Channel', icon: Sparkles },
        { id: 'outline', label: 'Outline', icon: FileText },
        { id: 'render', label: 'Render', icon: Film },
    ];
    return (
        <div className="flex gap-1 border-b border-zinc-800">
            {tabs.map(({ id, label, icon: Icon }) => (
                <button
                    key={id}
                    onClick={() => setTab(id)}
                    className={`flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                        tab === id
                            ? 'border-violet-500 text-violet-300'
                            : 'border-transparent text-zinc-500 hover:text-zinc-300'
                    }`}
                >
                    <Icon className="h-4 w-4" />
                    {label}
                </button>
            ))}
        </div>
    );
}

function ChannelTab({
    channels, longFormChannels, shortsChannels, connected,
    selectedChannel, onPickChannel, insights, insightsLoading,
    catalystPresent, onContinue,
}: {
    channels: ChannelInfo[];
    longFormChannels: ChannelInfo[];
    shortsChannels: ChannelInfo[];
    connected: ConnectedChannel[];
    selectedChannel: string;
    onPickChannel: (key: string) => void;
    insights: CatalystInsights | null;
    insightsLoading: boolean;
    catalystPresent: boolean | null;
    onContinue: () => void;
}) {
    const channel = channels.find((c) => c.key === selectedChannel) || null;
    // Map registry-key → connected-channel record so we can show OAuth +
    // harvest status badges on each card.
    const connectedByKey = new Map<string, ConnectedChannel>();
    for (const c of connected) {
        if (c.registry_key) connectedByKey.set(c.registry_key, c);
    }
    const renderCard = (c: ChannelInfo) => {
        const Icon = CHANNEL_ICON[c.key] || Sparkles;
        const isSelected = c.key === selectedChannel;
        const conn = connectedByKey.get(c.key);
        const hasOAuth = !!conn;
        const hasHarvest = !!(conn && conn.harvest_present);
        return (
            <button
                key={c.key}
                onClick={() => onPickChannel(c.key)}
                className={`text-left rounded-md border px-4 py-3 transition-colors ${
                    isSelected
                        ? 'border-violet-500/60 bg-violet-500/10'
                        : 'border-zinc-800 bg-zinc-950 hover:border-zinc-700'
                }`}
            >
                <div className="flex items-start justify-between gap-2 mb-1">
                    <div className="flex items-center gap-2">
                        <Icon className="h-5 w-5 text-violet-300" />
                        <span className="text-sm font-semibold text-white">{c.label}</span>
                    </div>
                    <span className="text-xs text-zinc-500">{c.icon}</span>
                </div>
                <div className="text-xs text-zinc-400 mb-2">{c.tagline}</div>
                <div className="flex items-center gap-2 text-[10px] text-zinc-500 flex-wrap">
                    <span className="flex items-center gap-1">
                        <Clock className="h-3 w-3" />
                        {c.default_minutes >= 60
                            ? `${(c.default_minutes / 60).toFixed(1)}h`
                            : `${c.default_minutes}m`}
                    </span>
                    <span>{c.fps}fps</span>
                    <span>~${c.cost_estimate_usd}</span>
                    {hasHarvest && (
                        <span className="rounded bg-emerald-500/10 border border-emerald-500/30 px-1.5 py-0.5 text-emerald-300">
                            Catalyst ✓
                        </span>
                    )}
                    {hasOAuth && !hasHarvest && (
                        <span className="rounded bg-amber-500/10 border border-amber-500/30 px-1.5 py-0.5 text-amber-300">
                            harvest pending
                        </span>
                    )}
                    {!hasOAuth && (
                        <span className="rounded bg-zinc-800 border border-zinc-700 px-1.5 py-0.5 text-zinc-500">
                            no OAuth
                        </span>
                    )}
                </div>
            </button>
        );
    };

    return (
        <section className="flex flex-col gap-6">
            <div>
                <h2 className="text-lg font-semibold text-white mb-1">Long-form channels</h2>
                <p className="text-sm text-zinc-400 mb-4">
                    Channels with locked long-form grammar (visual style, voice,
                    target length, decoded title-format winners). Catalyst Hub data feeds
                    the outline generator on the next tab. Channels marked{' '}
                    <span className="text-amber-300">harvest pending</span> were just
                    OAuth'd — Catalyst auto-tick will fill them within a minute.
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                    {longFormChannels.map(renderCard)}
                </div>
            </div>

            {shortsChannels.length > 0 && (
                <div>
                    <h2 className="text-lg font-semibold text-white mb-1">
                        Shorts channels (analytics only)
                    </h2>
                    <p className="text-sm text-zinc-400 mb-4">
                        These render via the Skeleton AI pipeline in the Create tab — listed
                        here so Catalyst data for every connected channel surfaces in one place.
                    </p>
                    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                        {shortsChannels.map(renderCard)}
                    </div>
                </div>
            )}

            {selectedChannel && (
                <div>
                    <h3 className="text-sm font-semibold text-white mb-2">
                        Catalyst signals for {channel?.label}
                    </h3>
                    {insightsLoading ? (
                        <div className="rounded-md bg-zinc-950 border border-zinc-800 px-4 py-6 flex items-center justify-center gap-2 text-sm text-zinc-500">
                            <Loader2 className="h-4 w-4 animate-spin" /> Fetching Catalyst data…
                        </div>
                    ) : catalystPresent === false ? (
                        <div className="rounded-md bg-amber-500/10 border border-amber-500/30 px-4 py-3 text-sm text-amber-200">
                            No Catalyst harvest yet for this channel. Connect it on the
                            Catalyst tab and wait for the first refresh — outlines will
                            still generate, just without channel-specific bias.
                        </div>
                    ) : insights ? (
                        <CatalystSignalCards insights={insights} />
                    ) : null}
                </div>
            )}

            {selectedChannel && (
                <div className="flex justify-end">
                    <button
                        onClick={onContinue}
                        className="rounded-md bg-violet-500 hover:bg-violet-600 px-4 py-2 text-sm font-semibold text-white"
                    >
                        Continue to Outline →
                    </button>
                </div>
            )}
        </section>
    );
}

function CatalystSignalCards({ insights }: { insights: CatalystInsights }) {
    const totalSignals = (insights.top_titles?.length || 0)
        + (insights.breakout_titles?.length || 0)
        + (insights.hook_patterns?.length || 0)
        + (insights.thumbnail_signals?.length || 0);
    if (totalSignals === 0) {
        return (
            <div className="rounded-md bg-zinc-950 border border-zinc-800 px-4 py-3 text-sm text-zinc-500">
                Catalyst is connected but the harvest hasn't surfaced top performers,
                breakouts, or hook formulas yet. Run a refresh on the Catalyst tab.
            </div>
        );
    }
    return (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {insights.top_titles?.length > 0 && (
                <SignalCard
                    title="Top performing titles"
                    icon={<Eye className="h-4 w-4" />}
                    items={insights.top_titles.slice(0, 5).map((t) => ({
                        primary: t.title,
                        secondary: `${t.views.toLocaleString()} views`,
                    }))}
                />
            )}
            {insights.breakout_titles?.length > 0 && (
                <SignalCard
                    title="Breakouts (high lift)"
                    icon={<TrendingUp className="h-4 w-4" />}
                    items={insights.breakout_titles.slice(0, 5).map((t) => ({
                        primary: t.title,
                        secondary: `${t.lift_vs_baseline.toFixed(1)}× baseline`,
                    }))}
                />
            )}
            {insights.hook_patterns?.length > 0 && (
                <SignalCard
                    title="Hook formulas that work"
                    icon={<Sparkles className="h-4 w-4" />}
                    items={insights.hook_patterns.slice(0, 6).map((h) => ({
                        primary: h, secondary: '',
                    }))}
                />
            )}
            {insights.thumbnail_signals?.length > 0 && (
                <SignalCard
                    title="Thumbnail patterns"
                    icon={<Music className="h-4 w-4" />}
                    items={insights.thumbnail_signals.slice(0, 4).map((t) => ({
                        primary: t, secondary: '',
                    }))}
                />
            )}
        </div>
    );
}

function SignalCard({
    title, icon, items,
}: {
    title: string;
    icon: React.ReactNode;
    items: { primary: string; secondary: string }[];
}) {
    return (
        <div className="rounded-md border border-zinc-800 bg-zinc-950 p-3">
            <div className="flex items-center gap-2 text-xs font-semibold text-zinc-300 mb-2">
                {icon} {title}
            </div>
            <ul className="space-y-1.5">
                {items.map((it, i) => (
                    <li key={i} className="text-xs text-zinc-400">
                        <div className="text-zinc-200 truncate" title={it.primary}>{it.primary}</div>
                        {it.secondary && <div className="text-[10px] text-zinc-500">{it.secondary}</div>}
                    </li>
                ))}
            </ul>
        </div>
    );
}

function OutlineTab({
    selectedChannel, channels, topic, setTopic, targetMinutes, setTargetMinutes,
    useCatalystContext, setUseCatalystContext, insights, onGenerate, outline,
    setOutline, busy, error, onContinue,
}: {
    selectedChannel: string;
    channels: ChannelInfo[];
    topic: string;
    setTopic: (s: string) => void;
    targetMinutes: number | '';
    setTargetMinutes: (n: number | '') => void;
    useCatalystContext: boolean;
    setUseCatalystContext: (b: boolean) => void;
    insights: CatalystInsights | null;
    onGenerate: () => void;
    outline: Outline | null;
    setOutline: (o: Outline | null) => void;
    busy: boolean;
    error: string;
    onContinue: () => void;
}) {
    const channel = channels.find((c) => c.key === selectedChannel);
    if (!channel) {
        return (
            <div className="rounded-md bg-amber-500/10 border border-amber-500/30 px-3 py-2 text-sm text-amber-200">
                Pick a channel on the Channel tab first.
            </div>
        );
    }
    const suggestionTitles: { title: string }[] = [
        ...((insights?.top_titles || []).map((t) => ({ title: t.title }))),
        ...((insights?.breakout_titles || []).map((t) => ({ title: t.title }))),
    ].slice(0, 6);

    return (
        <section className="flex flex-col gap-4">
            <div>
                <h2 className="text-lg font-semibold text-white mb-1">
                    Topic for {channel.label}
                </h2>
                <p className="text-xs text-zinc-500">
                    {channel.tagline}. Default length: {channel.default_minutes >= 60
                        ? `${(channel.default_minutes / 60).toFixed(1)}h`
                        : `${channel.default_minutes}m`}
                </p>
            </div>

            {channel.title_template && (
                <TitleTemplateCard channel={channel} />
            )}

            <div className="flex flex-col gap-2">
                <label className="text-xs font-semibold text-zinc-300">Topic</label>
                <input
                    type="text"
                    value={topic}
                    onChange={(e) => setTopic(e.target.value)}
                    placeholder={`e.g. "${channel.label === 'We Are Lacuna' ? 'The Dyatlov Pass mystery' : channel.label === 'Empire Magnates' ? 'How Sanjay Shah legally stole $1.6B' : channel.label === 'History Rewind' ? 'The Khmer Empire / Angkor' : 'Pick a topic for this channel'}"`}
                    className="rounded-md bg-zinc-950 border border-zinc-800 px-3 py-2 text-sm text-white placeholder-zinc-600 focus:border-violet-500 outline-none"
                />
            </div>

            {suggestionTitles.length > 0 && (
                <div>
                    <div className="text-xs font-semibold text-zinc-300 mb-2">
                        Or seed from a top performer on this channel:
                    </div>
                    <div className="flex flex-wrap gap-2">
                        {suggestionTitles.map((t, i) => (
                            <button
                                key={i}
                                onClick={() => setTopic(t.title)}
                                className="rounded-full bg-violet-500/10 border border-violet-500/30 px-3 py-1 text-xs text-violet-200 hover:bg-violet-500/20 max-w-[300px] truncate"
                                title={t.title}
                            >
                                {t.title}
                            </button>
                        ))}
                    </div>
                </div>
            )}

            <div className="flex items-center gap-4">
                <div className="flex flex-col gap-1">
                    <label className="text-xs font-semibold text-zinc-300">Target minutes</label>
                    <input
                        type="number"
                        min={1}
                        max={600}
                        value={targetMinutes}
                        onChange={(e) => {
                            const v = e.target.value;
                            setTargetMinutes(v === '' ? '' : Number(v));
                        }}
                        className="rounded-md bg-zinc-950 border border-zinc-800 px-3 py-1.5 text-sm text-white w-24"
                    />
                </div>
                <label className="flex items-center gap-2 mt-5 text-xs text-zinc-300 cursor-pointer">
                    <input
                        type="checkbox"
                        checked={useCatalystContext}
                        onChange={(e) => setUseCatalystContext(e.target.checked)}
                        className="rounded border-zinc-700 bg-zinc-950 text-violet-500 focus:ring-violet-500"
                    />
                    Bias outline with Catalyst signals
                </label>
            </div>

            <button
                onClick={onGenerate}
                disabled={busy || !topic.trim()}
                className="rounded-md bg-violet-500 hover:bg-violet-600 disabled:bg-zinc-800 disabled:text-zinc-500 disabled:cursor-not-allowed px-4 py-2.5 text-sm font-semibold text-white flex items-center justify-center gap-2"
            >
                {busy ? (
                    <><Loader2 className="h-4 w-4 animate-spin" /> Generating outline…</>
                ) : (
                    <><Wand2 className="h-4 w-4" /> Generate outline w/ AI</>
                )}
            </button>

            {error && (
                <div className="rounded-md bg-rose-500/10 border border-rose-500/30 px-3 py-2 text-sm text-rose-200">
                    {error}
                </div>
            )}

            {outline && (
                <OutlineEditor outline={outline} setOutline={setOutline} onContinue={onContinue} />
            )}
        </section>
    );
}

function TitleTemplateCard({ channel }: { channel: ChannelInfo }) {
    return (
        <div className="rounded-md border border-violet-500/30 bg-violet-500/5 p-3 flex flex-col gap-2">
            <div className="flex items-center gap-2 text-xs font-semibold text-violet-200">
                <Sparkles className="h-3.5 w-3.5" />
                Title format locked (decoded winner pattern)
            </div>
            <div className="text-xs text-zinc-300">
                <span className="text-zinc-500">Pattern:</span>{' '}
                <code className="text-violet-200 bg-zinc-900/60 px-1.5 py-0.5 rounded">
                    {channel.title_template}
                </code>
            </div>
            {channel.title_examples && channel.title_examples.length > 0 && (
                <div className="text-xs text-zinc-400">
                    <span className="text-zinc-500">Examples:</span>
                    <ul className="list-disc list-inside mt-1 space-y-0.5 text-zinc-300">
                        {channel.title_examples.slice(0, 3).map((ex, i) => (
                            <li key={i} className="truncate" title={ex}>{ex}</li>
                        ))}
                    </ul>
                </div>
            )}
            {channel.title_avoid && channel.title_avoid.length > 0 && (
                <div className="text-[10px] text-rose-300/80">
                    <span className="text-rose-300/60">Avoid:</span>{' '}
                    {channel.title_avoid.slice(0, 6).map((a, i, arr) => (
                        <span key={i}>
                            <span className="line-through">{a}</span>
                            {i < arr.length - 1 ? ', ' : ''}
                        </span>
                    ))}
                </div>
            )}
        </div>
    );
}

function OutlineEditor({
    outline, setOutline, onContinue,
}: {
    outline: Outline;
    setOutline: (o: Outline | null) => void;
    onContinue: () => void;
}) {
    return (
        <div className="rounded-md border border-zinc-800 bg-zinc-950 p-4 flex flex-col gap-3 mt-2">
            <input
                type="text"
                value={outline.title}
                onChange={(e) => setOutline({ ...outline, title: e.target.value })}
                className="bg-transparent text-base font-bold text-white border-b border-zinc-800 pb-1 focus:outline-none focus:border-violet-500"
            />
            <textarea
                value={outline.hook}
                onChange={(e) => setOutline({ ...outline, hook: e.target.value })}
                placeholder="Cold-open hook…"
                rows={2}
                className="bg-zinc-900 rounded-md text-sm text-zinc-200 px-3 py-2 focus:outline-none focus:border-violet-500 border border-zinc-800"
            />
            {outline.description !== undefined && (
                <div className="flex flex-col gap-1">
                    <label className="text-[10px] font-semibold text-zinc-500 uppercase tracking-wide">
                        YouTube description (channel CTR tail auto-appended)
                    </label>
                    <textarea
                        value={outline.description || ''}
                        onChange={(e) => setOutline({ ...outline, description: e.target.value })}
                        placeholder="YouTube description will auto-fill on generate…"
                        rows={4}
                        className="bg-zinc-900 rounded-md text-xs text-zinc-300 px-3 py-2 focus:outline-none focus:border-violet-500 border border-zinc-800 font-mono"
                    />
                </div>
            )}
            <div className="flex flex-col gap-2">
                {outline.chapters.map((c, i) => (
                    <div key={i} className="rounded-md bg-zinc-900 border border-zinc-800 p-3">
                        <div className="flex items-center justify-between mb-1">
                            <input
                                type="text"
                                value={c.title}
                                onChange={(e) => {
                                    const next = [...outline.chapters];
                                    next[i] = { ...c, title: e.target.value };
                                    setOutline({ ...outline, chapters: next });
                                }}
                                className="bg-transparent text-sm font-semibold text-white flex-1 focus:outline-none"
                            />
                            <input
                                type="number"
                                value={c.minutes}
                                min={1}
                                onChange={(e) => {
                                    const next = [...outline.chapters];
                                    next[i] = { ...c, minutes: Number(e.target.value) };
                                    setOutline({ ...outline, chapters: next });
                                }}
                                className="w-16 bg-zinc-950 rounded-md border border-zinc-800 px-2 py-0.5 text-xs text-zinc-300 ml-2"
                            />
                            <span className="text-[10px] text-zinc-500 ml-1">min</span>
                        </div>
                        <textarea
                            value={c.synopsis}
                            onChange={(e) => {
                                const next = [...outline.chapters];
                                next[i] = { ...c, synopsis: e.target.value };
                                setOutline({ ...outline, chapters: next });
                            }}
                            rows={2}
                            className="w-full bg-transparent text-xs text-zinc-300 mt-1 focus:outline-none resize-none"
                        />
                    </div>
                ))}
            </div>
            <div className="flex justify-end">
                <button
                    onClick={onContinue}
                    className="rounded-md bg-violet-500 hover:bg-violet-600 px-4 py-2 text-sm font-semibold text-white"
                >
                    Continue to Render →
                </button>
            </div>
        </div>
    );
}

function RenderTab({
    selectedChannel, channels, outline,
    activeJobId, jobStatus, jobFullState, renderError, renderStarting,
    recentJobs, recentLoading, onStart, onResume, onRefreshRecent,
    scenes, scenesLoading, regeneratingIdx, finalizingBusy,
    onRegenerateScene, onFinalize, onReloadScenes,
    cancellingBusy, onCancel,
}: {
    selectedChannel: string;
    channels: ChannelInfo[];
    outline: Outline | null;
    activeJobId: string;
    jobStatus: RenderJobStatus | null;
    jobFullState: JobFullState | null;
    renderError: string;
    renderStarting: boolean;
    recentJobs: RecentJobRow[];
    recentLoading: boolean;
    onStart: () => void;
    onResume: (jobId: string) => void;
    onRefreshRecent: () => void;
    scenes: SceneGridRow[];
    scenesLoading: boolean;
    regeneratingIdx: number | null;
    finalizingBusy: boolean;
    onRegenerateScene: (sceneIdx: number, newPrompt?: string) => void;
    onFinalize: () => void;
    onReloadScenes: (jobId: string) => void;
    cancellingBusy: boolean;
    onCancel: () => void;
}) {
    const channel = channels.find((c) => c.key === selectedChannel);
    const totalMinutes = outline ? outline.chapters.reduce((s, c) => s + c.minutes, 0) : 0;
    const isRunning = jobStatus
        && jobStatus.phase !== 'done'
        && jobStatus.phase !== 'failed'
        && jobStatus.phase !== 'awaiting_approval';
    const isDone = jobStatus?.phase === 'done';
    const isFailed = jobStatus?.phase === 'failed';
    const isAwaitingApproval = jobStatus?.phase === 'awaiting_approval';

    return (
        <section className="flex flex-col gap-6">
            {/* ── Confirm + start (when no active job, channel + outline picked) ── */}
            {!activeJobId && channel && outline && (
                <div className="flex flex-col gap-4">
                    <h2 className="text-lg font-semibold text-white">Confirm + Render</h2>
                    <div className="rounded-md border border-zinc-800 bg-zinc-950 p-4 flex flex-col gap-2">
                        <div className="text-sm text-zinc-300"><span className="text-zinc-500">Channel:</span> {channel.label}</div>
                        <div className="text-sm text-zinc-300"><span className="text-zinc-500">Title:</span> {outline.title}</div>
                        <div className="text-sm text-zinc-300"><span className="text-zinc-500">Chapters:</span> {outline.chapters.length}</div>
                        <div className="text-sm text-zinc-300"><span className="text-zinc-500">Length:</span> {totalMinutes >= 60 ? `${(totalMinutes / 60).toFixed(1)}h` : `${totalMinutes}m`}</div>
                        <div className="text-sm text-zinc-300"><span className="text-zinc-500">Pipeline:</span> {channel.pipeline_kind || 'sleep_doc'}</div>
                        <div className="text-sm text-zinc-300"><span className="text-zinc-500">Image model:</span> {channel.image_model_default}</div>
                        <div className="text-sm text-zinc-300"><span className="text-zinc-500">Voice:</span> {channel.voice_provider_default}</div>
                        <div className="text-sm text-zinc-300"><span className="text-zinc-500">Estimated fal cost:</span> ~${channel.cost_estimate_usd}</div>
                    </div>
                    <div className="rounded-md bg-amber-500/10 border border-amber-500/30 px-4 py-3 text-xs text-amber-200">
                        Two-stage render: <strong>Stage 1 generates stills only (~$1-3 fal)</strong> →
                        you review every scene → regenerate any off-style ones individually →
                        <strong> Stage 2 burns the rest (~${(channel.cost_estimate_usd - 3).toFixed(0)} fal)</strong>{' '}
                        on i2v + voice + SFX + compose. You can close this tab and
                        resume from the Recent Renders panel — it&apos;ll keep running.
                    </div>
                    <button
                        onClick={onStart}
                        disabled={renderStarting}
                        className="rounded-md bg-violet-500 hover:bg-violet-600 disabled:bg-zinc-800 disabled:text-zinc-500 disabled:cursor-not-allowed px-4 py-2.5 text-sm font-semibold text-white flex items-center justify-center gap-2"
                    >
                        {renderStarting ? (
                            <><Loader2 className="h-4 w-4 animate-spin" /> Starting…</>
                        ) : (
                            <><Film className="h-4 w-4" /> Generate Stills (~$1-3 fal — review before bulk)</>
                        )}
                    </button>
                    {renderError && (
                        <div className="rounded-md bg-rose-500/10 border border-rose-500/30 px-3 py-2 text-sm text-rose-200">
                            {renderError}
                        </div>
                    )}
                </div>
            )}

            {/* ── Per-scene approval gate (PR #127) ── */}
            {activeJobId && isAwaitingApproval && channel && (
                <SceneApprovalGate
                    channel={channel}
                    scenes={scenes}
                    scenesLoading={scenesLoading}
                    regeneratingIdx={regeneratingIdx}
                    finalizingBusy={finalizingBusy}
                    onRegenerateScene={onRegenerateScene}
                    onFinalize={onFinalize}
                    onReloadScenes={() => onReloadScenes(activeJobId)}
                    finalizeCostEstimate={Math.max(1, channel.cost_estimate_usd - 3)}
                    renderError={renderError}
                />
            )}

            {/* ── Active job progress / done / failed / cancelled ── */}
            {activeJobId && jobStatus && !isAwaitingApproval && (
                <ActiveJobCard
                    channel={channel || null}
                    outline={outline || null}
                    jobStatus={jobStatus}
                    jobFullState={jobFullState}
                    isRunning={!!isRunning}
                    isDone={!!isDone}
                    isFailed={!!isFailed}
                    renderError={renderError}
                    cancellingBusy={cancellingBusy}
                    onCancel={onCancel}
                />
            )}

            {/* ── Recent Renders gallery (always shown so user can resume any past job) ── */}
            <RecentRendersPanel
                recentJobs={recentJobs}
                recentLoading={recentLoading}
                onResume={onResume}
                onRefresh={onRefreshRecent}
                activeJobId={activeJobId}
            />

            {/* ── Hint when nothing is selected ── */}
            {!activeJobId && (!channel || !outline) && (
                <div className="rounded-md bg-amber-500/10 border border-amber-500/30 px-3 py-2 text-sm text-amber-200">
                    {!channel
                        ? 'Pick a channel on the Channel tab first.'
                        : 'Generate an outline on the Outline tab first — or resume a past render below.'}
                </div>
            )}
        </section>
    );
}

function SceneApprovalGate({
    channel, scenes, scenesLoading, regeneratingIdx, finalizingBusy,
    onRegenerateScene, onFinalize, onReloadScenes, finalizeCostEstimate, renderError,
}: {
    channel: ChannelInfo;
    scenes: SceneGridRow[];
    scenesLoading: boolean;
    regeneratingIdx: number | null;
    finalizingBusy: boolean;
    onRegenerateScene: (sceneIdx: number, newPrompt?: string) => void;
    onFinalize: () => void;
    onReloadScenes: () => void;
    finalizeCostEstimate: number;
    renderError: string;
}) {
    const presentCount = scenes.filter((s) => s.still_present).length;
    const allPresent = scenes.length > 0 && presentCount === scenes.length;
    return (
        <div className="flex flex-col gap-4">
            <div className="flex items-center justify-between">
                <div>
                    <h2 className="text-lg font-semibold text-white">
                        Review stills before bulk render
                    </h2>
                    <p className="text-xs text-zinc-500 mt-1">
                        {scenes.length} scenes generated.
                        Click Regenerate on any that are off-style. When all
                        look right, hit Approve + Finalize to spend the rest
                        of the fal budget on motion + voice + SFX + compose.
                    </p>
                </div>
                <button
                    onClick={onReloadScenes}
                    className="text-xs text-violet-400 hover:text-violet-300 flex items-center gap-1"
                    disabled={scenesLoading}
                >
                    <RefreshCw className={`h-3 w-3 ${scenesLoading ? 'animate-spin' : ''}`} />
                    Refresh
                </button>
            </div>

            {scenesLoading && scenes.length === 0 ? (
                <div className="rounded-md bg-zinc-950 border border-zinc-800 px-4 py-12 flex items-center justify-center gap-2 text-sm text-zinc-500">
                    <Loader2 className="h-4 w-4 animate-spin" /> Loading scene grid…
                </div>
            ) : (
                <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
                    {scenes.map((s) => (
                        <SceneTile
                            key={s.scene_idx}
                            scene={s}
                            regenerating={regeneratingIdx === s.scene_idx}
                            onRegenerate={onRegenerateScene}
                        />
                    ))}
                </div>
            )}

            {renderError && (
                <div className="rounded-md bg-rose-500/10 border border-rose-500/30 px-3 py-2 text-sm text-rose-200">
                    {renderError}
                </div>
            )}

            <div className="rounded-md bg-violet-500/10 border border-violet-500/30 px-4 py-3 text-xs text-violet-200">
                <strong>Stage 2 will burn ~${finalizeCostEstimate} fal</strong>{' '}
                on{' '}
                {channel.pipeline_kind === 'v5_episode'
                    ? 'LTX i2v animation, ElevenLabs narration, mmaudio SFX, 2-pass loudnorm, scene mux + concat with fade-out.'
                    : 'fal MiniMax narration, mmaudio ambient bed, thumbnails, ffmpeg ken-burns + 2-pass loudnorm + final compose.'}{' '}
                {presentCount}/{scenes.length} stills ready.
            </div>

            <button
                onClick={onFinalize}
                disabled={finalizingBusy || !allPresent}
                className="rounded-md bg-violet-500 hover:bg-violet-600 disabled:bg-zinc-800 disabled:text-zinc-500 disabled:cursor-not-allowed px-4 py-2.5 text-sm font-semibold text-white flex items-center justify-center gap-2"
            >
                {finalizingBusy ? (
                    <><Loader2 className="h-4 w-4 animate-spin" /> Kicking finalize…</>
                ) : !allPresent ? (
                    <>Waiting for {scenes.length - presentCount} stills…</>
                ) : (
                    <><Check className="h-4 w-4" /> Approve all + Finalize (~${finalizeCostEstimate} fal)</>
                )}
            </button>
        </div>
    );
}

function SceneTile({
    scene, regenerating, onRegenerate,
}: {
    scene: SceneGridRow;
    regenerating: boolean;
    onRegenerate: (sceneIdx: number, newPrompt?: string) => void;
}) {
    const [editing, setEditing] = useState(false);
    const [draftPrompt, setDraftPrompt] = useState(scene.scene_prompt);

    return (
        <div className="rounded-md border border-zinc-800 bg-zinc-950 overflow-hidden flex flex-col">
            <div className="aspect-video bg-zinc-900 relative">
                {scene.still_present && scene.still_url ? (
                    <img
                        src={scene.still_url}
                        alt={`scene ${scene.scene_idx}`}
                        className="w-full h-full object-cover"
                        loading="lazy"
                    />
                ) : (
                    <div className="absolute inset-0 flex items-center justify-center text-zinc-600">
                        <ImageIcon className="h-8 w-8" />
                    </div>
                )}
                {regenerating && (
                    <div className="absolute inset-0 bg-black/70 flex items-center justify-center">
                        <Loader2 className="h-6 w-6 animate-spin text-violet-400" />
                    </div>
                )}
                <div className="absolute top-1 left-1 rounded bg-zinc-900/80 px-1.5 py-0.5 text-[10px] text-zinc-300 font-mono">
                    ch {scene.chapter_index} · #{scene.scene_idx}
                </div>
            </div>
            <div className="p-2 flex flex-col gap-1.5">
                {editing ? (
                    <textarea
                        value={draftPrompt}
                        onChange={(e) => setDraftPrompt(e.target.value)}
                        rows={4}
                        className="rounded bg-zinc-900 border border-zinc-800 px-2 py-1 text-[10px] text-zinc-300 focus:outline-none focus:border-violet-500/60 font-mono"
                        autoFocus
                    />
                ) : (
                    <div
                        className="text-[10px] text-zinc-400 line-clamp-3 cursor-pointer hover:text-zinc-300"
                        title={scene.scene_prompt}
                        onClick={() => setEditing(true)}
                    >
                        {scene.scene_prompt || <span className="italic text-zinc-600">no prompt</span>}
                    </div>
                )}
                <div className="flex gap-1.5">
                    {editing ? (
                        <>
                            <button
                                onClick={() => {
                                    onRegenerate(scene.scene_idx, draftPrompt);
                                    setEditing(false);
                                }}
                                disabled={regenerating || !draftPrompt.trim()}
                                className="flex-1 rounded bg-violet-500 hover:bg-violet-600 disabled:bg-zinc-800 disabled:text-zinc-500 px-2 py-1 text-[10px] font-semibold text-white flex items-center justify-center gap-1"
                            >
                                <RefreshCw className="h-3 w-3" /> Regen w/ new prompt
                            </button>
                            <button
                                onClick={() => {
                                    setDraftPrompt(scene.scene_prompt);
                                    setEditing(false);
                                }}
                                className="rounded bg-zinc-800 hover:bg-zinc-700 px-2 py-1 text-[10px] text-zinc-300"
                            >
                                Cancel
                            </button>
                        </>
                    ) : (
                        <>
                            <button
                                onClick={() => onRegenerate(scene.scene_idx)}
                                disabled={regenerating}
                                className="flex-1 rounded bg-zinc-800 hover:bg-zinc-700 disabled:opacity-50 px-2 py-1 text-[10px] font-semibold text-zinc-200 flex items-center justify-center gap-1"
                                title="Re-render with the same prompt"
                            >
                                <RefreshCw className="h-3 w-3" /> Regen
                            </button>
                            <button
                                onClick={() => setEditing(true)}
                                disabled={regenerating}
                                className="rounded bg-zinc-800 hover:bg-zinc-700 disabled:opacity-50 px-2 py-1 text-[10px] text-zinc-300"
                                title="Edit prompt before regenerating"
                            >
                                Edit prompt
                            </button>
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}

function ActiveJobCard({
    channel, outline, jobStatus, jobFullState, isRunning, isDone, isFailed, renderError,
    cancellingBusy, onCancel,
}: {
    channel: ChannelInfo | null;
    outline: Outline | null;
    jobStatus: RenderJobStatus;
    jobFullState: JobFullState | null;
    isRunning: boolean;
    isDone: boolean;
    isFailed: boolean;
    renderError: string;
    cancellingBusy: boolean;
    onCancel: () => void;
}) {
    const phaseLabel = PHASE_LABELS[jobStatus.phase] || jobStatus.phase;
    const phaseIdx = PHASE_ORDER.indexOf(jobStatus.phase);
    const title = (jobFullState?.outline?.title) || outline?.title || '(no title)';
    const mp4Url = jobFullState?.mp4_url || '';
    const thumbs = jobFullState?.thumbnail_urls || [];
    const description = jobFullState?.outline?.description || outline?.description || '';
    const isCancelled = jobStatus.phase === 'cancelled';

    return (
        <div className="flex flex-col gap-4">
            <div className="flex items-center justify-between">
                <h2 className="text-lg font-semibold text-white">
                    {isDone ? 'Render complete'
                        : isFailed ? 'Render failed'
                        : isCancelled ? 'Render cancelled'
                        : 'Rendering…'}
                </h2>
                <div className="flex items-center gap-3">
                    {isRunning && (
                        <button
                            onClick={onCancel}
                            disabled={cancellingBusy}
                            className="rounded-md bg-rose-500/10 hover:bg-rose-500/20 border border-rose-500/40 px-3 py-1 text-xs font-semibold text-rose-200 disabled:opacity-50 flex items-center gap-1.5"
                            title="Cooperative cancel — stops at the next per-scene boundary. Already-rendered scenes stay on disk."
                        >
                            {cancellingBusy ? <Loader2 className="h-3 w-3 animate-spin" /> : null}
                            Stop render
                        </button>
                    )}
                    <div className="text-xs text-zinc-500">
                        job {jobStatus.job_id}
                    </div>
                </div>
            </div>

            <div className="rounded-md border border-zinc-800 bg-zinc-950 p-4 flex flex-col gap-3">
                <div className="text-sm text-white font-semibold">{title}</div>
                {channel && (
                    <div className="text-xs text-zinc-400">
                        {channel.label} · {channel.pipeline_kind || 'sleep_doc'} pipeline · ~${channel.cost_estimate_usd} fal
                    </div>
                )}

                {/* Progress bar */}
                <div className="flex flex-col gap-1.5">
                    <div className="flex items-center justify-between text-xs">
                        <span className="text-zinc-300 flex items-center gap-1.5">
                            {isRunning && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                            {isDone && <Check className="h-3.5 w-3.5 text-emerald-400" />}
                            {isFailed && <AlertTriangle className="h-3.5 w-3.5 text-rose-400" />}
                            {phaseLabel}
                        </span>
                        <span className="text-zinc-500">{jobStatus.percent}%</span>
                    </div>
                    <div className="h-1.5 rounded-full bg-zinc-800 overflow-hidden">
                        <div
                            className={`h-full transition-all duration-500 ${
                                isFailed ? 'bg-rose-500' : isDone ? 'bg-emerald-500' : 'bg-violet-500'
                            }`}
                            style={{ width: `${Math.max(2, jobStatus.percent)}%` }}
                        />
                    </div>
                    {/* Per-phase counters */}
                    {jobStatus.phase === 'chapters' && jobStatus.chapter_total ? (
                        <div className="text-[10px] text-zinc-500">
                            chapter {jobStatus.chapter_done}/{jobStatus.chapter_total}
                        </div>
                    ) : jobStatus.phase === 'scenes' && jobStatus.scene_total ? (
                        <div className="text-[10px] text-zinc-500">
                            scene {jobStatus.scene_done}/{jobStatus.scene_total}
                        </div>
                    ) : jobStatus.phase === 'narration' && jobStatus.narration_total ? (
                        <div className="text-[10px] text-zinc-500">
                            chapter {jobStatus.narration_done}/{jobStatus.narration_total} narrated
                        </div>
                    ) : null}
                </div>

                {/* Phase ladder */}
                <div className="flex items-center justify-between text-[10px] text-zinc-600">
                    {PHASE_ORDER.map((p, i) => (
                        <span
                            key={p}
                            className={`flex-1 text-center px-1 ${
                                i < phaseIdx ? 'text-emerald-400' : i === phaseIdx ? 'text-violet-300 font-semibold' : ''
                            }`}
                        >
                            {p}
                        </span>
                    ))}
                </div>

                {/* Error if failed */}
                {(isFailed || renderError) && (
                    <div className="rounded-md bg-rose-500/10 border border-rose-500/30 px-3 py-2 text-xs text-rose-200">
                        {jobStatus.error || renderError}
                    </div>
                )}

                {/* MP4 + downloads when done */}
                {isDone && mp4Url && (
                    <div className="flex flex-col gap-3">
                        <video
                            src={mp4Url}
                            controls
                            playsInline
                            className="w-full rounded-md bg-black"
                            style={{ maxHeight: 480 }}
                        />
                        <div className="grid grid-cols-2 gap-2 text-xs">
                            <div className="rounded-md bg-zinc-900 border border-zinc-800 px-3 py-2">
                                <div className="text-zinc-500 text-[10px] uppercase tracking-wide">Duration</div>
                                <div className="text-zinc-200 font-mono">
                                    {formatDuration(jobFullState?.mp4_duration_sec || 0)}
                                </div>
                            </div>
                            <div className="rounded-md bg-zinc-900 border border-zinc-800 px-3 py-2">
                                <div className="text-zinc-500 text-[10px] uppercase tracking-wide">File size</div>
                                <div className="text-zinc-200 font-mono">
                                    {formatBytes(jobFullState?.mp4_size_bytes || 0)}
                                </div>
                            </div>
                        </div>
                        <div className="flex gap-2">
                            <a
                                href={mp4Url}
                                download
                                className="flex-1 rounded-md bg-violet-500 hover:bg-violet-600 px-3 py-2 text-xs font-semibold text-white flex items-center justify-center gap-1.5"
                            >
                                <Download className="h-3.5 w-3.5" /> Download MP4
                            </a>
                            <a
                                href={mp4Url}
                                target="_blank"
                                rel="noreferrer"
                                className="rounded-md bg-zinc-800 hover:bg-zinc-700 px-3 py-2 text-xs font-semibold text-zinc-200 flex items-center justify-center gap-1.5"
                            >
                                <Play className="h-3.5 w-3.5" /> Open in tab
                            </a>
                        </div>
                    </div>
                )}

                {/* Thumbnails when generated (visible during/after) */}
                {thumbs.length > 0 && (
                    <div className="flex flex-col gap-2">
                        <div className="text-[10px] font-semibold text-zinc-500 uppercase tracking-wide">
                            Thumbnail candidates
                        </div>
                        <div className="grid grid-cols-3 gap-2">
                            {thumbs.map((url, i) => (
                                <a
                                    key={i}
                                    href={url}
                                    target="_blank"
                                    rel="noreferrer"
                                    className="block rounded-md overflow-hidden border border-zinc-800 hover:border-violet-500/60 transition-colors"
                                >
                                    <img
                                        src={url}
                                        alt={`thumbnail ${i + 1}`}
                                        className="w-full aspect-video object-cover bg-zinc-900"
                                        loading="lazy"
                                    />
                                </a>
                            ))}
                        </div>
                    </div>
                )}

                {/* Upload metadata when done */}
                {isDone && description && (
                    <div className="flex flex-col gap-2">
                        <div className="text-[10px] font-semibold text-zinc-500 uppercase tracking-wide flex items-center justify-between">
                            <span>YouTube description</span>
                            <button
                                onClick={() => {
                                    navigator.clipboard.writeText(description);
                                }}
                                className="text-violet-400 hover:text-violet-300 normal-case tracking-normal"
                            >
                                Copy
                            </button>
                        </div>
                        <pre className="rounded-md bg-zinc-900 border border-zinc-800 px-3 py-2 text-[11px] text-zinc-300 whitespace-pre-wrap font-mono max-h-40 overflow-y-auto">
                            {description}
                        </pre>
                    </div>
                )}
            </div>
        </div>
    );
}

function RecentRendersPanel({
    recentJobs, recentLoading, onResume, onRefresh, activeJobId,
}: {
    recentJobs: RecentJobRow[];
    recentLoading: boolean;
    onResume: (jobId: string) => void;
    onRefresh: () => void;
    activeJobId: string;
}) {
    return (
        <div className="flex flex-col gap-2">
            <div className="flex items-center justify-between">
                <h3 className="text-sm font-semibold text-white">Recent renders</h3>
                <button
                    onClick={onRefresh}
                    className="text-xs text-violet-400 hover:text-violet-300 flex items-center gap-1"
                    disabled={recentLoading}
                >
                    <RefreshCw className={`h-3 w-3 ${recentLoading ? 'animate-spin' : ''}`} /> Refresh
                </button>
            </div>
            {recentJobs.length === 0 ? (
                <div className="rounded-md bg-zinc-950 border border-zinc-800 px-4 py-6 text-center text-xs text-zinc-500">
                    {recentLoading ? 'Loading…' : 'No renders yet — pick a channel + topic and hit Render.'}
                </div>
            ) : (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                    {recentJobs.map((job) => (
                        <RecentJobCard
                            key={job.job_id}
                            job={job}
                            isActive={job.job_id === activeJobId}
                            onResume={onResume}
                        />
                    ))}
                </div>
            )}
        </div>
    );
}

function RecentJobCard({
    job, isActive, onResume,
}: {
    job: RecentJobRow;
    isActive: boolean;
    onResume: (jobId: string) => void;
}) {
    const phaseLabel = PHASE_LABELS[job.phase] || job.phase;
    const isDone = job.phase === 'done';
    const isFailed = job.phase === 'failed';
    const isRunning = !isDone && !isFailed;

    return (
        <div
            className={`rounded-md border ${
                isActive
                    ? 'border-violet-500/60 bg-violet-500/5'
                    : 'border-zinc-800 bg-zinc-950'
            } p-3 flex flex-col gap-2`}
        >
            {/* Thumbnail */}
            {job.thumbnail_url ? (
                <img
                    src={job.thumbnail_url}
                    alt=""
                    className="w-full aspect-video object-cover rounded bg-zinc-900"
                    loading="lazy"
                />
            ) : (
                <div className="w-full aspect-video rounded bg-zinc-900 border border-zinc-800 flex items-center justify-center text-zinc-700">
                    <ImageIcon className="h-8 w-8" />
                </div>
            )}

            {/* Title + channel */}
            <div className="text-xs font-semibold text-white truncate" title={job.title}>
                {job.title || '(no title)'}
            </div>
            <div className="text-[10px] text-zinc-500 flex items-center gap-1.5">
                {job.channel_label && <span>{job.channel_label}</span>}
                {job.channel_label && <span>·</span>}
                <span>{formatRelativeTime(job.created_at || job.started_at || 0)}</span>
            </div>

            {/* Status pill */}
            <div className="flex items-center gap-1.5 text-[10px]">
                {isDone && (
                    <span className="rounded bg-emerald-500/10 border border-emerald-500/30 px-1.5 py-0.5 text-emerald-300 flex items-center gap-1">
                        <Check className="h-2.5 w-2.5" /> Done · {formatDuration(job.mp4_duration_sec || 0)} · {formatBytes(job.mp4_size_bytes || 0)}
                    </span>
                )}
                {isFailed && (
                    <span className="rounded bg-rose-500/10 border border-rose-500/30 px-1.5 py-0.5 text-rose-300 flex items-center gap-1">
                        <AlertTriangle className="h-2.5 w-2.5" /> Failed
                    </span>
                )}
                {isRunning && (
                    <span className="rounded bg-violet-500/10 border border-violet-500/30 px-1.5 py-0.5 text-violet-300 flex items-center gap-1">
                        <Loader2 className="h-2.5 w-2.5 animate-spin" /> {phaseLabel} · {job.percent}%
                    </span>
                )}
            </div>

            {/* Actions */}
            <div className="flex gap-2 mt-1">
                <button
                    onClick={() => onResume(job.job_id)}
                    className="flex-1 rounded-md bg-zinc-800 hover:bg-zinc-700 px-2.5 py-1.5 text-[10px] font-semibold text-zinc-200 flex items-center justify-center gap-1"
                    title={`Resume / inspect job ${job.job_id}`}
                >
                    <RotateCcw className="h-3 w-3" /> {isRunning ? 'Resume' : 'Open'}
                </button>
                {isDone && job.mp4_url && (
                    <a
                        href={job.mp4_url}
                        download
                        className="rounded-md bg-violet-500 hover:bg-violet-600 px-2.5 py-1.5 text-[10px] font-semibold text-white flex items-center justify-center gap-1"
                    >
                        <Download className="h-3 w-3" /> MP4
                    </a>
                )}
            </div>
        </div>
    );
}
