import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { Bot, BrainCircuit, Loader2, RefreshCw, Save, Target, Youtube } from 'lucide-react';
import { API, AuthContext } from '../shared';

type CatalystChannel = {
    channel_id: string;
    title: string;
    channel_handle?: string;
    last_sync_error?: string;
    last_outcome_sync_at?: number;
    last_outcome_sync_count?: number;
    last_outcome_sync_error?: string;
    analytics_snapshot?: {
        channel_summary?: string;
        recent_upload_titles?: string[];
        uploaded_videos?: Array<{
            video_id?: string;
            title?: string;
            published_at?: string;
            thumbnail_url?: string;
            views?: number;
            average_view_percentage?: number;
            impression_click_through_rate?: number;
            duration_sec?: number;
            privacy_status?: string;
        }>;
        top_video_titles?: string[];
        packaging_learnings?: string[];
        retention_learnings?: string[];
        series_clusters?: Array<Record<string, any>>;
        channel_audit?: {
            summary?: string;
            honesty_note?: string;
            measured_facts?: string[];
            inferred_notes?: string[];
            limitations?: string[];
            strengths?: string[];
            warnings?: string[];
            next_moves?: string[];
            next_video_candidates?: string[];
            strongest_arc?: string;
            weakest_arc?: string;
            latest_failure_mode_label?: string;
            best_recent_title?: string;
            worst_recent_title?: string;
            coverage?: {
                recent_uploads?: number;
                top_videos?: number;
                series_clusters?: number;
            };
        };
        historical_compare?: {
            winner_vs_loser_summary?: string;
            measured_summary?: string;
            inference_summary?: string;
            honesty_note?: string;
            limitations?: string[];
            next_moves?: string[];
        };
    };
};

type CatalystWorkspaceSnapshot = {
    workspace_id: string;
    kind: 'shorts' | 'longform';
    memory_key: string;
    memory_public?: Record<string, any>;
    playbook?: Record<string, any>;
    selected_cluster?: Record<string, any>;
    cluster_context?: string;
    reference_summary?: string;
    reference_video_analysis?: {
        video?: {
            video_id?: string;
            title?: string;
            url?: string;
            source_kind?: string;
            source_channel?: string;
            views?: number;
            impressions?: number;
            average_view_duration_sec?: number;
            average_view_percentage?: number;
            impression_click_through_rate?: number;
            watch_time_hours?: number;
            duration_sec?: number;
        };
        frame_metrics?: Record<string, any>;
        evidence?: {
            analysis_mode?: string;
            analysis_mode_label?: string;
            confidence_label?: string;
            honesty_note?: string;
            measured_facts?: string[];
            inferred_notes?: string[];
            limitations?: string[];
            heuristic_used?: boolean;
            manual_evidence_summary?: string;
            manual_asset_count?: number;
            manual_transcript_supplied?: boolean;
        };
        analysis?: {
            summary?: string;
            honesty_note?: string;
            confidence_label?: string;
            analysis_mode_label?: string;
            measured_facts?: string[];
            inferred_notes?: string[];
            limitations?: string[];
            manual_evidence_summary?: string;
            why_it_worked?: string[];
            what_hurt_weaker_upload?: string[];
            hook_system?: string[];
            pacing_system?: string[];
            visual_system?: string[];
            sound_system?: string[];
            transition_system?: string[];
            structure_map?: string[];
            threed_translation_moves?: string[];
            title_thumbnail_rules?: string[];
            next_video_moves?: string[];
            avoid_rules?: string[];
            candidate_titles?: string[];
        };
    };
};

type CatalystLearningRow = {
    session_id: string;
    mode: string;
    format_preset: string;
    created_at: number;
    outcome_summary: string;
    selected_title: string;
    last_failure_mode_key?: string;
    last_failure_mode_label?: string;
    chapter_score_average?: number;
    preview_success_rate?: number;
    wins_to_keep?: string[];
    mistakes_to_avoid?: string[];
    next_video_moves?: string[];
};

type CatalystHubPayload = {
    ok?: boolean;
    default_channel_id?: string;
    selected_channel_id?: string;
    selected_channel?: CatalystChannel;
    channels?: CatalystChannel[];
    workspace_snapshots?: Record<string, CatalystWorkspaceSnapshot>;
    recent_learning?: CatalystLearningRow[];
    default_workspace_id?: string;
    generated_at?: number;
};

const WORKSPACE_LABELS: Record<string, string> = {
    skeleton: 'Skeleton AI',
    story: 'AI Stories',
    motivation: 'Motivation',
    daytrading: 'Day Trading',
    chatstory: 'Chat Story',
    documentary: 'Long Form Documentary',
    recap: 'Long Form Recap',
    explainer: 'Long Form Explainer',
    story_channel: 'Long Form Story Channel',
};

const APPLY_SCOPE_OPTIONS = [
    { value: 'all', label: 'Apply everywhere' },
    { value: 'shorts', label: 'Apply to all shorts' },
    { value: 'longform', label: 'Apply to all long-form' },
];

const WORKSPACE_ORDER = ['skeleton', 'story', 'motivation', 'daytrading', 'chatstory', 'documentary', 'recap', 'explainer', 'story_channel'];
const REFERENCE_SOURCE_LABELS: Record<string, string> = {
    connected_channel: 'Connected Channel',
    manual_upload: 'Uploaded Reference',
    external_url: 'External Reference',
    manual_reference: 'Manual Reference',
};

function splitLines(value: string): string[] {
    return String(value || '')
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean);
}

function normalizeCatalystText(value: string): string {
    return String(value || '')
        .replace(/â€¦/g, '...')
        .replace(/â€™/g, "'")
        .replace(/â€œ|â€/g, '"')
        .replace(/â€/g, '"')
        .replace(/â€“|â€”/g, '-')
        .replace(/\uFFFD/g, '')
        .trim();
}

function sanitizeUniqueTextList(values: string[] | undefined | null): string[] {
    const seen = new Set<string>();
    const result: string[] = [];
    for (const raw of values || []) {
        const value = normalizeCatalystText(String(raw || ''));
        if (!value) continue;
        const key = value.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        result.push(value);
    }
    return result;
}

function formatWhen(unix: number): string {
    if (!unix) return 'Never';
    try {
        return new Date(unix * 1000 || unix).toLocaleString();
    } catch {
        return 'Unknown';
    }
}

async function readJsonResponse<T = any>(res: Response): Promise<T | Record<string, any>> {
    const raw = await res.text().catch(() => '');
    if (!raw) return {};
    try {
        return JSON.parse(raw) as T;
    } catch {
        return {};
    }
}

export default function CatalystPanel() {
    const { session } = useContext(AuthContext);
    const [payload, setPayload] = useState<CatalystHubPayload | null>(null);
    const [selectedChannelId, setSelectedChannelId] = useState('');
    const [selectedWorkspaceId, setSelectedWorkspaceId] = useState('skeleton');
    const [directive, setDirective] = useState('');
    const [mission, setMission] = useState('');
    const [guardrails, setGuardrails] = useState('');
    const [targetNiches, setTargetNiches] = useState('');
    const [applyScope, setApplyScope] = useState('all');
    const [loading, setLoading] = useState(false);
    const [refreshing, setRefreshing] = useState(false);
    const [refreshingChannels, setRefreshingChannels] = useState(false);
    const [youtubeConnecting, setYoutubeConnecting] = useState(false);
    const [syncingOutcomes, setSyncingOutcomes] = useState(false);
    const [analyzingReference, setAnalyzingReference] = useState(false);
    const [clearingReference, setClearingReference] = useState(false);
    const [selectedReferenceVideoId, setSelectedReferenceVideoId] = useState('');
    const [referenceSourceUrl, setReferenceSourceUrl] = useState('');
    const [referenceSourceTitle, setReferenceSourceTitle] = useState('');
    const [referenceSourceChannel, setReferenceSourceChannel] = useState('');
    const [referenceVideoFile, setReferenceVideoFile] = useState<File | null>(null);
    const [referenceAnalyticsNotes, setReferenceAnalyticsNotes] = useState('');
    const [referenceTranscriptText, setReferenceTranscriptText] = useState('');
    const [referenceAnalyticsImages, setReferenceAnalyticsImages] = useState<File[]>([]);
    const [saving, setSaving] = useState(false);
    const [launching, setLaunching] = useState(false);
    const [stoppingSessionId, setStoppingSessionId] = useState('');
    const [error, setError] = useState('');

    const bearerHeaders = useMemo<Record<string, string>>(() => {
        const headers: Record<string, string> = {};
        if (session) headers.Authorization = `Bearer ${session.access_token}`;
        return headers;
    }, [session]);
    const jsonHeaders = useMemo<Record<string, string>>(
        () => ({ 'Content-Type': 'application/json', ...(session ? { Authorization: `Bearer ${session.access_token}` } : {}) }),
        [session]
    );

    const loadHub = useCallback(async (channelId?: string, refresh = false) => {
        if (!session) return;
        setError('');
        if (refresh) setRefreshing(true);
        else setLoading(true);
        try {
            const url = new URL(`${API}/api/catalyst/hub`);
            if (channelId) url.searchParams.set('channel_id', channelId);
            if (refresh) url.searchParams.set('refresh', 'true');
            const res = await fetch(url.toString(), { headers: bearerHeaders });
            const data = await readJsonResponse<CatalystHubPayload>(res) as any;
            if (!res.ok) throw new Error(String(data?.detail || data?.error || 'Failed to load Catalyst hub'));
            setPayload(data as CatalystHubPayload);
            const nextChannelId = String(data?.selected_channel_id || data?.default_channel_id || channelId || '').trim();
            if (nextChannelId) setSelectedChannelId(nextChannelId);
        } catch (e: any) {
            setError(String(e?.message || e || 'Failed to load Catalyst hub'));
        } finally {
            setLoading(false);
            setRefreshing(false);
        }
    }, [bearerHeaders, session]);

    useEffect(() => {
        if (!session) return;
        void loadHub('', false);
    }, [loadHub, session]);

    const workspaceSnapshots = useMemo<Record<string, CatalystWorkspaceSnapshot>>(
        () => Object.fromEntries(Object.entries(payload?.workspace_snapshots || {}).map(([key, value]) => [key, value as CatalystWorkspaceSnapshot])),
        [payload]
    );
    const busyLongformSessionId = useMemo(() => {
        const match = String(error || '').match(/session\s+(lf_\d+_\d+)/i);
        return String(match?.[1] || '').trim();
    }, [error]);

    const orderedWorkspaceIds = useMemo(
        () => WORKSPACE_ORDER.filter((workspaceId) => workspaceSnapshots[workspaceId]).concat(
            Object.keys(workspaceSnapshots).filter((workspaceId) => !WORKSPACE_ORDER.includes(workspaceId))
        ),
        [workspaceSnapshots]
    );
    const pendingLongformLaunchKey = useMemo(() => {
        const uid = String(session?.user?.id || 'guest').trim() || 'guest';
        return `nyptid_longform_launch_session_${uid}`;
    }, [session?.user?.id]);

    const selectedWorkspace = useMemo(
        () => workspaceSnapshots[selectedWorkspaceId] || null,
        [selectedWorkspaceId, workspaceSnapshots]
    );

    useEffect(() => {
        if (selectedWorkspaceId && workspaceSnapshots[selectedWorkspaceId]) return;
        const fallbackWorkspaceId = String(payload?.default_workspace_id || orderedWorkspaceIds[0] || 'skeleton').trim();
        if (fallbackWorkspaceId) setSelectedWorkspaceId(fallbackWorkspaceId);
    }, [orderedWorkspaceIds, payload?.default_workspace_id, selectedWorkspaceId, workspaceSnapshots]);

    useEffect(() => {
        const memory = selectedWorkspace?.memory_public || {};
        setDirective(String(memory.operator_directive || ''));
        setMission(String(memory.operator_mission || ''));
        setGuardrails(Array.isArray(memory.operator_guardrails) ? memory.operator_guardrails.join('\n') : '');
        setTargetNiches(Array.isArray(memory.operator_target_niches) ? memory.operator_target_niches.join('\n') : '');
        const scope = String(memory.operator_apply_scope || '').trim().toLowerCase();
        const normalizedScope = !scope
            ? 'all'
            : APPLY_SCOPE_OPTIONS.some((option) => option.value === scope)
                ? scope
                : orderedWorkspaceIds.includes(scope)
                    ? 'current'
                    : 'all';
        setApplyScope(normalizedScope);
    }, [orderedWorkspaceIds, selectedWorkspaceId, selectedWorkspace]);

    const startYouTubeConnect = useCallback(async () => {
        if (!session || youtubeConnecting) return;
        setYoutubeConnecting(true);
        setError('');
        try {
            const res = await fetch(`${API}/api/oauth/google/youtube/start`, {
                method: 'POST',
                headers: jsonHeaders,
                body: JSON.stringify({ next_url: window.location.href }),
            });
            const data = await readJsonResponse<any>(res);
            if (!res.ok) throw new Error(String(data?.detail || data?.error || `Request failed (${res.status})`));
            const authUrl = String(data?.auth_url || '').trim();
            if (!authUrl) throw new Error('Google auth URL missing');
            window.location.href = authUrl;
        } catch (e: any) {
            setError(String(e?.message || e || 'Failed to start YouTube connection'));
            setYoutubeConnecting(false);
        }
    }, [jsonHeaders, session, youtubeConnecting]);

    const persistSelectedChannel = useCallback(async (nextChannelId: string) => {
        const normalizedId = String(nextChannelId || '').trim();
        setSelectedChannelId(normalizedId);
        if (!session || !normalizedId) {
            await loadHub('', false);
            return;
        }
        setRefreshingChannels(true);
        setError('');
        try {
            const res = await fetch(`${API}/api/youtube/channels/select`, {
                method: 'POST',
                headers: jsonHeaders,
                body: JSON.stringify({ channel_id: normalizedId }),
            });
            const data = await readJsonResponse<any>(res);
            if (!res.ok) throw new Error(String(data?.detail || data?.error || 'Failed to save channel selection'));
            await loadHub(normalizedId, false);
        } catch (e: any) {
            setError(String(e?.message || e || 'Failed to save channel selection'));
        } finally {
            setRefreshingChannels(false);
        }
    }, [jsonHeaders, loadHub, session]);

    const handleRefresh = async (refreshOutcomes = false) => {
        if (!session || !selectedChannelId) return;
        setError('');
        if (refreshOutcomes) setSyncingOutcomes(true);
        else setRefreshing(true);
        try {
            const res = await fetch(`${API}/api/catalyst/hub/refresh`, {
                method: 'POST',
                headers: jsonHeaders,
                body: JSON.stringify({
                    channel_id: selectedChannelId,
                    include_public_benchmarks: true,
                    refresh_outcomes: refreshOutcomes,
                }),
            });
            const data = await readJsonResponse<any>(res);
            if (!res.ok) throw new Error(String(data?.detail || data?.error || 'Failed to refresh Catalyst hub'));
            setPayload(data as CatalystHubPayload);
        } catch (e: any) {
            setError(String(e?.message || e || 'Failed to refresh Catalyst hub'));
        } finally {
            setRefreshing(false);
            setSyncingOutcomes(false);
        }
    };

    const handleSave = async () => {
        if (!session || !selectedChannelId) return;
        setSaving(true);
        setError('');
        try {
            const res = await fetch(`${API}/api/catalyst/hub/instructions`, {
                method: 'POST',
                headers: jsonHeaders,
                body: JSON.stringify({
                    channel_id: selectedChannelId,
                    directive,
                    mission,
                    guardrails: splitLines(guardrails),
                    target_niches: splitLines(targetNiches),
                    apply_scope: applyScope === 'current' ? selectedWorkspaceId : applyScope,
                }),
            });
            const data = await readJsonResponse<any>(res);
            if (!res.ok) throw new Error(String(data?.detail || data?.error || 'Failed to save Catalyst instructions'));
            setPayload(data as CatalystHubPayload);
        } catch (e: any) {
            setError(String(e?.message || e || 'Failed to save Catalyst instructions'));
        } finally {
            setSaving(false);
        }
    };

    const handleLaunchLongform = async () => {
        if (!session || !selectedChannelId) return;
        if (!['documentary', 'recap', 'explainer', 'story_channel'].includes(selectedWorkspaceId)) {
            setError('Catalyst launch is only available for long-form workspaces right now.');
            return;
        }
        setLaunching(true);
        setError('');
        try {
            const res = await fetch(`${API}/api/catalyst/hub/launch`, {
                method: 'POST',
                headers: jsonHeaders,
                body: JSON.stringify({
                    channel_id: selectedChannelId,
                    workspace_id: selectedWorkspaceId,
                    mission,
                    directive,
                    guardrails: splitLines(guardrails),
                    target_niches: splitLines(targetNiches),
                    language: 'en',
                    animation_enabled: true,
                    sfx_enabled: true,
                    auto_pipeline: true,
                    include_public_benchmarks: true,
                    refresh_outcomes: true,
                }),
            });
            const data = await readJsonResponse<any>(res);
            if (!res.ok) throw new Error(String(data?.detail || data?.error || 'Failed to launch Catalyst long-form run'));
            const sessionId = String(data?.session?.session_id || '').trim();
            if (!sessionId) throw new Error('Catalyst launch returned no session id');
            try {
                sessionStorage.setItem(pendingLongformLaunchKey, sessionId);
            } catch {
                // ignore storage errors
            }
            const nextUrl = new URL(window.location.href);
            nextUrl.searchParams.set('page', 'dashboard');
            nextUrl.searchParams.set('tab', 'longform');
            window.location.href = nextUrl.toString();
        } catch (e: any) {
            setError(String(e?.message || e || 'Failed to launch Catalyst long-form run'));
        } finally {
            setLaunching(false);
        }
    };

    const handleRefreshChannels = async () => {
        setRefreshingChannels(true);
        await loadHub(selectedChannelId || '', false);
        setRefreshingChannels(false);
    };

    const handleAnalyzeReferenceVideo = async () => {
        if (!session || !selectedChannelId || !['documentary', 'recap', 'explainer', 'story_channel'].includes(selectedWorkspaceId)) return;
        setAnalyzingReference(true);
        setError('');
        try {
            const hasManualReferenceEvidence = Boolean(
                referenceSourceUrl.trim()
                || referenceSourceTitle.trim()
                || referenceSourceChannel.trim()
                || referenceVideoFile
                || referenceAnalyticsNotes.trim()
                || referenceTranscriptText.trim()
                || referenceAnalyticsImages.length > 0
            );
            const res = hasManualReferenceEvidence
                ? await (async () => {
                    const formData = new FormData();
                    formData.append('channel_id', selectedChannelId);
                    formData.append('workspace_id', selectedWorkspaceId);
                    formData.append('video_id', selectedReferenceVideoId || '');
                    formData.append('max_analysis_minutes', '20');
                    if (referenceSourceUrl.trim()) formData.append('reference_source_url', referenceSourceUrl.trim());
                    if (referenceSourceTitle.trim()) formData.append('reference_title', referenceSourceTitle.trim());
                    if (referenceSourceChannel.trim()) formData.append('reference_channel', referenceSourceChannel.trim());
                    if (referenceVideoFile) formData.append('reference_video', referenceVideoFile);
                    if (referenceAnalyticsNotes.trim()) formData.append('analytics_notes', referenceAnalyticsNotes.trim());
                    if (referenceTranscriptText.trim()) formData.append('transcript_text', referenceTranscriptText.trim());
                    referenceAnalyticsImages.forEach((file) => formData.append('analytics_images', file));
                    return fetch(`${API}/api/catalyst/hub/reference-video-analysis/manual`, {
                        method: 'POST',
                        headers: bearerHeaders,
                        body: formData,
                    });
                })()
                : await fetch(`${API}/api/catalyst/hub/reference-video-analysis`, {
                    method: 'POST',
                    headers: jsonHeaders,
                    body: JSON.stringify({
                        channel_id: selectedChannelId,
                        workspace_id: selectedWorkspaceId,
                        video_id: selectedReferenceVideoId || '',
                        max_analysis_minutes: 20.0,
                    }),
                });
            const data = await readJsonResponse<any>(res);
            if (!res.ok) throw new Error(String(data?.detail || data?.error || 'Failed to analyze the channel reference video'));
            if (data?.payload) setPayload(data.payload as CatalystHubPayload);
            if (hasManualReferenceEvidence) setReferenceAnalyticsImages([]);
        } catch (e: any) {
            setError(String(e?.message || e || 'Failed to analyze the channel reference video'));
        } finally {
            setAnalyzingReference(false);
        }
    };

    const handleClearReferenceVideo = async () => {
        if (!session || !selectedChannelId || !['documentary', 'recap', 'explainer', 'story_channel'].includes(selectedWorkspaceId)) return;
        setClearingReference(true);
        setError('');
        try {
            const res = await fetch(`${API}/api/catalyst/hub/reference-video-analysis/clear`, {
                method: 'POST',
                headers: jsonHeaders,
                body: JSON.stringify({
                    channel_id: selectedChannelId,
                    workspace_id: selectedWorkspaceId,
                }),
            });
            const data = await readJsonResponse<any>(res);
            if (!res.ok) throw new Error(String(data?.detail || data?.error || 'Failed to clear the channel reference video'));
            if (data?.payload) setPayload(data.payload as CatalystHubPayload);
            setReferenceSourceUrl('');
            setReferenceSourceTitle('');
            setReferenceSourceChannel('');
            setReferenceVideoFile(null);
            setReferenceAnalyticsImages([]);
        } catch (e: any) {
            setError(String(e?.message || e || 'Failed to clear the channel reference video'));
        } finally {
            setClearingReference(false);
        }
    };

    const handleStopBusySession = async () => {
        if (!session || !busyLongformSessionId) return;
        setStoppingSessionId(busyLongformSessionId);
        setError('');
        try {
            const res = await fetch(`${API}/api/longform/session/${busyLongformSessionId}/stop`, {
                method: 'POST',
                headers: jsonHeaders,
            });
            const data = await readJsonResponse<any>(res);
            if (!res.ok) throw new Error(String(data?.detail || data?.error || 'Failed to stop active long-form session'));
            await loadHub(selectedChannelId || '', false);
        } catch (e: any) {
            setError(String(e?.message || e || 'Failed to stop active long-form session'));
        } finally {
            setStoppingSessionId('');
        }
    };

    const channelOptions = payload?.channels || [];
    const selectedChannel = useMemo(
        () => channelOptions.find((row) => String(row.channel_id || '').trim() === String(selectedChannelId || '').trim()) || payload?.selected_channel || null,
        [channelOptions, payload?.selected_channel, selectedChannelId]
    );
    const memory = selectedWorkspace?.memory_public || {};
    const referenceVideoAnalysis = selectedWorkspace?.reference_video_analysis || null;
    const recentLearning = payload?.recent_learning || [];
    const applyScopeOptions = useMemo(
        () => [...APPLY_SCOPE_OPTIONS, { value: 'current', label: `Only this workspace (${WORKSPACE_LABELS[selectedWorkspaceId] || selectedWorkspaceId})` }],
        [selectedWorkspaceId]
    );
    const canLaunchLongform = Boolean(selectedChannelId && ['documentary', 'recap', 'explainer', 'story_channel'].includes(selectedWorkspaceId));
    const channelAudit = selectedChannel?.analytics_snapshot?.channel_audit || null;
    const historicalCompare = selectedChannel?.analytics_snapshot?.historical_compare || null;
    const referenceEvidence = referenceVideoAnalysis?.evidence || null;
    const referenceAnalysis = referenceVideoAnalysis?.analysis || null;
    const channelSyncError = useMemo(
        () => normalizeCatalystText(String(selectedChannel?.last_sync_error || '').trim()),
        [selectedChannel?.last_sync_error]
    );
    const channelOutcomeSyncError = useMemo(
        () => normalizeCatalystText(String(selectedChannel?.last_outcome_sync_error || '').trim()),
        [selectedChannel?.last_outcome_sync_error]
    );
    const channelMeasuredFacts = useMemo(
        () => sanitizeUniqueTextList(channelAudit?.measured_facts || []),
        [channelAudit?.measured_facts]
    );
    const channelLimitations = useMemo(() => {
        const rawLimitations = sanitizeUniqueTextList((channelAudit?.limitations || historicalCompare?.limitations || []) as string[]);
        const blocked = new Set(
            [channelSyncError, channelOutcomeSyncError]
                .filter(Boolean)
                .map((value) => value.toLowerCase())
        );
        return rawLimitations.filter((value) => !blocked.has(value.toLowerCase()));
    }, [channelAudit?.limitations, historicalCompare?.limitations, channelOutcomeSyncError, channelSyncError]);
    const referenceMeasuredFacts = useMemo(
        () => sanitizeUniqueTextList((referenceEvidence?.measured_facts || referenceVideoAnalysis?.analysis?.measured_facts || []) as string[]),
        [referenceEvidence?.measured_facts, referenceVideoAnalysis?.analysis?.measured_facts]
    );
    const referenceLimitations = useMemo(
        () => sanitizeUniqueTextList((referenceEvidence?.limitations || referenceVideoAnalysis?.analysis?.limitations || []) as string[]),
        [referenceEvidence?.limitations, referenceVideoAnalysis?.analysis?.limitations]
    );
    const uploadedVideoOptions = useMemo(
        () => (
            Array.isArray(selectedChannel?.analytics_snapshot?.uploaded_videos)
                ? [...selectedChannel.analytics_snapshot.uploaded_videos]
                : []
        )
            .filter((row) => String(row?.video_id || '').trim())
            .sort((a, b) =>
                (Number(b?.views || 0) - Number(a?.views || 0))
                || (Number(b?.average_view_percentage || 0) - Number(a?.average_view_percentage || 0))
                || (Number(b?.impression_click_through_rate || 0) - Number(a?.impression_click_through_rate || 0))
                || String(b?.published_at || '').localeCompare(String(a?.published_at || ''))
            ),
        [selectedChannel]
    );
    const appendReferenceAnalyticsImages = useCallback((incomingFiles: File[]) => {
        const normalized = incomingFiles
            .filter((file) => file && (/^image\/(png|jpeg|webp)$/i.test(file.type) || /\.(png|jpe?g|webp)$/i.test(file.name || '')))
            .map((file, idx) => {
                if (file.name) return file;
                const ext = /jpeg/i.test(file.type) ? 'jpg' : /webp/i.test(file.type) ? 'webp' : 'png';
                return new File([file], `studio-shot-${Date.now()}-${idx}.${ext}`, { type: file.type || `image/${ext}` });
            });
        if (normalized.length) setReferenceAnalyticsImages((prev) => [...prev, ...normalized].slice(0, 24));
    }, []);

    useEffect(() => {
        const currentReferenceVideoId = String(referenceVideoAnalysis?.video?.video_id || '').trim();
        if (currentReferenceVideoId) {
            setSelectedReferenceVideoId(currentReferenceVideoId);
            return;
        }
        setSelectedReferenceVideoId((prev) => {
            const currentSelection = String(prev || '').trim();
            if (currentSelection && uploadedVideoOptions.some((video) => String(video?.video_id || '').trim() === currentSelection)) {
                return currentSelection;
            }
            return '';
        });
    }, [referenceVideoAnalysis?.video?.video_id, uploadedVideoOptions]);

    const hasManualReferenceSource = Boolean(referenceSourceUrl.trim() || referenceSourceTitle.trim() || referenceSourceChannel.trim() || referenceVideoFile);
    const canAnalyzeReferenceVideo = Boolean((uploadedVideoOptions.length > 0 || hasManualReferenceSource) && !analyzingReference && !clearingReference);
    const hasManualReferenceEvidence = Boolean(referenceAnalyticsNotes.trim() || referenceTranscriptText.trim() || referenceAnalyticsImages.length > 0);
    const referenceManualEvidenceSummary = useMemo(
        () => normalizeCatalystText(String(referenceAnalysis?.manual_evidence_summary || referenceEvidence?.manual_evidence_summary || '').trim()),
        [referenceAnalysis?.manual_evidence_summary, referenceEvidence?.manual_evidence_summary]
    );

    if (!session) return null;

    return (
        <section className="space-y-6">
            <div className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                <div className="flex flex-wrap items-start justify-between gap-4">
                    <div>
                        <div className="flex items-center gap-2 text-sm font-semibold text-cyan-300">
                            <BrainCircuit className="h-4 w-4" />
                            Catalyst Hub
                        </div>
                        <h2 className="mt-3 text-3xl font-bold text-white">Direct the engine from one place</h2>
                        <p className="mt-3 max-w-3xl text-sm text-gray-400">
                            Catalyst uses connected-channel memory, pooled YouTube API keys, public benchmark mining, and your operator brief together.
                            Use this hub to tell it exactly what to optimize, what to avoid, and which niches or angles to push harder.
                        </p>
                    </div>
                    <div className="flex flex-wrap gap-2">
                        <button
                            type="button"
                            onClick={() => { void startYouTubeConnect(); }}
                            disabled={youtubeConnecting}
                            className="inline-flex items-center gap-2 rounded-xl border border-emerald-500/30 bg-emerald-500/10 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:border-emerald-400/50 hover:bg-emerald-500/15 disabled:cursor-not-allowed disabled:opacity-60"
                        >
                            {youtubeConnecting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Youtube className="h-4 w-4" />}
                            Connect YouTube
                        </button>
                        <button
                            type="button"
                            onClick={() => { void handleRefreshChannels(); }}
                            disabled={refreshingChannels}
                            className="inline-flex items-center gap-2 rounded-xl border border-white/[0.12] bg-white/[0.04] px-4 py-2 text-sm font-semibold text-gray-100 transition hover:border-white/[0.2] hover:bg-white/[0.08] disabled:cursor-not-allowed disabled:opacity-60"
                        >
                            {refreshingChannels ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                            Refresh Channels
                        </button>
                        <button
                            type="button"
                            onClick={() => void handleRefresh(false)}
                            disabled={refreshing || syncingOutcomes || !selectedChannelId}
                            className="inline-flex items-center gap-2 rounded-xl border border-cyan-500/30 bg-cyan-500/10 px-4 py-2 text-sm font-semibold text-cyan-100 transition hover:border-cyan-400/50 hover:bg-cyan-500/15 disabled:cursor-not-allowed disabled:opacity-60"
                        >
                            {refreshing ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                            Refresh Memory
                        </button>
                        <button
                            type="button"
                            onClick={() => void handleRefresh(true)}
                            disabled={refreshing || syncingOutcomes || !selectedChannelId}
                            className="inline-flex items-center gap-2 rounded-xl border border-amber-500/30 bg-amber-500/10 px-4 py-2 text-sm font-semibold text-amber-100 transition hover:border-amber-400/50 hover:bg-amber-500/15 disabled:cursor-not-allowed disabled:opacity-60"
                        >
                            {syncingOutcomes ? <Loader2 className="h-4 w-4 animate-spin" /> : <Youtube className="h-4 w-4" />}
                            Sync Outcomes
                        </button>
                        <button
                            type="button"
                            onClick={() => void handleAnalyzeReferenceVideo()}
                            disabled={analyzingReference || clearingReference || !selectedChannelId || !['documentary', 'recap', 'explainer', 'story_channel'].includes(selectedWorkspaceId)}
                            className="inline-flex items-center gap-2 rounded-xl border border-violet-500/30 bg-violet-500/10 px-4 py-2 text-sm font-semibold text-violet-100 transition hover:border-violet-400/50 hover:bg-violet-500/15 disabled:cursor-not-allowed disabled:opacity-60"
                        >
                            {analyzingReference ? <Loader2 className="h-4 w-4 animate-spin" /> : <BrainCircuit className="h-4 w-4" />}
                            Analyze Reference
                        </button>
                        <button
                            type="button"
                            onClick={() => void handleClearReferenceVideo()}
                            disabled={clearingReference || analyzingReference || !selectedChannelId || !['documentary', 'recap', 'explainer', 'story_channel'].includes(selectedWorkspaceId)}
                            className="inline-flex items-center gap-2 rounded-xl border border-rose-500/30 bg-rose-500/10 px-4 py-2 text-sm font-semibold text-rose-100 transition hover:border-rose-400/50 hover:bg-rose-500/15 disabled:cursor-not-allowed disabled:opacity-60"
                        >
                            {clearingReference ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                            Clear Reference
                        </button>
                    </div>
                </div>
            </div>

            {error && (
                <div className="rounded-2xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-200">
                    <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                        <div>{error}</div>
                        {busyLongformSessionId ? (
                            <button
                                type="button"
                                onClick={() => void handleStopBusySession()}
                                disabled={stoppingSessionId === busyLongformSessionId}
                                className="inline-flex items-center justify-center rounded-xl border border-red-400/30 bg-red-500/15 px-3 py-2 text-xs font-semibold text-red-100 transition hover:border-red-300/50 hover:bg-red-500/20 disabled:cursor-not-allowed disabled:opacity-60"
                            >
                                {stoppingSessionId === busyLongformSessionId ? 'Stopping run...' : `Stop ${busyLongformSessionId}`}
                            </button>
                        ) : null}
                    </div>
                </div>
            )}

            <div className="grid gap-6 xl:grid-cols-[1.1fr,0.9fr]">
                <div className="space-y-6">
                    <div className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                        <div className="grid gap-4 md:grid-cols-2">
                            <label className="space-y-2">
                                <span className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-200/70">Connected Channel</span>
                                <select
                                    value={selectedChannelId}
                                    onChange={(e) => { void persistSelectedChannel(e.target.value); }}
                                    className="w-full rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm text-white outline-none transition focus:border-cyan-400/50"
                                    style={{ colorScheme: 'dark', backgroundColor: '#0b0b0f', color: '#ffffff' }}
                                >
                                    <option value="" style={{ backgroundColor: '#0b0b0f', color: '#ffffff' }}>Select a connected channel</option>
                                    {channelOptions.map((channel) => (
                                        <option key={channel.channel_id} value={channel.channel_id} style={{ backgroundColor: '#0b0b0f', color: '#ffffff' }}>
                                            {channel.title}{channel.channel_handle ? ` (${channel.channel_handle})` : ''}
                                        </option>
                                    ))}
                                </select>
                            </label>
                            <label className="space-y-2">
                                <span className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-200/70">Workspace Focus</span>
                                <select
                                    value={selectedWorkspaceId}
                                    onChange={(e) => setSelectedWorkspaceId(e.target.value)}
                                    className="w-full rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm text-white outline-none transition focus:border-cyan-400/50"
                                    style={{ colorScheme: 'dark', backgroundColor: '#0b0b0f', color: '#ffffff' }}
                                >
                                    {Object.keys(workspaceSnapshots).map((workspaceId) => (
                                        <option key={workspaceId} value={workspaceId} style={{ backgroundColor: '#0b0b0f', color: '#ffffff' }}>
                                            {WORKSPACE_LABELS[workspaceId] || workspaceId}
                                        </option>
                                    ))}
                                </select>
                            </label>
                        </div>

                        <div className="mt-5 grid gap-4 md:grid-cols-2">
                            <label className="space-y-2">
                                <span className="text-xs font-semibold uppercase tracking-[0.18em] text-violet-200/70">Main Goal</span>
                                <input
                                    value={mission}
                                    onChange={(e) => setMission(e.target.value)}
                                    placeholder="Example: Make Fern-style business videos with stronger hooks and cleaner 3D system visuals."
                                    className="w-full rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm text-white outline-none transition focus:border-violet-400/50"
                                />
                                <p className="text-xs text-gray-500">The one-sentence outcome you want Catalyst to optimize for.</p>
                            </label>
                            <label className="space-y-2">
                                <span className="text-xs font-semibold uppercase tracking-[0.18em] text-violet-200/70">Where To Apply It</span>
                                <select
                                    value={applyScope}
                                    onChange={(e) => setApplyScope(e.target.value)}
                                    className="w-full rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm text-white outline-none transition focus:border-violet-400/50"
                                    style={{ colorScheme: 'dark', backgroundColor: '#0b0b0f', color: '#ffffff' }}
                                >
                                    {applyScopeOptions.map((option) => (
                                        <option key={option.value} value={option.value} style={{ backgroundColor: '#0b0b0f', color: '#ffffff' }}>
                                            {option.label}
                                        </option>
                                    ))}
                                </select>
                                <p className="text-xs text-gray-500">Choose whether this instruction affects everything, all shorts, all long-form, or just the current workspace.</p>
                            </label>
                        </div>

                        <label className="mt-5 block space-y-2">
                            <span className="text-xs font-semibold uppercase tracking-[0.18em] text-violet-200/70">What Catalyst Should Do</span>
                            <textarea
                                value={directive}
                                onChange={(e) => setDirective(e.target.value)}
                                placeholder="Write the exact instruction. Example: Study Empire Magnates winners, avoid generic lab shots, push polished 3D business-documentary visuals, and generate stronger click-first titles."
                                className="min-h-[180px] w-full rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm text-white outline-none transition focus:border-violet-400/50"
                            />
                            <p className="text-xs text-gray-500">This is your plain-English command to Catalyst. Use it to tell the engine what to build, what to avoid, and what to improve next.</p>
                        </label>

                        <div className="mt-5 grid gap-4 md:grid-cols-2">
                            <label className="space-y-2">
                                <span className="text-xs font-semibold uppercase tracking-[0.18em] text-violet-200/70">Guardrails</span>
                                <textarea
                                    value={guardrails}
                                    onChange={(e) => setGuardrails(e.target.value)}
                                    placeholder="One rule per line. Example: No stale repeated angles"
                                    className="min-h-[140px] w-full rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm text-white outline-none transition focus:border-violet-400/50"
                                />
                            </label>
                            <label className="space-y-2">
                                <span className="text-xs font-semibold uppercase tracking-[0.18em] text-violet-200/70">Priority Niches</span>
                                <textarea
                                    value={targetNiches}
                                    onChange={(e) => setTargetNiches(e.target.value)}
                                    placeholder="One niche per line. Example: dark psychology"
                                    className="min-h-[140px] w-full rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm text-white outline-none transition focus:border-violet-400/50"
                                />
                            </label>
                        </div>

                        <div className="mt-5 flex flex-wrap items-center gap-3">
                            <button
                                type="button"
                                onClick={() => void handleSave()}
                                disabled={saving || !selectedChannelId}
                                className="inline-flex items-center gap-2 rounded-2xl bg-violet-600 px-5 py-3 text-sm font-semibold text-white transition hover:bg-violet-500 disabled:cursor-not-allowed disabled:opacity-60"
                            >
                                {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                                Save Catalyst Directive
                            </button>
                            <button
                                type="button"
                                onClick={() => void handleLaunchLongform()}
                                disabled={launching || !canLaunchLongform}
                                className="inline-flex items-center gap-2 rounded-2xl border border-cyan-500/30 bg-cyan-500/10 px-5 py-3 text-sm font-semibold text-cyan-100 transition hover:border-cyan-400/50 hover:bg-cyan-500/15 disabled:cursor-not-allowed disabled:opacity-60"
                            >
                                {launching ? <Loader2 className="h-4 w-4 animate-spin" /> : <BrainCircuit className="h-4 w-4" />}
                                Autonomous Launch Long-Form
                            </button>
                            <span className="text-xs text-gray-500">
                                Saved directives are written into Catalyst channel memory and reused by shorts and long-form guidance.
                            </span>
                        </div>
                    </div>

                    <div className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                        <div className="flex items-center gap-2 text-sm font-semibold text-emerald-300">
                            <Target className="h-4 w-4" />
                            Recent Learning
                        </div>
                        <div className="mt-4 space-y-3">
                            {recentLearning.length === 0 && (
                                <div className="rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-4 text-sm text-gray-400">
                                    Catalyst has no recent learning records for this channel yet.
                                </div>
                            )}
                            {recentLearning.map((row) => (
                                <div key={`${row.session_id}_${row.created_at}`} className="rounded-2xl border border-white/[0.08] bg-black/20 p-4">
                                    <div className="flex flex-wrap items-center justify-between gap-3">
                                        <div>
                                            <div className="text-sm font-semibold text-white">
                                                {WORKSPACE_LABELS[row.format_preset] || row.format_preset || row.mode || 'Catalyst run'}
                                            </div>
                                            <div className="mt-1 text-xs text-gray-500">{formatWhen(row.created_at)}</div>
                                        </div>
                                        {row.last_failure_mode_label && (
                                            <span className="rounded-full border border-amber-400/30 bg-amber-500/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-amber-200">
                                                {row.last_failure_mode_label}
                                            </span>
                                        )}
                                    </div>
                                    {row.selected_title && <div className="mt-3 text-sm font-medium text-violet-200">{row.selected_title}</div>}
                                    {row.outcome_summary && <div className="mt-2 text-sm text-gray-300">{row.outcome_summary}</div>}
                                    {Array.isArray(row.next_video_moves) && row.next_video_moves.length > 0 && (
                                        <div className="mt-3 text-xs text-cyan-200/80">
                                            Next moves: {row.next_video_moves.slice(0, 3).join(' | ')}
                                        </div>
                                    )}
                                </div>
                            ))}
                        </div>
                    </div>
                </div>

                <div className="space-y-6">
                    <div className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                        <div className="flex items-center gap-2 text-sm font-semibold text-cyan-300">
                            <Youtube className="h-4 w-4" />
                            Channel Snapshot
                        </div>
                        {!selectedChannel ? (
                            <div className="mt-4 rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-4 text-sm text-gray-400">
                                Connect or select a YouTube channel to view Catalyst memory.
                            </div>
                        ) : (
                            <div className="mt-4 space-y-3">
                                <div>
                                    <div className="text-lg font-semibold text-white">{selectedChannel.title}</div>
                                    {selectedChannel.channel_handle && <div className="mt-1 text-sm text-gray-400">{selectedChannel.channel_handle}</div>}
                                </div>
                                {(channelAudit?.honesty_note || historicalCompare?.honesty_note) && (
                                    <div className="rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3 text-sm text-cyan-50">
                                        {normalizeCatalystText(String(channelAudit?.honesty_note || historicalCompare?.honesty_note || ''))}
                                    </div>
                                )}
                                {(channelAudit?.coverage?.recent_uploads || channelAudit?.coverage?.top_videos || channelAudit?.coverage?.series_clusters) ? (
                                    <div className="grid gap-3 sm:grid-cols-3">
                                        <StatCard label="Recent Uploads" value={String(channelAudit?.coverage?.recent_uploads || 0)} />
                                        <StatCard label="Top Videos" value={String(channelAudit?.coverage?.top_videos || 0)} />
                                        <StatCard label="Active Arcs" value={String(channelAudit?.coverage?.series_clusters || 0)} />
                                    </div>
                                ) : null}
                                <DetailList title="Measured Channel Data" values={channelMeasuredFacts} accent="emerald" />
                                <DetailList title="Channel Limitations" values={channelLimitations} accent="amber" />
                                {selectedChannel.last_outcome_sync_at ? (
                                    <div className="rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3 text-xs text-gray-300">
                                        Last outcome sync: {formatWhen(selectedChannel.last_outcome_sync_at)}{selectedChannel.last_outcome_sync_count ? ` | ${selectedChannel.last_outcome_sync_count} videos` : ''}
                                    </div>
                                ) : null}
                                {channelSyncError && (
                                    <div className="rounded-2xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-200">
                                        {channelSyncError}
                                    </div>
                                )}
                                {channelOutcomeSyncError && (
                                    <div className="rounded-2xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-200">
                                        {channelOutcomeSyncError}
                                    </div>
                                )}
                                {selectedChannel && (
                                    <div className="rounded-2xl border border-white/[0.08] bg-black/20 p-4">
                                        <div className="text-xs font-semibold uppercase tracking-[0.18em] text-gray-400">Reference Case File</div>
                                        {uploadedVideoOptions.length > 0 ? (
                                            <>
                                                <div className="mt-3 grid gap-3 md:grid-cols-[1fr,auto]">
                                                    <select
                                                        value={selectedReferenceVideoId}
                                                        onChange={(e) => setSelectedReferenceVideoId(e.target.value)}
                                                        className="w-full rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm text-white outline-none transition focus:border-violet-400/50"
                                                        style={{ colorScheme: 'dark', backgroundColor: '#0b0b0f', color: '#ffffff' }}
                                                    >
                                                        <option value="" style={{ backgroundColor: '#0b0b0f', color: '#ffffff' }}>
                                                            Let Catalyst choose the strongest measured upload
                                                        </option>
                                                        {uploadedVideoOptions.map((video) => {
                                                            const published = String(video.published_at || '').trim();
                                                            const title = String(video.title || '').trim() || String(video.video_id || '').trim();
                                                            const meta = [
                                                                published ? new Date(published).toLocaleDateString() : '',
                                                                typeof video.views === 'number' ? `${video.views} views` : '',
                                                            ].filter(Boolean).join(' | ');
                                                            return (
                                                                <option key={video.video_id} value={video.video_id} style={{ backgroundColor: '#0b0b0f', color: '#ffffff' }}>
                                                                    {meta ? `${title} (${meta})` : title}
                                                                </option>
                                                            );
                                                        })}
                                                    </select>
                                                    <button
                                                        type="button"
                                                        onClick={() => void handleAnalyzeReferenceVideo()}
                                                        disabled={!canAnalyzeReferenceVideo}
                                                        className="inline-flex items-center justify-center gap-2 rounded-2xl border border-violet-500/30 bg-violet-500/10 px-4 py-3 text-sm font-semibold text-violet-100 transition hover:border-violet-400/50 hover:bg-violet-500/15 disabled:cursor-not-allowed disabled:opacity-60"
                                                    >
                                                        {analyzingReference ? <Loader2 className="h-4 w-4 animate-spin" /> : <BrainCircuit className="h-4 w-4" />}
                                                        {hasManualReferenceSource ? 'Analyze Manual Reference' : hasManualReferenceEvidence ? 'Analyze With Studio Evidence' : selectedReferenceVideoId ? 'Analyze Selected' : 'Analyze Strongest Upload'}
                                                    </button>
                                                </div>
                                                <div className="mt-2 text-xs text-gray-500">
                                                    Leave the first option selected to let Catalyst pick the strongest measured upload, or override it with a specific video.
                                                </div>
                                            </>
                                        ) : (
                                            <div className="mt-3 rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3 text-xs text-gray-400">
                                                No measured uploads are loaded yet. You can still benchmark a manual or competitor reference by uploading the video file below and pasting the Studio screenshots.
                                            </div>
                                        )}
                                        <div
                                            className="mt-4 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 p-4 outline-none"
                                            tabIndex={0}
                                            onPaste={(e) => {
                                                const files = Array.from(e.clipboardData?.files || []);
                                                if (!files.length) return;
                                                appendReferenceAnalyticsImages(files);
                                                e.preventDefault();
                                            }}
                                            onDragOver={(e) => e.preventDefault()}
                                            onDrop={(e) => {
                                                const files = Array.from(e.dataTransfer?.files || []);
                                                if (!files.length) return;
                                                appendReferenceAnalyticsImages(files);
                                                e.preventDefault();
                                            }}
                                        >
                                            <div className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-200/70">Manual Studio Evidence</div>
                                            <p className="mt-2 text-sm text-cyan-50/90">
                                                Build a full case file here while Google OAuth is down: upload the actual reference video, paste or upload every YouTube Studio screenshot you can grab, and add transcript or operator notes. Catalyst will OCR visible metrics like impressions, CTR, AVD, traffic sources, device split, and early rank.
                                            </p>
                                            <div className="mt-3 grid gap-3 lg:grid-cols-3">
                                                <input
                                                    value={referenceSourceUrl}
                                                    onChange={(e) => setReferenceSourceUrl(e.target.value)}
                                                    placeholder="Optional: competitor/source video URL"
                                                    className="w-full rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm text-white outline-none transition placeholder:text-gray-500 focus:border-cyan-400/50"
                                                />
                                                <input
                                                    value={referenceSourceTitle}
                                                    onChange={(e) => setReferenceSourceTitle(e.target.value)}
                                                    placeholder="Optional: source title"
                                                    className="w-full rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm text-white outline-none transition placeholder:text-gray-500 focus:border-cyan-400/50"
                                                />
                                                <input
                                                    value={referenceSourceChannel}
                                                    onChange={(e) => setReferenceSourceChannel(e.target.value)}
                                                    placeholder="Optional: source channel"
                                                    className="w-full rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm text-white outline-none transition placeholder:text-gray-500 focus:border-cyan-400/50"
                                                />
                                            </div>
                                            <div className="mt-3 grid gap-3 lg:grid-cols-2">
                                                <textarea
                                                    value={referenceAnalyticsNotes}
                                                    onChange={(e) => setReferenceAnalyticsNotes(e.target.value)}
                                                    rows={5}
                                                    placeholder="Optional: paste plain-English observations like '1 of 8, Suggested Videos 92.9%, CTR 1.9%, AVD 4:03, mobile 94.7%'."
                                                    className="w-full rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm text-white outline-none transition placeholder:text-gray-500 focus:border-cyan-400/50"
                                                />
                                                <textarea
                                                    value={referenceTranscriptText}
                                                    onChange={(e) => setReferenceTranscriptText(e.target.value)}
                                                    rows={5}
                                                    placeholder="Optional: paste the intro transcript or key beats so Catalyst can connect packaging and retention to the actual story structure."
                                                    className="w-full rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm text-white outline-none transition placeholder:text-gray-500 focus:border-cyan-400/50"
                                                />
                                            </div>
                                            <div className="mt-3 flex flex-wrap items-center gap-3">
                                                <label className="inline-flex cursor-pointer items-center gap-2 rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm font-medium text-white transition hover:border-white/[0.18] hover:bg-white/[0.06]">
                                                    <input
                                                        type="file"
                                                        accept="video/*,.mp4,.mov,.m4v,.webm,.mkv,.avi"
                                                        className="hidden"
                                                        onChange={(e) => {
                                                            setReferenceVideoFile(e.target.files?.[0] || null);
                                                            e.currentTarget.value = '';
                                                        }}
                                                    />
                                                    Upload Reference Video
                                                </label>
                                                <span className="text-xs text-gray-400">
                                                    {referenceVideoFile ? referenceVideoFile.name : 'No reference video selected'}
                                                </span>
                                                {referenceVideoFile && (
                                                    <button
                                                        type="button"
                                                        onClick={() => setReferenceVideoFile(null)}
                                                        className="text-xs font-semibold text-gray-400 transition hover:text-white"
                                                    >
                                                        Clear video
                                                    </button>
                                                )}
                                            </div>
                                            <div className="mt-3 flex flex-wrap items-center gap-3">
                                                <label className="inline-flex cursor-pointer items-center gap-2 rounded-2xl border border-white/[0.1] bg-black/20 px-4 py-3 text-sm font-medium text-white transition hover:border-white/[0.18] hover:bg-white/[0.06]">
                                                    <input
                                                        type="file"
                                                        accept="image/png,image/jpeg,image/webp"
                                                        multiple
                                                        className="hidden"
                                                        onChange={(e) => {
                                                            const incoming = Array.from(e.target.files || []);
                                                            appendReferenceAnalyticsImages(incoming);
                                                            e.currentTarget.value = '';
                                                        }}
                                                    />
                                                    Upload Studio Screenshots
                                                </label>
                                                <span className="text-xs text-gray-400">
                                                    {referenceAnalyticsImages.length} screenshot{referenceAnalyticsImages.length === 1 ? '' : 's'} selected
                                                </span>
                                                <span className="text-xs text-gray-500">
                                                    Click this card and press Ctrl+V to paste screenshots directly.
                                                </span>
                                                {referenceAnalyticsImages.length > 0 && (
                                                    <button
                                                        type="button"
                                                        onClick={() => setReferenceAnalyticsImages([])}
                                                        className="text-xs font-semibold text-gray-400 transition hover:text-white"
                                                    >
                                                        Clear screenshots
                                                    </button>
                                                )}
                                            </div>
                                            {referenceAnalyticsImages.length > 0 && (
                                                <div className="mt-3 flex flex-wrap gap-2">
                                                    {referenceAnalyticsImages.slice(0, 8).map((file, idx) => (
                                                        <span key={`${file.name}_${idx}`} className="rounded-full border border-white/[0.1] bg-black/20 px-3 py-1 text-xs text-gray-300">
                                                            {file.name}
                                                        </span>
                                                    ))}
                                                    {referenceAnalyticsImages.length > 8 && (
                                                        <span className="rounded-full border border-white/[0.08] bg-black/20 px-3 py-1 text-xs text-gray-500">
                                                            +{referenceAnalyticsImages.length - 8} more
                                                        </span>
                                                    )}
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                )}
                                {(referenceMeasuredFacts.length > 0 || referenceLimitations.length > 0 || referenceVideoAnalysis?.video?.title) && referenceVideoAnalysis && (
                                    <div className="rounded-2xl border border-violet-500/20 bg-violet-500/10 p-4">
                                        <div className="text-xs font-semibold uppercase tracking-[0.18em] text-violet-200/70">Reference Video Breakdown</div>
                                        {referenceVideoAnalysis.video?.title && (
                                            <div className="mt-3 text-sm font-semibold text-white">{referenceVideoAnalysis.video.title}</div>
                                        )}
                                        {(referenceVideoAnalysis.video?.source_channel || referenceVideoAnalysis.video?.source_kind) && (
                                            <div className="mt-2 text-xs text-gray-400">
                                                Source: {referenceVideoAnalysis.video?.source_channel || 'Unknown'}
                                                {referenceVideoAnalysis.video?.source_kind ? ` · ${REFERENCE_SOURCE_LABELS[String(referenceVideoAnalysis.video.source_kind)] || String(referenceVideoAnalysis.video.source_kind)}` : ''}
                                            </div>
                                        )}
                                        {(referenceEvidence?.analysis_mode_label || referenceEvidence?.confidence_label) && (
                                            <div className="mt-3 flex flex-wrap gap-2">
                                                {referenceEvidence?.analysis_mode_label && (
                                                    <span className="rounded-full border border-cyan-500/20 bg-cyan-500/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-cyan-100">
                                                        Evidence: {referenceEvidence.analysis_mode_label}
                                                    </span>
                                                )}
                                                {referenceEvidence?.confidence_label && (
                                                    <span className="rounded-full border border-violet-500/20 bg-violet-500/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-violet-100">
                                                        Confidence: {referenceEvidence.confidence_label}
                                                    </span>
                                                )}
                                            </div>
                                        )}
                                        {(referenceEvidence?.honesty_note || referenceAnalysis?.honesty_note) && (
                                            <div className="mt-3 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3 text-sm text-cyan-50">
                                                {referenceEvidence?.honesty_note || referenceAnalysis?.honesty_note}
                                            </div>
                                        )}
                                        {referenceManualEvidenceSummary && (
                                            <div className="mt-3 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3 text-sm text-cyan-50">
                                                {referenceManualEvidenceSummary}
                                            </div>
                                        )}
                                        {(referenceVideoAnalysis.video?.views || referenceVideoAnalysis.video?.impressions || referenceVideoAnalysis.video?.average_view_duration_sec || referenceVideoAnalysis.video?.average_view_percentage || referenceVideoAnalysis.video?.impression_click_through_rate || referenceVideoAnalysis.video?.watch_time_hours) ? (
                                            <div className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                                                <StatCard label="Views" value={String(referenceVideoAnalysis.video?.views || 0)} />
                                                <StatCard label="Impressions" value={referenceVideoAnalysis.video?.impressions ? String(referenceVideoAnalysis.video.impressions) : 'N/A'} />
                                                <StatCard label="Avg View Duration" value={referenceVideoAnalysis.video?.average_view_duration_sec ? `${Math.floor(Number(referenceVideoAnalysis.video.average_view_duration_sec) / 60)}:${String(Math.floor(Number(referenceVideoAnalysis.video.average_view_duration_sec) % 60)).padStart(2, '0')}` : 'N/A'} />
                                                <StatCard label="Avg Viewed" value={referenceVideoAnalysis.video?.average_view_percentage ? `${Number(referenceVideoAnalysis.video.average_view_percentage).toFixed(2)}%` : 'N/A'} />
                                                <StatCard label="CTR" value={referenceVideoAnalysis.video?.impression_click_through_rate ? `${Number(referenceVideoAnalysis.video.impression_click_through_rate).toFixed(2)}%` : 'N/A'} />
                                                <StatCard label="Watch Time" value={referenceVideoAnalysis.video?.watch_time_hours ? `${Number(referenceVideoAnalysis.video.watch_time_hours).toFixed(1)}h` : 'N/A'} />
                                            </div>
                                        ) : null}
                                        <DetailList title="Measured Reference Evidence" values={referenceMeasuredFacts} accent="emerald" />
                                        <DetailList title="Reference Limitations" values={referenceLimitations} accent="amber" />
                                    </div>
                                )}
                            </div>
                        )}
                    </div>

                    <div className="rounded-3xl border border-white/[0.06] bg-white/[0.02] p-6">
                        <div className="flex items-center gap-2 text-sm font-semibold text-violet-300">
                            <Bot className="h-4 w-4" />
                            {WORKSPACE_LABELS[selectedWorkspaceId] || selectedWorkspaceId} Memory
                        </div>
                        {loading && (
                            <div className="mt-4 flex items-center gap-3 rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-4 text-sm text-gray-300">
                                <Loader2 className="h-4 w-4 animate-spin" />
                                Loading Catalyst memory...
                            </div>
                        )}
                        {!loading && !selectedWorkspace && (
                            <div className="mt-4 rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-4 text-sm text-gray-400">
                                No Catalyst memory snapshot exists for this workspace yet.
                            </div>
                        )}
                        {!loading && selectedWorkspace && (
                            <div className="mt-4 space-y-4">
                                {(referenceEvidence?.honesty_note || referenceAnalysis?.honesty_note) && (
                                    <div className="rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3 text-sm text-cyan-50">
                                        {referenceEvidence?.honesty_note || referenceAnalysis?.honesty_note}
                                    </div>
                                )}
                                <div className="grid gap-3 sm:grid-cols-2">
                                    <StatCard label="Archetype" value={String(memory.archetype_label || 'Unclassified')} />
                                    <StatCard label="Series / Arc" value={String(memory.series_anchor || memory.selected_cluster_label || 'General')} />
                                    <StatCard label="Avg CTR" value={memory.average_ctr ? `${Number(memory.average_ctr).toFixed(2)}%` : 'N/A'} />
                                    <StatCard label="Avg Viewed" value={memory.average_average_percentage_viewed ? `${Number(memory.average_average_percentage_viewed).toFixed(2)}%` : 'N/A'} />
                                </div>
                                <DetailGroup title="Guardrails" values={memory.operator_guardrails || []} accent="cyan" />
                                <DetailGroup title="Priority Niches" values={memory.operator_target_niches || []} accent="cyan" />
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </section>
    );
}

function StatCard({ label, value }: { label: string; value: string }) {
    return (
        <div className="rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3">
            <div className="text-[10px] uppercase tracking-[0.18em] text-gray-500">{label}</div>
            <div className="mt-2 text-lg font-semibold text-white">{value}</div>
        </div>
    );
}

function DetailGroup({ title, values, accent }: { title: string; values: string[]; accent: 'emerald' | 'amber' | 'cyan' }) {
    const cleanValues = sanitizeUniqueTextList(values);
    const palette = accent === 'emerald'
        ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-50'
        : accent === 'amber'
            ? 'border-amber-500/20 bg-amber-500/10 text-amber-50'
            : 'border-cyan-500/20 bg-cyan-500/10 text-cyan-50';
    if (cleanValues.length === 0) return null;
    return (
        <div>
            <div className="mb-2 text-xs font-semibold uppercase tracking-[0.18em] text-gray-400">{title}</div>
            <div className="flex flex-wrap gap-2">
                {cleanValues.slice(0, 8).map((value) => (
                    <span key={value} className={`rounded-full border px-3 py-1.5 text-xs ${palette}`}>
                        {value}
                    </span>
                ))}
            </div>
        </div>
    );
}

function DetailList({
    title,
    values,
    accent,
}: {
    title: string;
    values: string[];
    accent: 'emerald' | 'amber' | 'cyan' | 'violet';
}) {
    const cleanValues = sanitizeUniqueTextList(values);
    const palette = accent === 'emerald'
        ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-50'
        : accent === 'amber'
            ? 'border-amber-500/20 bg-amber-500/10 text-amber-50'
            : accent === 'violet'
                ? 'border-violet-500/20 bg-violet-500/10 text-violet-50'
                : 'border-cyan-500/20 bg-cyan-500/10 text-cyan-50';
    if (cleanValues.length === 0) return null;
    return (
        <div>
            <div className="mb-2 text-xs font-semibold uppercase tracking-[0.18em] text-gray-400">{title}</div>
            <div className="space-y-2">
                {cleanValues.slice(0, 8).map((value) => (
                    <div key={value} className={`rounded-2xl border px-4 py-3 text-sm ${palette}`}>
                        {value}
                    </div>
                ))}
            </div>
        </div>
    );
}
