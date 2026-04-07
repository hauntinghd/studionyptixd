import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { AlertTriangle, CheckCircle2, Clock3, Download, FolderKanban, Loader2, RefreshCw, RotateCcw, Sparkles, Wand2 } from 'lucide-react';
import { API, AuthContext, startYouTubeBrowserConnect } from '../shared';
import { ProgressBar, RenderProgressWindow } from '../components/StudioWidgets';

type LongFormChapter = {
    index: number;
    title: string;
    summary: string;
    target_sec: number;
    status: string;
    retry_count: number;
    viral_score: number;
    brand_slot: string;
    scene_count: number;
    scenes: Array<{
        scene_num: number;
        duration_sec: number;
        narration: string;
        visual_description: string;
        text_overlay: string;
        assigned_character_id?: string;
        assigned_character_name?: string;
        image_url: string;
        image_status: string;
        image_error: string;
        image_provider?: string;
        image_provider_label?: string;
    }>;
    last_error: string;
};

type LongFormReviewState = {
    total_chapters: number;
    approved_chapters: number;
    pending_chapters: number;
    all_approved: boolean;
    viral_score_total: number;
};

type LongFormDraftProgress = {
    total_chapters?: number;
    generated_chapters?: number;
    approved_chapters?: number;
    failed_chapters?: number;
    preview_scene_total?: number;
    preview_scene_generated?: number;
    stage?: string;
};

type LongFormCharacterReference = {
    character_id: string;
    name: string;
    reference_image_public_url?: string;
    reference_lock_mode?: string;
    reference_quality?: Record<string, any>;
    created_at?: number;
};

type ConnectedYouTubeChannel = {
    channel_id: string;
    title: string;
    channel_handle?: string;
    channel_url?: string;
    thumbnail_url?: string;
    subscriber_count?: number;
    video_count?: number;
    last_outcome_sync_at?: number;
    last_outcome_sync_count?: number;
    last_outcome_sync_error?: string;
    analytics_snapshot?: {
        channel_summary?: string;
        title_pattern_hints?: string[];
        recent_upload_titles?: string[];
        top_video_titles?: string[];
        packaging_learnings?: string[];
        retention_learnings?: string[];
        historical_compare?: {
            winner_vs_loser_summary?: string;
            next_moves?: string[];
            best_recent_video?: {
                title?: string;
                views?: number;
            };
            worst_recent_video?: {
                title?: string;
                views?: number;
            };
        };
    };
    last_sync_error?: string;
};

type LongFormSession = {
    session_id: string;
    template: string;
    format_preset: string;
    auto_pipeline: boolean;
    topic: string;
    input_title: string;
    input_description: string;
    source_url: string;
    youtube_channel_id?: string;
    analytics_notes: string;
    strategy_notes: string;
    target_minutes: number;
    language: string;
    resolution: string;
    animation_enabled: boolean;
    sfx_enabled: boolean;
    whisper_mode: string;
    status: string;
    job_id: string;
    paused_error: any;
    has_reference_image?: boolean;
    reference_image_uploaded?: boolean;
    reference_image_public_url?: string;
    reference_lock_mode?: string;
    character_references?: LongFormCharacterReference[];
    edit_blueprint?: Record<string, any>;
    learning_record?: Record<string, any>;
    latest_outcome?: Record<string, any>;
    channel_memory?: Record<string, any>;
    catalyst_preflight?: Record<string, any>;
    metadata_pack: {
        title_variants?: string[];
        description_variants?: string[];
        thumbnail_prompts?: string[];
        tags?: string[];
        source_video?: Record<string, any>;
        source_analysis?: Record<string, any>;
        youtube_channel?: Record<string, any>;
        selected_series_cluster?: Record<string, any>;
        source_context?: string;
        strategy_notes?: string;
        marketing_doctrine?: string[];
        analytics_evidence_summary?: string;
        analytics_asset_count?: number;
        manual_transcript_supplied?: boolean;
        manual_transcript_excerpt?: string;
        analytics_notes_effective?: string;
    };
    chapters: LongFormChapter[];
    review_state: LongFormReviewState;
    draft_progress?: LongFormDraftProgress;
    created_at: number;
    updated_at: number;
    package: {
        output_file?: string;
        chapters?: Array<{ index: number; title: string; start_sec: number; end_sec: number }>;
        title_variants?: string[];
        description_variants?: string[];
        thumbnail_prompts?: string[];
        tags?: string[];
        selected_title?: string;
        selected_description?: string;
        selected_tags?: string[];
        thumbnail_prompt?: string;
        thumbnail_file?: string;
        thumbnail_url?: string;
        thumbnail_error?: string;
    };
};

type LongFormSessionSummary = {
    session_id: string;
    template: string;
    format_preset: string;
    auto_pipeline: boolean;
    topic: string;
    input_title: string;
    source_url: string;
    target_minutes: number;
    language: string;
    resolution: string;
    status: string;
    job_id: string;
    review_state: LongFormReviewState;
    draft_progress?: LongFormDraftProgress;
    paused_error: any;
    preview_image_url: string;
    output_file: string;
    created_at: number;
    updated_at: number;
};

type LongFormTemplate = 'story' | 'skeleton';
type LongFormPreset = 'recap' | 'explainer' | 'documentary' | 'story_channel';

const PRESET_LABELS: Record<LongFormPreset, string> = {
    recap: 'Recap',
    explainer: 'Explainer',
    documentary: 'Documentary',
    story_channel: 'Story Channel',
};

const MARKETING_DOCTRINE_POINTS = [
    "Be active in the Daily Marketing Channel.",
    "Analyze and Improve. Evaluate each marketing piece to understand what works and what doesn't. Think about how you could improve it.",
    "Small, daily improvements in your marketing skills can lead to significant progress over time due to compounding.",
    "Just like in boxing or other martial arts, consistent practice and real-world application are crucial for mastering marketing.",
    "Engage with the daily challenges to continuously hone your skills. Missing a day occasionally is okay, but don't make it a habit.",
    "Regardless of your field or business, understanding and practicing marketing is fundamental to success.",
    "Treat the daily marketing challenges seriously and make it a part of your routine to see substantial benefits in your marketing abilities.",
    "Mastering marketing has enabled Arno to start and scale companies and avoid manual labor by understanding how to attract clients and improve businesses.",
    "It is a long-lasting skill. Marketing has been around for millennia and will continue to be valuable in the future.",
    "Anyone can learn it. It doesn't require special skills, abilities, or connections. Pay attention, focus, and you can succeed.",
    "High ROI (Return On Investment). Direct response marketing offers the highest and most reliable return on investment, outperforming traditional investments.",
    "Learning marketing helps you see opportunities and gaps that others miss, making life easier.",
    "You don't need to be the world's best marketer; being better than most is enough to succeed.",
    "It is a fast skill to learn. With ten days of dedicated study, you can acquire valuable marketing skills.",
    "Be ready for a significant change as you learn and apply these marketing skills.",
];

const STATUS_LABELS: Record<string, string> = {
    bootstrapping: 'Analyzing Source + Building Brief',
    awaiting_previous_approval: 'Waiting For Prior Chapter Approval',
    draft_generating: 'Generating Draft Chapters',
    draft_generating_images: 'Script Locked, Generating Images',
    draft_review: 'Draft Review',
    rendering: 'Rendering',
    complete: 'Complete',
    paused_needs_fix: 'Paused - Needs Fix',
    error: 'Error',
};

function chapterStatusClass(status: string): string {
    const s = String(status || '').toLowerCase();
    if (s === 'approved') return 'text-emerald-300 border-emerald-400/40 bg-emerald-500/10';
    if (s === 'regenerating' || s === 'draft_generating_images') return 'text-amber-300 border-amber-400/40 bg-amber-500/10';
    if (s === 'awaiting_previous_approval') return 'text-sky-300 border-sky-400/40 bg-sky-500/10';
    return 'text-gray-300 border-white/[0.16] bg-white/[0.03]';
}

function joinOutcomeLines(values: any): string {
    if (!Array.isArray(values)) return '';
    return values.map((value) => String(value || '').trim()).filter(Boolean).join('\n');
}

function splitOutcomeLines(value: string): string[] {
    return String(value || '')
        .split(/\r?\n/)
        .map((item) => item.trim())
        .filter(Boolean);
}

function fileToDataUrl(file: File): Promise<string> {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(String(reader.result || ''));
        reader.onerror = () => reject(new Error('Failed to read reference image'));
        reader.readAsDataURL(file);
    });
}

export default function LongFormPanel() {
    const { session, ownerOverride, longformOwnerBeta } = useContext(AuthContext);
    const [activeTab, setActiveTab] = useState<'create' | 'projects'>('create');
    const [template, setTemplate] = useState<LongFormTemplate>('story');
    const [formatPreset, setFormatPreset] = useState<LongFormPreset>('documentary');
    const [topic, setTopic] = useState('');
    const [inputTitle, setInputTitle] = useState('');
    const [inputDescription, setInputDescription] = useState('');
    const [sourceUrl, setSourceUrl] = useState('');
    const [youtubeChannelId, setYoutubeChannelId] = useState('');
    const [analyticsNotes, setAnalyticsNotes] = useState('');
    const [transcriptText, setTranscriptText] = useState('');
    const [analyticsImages, setAnalyticsImages] = useState<File[]>([]);
    const [subjectReferenceImage, setSubjectReferenceImage] = useState<File | null>(null);
    const [characterReferenceName, setCharacterReferenceName] = useState('');
    const [characterReferenceImage, setCharacterReferenceImage] = useState<File | null>(null);
    const [subjectReferenceAttached, setSubjectReferenceAttached] = useState(false);
    const [applyMarketingDoctrine, setApplyMarketingDoctrine] = useState(true);
    const [targetMinutes, setTargetMinutes] = useState(8);
    const [language, setLanguage] = useState('en');
    const [languages, setLanguages] = useState<Array<{ code: string; name: string }>>([{ code: 'en', name: 'English' }]);
    const [animationEnabled, setAnimationEnabled] = useState(true);
    const [sfxEnabled, setSfxEnabled] = useState(true);
    const [whisperMode, setWhisperMode] = useState<'off' | 'subtle' | 'cinematic'>('subtle');
    const [lfSession, setLfSession] = useState<LongFormSession | null>(null);
    const [jobStatus, setJobStatus] = useState<any>(null);
    const [creating, setCreating] = useState(false);
    const [refreshing, setRefreshing] = useState(false);
    const [finalizing, setFinalizing] = useState(false);
    const [stopping, setStopping] = useState(false);
    const [uploadingReference, setUploadingReference] = useState(false);
    const [uploadingCharacterReference, setUploadingCharacterReference] = useState(false);
    const [actionBusy, setActionBusy] = useState('');
    const [error, setError] = useState('');
    const [projectsError, setProjectsError] = useState('');
    const [chapterReasons, setChapterReasons] = useState<Record<number, string>>({});
    const [fixNote, setFixNote] = useState('');
    const [autoPipeline, setAutoPipeline] = useState(false);
    const [youtubeChannels, setYoutubeChannels] = useState<ConnectedYouTubeChannel[]>([]);
    const [youtubeLoading, setYoutubeLoading] = useState(false);
    const [youtubeError, setYoutubeError] = useState('');
    const [youtubeConnecting, setYoutubeConnecting] = useState(false);
    const [youtubeOutcomeSyncing, setYoutubeOutcomeSyncing] = useState(false);
    const [sessionIdInput, setSessionIdInput] = useState('');
    const [projectSessions, setProjectSessions] = useState<LongFormSessionSummary[]>([]);
    const [projectsLoading, setProjectsLoading] = useState(false);
    const [outcomeVideoUrl, setOutcomeVideoUrl] = useState('');
    const [outcomeViews, setOutcomeViews] = useState('');
    const [outcomeImpressions, setOutcomeImpressions] = useState('');
    const [outcomeAvd, setOutcomeAvd] = useState('');
    const [outcomeAvp, setOutcomeAvp] = useState('');
    const [outcomeCtr, setOutcomeCtr] = useState('');
    const [outcomeFirst30, setOutcomeFirst30] = useState('');
    const [outcomeFirst60, setOutcomeFirst60] = useState('');
    const [outcomeSummary, setOutcomeSummary] = useState('');
    const [outcomeStrongSignals, setOutcomeStrongSignals] = useState('');
    const [outcomeWeakPoints, setOutcomeWeakPoints] = useState('');
    const [outcomeHookWins, setOutcomeHookWins] = useState('');
    const [outcomeHookWatchouts, setOutcomeHookWatchouts] = useState('');
    const [outcomePacingWins, setOutcomePacingWins] = useState('');
    const [outcomePacingWatchouts, setOutcomePacingWatchouts] = useState('');
    const [outcomeVisualWins, setOutcomeVisualWins] = useState('');
    const [outcomeVisualWatchouts, setOutcomeVisualWatchouts] = useState('');
    const [outcomeSoundWins, setOutcomeSoundWins] = useState('');
    const [outcomeSoundWatchouts, setOutcomeSoundWatchouts] = useState('');
    const [outcomePackagingWins, setOutcomePackagingWins] = useState('');
    const [outcomePackagingWatchouts, setOutcomePackagingWatchouts] = useState('');
    const [outcomeRetentionWins, setOutcomeRetentionWins] = useState('');
    const [outcomeRetentionWatchouts, setOutcomeRetentionWatchouts] = useState('');
    const [outcomeNextMoves, setOutcomeNextMoves] = useState('');
    const [outcomeSaving, setOutcomeSaving] = useState(false);
    const [outcomeAutoSaving, setOutcomeAutoSaving] = useState(false);
    const [renderMonitorDismissed, setRenderMonitorDismissed] = useState(false);
    const restoredSessionUserRef = useRef('');
    const outcomeSeedRef = useRef('');
    const prefillAppliedRef = useRef(false);
    const pendingPrefillChannelHintRef = useRef('');

    const lastSessionStorageKey = useMemo(() => {
        const uid = String(session?.user?.id || 'guest').trim() || 'guest';
        return `nyptid_longform_last_session_${uid}`;
    }, [session?.user?.id]);
    const pendingLaunchStorageKey = useMemo(() => {
        const uid = String(session?.user?.id || 'guest').trim() || 'guest';
        return `nyptid_longform_launch_session_${uid}`;
    }, [session?.user?.id]);

    const persistSessionId = useCallback((value: string) => {
        try {
            const sid = String(value || '').trim();
            if (sid) {
                sessionStorage.setItem(lastSessionStorageKey, sid);
            } else {
                sessionStorage.removeItem(lastSessionStorageKey);
            }
        } catch {
            // ignore storage errors
        }
    }, [lastSessionStorageKey]);

    const authHeaders = useMemo(() => {
        const out: Record<string, string> = { 'Content-Type': 'application/json' };
        if (session) out.Authorization = `Bearer ${session.access_token}`;
        return out;
    }, [session]);

    const authOnlyHeaders = useMemo(() => {
        const out: Record<string, string> = {};
        if (session) out.Authorization = `Bearer ${session.access_token}`;
        return out;
    }, [session]);

    useEffect(() => {
        const paused = lfSession?.paused_error;
        if (!paused) {
            setFixNote('');
            return;
        }
        const suggested = String(paused?.suggested_fix_note || '').trim();
        if (!suggested) return;
        setFixNote((current) => (String(current || '').trim() ? current : suggested));
    }, [lfSession?.paused_error, lfSession?.session_id]);

    useEffect(() => {
        setSubjectReferenceAttached(Boolean(lfSession?.has_reference_image || lfSession?.reference_image_uploaded));
    }, [lfSession?.has_reference_image, lfSession?.reference_image_uploaded, lfSession?.session_id]);

    const apiCall = useCallback(async (path: string, init: RequestInit = {}) => {
        const merged: RequestInit = {
            ...init,
            headers: {
                ...(init.headers || {}),
                ...(authHeaders || {}),
            },
        };
        const res = await fetch(`${API}${path}`, merged);
        const payload = await res.json().catch(() => ({}));
        if (!res.ok) {
            throw new Error(String((payload as any).detail || `Request failed (${res.status})`));
        }
        return payload;
    }, [authHeaders]);

    const apiCallFormData = useCallback(async (path: string, body: FormData) => {
        const res = await fetch(`${API}${path}`, {
            method: 'POST',
            headers: authOnlyHeaders,
            body,
        });
        const payload = await res.json().catch(() => ({}));
        if (!res.ok) {
            throw new Error(String((payload as any).detail || `Request failed (${res.status})`));
        }
        return payload;
    }, [authOnlyHeaders]);

    const loadYouTubeChannels = useCallback(async (silent = false, preferredChannelId = '') => {
        if (!session) return;
        if (!silent) setYoutubeLoading(true);
        setYoutubeError('');
        try {
            const res = await fetch(`${API}/api/youtube/channels?sync=true`, {
                headers: authOnlyHeaders,
            });
            const payload = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(String((payload as any).detail || `Request failed (${res.status})`));
            const rows = Array.isArray((payload as any).channels) ? (payload as any).channels as ConnectedYouTubeChannel[] : [];
            setYoutubeChannels(rows);
            const defaultId = String((payload as any).default_channel_id || '').trim();
            const preferredId = String(preferredChannelId || '').trim();
            setYoutubeChannelId((current) => {
                const currentId = String(current || '').trim();
                const keepId = preferredId || currentId;
                if (keepId && rows.some((row) => String(row.channel_id || '').trim() === keepId)) {
                    return keepId;
                }
                if (defaultId && rows.some((row) => String(row.channel_id || '').trim() === defaultId)) {
                    return defaultId;
                }
                if (rows.length > 0) {
                    return String(rows[0]?.channel_id || '').trim();
                }
                return '';
            });
        } catch (e: any) {
            setYoutubeChannels([]);
            setYoutubeError(e?.message || 'Failed to load connected YouTube channels');
        } finally {
            if (!silent) setYoutubeLoading(false);
        }
    }, [authOnlyHeaders, session]);

    const persistSelectedYouTubeChannel = useCallback(async (nextChannelId: string) => {
        const normalizedId = String(nextChannelId || '').trim();
        setYoutubeChannelId(normalizedId);
        if (!session || !normalizedId) return;
        setYoutubeError('');
        try {
            const payload = await apiCall('/api/youtube/channels/select', {
                method: 'POST',
                body: JSON.stringify({ channel_id: normalizedId }),
            });
            const updatedChannel = (payload as any).channel as ConnectedYouTubeChannel | undefined;
            if (updatedChannel && String(updatedChannel.channel_id || '').trim()) {
                setYoutubeChannels((current) => {
                    const nextRows = [...current];
                    const existingIndex = nextRows.findIndex(
                        (row) => String(row.channel_id || '').trim() === String(updatedChannel.channel_id || '').trim(),
                    );
                    if (existingIndex >= 0) {
                        nextRows[existingIndex] = updatedChannel;
                    } else {
                        nextRows.unshift(updatedChannel);
                    }
                    return nextRows;
                });
            }
        } catch (e: any) {
            setYoutubeError(e?.message || 'Failed to save connected YouTube channel selection');
        }
    }, [apiCall, session]);

    const startYouTubeConnect = useCallback(async () => {
        if (!session) return;
        setYoutubeConnecting(true);
        setYoutubeError('');
        try {
            startYouTubeBrowserConnect(session.access_token, window.location.href);
        } catch (e: any) {
            setYoutubeError(e?.message || 'Failed to start Google YouTube connection');
            setYoutubeConnecting(false);
        }
    }, [session]);

    const refreshStatus = useCallback(async (id?: string, silent = false) => {
        const targetId = String(id || lfSession?.session_id || '').trim();
        if (!targetId) return;
        if (!silent) setRefreshing(true);
        setError('');
        try {
            const payload = await apiCall(`/api/longform/session/${targetId}/status`);
            setLfSession((payload as any).session || null);
            setJobStatus((payload as any).job || null);
            setSessionIdInput(targetId);
            persistSessionId(targetId);
        } catch (e: any) {
            setLfSession(null);
            setJobStatus(null);
            setError(e?.message || 'Failed to load long-form status');
        } finally {
            if (!silent) setRefreshing(false);
        }
    }, [apiCall, lfSession?.session_id, persistSessionId]);

    const syncConnectedChannelOutcomes = useCallback(async (sessionScoped = false) => {
        const selectedChannelId = String(youtubeChannelId || '').trim();
        if (!session || !selectedChannelId) return;
        setYoutubeOutcomeSyncing(true);
        setYoutubeError('');
        try {
            const payload = await apiCall(`/api/youtube/channels/${selectedChannelId}/sync-outcomes`, {
                method: 'POST',
                body: JSON.stringify({
                    session_id: sessionScoped ? String(lfSession?.session_id || '').trim() : '',
                    candidate_limit: 18,
                    refresh_existing: false,
                }),
            });
            const syncedSession = (payload as any).session as LongFormSession | undefined;
            if (syncedSession && String(syncedSession.session_id || '').trim()) {
                setLfSession(syncedSession);
                setSessionIdInput(String(syncedSession.session_id || '').trim());
            } else if (lfSession?.session_id && sessionScoped) {
                await refreshStatus(lfSession.session_id, true);
            }
            await loadYouTubeChannels(true, selectedChannelId);
        } catch (e: any) {
            setYoutubeError(e?.message || 'Failed to sync published channel outcomes');
        } finally {
            setYoutubeOutcomeSyncing(false);
        }
    }, [apiCall, lfSession?.session_id, loadYouTubeChannels, refreshStatus, session, youtubeChannelId]);

    const loadProjects = useCallback(async () => {
        if (!session) return;
        setProjectsLoading(true);
        setProjectsError('');
        try {
            const payload = await apiCall('/api/longform/sessions?limit=50');
            const rows = Array.isArray((payload as any).sessions) ? (payload as any).sessions : [];
            setProjectSessions(rows);
        } catch (e: any) {
            setProjectSessions([]);
            setProjectsError(e?.message || 'Failed to load long-form projects');
        } finally {
            setProjectsLoading(false);
        }
    }, [apiCall, session]);

    const openProjectSession = useCallback(async (sessionId: string) => {
        const sid = String(sessionId || '').trim();
        if (!sid) return;
        setActiveTab('create');
        setSessionIdInput(sid);
        persistSessionId(sid);
        await refreshStatus(sid, false);
    }, [persistSessionId, refreshStatus]);

    useEffect(() => {
        (async () => {
            try {
                const res = await fetch(`${API}/api/languages`);
                if (!res.ok) return;
                const data = await res.json().catch(() => ({}));
                if (Array.isArray(data.languages) && data.languages.length > 0) {
                    setLanguages(data.languages);
                }
            } catch {
                // ignore
            }
        })();
    }, []);

    useEffect(() => {
        if (!lfSession?.session_id) return;
        const id = setInterval(() => {
            refreshStatus(lfSession.session_id, true);
        }, 5000);
        return () => clearInterval(id);
    }, [lfSession?.session_id, refreshStatus]);

    useEffect(() => {
        if (activeTab !== 'projects') return;
        void loadProjects();
    }, [activeTab, loadProjects]);

    useEffect(() => {
        if (!session) return;
        void loadYouTubeChannels(true);
    }, [session, loadYouTubeChannels]);

    useEffect(() => {
        if (prefillAppliedRef.current || typeof window === 'undefined') return;
        const params = new URLSearchParams(window.location.search);
        const hasPrefill = Array.from(params.keys()).some((key) => key.startsWith('lf_'));
        if (!hasPrefill) return;

        const templateParam = String(params.get('lf_template') || '').trim().toLowerCase();
        const formatParam = String(params.get('lf_format') || '').trim().toLowerCase();
        const topicParam = String(params.get('lf_topic') || '').trim();
        const titleParam = String(params.get('lf_title') || '').trim();
        const descriptionParam = String(params.get('lf_description') || '').trim();
        const sourceParam = String(params.get('lf_source') || '').trim();
        const channelHint = String(params.get('lf_channel') || '').trim();
        const autoParam = String(params.get('lf_auto') || '').trim().toLowerCase();
        const minutesRaw = Number(params.get('lf_minutes') || '');

        if (templateParam === 'story' || templateParam === 'skeleton') {
            setTemplate(templateParam);
        }
        if (formatParam === 'recap' || formatParam === 'explainer' || formatParam === 'documentary' || formatParam === 'story_channel') {
            setFormatPreset(formatParam);
        }
        if (topicParam) setTopic(topicParam);
        if (titleParam) setInputTitle(titleParam);
        if (descriptionParam) setInputDescription(descriptionParam);
        if (sourceParam) setSourceUrl(sourceParam);
        if (Number.isFinite(minutesRaw) && minutesRaw > 0) {
            setTargetMinutes(Math.max(2, Math.min(30, minutesRaw)));
        }
        if (autoParam) {
            setAutoPipeline(['1', 'true', 'yes', 'on'].includes(autoParam));
        }
        if (channelHint) {
            pendingPrefillChannelHintRef.current = channelHint;
        }
        setActiveTab('create');
        prefillAppliedRef.current = true;

        const cleanup = new URL(window.location.href);
        ['lf_template', 'lf_format', 'lf_topic', 'lf_title', 'lf_description', 'lf_source', 'lf_channel', 'lf_minutes', 'lf_auto'].forEach((key) => {
            cleanup.searchParams.delete(key);
        });
        window.history.replaceState({}, '', cleanup.toString());
    }, []);

    useEffect(() => {
        const hintRaw = String(pendingPrefillChannelHintRef.current || '').trim();
        if (!hintRaw || youtubeChannels.length === 0) return;
        const normalizedHint = hintRaw.toLowerCase();
        const match = youtubeChannels.find((row) => {
            const channelId = String(row.channel_id || '').trim().toLowerCase();
            const title = String(row.title || '').trim().toLowerCase();
            const handle = String(row.channel_handle || '').trim().toLowerCase();
            const url = String(row.channel_url || '').trim().toLowerCase();
            return channelId === normalizedHint
                || title === normalizedHint
                || handle === normalizedHint
                || title.includes(normalizedHint)
                || handle.includes(normalizedHint)
                || url.includes(normalizedHint);
        });
        if (!match) return;
        setYoutubeChannelId(String(match.channel_id || '').trim());
        pendingPrefillChannelHintRef.current = '';
    }, [youtubeChannels]);

    useEffect(() => {
        const uid = String(session?.user?.id || '').trim();
        if (!uid) return;
        let pendingSessionId = '';
        try {
            pendingSessionId = String(sessionStorage.getItem(pendingLaunchStorageKey) || '').trim();
            if (pendingSessionId) {
                sessionStorage.removeItem(pendingLaunchStorageKey);
            }
        } catch {
            pendingSessionId = '';
        }
        if (!pendingSessionId) return;
        restoredSessionUserRef.current = uid;
        setActiveTab('create');
        setSessionIdInput(pendingSessionId);
        persistSessionId(pendingSessionId);
        void refreshStatus(pendingSessionId, false);
    }, [pendingLaunchStorageKey, persistSessionId, refreshStatus, session?.user?.id]);

    useEffect(() => {
        const uid = String(session?.user?.id || '').trim();
        if (!uid) return;
        if (restoredSessionUserRef.current === uid) return;
        restoredSessionUserRef.current = uid;
        setLfSession(null);
        setJobStatus(null);
        setSessionIdInput('');
        setError('');
    }, [session?.user?.id]);

    useEffect(() => {
        if (!lfSession?.session_id) return;
        persistSessionId(lfSession.session_id);
    }, [lfSession?.session_id, persistSessionId]);

    useEffect(() => {
        if (ownerOverride) {
            setAutoPipeline(true);
        }
    }, [ownerOverride]);

    const canUseDeepAnalysis = Boolean(ownerOverride || longformOwnerBeta);
    const missingManualBrief = !topic.trim() || !inputTitle.trim() || !inputDescription.trim();
    const hasDeepAnalysisInputs = Boolean(
        sourceUrl.trim()
        || youtubeChannelId.trim()
        || analyticsNotes.trim()
        || transcriptText.trim()
        || analyticsImages.length > 0
    );
    const deepAnalysisRequested = Boolean(
        hasDeepAnalysisInputs
        || (ownerOverride && autoPipeline)
    );
    const canCreateFromSourceOnly = canUseDeepAnalysis && Boolean(sourceUrl.trim());
    const canCreateFromDeepAnalysisOnly = canUseDeepAnalysis && hasDeepAnalysisInputs;
    const createDisabled = creating || (!canCreateFromDeepAnalysisOnly && missingManualBrief);

    const createSession = useCallback(async () => {
        if (!session) return;
        if (deepAnalysisRequested && !canUseDeepAnalysis) {
            setError('Source-video deep analysis is owner beta for now. Public Long Form stays on the lighter manual workflow while Catalyst is being tuned.');
            return;
        }
        setCreating(true);
        setError('');
        setLfSession(null);
        setJobStatus(null);
        setSessionIdInput('');
        persistSessionId('');
        try {
            const formattedDescription = inputDescription.trim()
                ? `Format preset: ${PRESET_LABELS[formatPreset]}. ${inputDescription.trim()}`.trim()
                : '';
            const referenceImageDataUrl = subjectReferenceImage ? await fileToDataUrl(subjectReferenceImage) : '';
            const useBootstrapRoute = Boolean(canUseDeepAnalysis && deepAnalysisRequested);
            const payload = useBootstrapRoute
                ? await (() => {
                    const formData = new FormData();
                    formData.append('template', template);
                    formData.append('topic', topic.trim());
                    formData.append('input_title', inputTitle.trim());
                    formData.append('input_description', formattedDescription);
                    formData.append('format_preset', formatPreset);
                    formData.append('source_url', sourceUrl.trim());
                    formData.append('youtube_channel_id', youtubeChannelId.trim());
                    formData.append('analytics_notes', analyticsNotes.trim());
                    formData.append('strategy_notes', applyMarketingDoctrine ? MARKETING_DOCTRINE_POINTS.join('\n') : '');
                    formData.append('transcript_text', transcriptText.trim());
                    formData.append('auto_pipeline', ownerOverride && autoPipeline ? 'true' : 'false');
                    formData.append('target_minutes', String(targetMinutes));
                    formData.append('language', language);
                    formData.append('animation_enabled', animationEnabled ? 'true' : 'false');
                    formData.append('sfx_enabled', sfxEnabled ? 'true' : 'false');
                    formData.append('whisper_mode', whisperMode);
                    formData.append('reference_lock_mode', 'strict');
                    if (subjectReferenceImage) {
                        formData.append('subject_reference_image', subjectReferenceImage);
                    }
                    analyticsImages.forEach((file) => formData.append('analytics_images', file));
                    return apiCallFormData('/api/longform/session/bootstrap', formData);
                })()
                : await apiCall('/api/longform/session', {
                    method: 'POST',
                    body: JSON.stringify({
                        template,
                        topic: topic.trim(),
                        input_title: inputTitle.trim(),
                        input_description: formattedDescription,
                        format_preset: formatPreset,
                        source_url: '',
                        youtube_channel_id: '',
                        analytics_notes: '',
                        strategy_notes: applyMarketingDoctrine ? MARKETING_DOCTRINE_POINTS.join('\n') : '',
                        transcript_text: '',
                        auto_pipeline: false,
                        target_minutes: targetMinutes,
                        language,
                        animation_enabled: animationEnabled,
                        sfx_enabled: sfxEnabled,
                        whisper_mode: whisperMode,
                        reference_image_url: referenceImageDataUrl,
                        reference_lock_mode: 'strict',
                    }),
                });
            const created = (payload as any).session as LongFormSession;
            setLfSession(created);
            setJobStatus(null);
            setSessionIdInput(created?.session_id || '');
            persistSessionId(created?.session_id || '');
            setChapterReasons({});
            setSubjectReferenceAttached(Boolean(created?.has_reference_image || created?.reference_image_uploaded || subjectReferenceImage));
        } catch (e: any) {
            setError(e?.message || 'Failed to create long-form session');
        } finally {
            setCreating(false);
        }
    }, [
        animationEnabled,
        apiCall,
        apiCallFormData,
        autoPipeline,
        canUseDeepAnalysis,
        deepAnalysisRequested,
        inputDescription,
        inputTitle,
        language,
        analyticsNotes,
        analyticsImages,
        applyMarketingDoctrine,
        ownerOverride,
        persistSessionId,
        session,
        sourceUrl,
        sfxEnabled,
        subjectReferenceImage,
        targetMinutes,
        template,
        topic,
        transcriptText,
        formatPreset,
        whisperMode,
    ]);

    const attachSubjectReferenceToSession = useCallback(async () => {
        if (!lfSession?.session_id || !subjectReferenceImage) return;
        setUploadingReference(true);
        setError('');
        try {
            const formData = new FormData();
            formData.append('reference_image', subjectReferenceImage);
            formData.append('reference_lock_mode', 'strict');
            const payload = await apiCallFormData(`/api/longform/session/${lfSession.session_id}/reference-image`, formData);
            const updated = (payload as any).session as LongFormSession;
            setLfSession(updated);
            setSubjectReferenceAttached(Boolean(updated?.has_reference_image || updated?.reference_image_uploaded));
        } catch (e: any) {
            setError(e?.message || 'Failed to attach subject reference image');
        } finally {
            setUploadingReference(false);
        }
    }, [apiCallFormData, lfSession?.session_id, subjectReferenceImage]);

    const uploadCharacterReferenceToSession = useCallback(async () => {
        if (!lfSession?.session_id || !characterReferenceImage || !String(characterReferenceName || '').trim()) return;
        setUploadingCharacterReference(true);
        setError('');
        try {
            const formData = new FormData();
            formData.append('character_name', String(characterReferenceName || '').trim());
            formData.append('reference_image', characterReferenceImage);
            formData.append('reference_lock_mode', 'strict');
            const payload = await apiCallFormData(`/api/longform/session/${lfSession.session_id}/character-reference`, formData);
            const updated = (payload as any).session as LongFormSession;
            setLfSession(updated);
            setCharacterReferenceName('');
            setCharacterReferenceImage(null);
        } catch (e: any) {
            setError(e?.message || 'Failed to add character reference');
        } finally {
            setUploadingCharacterReference(false);
        }
    }, [apiCallFormData, characterReferenceImage, characterReferenceName, lfSession?.session_id]);

    const saveSceneCharacterAssignment = useCallback(async (chapterIndex: number, sceneNum: number, characterId: string) => {
        if (!lfSession?.session_id) return;
        const busyKey = `scene-assignment:${chapterIndex}:${sceneNum}`;
        setActionBusy(busyKey);
        setError('');
        try {
            const payload = await apiCall(`/api/longform/session/${lfSession.session_id}/scene-assignment`, {
                method: 'POST',
                body: JSON.stringify({
                    chapter_index: chapterIndex,
                    scene_num: sceneNum,
                    character_id: String(characterId || '').trim(),
                }),
            });
            setLfSession((payload as any).session || null);
        } catch (e: any) {
            setError(e?.message || 'Failed to save scene character assignment');
        } finally {
            setActionBusy('');
        }
    }, [apiCall, lfSession?.session_id]);

    const chapterAction = useCallback(async (chapterIndex: number, action: 'approve' | 'regenerate') => {
        if (!lfSession?.session_id) return;
        const busyKey = `${action}:${chapterIndex}`;
        setActionBusy(busyKey);
        setError('');
        try {
            const payload = await apiCall(`/api/longform/session/${lfSession.session_id}/chapter-action`, {
                method: 'POST',
                body: JSON.stringify({
                    chapter_index: chapterIndex,
                    action,
                    reason: String(chapterReasons[chapterIndex] || '').trim(),
                }),
            });
            setLfSession((payload as any).session || null);
        } catch (e: any) {
            setError(e?.message || `Failed to ${action} chapter`);
        } finally {
            setActionBusy('');
        }
    }, [apiCall, chapterReasons, lfSession?.session_id]);

    const resolvePausedError = useCallback(async (forceAccept: boolean) => {
        if (!lfSession?.session_id || !lfSession?.paused_error) return;
        const chapterIndex = Number(lfSession.paused_error.chapter_index || 0);
        setActionBusy(`resolve:${chapterIndex}`);
        setError('');
        try {
            const payload = await apiCall(`/api/longform/session/${lfSession.session_id}/resolve-error`, {
                method: 'POST',
                body: JSON.stringify({
                    chapter_index: chapterIndex,
                    fix_note: String(fixNote || '').trim(),
                    force_accept: forceAccept,
                }),
            });
            setLfSession((payload as any).session || null);
            setFixNote('');
        } catch (e: any) {
            setError(e?.message || 'Failed to resolve paused error');
        } finally {
            setActionBusy('');
        }
    }, [apiCall, fixNote, lfSession]);

    const finalizeSession = useCallback(async () => {
        if (!lfSession?.session_id) return;
        setFinalizing(true);
        setError('');
        try {
            await apiCall(`/api/longform/session/${lfSession.session_id}/finalize`, { method: 'POST' });
            await refreshStatus(lfSession.session_id, false);
        } catch (e: any) {
            setError(e?.message || 'Failed to finalize long-form session');
        } finally {
            setFinalizing(false);
        }
    }, [apiCall, lfSession?.session_id, refreshStatus]);

    const stopSession = useCallback(async () => {
        if (!lfSession?.session_id) return;
        setStopping(true);
        setError('');
        try {
            await apiCall(`/api/longform/session/${lfSession.session_id}/stop`, { method: 'POST' });
            await refreshStatus(lfSession.session_id, false);
        } catch (e: any) {
            setError(e?.message || 'Failed to stop long-form session');
        } finally {
            setStopping(false);
        }
    }, [apiCall, lfSession?.session_id, refreshStatus]);

    const outputFile = String(jobStatus?.output_file || lfSession?.package?.output_file || '');
    const outputUrl = outputFile ? `${API}/api/download/${outputFile}` : '';
    const hasPublishPackage = Boolean(outputFile);
    const review = lfSession?.review_state;
    const draftProgress = lfSession?.draft_progress;
    const sourceVideo = (lfSession?.metadata_pack?.source_video || {}) as Record<string, any>;
    const sourceAnalysis = (lfSession?.metadata_pack?.source_analysis || {}) as Record<string, any>;
    const connectedYouTubeChannel = (lfSession?.metadata_pack?.youtube_channel || {}) as Record<string, any>;
    const characterReferences = Array.isArray(lfSession?.character_references)
        ? (lfSession?.character_references as LongFormCharacterReference[]).filter((value) => String(value?.character_id || '').trim())
        : [];
    const selectedSeriesCluster = (lfSession?.metadata_pack?.selected_series_cluster || {}) as Record<string, any>;
    const selectedSeriesKeywords = Array.isArray(selectedSeriesCluster?.keywords)
        ? selectedSeriesCluster.keywords.filter((value: any) => String(value || '').trim())
        : [];
    const selectedSeriesTitles = Array.isArray(selectedSeriesCluster?.sample_titles)
        ? selectedSeriesCluster.sample_titles.filter((value: any) => String(value || '').trim())
        : [];
    const channelMemory = (lfSession?.channel_memory || {}) as Record<string, any>;
    const latestOutcome = (lfSession?.latest_outcome || {}) as Record<string, any>;
    const referenceComparison = (latestOutcome?.reference_comparison || {}) as Record<string, any>;
    const referenceScores = (referenceComparison?.scores || {}) as Record<string, any>;
    const rewritePressure = (channelMemory?.rewrite_pressure || {}) as Record<string, any>;
    const catalystPreflight = (lfSession?.catalyst_preflight || {}) as Record<string, any>;
    const rewriteCategories = Array.isArray(rewritePressure?.categories)
        ? rewritePressure.categories.filter((value: any) => value && String(value.key || '').trim())
        : [];
    const rewritePriorities = Array.isArray(rewritePressure?.next_run_priorities)
        ? rewritePressure.next_run_priorities.filter((value: any) => String(value || '').trim())
        : [];
    const preflightStatus = String(catalystPreflight?.status || '').trim().toLowerCase();
    const preflightReadinessScore = Number(catalystPreflight?.readiness_score || 0);
    const preflightBlockers = Array.isArray(catalystPreflight?.blockers)
        ? catalystPreflight.blockers.filter((value: any) => String(value || '').trim())
        : [];
    const preflightWarnings = Array.isArray(catalystPreflight?.warnings)
        ? catalystPreflight.warnings.filter((value: any) => String(value || '').trim())
        : [];
    const preflightStrengths = Array.isArray(catalystPreflight?.strengths)
        ? catalystPreflight.strengths.filter((value: any) => String(value || '').trim())
        : [];
    const preflightNextFixes = Array.isArray(catalystPreflight?.next_fixes)
        ? catalystPreflight.next_fixes.filter((value: any) => String(value || '').trim())
        : [];
    const referenceChannels = Array.isArray(referenceComparison?.benchmark_channels)
        ? referenceComparison.benchmark_channels.filter((value: any) => String(value || '').trim())
        : [];
    const titleVariants = hasPublishPackage && Array.isArray(lfSession?.package?.title_variants) ? lfSession?.package?.title_variants as string[] : [];
    const descriptionVariants = hasPublishPackage && Array.isArray(lfSession?.package?.description_variants) ? lfSession?.package?.description_variants as string[] : [];
    const thumbnailPrompts = hasPublishPackage && Array.isArray(lfSession?.package?.thumbnail_prompts) ? lfSession?.package?.thumbnail_prompts as string[] : [];
    const publishTags = hasPublishPackage && Array.isArray(lfSession?.package?.tags) ? lfSession?.package?.tags as string[] : [];
    const selectedTitle = hasPublishPackage ? String(lfSession?.package?.selected_title || titleVariants[0] || '') : '';
    const selectedDescription = hasPublishPackage ? String(lfSession?.package?.selected_description || descriptionVariants[0] || '') : '';
    const selectedTags = hasPublishPackage && Array.isArray(lfSession?.package?.selected_tags) ? lfSession?.package?.selected_tags as string[] : publishTags;
    const resolveSceneImageUrl = useCallback((raw: string) => {
        const u = String(raw || '').trim();
        if (!u) return '';
        if (u.startsWith('http://') || u.startsWith('https://')) return u;
        return `${API}${u}`;
    }, []);
    const packageThumbnailUrl = hasPublishPackage ? resolveSceneImageUrl(String(lfSession?.package?.thumbnail_url || '')) : '';
    const packageThumbnailError = hasPublishPackage ? String(lfSession?.package?.thumbnail_error || '') : '';
    const flattenedRenderScenes = useMemo(() => {
        const chapters = Array.isArray(lfSession?.chapters) ? lfSession.chapters : [];
        return chapters.flatMap((chapter) => {
            const chapterIndex = Number(chapter?.index || 0);
            const chapterTitle = String(chapter?.title || `Chapter ${chapterIndex + 1}`);
            const scenes = Array.isArray(chapter?.scenes) ? chapter.scenes : [];
            return scenes.map((scene) => ({
                chapterIndex,
                chapterTitle,
                sceneNum: Number(scene?.scene_num || 0),
                imageUrl: resolveSceneImageUrl(String(scene?.image_url || '')),
            }));
        });
    }, [lfSession?.chapters, resolveSceneImageUrl]);
    const longformRenderPreview = useMemo(() => {
        const directPreviewUrl = resolveSceneImageUrl(String(jobStatus?.preview_url || ''));
        if (directPreviewUrl) {
            return {
                url: directPreviewUrl,
                kind: String(jobStatus?.preview_type || 'image') === 'video' ? 'video' as const : 'image' as const,
                label: String(jobStatus?.preview_label || 'Current long-form render preview'),
            };
        }
        const safeSceneIndex = Math.max(0, Number(jobStatus?.current_scene || 1) - 1);
        const candidate = flattenedRenderScenes[safeSceneIndex] || flattenedRenderScenes[flattenedRenderScenes.length - 1];
        if (candidate?.imageUrl) {
            return {
                url: candidate.imageUrl,
                kind: 'image' as const,
                label: `Chapter ${candidate.chapterIndex + 1} · Scene ${candidate.sceneNum || safeSceneIndex + 1} preview`,
            };
        }
        if (outputUrl) {
            return {
                url: outputUrl,
                kind: 'video' as const,
                label: 'Current long-form render output',
            };
        }
        return null;
    }, [flattenedRenderScenes, jobStatus?.current_scene, jobStatus?.preview_label, jobStatus?.preview_type, jobStatus?.preview_url, outputUrl, resolveSceneImageUrl]);
    const formatTimestamp = useCallback((value: number) => {
        const ts = Number(value || 0);
        if (ts <= 0) return 'Unknown';
        return new Date(ts * 1000).toLocaleString();
    }, []);

    useEffect(() => {
        if (lfSession?.status === 'rendering' && lfSession?.job_id) {
            setRenderMonitorDismissed(false);
        }
    }, [lfSession?.job_id, lfSession?.status]);

    useEffect(() => {
        const seedKey = `${String(lfSession?.session_id || '')}:${String(latestOutcome?.created_at || 0)}:${outputFile}`;
        if (!seedKey || outcomeSeedRef.current === seedKey) return;
        outcomeSeedRef.current = seedKey;
        setOutcomeVideoUrl(String(latestOutcome?.video_url || ''));
        setOutcomeViews(latestOutcome?.metrics?.views ? String(latestOutcome.metrics.views) : '');
        setOutcomeImpressions(latestOutcome?.metrics?.impressions ? String(latestOutcome.metrics.impressions) : '');
        setOutcomeAvd(latestOutcome?.metrics?.average_view_duration_sec ? String(latestOutcome.metrics.average_view_duration_sec) : '');
        setOutcomeAvp(latestOutcome?.metrics?.average_percentage_viewed ? String(latestOutcome.metrics.average_percentage_viewed) : '');
        setOutcomeCtr(latestOutcome?.metrics?.impression_click_through_rate ? String(latestOutcome.metrics.impression_click_through_rate) : '');
        setOutcomeFirst30(latestOutcome?.metrics?.first_30_sec_retention_pct ? String(latestOutcome.metrics.first_30_sec_retention_pct) : '');
        setOutcomeFirst60(latestOutcome?.metrics?.first_60_sec_retention_pct ? String(latestOutcome.metrics.first_60_sec_retention_pct) : '');
        setOutcomeSummary(String(latestOutcome?.operator_summary || ''));
        setOutcomeStrongSignals(joinOutcomeLines(latestOutcome?.strongest_signals));
        setOutcomeWeakPoints(joinOutcomeLines(latestOutcome?.weak_points));
        setOutcomeHookWins(joinOutcomeLines(latestOutcome?.hook_wins));
        setOutcomeHookWatchouts(joinOutcomeLines(latestOutcome?.hook_watchouts));
        setOutcomePacingWins(joinOutcomeLines(latestOutcome?.pacing_wins));
        setOutcomePacingWatchouts(joinOutcomeLines(latestOutcome?.pacing_watchouts));
        setOutcomeVisualWins(joinOutcomeLines(latestOutcome?.visual_wins));
        setOutcomeVisualWatchouts(joinOutcomeLines(latestOutcome?.visual_watchouts));
        setOutcomeSoundWins(joinOutcomeLines(latestOutcome?.sound_wins));
        setOutcomeSoundWatchouts(joinOutcomeLines(latestOutcome?.sound_watchouts));
        setOutcomePackagingWins(joinOutcomeLines(latestOutcome?.packaging_wins));
        setOutcomePackagingWatchouts(joinOutcomeLines(latestOutcome?.packaging_watchouts));
        setOutcomeRetentionWins(joinOutcomeLines(latestOutcome?.retention_wins));
        setOutcomeRetentionWatchouts(joinOutcomeLines(latestOutcome?.retention_watchouts));
        setOutcomeNextMoves(joinOutcomeLines(latestOutcome?.next_video_moves));
    }, [latestOutcome, lfSession?.session_id, outputFile]);

    const activeLongformRenderStatus = lfSession?.status === 'rendering' && lfSession?.job_id
        ? (
            jobStatus || {
                status: 'queued',
                progress: 0,
                current_scene: 0,
                total_scenes: flattenedRenderScenes.length,
                resolution: lfSession?.resolution,
            }
        )
        : null;
    const longformRenderProgressWindow = activeLongformRenderStatus && !renderMonitorDismissed && activeLongformRenderStatus.status !== 'complete' && activeLongformRenderStatus.status !== 'error' ? (
        <RenderProgressWindow
            jobStatus={activeLongformRenderStatus}
            title={`${String(lfSession?.input_title || lfSession?.topic || 'Long-Form Documentary')} · Long-Form Render`}
            previewUrl={longformRenderPreview?.url || null}
            previewType={longformRenderPreview?.kind || 'image'}
            previewLabel={longformRenderPreview?.label || 'Current long-form render preview'}
            onDismiss={() => setRenderMonitorDismissed(true)}
        />
    ) : null;

    const submitOutcome = useCallback(async () => {
        if (!lfSession?.session_id) return;
        setOutcomeSaving(true);
        setError('');
        try {
            const toNumber = (value: string) => {
                const parsed = Number(String(value || '').trim());
                return Number.isFinite(parsed) ? parsed : 0;
            };
            const payload = await apiCall(`/api/longform/session/${lfSession.session_id}/outcome`, {
                method: 'POST',
                body: JSON.stringify({
                    video_url: outcomeVideoUrl.trim(),
                    title_used: selectedTitle,
                    description_used: selectedDescription,
                    thumbnail_prompt: String(lfSession?.package?.thumbnail_prompt || ''),
                    thumbnail_url: String(lfSession?.package?.thumbnail_url || ''),
                    tags: selectedTags,
                    views: Math.round(toNumber(outcomeViews)),
                    impressions: Math.round(toNumber(outcomeImpressions)),
                    average_view_duration_sec: toNumber(outcomeAvd),
                    average_percentage_viewed: toNumber(outcomeAvp),
                    impression_click_through_rate: toNumber(outcomeCtr),
                    first_30_sec_retention_pct: toNumber(outcomeFirst30),
                    first_60_sec_retention_pct: toNumber(outcomeFirst60),
                    operator_summary: outcomeSummary.trim(),
                    strongest_signals: splitOutcomeLines(outcomeStrongSignals),
                    weak_points: splitOutcomeLines(outcomeWeakPoints),
                    hook_wins: splitOutcomeLines(outcomeHookWins),
                    hook_watchouts: splitOutcomeLines(outcomeHookWatchouts),
                    pacing_wins: splitOutcomeLines(outcomePacingWins),
                    pacing_watchouts: splitOutcomeLines(outcomePacingWatchouts),
                    visual_wins: splitOutcomeLines(outcomeVisualWins),
                    visual_watchouts: splitOutcomeLines(outcomeVisualWatchouts),
                    sound_wins: splitOutcomeLines(outcomeSoundWins),
                    sound_watchouts: splitOutcomeLines(outcomeSoundWatchouts),
                    packaging_wins: splitOutcomeLines(outcomePackagingWins),
                    packaging_watchouts: splitOutcomeLines(outcomePackagingWatchouts),
                    retention_wins: splitOutcomeLines(outcomeRetentionWins),
                    retention_watchouts: splitOutcomeLines(outcomeRetentionWatchouts),
                    next_video_moves: splitOutcomeLines(outcomeNextMoves),
                    auto_fetch_channel_metrics: true,
                }),
            });
            setLfSession((payload as any).session || null);
        } catch (e: any) {
            setError(e?.message || 'Failed to ingest post-publish outcome');
        } finally {
            setOutcomeSaving(false);
        }
    }, [
        apiCall,
        lfSession?.package,
        lfSession?.session_id,
        outcomeAvd,
        outcomeAvp,
        outcomeCtr,
        outcomeFirst30,
        outcomeFirst60,
        outcomeHookWatchouts,
        outcomeHookWins,
        outcomeImpressions,
        outcomeNextMoves,
        outcomePackagingWatchouts,
        outcomePackagingWins,
        outcomePacingWatchouts,
        outcomePacingWins,
        outcomeRetentionWatchouts,
        outcomeRetentionWins,
        outcomeSoundWatchouts,
        outcomeSoundWins,
        outcomeStrongSignals,
        outcomeSummary,
        outcomeVideoUrl,
        outcomeViews,
        outcomeVisualWatchouts,
        outcomeVisualWins,
        outcomeWeakPoints,
        selectedDescription,
        selectedTags,
        selectedTitle,
    ]);

    const autoPullOutcome = useCallback(async () => {
        if (!lfSession?.session_id) return;
        setOutcomeAutoSaving(true);
        setError('');
        try {
            const payload = await apiCall(`/api/longform/session/${lfSession.session_id}/outcome/auto`, {
                method: 'POST',
                body: JSON.stringify({
                    video_url: outcomeVideoUrl.trim(),
                    candidate_limit: 12,
                    auto_fetch_channel_metrics: true,
                }),
            });
            setLfSession((payload as any).session || null);
        } catch (e: any) {
            setError(e?.message || 'Failed to auto-pull post-publish outcome');
        } finally {
            setOutcomeAutoSaving(false);
        }
    }, [apiCall, lfSession?.session_id, outcomeVideoUrl]);

    return (
        <div className="max-w-5xl mx-auto px-6 pb-10 space-y-6">
            <div className="rounded-xl border border-violet-400/25 bg-violet-500/10 p-4">
                <div className="flex items-center gap-2 text-violet-200">
                    <Sparkles className="w-4 h-4" />
                    <p className="text-sm font-semibold">Catalyst Long Form (2 to 10 minutes)</p>
                </div>
                <p className="text-xs text-violet-100/80 mt-1">
                    End-to-end faceless pipeline: topic to chapters, chapter review/approve, finalize render, then package export. Owner autopilot can now use source URL, transcript, and analytics screenshots together.
                </p>
                {lfSession?.status === 'bootstrapping' ? (
                    <p className="text-xs text-violet-100/70 mt-2">
                        Source analysis is running in the background. Studio will keep polling this session and begin chapter generation automatically when the brief is ready.
                    </p>
                ) : null}
            </div>

            <div className="grid grid-cols-2 gap-2 p-1 rounded-xl border border-white/[0.06] bg-white/[0.02]">
                <button
                    onClick={() => setActiveTab('create')}
                    className={`py-2.5 rounded-lg text-sm font-medium transition ${
                        activeTab === 'create'
                            ? 'bg-violet-600 text-white'
                            : 'text-gray-400 hover:text-white hover:bg-white/[0.04]'
                    }`}
                >
                    Create
                </button>
                <button
                    onClick={() => setActiveTab('projects')}
                    className={`py-2.5 rounded-lg text-sm font-medium transition ${
                        activeTab === 'projects'
                            ? 'bg-violet-600 text-white'
                            : 'text-gray-400 hover:text-white hover:bg-white/[0.04]'
                    }`}
                >
                    Projects
                </button>
            </div>

            {activeTab === 'create' && (
                <>
            <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-5 space-y-4">
                <h3 className="font-semibold text-white flex items-center gap-2">
                    <Wand2 className="w-4 h-4 text-violet-300" />
                    New Long-Form Session
                </h3>
                <div className="grid md:grid-cols-2 gap-3">
                    <div className="text-sm text-gray-300 md:col-span-2">
                        Connected YouTube Channel
                        <div className="mt-1 flex flex-col gap-3 md:flex-row md:items-start">
                            <select
                                value={youtubeChannelId}
                                onChange={(e) => void persistSelectedYouTubeChannel(e.target.value)}
                                disabled={!canUseDeepAnalysis}
                                className="w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white md:max-w-xl"
                            >
                                <option value="">No connected channel selected</option>
                                {youtubeChannels.map((channel) => (
                                    <option key={channel.channel_id} value={channel.channel_id}>
                                        {channel.title}{channel.channel_handle ? ` (${channel.channel_handle})` : ''}
                                    </option>
                                ))}
                            </select>
                            <div className="flex gap-2">
                                <button
                                    type="button"
                                    onClick={startYouTubeConnect}
                                    disabled={!canUseDeepAnalysis || youtubeConnecting}
                                    className="rounded-lg border border-cyan-400/30 bg-cyan-500/10 px-3 py-2 text-sm font-medium text-cyan-100 transition hover:bg-cyan-500/20 disabled:opacity-60"
                                >
                                    {youtubeConnecting ? 'Opening Google...' : 'Connect YouTube'}
                                </button>
                                <button
                                    type="button"
                                    onClick={() => void loadYouTubeChannels(false)}
                                    disabled={!canUseDeepAnalysis || youtubeLoading}
                                    className="rounded-lg border border-white/[0.1] bg-white/[0.04] px-3 py-2 text-sm font-medium text-white transition hover:bg-white/[0.08] disabled:opacity-60"
                                >
                                    {youtubeLoading ? 'Refreshing...' : 'Refresh Channels'}
                                </button>
                                <button
                                    type="button"
                                    onClick={() => void syncConnectedChannelOutcomes(false)}
                                    disabled={!canUseDeepAnalysis || !youtubeChannelId || youtubeOutcomeSyncing}
                                    className="rounded-lg border border-amber-400/30 bg-amber-500/10 px-3 py-2 text-sm font-medium text-amber-100 transition hover:bg-amber-500/20 disabled:opacity-60"
                                >
                                    {youtubeOutcomeSyncing ? 'Syncing Outcomes...' : 'Sync Outcomes'}
                                </button>
                            </div>
                        </div>
                        <p className="mt-2 text-xs text-cyan-300/80">
                            When selected, Catalyst will use this channel’s recent winners, packaging patterns, and private analytics context while building the next long-form video.
                        </p>
                        {youtubeError ? <p className="mt-2 text-xs text-red-400">{youtubeError}</p> : null}
                        {youtubeChannels.length > 0 && youtubeChannelId ? (
                            <div className="mt-3 rounded-lg border border-cyan-400/20 bg-cyan-500/5 p-3 text-xs text-gray-300">
                                {(() => {
                                    const current = youtubeChannels.find((row) => row.channel_id === youtubeChannelId);
                                    if (!current) return <span>No saved channel context yet.</span>;
                                    return (
                                        <>
                                            <p className="font-semibold text-white">{current.title}</p>
                                            {current.analytics_snapshot?.channel_summary ? (
                                                <p className="mt-2">{current.analytics_snapshot.channel_summary}</p>
                                            ) : null}
                                            {current.analytics_snapshot?.historical_compare?.winner_vs_loser_summary ? (
                                                <div className="mt-2 rounded-lg border border-amber-400/20 bg-amber-500/5 p-2 text-[11px] text-amber-100">
                                                    <p className="font-semibold uppercase tracking-[0.16em] text-amber-200">Historical Compare</p>
                                                    <p className="mt-1 text-gray-200">{current.analytics_snapshot.historical_compare.winner_vs_loser_summary}</p>
                                                    {current.analytics_snapshot.historical_compare.best_recent_video?.title ? (
                                                        <p className="mt-1 text-gray-300">
                                                            Best recent: {current.analytics_snapshot.historical_compare.best_recent_video.title}
                                                            {typeof current.analytics_snapshot.historical_compare.best_recent_video.views === 'number'
                                                                ? ` (${current.analytics_snapshot.historical_compare.best_recent_video.views} views)`
                                                                : ''}
                                                        </p>
                                                    ) : null}
                                                    {current.analytics_snapshot.historical_compare.worst_recent_video?.title ? (
                                                        <p className="mt-1 text-gray-300">
                                                            Weakest recent: {current.analytics_snapshot.historical_compare.worst_recent_video.title}
                                                            {typeof current.analytics_snapshot.historical_compare.worst_recent_video.views === 'number'
                                                                ? ` (${current.analytics_snapshot.historical_compare.worst_recent_video.views} views)`
                                                                : ''}
                                                        </p>
                                                    ) : null}
                                                </div>
                                            ) : null}
                                            {current.last_sync_error ? (
                                                <p className="mt-2 text-amber-300">Last sync note: {current.last_sync_error}</p>
                                            ) : null}
                                            <div className="mt-2 grid gap-2 md:grid-cols-3 text-[11px] text-cyan-100/80">
                                                <div>Outcome syncs: {Number(current.last_outcome_sync_count || 0)}</div>
                                                <div>Last outcome sync: {current.last_outcome_sync_at ? new Date(Number(current.last_outcome_sync_at) * 1000).toLocaleString() : 'never'}</div>
                                                <div>Outcome sync note: {String(current.last_outcome_sync_error || 'clean')}</div>
                                            </div>
                                        </>
                                    );
                                })()}
                            </div>
                        ) : null}
                    </div>
                    <label className="text-sm text-gray-300">
                        Template Style
                        <select value={template} onChange={(e) => setTemplate(e.target.value as LongFormTemplate)}
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white">
                            <option value="story">Story</option>
                            <option value="skeleton">Skeleton</option>
                        </select>
                    </label>
                    <label className="text-sm text-gray-300">
                        Content Format
                        <select value={formatPreset} onChange={(e) => setFormatPreset(e.target.value as LongFormPreset)}
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white">
                            <option value="recap">Recap</option>
                            <option value="explainer">Explainer</option>
                            <option value="documentary">Documentary</option>
                            <option value="story_channel">Story Channel</option>
                        </select>
                    </label>
                    <div className="text-sm text-gray-300">
                        Visual Engine
                        <div className="mt-1 rounded-lg border border-cyan-500/20 bg-cyan-500/5 px-3 py-2 text-sm text-cyan-100">
                            {template === 'skeleton' ? 'Catalyst Skeleton 3D' : 'Catalyst Documentary 3D'}
                        </div>
                        <p className="mt-2 text-xs text-cyan-300/80">
                            {template === 'skeleton'
                                ? 'Long Form now uses the Skeleton identity engine directly. Content Format controls the documentary or story grammar around it.'
                                : 'Long Form now uses the documentary 3D engine directly. Content Format is the real creative driver.'}
                        </p>
                    </div>
                    <label className="text-sm text-gray-300">
                        Target Minutes
                        <input
                            type="number"
                            min={2}
                            max={30}
                            step={0.5}
                            value={targetMinutes}
                            onChange={(e) => setTargetMinutes(Math.max(2, Math.min(30, Number(e.target.value || 8))))}
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white"
                        />
                    </label>
                    <label className="text-sm text-gray-300 md:col-span-2">
                        Topic
                        <input
                            value={topic}
                            onChange={(e) => setTopic(e.target.value)}
                            placeholder="Optional if Source Video URL is filled. Example: Why some manga recaps hold attention for 20 minutes while others die in the first 90 seconds"
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white"
                        />
                    </label>
                    <label className="text-sm text-gray-300 md:col-span-2">
                        Video Title
                        <input
                            value={inputTitle}
                            onChange={(e) => setInputTitle(e.target.value)}
                            placeholder="Optional if Source Video URL is filled. Example: How top recap channels keep viewers watching"
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white"
                        />
                    </label>
                    <label className="text-sm text-gray-300 md:col-span-2">
                        Video Description
                        <textarea
                            value={inputDescription}
                            onChange={(e) => setInputDescription(e.target.value)}
                            rows={3}
                            placeholder="Optional if Source Video URL is filled. Otherwise describe the structure, tone, pacing, references, research angle, and retention goals for this long-form video."
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white resize-none"
                        />
                    </label>
                    <label className="text-sm text-gray-300 md:col-span-2">
                        Source Video URL
                        <input
                            value={sourceUrl}
                            onChange={(e) => setSourceUrl(e.target.value)}
                            placeholder="Optional: paste a YouTube video URL so Catalyst can study the source title, description, chapters, transcript, and public packaging."
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white"
                        />
                        <p className="mt-2 text-xs text-cyan-300/80">
                            {canUseDeepAnalysis
                                ? 'If you only paste a source URL, Catalyst now auto-derives the follow-up topic, title, and description from the source video and rebuilds a stronger version from that angle.'
                                : 'Source-video analysis stays owner beta for now. Public Long Form needs a manual topic, title, and description while the heavier Catalyst path is being tuned.'}
                        </p>
                    </label>
                    <label className="text-sm text-gray-300 md:col-span-2">
                        Private Performance Notes
                        <textarea
                            value={analyticsNotes}
                            onChange={(e) => setAnalyticsNotes(e.target.value)}
                            rows={3}
                            placeholder="Optional: CTR, average view duration, where viewers dropped, why you think the old video worked, and what absolutely must improve."
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white resize-none"
                        />
                    </label>
                    <label className="text-sm text-gray-300 md:col-span-2">
                        Manual Transcript
                        <textarea
                            value={transcriptText}
                            onChange={(e) => setTranscriptText(e.target.value)}
                            rows={5}
                            placeholder="Optional: paste the transcript or the most important spoken beats so Catalyst can improve the follow-up script even before YouTube API access is restored."
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white resize-none"
                        />
                    </label>
                    <label className="text-sm text-gray-300 md:col-span-2">
                        Analytics Screenshots
                        <input
                            type="file"
                            accept="image/*"
                            multiple
                            onChange={(e) => {
                                const incoming = Array.from(e.target.files || []);
                                if (incoming.length > 0) {
                                    setAnalyticsImages((prev) => [...prev, ...incoming]);
                                }
                                e.currentTarget.value = '';
                            }}
                            className="mt-1 block w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white file:mr-3 file:rounded-md file:border-0 file:bg-violet-600 file:px-3 file:py-2 file:text-sm file:font-medium file:text-white"
                        />
                        <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-gray-400">
                            <span>{analyticsImages.length} screenshot{analyticsImages.length === 1 ? '' : 's'} selected</span>
                            {analyticsImages.length > 0 ? (
                                <button
                                    type="button"
                                    onClick={() => setAnalyticsImages([])}
                                    className="rounded-md border border-white/[0.12] px-2 py-1 text-[11px] text-white hover:bg-white/[0.06]"
                                >
                                    Clear
                                </button>
                            ) : null}
                        </div>
                        {analyticsImages.length > 0 ? (
                            <p className="mt-2 text-xs text-cyan-300/80">
                                Catalyst will inspect up to the first 24 screenshots and turn them into retention and packaging guidance for the next version.
                            </p>
                        ) : null}
                        {analyticsImages.length > 0 ? (
                            <div className="mt-2 flex flex-wrap gap-2">
                                {analyticsImages.slice(0, 10).map((file, idx) => (
                                    <span key={`${file.name}-${idx}`} className="rounded-md border border-white/[0.08] bg-black/20 px-2 py-1 text-[11px] text-gray-300">
                                        {file.name}
                                    </span>
                                ))}
                                {analyticsImages.length > 10 ? (
                                    <span className="rounded-md border border-white/[0.08] bg-black/20 px-2 py-1 text-[11px] text-gray-400">
                                        +{analyticsImages.length - 10} more
                                    </span>
                                ) : null}
                            </div>
                        ) : null}
                    </label>
                    <label className="text-sm text-gray-300 md:col-span-2">
                        Subject Reference Image
                        <input
                            type="file"
                            accept="image/*"
                            onChange={(e) => {
                                const next = e.target.files?.[0] || null;
                                setSubjectReferenceImage(next);
                                e.currentTarget.value = '';
                            }}
                            className="mt-1 block w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white file:mr-3 file:rounded-md file:border-0 file:bg-cyan-600 file:px-3 file:py-2 file:text-sm file:font-medium file:text-white"
                        />
                        <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-gray-400">
                            <span>
                                {subjectReferenceImage
                                    ? `Selected: ${subjectReferenceImage.name}`
                                    : subjectReferenceAttached
                                        ? 'Reference image already attached to this session'
                                        : 'Optional but recommended for celebrities, rappers, athletes, or any recurring real person'}
                            </span>
                            {(subjectReferenceImage || subjectReferenceAttached) ? (
                                <button
                                    type="button"
                                    onClick={() => {
                                        setSubjectReferenceImage(null);
                                        if (!lfSession?.session_id) {
                                            setSubjectReferenceAttached(false);
                                        }
                                    }}
                                    className="rounded-md border border-white/[0.12] px-2 py-1 text-[11px] text-white hover:bg-white/[0.06]"
                                >
                                    Clear
                                </button>
                            ) : null}
                        </div>
                        <p className="mt-2 text-xs text-cyan-300/80">
                            Upload one clear face/body reference for the main recurring person so long-form can keep identity consistent across scenes instead of drifting into lookalikes.
                        </p>
                    </label>
                    <label className="text-sm text-gray-300">
                        Language
                        <select value={language} onChange={(e) => setLanguage(e.target.value)}
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white">
                            {languages.map((l) => (
                                <option key={l.code} value={l.code}>{l.name}</option>
                            ))}
                        </select>
                    </label>
                    <label className="text-sm text-gray-300">
                        Whisper Mode
                        <select value={whisperMode} onChange={(e) => setWhisperMode(e.target.value as 'off' | 'subtle' | 'cinematic')}
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white">
                            <option value="off">Off</option>
                            <option value="subtle">Subtle</option>
                            <option value="cinematic">Cinematic</option>
                        </select>
                    </label>
                </div>
                <div className="flex flex-wrap items-center gap-4 text-sm">
                    <label className="inline-flex items-center gap-2 text-gray-300">
                        <input type="checkbox" checked={animationEnabled} onChange={(e) => setAnimationEnabled(e.target.checked)} />
                        Scene animation enabled
                    </label>
                    <label className="inline-flex items-center gap-2 text-gray-300">
                        <input type="checkbox" checked={sfxEnabled} onChange={(e) => setSfxEnabled(e.target.checked)} />
                        SFX enabled
                    </label>
                    <label className="inline-flex items-center gap-2 text-gray-300">
                        <input type="checkbox" checked={applyMarketingDoctrine} onChange={(e) => setApplyMarketingDoctrine(e.target.checked)} />
                        Apply marketing doctrine
                    </label>
                    {ownerOverride ? (
                        <label className="inline-flex items-center gap-2 text-cyan-200">
                            <input type="checkbox" checked={autoPipeline} onChange={(e) => setAutoPipeline(e.target.checked)} />
                            Owner autopilot: auto-approve chapters and auto-render
                        </label>
                    ) : null}
                </div>
                <div className="rounded-xl border border-cyan-500/20 bg-cyan-500/5 p-4">
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300">Marketing Doctrine</p>
                    <p className="mt-2 text-xs text-gray-400">
                        The lesson points you sent are now available as a reusable strategy layer for Long Form. When enabled, Catalyst uses them to shape packaging and retention decisions.
                    </p>
                    <div className="mt-3 grid gap-2 md:grid-cols-2">
                        {MARKETING_DOCTRINE_POINTS.map((point) => (
                            <div key={point} className="rounded-lg border border-white/[0.06] bg-black/20 px-3 py-2 text-xs text-gray-300">
                                {point}
                            </div>
                        ))}
                    </div>
                </div>
                <div className="flex flex-wrap gap-3">
                    <button
                        onClick={createSession}
                        disabled={createDisabled}
                        className="px-4 py-2 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-sm font-medium disabled:opacity-60 transition"
                    >
                        {creating
                            ? 'Creating...'
                            : ownerOverride && autoPipeline
                                ? 'Create + Auto Run'
                                : canCreateFromSourceOnly && missingManualBrief
                                    ? 'Create From Source Video'
                                    : canCreateFromDeepAnalysisOnly && missingManualBrief
                                        ? 'Create From Analysis Inputs'
                                        : 'Create Session'}
                    </button>
                    <div className="flex items-center gap-2">
                        <input
                            value={sessionIdInput}
                            onChange={(e) => setSessionIdInput(e.target.value)}
                            placeholder="Existing session id"
                            className="w-72 rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white"
                        />
                        <button
                            onClick={() => refreshStatus(sessionIdInput, false)}
                            disabled={refreshing || !sessionIdInput.trim()}
                            className="px-3 py-2 rounded-lg bg-white/10 hover:bg-white/15 text-sm text-white disabled:opacity-60 transition"
                        >
                            {refreshing ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                        </button>
                    </div>
                </div>
                {error && <p className="text-sm text-red-400">{error}</p>}
            </div>

            {lfSession && (
                <div className="space-y-4">
                    <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-5 space-y-3">
                        <div className="flex flex-wrap items-center justify-between gap-3">
                            <div>
                                <h3 className="font-semibold text-white">Session {lfSession.session_id}</h3>
                                <p className="text-xs text-gray-500">
                                    Status: {STATUS_LABELS[lfSession.status] || lfSession.status}
                                </p>
                                {lfSession.auto_pipeline ? (
                                    <p className="mt-1 text-xs text-cyan-300">Owner autopilot is active on this session.</p>
                                ) : null}
                            </div>
                            <div className="text-xs text-gray-400">
                                {review ? `${review.approved_chapters}/${review.total_chapters} chapters approved` : 'No review stats'}
                            </div>
                        </div>

                        <div className="rounded-lg border border-cyan-400/20 bg-cyan-500/5 p-3">
                            <div className="flex flex-wrap items-center justify-between gap-3">
                                <div>
                                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300">Subject Reference</p>
                                    <p className="mt-2 text-sm text-gray-300">
                                        {lfSession.has_reference_image || lfSession.reference_image_uploaded
                                            ? 'A subject reference is attached for this session. Long-form will use it first for identity continuity when that person appears.'
                                            : 'No subject reference is attached yet. Upload one if this video needs a recurring celebrity or real person to stay visually consistent.'}
                                    </p>
                                </div>
                                <div className="flex flex-wrap items-center gap-2">
                                    <span className={`rounded-full border px-2 py-1 text-[11px] ${
                                        lfSession.has_reference_image || lfSession.reference_image_uploaded
                                            ? 'border-emerald-400/30 bg-emerald-500/10 text-emerald-200'
                                            : 'border-white/[0.12] bg-white/[0.03] text-gray-300'
                                    }`}>
                                        {lfSession.has_reference_image || lfSession.reference_image_uploaded ? 'Attached' : 'Missing'}
                                    </span>
                                    {subjectReferenceImage ? (
                                        <button
                                            type="button"
                                            onClick={attachSubjectReferenceToSession}
                                            disabled={uploadingReference}
                                            className="rounded-lg bg-cyan-600 px-3 py-2 text-sm font-medium text-white hover:bg-cyan-500 disabled:opacity-60"
                                        >
                                            {uploadingReference ? 'Attaching...' : 'Attach Selected Reference'}
                                        </button>
                                    ) : null}
                                </div>
                            </div>
                            {lfSession.reference_image_public_url ? (
                                <div className="mt-3">
                                    <img
                                        src={lfSession.reference_image_public_url}
                                        alt="Long-form subject reference"
                                        className="h-24 w-24 rounded-lg border border-white/[0.08] object-cover"
                                    />
                                </div>
                            ) : null}
                        </div>

                        <div className="rounded-lg border border-fuchsia-400/20 bg-fuchsia-500/5 p-3 space-y-3">
                            <div className="flex flex-wrap items-start justify-between gap-3">
                                <div>
                                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fuchsia-300">Character References</p>
                                    <p className="mt-2 text-sm text-gray-300">
                                        Upload named reference images for recurring people. Scene assignments use these first for identity continuity during the next regenerate and final render.
                                    </p>
                                </div>
                                <span className="rounded-full border border-fuchsia-400/25 bg-fuchsia-500/10 px-2 py-1 text-[11px] text-fuchsia-100">
                                    {characterReferences.length} character{characterReferences.length === 1 ? '' : 's'}
                                </span>
                            </div>
                            <div className="grid gap-3 md:grid-cols-[220px,1fr_auto]">
                                <input
                                    value={characterReferenceName}
                                    onChange={(e) => setCharacterReferenceName(e.target.value)}
                                    placeholder="Character name"
                                    className="rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white"
                                />
                                <label className="rounded-lg border border-dashed border-white/[0.14] bg-black/20 px-3 py-2 text-sm text-gray-300 cursor-pointer hover:border-fuchsia-400/40 transition">
                                    <input
                                        type="file"
                                        accept="image/*"
                                        className="hidden"
                                        onChange={(e) => setCharacterReferenceImage(e.target.files?.[0] || null)}
                                    />
                                    {characterReferenceImage ? characterReferenceImage.name : 'Choose character reference image'}
                                </label>
                                <button
                                    type="button"
                                    onClick={uploadCharacterReferenceToSession}
                                    disabled={uploadingCharacterReference || !characterReferenceImage || !String(characterReferenceName || '').trim()}
                                    className="rounded-lg bg-fuchsia-600 px-3 py-2 text-sm font-medium text-white hover:bg-fuchsia-500 disabled:opacity-60"
                                >
                                    {uploadingCharacterReference ? 'Adding...' : 'Add Character'}
                                </button>
                            </div>
                            {characterReferences.length > 0 ? (
                                <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                                    {characterReferences.map((character) => (
                                        <div key={character.character_id} className="rounded-lg border border-white/[0.08] bg-black/20 p-3 space-y-2">
                                            <div className="flex items-center gap-3">
                                                {character.reference_image_public_url ? (
                                                    <img
                                                        src={character.reference_image_public_url}
                                                        alt={character.name}
                                                        className="h-14 w-14 rounded-lg border border-white/[0.08] object-cover"
                                                    />
                                                ) : (
                                                    <div className="h-14 w-14 rounded-lg border border-white/[0.08] bg-white/[0.03]" />
                                                )}
                                                <div>
                                                    <p className="text-sm font-semibold text-white">{character.name}</p>
                                                    <p className="text-[11px] uppercase tracking-[0.16em] text-fuchsia-200/80">
                                                        {String(character.reference_lock_mode || 'strict')}
                                                    </p>
                                                </div>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            ) : null}
                        </div>

                        {jobStatus?.status && (
                            <ProgressBar progress={Number(jobStatus.progress || 0)} status={String(jobStatus.status || '')} />
                        )}
                        {lfSession.status === 'draft_generating' && (
                            <p className="text-xs text-violet-200">
                                Draft generation: {Number(draftProgress?.generated_chapters || 0)}/{Number(draftProgress?.total_chapters || 0)}
                                {Number(draftProgress?.preview_scene_total || 0) > 0 ? `, scene previews: ${Number(draftProgress?.preview_scene_generated || 0)}/${Number(draftProgress?.preview_scene_total || 0)}` : ''}
                                {Number(draftProgress?.failed_chapters || 0) > 0 ? `, fallback used: ${Number(draftProgress?.failed_chapters || 0)}` : ''}
                            </p>
                        )}

                        {lfSession?.paused_error && (
                            <div className="rounded-lg border border-amber-400/30 bg-amber-500/10 p-3 space-y-2">
                                <p className="text-sm text-amber-200 flex items-center gap-2">
                                    <AlertTriangle className="w-4 h-4" />
                                    Paused: {String(lfSession.paused_error.error || 'manual review needed')}
                                </p>
                                <p className="text-xs text-amber-100/80">
                                    Chapter {Number(lfSession.paused_error.chapter_index || 0) + 1}, scene {Number(lfSession.paused_error.scene_index || 0) + 1}
                                </p>
                                <textarea
                                    value={fixNote}
                                    onChange={(e) => setFixNote(e.target.value)}
                                    rows={2}
                                    placeholder="Fix note (what should change in regenerated chapter)"
                                    className="w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white resize-none"
                                />
                                {String(lfSession.paused_error?.suggested_fix_note || '').trim() ? (
                                    <p className="text-[11px] text-white/55">
                                        Catalyst prefilled a suggested fix note from the blocked preflight issues. Edit it if you want a more specific regeneration.
                                    </p>
                                ) : null}
                                <div className="flex gap-2">
                                    <button
                                        onClick={() => resolvePausedError(false)}
                                        disabled={actionBusy.startsWith('resolve:')}
                                        className="px-3 py-2 rounded-lg bg-amber-600 hover:bg-amber-500 text-white text-sm font-medium disabled:opacity-60"
                                    >
                                        {actionBusy.startsWith('resolve:') ? 'Applying...' : 'Regenerate Fix'}
                                    </button>
                                    <button
                                        onClick={() => resolvePausedError(true)}
                                        disabled={actionBusy.startsWith('resolve:')}
                                        className="px-3 py-2 rounded-lg bg-white/10 hover:bg-white/15 text-white text-sm font-medium disabled:opacity-60"
                                    >
                                        Force Accept
                                    </button>
                                </div>
                            </div>
                        )}

                                {(lfSession.source_url || sourceVideo.title || sourceAnalysis.what_worked || sourceAnalysis.what_hurt || connectedYouTubeChannel.channel_title) && (
                            <div className="rounded-lg border border-cyan-400/20 bg-cyan-500/5 p-4 space-y-3">
                                <div>
                                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300">Source Analysis</p>
                                    {lfSession.source_url && (
                                        <p className="mt-2 text-xs text-gray-400 break-all">{lfSession.source_url}</p>
                                    )}
                                </div>
                                {sourceVideo.title && (
                                    <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                        <p className="text-sm font-semibold text-white">{String(sourceVideo.title)}</p>
                                        <p className="mt-1 text-xs text-gray-500">
                                            {sourceVideo.channel ? `${String(sourceVideo.channel)} · ` : ''}
                                            {sourceVideo.duration_sec ? `${Number(sourceVideo.duration_sec)}s analyzed` : 'public source analyzed'}
                                        </p>
                                        {sourceVideo.public_summary && (
                                            <p className="mt-2 text-xs text-gray-400">{String(sourceVideo.public_summary)}</p>
                                        )}
                                    </div>
                                )}
                                {connectedYouTubeChannel.channel_title ? (
                                    <div className="rounded-lg border border-cyan-400/20 bg-black/20 p-3">
                                        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300">Connected Channel Context</p>
                                        <p className="mt-2 text-sm font-semibold text-white">{String(connectedYouTubeChannel.channel_title)}</p>
                                        {connectedYouTubeChannel.summary ? (
                                            <p className="mt-2 text-xs text-gray-400">{String(connectedYouTubeChannel.summary)}</p>
                                        ) : null}
                                        {Array.isArray(connectedYouTubeChannel.title_pattern_hints) && connectedYouTubeChannel.title_pattern_hints.length > 0 ? (
                                            <ul className="mt-2 space-y-1 text-xs text-gray-300">
                                                {connectedYouTubeChannel.title_pattern_hints.slice(0, 4).map((item: string, idx: number) => (
                                                    <li key={`channel-title-hint-${idx}`}>- {item}</li>
                                                ))}
                                            </ul>
                                        ) : null}
                                    </div>
                                ) : null}
                                {Object.keys(selectedSeriesCluster).length ? (
                                    <div className="rounded-lg border border-fuchsia-400/20 bg-fuchsia-500/5 p-3 space-y-3">
                                        <div className="flex flex-wrap items-start justify-between gap-3">
                                            <div>
                                                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fuchsia-300">Matched Catalyst Arc</p>
                                                <p className="mt-2 text-sm font-semibold text-white">
                                                    {String(selectedSeriesCluster.label || selectedSeriesCluster.series_anchor || 'Channel cluster')}
                                                </p>
                                                {selectedSeriesCluster.follow_up_rule ? (
                                                    <p className="mt-2 text-xs text-gray-300">{String(selectedSeriesCluster.follow_up_rule)}</p>
                                                ) : null}
                                            </div>
                                            <div className="rounded-lg border border-fuchsia-400/20 bg-black/20 px-3 py-2 text-xs text-fuchsia-100/90 space-y-1">
                                                <div>Anchor: {String(selectedSeriesCluster.series_anchor || 'n/a')}</div>
                                                <div>Niche: {String(selectedSeriesCluster.niche_label || 'n/a')}</div>
                                            </div>
                                        </div>
                                        <div className="grid gap-3 md:grid-cols-4 text-xs text-gray-300">
                                            <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                                <div className="text-gray-500">Avg views</div>
                                                <div className="mt-1 text-lg font-semibold text-white">
                                                    {Number(selectedSeriesCluster.avg_views || 0).toFixed(0)}
                                                </div>
                                            </div>
                                            <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                                <div className="text-gray-500">Avg CTR</div>
                                                <div className="mt-1 text-lg font-semibold text-white">
                                                    {Number(selectedSeriesCluster.avg_ctr || 0).toFixed(2)}%
                                                </div>
                                            </div>
                                            <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                                <div className="text-gray-500">Avg viewed</div>
                                                <div className="mt-1 text-lg font-semibold text-white">
                                                    {Number(selectedSeriesCluster.avg_avp || 0).toFixed(1)}%
                                                </div>
                                            </div>
                                            <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                                <div className="text-gray-500">Videos</div>
                                                <div className="mt-1 text-lg font-semibold text-white">
                                                    {Number(selectedSeriesCluster.video_count || 0).toFixed(0)}
                                                </div>
                                            </div>
                                        </div>
                                        {selectedSeriesKeywords.length ? (
                                            <div>
                                                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-gray-400">Cluster Keywords</p>
                                                <div className="mt-2 flex flex-wrap gap-2 text-xs text-fuchsia-100">
                                                    {selectedSeriesKeywords.slice(0, 10).map((item: string, idx: number) => (
                                                        <span key={`series-keyword-${idx}`} className="rounded-full border border-fuchsia-400/25 bg-fuchsia-500/10 px-2 py-1">
                                                            {item}
                                                        </span>
                                                    ))}
                                                </div>
                                            </div>
                                        ) : null}
                                        {selectedSeriesTitles.length ? (
                                            <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-gray-400">Reference Titles In This Arc</p>
                                                <ul className="mt-2 space-y-1 text-sm text-gray-300">
                                                    {selectedSeriesTitles.slice(0, 4).map((item: string, idx: number) => (
                                                        <li key={`series-title-${idx}`}>- {item}</li>
                                                    ))}
                                                </ul>
                                            </div>
                                        ) : null}
                                    </div>
                                ) : null}
                                <div className="grid gap-3 md:grid-cols-2">
                                    {sourceAnalysis.what_worked && (
                                        <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/5 p-3">
                                            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">What Worked</p>
                                            <p className="mt-2 text-sm text-gray-300">{String(sourceAnalysis.what_worked)}</p>
                                        </div>
                                    )}
                                    {sourceAnalysis.what_hurt && (
                                        <div className="rounded-lg border border-amber-500/20 bg-amber-500/5 p-3">
                                            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-amber-300">What Hurt</p>
                                            <p className="mt-2 text-sm text-gray-300">{String(sourceAnalysis.what_hurt)}</p>
                                        </div>
                                    )}
                                </div>
                                {lfSession.metadata_pack?.analytics_evidence_summary ? (
                                    <div className="rounded-lg border border-violet-400/20 bg-violet-500/5 p-3">
                                        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-violet-300">Analytics Evidence Summary</p>
                                        <p className="mt-2 text-sm text-gray-300">{String(lfSession.metadata_pack.analytics_evidence_summary)}</p>
                                        <p className="mt-2 text-xs text-gray-500">
                                            {Number(lfSession.metadata_pack.analytics_asset_count || 0)} screenshot{Number(lfSession.metadata_pack.analytics_asset_count || 0) === 1 ? '' : 's'} analyzed
                                            {lfSession.metadata_pack.manual_transcript_supplied ? ' · manual transcript supplied' : ''}
                                        </p>
                                    </div>
                                ) : null}
                                {(Array.isArray(sourceAnalysis.retention_findings) && sourceAnalysis.retention_findings.length > 0) || (Array.isArray(sourceAnalysis.packaging_findings) && sourceAnalysis.packaging_findings.length > 0) ? (
                                    <div className="grid gap-3 md:grid-cols-2">
                                        {Array.isArray(sourceAnalysis.retention_findings) && sourceAnalysis.retention_findings.length > 0 ? (
                                            <div className="rounded-lg border border-sky-400/20 bg-sky-500/5 p-3">
                                                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-sky-300">Retention Findings</p>
                                                <ul className="mt-2 space-y-1 text-sm text-gray-300">
                                                    {sourceAnalysis.retention_findings.slice(0, 5).map((item: string, idx: number) => (
                                                        <li key={`retention-${idx}`}>- {item}</li>
                                                    ))}
                                                </ul>
                                            </div>
                                        ) : null}
                                        {Array.isArray(sourceAnalysis.packaging_findings) && sourceAnalysis.packaging_findings.length > 0 ? (
                                            <div className="rounded-lg border border-amber-400/20 bg-amber-500/5 p-3">
                                                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-amber-300">Packaging Findings</p>
                                                <ul className="mt-2 space-y-1 text-sm text-gray-300">
                                                    {sourceAnalysis.packaging_findings.slice(0, 5).map((item: string, idx: number) => (
                                                        <li key={`packaging-${idx}`}>- {item}</li>
                                                    ))}
                                                </ul>
                                            </div>
                                        ) : null}
                                    </div>
                                ) : null}
                                {preflightStatus ? (
                                    <div className={`rounded-lg border p-3 space-y-3 ${
                                        preflightStatus === 'blocked'
                                            ? 'border-red-400/20 bg-red-500/5'
                                            : preflightStatus === 'needs_attention'
                                                ? 'border-amber-400/20 bg-amber-500/5'
                                                : 'border-emerald-400/20 bg-emerald-500/5'
                                    }`}>
                                        <div className="flex flex-wrap items-start justify-between gap-3">
                                            <div>
                                                <p className={`text-xs font-semibold uppercase tracking-[0.18em] ${
                                                    preflightStatus === 'blocked'
                                                        ? 'text-red-300'
                                                        : preflightStatus === 'needs_attention'
                                                            ? 'text-amber-300'
                                                            : 'text-emerald-300'
                                                }`}>Catalyst Preflight</p>
                                                <p className="mt-2 text-sm text-gray-300">
                                                    {String(catalystPreflight.summary || 'Catalyst is scoring whether this run is safe to finalize.')}
                                                </p>
                                            </div>
                                            <div className="rounded-lg border border-white/[0.08] bg-black/20 px-3 py-2 text-xs text-gray-200 space-y-1 min-w-[140px]">
                                                <div className="uppercase tracking-[0.16em] text-[10px] text-gray-500">{preflightStatus.replace('_', ' ')}</div>
                                                <div className="text-lg font-semibold text-white">{preflightReadinessScore ? preflightReadinessScore.toFixed(0) : 'n/a'}</div>
                                                <div className="text-gray-400">readiness score</div>
                                            </div>
                                        </div>
                                        <div className="grid gap-3 lg:grid-cols-2">
                                            {preflightBlockers.length > 0 ? (
                                                <div className="rounded-lg border border-red-400/20 bg-black/20 p-3">
                                                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-red-300">Blocking Issues</p>
                                                    <ul className="mt-2 space-y-1 text-sm text-gray-300">
                                                        {preflightBlockers.slice(0, 5).map((item: any, idx: number) => (
                                                            <li key={`preflight-blocker-${idx}`}>- {String(item)}</li>
                                                        ))}
                                                    </ul>
                                                </div>
                                            ) : null}
                                            {preflightWarnings.length > 0 ? (
                                                <div className="rounded-lg border border-amber-400/20 bg-black/20 p-3">
                                                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-amber-300">Warnings</p>
                                                    <ul className="mt-2 space-y-1 text-sm text-gray-300">
                                                        {preflightWarnings.slice(0, 5).map((item: any, idx: number) => (
                                                            <li key={`preflight-warning-${idx}`}>- {String(item)}</li>
                                                        ))}
                                                    </ul>
                                                </div>
                                            ) : null}
                                            {preflightStrengths.length > 0 ? (
                                                <div className="rounded-lg border border-emerald-400/20 bg-black/20 p-3">
                                                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-emerald-300">What Is Holding Up</p>
                                                    <ul className="mt-2 space-y-1 text-sm text-gray-300">
                                                        {preflightStrengths.slice(0, 5).map((item: any, idx: number) => (
                                                            <li key={`preflight-strength-${idx}`}>- {String(item)}</li>
                                                        ))}
                                                    </ul>
                                                </div>
                                            ) : null}
                                            {preflightNextFixes.length > 0 ? (
                                                <div className="rounded-lg border border-cyan-400/20 bg-black/20 p-3">
                                                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-cyan-300">Next Fixes</p>
                                                    <ul className="mt-2 space-y-1 text-sm text-gray-300">
                                                        {preflightNextFixes.slice(0, 5).map((item: any, idx: number) => (
                                                            <li key={`preflight-next-${idx}`}>- {String(item)}</li>
                                                        ))}
                                                    </ul>
                                                </div>
                                            ) : null}
                                        </div>
                                    </div>
                                ) : null}
                                {rewriteCategories.length > 0 ? (
                                    <div className="rounded-lg border border-rose-400/20 bg-rose-500/5 p-3 space-y-3">
                                        <div className="flex flex-wrap items-start justify-between gap-3">
                                            <div>
                                                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-rose-300">Catalyst Rewrite Pressure</p>
                                                <p className="mt-2 text-sm text-gray-300">
                                                    {String(rewritePressure.summary || 'Catalyst is ranking which parts of the next run need the hardest correction.')}
                                                </p>
                                            </div>
                                            {rewritePressure.primary_focus ? (
                                                <div className="rounded-lg border border-rose-400/20 bg-black/20 px-3 py-2 text-xs text-rose-100/90 space-y-1">
                                                    <div>Primary: {String(rewritePressure.primary_focus)}</div>
                                                    <div>Secondary: {String(rewritePressure.secondary_focus || 'n/a')}</div>
                                                </div>
                                            ) : null}
                                        </div>
                                        <div className="grid gap-3 md:grid-cols-5 text-xs text-gray-300">
                                            {rewriteCategories.slice(0, 5).map((entry: any) => (
                                                <div key={String(entry.key)} className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                                    <div className="text-gray-500">{String(entry.label || entry.key)}</div>
                                                    <div className="mt-1 text-lg font-semibold text-white">{Number(entry.score || 0).toFixed(0)}</div>
                                                    <div className="mt-1 uppercase tracking-[0.16em] text-[10px] text-rose-200/80">{String(entry.severity || 'stable')}</div>
                                                </div>
                                            ))}
                                        </div>
                                        {rewritePriorities.length > 0 ? (
                                            <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-gray-400">Next-Run Priorities</p>
                                                <ul className="mt-2 space-y-1 text-sm text-gray-300">
                                                    {rewritePriorities.slice(0, 5).map((item: any, idx: number) => (
                                                        <li key={`rewrite-priority-${idx}`}>- {String(item)}</li>
                                                    ))}
                                                </ul>
                                            </div>
                                        ) : null}
                                    </div>
                                ) : null}
                                {hasPublishPackage && (titleVariants.length > 0 || descriptionVariants.length > 0 || thumbnailPrompts.length > 0 || publishTags.length > 0) ? (
                                    <div className="grid gap-3 lg:grid-cols-3">
                                        {titleVariants.length > 0 ? (
                                            <div className="rounded-lg border border-violet-400/20 bg-violet-500/5 p-3">
                                                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-violet-300">Title Variants</p>
                                                <ul className="mt-2 space-y-2 text-sm text-gray-300">
                                                    {titleVariants.slice(0, 3).map((item, idx) => (
                                                        <li key={`title-${idx}`}>{idx + 1}. {item}</li>
                                                    ))}
                                                </ul>
                                            </div>
                                        ) : null}
                                        {descriptionVariants.length > 0 ? (
                                            <div className="rounded-lg border border-emerald-400/20 bg-emerald-500/5 p-3">
                                                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">Description Variants</p>
                                                <ul className="mt-2 space-y-2 text-sm text-gray-300">
                                                    {descriptionVariants.slice(0, 3).map((item, idx) => (
                                                        <li key={`description-${idx}`}>{idx + 1}. {item}</li>
                                                    ))}
                                                </ul>
                                            </div>
                                        ) : null}
                                        {thumbnailPrompts.length > 0 ? (
                                            <div className="rounded-lg border border-amber-400/20 bg-amber-500/5 p-3">
                                                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-amber-300">Thumbnail Angles</p>
                                                <ul className="mt-2 space-y-2 text-sm text-gray-300">
                                                    {thumbnailPrompts.slice(0, 3).map((item, idx) => (
                                                        <li key={`thumb-${idx}`}>{idx + 1}. {item}</li>
                                                    ))}
                                                </ul>
                                            </div>
                                        ) : null}
                                        {publishTags.length > 0 ? (
                                            <div className="rounded-lg border border-sky-400/20 bg-sky-500/5 p-3">
                                                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-sky-300">Tags</p>
                                                <div className="mt-2 flex flex-wrap gap-2 text-xs text-sky-100">
                                                    {publishTags.slice(0, 16).map((item, idx) => (
                                                        <span key={`tag-${idx}`} className="rounded-full border border-sky-400/25 bg-sky-500/10 px-2 py-1">
                                                            {item}
                                                        </span>
                                                    ))}
                                                </div>
                                            </div>
                                        ) : null}
                                    </div>
                                ) : null}
                            </div>
                        )}

                        <div className="flex flex-wrap gap-2">
                            <button
                                onClick={finalizeSession}
                                disabled={finalizing || !review?.all_approved || !!lfSession.paused_error || lfSession.status === 'rendering'}
                                className="px-4 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-sm font-medium disabled:opacity-50 transition"
                            >
                                {finalizing ? 'Queueing Render...' : 'Finalize & Render'}
                            </button>
                            {lfSession.status !== 'complete' && lfSession.status !== 'stopped' && (
                                <button
                                    onClick={stopSession}
                                    disabled={stopping}
                                    className="px-4 py-2 rounded-lg bg-red-600/90 hover:bg-red-500 text-white text-sm font-medium disabled:opacity-50 transition"
                                >
                                    {stopping ? 'Stopping...' : 'Stop Session'}
                                </button>
                            )}
                            <button
                                onClick={() => refreshStatus(lfSession.session_id, false)}
                                disabled={refreshing}
                                className="px-3 py-2 rounded-lg bg-white/10 hover:bg-white/15 text-white text-sm disabled:opacity-60 transition"
                            >
                                {refreshing ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Refresh'}
                            </button>
                            {outputUrl && (
                                <a
                                    href={outputUrl}
                                    target="_blank"
                                    rel="noreferrer"
                                    className="px-3 py-2 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-sm inline-flex items-center gap-2"
                                >
                                    <Download className="w-4 h-4" />
                                    Download Long-Form MP4
                                </a>
                            )}
                        </div>
                    </div>

                    <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-5 space-y-3">
                        <h3 className="font-semibold text-white">Chapter Review</h3>
                        <div className="space-y-3">
                            {lfSession.chapters.map((chapter) => (
                                <div key={chapter.index} className="rounded-lg border border-white/[0.08] bg-black/20 p-4 space-y-3">
                                    <div className="flex flex-wrap items-center justify-between gap-2">
                                        <div>
                                            <p className="text-sm font-semibold text-white">
                                                {chapter.index + 1}. {chapter.title}
                                            </p>
                                            <p className="text-xs text-gray-400 mt-1">{chapter.summary}</p>
                                        </div>
                                        <span className={`text-xs border px-2 py-1 rounded-full ${chapterStatusClass(chapter.status)}`}>
                                            {chapter.status}
                                        </span>
                                    </div>
                                    <div className="text-xs text-gray-400 flex flex-wrap gap-3">
                                        <span className="inline-flex items-center gap-1"><Clock3 className="w-3 h-3" /> {Math.round(chapter.target_sec)}s</span>
                                        <span>Scenes: {chapter.scene_count}</span>
                                        <span>Viral score: {chapter.viral_score}</span>
                                        <span>Retries: {chapter.retry_count}</span>
                                        {chapter.brand_slot ? <span>Brand slot: {chapter.brand_slot}</span> : null}
                                    </div>
                                    {Array.isArray(chapter.scenes) && chapter.scenes.length > 0 ? (
                                        <div className="grid sm:grid-cols-2 xl:grid-cols-3 gap-3">
                                            {chapter.scenes.map((scene) => {
                                                const imageUrl = resolveSceneImageUrl(scene.image_url);
                                                const sceneAssignmentBusyKey = `scene-assignment:${chapter.index}:${scene.scene_num}`;
                                                return (
                                                    <div key={`${chapter.index}-${scene.scene_num}`} className="rounded-lg border border-white/[0.08] bg-white/[0.02] p-2 space-y-2">
                                                        <div className="aspect-video rounded-md bg-black/40 border border-white/[0.08] overflow-hidden">
                                                            {imageUrl ? (
                                                                <img src={imageUrl} alt={`Chapter ${chapter.index + 1} scene ${scene.scene_num}`} className="w-full h-full object-contain bg-black" />
                                                            ) : (
                                                                <div className="w-full h-full flex items-center justify-center text-[11px] text-gray-400">
                                                                    {scene.image_status === 'generating' ? 'Generating preview...' : 'Preview unavailable'}
                                                                </div>
                                                            )}
                                                        </div>
                                                        <div className="text-[11px] text-gray-300 flex items-center justify-between">
                                                            <span>Scene {scene.scene_num}</span>
                                                            <span>{Math.round(Number(scene.duration_sec || 5))}s</span>
                                                        </div>
                                                        {scene.image_provider_label ? (
                                                            <p className="text-[10px] uppercase tracking-[0.16em] text-cyan-300/80">
                                                                Provider: {scene.image_provider_label}
                                                            </p>
                                                        ) : null}
                                                        <div className="space-y-1">
                                                            <p className="text-[10px] uppercase tracking-[0.16em] text-fuchsia-300/80">Primary Character</p>
                                                            <select
                                                                value={String(scene.assigned_character_id || '')}
                                                                onChange={(e) => saveSceneCharacterAssignment(chapter.index, scene.scene_num, e.target.value)}
                                                                disabled={actionBusy === sceneAssignmentBusyKey}
                                                                className="w-full rounded-lg border border-white/[0.1] bg-black/30 px-2 py-2 text-[11px] text-white"
                                                            >
                                                                <option value="">No explicit character</option>
                                                                {characterReferences.map((character) => (
                                                                    <option key={character.character_id} value={character.character_id}>
                                                                        {character.name}
                                                                    </option>
                                                                ))}
                                                            </select>
                                                            <p className="text-[10px] text-gray-500">
                                                                {scene.assigned_character_name
                                                                    ? `Assigned: ${scene.assigned_character_name}. Regenerate the chapter to refresh the preview with this identity.`
                                                                    : 'Use a named character when this scene needs the same recurring person.'}
                                                            </p>
                                                        </div>
                                                        <p className="text-[11px] text-gray-500">{scene.narration || 'No narration yet.'}</p>
                                                        {scene.image_error ? <p className="text-[11px] text-red-300">{scene.image_error}</p> : null}
                                                    </div>
                                                );
                                            })}
                                        </div>
                                    ) : null}
                                    {chapter.last_error && !String(chapter.last_error).toLowerCase().includes('fallback draft generated') ? (
                                        <p className="text-xs text-red-300">Last error: {chapter.last_error}</p>
                                    ) : null}
                                    <div className="grid md:grid-cols-[1fr_auto] gap-2">
                                        <input
                                            value={chapterReasons[chapter.index] || ''}
                                            onChange={(e) => setChapterReasons(prev => ({ ...prev, [chapter.index]: e.target.value }))}
                                            placeholder="Regeneration note (optional)"
                                            className="rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white"
                                        />
                                        <div className="flex gap-2">
                                            <button
                                                onClick={() => chapterAction(chapter.index, 'approve')}
                                                disabled={actionBusy.length > 0 || chapter.status === 'approved' || chapter.status === 'draft_generating' || chapter.status === 'draft_generating_images' || chapter.status === 'awaiting_previous_approval'}
                                                className="px-3 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-sm disabled:opacity-50 inline-flex items-center gap-1"
                                            >
                                                <CheckCircle2 className="w-4 h-4" />
                                                Approve
                                            </button>
                                            <button
                                                onClick={() => chapterAction(chapter.index, 'regenerate')}
                                                disabled={actionBusy.length > 0 || chapter.status === 'draft_generating' || chapter.status === 'draft_generating_images' || chapter.status === 'awaiting_previous_approval'}
                                                className="px-3 py-2 rounded-lg bg-white/10 hover:bg-white/15 text-white text-sm disabled:opacity-50 inline-flex items-center gap-1"
                                            >
                                                <RotateCcw className="w-4 h-4" />
                                                Regenerate
                                            </button>
                                        </div>
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>

                    {hasPublishPackage ? (
                        <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-5">
                            <h3 className="font-semibold text-white mb-3">Publish Package</h3>
                            <div className="grid gap-4 lg:grid-cols-[320px,1fr] mb-4">
                                <div className="rounded-xl border border-white/[0.08] bg-black/20 p-3 space-y-3">
                                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-violet-300">Selected Thumbnail</p>
                                    {packageThumbnailUrl ? (
                                        <img
                                            src={packageThumbnailUrl}
                                            alt="Generated publish thumbnail"
                                            className="w-full aspect-video object-cover rounded-lg border border-white/[0.08]"
                                        />
                                    ) : (
                                        <div className="w-full aspect-video rounded-lg border border-dashed border-white/[0.12] bg-black/20 flex items-center justify-center text-xs text-gray-500">
                                            Thumbnail not generated yet
                                        </div>
                                    )}
                                    {packageThumbnailError ? (
                                        <p className="text-xs text-amber-300">{packageThumbnailError}</p>
                                    ) : null}
                                </div>
                                <div className="rounded-xl border border-white/[0.08] bg-black/20 p-4 space-y-3">
                                    <div>
                                        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-violet-300">Selected Title</p>
                                        <p className="mt-2 text-base font-semibold text-white">{selectedTitle || 'Pending title selection'}</p>
                                    </div>
                                    <div>
                                        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">Selected Description</p>
                                        <p className="mt-2 text-sm text-gray-300">{selectedDescription || 'Pending description selection'}</p>
                                    </div>
                                    <div>
                                        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-sky-300">Selected Tags</p>
                                        <div className="mt-2 flex flex-wrap gap-2 text-xs text-sky-100">
                                            {selectedTags.map((tag, idx) => (
                                                <span key={`selected-tag-${idx}`} className="rounded-full border border-sky-400/25 bg-sky-500/10 px-2 py-1">
                                                    {tag}
                                                </span>
                                            ))}
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div className="grid md:grid-cols-4 gap-4 text-xs text-gray-300">
                                <div>
                                    <p className="text-gray-500 mb-1">Title Variants</p>
                                    <ul className="space-y-1">
                                        {titleVariants.map((t, idx) => (
                                            <li key={`${t}-${idx}`}>- {t}</li>
                                        ))}
                                    </ul>
                                </div>
                                <div>
                                    <p className="text-gray-500 mb-1">Description Variants</p>
                                    <ul className="space-y-1">
                                        {descriptionVariants.map((d, idx) => (
                                            <li key={`${idx}-${d.slice(0, 20)}`}>- {d}</li>
                                        ))}
                                    </ul>
                                </div>
                                <div>
                                    <p className="text-gray-500 mb-1">Thumbnail Angles</p>
                                    <ul className="space-y-1">
                                        {thumbnailPrompts.map((p, idx) => (
                                            <li key={`${idx}-${p.slice(0, 20)}`}>- {p}</li>
                                        ))}
                                    </ul>
                                </div>
                                <div>
                                    <p className="text-gray-500 mb-1">Tags</p>
                                    <ul className="space-y-1">
                                        {publishTags.map((tag, idx) => (
                                            <li key={`${idx}-${tag}`}>- {tag}</li>
                                        ))}
                                    </ul>
                                </div>
                            </div>
                        </div>
                    ) : null}

                    {hasPublishPackage ? (
                        <div className="rounded-xl border border-cyan-400/20 bg-cyan-500/5 p-5 space-y-4">
                            <div className="flex flex-wrap items-start justify-between gap-3">
                                <div>
                                    <h3 className="font-semibold text-white">Post-Publish Outcome Ingestion</h3>
                                    <p className="mt-1 text-xs text-cyan-100/80">
                                        Feed the real result back into Catalyst after the video goes live. If you paste the live YouTube URL, Studio will auto-pull what it can from the connected channel before applying the weighted memory update.
                                    </p>
                                </div>
                                {channelMemory.summary ? (
                                    <div className="rounded-lg border border-cyan-400/20 bg-black/20 px-3 py-2 text-xs text-cyan-100/90">
                                        <div>Measured outcomes: {Number(channelMemory.outcome_count || 0)}</div>
                                        <div>Avg CTR: {Number(channelMemory.average_ctr || 0).toFixed(2)}%</div>
                                        <div>Avg viewed: {Number(channelMemory.average_average_percentage_viewed || 0).toFixed(2)}%</div>
                                    </div>
                                ) : null}
                            </div>

                            <div className="grid gap-3 md:grid-cols-4">
                                <label className="text-sm text-gray-300 md:col-span-2">
                                    Published YouTube URL
                                    <input
                                        value={outcomeVideoUrl}
                                        onChange={(e) => setOutcomeVideoUrl(e.target.value)}
                                        placeholder="https://www.youtube.com/watch?v=..."
                                        className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white"
                                    />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Views
                                    <input value={outcomeViews} onChange={(e) => setOutcomeViews(e.target.value)} placeholder="309" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Impressions
                                    <input value={outcomeImpressions} onChange={(e) => setOutcomeImpressions(e.target.value)} placeholder="5000" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    AVD (sec)
                                    <input value={outcomeAvd} onChange={(e) => setOutcomeAvd(e.target.value)} placeholder="158" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Avg Viewed %
                                    <input value={outcomeAvp} onChange={(e) => setOutcomeAvp(e.target.value)} placeholder="30.8" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    CTR %
                                    <input value={outcomeCtr} onChange={(e) => setOutcomeCtr(e.target.value)} placeholder="1.4" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    First 30s Retention %
                                    <input value={outcomeFirst30} onChange={(e) => setOutcomeFirst30(e.target.value)} placeholder="72" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    First 60s Retention %
                                    <input value={outcomeFirst60} onChange={(e) => setOutcomeFirst60(e.target.value)} placeholder="58" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                            </div>

                            <label className="block text-sm text-gray-300">
                                Operator Summary
                                <textarea
                                    value={outcomeSummary}
                                    onChange={(e) => setOutcomeSummary(e.target.value)}
                                    rows={3}
                                    placeholder="What did this video do well? What clearly hurt it? What should Catalyst change on the next run?"
                                    className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white"
                                />
                            </label>

                            <div className="grid gap-3 md:grid-cols-2">
                                <label className="text-sm text-gray-300">
                                    Strongest Signals
                                    <textarea value={outcomeStrongSignals} onChange={(e) => setOutcomeStrongSignals(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Weak Points
                                    <textarea value={outcomeWeakPoints} onChange={(e) => setOutcomeWeakPoints(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Hook Wins
                                    <textarea value={outcomeHookWins} onChange={(e) => setOutcomeHookWins(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Hook Watchouts
                                    <textarea value={outcomeHookWatchouts} onChange={(e) => setOutcomeHookWatchouts(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Pacing Wins
                                    <textarea value={outcomePacingWins} onChange={(e) => setOutcomePacingWins(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Pacing Watchouts
                                    <textarea value={outcomePacingWatchouts} onChange={(e) => setOutcomePacingWatchouts(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Visual Wins
                                    <textarea value={outcomeVisualWins} onChange={(e) => setOutcomeVisualWins(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Visual Watchouts
                                    <textarea value={outcomeVisualWatchouts} onChange={(e) => setOutcomeVisualWatchouts(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Sound Wins
                                    <textarea value={outcomeSoundWins} onChange={(e) => setOutcomeSoundWins(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Sound Watchouts
                                    <textarea value={outcomeSoundWatchouts} onChange={(e) => setOutcomeSoundWatchouts(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Packaging Wins
                                    <textarea value={outcomePackagingWins} onChange={(e) => setOutcomePackagingWins(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Packaging Watchouts
                                    <textarea value={outcomePackagingWatchouts} onChange={(e) => setOutcomePackagingWatchouts(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Retention Wins
                                    <textarea value={outcomeRetentionWins} onChange={(e) => setOutcomeRetentionWins(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                                <label className="text-sm text-gray-300">
                                    Retention Watchouts
                                    <textarea value={outcomeRetentionWatchouts} onChange={(e) => setOutcomeRetentionWatchouts(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                                </label>
                            </div>

                            <label className="block text-sm text-gray-300">
                                Next Video Moves
                                <textarea value={outcomeNextMoves} onChange={(e) => setOutcomeNextMoves(e.target.value)} rows={4} placeholder="One item per line" className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white" />
                            </label>

                            <div className="flex flex-wrap gap-3">
                                <button
                                    onClick={() => void syncConnectedChannelOutcomes(true)}
                                    disabled={outcomeSaving || outcomeAutoSaving || youtubeOutcomeSyncing || !youtubeChannelId}
                                    className="px-4 py-2 rounded-lg border border-amber-400/30 bg-amber-500/10 hover:bg-amber-500/20 text-amber-100 text-sm font-medium disabled:opacity-50 transition"
                                >
                                    {youtubeOutcomeSyncing ? 'Syncing Published Outcome...' : 'Sync Current Session From Channel'}
                                </button>
                                <button
                                    onClick={autoPullOutcome}
                                    disabled={outcomeSaving || outcomeAutoSaving || youtubeOutcomeSyncing}
                                    className="px-4 py-2 rounded-lg border border-cyan-400/30 bg-black/20 hover:bg-cyan-500/10 text-cyan-100 text-sm font-medium disabled:opacity-50 transition"
                                >
                                    {outcomeAutoSaving ? 'Auto-Pulling Channel Outcome...' : 'Auto-Pull From Connected Channel'}
                                </button>
                                <button
                                    onClick={submitOutcome}
                                    disabled={outcomeSaving || outcomeAutoSaving || youtubeOutcomeSyncing}
                                    className="px-4 py-2 rounded-lg bg-cyan-600 hover:bg-cyan-500 text-white text-sm font-medium disabled:opacity-50 transition"
                                >
                                    {outcomeSaving ? 'Updating Catalyst Memory...' : 'Ingest Outcome Into Catalyst'}
                                </button>
                            </div>

                            {latestOutcome?.operator_summary ? (
                                <div className="rounded-xl border border-cyan-400/20 bg-black/20 p-4 space-y-2">
                                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300">Latest Outcome Memory</p>
                                    <p className="text-sm text-gray-300">{String(latestOutcome.operator_summary)}</p>
                                    <div className="grid gap-3 md:grid-cols-3 text-xs text-gray-300">
                                        <div>Weight: {Number(latestOutcome.weight || 0).toFixed(2)}</div>
                                        <div>CTR: {Number(latestOutcome.metrics?.impression_click_through_rate || 0).toFixed(2)}%</div>
                                        <div>Avg Viewed: {Number(latestOutcome.metrics?.average_percentage_viewed || 0).toFixed(2)}%</div>
                                    </div>
                                </div>
                            ) : null}

                            {Object.keys(referenceComparison).length ? (
                                <div className="rounded-xl border border-amber-400/20 bg-amber-500/5 p-4 space-y-4">
                                    <div className="flex flex-wrap items-start justify-between gap-3">
                                        <div>
                                            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-amber-300">Catalyst Reference Delta</p>
                                            <p className="mt-1 text-sm text-gray-300">
                                                {String(referenceComparison.reference_summary || 'Measured against the documentary reference playbook to drive the next run.')}
                                            </p>
                                        </div>
                                        <div className="rounded-lg border border-amber-400/20 bg-black/20 px-3 py-2 text-xs text-amber-100/90 space-y-1">
                                            <div>Tier: {String(referenceComparison.tier || 'n/a')}</div>
                                            <div>Overall: {Number(referenceScores.overall || 0).toFixed(0)}/100</div>
                                        </div>
                                    </div>

                                    <div className="grid gap-3 md:grid-cols-6 text-xs text-gray-300">
                                        <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                            <div className="text-gray-500">Hook</div>
                                            <div className="mt-1 text-lg font-semibold text-white">{Number(referenceScores.hook || 0).toFixed(0)}</div>
                                        </div>
                                        <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                            <div className="text-gray-500">Pacing</div>
                                            <div className="mt-1 text-lg font-semibold text-white">{Number(referenceScores.pacing || 0).toFixed(0)}</div>
                                        </div>
                                        <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                            <div className="text-gray-500">Visuals</div>
                                            <div className="mt-1 text-lg font-semibold text-white">{Number(referenceScores.visuals || 0).toFixed(0)}</div>
                                        </div>
                                        <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                            <div className="text-gray-500">Sound</div>
                                            <div className="mt-1 text-lg font-semibold text-white">{Number(referenceScores.sound || 0).toFixed(0)}</div>
                                        </div>
                                        <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                            <div className="text-gray-500">Packaging</div>
                                            <div className="mt-1 text-lg font-semibold text-white">{Number(referenceScores.packaging || 0).toFixed(0)}</div>
                                        </div>
                                        <div className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                            <div className="text-gray-500">Title Novelty</div>
                                            <div className="mt-1 text-lg font-semibold text-white">{Number(referenceScores.title_novelty || 0).toFixed(0)}</div>
                                        </div>
                                    </div>

                                    {referenceChannels.length ? (
                                        <div className="text-xs text-gray-400">
                                            Best matching reference channels: {referenceChannels.join(', ')}
                                        </div>
                                    ) : null}

                                    <div className="grid gap-3 md:grid-cols-2">
                                        {[
                                            ['Hook rewrites', referenceComparison.hook_rewrites],
                                            ['Pacing rewrites', referenceComparison.pacing_rewrites],
                                            ['Visual rewrites', referenceComparison.visual_rewrites],
                                            ['Sound rewrites', referenceComparison.sound_rewrites],
                                            ['Packaging rewrites', referenceComparison.packaging_rewrites],
                                            ['Next-run moves', referenceComparison.next_run_moves],
                                        ].map(([label, values]) => {
                                            const rows = Array.isArray(values) ? values.filter((value: any) => String(value || '').trim()) : [];
                                            if (!rows.length) return null;
                                            return (
                                                <div key={String(label)} className="rounded-lg border border-white/[0.08] bg-black/20 p-3">
                                                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-gray-400">{label}</p>
                                                    <ul className="mt-2 space-y-1 text-sm text-gray-300">
                                                        {rows.slice(0, 4).map((value: any, idx: number) => (
                                                            <li key={`${label}-${idx}`}>- {String(value)}</li>
                                                        ))}
                                                    </ul>
                                                </div>
                                            );
                                        })}
                                    </div>
                                </div>
                            ) : null}
                        </div>
                    ) : null}
                </div>
            )}
                </>
            )}

            {activeTab === 'projects' && (
                <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-5 space-y-4">
                    <div className="flex flex-wrap items-center justify-between gap-3">
                        <h3 className="font-semibold text-white flex items-center gap-2">
                            <FolderKanban className="w-4 h-4 text-violet-300" />
                            Long-Form Projects
                        </h3>
                        <button
                            onClick={() => loadProjects()}
                            disabled={projectsLoading}
                            className="px-3 py-2 rounded-lg bg-white/10 hover:bg-white/15 text-white text-sm disabled:opacity-60 transition"
                        >
                            {projectsLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Refresh'}
                        </button>
                    </div>
                    {projectsError ? <p className="text-sm text-red-400">{projectsError}</p> : null}
                    {projectsLoading ? <p className="text-sm text-gray-400">Loading long-form projects...</p> : null}
                    {!projectsLoading && projectSessions.length === 0 ? (
                        <p className="text-sm text-gray-500">No long-form sessions found yet.</p>
                    ) : null}
                    <div className="space-y-3">
                        {projectSessions.map((project) => {
                            const previewUrl = resolveSceneImageUrl(project.preview_image_url);
                            const approved = Number(project.review_state?.approved_chapters || 0);
                            const total = Number(project.review_state?.total_chapters || 0);
                            const outputUrlForProject = project.output_file ? `${API}/api/download/${project.output_file}` : '';
                            const title = String(project.input_title || project.topic || 'Untitled session');
                            return (
                                <div key={project.session_id} className="rounded-lg border border-white/[0.08] bg-black/20 p-4 space-y-3">
                                    <div className="flex flex-wrap items-start justify-between gap-3">
                                        <div className="flex items-start gap-3">
                                            <div className="w-36 aspect-video rounded-md bg-black/40 border border-white/[0.08] overflow-hidden flex items-center justify-center">
                                                {previewUrl ? (
                                                    <img src={previewUrl} alt={`Preview ${project.session_id}`} className="w-full h-full object-contain bg-black" />
                                                ) : (
                                                    <span className="text-[11px] text-gray-500 px-2 text-center">No scene preview yet</span>
                                                )}
                                            </div>
                                            <div className="space-y-1">
                                                <p className="text-sm font-semibold text-white">{title}</p>
                                                <p className="text-xs text-gray-500">{project.session_id}</p>
                                                <p className="text-xs text-gray-400">
                                                    {PRESET_LABELS[(project.format_preset as LongFormPreset) || 'documentary']} - {STATUS_LABELS[project.status] || project.status} - {approved}/{total} chapters approved
                                                </p>
                                                {project.auto_pipeline ? (
                                                    <p className="text-xs text-cyan-300">Owner autopilot enabled</p>
                                                ) : null}
                                                <p className="text-xs text-gray-500">
                                                    Updated {formatTimestamp(project.updated_at)}
                                                </p>
                                            </div>
                                        </div>
                                        <div className="flex flex-wrap items-center gap-2">
                                            <button
                                                onClick={() => void openProjectSession(project.session_id)}
                                                className="px-3 py-2 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-sm font-medium transition"
                                            >
                                                Resume
                                            </button>
                                            {outputUrlForProject ? (
                                                <a
                                                    href={outputUrlForProject}
                                                    target="_blank"
                                                    rel="noreferrer"
                                                    className="px-3 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-sm inline-flex items-center gap-2"
                                                >
                                                    <Download className="w-4 h-4" />
                                                    Download
                                                </a>
                                            ) : null}
                                        </div>
                                    </div>
                                    {project.paused_error ? (
                                        <p className="text-xs text-amber-300">
                                            Paused: {String(project.paused_error.error || 'manual review needed')}
                                        </p>
                                    ) : null}
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}
            {longformRenderProgressWindow}
        </div>
    );
}
