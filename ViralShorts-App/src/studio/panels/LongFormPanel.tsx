import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { AlertTriangle, CheckCircle2, Clock3, Download, FolderKanban, Loader2, RefreshCw, RotateCcw, Sparkles, Wand2 } from 'lucide-react';
import { API, AuthContext } from '../shared';
import { ProgressBar } from '../components/StudioWidgets';

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
        image_url: string;
        image_status: string;
        image_error: string;
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

type LongFormSession = {
    session_id: string;
    template: string;
    topic: string;
    input_title: string;
    input_description: string;
    target_minutes: number;
    language: string;
    resolution: string;
    animation_enabled: boolean;
    sfx_enabled: boolean;
    whisper_mode: string;
    status: string;
    job_id: string;
    paused_error: any;
    metadata_pack: {
        title_variants?: string[];
        description_variants?: string[];
        thumbnail_prompts?: string[];
        tags?: string[];
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
    };
};

type LongFormSessionSummary = {
    session_id: string;
    template: string;
    topic: string;
    input_title: string;
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

type LongFormPreset = 'recap' | 'explainer' | 'documentary' | 'story_channel';

const STATUS_LABELS: Record<string, string> = {
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

export default function LongFormPanel() {
    const { session } = useContext(AuthContext);
    const [activeTab, setActiveTab] = useState<'create' | 'projects'>('create');
    const [template, setTemplate] = useState<'story' | 'skeleton'>('story');
    const [formatPreset, setFormatPreset] = useState<LongFormPreset>('explainer');
    const [topic, setTopic] = useState('');
    const [inputTitle, setInputTitle] = useState('');
    const [inputDescription, setInputDescription] = useState('');
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
    const [actionBusy, setActionBusy] = useState('');
    const [error, setError] = useState('');
    const [projectsError, setProjectsError] = useState('');
    const [chapterReasons, setChapterReasons] = useState<Record<number, string>>({});
    const [fixNote, setFixNote] = useState('');
    const [sessionIdInput, setSessionIdInput] = useState('');
    const [projectSessions, setProjectSessions] = useState<LongFormSessionSummary[]>([]);
    const [projectsLoading, setProjectsLoading] = useState(false);
    const restoredSessionUserRef = useRef('');

    const lastSessionStorageKey = useMemo(() => {
        const uid = String(session?.user?.id || 'guest').trim() || 'guest';
        return `nyptid_longform_last_session_${uid}`;
    }, [session?.user?.id]);

    const persistSessionId = useCallback((value: string) => {
        try {
            const sid = String(value || '').trim();
            if (sid) {
                localStorage.setItem(lastSessionStorageKey, sid);
            } else {
                localStorage.removeItem(lastSessionStorageKey);
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
            setError(e?.message || 'Failed to load long-form status');
        } finally {
            if (!silent) setRefreshing(false);
        }
    }, [apiCall, lfSession?.session_id, persistSessionId]);

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
        const uid = String(session?.user?.id || '').trim();
        if (!uid) return;
        if (restoredSessionUserRef.current === uid) return;
        restoredSessionUserRef.current = uid;
        let saved = '';
        try {
            saved = String(localStorage.getItem(lastSessionStorageKey) || '').trim();
        } catch {
            saved = '';
        }
        if (!saved) return;
        setSessionIdInput(saved);
        void refreshStatus(saved, true);
    }, [lastSessionStorageKey, refreshStatus, session?.user?.id]);

    useEffect(() => {
        if (!lfSession?.session_id) return;
        persistSessionId(lfSession.session_id);
    }, [lfSession?.session_id, persistSessionId]);

    const createSession = useCallback(async () => {
        if (!session) return;
        setCreating(true);
        setError('');
        try {
            const presetLabelMap: Record<LongFormPreset, string> = {
                recap: 'Recap',
                explainer: 'Explainer',
                documentary: 'Documentary',
                story_channel: 'Story Channel',
            };
            const payload = await apiCall('/api/longform/session', {
                method: 'POST',
                body: JSON.stringify({
                    template,
                    topic: topic.trim(),
                    input_title: inputTitle.trim(),
                    input_description: `Format preset: ${presetLabelMap[formatPreset]}. ${inputDescription.trim()}`.trim(),
                    target_minutes: targetMinutes,
                    language,
                    animation_enabled: animationEnabled,
                    sfx_enabled: sfxEnabled,
                    whisper_mode: whisperMode,
                }),
            });
            const created = (payload as any).session as LongFormSession;
            setLfSession(created);
            setJobStatus(null);
            setSessionIdInput(created?.session_id || '');
            persistSessionId(created?.session_id || '');
            setChapterReasons({});
        } catch (e: any) {
            setError(e?.message || 'Failed to create long-form session');
        } finally {
            setCreating(false);
        }
    }, [
        animationEnabled,
        apiCall,
        inputDescription,
        inputTitle,
        language,
        persistSessionId,
        session,
        sfxEnabled,
        targetMinutes,
        template,
        topic,
        formatPreset,
        whisperMode,
    ]);

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

    const outputFile = String(jobStatus?.output_file || lfSession?.package?.output_file || '');
    const outputUrl = outputFile ? `${API}/api/download/${outputFile}` : '';
    const review = lfSession?.review_state;
    const draftProgress = lfSession?.draft_progress;
    const resolveSceneImageUrl = useCallback((raw: string) => {
        const u = String(raw || '').trim();
        if (!u) return '';
        if (u.startsWith('http://') || u.startsWith('https://')) return u;
        return `${API}${u}`;
    }, []);
    const formatTimestamp = useCallback((value: number) => {
        const ts = Number(value || 0);
        if (ts <= 0) return 'Unknown';
        return new Date(ts * 1000).toLocaleString();
    }, []);

    return (
        <div className="max-w-5xl mx-auto px-6 pb-10 space-y-6">
            <div className="rounded-xl border border-violet-400/25 bg-violet-500/10 p-4">
                <div className="flex items-center gap-2 text-violet-200">
                    <Sparkles className="w-4 h-4" />
                    <p className="text-sm font-semibold">Catalyst Long Form (2 to 10 minutes)</p>
                </div>
                <p className="text-xs text-violet-100/80 mt-1">
                    End-to-end faceless pipeline: topic to chapters, chapter review/approve, finalize render, then package export.
                </p>
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
                    <label className="text-sm text-gray-300">
                        Template
                        <select value={template} onChange={(e) => setTemplate(e.target.value as 'story' | 'skeleton')}
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white">
                            <option value="story">Cinematic</option>
                            <option value="skeleton">Skeleton AI</option>
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
                    <label className="text-sm text-gray-300">
                        Target Minutes
                        <input
                            type="number"
                            min={2}
                            max={10}
                            step={0.5}
                            value={targetMinutes}
                            onChange={(e) => setTargetMinutes(Math.max(2, Math.min(10, Number(e.target.value || 8))))}
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white"
                        />
                    </label>
                    <label className="text-sm text-gray-300 md:col-span-2">
                        Topic
                        <input
                            value={topic}
                            onChange={(e) => setTopic(e.target.value)}
                            placeholder="Why some manga recaps hold attention for 20 minutes while others die in the first 90 seconds"
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white"
                        />
                    </label>
                    <label className="text-sm text-gray-300 md:col-span-2">
                        Video Title
                        <input
                            value={inputTitle}
                            onChange={(e) => setInputTitle(e.target.value)}
                            placeholder="How top recap channels keep viewers watching"
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white"
                        />
                    </label>
                    <label className="text-sm text-gray-300 md:col-span-2">
                        Video Description
                        <textarea
                            value={inputDescription}
                            onChange={(e) => setInputDescription(e.target.value)}
                            rows={3}
                            placeholder="Describe the structure, tone, pacing, references, research angle, and retention goals for this long-form video."
                            className="mt-1 w-full rounded-lg bg-black/30 border border-white/[0.1] px-3 py-2 text-sm text-white resize-none"
                        />
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
                </div>
                <div className="flex flex-wrap gap-3">
                    <button
                        onClick={createSession}
                        disabled={creating || !topic.trim() || !inputTitle.trim() || !inputDescription.trim()}
                        className="px-4 py-2 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-sm font-medium disabled:opacity-60 transition"
                    >
                        {creating ? 'Creating...' : 'Create Session'}
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
                            </div>
                            <div className="text-xs text-gray-400">
                                {review ? `${review.approved_chapters}/${review.total_chapters} chapters approved` : 'No review stats'}
                            </div>
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

                        <div className="flex flex-wrap gap-2">
                            <button
                                onClick={finalizeSession}
                                disabled={finalizing || !review?.all_approved || !!lfSession.paused_error || lfSession.status === 'rendering'}
                                className="px-4 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-sm font-medium disabled:opacity-50 transition"
                            >
                                {finalizing ? 'Queueing Render...' : 'Finalize & Render'}
                            </button>
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
                                                        <p className="text-[11px] text-gray-500">{scene.narration || 'No narration yet.'}</p>
                                                        {scene.image_error ? <p className="text-[11px] text-red-300">{scene.image_error}</p> : null}
                                                    </div>
                                                );
                                            })}
                                        </div>
                                    ) : null}
                                    {chapter.last_error ? (
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

                    <div className="rounded-xl border border-white/[0.08] bg-white/[0.02] p-5">
                        <h3 className="font-semibold text-white mb-3">Metadata Pack</h3>
                        <div className="grid md:grid-cols-3 gap-4 text-xs text-gray-300">
                            <div>
                                <p className="text-gray-500 mb-1">Title Variants</p>
                                <ul className="space-y-1">
                                    {(lfSession.metadata_pack?.title_variants || []).map((t, idx) => (
                                        <li key={`${t}-${idx}`}>- {t}</li>
                                    ))}
                                </ul>
                            </div>
                            <div>
                                <p className="text-gray-500 mb-1">Description Variants</p>
                                <ul className="space-y-1">
                                    {(lfSession.metadata_pack?.description_variants || []).map((d, idx) => (
                                        <li key={`${idx}-${d.slice(0, 20)}`}>- {d}</li>
                                    ))}
                                </ul>
                            </div>
                            <div>
                                <p className="text-gray-500 mb-1">Thumbnail Prompts</p>
                                <ul className="space-y-1">
                                    {(lfSession.metadata_pack?.thumbnail_prompts || []).map((p, idx) => (
                                        <li key={`${idx}-${p.slice(0, 20)}`}>- {p}</li>
                                    ))}
                                </ul>
                            </div>
                        </div>
                    </div>
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
                                                    {project.template} - {STATUS_LABELS[project.status] || project.status} - {approved}/{total} chapters approved
                                                </p>
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
        </div>
    );
}
