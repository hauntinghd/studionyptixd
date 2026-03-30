import { useCallback, useContext, useEffect, useState } from 'react';
import { Camera, CheckCircle2, Download, Eye, Image, Loader2, Palette, Sparkles, Trash2, UploadCloud, X } from 'lucide-react';
import { API, AuthContext, GENERATION_API } from '../shared';
import { ThumbProgressBar } from '../components/StudioWidgets';

interface ThumbFile { id: string; name: string; size: number; url: string; created_at?: number; }

interface TrainingStatus {
    lora_available: boolean;
    is_training: boolean;
    training_available?: boolean;
    total_images: number;
    local_library_images?: number;
    trained_images: number;
    version: number;
    last_train: number;
}

export default function ThumbnailPanel() {
    const { session, ownerOverride, role } = useContext(AuthContext);
    const [subTab, setSubTab] = useState<'generate' | 'library'>('generate');
    const [mode, setMode] = useState<'describe' | 'style_transfer' | 'screenshot_analysis'>('describe');
    const [description, setDescription] = useState('');
    const [styleDesc, setStyleDesc] = useState('');
    const [selectedStyleRef, setSelectedStyleRef] = useState<string>('');
    const [screenshotDesc, setScreenshotDesc] = useState('');
    const [library, setLibrary] = useState<ThumbFile[]>([]);
    const [uploading, setUploading] = useState(false);
    const [jobId, setJobId] = useState<string | null>(null);
    const [jobStatus, setJobStatus] = useState<any>(null);
    const [loading, setLoading] = useState(false);
    const [previewUrl, setPreviewUrl] = useState<string | null>(null);
    const [trainingStatus, setTrainingStatus] = useState<TrainingStatus | null>(null);
    const [thumbFeedbackSent, setThumbFeedbackSent] = useState<Record<string, boolean>>({});
    const [syncingLibrary, setSyncingLibrary] = useState(false);
    const [syncMessage, setSyncMessage] = useState('');
    const withThumbToken = useCallback((path: string) => {
        const base = path.startsWith("/api/thumbnails/generated/") ? GENERATION_API : API;
        if (!session?.access_token) return `${base}${path}`;
        const sep = path.includes('?') ? '&' : '?';
        return `${base}${path}${sep}access_token=${encodeURIComponent(session.access_token)}`;
    }, [session]);
    const parseJsonResponse = useCallback(async (res: Response, fallbackMessage: string) => {
        const raw = await res.text().catch(() => "");
        if (!raw.trim()) return {};
        try {
            return JSON.parse(raw);
        } catch {
            throw new Error(fallbackMessage);
        }
    }, []);

    const fetchLibrary = useCallback(async () => {
        try {
            const res = await fetch(`${API}/api/thumbnails/library`, {
                headers: session ? { Authorization: `Bearer ${session.access_token}` } : {},
            });
            if (res.ok) {
                const data = await parseJsonResponse(res, "Thumbnail library returned non-JSON response.");
                setLibrary((data as any).files || []);
            }
        } catch { /* ignore */ }
    }, [session, parseJsonResponse]);

    const trainingControlsAvailable = Boolean(trainingStatus?.training_available) && (ownerOverride || role === 'admin');

    const fetchTrainingStatus = useCallback(async () => {
        try {
            const res = await fetch(`${API}/api/thumbnails/training-status`, {
                headers: session ? { Authorization: `Bearer ${session.access_token}` } : {},
            });
            if (res.ok) {
                const data = await parseJsonResponse(res, "Training status returned non-JSON response.");
                setTrainingStatus(data as TrainingStatus);
            }
        } catch { /* ignore */ }
    }, [session, parseJsonResponse]);

    useEffect(() => { fetchLibrary(); fetchTrainingStatus(); }, [fetchLibrary, fetchTrainingStatus]);

    useEffect(() => {
        const interval = setInterval(fetchTrainingStatus, 15000);
        return () => clearInterval(interval);
    }, [fetchTrainingStatus]);

    useEffect(() => {
        if (!jobId) return;
        const interval = setInterval(async () => {
            try {
                const res = await fetch(`${GENERATION_API}/api/status/${jobId}`);
                const data = await parseJsonResponse(res, "Status endpoint returned non-JSON response.");
                setJobStatus(data);
                if (data.status === 'complete' || data.status === 'error') {
                    clearInterval(interval);
                    setLoading(false);
                }
            } catch { /* retry */ }
        }, 2000);
        return () => clearInterval(interval);
    }, [jobId, parseJsonResponse]);

    const handleUpload = async (files: FileList) => {
        setUploading(true);
        const formData = new FormData();
        Array.from(files).forEach(f => formData.append('files', f));
        try {
            const res = await fetch(`${API}/api/thumbnails/upload`, {
                method: 'POST',
                headers: session ? { Authorization: `Bearer ${session.access_token}` } : {},
                body: formData,
            });
            if (res.ok) await fetchLibrary();
            else {
                const txt = await res.text().catch(() => "");
                alert(txt || `Upload failed (${res.status})`);
            }
        } catch { /* ignore */ }
        setUploading(false);
    };

    const handleDelete = async (id: string) => {
        await fetch(`${API}/api/thumbnails/library/${id}`, {
            method: 'DELETE',
            headers: session ? { Authorization: `Bearer ${session.access_token}` } : {},
        });
        setLibrary(prev => prev.filter(f => f.id !== id));
        if (selectedStyleRef === id) setSelectedStyleRef('');
    };

    const handleSyncLibrary = useCallback(async () => {
        if (!session) return;
        setSyncingLibrary(true);
        setSyncMessage('');
        try {
            const res = await fetch(`${API}/api/thumbnails/sync-library`, {
                method: 'POST',
                headers: { Authorization: `Bearer ${session.access_token}` },
            });
            const data = await parseJsonResponse(res, "Sync library returned non-JSON response.");
            if (!res.ok) {
                setSyncMessage(data?.detail || `Sync failed (${res.status})`);
                setSyncingLibrary(false);
                return;
            }
            const synced = Number(data?.synced || 0);
            const failed = Number(data?.failed || 0);
            const total = Number(data?.queued || 0);
            if (data?.status === 'no_files') {
                setSyncMessage('No local library files found on this server instance.');
            } else if (failed > 0) {
                setSyncMessage(`Synced ${synced}/${total}. ${failed} failed; check server logs.`);
            } else {
                setSyncMessage(`Sync complete: ${synced}/${total} thumbnails pushed into the training set.`);
            }
            await fetchTrainingStatus();
        } catch {
            setSyncMessage('Sync request failed. Please try again.');
        }
        setSyncingLibrary(false);
    }, [session, fetchTrainingStatus, parseJsonResponse]);

    const sendThumbFeedback = useCallback(async (generationId: string, accepted: boolean) => {
        if (!generationId || !session) return;
        if (thumbFeedbackSent[generationId]) return;
        try {
            const res = await fetch(`${API}/api/thumbnails/feedback`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${session.access_token}` },
                body: JSON.stringify({ generation_id: generationId, accepted }),
            });
            if (res.ok) {
                setThumbFeedbackSent(prev => ({ ...prev, [generationId]: true }));
            }
        } catch {
            // ignore feedback send failures
        }
    }, [session, thumbFeedbackSent]);

    const handleGenerate = async () => {
        if (!description && mode === 'describe') return;
        if (jobStatus?.status === 'complete' && jobStatus?.generation_id) {
            await sendThumbFeedback(jobStatus.generation_id, false);
        }
        setLoading(true);
        setJobStatus(null);
        setJobId(null);

        const headers: Record<string, string> = { 'Content-Type': 'application/json' };
        if (session) headers['Authorization'] = `Bearer ${session.access_token}`;

        try {
            const body: any = { mode, description };
            if (mode === 'style_transfer') {
                body.style_reference_id = selectedStyleRef;
                body.screenshot_description = styleDesc;
            } else if (mode === 'screenshot_analysis') {
                body.screenshot_description = screenshotDesc;
            }

            const res = await fetch(`${GENERATION_API}/api/thumbnails/generate`, {
                method: 'POST', headers, body: JSON.stringify(body),
            });
            const data = await parseJsonResponse(res, "Thumbnail generation returned non-JSON response.");
            if (!res.ok) {
                throw new Error((data as any)?.detail || `Thumbnail generation failed (${res.status})`);
            }
            if (data.job_id) setJobId(data.job_id);
            else {
                setLoading(false);
                alert('Thumbnail generation did not return a job id.');
            }
        } catch (e: any) {
            setLoading(false);
            alert(e?.message || 'Thumbnail generation failed.');
        }
    };

    const modes = [
        { id: 'describe' as const, icon: <Sparkles className="w-4 h-4" />, title: 'Describe', desc: 'Describe the video and generate a click-driven thumbnail' },
        { id: 'style_transfer' as const, icon: <Palette className="w-4 h-4" />, title: 'Style Transfer', desc: 'Use your private library as a style reference' },
        { id: 'screenshot_analysis' as const, icon: <Camera className="w-4 h-4" />, title: 'Channel Analysis', desc: 'Teach Catalyst what already works on your channel' },
    ];

    return (
        <div className="max-w-4xl mx-auto px-6 pb-10 space-y-8">
            <div className="text-center mb-2">
                <h1 className="text-2xl font-bold mb-2">Catalyst Thumbnail Engine</h1>
                <p className="text-gray-500 text-sm max-w-xl mx-auto">Generate click-driven thumbnails, keep private reference winners in your library, and use the same Studio account that owns the rest of your faceless workflow.</p>
            </div>

            <div className="flex gap-1 p-1 bg-white/[0.03] border border-white/[0.06] rounded-xl">
                <button onClick={() => setSubTab('generate')}
                    className={`flex-1 py-2.5 rounded-lg text-sm font-medium transition-all ${
                        subTab === 'generate' ? 'bg-violet-600 text-white shadow-lg shadow-violet-600/20' : 'text-gray-400 hover:text-white'
                    }`}>
                    <Sparkles className="w-4 h-4 inline mr-1.5" />Generate
                </button>
                <button onClick={() => setSubTab('library')}
                    className={`flex-1 py-2.5 rounded-lg text-sm font-medium transition-all ${
                        subTab === 'library' ? 'bg-violet-600 text-white shadow-lg shadow-violet-600/20' : 'text-gray-400 hover:text-white'
                    }`}>
                    <Image className="w-4 h-4 inline mr-1.5" />Library ({library.length})
                </button>
            </div>

            {subTab === 'library' ? (
                <div className="space-y-6">
                    {trainingStatus && (
                        <div className={`p-4 rounded-xl border flex items-center gap-3 ${
                            trainingStatus.is_training
                                ? 'bg-amber-500/5 border-amber-500/20'
                                : trainingStatus.lora_available
                                    ? 'bg-emerald-500/5 border-emerald-500/20'
                                    : 'bg-white/[0.02] border-white/[0.06]'
                        }`}>
                            <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${
                                trainingStatus.is_training ? 'bg-amber-500/10' : trainingStatus.lora_available ? 'bg-emerald-500/10' : 'bg-white/[0.05]'
                            }`}>
                                {trainingStatus.is_training
                                    ? <Loader2 className="w-5 h-5 text-amber-400 animate-spin" />
                                    : trainingStatus.lora_available
                                        ? <Sparkles className="w-5 h-5 text-emerald-400" />
                                        : <Eye className="w-5 h-5 text-gray-500" />
                                }
                            </div>
                            <div className="flex-1">
                                {(() => {
                                    const remoteCount = Number(trainingStatus.total_images || 0);
                                    const localCount = Number(trainingStatus.local_library_images || 0);
                                    const pendingSync = remoteCount === 0 && localCount > 0 && trainingControlsAvailable;
                                    const canSyncNow = localCount > remoteCount && trainingControlsAvailable;
                                    return (
                                        <>
                                <p className={`text-sm font-medium ${
                                    trainingStatus.is_training ? 'text-amber-300' : trainingStatus.lora_available ? 'text-emerald-300' : 'text-gray-400'
                                }`}>
                                    {trainingStatus.is_training
                                        ? 'Catalyst is training on the owner thumbnail set...'
                                        : trainingStatus.lora_available
                                            ? `Catalyst thumbnail training ready (v${trainingStatus.version}, ${trainingStatus.trained_images} images)`
                                            : pendingSync
                                                ? `Syncing ${localCount} uploaded thumbnails into the training set...`
                                                : trainingControlsAvailable
                                                    ? `Upload ${Math.max(0, 5 - remoteCount)} more thumbnails to start training`
                                                    : 'Your private library is ready for describe and style-reference generation'
                                    }
                                </p>
                                <p className="text-gray-600 text-xs mt-0.5">
                                    {remoteCount} synced training images
                                    {localCount > remoteCount ? ` (${localCount} in local library)` : ''}
                                    {trainingStatus.lora_available && trainingStatus.total_images > trainingStatus.trained_images &&
                                        ` (${trainingStatus.total_images - trainingStatus.trained_images} new, will retrain soon)`
                                    }
                                </p>
                                {canSyncNow && (
                                    <>
                                        <button
                                            onClick={handleSyncLibrary}
                                            disabled={syncingLibrary}
                                            className="mt-2 px-3 py-1.5 rounded-lg text-xs bg-violet-600 hover:bg-violet-500 disabled:opacity-50 text-white transition-all"
                                        >
                                            {syncingLibrary ? 'Syncing...' : `Sync ${localCount - remoteCount} unsynced thumbnails now`}
                                        </button>
                                        {syncMessage && (
                                            <p className="text-[11px] text-gray-500 mt-1">{syncMessage}</p>
                                        )}
                                    </>
                                )}
                                {!trainingControlsAvailable && (
                                    <p className="text-[11px] text-gray-500 mt-2">
                                        Owner-only training sync stays internal. Public accounts can still upload references and generate thumbnails from their private library.
                                    </p>
                                )}
                                        </>
                                    );
                                })()}
                            </div>
                        </div>
                    )}

                    <label className="block border-2 border-dashed border-white/[0.08] hover:border-violet-500/30 hover:bg-violet-500/[0.02] rounded-2xl p-8 text-center cursor-pointer transition-all">
                        <UploadCloud className={`w-10 h-10 mx-auto mb-3 ${uploading ? 'text-violet-400 animate-pulse' : 'text-gray-600'}`} />
                        <p className="text-gray-300 font-medium">{uploading ? 'Uploading...' : 'Upload Reference Thumbnails'}</p>
                        <p className="text-gray-600 text-xs mt-1">PNG, JPG, WebP. Use this library for style references and future training data.</p>
                        <input type="file" className="hidden" accept="image/png,image/jpeg,image/webp" multiple
                            onChange={e => { if (e.target.files?.length) handleUpload(e.target.files); }} />
                    </label>

                    {library.length === 0 ? (
                        <div className="text-center py-12">
                            <Image className="w-12 h-12 mx-auto text-gray-700 mb-3" />
                            <p className="text-gray-500">No thumbnails yet</p>
                            <p className="text-gray-600 text-xs mt-1">Upload reference winners here so Catalyst can reuse your style across future generations.</p>
                        </div>
                    ) : (
                        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
                            {library.map(f => (
                                <div key={f.id} className="group relative rounded-xl overflow-hidden border border-white/[0.06] bg-white/[0.02] hover:border-violet-500/30 transition-all">
                                    <img src={withThumbToken(f.url)} alt={f.name}
                                        className="w-full aspect-video object-cover cursor-pointer"
                                        onClick={() => setPreviewUrl(withThumbToken(f.url))} />
                                    <div className="absolute inset-0 bg-black/0 group-hover:bg-black/40 transition-all flex items-center justify-center opacity-0 group-hover:opacity-100">
                                        <button onClick={() => setPreviewUrl(withThumbToken(f.url))}
                                            className="p-2 bg-white/10 rounded-lg mr-2 hover:bg-white/20 transition">
                                            <Eye className="w-4 h-4" />
                                        </button>
                                        <button onClick={() => handleDelete(f.id)}
                                            className="p-2 bg-red-500/20 rounded-lg hover:bg-red-500/40 transition">
                                            <Trash2 className="w-4 h-4 text-red-400" />
                                        </button>
                                    </div>
                                    <div className="p-2">
                                        <p className="text-[10px] text-gray-500 truncate">{f.name}</p>
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            ) : (
                <div className="space-y-6">
                    {trainingStatus?.lora_available && (
                        <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-emerald-500/5 border border-emerald-500/20 text-xs">
                            <Sparkles className="w-3.5 h-3.5 text-emerald-400" />
                            <span className="text-emerald-300 font-medium">Catalyst trained on {trainingStatus.trained_images} of your thumbnails</span>
                            <span className="text-gray-600">v{trainingStatus.version}</span>
                        </div>
                    )}

                    <div className="grid grid-cols-3 gap-3">
                        {modes.map(m => (
                            <button key={m.id} onClick={() => setMode(m.id)}
                                className={`p-4 rounded-xl text-left transition-all border-2 ${
                                    mode === m.id
                                        ? 'border-violet-500 bg-violet-500/10'
                                        : 'border-white/[0.06] bg-white/[0.02] hover:border-white/20'
                                }`}>
                                <div className={`mb-2 ${mode === m.id ? 'text-violet-400' : 'text-gray-500'}`}>{m.icon}</div>
                                <div className="text-sm font-bold">{m.title}</div>
                                <div className="text-[10px] text-gray-500 mt-0.5">{m.desc}</div>
                            </button>
                        ))}
                    </div>

                    {mode === 'describe' && (
                        <div>
                            <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">Describe Your Video</h2>
                            <textarea
                                value={description}
                                onChange={e => setDescription(e.target.value)}
                                disabled={loading}
                                placeholder={"Describe your video in detail. The AI will design a click-optimized thumbnail.\ne.g., \"A comparison video about why software engineers earn more than doctors, shocking statistics, aimed at 18-30 year olds\""}
                                rows={4}
                                className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-5 py-4 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all disabled:opacity-50 resize-none"
                            />
                        </div>
                    )}

                    {mode === 'style_transfer' && (
                        <div className="space-y-4">
                            <div>
                                <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">
                                    Select Style Reference
                                    {library.length === 0 && <span className="text-amber-400 ml-2">(upload thumbnails to library first)</span>}
                                </h2>
                                {library.length > 0 ? (
                                    <div className="grid grid-cols-4 md:grid-cols-6 gap-2">
                                        {library.map(f => (
                                            <button key={f.id} onClick={() => setSelectedStyleRef(f.id)}
                                                className={`rounded-lg overflow-hidden border-2 transition-all ${
                                                    selectedStyleRef === f.id ? 'border-violet-500 ring-2 ring-violet-500/30' : 'border-white/[0.06] hover:border-white/20'
                                                }`}>
                                                <img src={withThumbToken(f.url)} alt={f.name} className="w-full aspect-video object-cover" />
                                            </button>
                                        ))}
                                    </div>
                                ) : (
                                    <div className="p-4 rounded-xl bg-white/[0.02] border border-white/[0.06] text-center">
                                        <p className="text-gray-500 text-sm">Go to the Library tab and upload the thumbnail styles you want Catalyst to reference.</p>
                                    </div>
                                )}
                            </div>
                            <div>
                                <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">Describe Your New Thumbnail</h2>
                                <textarea
                                    value={styleDesc}
                                    onChange={e => setStyleDesc(e.target.value)}
                                    disabled={loading}
                                    placeholder="Describe what your new thumbnail should show, using the selected style as a reference..."
                                    rows={3}
                                    className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-5 py-4 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all disabled:opacity-50 resize-none"
                                />
                            </div>
                            <input type="hidden" value={description} />
                            {!description && styleDesc && (
                                <p className="text-amber-400 text-xs">Also fill in a brief overall description above for best results, or this field will be used.</p>
                            )}
                        </div>
                    )}

                    {mode === 'screenshot_analysis' && (
                        <div className="space-y-4">
                            <div>
                                <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">Describe Your Channel's Thumbnails</h2>
                                <textarea
                                    value={screenshotDesc}
                                    onChange={e => setScreenshotDesc(e.target.value)}
                                    disabled={loading}
                                    placeholder={"Paste a screenshot description of your YouTube channel, or describe what your thumbnails typically look like:\ne.g., \"My thumbnails use bold red/yellow text, shocked face reactions, dark backgrounds, and always show a comparison split screen. My best performing ones have numbers in them.\""}
                                    rows={4}
                                    className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-5 py-4 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all disabled:opacity-50 resize-none"
                                />
                            </div>
                            <div>
                                <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">New Video to Make Thumbnail For</h2>
                                <input
                                    type="text"
                                    value={description}
                                    onChange={e => setDescription(e.target.value)}
                                    disabled={loading}
                                    placeholder="e.g., Top 5 richest YouTubers of 2026"
                                    className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-5 py-4 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all disabled:opacity-50"
                                />
                            </div>
                        </div>
                    )}

                    <button onClick={handleGenerate}
                        disabled={loading || (!description && mode !== 'style_transfer') || (mode === 'style_transfer' && !styleDesc && !description)}
                        className="w-full py-4 bg-violet-600 hover:bg-violet-500 disabled:opacity-40 text-white font-bold rounded-xl text-lg transition-all flex items-center justify-center gap-3 shadow-lg shadow-violet-600/20 active:scale-[0.99]">
                        {loading ? (
                            <><Loader2 className="w-5 h-5 animate-spin" /> Generating Thumbnail...</>
                        ) : (
                            <><Sparkles className="w-5 h-5" /> Generate Thumbnail</>
                        )}
                    </button>

                    {jobStatus && (
                        <div className={`rounded-2xl border transition-all overflow-hidden ${
                            jobStatus.status === 'complete' ? 'border-emerald-500/30 bg-emerald-500/[0.03]' :
                            jobStatus.status === 'error' ? 'border-red-500/30 bg-red-500/[0.03]' :
                            'border-violet-500/20 bg-violet-500/[0.02]'
                        }`}>
                            {jobStatus.ai_analysis && (
                                <div className="px-6 pt-5 pb-3 border-b border-white/5">
                                    <p className="text-xs text-gray-500 uppercase tracking-wider mb-2">AI Design Strategy</p>
                                    {jobStatus.ai_analysis.style_notes && (
                                        <p className="text-gray-400 text-xs italic">{jobStatus.ai_analysis.style_notes}</p>
                                    )}
                                    {jobStatus.ai_analysis.patterns?.length > 0 && (
                                        <div className="flex flex-wrap gap-1.5 mt-2">
                                            {jobStatus.ai_analysis.patterns.map((p: string, i: number) => (
                                                <span key={i} className="px-2 py-0.5 bg-violet-500/10 text-violet-300 text-[10px] rounded-lg">{p}</span>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            )}
                            {jobStatus.status === 'error' ? (
                                <div className="p-8 text-center">
                                    <p className="text-red-400 font-bold">Generation Failed</p>
                                    <p className="text-gray-500 text-sm mt-1">{jobStatus.error}</p>
                                    <button onClick={() => { setJobStatus(null); setJobId(null); setLoading(false); }}
                                        className="mt-4 px-6 py-2 bg-white/5 hover:bg-white/10 rounded-lg text-sm transition">
                                        Try Again
                                    </button>
                                </div>
                            ) : jobStatus.status === 'complete' ? (
                                <div>
                                    <img src={withThumbToken(jobStatus.output_url)} alt="Generated Thumbnail"
                                        className="w-full cursor-pointer"
                                        onClick={() => setPreviewUrl(withThumbToken(jobStatus.output_url))} />
                                    <div className="p-6 space-y-3">
                                        <div className="flex items-center gap-2">
                                            <CheckCircle2 className="w-5 h-5 text-emerald-400" />
                                            <span className="text-emerald-400 font-bold">Thumbnail Ready</span>
                                        </div>
                                        <div className="flex gap-3">
                                            <a href={withThumbToken(jobStatus.output_url)} download
                                                onClick={() => { if (jobStatus.generation_id) void sendThumbFeedback(jobStatus.generation_id, true); }}
                                                className="flex-1 flex items-center justify-center gap-2 py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl transition-all">
                                                <Download className="w-5 h-5" /> Download PNG
                                            </a>
                                            <button onClick={() => { void handleGenerate(); }}
                                                className="flex-1 py-3 bg-white/5 hover:bg-white/10 text-gray-300 font-medium rounded-xl transition-all">
                                                Regenerate Thumbnail
                                            </button>
                                        </div>
                                    </div>
                                </div>
                            ) : (
                                <div className="p-8">
                                    <ThumbProgressBar progress={jobStatus.progress || 0} status={jobStatus.status} />
                                </div>
                            )}
                        </div>
                    )}
                </div>
            )}

            {previewUrl && (
                <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-sm flex items-center justify-center p-6"
                    onClick={() => setPreviewUrl(null)}>
                    <div className="relative max-w-4xl w-full">
                        <button onClick={() => setPreviewUrl(null)}
                            className="absolute -top-10 right-0 p-2 text-gray-400 hover:text-white transition">
                            <X className="w-6 h-6" />
                        </button>
                        <img src={previewUrl} alt="Preview" className="w-full rounded-xl" />
                    </div>
                </div>
            )}
        </div>
    );
}

