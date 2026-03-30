import { useContext, useEffect, useState } from 'react';
import { CheckCircle2, Clock, Download, FileVideo, Loader2, Lock, Plus, UploadCloud, Wand2 } from 'lucide-react';
import { AuthContext, GENERATION_API } from '../shared';
import { FeedbackWidget, JobDiagnostics, ProgressBar } from '../components/StudioWidgets';

export default function ClonePanel() {
    const { session, plan } = useContext(AuthContext);
    const [viralFile, setViralFile] = useState<File | null>(null);
    const [topic, setTopic] = useState("");
    const [viralUrl, setViralUrl] = useState("");
    const [showSource, setShowSource] = useState(false);
    const [resolution, setResolution] = useState<'720p' | '1080p'>('720p');
    const [jobId, setJobId] = useState<string | null>(null);
    const [jobStatus, setJobStatus] = useState<any>(null);
    const [loading, setLoading] = useState(false);
    const readJsonResponse = async <T = any>(res: Response): Promise<{ data: T | null; raw: string }> => {
        const raw = await res.text().catch(() => "");
        if (!raw) return { data: null, raw: "" };
        try {
            return { data: JSON.parse(raw) as T, raw };
        } catch {
            return { data: null, raw };
        }
    };

    const canClone = plan === 'creator' || plan === 'pro' || plan === 'elite';
    const canUse1080p = true;

    useEffect(() => {
        if (!jobId) return;
        const interval = setInterval(async () => {
            try {
                const res = await fetch(`${GENERATION_API}/api/status/${jobId}`);
                if (!res.ok) return;
                const { data } = await readJsonResponse<any>(res);
                if (!data || typeof data !== "object") return;
                setJobStatus(data);
                if (data.status === "complete" || data.status === "error") {
                    clearInterval(interval);
                    setLoading(false);
                }
            } catch { /* retry */ }
        }, 2000);
        return () => clearInterval(interval);
    }, [jobId]);

    const handleClone = async () => {
        if (!topic) return;
        setLoading(true);
        setJobStatus(null);
        setJobId(null);

        const fullTopic = viralUrl ? `${topic} [Source: ${viralUrl}]` : topic;
        const formData = new FormData();
        formData.append("topic", fullTopic);
        formData.append("resolution", canUse1080p ? resolution : '720p');
        if (viralFile) formData.append("file", viralFile);

        const headers: Record<string, string> = {};
        if (session) headers["Authorization"] = `Bearer ${session.access_token}`;

        try {
            const res = await fetch(`${GENERATION_API}/api/clone`, { method: "POST", headers, body: formData });
            const { data } = await readJsonResponse<any>(res);
            if (data.job_id) setJobId(data.job_id);
            else setLoading(false);
        } catch { setLoading(false); }
    };

    return (
            <div className="max-w-3xl mx-auto px-6 pb-10 space-y-8">
                <div className="text-center mb-4">
                    <h1 className="text-2xl font-bold mb-2">Clone a Viral Short</h1>
                    <p className="text-gray-500 text-sm max-w-lg mx-auto">Just tell us the new topic. AI auto-detects the best template, reverse-engineers what makes content go viral, and generates a new short for you.</p>
                </div>

                {!canClone && (
                    <div className="p-4 rounded-xl bg-amber-500/10 border border-amber-500/20 flex items-center gap-3">
                        <Lock className="w-5 h-5 text-amber-400 shrink-0" />
                        <div>
                            <p className="text-amber-300 text-sm font-medium">Clone requires Creator plan or higher</p>
                            <p className="text-gray-500 text-xs mt-0.5">Upgrade to access the viral cloning engine.</p>
                        </div>
                    </div>
                )}

                <div>
                    <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">New Topic</h2>
                    <input
                        type="text"
                        value={topic}
                        onChange={(e) => setTopic(e.target.value)}
                        disabled={loading || !canClone}
                        placeholder="e.g., Why F1 drivers earn more than NFL players"
                        className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-5 py-4 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 focus:border-violet-500/50 transition-all disabled:opacity-50 text-lg"
                        onKeyDown={(e) => e.key === 'Enter' && !loading && canClone && handleClone()}
                    />
                </div>

                <button
                    type="button"
                    onClick={() => setShowSource(!showSource)}
                    className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-300 transition"
                >
                    <Plus className={`w-4 h-4 transition-transform ${showSource ? 'rotate-45' : ''}`} />
                    {showSource ? 'Hide source reference' : 'Add source video (optional)'}
                </button>

                {showSource && (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 animate-in fade-in">
                        <label className={`block border-2 border-dashed rounded-2xl p-6 text-center transition-all ${
                            canClone ? 'border-white/[0.08] hover:border-violet-500/30 hover:bg-violet-500/[0.02] cursor-pointer' : 'border-white/[0.04] opacity-50 cursor-not-allowed'
                        }`}>
                            {viralFile ? (
                                <div className="flex flex-col items-center gap-2">
                                    <FileVideo className="w-7 h-7 text-violet-400" />
                                    <p className="text-violet-300 font-medium text-xs">{viralFile.name}</p>
                                    <p className="text-gray-600 text-[10px]">Click to change</p>
                                </div>
                            ) : (
                                <div className="flex flex-col items-center gap-2">
                                    <UploadCloud className="w-7 h-7 text-gray-600" />
                                    <p className="text-gray-400 font-medium text-xs">Upload MP4</p>
                                </div>
                            )}
                            <input type="file" className="hidden" accept="video/mp4" disabled={!canClone}
                                onChange={e => { if (e.target.files) setViralFile(e.target.files[0]); }} />
                        </label>

                        <div className={`border-2 border-dashed rounded-2xl p-6 flex flex-col justify-center ${
                            canClone ? 'border-white/[0.08]' : 'border-white/[0.04] opacity-50'
                        }`}>
                            <p className="text-gray-500 text-[10px] uppercase tracking-wider mb-2 text-center">Or paste a link</p>
                            <input
                                type="url"
                                value={viralUrl}
                                onChange={e => setViralUrl(e.target.value)}
                                disabled={!canClone || loading}
                                placeholder="https://tiktok.com/... or youtube.com/shorts/..."
                                className="w-full bg-white/[0.03] border border-white/[0.06] rounded-lg px-3 py-2 text-white placeholder:text-gray-600 text-xs focus:outline-none focus:ring-2 focus:ring-violet-500/50 disabled:opacity-50"
                            />
                        </div>
                    </div>
                )}

                <div className="flex gap-3">
                    <button onClick={() => !loading && setResolution('720p')}
                        className={`flex-1 p-3 rounded-xl text-center transition-all border-2 ${
                            resolution === '720p' ? 'border-violet-500 bg-violet-500/10' : 'border-white/[0.06] bg-white/[0.02]'
                        }`}>
                        <div className="text-sm font-bold">720p</div>
                        <div className="text-[10px] text-gray-500">Faster</div>
                    </button>
                    <button onClick={() => canUse1080p && !loading && setResolution('1080p')}
                        className={`flex-1 p-3 rounded-xl text-center transition-all border-2 ${
                            resolution === '1080p' ? 'border-violet-500 bg-violet-500/10' : 'border-white/[0.06] bg-white/[0.02]'
                        } ${!canUse1080p ? 'opacity-40 cursor-not-allowed' : ''}`}>
                        <div className="text-sm font-bold">1080p</div>
                        <div className="text-[10px] text-gray-500">{canUse1080p ? 'Max quality' : 'Temporarily unavailable'}</div>
                    </button>
                </div>

                <button onClick={handleClone} disabled={loading || !topic || !canClone}
                    className="w-full py-4 bg-violet-600 hover:bg-violet-500 disabled:opacity-40 text-white font-bold rounded-xl text-lg transition-all flex items-center justify-center gap-3 shadow-lg shadow-violet-600/20 active:scale-[0.99]">
                    {loading ? (
                        <><Loader2 className="w-5 h-5 animate-spin" /> Analyzing &amp; Generating...</>
                    ) : (
                        <><Wand2 className="w-5 h-5" /> Clone Viral Formula</>
                    )}
                </button>

                {jobStatus && (
                    <div className={`rounded-2xl border transition-all overflow-hidden ${
                        jobStatus.status === 'complete' ? 'border-emerald-500/30 bg-emerald-500/[0.03]' :
                        jobStatus.status === 'error' ? 'border-red-500/30 bg-red-500/[0.03]' :
                        'border-violet-500/20 bg-violet-500/[0.02]'
                    }`}>
                        {jobStatus.viral_analysis && (
                            <div className="px-6 pt-5 pb-3 border-b border-white/5">
                                <p className="text-xs text-gray-500 uppercase tracking-wider mb-2">Viral Analysis</p>
                                <div className="flex flex-wrap gap-2">
                                    {jobStatus.viral_analysis.hook_type && (
                                        <span className="px-2 py-1 bg-violet-500/10 text-violet-300 text-xs rounded-lg">Hook: {jobStatus.viral_analysis.hook_type}</span>
                                    )}
                                    {jobStatus.template && jobStatus.template !== 'analyzing...' && (
                                        <span className="px-2 py-1 bg-cyan-500/10 text-cyan-300 text-xs rounded-lg">Template: {jobStatus.template}</span>
                                    )}
                                    {jobStatus.viral_analysis.pacing && (
                                        <span className="px-2 py-1 bg-amber-500/10 text-amber-300 text-xs rounded-lg">Pacing: {jobStatus.viral_analysis.pacing}</span>
                                    )}
                                </div>
                                {jobStatus.viral_analysis.what_made_it_viral && (
                                    <p className="text-gray-400 text-xs mt-2 italic">{jobStatus.viral_analysis.what_made_it_viral}</p>
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
                                <video controls autoPlay className="w-full max-h-[500px] bg-black"
                                    src={`${GENERATION_API}/api/download/${jobStatus.output_file}`} />
                                <div className="p-6 space-y-4">
                                    <div className="flex items-center justify-between">
                                        <h3 className="font-bold text-lg text-emerald-400">{jobStatus.metadata?.title}</h3>
                                        <CheckCircle2 className="w-6 h-6 text-emerald-400 shrink-0" />
                                    </div>
                                    {jobStatus.metadata?.description && (
                                        <p className="text-gray-500 text-xs">{jobStatus.metadata.description}</p>
                                    )}
                                    <a href={`${GENERATION_API}/api/download/${jobStatus.output_file}`} download
                                        className="flex items-center justify-center gap-2 w-full py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl transition-all">
                                        <Download className="w-5 h-5" /> Download MP4
                                    </a>
                                    <button onClick={() => { setJobStatus(null); setJobId(null); }}
                                        className="w-full py-3 bg-white/5 hover:bg-white/10 text-gray-300 font-medium rounded-xl transition-all">
                                        Clone Another
                                    </button>
                                    <FeedbackWidget jobId={jobId || ''} feature="clone" />
                                </div>
                            </div>
                        ) : (
                            <div className="p-8 space-y-4">
                                <ProgressBar progress={jobStatus.progress || 0} status={jobStatus.status} />
                                {jobStatus.queue_position > 0 && jobStatus.status === 'queued' && (
                                    <div className="flex items-center justify-center gap-2 text-sm">
                                        <Clock className="w-4 h-4 text-violet-400" />
                                        <p className="text-gray-400">
                                            Position <span className="text-violet-400 font-bold">{jobStatus.queue_position}</span> of {jobStatus.queue_total} in queue
                                        </p>
                                    </div>
                                )}
                                {jobStatus.current_scene && jobStatus.total_scenes && (
                                    <p className="text-center text-sm text-gray-600">
                                        Rendering scene {jobStatus.current_scene} of {jobStatus.total_scenes}
                                        {jobStatus.resolution && <span className="ml-1 text-violet-400">({jobStatus.resolution})</span>}
                                    </p>
                                )}
                                {jobStatus.status === 'error' && (
                                    <p className="text-center text-sm text-red-400">{jobStatus.error || 'Generation failed'}</p>
                                )}
                                <JobDiagnostics jobStatus={jobStatus} />
                            </div>
                        )}
                    </div>
                )}
            </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════════════
   THUMBNAIL PANEL (inside Dashboard)
   ═══════════════════════════════════════════════════════════════════════════ */

