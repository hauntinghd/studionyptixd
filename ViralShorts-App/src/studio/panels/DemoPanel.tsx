import { useContext, useEffect, useState } from 'react';
import { CheckCircle2, Download, Eye, Film, Loader2, Lock, Monitor, Pause, Play, Search, Sparkles, UploadCloud, User, Volume2, Wand2, Zap } from 'lucide-react';
import { API, AuthContext, GENERATION_API } from '../shared';
import { FeedbackWidget } from '../components/StudioWidgets';

export default function DemoPanel() {
    const { session, role, demoAccess, checkoutDemo, demoComingSoon } = useContext(AuthContext);
    const isAdmin = role === 'admin';
    const [referenceFile, setReferenceFile] = useState<File | null>(null);
    const [demoFile, setDemoFile] = useState<File | null>(null);
    const [faceFile, setFaceFile] = useState<File | null>(null);
    const [autoFace, setAutoFace] = useState(true);
    const [productName, setProductName] = useState('');
    const [referenceNotes, setReferenceNotes] = useState('');
    const [pipPosition, setPipPosition] = useState('bottom-right');
    const [loading, setLoading] = useState(false);
    const [jobId, setJobId] = useState<string | null>(null);
    const [jobStatus, setJobStatus] = useState<any>(null);

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

    const [demoError, setDemoError] = useState<string | null>(null);
    const [uploadProgress, setUploadProgress] = useState<number | null>(null);
    const [compressStatus, setCompressStatus] = useState<string | null>(null);

    const [voices, setVoices] = useState<any[]>([]);
    const [voicesLoading, setVoicesLoading] = useState(false);
    const [selectedVoiceId, setSelectedVoiceId] = useState('');
    const [voiceSearch, setVoiceSearch] = useState('');
    const [previewAudio, setPreviewAudio] = useState<HTMLAudioElement | null>(null);
    const [playingVoiceId, setPlayingVoiceId] = useState<string | null>(null);
    const [previewLoading, setPreviewLoading] = useState<string | null>(null);
    const readJsonResponse = async <T = any>(res: Response): Promise<{ data: T | null; raw: string }> => {
        const raw = await res.text().catch(() => "");
        if (!raw) return { data: null, raw: "" };
        try {
            return { data: JSON.parse(raw) as T, raw };
        } catch {
            return { data: null, raw };
        }
    };

    useEffect(() => {
        let cancelled = false;
        (async () => {
            setVoicesLoading(true);
            try {
                const res = await fetch(`${API}/api/voices`, {
                    headers: session ? { Authorization: `Bearer ${session.access_token}` } : {},
                });
                if (res.ok) {
                    const { data } = await readJsonResponse<any>(res);
                    if (!cancelled) setVoices(data.voices || []);
                }
            } catch { /* silent */ }
            if (!cancelled) setVoicesLoading(false);
        })();
        return () => { cancelled = true; };
    }, [session]);

    const handlePreviewVoice = async (voiceId: string, previewUrl?: string) => {
        if (previewAudio) {
            previewAudio.pause();
            previewAudio.currentTime = 0;
            setPreviewAudio(null);
        }
        if (playingVoiceId === voiceId) {
            setPlayingVoiceId(null);
            return;
        }
        if (previewUrl) {
            const audio = new Audio(previewUrl);
            audio.onended = () => { setPlayingVoiceId(null); setPreviewAudio(null); };
            setPreviewAudio(audio);
            setPlayingVoiceId(voiceId);
            audio.play();
            return;
        }
        setPreviewLoading(voiceId);
        try {
            const res = await fetch(`${API}/api/voices/preview`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', ...(session ? { Authorization: `Bearer ${session.access_token}` } : {}) },
                body: JSON.stringify({ voice_id: voiceId }),
            });
            if (!res.ok) throw new Error('Preview failed');
            const blob = await res.blob();
            const url = URL.createObjectURL(blob);
            const audio = new Audio(url);
            audio.onended = () => { setPlayingVoiceId(null); setPreviewAudio(null); URL.revokeObjectURL(url); };
            setPreviewAudio(audio);
            setPlayingVoiceId(voiceId);
            audio.play();
        } catch { /* silent */ }
        setPreviewLoading(null);
    };

    const filteredVoices = voices.filter(v => {
        if (!voiceSearch) return true;
        const q = voiceSearch.toLowerCase();
        return v.name?.toLowerCase().includes(q) || v.gender?.toLowerCase().includes(q) || v.accent?.toLowerCase().includes(q) || v.description?.toLowerCase().includes(q);
    });

    const MAX_FILE_MB = 50;

    const compressVideoInBrowser = async (file: File, label: string): Promise<File> => {
        const sizeMB = file.size / (1024 * 1024);
        if (sizeMB <= MAX_FILE_MB) return file;

        setCompressStatus(`Compressing ${label} (${sizeMB.toFixed(0)}MB → ~${Math.round(sizeMB * 0.15)}MB)...`);

        return new Promise<File>((resolve, reject) => {
            const video = document.createElement('video');
            video.muted = true;
            video.playsInline = true;
            video.preload = 'auto';

            const url = URL.createObjectURL(file);
            video.src = url;

            video.onloadedmetadata = () => {
                const scale = Math.min(1, 720 / video.videoHeight);
                const w = Math.round(video.videoWidth * scale / 2) * 2;
                const h = Math.round(video.videoHeight * scale / 2) * 2;

                const canvas = document.createElement('canvas');
                canvas.width = w;
                canvas.height = h;
                const ctx = canvas.getContext('2d')!;

                const targetBitrate = Math.min(1500000, Math.round((MAX_FILE_MB * 8_000_000) / (video.duration || 60)));
                const stream = canvas.captureStream(24);

                let recorder: MediaRecorder;
                try {
                    recorder = new MediaRecorder(stream, {
                        mimeType: 'video/webm;codecs=vp8',
                        videoBitsPerSecond: targetBitrate
                    });
                } catch {
                    recorder = new MediaRecorder(stream, { videoBitsPerSecond: targetBitrate });
                }

                const chunks: Blob[] = [];
                recorder.ondataavailable = (e) => { if (e.data.size > 0) chunks.push(e.data); };

                recorder.onstop = () => {
                    URL.revokeObjectURL(url);
                    const blob = new Blob(chunks, { type: 'video/webm' });
                    const compressed = new File([blob], file.name.replace(/\.\w+$/, '.webm'), { type: 'video/webm' });
                    setCompressStatus(`Compressed ${label}: ${sizeMB.toFixed(0)}MB → ${(compressed.size / (1024*1024)).toFixed(0)}MB`);
                    resolve(compressed);
                };

                recorder.onerror = () => {
                    URL.revokeObjectURL(url);
                    reject(new Error(`Browser compression failed for ${label}`));
                };

                recorder.start(100);
                video.play();

                const dur = video.duration;
                const draw = () => {
                    if (video.ended || video.paused) {
                        recorder.stop();
                        return;
                    }
                    ctx.drawImage(video, 0, 0, w, h);
                    const pct = Math.round((video.currentTime / dur) * 100);
                    setCompressStatus(`Compressing ${label}: ${pct}% (${sizeMB.toFixed(0)}MB → 720p)`);
                    requestAnimationFrame(draw);
                };
                draw();

                video.onended = () => { recorder.stop(); };
            };

            video.onerror = () => {
                URL.revokeObjectURL(url);
                reject(new Error(`Could not load ${label} video for compression`));
            };
        });
    };

    const handleGenerate = async () => {
        if (!demoFile) return;
        setLoading(true);
        setJobStatus(null);
        setJobId(null);
        setDemoError(null);
        setUploadProgress(0);
        setCompressStatus(null);

        try {
            let finalDemo: File = demoFile;
            let finalRef: File | null = referenceFile;

            if (demoFile.size / (1024 * 1024) > MAX_FILE_MB) {
                finalDemo = await compressVideoInBrowser(demoFile, 'demo video');
            }
            if (referenceFile && referenceFile.size / (1024 * 1024) > MAX_FILE_MB) {
                finalRef = await compressVideoInBrowser(referenceFile, 'reference video');
            }
            setCompressStatus(null);

            const formData = new FormData();
            formData.append('demo_video', finalDemo);
            if (finalRef) formData.append('reference_video', finalRef);
            if (!autoFace && faceFile) formData.append('face_image', faceFile);
            formData.append('product_name', productName);
            formData.append('reference_notes', referenceNotes);
            formData.append('pip_position', pipPosition);
            if (selectedVoiceId) formData.append('voice_id', selectedVoiceId);

            const totalSize = (finalDemo?.size || 0) + (finalRef?.size || 0) + (faceFile?.size || 0);
            const totalMB = (totalSize / (1024 * 1024)).toFixed(0);

            const result = await new Promise<any>((resolve, reject) => {
                const xhr = new XMLHttpRequest();
                xhr.open('POST', `${GENERATION_API}/api/demo`);
                if (session) xhr.setRequestHeader('Authorization', `Bearer ${session.access_token}`);

                xhr.upload.onprogress = (e) => {
                    if (e.lengthComputable) {
                        setUploadProgress(Math.round((e.loaded / e.total) * 100));
                    }
                };

                xhr.onload = () => {
                    setUploadProgress(null);
                    if (xhr.status >= 200 && xhr.status < 300) {
                        try { resolve(JSON.parse(xhr.responseText)); }
                        catch { reject(new Error('Invalid response from server')); }
                    } else {
                        try {
                            const err = JSON.parse(xhr.responseText);
                            reject(new Error(err.detail || `Server error: ${xhr.status}`));
                        } catch { reject(new Error(`Upload failed (${xhr.status})`)); }
                    }
                };

                xhr.onerror = () => {
                    setUploadProgress(null);
                    reject(new Error('Network error. Connection lost during upload.'));
                };

                xhr.ontimeout = () => {
                    setUploadProgress(null);
                    reject(new Error(`Upload timed out (${totalMB}MB is large -- try a shorter clip)`));
                };

                xhr.timeout = 600000;
                xhr.send(formData);
            });

            if (result.job_id) setJobId(result.job_id);
            else {
                setDemoError('No job ID returned -- server may have rejected the request');
                setLoading(false);
            }
        } catch (e: any) {
            setDemoError(e?.message || 'Upload failed');
            setLoading(false);
            setUploadProgress(null);
        }
    };

    const statusLabels: Record<string, string> = {
        queued: 'Starting...',
        compressing: 'Auto-compressing large video files...',
        compressing_demo: 'Compressing demo video to 720p...',
        compressing_reference: 'Compressing reference video to 720p...',
        analyzing_reference: 'Analyzing reference video style...',
        analyzing: 'Analyzing screen recording frame-by-frame...',
        scripting: 'Writing voiceover script with AI...',
        generating_voice: 'Generating voiceover with ElevenLabs...',
        generating_sfx: 'Generating sound effects...',
        generating_face: 'Generating AI presenter face...',
        compositing: 'Compositing final demo video...',
        complete: 'Done!',
        error: 'Generation failed'
    };

    if (!demoAccess) {
        return (
            <div className="max-w-2xl mx-auto px-6 pb-10 pt-8">
                <div className="text-center space-y-6">
                    <div className="inline-flex items-center justify-center w-20 h-20 rounded-2xl bg-gradient-to-br from-violet-600/20 to-purple-600/20 border border-violet-500/20">
                        <Lock className="w-10 h-10 text-violet-400" />
                    </div>
                    <div>
                        <h2 className="text-2xl font-bold mb-2">AI Product Demo Generator</h2>
                        <p className="text-gray-400 text-sm max-w-md mx-auto">
                            Transform raw screen recordings into polished, professional product demos with AI-generated voiceovers, talking head presenters, and synced captions.
                        </p>
                    </div>
                    <div className="bg-white/[0.02] border border-white/[0.06] rounded-2xl p-6 space-y-4 text-left">
                        <div className="flex items-center gap-3">
                            <div className="w-8 h-8 rounded-lg bg-violet-500/10 flex items-center justify-center"><Wand2 className="w-4 h-4 text-violet-400" /></div>
                            <div><p className="text-sm font-medium text-gray-200">AI Script Writing</p><p className="text-xs text-gray-500">Analyzes your screen recording and writes a professional voiceover script</p></div>
                        </div>
                        <div className="flex items-center gap-3">
                            <div className="w-8 h-8 rounded-lg bg-violet-500/10 flex items-center justify-center"><Volume2 className="w-4 h-4 text-violet-400" /></div>
                            <div><p className="text-sm font-medium text-gray-200">Choose Your Voice</p><p className="text-xs text-gray-500">Pick from dozens of premium ElevenLabs voices and preview before generating</p></div>
                        </div>
                        <div className="flex items-center gap-3">
                            <div className="w-8 h-8 rounded-lg bg-violet-500/10 flex items-center justify-center"><User className="w-4 h-4 text-violet-400" /></div>
                            <div><p className="text-sm font-medium text-gray-200">AI Talking Head</p><p className="text-xs text-gray-500">Auto-generated presenter with lip-sync composited into your demo</p></div>
                        </div>
                        <div className="flex items-center gap-3">
                            <div className="w-8 h-8 rounded-lg bg-violet-500/10 flex items-center justify-center"><Film className="w-4 h-4 text-violet-400" /></div>
                            <div><p className="text-sm font-medium text-gray-200">Word-Synced Captions</p><p className="text-xs text-gray-500">Perfectly timed captions burned into the final video</p></div>
                        </div>
                    </div>
                    <div className="bg-gradient-to-r from-violet-600/10 to-purple-600/10 border border-violet-500/20 rounded-2xl p-6">
                        <p className="text-3xl font-bold text-white">$150<span className="text-base font-normal text-gray-400">/month</span></p>
                        <p className="text-sm text-gray-400 mt-1">Unlimited product demo videos</p>
                        <button
                            onClick={() => {
                                if (demoComingSoon && !isAdmin) return;
                                checkoutDemo();
                            }}
                            disabled={demoComingSoon && !isAdmin}
                            className={`mt-4 w-full py-3 rounded-xl font-semibold transition-all flex items-center justify-center gap-2 ${
                                demoComingSoon && !isAdmin
                                    ? 'bg-white/5 text-gray-500 border border-white/10 cursor-not-allowed'
                                    : 'bg-gradient-to-r from-violet-600 to-purple-600 text-white hover:shadow-lg hover:shadow-violet-600/20 hover:-translate-y-0.5'
                            }`}>
                            <Zap className="w-5 h-5" /> {demoComingSoon && !isAdmin ? 'Coming Soon' : 'Upgrade to Demo Pro'}
                        </button>
                    </div>
                </div>
            </div>
        );
    }

    return (
        <div className="max-w-4xl mx-auto px-6 pb-10 space-y-8">
            <div>
                <h2 className="text-xl font-bold mb-2">AI Product Demo Generator</h2>
                <p className="text-gray-500 text-sm">Upload a screen recording of your software + a face photo. AI writes the script, generates a talking head with lip-sync, and composites a professional product demo video.</p>
            </div>

            <div className="space-y-6">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div>
                        <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">Reference Video <span className="text-gray-600 normal-case">(style guide)</span></h3>
                        <label className={`flex flex-col items-center justify-center p-8 rounded-xl border-2 border-dashed transition-all cursor-pointer ${
                            referenceFile ? 'border-violet-500 bg-violet-500/5' : 'border-white/[0.08] hover:border-violet-500/30 bg-white/[0.02]'
                        }`}>
                            <input type="file" accept="video/*" className="hidden" onChange={(e) => setReferenceFile(e.target.files?.[0] || null)} disabled={loading} />
                            {referenceFile ? (
                                <div className="text-center">
                                    <Eye className="w-8 h-8 text-violet-400 mx-auto mb-2" />
                                    <p className="text-sm text-violet-300 font-medium truncate max-w-[180px]">{referenceFile.name}</p>
                                    <p className="text-xs text-gray-500 mt-1">{(referenceFile.size / 1024 / 1024).toFixed(1)} MB</p>
                                </div>
                            ) : (
                                <div className="text-center">
                                    <Eye className="w-8 h-8 text-gray-500 mx-auto mb-2" />
                                    <p className="text-sm text-gray-400">Upload reference video</p>
                                    <p className="text-xs text-gray-600 mt-1">The style you want to match</p>
                                </div>
                            )}
                        </label>
                    </div>

                    <div>
                        <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-3">Your Demo Video <span className="text-red-400">*</span></h3>
                        <label className={`flex flex-col items-center justify-center p-8 rounded-xl border-2 border-dashed transition-all cursor-pointer ${
                            demoFile ? 'border-violet-500 bg-violet-500/5' : 'border-white/[0.08] hover:border-violet-500/30 bg-white/[0.02]'
                        }`}>
                            <input type="file" accept="video/*" className="hidden" onChange={(e) => setDemoFile(e.target.files?.[0] || null)} disabled={loading} />
                            {demoFile ? (
                                <div className="text-center">
                                    <Film className="w-8 h-8 text-violet-400 mx-auto mb-2" />
                                    <p className="text-sm text-violet-300 font-medium truncate max-w-[180px]">{demoFile.name}</p>
                                    <p className="text-xs text-gray-500 mt-1">{(demoFile.size / 1024 / 1024).toFixed(1)} MB</p>
                                </div>
                            ) : (
                                <div className="text-center">
                                    <UploadCloud className="w-8 h-8 text-gray-500 mx-auto mb-2" />
                                    <p className="text-sm text-gray-400">Upload raw screen recording</p>
                                    <p className="text-xs text-gray-600 mt-1">The software demo to edit</p>
                                </div>
                            )}
                        </label>
                    </div>
                </div>

                <div>
                    <div className="flex items-center justify-between mb-3">
                        <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wider">AI Presenter</h3>
                        <button onClick={() => !loading && setAutoFace(!autoFace)}
                            className={`text-xs px-3 py-1 rounded-full transition-all ${
                                autoFace ? 'bg-violet-500/20 text-violet-300 border border-violet-500/30' : 'bg-white/[0.03] text-gray-500 border border-white/[0.08]'
                            }`}>
                            {autoFace ? 'Auto-Generate Face' : 'Upload Custom Face'}
                        </button>
                    </div>
                    {autoFace ? (
                        <div className="flex flex-col items-center justify-center p-8 rounded-xl border-2 border-dashed border-violet-500/30 bg-violet-500/5">
                            <Sparkles className="w-8 h-8 text-violet-400 mx-auto mb-2" />
                            <p className="text-sm text-violet-300 font-medium">AI-Generated Male Face</p>
                            <p className="text-xs text-gray-500 mt-1">A unique, realistic face will be auto-generated</p>
                        </div>
                    ) : (
                        <label className={`flex flex-col items-center justify-center p-8 rounded-xl border-2 border-dashed transition-all cursor-pointer ${
                            faceFile ? 'border-violet-500 bg-violet-500/5' : 'border-white/[0.08] hover:border-violet-500/30 bg-white/[0.02]'
                        }`}>
                            <input type="file" accept="image/*" className="hidden" onChange={(e) => setFaceFile(e.target.files?.[0] || null)} disabled={loading} />
                            {faceFile ? (
                                <div className="text-center">
                                    <User className="w-8 h-8 text-violet-400 mx-auto mb-2" />
                                    <p className="text-sm text-violet-300 font-medium">{faceFile.name}</p>
                                    <p className="text-xs text-gray-500 mt-1">{(faceFile.size / 1024 / 1024).toFixed(1)} MB</p>
                                </div>
                            ) : (
                                <div className="text-center">
                                    <User className="w-8 h-8 text-gray-500 mx-auto mb-2" />
                                    <p className="text-sm text-gray-400">Upload face photo (optional)</p>
                                    <p className="text-xs text-gray-600 mt-1">Leave empty to render without talking-head face</p>
                                </div>
                            )}
                        </label>
                    )}
                </div>
            </div>

            <div className="space-y-4">
                <div>
                    <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-2">Product Name</h3>
                    <input type="text" value={productName} onChange={(e) => setProductName(e.target.value)}
                        disabled={loading} placeholder="e.g., BrayneAI, Notion, Stripe Dashboard"
                        className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-4 py-3 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all disabled:opacity-50" />
                </div>

                <div>
                    <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-2">Style Notes (optional)</h3>
                    <textarea value={referenceNotes} onChange={(e) => setReferenceNotes(e.target.value)}
                        disabled={loading} placeholder="Describe the style you want: e.g., 'Energetic and fast-paced like a YC demo day pitch' or 'Calm and professional like an Apple keynote'"
                        rows={2}
                        className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl px-4 py-3 text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all disabled:opacity-50 resize-none" />
                </div>

                <div>
                    <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-2">Voice</h3>
                    {voicesLoading ? (
                        <div className="flex items-center gap-2 text-gray-500 text-sm py-3">
                            <Loader2 className="w-4 h-4 animate-spin" /> Loading voices from ElevenLabs...
                        </div>
                    ) : voices.length === 0 ? (
                        <p className="text-gray-600 text-sm py-2">No voices found. Using default.</p>
                    ) : (
                        <div className="space-y-3">
                            <div className="relative">
                                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" />
                                <input type="text" value={voiceSearch} onChange={(e) => setVoiceSearch(e.target.value)}
                                    placeholder="Search voices by name, gender, accent..."
                                    className="w-full bg-white/[0.03] border border-white/[0.08] rounded-xl pl-10 pr-4 py-2.5 text-sm text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all" />
                            </div>
                            <div className="max-h-48 overflow-y-auto rounded-xl border border-white/[0.06] bg-white/[0.02] divide-y divide-white/[0.04]">
                                <button onClick={() => setSelectedVoiceId('')}
                                    className={`w-full flex items-center gap-3 px-4 py-2.5 text-left transition-all ${
                                        !selectedVoiceId ? 'bg-violet-500/10 text-violet-300' : 'text-gray-400 hover:bg-white/[0.03]'
                                    }`}>
                                    <Volume2 className="w-4 h-4 flex-shrink-0" />
                                    <div className="flex-1 min-w-0">
                                        <p className="text-sm font-medium truncate">Default Voice</p>
                                        <p className="text-xs text-gray-600">Auto-selected based on template</p>
                                    </div>
                                </button>
                                {filteredVoices.map(v => (
                                    <div key={v.voice_id}
                                        className={`flex items-center gap-3 px-4 py-2.5 transition-all ${
                                            selectedVoiceId === v.voice_id ? 'bg-violet-500/10' : 'hover:bg-white/[0.03]'
                                        }`}>
                                        <button onClick={() => handlePreviewVoice(v.voice_id, v.preview_url)}
                                            className="flex-shrink-0 w-8 h-8 rounded-full bg-white/[0.05] hover:bg-violet-500/20 flex items-center justify-center transition-all"
                                            title="Preview voice">
                                            {previewLoading === v.voice_id ? (
                                                <Loader2 className="w-3.5 h-3.5 animate-spin text-violet-400" />
                                            ) : playingVoiceId === v.voice_id ? (
                                                <Pause className="w-3.5 h-3.5 text-violet-400" />
                                            ) : (
                                                <Play className="w-3.5 h-3.5 text-gray-400" />
                                            )}
                                        </button>
                                        <button onClick={() => setSelectedVoiceId(v.voice_id)}
                                            className="flex-1 min-w-0 text-left">
                                            <div className="flex items-center gap-2">
                                                <p className={`text-sm font-medium truncate ${selectedVoiceId === v.voice_id ? 'text-violet-300' : 'text-gray-300'}`}>{v.name}</p>
                                                {v.gender && <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-white/[0.05] text-gray-500 flex-shrink-0">{v.gender}</span>}
                                            </div>
                                            <p className="text-xs text-gray-600 truncate">
                                                {[v.accent, v.age, v.description].filter(Boolean).join(' · ') || v.category || 'ElevenLabs voice'}
                                            </p>
                                        </button>
                                        {selectedVoiceId === v.voice_id && (
                                            <CheckCircle2 className="w-4 h-4 text-violet-400 flex-shrink-0" />
                                        )}
                                    </div>
                                ))}
                            </div>
                            {selectedVoiceId && (
                                <p className="text-xs text-violet-400">
                                    Selected: {voices.find(v => v.voice_id === selectedVoiceId)?.name || selectedVoiceId}
                                </p>
                            )}
                        </div>
                    )}
                </div>

                <div>
                    <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wider mb-2">Face Position</h3>
                    <div className="grid grid-cols-4 gap-2">
                        {[
                            { id: 'bottom-right', label: 'Bottom Right' },
                            { id: 'bottom-left', label: 'Bottom Left' },
                            { id: 'top-right', label: 'Top Right' },
                            { id: 'top-left', label: 'Top Left' },
                        ].map(pos => (
                            <button key={pos.id} onClick={() => !loading && setPipPosition(pos.id)}
                                className={`p-2 rounded-lg text-xs font-medium transition-all border ${
                                    pipPosition === pos.id ? 'border-violet-500 bg-violet-500/10 text-violet-300' : 'border-white/[0.06] text-gray-500 hover:border-white/20'
                                } ${loading ? 'opacity-50' : ''}`}>
                                {pos.label}
                            </button>
                        ))}
                    </div>
                </div>
            </div>

            <button onClick={handleGenerate}
                disabled={loading || !demoFile}
                className={`w-full py-4 rounded-xl font-semibold text-lg transition-all flex items-center justify-center gap-3 ${
                    loading || !demoFile
                        ? 'bg-gray-700/50 text-gray-500 cursor-not-allowed'
                        : 'bg-gradient-to-r from-violet-600 to-purple-600 text-white hover:shadow-lg hover:shadow-violet-600/20 hover:-translate-y-0.5'
                }`}>
                {loading ? (
                    <><Loader2 className="w-5 h-5 animate-spin" /> Generating Demo...</>
                ) : (
                    <><Monitor className="w-5 h-5" /> Generate Product Demo</>
                )}
            </button>

            {demoError && !jobStatus && (
                <div className="bg-red-500/5 border border-red-500/20 rounded-xl px-5 py-4">
                    <p className="text-red-400 text-sm font-medium">{demoError}</p>
                </div>
            )}

            {compressStatus && !jobStatus && (
                <div className="bg-white/[0.02] border border-amber-500/20 rounded-2xl overflow-hidden">
                    <div className="px-6 pt-5 pb-4">
                        <div className="flex items-center gap-2 mb-2">
                            <Loader2 className="w-4 h-4 animate-spin text-amber-400" />
                            <p className="text-sm font-medium text-amber-300">{compressStatus}</p>
                        </div>
                        <p className="text-xs text-gray-600">Compressing in your browser to reduce upload size. This may take a moment...</p>
                    </div>
                </div>
            )}

            {uploadProgress !== null && !compressStatus && !jobStatus && (
                <div className="bg-white/[0.02] border border-white/[0.06] rounded-2xl overflow-hidden">
                    <div className="px-6 pt-5 pb-4">
                        <div className="flex items-center justify-between mb-3">
                            <p className="text-sm font-medium flex items-center gap-2">
                                <Loader2 className="w-4 h-4 animate-spin text-violet-400" />
                                Uploading files to server...
                            </p>
                            <span className="text-xs text-gray-500">{uploadProgress}%</span>
                        </div>
                        <div className="w-full bg-white/[0.05] rounded-full h-2">
                            <div className="bg-gradient-to-r from-blue-500 to-violet-500 h-2 rounded-full transition-all duration-300"
                                style={{ width: `${uploadProgress}%` }} />
                        </div>
                        <p className="text-xs text-gray-600 mt-2">
                            {uploadProgress < 100
                                ? `Uploading ${((demoFile?.size || 0) / (1024*1024)).toFixed(0)}MB${referenceFile ? ` + ${((referenceFile.size) / (1024*1024)).toFixed(0)}MB` : ''} to server...`
                                : 'Upload complete, server is processing...'}
                        </p>
                    </div>
                </div>
            )}

            {jobStatus && (
                <div className="bg-white/[0.02] border border-white/[0.06] rounded-2xl overflow-hidden">
                    <div className="px-6 pt-5 pb-4">
                        <div className="flex items-center justify-between mb-3">
                            <p className="text-sm font-medium">{statusLabels[jobStatus.status] || jobStatus.status}</p>
                            <span className="text-xs text-gray-500">{jobStatus.progress || 0}%</span>
                        </div>
                        <div className="w-full bg-white/[0.05] rounded-full h-2">
                            <div className="bg-gradient-to-r from-violet-500 to-purple-500 h-2 rounded-full transition-all duration-500"
                                style={{ width: `${jobStatus.progress || 0}%` }} />
                        </div>
                    </div>

                    {jobStatus.compress_info && (jobStatus.status === 'compressing' || jobStatus.status === 'compressing_demo' || jobStatus.status === 'compressing_reference') && (
                        <div className="px-6 py-3 border-t border-white/[0.05]">
                            <div className="flex items-center gap-2 text-xs text-amber-300">
                                <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                <span>Compressing {jobStatus.compress_info.label} video: {jobStatus.compress_info.original_size_mb}MB → 720p</span>
                            </div>
                        </div>
                    )}

                    {jobStatus.compress_info && jobStatus.compress_info.compressed_size_mb && jobStatus.status !== 'compressing' && jobStatus.status !== 'compressing_demo' && jobStatus.status !== 'compressing_reference' && (
                        <div className="px-6 py-2 border-t border-white/[0.05]">
                            <p className="text-xs text-emerald-400">Compressed: {jobStatus.compress_info.original_size_mb}MB → {jobStatus.compress_info.compressed_size_mb}MB</p>
                        </div>
                    )}

                    {jobStatus.script && (
                        <div className="px-6 py-3 border-t border-white/[0.05]">
                            <p className="text-xs text-gray-500 uppercase tracking-wider mb-1">Script Preview</p>
                            <p className="text-xs text-gray-400 line-clamp-3">
                                {jobStatus.script.segments?.slice(0, 3).map((s: any) => s.text || s.narration).join(' ')}
                            </p>
                        </div>
                    )}

                    {jobStatus.status === 'complete' && jobStatus.output_url && (
                        <div className="px-6 py-4 border-t border-white/[0.05] space-y-3">
                            <video controls className="w-full rounded-xl" src={`${GENERATION_API}${jobStatus.output_url}`} />
                            <a href={`${GENERATION_API}${jobStatus.output_url}`} download
                                className="flex items-center justify-center gap-2 w-full py-3 bg-violet-600 hover:bg-violet-500 text-white rounded-xl font-medium transition-all">
                                <Download className="w-4 h-4" /> Download Demo Video
                            </a>
                            <FeedbackWidget jobId={jobId || ''} feature="product_demo" />
                        </div>
                    )}

                    {jobStatus.status === 'error' && (
                        <div className="px-6 py-4 border-t border-red-500/20">
                            <p className="text-red-400 text-sm">{jobStatus.error || 'Generation failed. Please try again.'}</p>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}


