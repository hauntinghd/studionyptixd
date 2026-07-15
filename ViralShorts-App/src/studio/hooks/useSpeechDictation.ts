/**
 * Cross-browser voice dictation for Studio Agent.
 * - Preferred: xAI live streaming STT via authenticated backend WebSocket proxy.
 * - Fallback: MediaRecorder + server xAI/FAL batch STT (Firefox-safe, reliable Bearer auth).
 * - Last resort: browser Web Speech API when xAI path is unavailable.
 *
 * Emits live waveform levels while listening so the UI can show audio activity
 * instead of a status banner.
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import { resolveStudioBackendUrl, resolveStudioWsUrl } from '../shared';

export type DictationEngine = 'live-xai' | 'webspeech' | 'record' | 'none';

export const DICTATION_BAR_COUNT = 28;

type SpeechRecognitionResultLike = {
    isFinal: boolean;
    0?: { transcript?: string };
};

type SpeechRecognitionEventLike = {
    resultIndex: number;
    results: ArrayLike<SpeechRecognitionResultLike>;
};

type SpeechRecognitionErrorEventLike = {
    error?: string;
};

type SpeechRecognitionLike = {
    lang: string;
    continuous: boolean;
    interimResults: boolean;
    maxAlternatives: number;
    onresult: ((ev: SpeechRecognitionEventLike) => void) | null;
    onerror: ((ev: SpeechRecognitionErrorEventLike) => void) | null;
    onend: (() => void) | null;
    start: () => void;
    stop: () => void;
    abort: () => void;
};


function getSpeechRecognitionCtor(): (new () => SpeechRecognitionLike) | null {
    if (typeof window === 'undefined') return null;
    const w = window as Window & {
        SpeechRecognition?: new () => SpeechRecognitionLike;
        webkitSpeechRecognition?: new () => SpeechRecognitionLike;
    };
    return w.SpeechRecognition || w.webkitSpeechRecognition || null;
}

function pickRecorderMime(): string {
    if (typeof MediaRecorder === 'undefined') return '';
    const candidates = [
        'audio/webm;codecs=opus',
        'audio/webm',
        'audio/ogg;codecs=opus',
        'audio/ogg',
    ];
    for (const mime of candidates) {
        try {
            if (MediaRecorder.isTypeSupported(mime)) return mime;
        } catch {
            /* ignore */
        }
    }
    return '';
}

function floatTo16BitPCM(input: Float32Array): ArrayBuffer {
    const output = new Int16Array(input.length);
    for (let i = 0; i < input.length; i += 1) {
        const sample = Math.max(-1, Math.min(1, input[i]));
        output[i] = sample < 0 ? sample * 0x8000 : sample * 0x7fff;
    }
    return output.buffer;
}

function downsampleBuffer(buffer: Float32Array, inputRate: number, outputRate: number): Float32Array {
    if (outputRate === inputRate) return buffer;
    const ratio = inputRate / outputRate;
    const newLength = Math.round(buffer.length / ratio);
    const result = new Float32Array(newLength);
    let offsetResult = 0;
    let offsetBuffer = 0;
    while (offsetResult < result.length) {
        const nextOffsetBuffer = Math.round((offsetResult + 1) * ratio);
        let accum = 0;
        let count = 0;
        for (let i = offsetBuffer; i < nextOffsetBuffer && i < buffer.length; i += 1) {
            accum += buffer[i];
            count += 1;
        }
        result[offsetResult] = count ? accum / count : 0;
        offsetResult += 1;
        offsetBuffer = nextOffsetBuffer;
    }
    return result;
}

/** Split audio frame into BAR_COUNT height values (0..1) for the waveform UI. */
function frameToBars(input: Float32Array, barCount = DICTATION_BAR_COUNT): number[] {
    if (!input.length) return Array.from({ length: barCount }, () => 0.06);
    const chunk = Math.max(1, Math.floor(input.length / barCount));
    const bars: number[] = [];
    for (let i = 0; i < barCount; i += 1) {
        let sum = 0;
        const start = i * chunk;
        const end = Math.min(input.length, start + chunk);
        for (let j = start; j < end; j += 1) {
            const v = input[j];
            sum += v * v;
        }
        const n = Math.max(1, end - start);
        const rms = Math.sqrt(sum / n);
        // Boost quiet mics; soft floor so idle bars still breathe a little.
        bars.push(Math.min(1, Math.max(0.05, rms * 5.5)));
    }
    return bars;
}

function normalizeForMerge(s: string): string {
    return s
        .toLowerCase()
        .replace(/[^\p{L}\p{N}\s]/gu, '')
        .replace(/\s+/g, ' ')
        .trim();
}

/**
 * Merge a transcript segment into the accumulated text without repeating
 * content. The STT provider re-sends finalized utterances (final partial,
 * transcript.done, utterance_boundary) with different punctuation/casing;
 * naive endsWith/includes checks miss those and double the dictation.
 */
function mergeTranscript(prev: string, next: string): string {
    const p = prev.trim();
    const n = next.trim();
    if (!p) return n;
    if (!n) return p;
    const np = normalizeForMerge(p);
    const nn = normalizeForMerge(n);
    if (!nn || np.includes(nn)) return p;
    if (nn.includes(np)) return n;
    // Longest word-suffix of prev that is a word-prefix of next — drop the
    // overlap so "…going to the" + "to the store" joins as "…going to the store".
    const prevWords = np.split(' ');
    const nextWords = nn.split(' ');
    const maxOverlap = Math.min(prevWords.length, nextWords.length);
    for (let k = maxOverlap; k > 0; k -= 1) {
        if (prevWords.slice(prevWords.length - k).join(' ') === nextWords.slice(0, k).join(' ')) {
            const surfaceWords = n.split(/\s+/);
            return `${p} ${surfaceWords.slice(k).join(' ')}`.trim();
        }
    }
    return `${p} ${n}`.trim();
}

function isAuthOrPlanError(message: string): boolean {
    const low = String(message || '').toLowerCase();
    return (
        low.includes('authentication')
        || low.includes('sign in')
        || low.includes('auth_required')
        || low.includes('plan_required')
        || low.includes('studio pro')
        || low.includes('requires an active studio')
        || low.includes('not signed in')
    );
}

function isStatusOnlyInterim(text: string): boolean {
    const t = String(text || '').trim().toLowerCase();
    if (!t) return true;
    return (
        t.startsWith('listening')
        || t.startsWith('connecting')
        || t.startsWith('transcribing')
        || t.startsWith('finalizing')
        || t.includes('tap mic')
        || t.includes('still listening')
        || t.includes('record mode')
        || t.includes('speak freely')
    );
}

export function useSpeechDictation({
    getAccessToken,
    onFinalText,
    onInterimText,
    lang = typeof navigator !== 'undefined' ? navigator.language || 'en-US' : 'en-US',
}: {
    getAccessToken: () => Promise<string>;
    onFinalText: (text: string) => void;
    onInterimText?: (text: string) => void;
    lang?: string;
}) {
    const [engine, setEngine] = useState<DictationEngine>('none');
    const [listening, setListening] = useState(false);
    const [transcribing, setTranscribing] = useState(false);
    const [error, setError] = useState('');
    const [levels, setLevels] = useState<number[]>(() => Array.from({ length: DICTATION_BAR_COUNT }, () => 0.08));
    const recognitionRef = useRef<SpeechRecognitionLike | null>(null);
    const mediaStreamRef = useRef<MediaStream | null>(null);
    const mediaRecorderRef = useRef<MediaRecorder | null>(null);
    const audioContextRef = useRef<AudioContext | null>(null);
    const processorRef = useRef<ScriptProcessorNode | null>(null);
    const sourceRef = useRef<MediaStreamAudioSourceNode | null>(null);
    const meterProcessorRef = useRef<ScriptProcessorNode | null>(null);
    const wsRef = useRef<WebSocket | null>(null);
    const chunksRef = useRef<Blob[]>([]);
    const listeningRef = useRef(false);
    const webFinalRef = useRef('');
    const liveFinalRef = useRef('');
    const lastHeardRef = useRef('');
    const finalizeTimerRef = useRef<number | null>(null);
    const deliveredRef = useRef(false);
    const liveFailedRef = useRef(false);
    const liveReadyRef = useRef(false);
    const preferRecordRef = useRef(false);
    const levelsRafRef = useRef<number | null>(null);
    const pendingLevelsRef = useRef<number[] | null>(null);

    const clearFinalizeTimer = useCallback(() => {
        if (finalizeTimerRef.current != null) {
            window.clearTimeout(finalizeTimerRef.current);
            finalizeTimerRef.current = null;
        }
    }, []);

    const finishTranscribing = useCallback(() => {
        clearFinalizeTimer();
        setTranscribing(false);
    }, [clearFinalizeTimer]);

    const pushLevels = useCallback((next: number[]) => {
        pendingLevelsRef.current = next;
        if (levelsRafRef.current != null) return;
        levelsRafRef.current = window.requestAnimationFrame(() => {
            levelsRafRef.current = null;
            if (pendingLevelsRef.current) {
                setLevels(pendingLevelsRef.current);
                pendingLevelsRef.current = null;
            }
        });
    }, []);

    const resetLevels = useCallback(() => {
        setLevels(Array.from({ length: DICTATION_BAR_COUNT }, () => 0.08));
    }, []);

    const publishInterim = useCallback(
        (text: string) => {
            // Never surface instructional status strings — only real transcript.
            if (isStatusOnlyInterim(text)) {
                onInterimText?.('');
                return;
            }
            // Strip trailing status suffixes if the server/path mixed them in.
            const cleaned = String(text || '')
                .replace(/\s*·\s*still listening[^.]*$/i, '')
                .replace(/\s*—\s*tap mic[^.]*$/i, '')
                .trim();
            onInterimText?.(cleaned);
        },
        [onInterimText],
    );

    const deliverFinalText = useCallback(
        (text: string) => {
            const chunk = String(text || '').trim();
            if (!chunk || deliveredRef.current) return;
            deliveredRef.current = true;
            onFinalText(chunk);
            onInterimText?.('');
            finishTranscribing();
        },
        [finishTranscribing, onFinalText, onInterimText],
    );

    useEffect(() => {
        const hasMic =
            typeof navigator !== 'undefined'
            && typeof navigator.mediaDevices?.getUserMedia === 'function';
        const hasWs = typeof WebSocket !== 'undefined';
        if (hasMic && hasWs && !preferRecordRef.current) {
            setEngine('live-xai');
            return;
        }
        if (hasMic) {
            setEngine('record');
            return;
        }
        if (getSpeechRecognitionCtor()) {
            setEngine('webspeech');
            return;
        }
        setEngine('none');
    }, []);

    const stopMeterOnly = useCallback(() => {
        try {
            meterProcessorRef.current?.disconnect();
        } catch {
            /* ignore */
        }
        meterProcessorRef.current = null;
    }, []);

    const cleanupMedia = useCallback(() => {
        stopMeterOnly();
        try {
            processorRef.current?.disconnect();
        } catch {
            /* ignore */
        }
        processorRef.current = null;
        try {
            sourceRef.current?.disconnect();
        } catch {
            /* ignore */
        }
        sourceRef.current = null;
        try {
            if (audioContextRef.current && audioContextRef.current.state !== 'closed') {
                void audioContextRef.current.close();
            }
        } catch {
            /* ignore */
        }
        audioContextRef.current = null;
        if (mediaRecorderRef.current) {
            try {
                if (mediaRecorderRef.current.state === 'recording') mediaRecorderRef.current.stop();
            } catch {
                /* ignore */
            }
            mediaRecorderRef.current = null;
        }
        if (mediaStreamRef.current) {
            for (const track of mediaStreamRef.current.getTracks()) {
                try {
                    track.stop();
                } catch {
                    /* ignore */
                }
            }
            mediaStreamRef.current = null;
        }
        if (wsRef.current) {
            try {
                wsRef.current.onopen = null;
                wsRef.current.onmessage = null;
                wsRef.current.onerror = null;
                wsRef.current.onclose = null;
                if (
                    wsRef.current.readyState === WebSocket.OPEN
                    || wsRef.current.readyState === WebSocket.CONNECTING
                ) {
                    wsRef.current.close();
                }
            } catch {
                /* ignore */
            }
            wsRef.current = null;
        }
        resetLevels();
    }, [resetLevels, stopMeterOnly]);

    const attachLevelMeter = useCallback(
        async (stream: MediaStream) => {
            const AudioCtx =
                window.AudioContext
                || (window as unknown as { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
            if (!AudioCtx) return null;
            let audioContext = audioContextRef.current;
            if (!audioContext || audioContext.state === 'closed') {
                audioContext = new AudioCtx();
                audioContextRef.current = audioContext;
            }
            if (audioContext.state === 'suspended') {
                try {
                    await audioContext.resume();
                } catch {
                    /* ignore */
                }
            }
            const source = audioContext.createMediaStreamSource(stream);
            sourceRef.current = source;
            const meter = audioContext.createScriptProcessor(2048, 1, 1);
            meterProcessorRef.current = meter;
            meter.onaudioprocess = (ev) => {
                if (!listeningRef.current) return;
                const input = ev.inputBuffer.getChannelData(0);
                pushLevels(frameToBars(input));
            };
            source.connect(meter);
            meter.connect(audioContext.destination);
            return { audioContext, source };
        },
        [pushLevels],
    );

    const stopWebSpeech = useCallback(() => {
        const rec = recognitionRef.current;
        recognitionRef.current = null;
        if (rec) {
            try {
                rec.stop();
            } catch {
                try {
                    rec.abort();
                } catch {
                    /* ignore */
                }
            }
        }
    }, []);

    const uploadAndTranscribe = useCallback(
        async (blob: Blob) => {
            setTranscribing(true);
            setError('');
            onInterimText?.('');
            try {
                const tok = await getAccessToken();
                const ext = blob.type.includes('ogg') ? 'ogg' : 'webm';
                const form = new FormData();
                form.append('audio', blob, `dictation.${ext}`);
                const res = await fetch(resolveStudioBackendUrl('/api/studio-agent/dictation'), {
                    method: 'POST',
                    headers: { Authorization: `Bearer ${tok}` },
                    body: form,
                });
                const data = (await res.json().catch(() => ({}))) as Record<string, unknown>;
                if (!res.ok) {
                    const detail = String(data.detail || data.error || `Transcription failed (${res.status})`);
                    if (res.status === 401) {
                        throw new Error('Sign in required. Refresh Studio, then try the mic again.');
                    }
                    if (res.status === 403) {
                        throw new Error(
                            detail || 'Studio Agent requires an active Studio or Studio Pro plan.',
                        );
                    }
                    throw new Error(detail);
                }
                const text = String(data.text || '').trim();
                if (!text) throw new Error('No speech detected.');
                onFinalText(text);
                onInterimText?.('');
            } catch (e) {
                setError((e as Error).message);
                onInterimText?.('');
            } finally {
                finishTranscribing();
            }
        },
        [finishTranscribing, getAccessToken, onFinalText, onInterimText],
    );

    const startRecord = useCallback(async () => {
        setError('');
        deliveredRef.current = false;
        onInterimText?.('');
        const mime = pickRecorderMime();
        if (!mime) {
            setError('This browser cannot record audio for dictation.');
            finishTranscribing();
            return;
        }
        try {
            const stream = await navigator.mediaDevices.getUserMedia({
                audio: {
                    echoCancellation: true,
                    noiseSuppression: true,
                    autoGainControl: true,
                },
            });
            mediaStreamRef.current = stream;
            chunksRef.current = [];
            await attachLevelMeter(stream);
            const recorder = new MediaRecorder(stream, { mimeType: mime });
            mediaRecorderRef.current = recorder;
            recorder.ondataavailable = (ev) => {
                if (ev.data?.size) chunksRef.current.push(ev.data);
            };
            recorder.onstop = () => {
                const blob = new Blob(chunksRef.current, { type: mime });
                cleanupMedia();
                if (blob.size < 800) {
                    setError('Recording too short — hold the mic and speak again.');
                    finishTranscribing();
                    return;
                }
                void uploadAndTranscribe(blob);
            };
            recorder.start(250);
            listeningRef.current = true;
            setListening(true);
            setEngine('record');
        } catch (e) {
            cleanupMedia();
            const msg = (e as Error).message || 'Microphone access denied';
            if (/denied|notallowed|permission/i.test(msg)) {
                setError(
                    'Microphone blocked — allow mic access for this site in browser settings, then reload.',
                );
            } else {
                setError(msg);
            }
            listeningRef.current = false;
            setListening(false);
            finishTranscribing();
            onInterimText?.('');
        }
    }, [attachLevelMeter, cleanupMedia, finishTranscribing, onInterimText, uploadAndTranscribe]);

    const fallbackToRecord = useCallback(
        (reason: string) => {
            liveFailedRef.current = true;
            preferRecordRef.current = true;
            cleanupMedia();
            listeningRef.current = false;
            setListening(false);
            finishTranscribing();
            setEngine('record');
            const note = reason
                ? `Live voice unavailable (${reason}). Switched to record mode — tap mic, speak, tap again.`
                : 'Live voice unavailable. Switched to record mode — tap mic, speak, tap again.';
            setError(note);
            onInterimText?.('');
            void startRecord();
        },
        [cleanupMedia, finishTranscribing, onInterimText, startRecord],
    );

    const startLiveXai = useCallback(async () => {
        setError('');
        deliveredRef.current = false;
        liveFailedRef.current = false;
        liveReadyRef.current = false;
        liveFinalRef.current = '';
        lastHeardRef.current = '';
        finishTranscribing();
        onInterimText?.('');
        try {
            const tok = await getAccessToken();
            const ws = new WebSocket(resolveStudioWsUrl('/api/studio-agent/dictation/stream', {
                ...(tok ? { token: tok } : {}),
                language: lang.split('-')[0] || 'en',
            }));
            wsRef.current = ws;
            ws.binaryType = 'arraybuffer';

            ws.onmessage = (event) => {
                try {
                    const payload = JSON.parse(String(event.data || '{}')) as Record<string, unknown>;
                    const type = String(payload.type || '');
                    if (type === 'error') {
                        const msg = String(payload.message || 'Live dictation failed');
                        const code = String(payload.code || '');
                        if (/asr stream timed out/i.test(msg)) {
                            fallbackToRecord('ASR timeout');
                            return;
                        }
                        if (!liveReadyRef.current) {
                            fallbackToRecord(
                                code === 'plan_required'
                                    ? 'plan required'
                                    : isAuthOrPlanError(msg)
                                      ? 'sign-in'
                                      : msg.slice(0, 80),
                            );
                            return;
                        }
                        setError(msg);
                        listeningRef.current = false;
                        setListening(false);
                        finishTranscribing();
                        cleanupMedia();
                        onInterimText?.('');
                        return;
                    }
                    if (type === 'reconnected') {
                        setError('');
                        onInterimText?.('');
                        return;
                    }
                    if (type === 'status') {
                        return;
                    }
                    if (type === 'ready') {
                        liveReadyRef.current = true;
                        // No status banner — waveform + mic state are enough.
                        onInterimText?.('');
                        return;
                    }
                    if (type === 'utterance_boundary') {
                        const seg = String(payload.text || '').trim();
                        if (seg) {
                            liveFinalRef.current = mergeTranscript(liveFinalRef.current, seg);
                            lastHeardRef.current = liveFinalRef.current;
                            publishInterim(liveFinalRef.current);
                        }
                        return;
                    }
                    if (type === 'transcript.partial' || type === 'transcript.done') {
                        const text = String(payload.text || '').trim();
                        const isFinal = Boolean(payload.is_final);
                        const speechFinal = Boolean(payload.speech_final);
                        const utteranceEnd = speechFinal || type === 'transcript.done';

                        if (text) {
                            if (utteranceEnd || isFinal) {
                                liveFinalRef.current = mergeTranscript(liveFinalRef.current, text);
                                lastHeardRef.current = liveFinalRef.current;
                                publishInterim(liveFinalRef.current);
                            } else {
                                // In-progress utterance: preview only, never committed.
                                const combined = mergeTranscript(liveFinalRef.current, text);
                                lastHeardRef.current = combined;
                                publishInterim(combined);
                            }
                        }
                    }
                } catch {
                    /* ignore malformed frames */
                }
            };

            await new Promise<void>((resolve, reject) => {
                const timer = window.setTimeout(() => reject(new Error('Live voice connection timed out')), 12000);
                ws.onopen = () => {
                    window.clearTimeout(timer);
                    try {
                        ws.send(JSON.stringify({ type: 'auth', token: tok }));
                    } catch {
                        /* ignore */
                    }
                    resolve();
                };
                ws.onerror = () => {
                    window.clearTimeout(timer);
                    reject(new Error('Could not open live voice socket'));
                };
            });

            await new Promise<void>((resolve) => {
                const started = Date.now();
                const tick = window.setInterval(() => {
                    if (liveReadyRef.current || liveFailedRef.current || Date.now() - started > 6000) {
                        window.clearInterval(tick);
                        resolve();
                    }
                }, 50);
            });
            if (liveFailedRef.current) {
                return;
            }
            if (!liveReadyRef.current) {
                fallbackToRecord('no ready signal');
                return;
            }

            const stream = await navigator.mediaDevices.getUserMedia({
                audio: {
                    echoCancellation: true,
                    noiseSuppression: true,
                    autoGainControl: true,
                },
            });
            mediaStreamRef.current = stream;
            const AudioCtx =
                window.AudioContext
                || (window as unknown as { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
            if (!AudioCtx) {
                fallbackToRecord('audio context unavailable');
                return;
            }
            const audioContext = new AudioCtx();
            audioContextRef.current = audioContext;
            if (audioContext.state === 'suspended') {
                try {
                    await audioContext.resume();
                } catch {
                    /* ignore */
                }
            }
            const source = audioContext.createMediaStreamSource(stream);
            sourceRef.current = source;
            const processor = audioContext.createScriptProcessor(4096, 1, 1);
            processorRef.current = processor;
            const targetRate = 16000;
            processor.onaudioprocess = (ev) => {
                const input = ev.inputBuffer.getChannelData(0);
                if (listeningRef.current) {
                    pushLevels(frameToBars(input));
                }
                const socket = wsRef.current;
                if (!socket || socket.readyState !== WebSocket.OPEN) return;
                const downsampled = downsampleBuffer(input, audioContext.sampleRate, targetRate);
                try {
                    socket.send(floatTo16BitPCM(downsampled));
                } catch {
                    /* ignore send while closing */
                }
            };
            source.connect(processor);
            processor.connect(audioContext.destination);
            listeningRef.current = true;
            setListening(true);
            setEngine('live-xai');
        } catch (e) {
            const msg = (e as Error).message || 'Could not start live voice dictation';
            if (isAuthOrPlanError(msg) || /socket|timeout|connect/i.test(msg)) {
                fallbackToRecord(msg.slice(0, 80));
                return;
            }
            cleanupMedia();
            setError(msg);
            listeningRef.current = false;
            setListening(false);
            finishTranscribing();
            onInterimText?.('');
        }
    }, [
        cleanupMedia,
        fallbackToRecord,
        finishTranscribing,
        getAccessToken,
        lang,
        onInterimText,
        publishInterim,
        pushLevels,
    ]);

    const stopLiveXai = useCallback(() => {
        const socket = wsRef.current;
        if (socket && socket.readyState === WebSocket.OPEN) {
            try {
                socket.send(JSON.stringify({ type: 'audio.done' }));
            } catch {
                /* ignore */
            }
        }
        listeningRef.current = false;
        setListening(false);
        resetLevels();
        const pendingText = (liveFinalRef.current || lastHeardRef.current).trim();
        if (pendingText) {
            deliverFinalText(pendingText);
            liveFinalRef.current = '';
            lastHeardRef.current = '';
            clearFinalizeTimer();
            finalizeTimerRef.current = window.setTimeout(() => {
                cleanupMedia();
                finishTranscribing();
            }, 400);
            return;
        }
        if (liveReadyRef.current) {
            setTranscribing(true);
            onInterimText?.('');
            clearFinalizeTimer();
            finalizeTimerRef.current = window.setTimeout(() => {
                deliverFinalText(liveFinalRef.current || lastHeardRef.current);
                onInterimText?.('');
                finishTranscribing();
                cleanupMedia();
            }, 1500);
            return;
        }
        finishTranscribing();
        cleanupMedia();
        onInterimText?.('');
    }, [cleanupMedia, clearFinalizeTimer, deliverFinalText, finishTranscribing, onInterimText, resetLevels]);

    const startWebSpeech = useCallback(() => {
        const Ctor = getSpeechRecognitionCtor();
        if (!Ctor) return;
        setError('');
        finishTranscribing();
        const rec = new Ctor();
        recognitionRef.current = rec;
        rec.lang = lang;
        rec.continuous = true;
        rec.interimResults = true;
        rec.maxAlternatives = 1;

        webFinalRef.current = '';
        deliveredRef.current = false;

        rec.onresult = (event: SpeechRecognitionEventLike) => {
            let interim = '';
            for (let i = event.resultIndex; i < event.results.length; i += 1) {
                const piece = event.results[i];
                const transcript = String(piece[0]?.transcript || '');
                if (piece.isFinal) {
                    webFinalRef.current += transcript;
                } else {
                    interim += transcript;
                }
            }
            const combined = `${webFinalRef.current}${interim}`.trim();
            if (combined) publishInterim(combined);
        };

        rec.onerror = (event: SpeechRecognitionErrorEventLike) => {
            const code = String(event.error || '');
            if (code === 'aborted' || code === 'no-speech') return;
            if (code === 'not-allowed') {
                setError('Microphone or speech recognition blocked — check browser permissions.');
            } else if (code === 'network') {
                setError('Speech recognition network error — try again or use record mode.');
            } else {
                setError(`Dictation error: ${code || 'unknown'}`);
            }
            listeningRef.current = false;
            setListening(false);
            resetLevels();
            finishTranscribing();
        };

        rec.onend = () => {
            const text = webFinalRef.current.trim();
            webFinalRef.current = '';
            if (text) onFinalText(text);
            onInterimText?.('');
            listeningRef.current = false;
            setListening(false);
            resetLevels();
            recognitionRef.current = null;
            finishTranscribing();
        };

        try {
            rec.start();
            listeningRef.current = true;
            setListening(true);
            setEngine('webspeech');
            // Soft idle pulse while Web Speech has no raw PCM meter.
            const idle = window.setInterval(() => {
                if (!listeningRef.current || engine !== 'webspeech') {
                    window.clearInterval(idle);
                    return;
                }
                pushLevels(
                    Array.from({ length: DICTATION_BAR_COUNT }, (_, i) => {
                        const t = Date.now() / 220 + i * 0.35;
                        return 0.12 + (Math.sin(t) * 0.5 + 0.5) * 0.28;
                    }),
                );
            }, 80);
            onInterimText?.('');
        } catch (e) {
            setError((e as Error).message || 'Could not start speech recognition');
            recognitionRef.current = null;
            finishTranscribing();
        }
    }, [engine, finishTranscribing, lang, onFinalText, onInterimText, publishInterim, pushLevels, resetLevels]);

    const start = useCallback(() => {
        if (preferRecordRef.current) {
            void startRecord();
            return;
        }
        if (engine === 'live-xai') {
            void startLiveXai();
            return;
        }
        if (engine === 'webspeech') {
            void startWebSpeech();
            return;
        }
        if (engine === 'record') {
            void startRecord();
            return;
        }
        setError('Voice dictation is not supported in this browser.');
    }, [engine, startLiveXai, startRecord, startWebSpeech]);

    const stop = useCallback(() => {
        listeningRef.current = false;
        if (engine === 'live-xai' && !preferRecordRef.current) {
            stopLiveXai();
            return;
        }
        if (engine === 'webspeech') {
            stopWebSpeech();
            setListening(false);
            resetLevels();
            return;
        }
        if ((engine === 'record' || preferRecordRef.current) && mediaRecorderRef.current?.state === 'recording') {
            setTranscribing(true);
            setListening(false);
            resetLevels();
            onInterimText?.('');
            try {
                mediaRecorderRef.current.stop();
            } catch {
                cleanupMedia();
                finishTranscribing();
            }
            return;
        }
        cleanupMedia();
        setListening(false);
        finishTranscribing();
        onInterimText?.('');
    }, [cleanupMedia, engine, finishTranscribing, onInterimText, resetLevels, stopLiveXai, stopWebSpeech]);

    const toggle = useCallback(() => {
        if (listening) {
            stop();
        } else if (!transcribing) {
            start();
        }
    }, [listening, start, stop, transcribing]);

    useEffect(() => () => {
        listeningRef.current = false;
        clearFinalizeTimer();
        stopWebSpeech();
        cleanupMedia();
        setTranscribing(false);
        if (levelsRafRef.current != null) {
            window.cancelAnimationFrame(levelsRafRef.current);
        }
    }, [cleanupMedia, clearFinalizeTimer, stopWebSpeech]);

    return {
        engine,
        listening,
        transcribing,
        error,
        setError,
        levels,
        start,
        stop,
        toggle,
        supported: engine !== 'none',
    };
}
