/**
 * Cross-browser voice dictation for Studio Agent.
 *
 * Audio is recorded locally and sent to the authenticated batch endpoint,
 * which is the only supported FAL transcription lane. Studio intentionally
 * does not open a live provider WebSocket or use browser speech services.
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import { resolveStudioBackendUrl } from '../shared';

export type DictationEngine = 'record' | 'none';

export const DICTATION_BAR_COUNT = 28;

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
            /* ignore unsupported recorder probes */
        }
    }
    return '';
}

function frameToBars(input: Float32Array, barCount = DICTATION_BAR_COUNT): number[] {
    if (!input.length) return Array.from({ length: barCount }, () => 0.06);
    const chunk = Math.max(1, Math.floor(input.length / barCount));
    const bars: number[] = [];
    for (let i = 0; i < barCount; i += 1) {
        let sum = 0;
        const start = i * chunk;
        const end = Math.min(input.length, start + chunk);
        for (let j = start; j < end; j += 1) {
            const value = input[j];
            sum += value * value;
        }
        const rms = Math.sqrt(sum / Math.max(1, end - start));
        bars.push(Math.min(1, Math.max(0.05, rms * 5.5)));
    }
    return bars;
}

export function useSpeechDictation({
    getAccessToken,
    onFinalText,
    onInterimText,
}: {
    getAccessToken: () => Promise<string>;
    onFinalText: (text: string) => void;
    onInterimText?: (text: string) => void;
    /** Kept for caller compatibility; the FAL batch endpoint detects language. */
    lang?: string;
}) {
    const [engine, setEngine] = useState<DictationEngine>('none');
    const [listening, setListening] = useState(false);
    const [transcribing, setTranscribing] = useState(false);
    const [error, setError] = useState('');
    const [levels, setLevels] = useState<number[]>(
        () => Array.from({ length: DICTATION_BAR_COUNT }, () => 0.08),
    );
    const mediaStreamRef = useRef<MediaStream | null>(null);
    const mediaRecorderRef = useRef<MediaRecorder | null>(null);
    const audioContextRef = useRef<AudioContext | null>(null);
    const sourceRef = useRef<MediaStreamAudioSourceNode | null>(null);
    const meterRef = useRef<ScriptProcessorNode | null>(null);
    const chunksRef = useRef<Blob[]>([]);
    const listeningRef = useRef(false);
    const levelsRafRef = useRef<number | null>(null);
    const pendingLevelsRef = useRef<number[] | null>(null);
    const mountedRef = useRef(true);

    const pushLevels = useCallback((next: number[]) => {
        pendingLevelsRef.current = next;
        if (levelsRafRef.current != null) return;
        levelsRafRef.current = window.requestAnimationFrame(() => {
            levelsRafRef.current = null;
            if (pendingLevelsRef.current && mountedRef.current) {
                setLevels(pendingLevelsRef.current);
                pendingLevelsRef.current = null;
            }
        });
    }, []);

    const resetLevels = useCallback(() => {
        if (mountedRef.current) {
            setLevels(Array.from({ length: DICTATION_BAR_COUNT }, () => 0.08));
        }
    }, []);

    const releaseAudio = useCallback(() => {
        try {
            meterRef.current?.disconnect();
        } catch {
            /* ignore */
        }
        meterRef.current = null;
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
        if (mediaStreamRef.current) {
            for (const track of mediaStreamRef.current.getTracks()) {
                try {
                    track.stop();
                } catch {
                    /* ignore */
                }
            }
        }
        mediaStreamRef.current = null;
        resetLevels();
    }, [resetLevels]);

    const attachLevelMeter = useCallback(async (stream: MediaStream) => {
        const AudioCtx = window.AudioContext
            || (window as unknown as { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
        if (!AudioCtx) return;
        const audioContext = new AudioCtx();
        audioContextRef.current = audioContext;
        if (audioContext.state === 'suspended') {
            try {
                await audioContext.resume();
            } catch {
                /* waveform is optional */
            }
        }
        const source = audioContext.createMediaStreamSource(stream);
        const meter = audioContext.createScriptProcessor(2048, 1, 1);
        sourceRef.current = source;
        meterRef.current = meter;
        meter.onaudioprocess = (event) => {
            if (!listeningRef.current) return;
            pushLevels(frameToBars(event.inputBuffer.getChannelData(0)));
        };
        source.connect(meter);
        meter.connect(audioContext.destination);
    }, [pushLevels]);

    const uploadAndTranscribe = useCallback(async (blob: Blob) => {
        if (!mountedRef.current) return;
        setTranscribing(true);
        setError('');
        onInterimText?.('');
        try {
            const token = await getAccessToken();
            const ext = blob.type.includes('ogg') ? 'ogg' : 'webm';
            const form = new FormData();
            form.append('audio', blob, `dictation.${ext}`);
            const res = await fetch(resolveStudioBackendUrl('/api/studio-agent/dictation'), {
                method: 'POST',
                headers: { Authorization: `Bearer ${token}` },
                body: form,
            });
            const data = (await res.json().catch(() => ({}))) as Record<string, unknown>;
            if (!res.ok) {
                const detail = String(data.detail || data.error || `Transcription failed (${res.status})`);
                if (res.status === 401) {
                    throw new Error('Sign in required. Refresh Studio, then try the mic again.');
                }
                if (res.status === 403) {
                    throw new Error(detail || 'Studio Agent requires an active Studio plan.');
                }
                throw new Error(detail);
            }
            const provider = String(data.provider || '').trim().toLowerCase();
            if (provider && provider !== 'fal') {
                throw new Error('Studio rejected a non-FAL dictation response. Please try again.');
            }
            const text = String(data.text || '').trim();
            if (!text) throw new Error('No speech detected.');
            if (mountedRef.current) onFinalText(text);
            onInterimText?.('');
        } catch (caught) {
            if (mountedRef.current) setError((caught as Error).message);
            onInterimText?.('');
        } finally {
            if (mountedRef.current) setTranscribing(false);
        }
    }, [getAccessToken, onFinalText, onInterimText]);

    const start = useCallback(async () => {
        if (engine !== 'record' || listeningRef.current || transcribing) return;
        setError('');
        onInterimText?.('');
        const mime = pickRecorderMime();
        if (!mime) {
            setError('This browser cannot record audio for dictation.');
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
            if (!mountedRef.current) {
                for (const track of stream.getTracks()) track.stop();
                return;
            }
            mediaStreamRef.current = stream;
            chunksRef.current = [];
            await attachLevelMeter(stream);
            const recorder = new MediaRecorder(stream, { mimeType: mime });
            mediaRecorderRef.current = recorder;
            recorder.ondataavailable = (event) => {
                if (event.data?.size) chunksRef.current.push(event.data);
            };
            recorder.onerror = () => {
                mediaRecorderRef.current = null;
                listeningRef.current = false;
                releaseAudio();
                if (mountedRef.current) {
                    setListening(false);
                    setTranscribing(false);
                    setError('Audio recording failed. Please try again.');
                }
            };
            recorder.onstop = () => {
                mediaRecorderRef.current = null;
                listeningRef.current = false;
                const blob = new Blob(chunksRef.current, { type: mime });
                chunksRef.current = [];
                releaseAudio();
                if (!mountedRef.current) return;
                setListening(false);
                if (blob.size < 800) {
                    setTranscribing(false);
                    setError('Recording too short - hold the mic and speak again.');
                    return;
                }
                void uploadAndTranscribe(blob);
            };
            recorder.start(250);
            listeningRef.current = true;
            setListening(true);
        } catch (caught) {
            releaseAudio();
            listeningRef.current = false;
            setListening(false);
            const message = (caught as Error).message || 'Microphone access denied';
            setError(
                /denied|notallowed|permission/i.test(message)
                    ? 'Microphone blocked - allow mic access for this site in browser settings, then reload.'
                    : message,
            );
        }
    }, [attachLevelMeter, engine, onInterimText, releaseAudio, transcribing, uploadAndTranscribe]);

    const stop = useCallback(() => {
        listeningRef.current = false;
        setListening(false);
        resetLevels();
        onInterimText?.('');
        const recorder = mediaRecorderRef.current;
        if (recorder?.state === 'recording') {
            setTranscribing(true);
            try {
                recorder.stop();
            } catch {
                mediaRecorderRef.current = null;
                releaseAudio();
                setTranscribing(false);
            }
            return;
        }
        releaseAudio();
        setTranscribing(false);
    }, [onInterimText, releaseAudio, resetLevels]);

    const toggle = useCallback(() => {
        if (listening) stop();
        else if (!transcribing) void start();
    }, [listening, start, stop, transcribing]);

    useEffect(() => {
        const supported = typeof navigator !== 'undefined'
            && typeof navigator.mediaDevices?.getUserMedia === 'function'
            && typeof MediaRecorder !== 'undefined'
            && Boolean(pickRecorderMime());
        setEngine(supported ? 'record' : 'none');
    }, []);

    useEffect(() => {
        mountedRef.current = true;
        return () => {
            mountedRef.current = false;
            listeningRef.current = false;
            const recorder = mediaRecorderRef.current;
            mediaRecorderRef.current = null;
            if (recorder) {
                recorder.ondataavailable = null;
                recorder.onerror = null;
                recorder.onstop = null;
                try {
                    if (recorder.state === 'recording') recorder.stop();
                } catch {
                    /* ignore */
                }
            }
            releaseAudio();
            if (levelsRafRef.current != null) {
                window.cancelAnimationFrame(levelsRafRef.current);
            }
        };
    }, [releaseAudio]);

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
        supported: engine === 'record',
    };
}
