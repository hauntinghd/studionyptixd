/**
 * Cross-browser voice dictation for Studio Agent.
 * - Chrome / Edge / Safari: Web Speech API (live interim text).
 * - Firefox and others without SpeechRecognition: MediaRecorder + server whisper.
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import { resolveStudioBackendUrl } from '../shared.tsx';

export type DictationEngine = 'webspeech' | 'record' | 'none';

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
    const recognitionRef = useRef<SpeechRecognitionLike | null>(null);
    const mediaStreamRef = useRef<MediaStream | null>(null);
    const mediaRecorderRef = useRef<MediaRecorder | null>(null);
    const chunksRef = useRef<Blob[]>([]);
    const listeningRef = useRef(false);
    const webFinalRef = useRef('');

    useEffect(() => {
        const SR = getSpeechRecognitionCtor();
        if (SR) {
            setEngine('webspeech');
            return;
        }
        if (
            typeof navigator !== 'undefined'
            && typeof navigator.mediaDevices?.getUserMedia === 'function'
        ) {
            setEngine('record');
            return;
        }
        setEngine('none');
    }, []);

    const cleanupMedia = useCallback(() => {
        if (mediaRecorderRef.current && mediaRecorderRef.current.state !== 'inactive') {
            try {
                mediaRecorderRef.current.stop();
            } catch {
                /* ignore */
            }
        }
        mediaRecorderRef.current = null;
        if (mediaStreamRef.current) {
            mediaStreamRef.current.getTracks().forEach((t) => t.stop());
            mediaStreamRef.current = null;
        }
        chunksRef.current = [];
    }, []);

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
                    throw new Error(String(data.detail || data.error || `Transcription failed (${res.status})`));
                }
                const text = String(data.text || '').trim();
                if (!text) throw new Error('No speech detected.');
                onFinalText(text);
                onInterimText?.('');
            } catch (e) {
                setError((e as Error).message);
            } finally {
                setTranscribing(false);
            }
        },
        [getAccessToken, onFinalText, onInterimText],
    );

    const startRecord = useCallback(async () => {
        setError('');
        onInterimText?.('Listening… (Firefox uses record-then-transcribe)');
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
            mediaStreamRef.current = stream;
            chunksRef.current = [];
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
                    setTranscribing(false);
                    return;
                }
                void uploadAndTranscribe(blob);
            };
            recorder.start(250);
            listeningRef.current = true;
            setListening(true);
        } catch (e) {
            cleanupMedia();
            const msg = (e as Error).message || 'Microphone access denied';
            if (/denied|notallowed|permission/i.test(msg)) {
                setError(
                    'Microphone blocked — allow mic access for this site in Firefox/Chrome settings, then reload.',
                );
            } else {
                setError(msg);
            }
            listeningRef.current = false;
            setListening(false);
            onInterimText?.('');
        }
    }, [cleanupMedia, onInterimText, uploadAndTranscribe]);

    const startWebSpeech = useCallback(() => {
        const Ctor = getSpeechRecognitionCtor();
        if (!Ctor) return;
        setError('');
        const rec = new Ctor();
        recognitionRef.current = rec;
        rec.lang = lang;
        rec.continuous = true;
        rec.interimResults = true;
        rec.maxAlternatives = 1;

        webFinalRef.current = '';

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
            if (combined) onInterimText?.(combined);
        };

        rec.onerror = (event: SpeechRecognitionErrorEventLike) => {
            const code = String(event.error || '');
            if (code === 'aborted' || code === 'no-speech') return;
            if (code === 'not-allowed') {
                setError('Microphone or speech recognition blocked — check browser permissions.');
            } else if (code === 'network') {
                setError('Speech recognition network error — try again or use Firefox record mode.');
            } else {
                setError(`Dictation error: ${code || 'unknown'}`);
            }
            listeningRef.current = false;
            setListening(false);
        };

        rec.onend = () => {
            const text = webFinalRef.current.trim();
            webFinalRef.current = '';
            if (text) onFinalText(text);
            onInterimText?.('');
            listeningRef.current = false;
            setListening(false);
            recognitionRef.current = null;
        };

        try {
            rec.start();
            listeningRef.current = true;
            setListening(true);
            onInterimText?.('');
        } catch (e) {
            setError((e as Error).message || 'Could not start speech recognition');
            recognitionRef.current = null;
        }
    }, [lang, onFinalText, onInterimText]);

    const start = useCallback(() => {
        if (engine === 'webspeech') {
            void startWebSpeech();
            return;
        }
        if (engine === 'record') {
            void startRecord();
            return;
        }
        setError('Voice dictation is not supported in this browser.');
    }, [engine, startRecord, startWebSpeech]);

    const stop = useCallback(() => {
        listeningRef.current = false;
        if (engine === 'webspeech') {
            stopWebSpeech();
            setListening(false);
            return;
        }
        if (engine === 'record' && mediaRecorderRef.current?.state === 'recording') {
            setTranscribing(true);
            setListening(false);
            onInterimText?.('Transcribing…');
            try {
                mediaRecorderRef.current.stop();
            } catch {
                cleanupMedia();
                setTranscribing(false);
            }
            return;
        }
        cleanupMedia();
        setListening(false);
        setTranscribing(false);
    }, [cleanupMedia, engine, onInterimText, stopWebSpeech]);

    const toggle = useCallback(() => {
        if (listening) {
            stop();
        } else if (!transcribing) {
            start();
        }
    }, [listening, start, stop, transcribing]);

    useEffect(() => () => {
        listeningRef.current = false;
        stopWebSpeech();
        cleanupMedia();
    }, [cleanupMedia, stopWebSpeech]);

    return {
        engine,
        listening,
        transcribing,
        error,
        setError,
        start,
        stop,
        toggle,
        supported: engine !== 'none',
    };
}
