import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { ArrowLeft, ArrowRight, Download, Loader2, Lock, Pause, Play, Plus, Sparkles, Trash2, Upload, Wand2 } from 'lucide-react';
import ChatStoryPhonePreview from '../components/ChatStoryPhonePreview';
import { API, AuthContext, BILLING_SITE_URL, GENERATION_API, hasChatStoryTemplateAccess } from '../shared';
import { activeCustomVoices } from '../lib/studioVoiceLibrary';
import {
    chatStoryBackgrounds,
    chatStoryMusicOptions,
    chatStorySampleMessages,
    chatStorySfxOptions,
    chatStorySteps,
    chatStoryThemes,
    getChatStoryBackground,
    getChatStoryMusic,
    getChatStoryTheme,
    getChatStoryVoiceLabel,
    makeChatStoryMessage,
    type ChatStoryMessage,
    type ChatStoryStep,
} from '../lib/chatStoryConfig';

type Props = {
    onBack?: () => void;
};

type RenderResult = {
    outputFile: string;
    videoUrl: string;
    durationSec?: number;
    messageCount?: number;
    voice?: string;
    theme?: string;
    background?: string;
    usedBackgroundVideo?: boolean;
};

const monthlyPlanCardMeta = [
    {
        id: 'starter',
        title: 'Starter',
        fallbackPrice: '$14/mo',
        desc: 'Unlocks Chat Story on the first paid Catalyst membership tier.',
        bullets: ['Chat Story access', 'Recurring monthly access', 'Free chat renders', 'Standard queue priority'],
    },
    {
        id: 'creator',
        title: 'Creator',
        fallbackPrice: '$24/mo',
        desc: 'Best fit for active creators who need more monthly headroom.',
        bullets: ['Chat Story access', 'More monthly headroom', 'Priority queue handling', 'Higher monthly automation headroom'],
        badge: 'Recommended',
    },
    {
        id: 'pro',
        title: 'Pro',
        fallbackPrice: '$39/mo',
        desc: 'For teams running higher-volume short-form production.',
        bullets: ['Chat Story access', 'Highest public monthly headroom', 'Top queue priority', 'Best fit for daily operator volume'],
    },
];

async function fileToDataUrl(file: File): Promise<string> {
    return await new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(String(reader.result || ''));
        reader.onerror = () => reject(new Error('Failed to read file.'));
        reader.readAsDataURL(file);
    });
}

async function parseApiError(response: Response): Promise<string> {
    const raw = await response.text().catch(() => '');
    if (!raw) return `Request failed (${response.status}).`;
    try {
        const parsed = JSON.parse(raw);
        return String(parsed?.detail || parsed?.error || parsed?.message || `Request failed (${response.status}).`);
    } catch {
        return raw.slice(0, 240);
    }
}

function makeAiDraft(characterName: string, projectName: string): ChatStoryMessage[] {
    const hero = (characterName || 'Dean Winchester').trim() || 'Dean Winchester';
    const project = (projectName || 'untitled chat story').trim();
    const lowered = `${hero} ${project}`.toLowerCase();
    if (lowered.includes('dean')) {
        return [
            makeChatStoryMessage('receiver', 'Dean, why are you texting me from a graveyard at 2 AM?', 'left'),
            makeChatStoryMessage('sender', 'because the graveyard is texting back', 'right'),
            makeChatStoryMessage('receiver', 'that is not how i wanted my night to go', 'left'),
            makeChatStoryMessage('sender', 'good. open the trunk. bring salt. no questions.', 'right'),
            makeChatStoryMessage('receiver', 'you say no questions like that makes me ask fewer questions', 'left'),
            makeChatStoryMessage('sender', 'if the angel blade starts glowing, run toward me not away from me', 'right'),
        ];
    }
    return [
        makeChatStoryMessage('receiver', `did you really ship ${project || 'that'} tonight?`, 'left'),
        makeChatStoryMessage('sender', 'yes but the interesting part is what happened right after', 'right'),
        makeChatStoryMessage('receiver', 'that sounds like the start of a bad idea', 'left'),
        makeChatStoryMessage('sender', 'exactly. now keep texting back like you are not nervous.', 'right'),
        makeChatStoryMessage('receiver', 'i am absolutely nervous', 'left'),
        makeChatStoryMessage('sender', 'perfect. that is the tone.', 'right'),
    ];
}

function normalizeRoleSide(role: 'sender' | 'receiver'): 'left' | 'right' {
    return role === 'sender' ? 'right' : 'left';
}

export default function ChatStoryPanel({ onBack }: Props) {
    const { session, role, plan, billingActive, publicPlanPrices } = useContext(AuthContext);
    const hasChatStoryAccess = hasChatStoryTemplateAccess(plan, billingActive, role);
    const [step, setStep] = useState<ChatStoryStep>('script');
    const [previewMode, setPreviewMode] = useState<'video' | 'message'>('video');
    const [projectName, setProjectName] = useState('Untitled Project');
    const [characterName, setCharacterName] = useState('Omatic');
    const [messages, setMessages] = useState<ChatStoryMessage[]>(chatStorySampleMessages());
    const [themeId, setThemeId] = useState(chatStoryThemes[0].id);
    const [backgroundId, setBackgroundId] = useState(chatStoryBackgrounds[0].id);
    const [backgroundVideoFile, setBackgroundVideoFile] = useState<File | null>(null);
    const [backgroundVideoUrl, setBackgroundVideoUrl] = useState<string | null>(null);
    const [avatarFile, setAvatarFile] = useState<File | null>(null);
    const [avatarDataUrl, setAvatarDataUrl] = useState('');
    const [musicId, setMusicId] = useState(chatStoryMusicOptions[0].id);
    const [voiceId, setVoiceId] = useState(activeCustomVoices[0]?.id || 'studio_voice_core');
    const [voiceSpeed, setVoiceSpeed] = useState(activeCustomVoices[0]?.defaultSpeed || 1);
    const [selectedSfxIds, setSelectedSfxIds] = useState<string[]>(['message_send', 'message_receive']);
    const [rendering, setRendering] = useState(false);
    const [renderError, setRenderError] = useState('');
    const [renderResult, setRenderResult] = useState<RenderResult | null>(null);
    const [playingPreviewKey, setPlayingPreviewKey] = useState('');
    const audioPreviewRef = useRef<HTMLAudioElement | null>(null);

    useEffect(() => {
        if (!backgroundVideoFile) {
            setBackgroundVideoUrl(null);
            return;
        }
        const objectUrl = URL.createObjectURL(backgroundVideoFile);
        setBackgroundVideoUrl(objectUrl);
        return () => URL.revokeObjectURL(objectUrl);
    }, [backgroundVideoFile]);

    useEffect(() => {
        return () => {
            if (audioPreviewRef.current) {
                audioPreviewRef.current.pause();
                audioPreviewRef.current = null;
            }
        };
    }, []);

    const theme = getChatStoryTheme(themeId);
    const background = getChatStoryBackground(backgroundId);
    const music = getChatStoryMusic(musicId);
    const voiceLabel = getChatStoryVoiceLabel(activeCustomVoices, voiceId);
    const stepIndex = chatStorySteps.indexOf(step);
    const selectedMessageCount = messages.filter((message) => String(message.text || '').trim()).length;

    const topCopy = useMemo(() => {
        if (hasChatStoryAccess) {
            return 'Build text-message shorts with owned voices, shipped music and SFX, uploaded backgrounds, and a live phone preview that matches the Catalyst render path.';
        }
        return 'Chat Story follows the Catalyst membership lane. Once membership is active, renders do not burn wallet credits.';
    }, [hasChatStoryAccess]);
    const monthlyPlanCards = useMemo(() => {
        return monthlyPlanCardMeta.map((planCard) => {
            const raw = Number((publicPlanPrices as Record<string, number>)[planCard.id]);
            const price = Number.isFinite(raw) && raw > 0
                ? `$${raw.toFixed(raw % 1 === 0 ? 0 : 2)}/mo`
                : planCard.fallbackPrice;
            return {
                ...planCard,
                price,
            };
        });
    }, [publicPlanPrices]);

    const stopPreviewAudio = useCallback(() => {
        if (audioPreviewRef.current) {
            audioPreviewRef.current.pause();
            audioPreviewRef.current.currentTime = 0;
            audioPreviewRef.current = null;
        }
        setPlayingPreviewKey('');
    }, []);

    const playPreviewAudio = useCallback((key: string, src: string) => {
        if (!src) return;
        if (playingPreviewKey === key) {
            stopPreviewAudio();
            return;
        }
        stopPreviewAudio();
        const audio = new Audio(src);
        audioPreviewRef.current = audio;
        setPlayingPreviewKey(key);
        audio.onended = () => {
            if (audioPreviewRef.current === audio) {
                audioPreviewRef.current = null;
            }
            setPlayingPreviewKey('');
        };
        audio.onerror = () => {
            if (audioPreviewRef.current === audio) {
                audioPreviewRef.current = null;
            }
            setPlayingPreviewKey('');
        };
        void audio.play().catch(() => {
            if (audioPreviewRef.current === audio) {
                audioPreviewRef.current = null;
            }
            setPlayingPreviewKey('');
        });
    }, [playingPreviewKey, stopPreviewAudio]);

    const openSubscriptionPage = () => {
        window.location.href = `${BILLING_SITE_URL}?page=subscription`;
    };

    const updateMessage = (messageId: string, patch: Partial<ChatStoryMessage>) => {
        setMessages((current) => current.map((message) => {
            if (message.id !== messageId) return message;
            const next = { ...message, ...patch };
            if (patch.role && !patch.side) {
                next.side = normalizeRoleSide(patch.role);
            }
            return next;
        }));
    };

    const removeMessage = (messageId: string) => {
        setMessages((current) => current.filter((message) => message.id !== messageId));
    };

    const addMessage = (roleName: 'sender' | 'receiver') => {
        setMessages((current) => [...current, makeChatStoryMessage(roleName, '', normalizeRoleSide(roleName))]);
    };

    const swapConversation = () => {
        setMessages((current) => current.map((message) => {
            const nextRole = message.role === 'sender' ? 'receiver' : 'sender';
            return {
                ...message,
                role: nextRole,
                side: normalizeRoleSide(nextRole),
            };
        }));
    };

    const importScript = () => {
        const raw = window.prompt('Paste one message per line. Use "sender:" or "receiver:" prefixes if you want explicit roles.');
        if (!raw) return;
        const lines = raw.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
        if (!lines.length) return;
        const imported = lines.map((line, index) => {
            const match = line.match(/^(sender|receiver)\s*:\s*(.+)$/i);
            const roleName = match ? (match[1].toLowerCase() as 'sender' | 'receiver') : (index % 2 === 0 ? 'receiver' : 'sender');
            const text = match ? match[2].trim() : line;
            return makeChatStoryMessage(roleName, text, normalizeRoleSide(roleName));
        });
        setMessages(imported);
    };

    const generateAiScript = () => {
        setMessages(makeAiDraft(characterName, projectName));
    };

    const handleAvatarUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        if (!file) return;
        setAvatarFile(file);
        try {
            setAvatarDataUrl(await fileToDataUrl(file));
        } catch {
            setRenderError('Failed to load avatar preview.');
        }
    };

    const handleBackgroundVideoUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        if (!file) return;
        setBackgroundVideoFile(file);
        setRenderError('');
    };

    const toggleSfx = (sfxId: string) => {
        setSelectedSfxIds((current) => current.includes(sfxId) ? current.filter((id) => id !== sfxId) : [...current, sfxId]);
    };

    const handleRender = async () => {
        if (!session) {
            setRenderError('Sign in first.');
            return;
        }
        if (!hasChatStoryAccess) {
            setRenderError('Chat Story requires an active Catalyst membership.');
            return;
        }
        if (!messages.some((message) => String(message.text || '').trim())) {
            setRenderError('Add at least one message before rendering.');
            return;
        }

        setRendering(true);
        setRenderError('');
        setRenderResult(null);
        try {
            const payload = {
                projectName,
                characterName,
                messages,
                themeId,
                backgroundId,
                musicId,
                voiceId,
                voiceSpeed,
                sfxIds: selectedSfxIds,
            };
            const formData = new FormData();
            formData.append('payload', JSON.stringify(payload));
            if (avatarFile) formData.append('avatar', avatarFile);
            if (backgroundVideoFile) formData.append('background_video', backgroundVideoFile);

            const response = await fetch(`${API}/api/chatstory/render`, {
                method: 'POST',
                headers: { Authorization: `Bearer ${session.access_token}` },
                body: formData,
            });
            if (!response.ok) throw new Error(await parseApiError(response));
            const data = await response.json();
            const downloadPath = String(data?.download_url || '');
            setRenderResult({
                outputFile: String(data?.output_file || ''),
                videoUrl: downloadPath ? `${GENERATION_API}${downloadPath}` : '',
                durationSec: Number(data?.duration_sec || 0),
                messageCount: Number(data?.message_count || messages.length),
                voice: String(data?.voice || voiceLabel),
                theme: String(data?.theme || theme.label),
                background: String(data?.background || background.label),
                usedBackgroundVideo: Boolean(data?.used_background_video),
            });
            setPreviewMode('video');
        } catch (error: any) {
            setRenderError(error?.message || 'Chat Story render failed.');
        } finally {
            setRendering(false);
        }
    };

    const latestRenderVideoUrl = renderResult?.videoUrl || (renderResult?.outputFile ? `${GENERATION_API}/api/download/${renderResult.outputFile}` : '');
    const moveStep = (direction: -1 | 1) => {
        const nextIndex = Math.min(chatStorySteps.length - 1, Math.max(0, stepIndex + direction));
        setStep(chatStorySteps[nextIndex]);
    };

    return (
        <div className="space-y-6">
            <div className="rounded-[28px] border border-white/[0.06] bg-white/[0.02] p-6">
                <div className="flex flex-wrap items-start justify-between gap-4">
                    <div className="space-y-3">
                        <div className="flex items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.24em] text-violet-300">
                            <Sparkles className="h-4 w-4" />
                            Premium Template
                        </div>
                        <div>
                            <h2 className="text-3xl font-bold text-white">Chat Story</h2>
                            <p className="mt-2 max-w-3xl text-sm text-gray-400">{topCopy}</p>
                        </div>
                    </div>
                    <div className="flex flex-wrap items-center gap-3">
                        {onBack && (
                            <button
                                type="button"
                                onClick={onBack}
                                className="inline-flex items-center gap-2 rounded-xl border border-white/[0.08] bg-white/[0.02] px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:border-white/[0.14] hover:bg-white/[0.06]"
                            >
                                <ArrowLeft className="h-4 w-4" />
                                Back
                            </button>
                        )}
                        <div className="rounded-full border border-violet-500/30 bg-violet-500/10 px-4 py-2 text-sm font-semibold text-violet-200">
                            Editor + Preview Alpha
                        </div>
                        <div className="rounded-full border border-cyan-500/20 bg-cyan-500/10 px-4 py-2 text-sm font-semibold text-cyan-100">
                            ~10s render baseline
                        </div>
                    </div>
                </div>
            </div>

            {!hasChatStoryAccess && (
                <div className="rounded-[28px] border border-amber-500/20 bg-amber-500/10 p-6">
                    <div className="flex flex-wrap items-start justify-between gap-4">
                        <div>
                            <div className="flex items-center gap-2 text-sm font-semibold text-amber-200">
                                <Lock className="h-4 w-4" />
                                Membership required
                            </div>
                            <h3 className="mt-2 text-2xl font-bold text-white">Catalyst membership required</h3>
                            <p className="mt-2 max-w-3xl text-sm text-amber-100/80">
                                Chat Story does not burn wallet credits. Catalyst membership unlocks the template and its render path, while wallet packs stay available for heavier animation elsewhere in Studio.
                            </p>
                        </div>
                        <button
                            type="button"
                            onClick={openSubscriptionPage}
                            className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500"
                        >
                            Open Membership
                        </button>
                    </div>
                    <div className="mt-6 grid gap-4 lg:grid-cols-3">
                        {monthlyPlanCards.map((card) => (
                            <div key={card.id} className={`relative rounded-3xl border p-5 ${card.badge ? 'border-violet-500/40 bg-violet-500/[0.06]' : 'border-white/[0.08] bg-black/20'}`}>
                                {card.badge && (
                                    <div className="absolute right-4 top-4 rounded-full border border-violet-400/40 bg-violet-500/15 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-violet-200">
                                        {card.badge}
                                    </div>
                                )}
                                <p className="text-sm font-semibold text-white">{card.title}</p>
                                <p className="mt-2 text-4xl font-bold text-white">{card.price}</p>
                                <p className="mt-2 text-sm text-gray-400">{card.desc}</p>
                                <div className="mt-4 space-y-2">
                                    {card.bullets.map((bullet) => (
                                        <div key={bullet} className="text-sm text-gray-300">• {bullet}</div>
                                    ))}
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            <div className="grid gap-6 xl:grid-cols-[104px,minmax(0,1fr),440px]">
                <aside className="space-y-4 rounded-[28px] border border-white/[0.06] bg-white/[0.02] p-4 xl:sticky xl:top-20 xl:self-start">
                    {chatStorySteps.map((currentStep) => (
                        <button
                            key={currentStep}
                            type="button"
                            onClick={() => setStep(currentStep)}
                            className={`w-full rounded-2xl border px-4 py-4 text-sm font-medium transition ${
                                step === currentStep
                                    ? 'border-violet-500 bg-violet-500/10 text-white'
                                    : 'border-white/[0.08] bg-black/20 text-gray-400 hover:border-violet-500/30 hover:text-white'
                            }`}
                        >
                            {currentStep === 'background' ? 'Background Video' : currentStep.charAt(0).toUpperCase() + currentStep.slice(1)}
                        </button>
                    ))}
                </aside>

                <div className="rounded-[28px] border border-white/[0.06] bg-white/[0.02] p-6 space-y-6">
                    <div className="space-y-4 border-b border-white/[0.06] pb-5">
                        <input
                            value={projectName}
                            onChange={(event) => setProjectName(event.target.value)}
                            className="w-full rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3 text-xl font-bold text-white outline-none transition focus:border-violet-500/50"
                            placeholder="Untitled Project"
                        />
                        <div className="flex flex-wrap gap-3">
                            <button type="button" onClick={swapConversation} className="rounded-xl border border-white/[0.08] bg-black/20 px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:bg-white/[0.04]">Swap</button>
                            <button type="button" onClick={() => setMessages(chatStorySampleMessages())} className="rounded-xl border border-white/[0.08] bg-black/20 px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:bg-white/[0.04]">Sample Script</button>
                            <button type="button" onClick={importScript} className="rounded-xl border border-white/[0.08] bg-black/20 px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:bg-white/[0.04]">Import</button>
                            <button type="button" onClick={generateAiScript} className="rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500">Generate AI Script</button>
                        </div>
                    </div>

                    {step === 'script' && (
                        <div className="grid gap-6 xl:grid-cols-[280px,minmax(0,1fr)]">
                            <div className="space-y-4">
                                <div className="rounded-3xl border border-white/[0.08] bg-black/20 p-5">
                                    <div className="flex items-center gap-4">
                                        <div className="flex h-16 w-16 items-center justify-center overflow-hidden rounded-full border border-white/[0.08] bg-white/[0.03] text-2xl font-bold text-white">
                                            {avatarDataUrl ? <img src={avatarDataUrl} alt="Avatar" className="h-full w-full object-cover" /> : (characterName.slice(0, 1).toUpperCase() || 'O')}
                                        </div>
                                        <div>
                                            <p className="text-xl font-bold text-white">Upload Avatar</p>
                                            <p className="mt-1 text-sm text-gray-400">Recommended: 400px x 400px, PNG or JPEG.</p>
                                        </div>
                                    </div>
                                    <label className="mt-4 inline-flex cursor-pointer items-center gap-2 rounded-xl border border-white/[0.08] bg-black/20 px-4 py-2.5 text-sm font-medium text-white transition hover:bg-white/[0.04]">
                                        <Upload className="h-4 w-4" />
                                        Upload
                                        <input type="file" accept="image/png,image/jpeg,image/webp" className="hidden" onChange={handleAvatarUpload} />
                                    </label>
                                    <div className="mt-4">
                                        <label className="text-sm font-medium text-gray-300">Character Name</label>
                                        <input
                                            value={characterName}
                                            onChange={(event) => setCharacterName(event.target.value)}
                                            className="mt-2 w-full rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3 text-base text-white outline-none transition focus:border-violet-500/50"
                                        />
                                    </div>
                                </div>
                                <div className="rounded-3xl border border-emerald-500/20 bg-emerald-500/10 p-5">
                                    <p className="text-sm font-semibold text-emerald-100">Render path</p>
                                    <p className="mt-2 text-sm text-emerald-100/80">Membership unlocks this lane, and renders here do not burn wallet credits. Background video uploads, owned voices, and shipped music/SFX all go through the real backend render route.</p>
                                </div>
                            </div>
                            <div className="space-y-4">
                                {messages.map((message, index) => (
                                    <div key={message.id} className="rounded-3xl border border-white/[0.08] bg-black/20 p-4">
                                        <div className="flex flex-wrap items-center justify-between gap-3">
                                            <div className="inline-flex rounded-xl border border-white/[0.08] bg-black/20 p-1">
                                                <button
                                                    type="button"
                                                    onClick={() => updateMessage(message.id, { role: 'receiver', side: 'left' })}
                                                    className={`rounded-lg px-4 py-2 text-sm font-semibold transition ${message.role === 'receiver' ? 'bg-white text-black' : 'text-gray-300 hover:bg-white/[0.05]'}`}
                                                >
                                                    Receiver
                                                </button>
                                                <button
                                                    type="button"
                                                    onClick={() => updateMessage(message.id, { role: 'sender', side: 'right' })}
                                                    className={`rounded-lg px-4 py-2 text-sm font-semibold transition ${message.role === 'sender' ? 'bg-violet-600 text-white' : 'text-gray-300 hover:bg-white/[0.05]'}`}
                                                >
                                                    Sender
                                                </button>
                                            </div>
                                            <button
                                                type="button"
                                                onClick={() => removeMessage(message.id)}
                                                className="inline-flex items-center gap-2 rounded-xl border border-white/[0.08] bg-black/20 px-3 py-2 text-sm font-medium text-gray-300 transition hover:bg-white/[0.04]"
                                            >
                                                <Trash2 className="h-4 w-4" />
                                                Remove
                                            </button>
                                        </div>
                                        <textarea
                                            value={message.text}
                                            onChange={(event) => updateMessage(message.id, { text: event.target.value })}
                                            rows={4}
                                            className="mt-4 w-full rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3 text-base text-white outline-none transition focus:border-violet-500/50 resize-none"
                                            placeholder="Enter your message here"
                                        />
                                        <div className="mt-4 max-w-[140px]">
                                            <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-gray-500">Side</label>
                                            <select
                                                value={message.side}
                                                onChange={(event) => updateMessage(message.id, { side: event.target.value as 'left' | 'right' })}
                                                className="w-full rounded-xl border border-white/[0.08] bg-black/20 px-3 py-2.5 text-sm text-white outline-none transition focus:border-violet-500/50"
                                            >
                                                <option value="left">Left</option>
                                                <option value="right">Right</option>
                                            </select>
                                        </div>
                                        <div className="mt-3 text-xs text-gray-500">Message {index + 1}</div>
                                    </div>
                                ))}
                                <div className="flex flex-wrap gap-3">
                                    <button type="button" onClick={() => addMessage('receiver')} className="inline-flex items-center gap-2 rounded-xl border border-white/[0.08] bg-black/20 px-4 py-2.5 text-sm font-medium text-white transition hover:bg-white/[0.04]"><Plus className="h-4 w-4" /> Add Receiver</button>
                                    <button type="button" onClick={() => addMessage('sender')} className="inline-flex items-center gap-2 rounded-xl border border-white/[0.08] bg-black/20 px-4 py-2.5 text-sm font-medium text-white transition hover:bg-white/[0.04]"><Plus className="h-4 w-4" /> Add Sender</button>
                                </div>
                            </div>
                        </div>
                    )}

                    {step === 'theme' && (
                        <div className="space-y-5">
                            <div className="grid gap-4 lg:grid-cols-2">
                                {chatStoryThemes.map((cardTheme) => (
                                    <button
                                        key={cardTheme.id}
                                        type="button"
                                        onClick={() => setThemeId(cardTheme.id)}
                                        className={`rounded-3xl border p-4 text-left transition ${themeId === cardTheme.id ? 'border-violet-500 bg-violet-500/10' : 'border-white/[0.08] bg-black/20 hover:border-violet-500/30'}`}
                                    >
                                        <p className="text-xl font-bold text-white">{cardTheme.label}</p>
                                        <div className="mt-4">
                                            <ChatStoryPhonePreview
                                                previewMode="message"
                                                theme={cardTheme}
                                                background={background}
                                                backgroundVideoUrl={backgroundVideoUrl}
                                                characterName={characterName}
                                                avatarDataUrl={avatarDataUrl}
                                                messages={messages}
                                                size="card"
                                            />
                                        </div>
                                    </button>
                                ))}
                            </div>
                        </div>
                    )}

                    {step === 'background' && (
                        <div className="space-y-5">
                            <div className="grid gap-4 lg:grid-cols-3">
                                {chatStoryBackgrounds.map((cardBackground) => (
                                    <button
                                        key={cardBackground.id}
                                        type="button"
                                        onClick={() => {
                                            setBackgroundId(cardBackground.id);
                                            setPreviewMode('video');
                                        }}
                                        className={`rounded-3xl border p-4 text-left transition ${backgroundId === cardBackground.id ? 'border-violet-500 bg-violet-500/10' : 'border-white/[0.08] bg-black/20 hover:border-violet-500/30'}`}
                                    >
                                        <p className="text-xl font-bold text-white">{cardBackground.label}</p>
                                        <div className="mt-4">
                                            <ChatStoryPhonePreview
                                                previewMode="video"
                                                theme={theme}
                                                background={cardBackground}
                                                backgroundVideoUrl={backgroundId === cardBackground.id ? backgroundVideoUrl : null}
                                                characterName={characterName}
                                                avatarDataUrl={avatarDataUrl}
                                                messages={messages}
                                                size="card"
                                            />
                                        </div>
                                        <div className="mt-4 flex items-center gap-2">
                                            {cardBackground.gradient.map((color) => (
                                                <span
                                                    key={`${cardBackground.id}_${color}`}
                                                    className="h-3 w-3 rounded-full border border-white/10"
                                                    style={{ backgroundColor: color }}
                                                />
                                            ))}
                                        </div>
                                    </button>
                                ))}
                            </div>

                            <div className="rounded-3xl border border-white/[0.08] bg-black/20 p-5">
                                <div className="flex flex-wrap items-start justify-between gap-4">
                                    <div>
                                        <p className="text-xl font-bold text-white">Optional Background Video</p>
                                        <p className="mt-2 max-w-2xl text-sm text-gray-400">
                                            Upload your own gameplay or B-roll clip to sit behind the phone mockup. If nothing is uploaded, the selected built-in background stays active.
                                        </p>
                                    </div>
                                    {backgroundVideoFile && (
                                        <button
                                            type="button"
                                            onClick={() => setBackgroundVideoFile(null)}
                                            className="rounded-xl border border-white/[0.08] bg-white/[0.03] px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:bg-white/[0.06]"
                                        >
                                            Remove Upload
                                        </button>
                                    )}
                                </div>

                                <div className="mt-5 flex flex-wrap items-center gap-3">
                                    <label className="inline-flex cursor-pointer items-center gap-2 rounded-xl border border-white/[0.08] bg-black/20 px-4 py-2.5 text-sm font-medium text-white transition hover:bg-white/[0.04]">
                                        <Upload className="h-4 w-4" />
                                        Upload Video
                                        <input type="file" accept="video/mp4,video/quicktime,video/webm" className="hidden" onChange={handleBackgroundVideoUpload} />
                                    </label>
                                    <span className="text-xs text-gray-500">Recommended: vertical MP4, 9:16, 10-15s loop.</span>
                                </div>

                                {backgroundVideoFile && (
                                    <div className="mt-5 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3 text-sm text-cyan-100">
                                        <div className="font-semibold">{backgroundVideoFile.name}</div>
                                        <div className="mt-1 text-cyan-100/80">{(backgroundVideoFile.size / (1024 * 1024)).toFixed(2)} MB uploaded for render.</div>
                                    </div>
                                )}
                            </div>
                        </div>
                    )}

                    {step === 'audio' && (
                        <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr),320px]">
                            <div className="space-y-5">
                                <div className="rounded-3xl border border-white/[0.08] bg-black/20 p-5">
                                    <div className="flex flex-wrap items-start justify-between gap-4">
                                        <div>
                                            <p className="text-xl font-bold text-white">Voice Presets</p>
                                            <p className="mt-2 text-sm text-gray-400">Owned Studio voices feed the live render route. Selecting a preset also resets the speed to that voice's default cadence.</p>
                                        </div>
                                        <div className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-4 py-2 text-xs font-semibold uppercase tracking-[0.18em] text-emerald-200">
                                            Owned Voice Library
                                        </div>
                                    </div>
                                    <div className="mt-5 grid gap-3 md:grid-cols-2">
                                        {activeCustomVoices.map((voice) => {
                                            const active = voiceId === voice.id;
                                            return (
                                                <button
                                                    key={voice.id}
                                                    type="button"
                                                    onClick={() => {
                                                        setVoiceId(voice.id);
                                                        setVoiceSpeed(voice.defaultSpeed);
                                                    }}
                                                    className={`rounded-2xl border p-4 text-left transition ${active ? 'border-violet-500 bg-violet-500/10' : 'border-white/[0.08] bg-black/20 hover:border-violet-500/30'}`}
                                                >
                                                    <div className="flex items-start justify-between gap-3">
                                                        <div>
                                                            <p className="text-sm font-semibold text-white">{voice.name}</p>
                                                            <p className="mt-1 text-xs text-gray-500">{voice.profile}</p>
                                                        </div>
                                                        <span className="rounded-full border border-white/10 bg-white/[0.03] px-2 py-1 text-[10px] uppercase tracking-[0.16em] text-gray-400">
                                                            {voice.source}
                                                        </span>
                                                    </div>
                                                </button>
                                            );
                                        })}
                                    </div>
                                </div>

                                <div className="rounded-3xl border border-white/[0.08] bg-black/20 p-5">
                                    <div className="flex items-center justify-between gap-4">
                                        <div>
                                            <p className="text-xl font-bold text-white">Voice Speed</p>
                                            <p className="mt-2 text-sm text-gray-400">Tune narration pacing before render. The preview phone stays live while you adjust.</p>
                                        </div>
                                        <div className="rounded-full border border-cyan-500/20 bg-cyan-500/10 px-4 py-2 text-sm font-semibold text-cyan-100">
                                            {voiceSpeed.toFixed(2)}x
                                        </div>
                                    </div>
                                    <input
                                        type="range"
                                        min="0.8"
                                        max="1.3"
                                        step="0.01"
                                        value={voiceSpeed}
                                        onChange={(event) => setVoiceSpeed(Number(event.target.value))}
                                        className="mt-5 w-full accent-violet-500"
                                    />
                                </div>
                            </div>

                            <div className="space-y-5">
                                <div className="rounded-3xl border border-white/[0.08] bg-black/20 p-5">
                                    <p className="text-xl font-bold text-white">Background Music</p>
                                    <div className="mt-4 space-y-3">
                                        {chatStoryMusicOptions.map((option) => {
                                            const active = musicId === option.id;
                                            const previewKey = `music_${option.id}`;
                                            const isPlaying = playingPreviewKey === previewKey;
                                            return (
                                                <div
                                                    key={option.id}
                                                    className={`rounded-2xl border p-4 transition ${active ? 'border-violet-500 bg-violet-500/10' : 'border-white/[0.08] bg-black/20'}`}
                                                >
                                                    <div className="flex items-center justify-between gap-3">
                                                        <button
                                                            type="button"
                                                            onClick={() => setMusicId(option.id)}
                                                            className="min-w-0 flex-1 text-left"
                                                        >
                                                            <p className="truncate text-sm font-semibold text-white">{option.label}</p>
                                                            <p className="mt-1 text-xs text-gray-500">{option.src ? 'Built-in preview available' : 'Silence under the chat flow'}</p>
                                                        </button>
                                                        {option.src ? (
                                                            <button
                                                                type="button"
                                                                onClick={() => playPreviewAudio(previewKey, option.src)}
                                                                className="rounded-xl border border-white/[0.08] bg-white/[0.03] p-2 text-gray-200 transition hover:bg-white/[0.06]"
                                                            >
                                                                {isPlaying ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
                                                            </button>
                                                        ) : (
                                                            <span className="text-[11px] font-semibold uppercase tracking-[0.16em] text-gray-500">Muted</span>
                                                        )}
                                                    </div>
                                                </div>
                                            );
                                        })}
                                    </div>
                                </div>

                                <div className="rounded-3xl border border-white/[0.08] bg-black/20 p-5">
                                    <div className="flex items-center justify-between gap-4">
                                        <p className="text-xl font-bold text-white">SFX</p>
                                        <div className="text-xs font-semibold uppercase tracking-[0.18em] text-gray-500">{selectedSfxIds.length} active</div>
                                    </div>
                                    <div className="mt-4 space-y-3">
                                        {chatStorySfxOptions.map((option) => {
                                            const enabled = selectedSfxIds.includes(option.id);
                                            const previewKey = `sfx_${option.id}`;
                                            const isPlaying = playingPreviewKey === previewKey;
                                            return (
                                                <div
                                                    key={option.id}
                                                    className={`rounded-2xl border p-4 transition ${enabled ? 'border-cyan-500/40 bg-cyan-500/10' : 'border-white/[0.08] bg-black/20'}`}
                                                >
                                                    <div className="flex items-center justify-between gap-3">
                                                        <button
                                                            type="button"
                                                            onClick={() => toggleSfx(option.id)}
                                                            className="min-w-0 flex-1 text-left"
                                                        >
                                                            <p className="truncate text-sm font-semibold text-white">{option.label}</p>
                                                            <p className="mt-1 text-xs text-gray-500">{enabled ? 'Included in render' : 'Not used right now'}</p>
                                                        </button>
                                                        <button
                                                            type="button"
                                                            onClick={() => playPreviewAudio(previewKey, option.src)}
                                                            className="rounded-xl border border-white/[0.08] bg-white/[0.03] p-2 text-gray-200 transition hover:bg-white/[0.06]"
                                                        >
                                                            {isPlaying ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
                                                        </button>
                                                    </div>
                                                </div>
                                            );
                                        })}
                                    </div>
                                </div>
                            </div>
                        </div>
                    )}

                    {renderError && (
                        <div className="rounded-2xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                            {renderError}
                        </div>
                    )}

                    <div className="flex flex-wrap items-center justify-between gap-3 border-t border-white/[0.06] pt-5">
                        <div className="text-sm text-gray-500">
                            Step {stepIndex + 1} of {chatStorySteps.length}
                        </div>
                        <div className="flex flex-wrap gap-3">
                            <button
                                type="button"
                                onClick={() => moveStep(-1)}
                                disabled={stepIndex === 0}
                                className="rounded-xl border border-white/[0.08] bg-white/[0.03] px-4 py-2.5 text-sm font-medium text-gray-200 transition hover:bg-white/[0.06] disabled:opacity-40"
                            >
                                Previous
                            </button>
                            {stepIndex < chatStorySteps.length - 1 ? (
                                <button
                                    type="button"
                                    onClick={() => moveStep(1)}
                                    className="inline-flex items-center gap-2 rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500"
                                >
                                    Next Step
                                    <ArrowRight className="h-4 w-4" />
                                </button>
                            ) : (
                                <button
                                    type="button"
                                    onClick={() => {
                                        if (!hasChatStoryAccess) {
                                            openSubscriptionPage();
                                            return;
                                        }
                                        void handleRender();
                                    }}
                                    disabled={rendering}
                                    className="inline-flex items-center gap-2 rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-violet-500 disabled:opacity-60"
                                >
                                    {rendering ? <Loader2 className="h-4 w-4 animate-spin" /> : hasChatStoryAccess ? <Wand2 className="h-4 w-4" /> : <Lock className="h-4 w-4" />}
                                    {rendering ? 'Rendering...' : hasChatStoryAccess ? 'Render Chat Story' : 'Open Membership'}
                                </button>
                            )}
                        </div>
                    </div>
                </div>

                <aside className="space-y-5 rounded-[28px] border border-white/[0.06] bg-white/[0.02] p-5 xl:sticky xl:top-20 xl:self-start">
                    <div className="flex items-center justify-between gap-3">
                        <div>
                            <p className="text-[10px] font-semibold uppercase tracking-[0.24em] text-cyan-300">Live Preview</p>
                            <p className="mt-2 text-lg font-bold text-white">Phone Canvas</p>
                        </div>
                        <div className="inline-flex rounded-xl border border-white/[0.08] bg-black/20 p-1">
                            <button
                                type="button"
                                onClick={() => setPreviewMode('message')}
                                className={`rounded-lg px-3 py-2 text-xs font-semibold transition ${previewMode === 'message' ? 'bg-white text-black' : 'text-gray-300 hover:bg-white/[0.05]'}`}
                            >
                                Messages
                            </button>
                            <button
                                type="button"
                                onClick={() => setPreviewMode('video')}
                                className={`rounded-lg px-3 py-2 text-xs font-semibold transition ${previewMode === 'video' ? 'bg-violet-600 text-white' : 'text-gray-300 hover:bg-white/[0.05]'}`}
                            >
                                Video
                            </button>
                        </div>
                    </div>

                    <ChatStoryPhonePreview
                        previewMode={previewMode}
                        theme={theme}
                        background={background}
                        backgroundVideoUrl={backgroundVideoUrl}
                        characterName={characterName}
                        avatarDataUrl={avatarDataUrl}
                        messages={messages}
                        size="panel"
                    />

                    <div className="rounded-3xl border border-white/[0.08] bg-black/20 p-5">
                        <div className="flex items-center justify-between gap-3">
                            <div>
                                <p className="text-[10px] font-semibold uppercase tracking-[0.24em] text-gray-500">Render Summary</p>
                                <p className="mt-2 text-lg font-bold text-white">{projectName || 'Untitled Project'}</p>
                            </div>
                            <div className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-3 py-1.5 text-xs font-semibold text-emerald-200">
                                {selectedMessageCount} messages
                            </div>
                        </div>
                        <div className="mt-5 grid gap-3 sm:grid-cols-2">
                            <div className="rounded-2xl border border-white/[0.08] bg-white/[0.03] px-4 py-3">
                                <div className="text-[10px] uppercase tracking-[0.18em] text-gray-500">Theme</div>
                                <div className="mt-2 text-sm font-semibold text-white">{theme.label}</div>
                            </div>
                            <div className="rounded-2xl border border-white/[0.08] bg-white/[0.03] px-4 py-3">
                                <div className="text-[10px] uppercase tracking-[0.18em] text-gray-500">Background</div>
                                <div className="mt-2 text-sm font-semibold text-white">{background.label}</div>
                            </div>
                            <div className="rounded-2xl border border-white/[0.08] bg-white/[0.03] px-4 py-3">
                                <div className="text-[10px] uppercase tracking-[0.18em] text-gray-500">Voice</div>
                                <div className="mt-2 text-sm font-semibold text-white">{voiceLabel}</div>
                            </div>
                            <div className="rounded-2xl border border-white/[0.08] bg-white/[0.03] px-4 py-3">
                                <div className="text-[10px] uppercase tracking-[0.18em] text-gray-500">Music</div>
                                <div className="mt-2 text-sm font-semibold text-white">{music.label}</div>
                            </div>
                        </div>
                        <div className="mt-3 rounded-2xl border border-white/[0.08] bg-white/[0.03] px-4 py-3 text-sm text-gray-300">
                            {backgroundVideoFile ? `Uploaded background video: ${backgroundVideoFile.name}` : 'No uploaded background video. Built-in gradient background will render.'}
                        </div>
                    </div>

                    {renderResult ? (
                        <div className="rounded-3xl border border-emerald-500/20 bg-emerald-500/[0.05] p-5 space-y-4">
                            <div className="flex items-center justify-between gap-3">
                                <div>
                                    <p className="text-[10px] font-semibold uppercase tracking-[0.24em] text-emerald-200/80">Latest Render</p>
                                    <p className="mt-2 text-lg font-bold text-white">Chat Story ready</p>
                                </div>
                                <div className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-3 py-1.5 text-xs font-semibold text-emerald-200">
                                    {renderResult.durationSec ? `${renderResult.durationSec.toFixed(1)}s` : 'Rendered'}
                                </div>
                            </div>

                            {latestRenderVideoUrl ? (
                                <video controls className="w-full rounded-2xl bg-black" src={latestRenderVideoUrl} />
                            ) : (
                                <div className="rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-6 text-center text-sm text-gray-400">
                                    Render finished, but no preview URL was returned.
                                </div>
                            )}

                            <div className="grid gap-3 sm:grid-cols-2">
                                <div className="rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3">
                                    <div className="text-[10px] uppercase tracking-[0.18em] text-gray-500">Voice</div>
                                    <div className="mt-2 text-sm font-semibold text-white">{renderResult.voice || voiceLabel}</div>
                                </div>
                                <div className="rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3">
                                    <div className="text-[10px] uppercase tracking-[0.18em] text-gray-500">Messages</div>
                                    <div className="mt-2 text-sm font-semibold text-white">{renderResult.messageCount || selectedMessageCount}</div>
                                </div>
                                <div className="rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3">
                                    <div className="text-[10px] uppercase tracking-[0.18em] text-gray-500">Theme</div>
                                    <div className="mt-2 text-sm font-semibold text-white">{renderResult.theme || theme.label}</div>
                                </div>
                                <div className="rounded-2xl border border-white/[0.08] bg-black/20 px-4 py-3">
                                    <div className="text-[10px] uppercase tracking-[0.18em] text-gray-500">Background</div>
                                    <div className="mt-2 text-sm font-semibold text-white">{renderResult.background || background.label}</div>
                                </div>
                            </div>

                            {latestRenderVideoUrl && (
                                <a
                                    href={latestRenderVideoUrl}
                                    download={renderResult.outputFile || 'chatstory.mp4'}
                                    className="inline-flex w-full items-center justify-center gap-2 rounded-xl bg-emerald-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-emerald-500"
                                >
                                    <Download className="h-4 w-4" />
                                    Download MP4
                                </a>
                            )}
                        </div>
                    ) : (
                        <div className="rounded-3xl border border-white/[0.08] bg-black/20 px-5 py-6 text-sm text-gray-400">
                            No render yet. Finish audio setup, then run the backend render from the last step.
                        </div>
                    )}
                </aside>
            </div>
        </div>
    );
}

