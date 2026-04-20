import { Video, X } from 'lucide-react';

export interface AnimateAllScene {
    index: number;
    imageData?: string;
    visual_description: string;
    duration_sec?: number;
    animation_prompt?: string;
}

export interface AnimateAllModel {
    id: string;
    label: string;
    subtitle: string;           // e.g. "Free with your plan — 8s @ 1080p with audio"
    parallel_limit: number;     // up to N scenes parallel based on plan
    fixed_duration_sec: number; // 6 | 8 | 10
    supports_audio: boolean;
}

interface Props {
    open: boolean;
    scenes: AnimateAllScene[];
    model: AnimateAllModel;
    availableModels?: AnimateAllModel[];
    onModelChange?: (modelId: string) => void;
    availableCredits: number;
    requiredCredits: number;
    disabled?: boolean;
    muteAudio: boolean;
    onMuteAudioToggle: (value: boolean) => void;
    onClose: () => void;
    onAnimate: () => void;
}

// Animate All Scenes modal — matches the Korpi reference Casey flagged
// 2026-04-19. Scene thumbnails on the left, model + duration + audio
// toggle on the right, sticky footer with credit status + CTA.
export default function AnimateAllModal({
    open,
    scenes,
    model,
    availableModels,
    onModelChange,
    availableCredits,
    requiredCredits,
    disabled = false,
    muteAudio,
    onMuteAudioToggle,
    onClose,
    onAnimate,
}: Props) {
    if (!open) return null;

    const sceneDurationSec = Math.max(1, Math.round((scenes[0]?.duration_sec || 6) * 100) / 100);
    const modelDurationSec = model.fixed_duration_sec;
    const shortCredits = requiredCredits > availableCredits;

    return (
        <div
            className="fixed inset-0 z-50 flex items-center justify-center bg-black/75 p-4"
            onClick={onClose}
        >
            <div
                className="w-full max-w-5xl overflow-hidden rounded-2xl border border-white/10 bg-[#0b0b12] shadow-2xl"
                onClick={(e) => e.stopPropagation()}
            >
                {/* header */}
                <div className="flex items-start justify-between gap-4 border-b border-white/[0.06] p-5">
                    <div className="flex items-center gap-3">
                        <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-gradient-to-br from-violet-500/25 to-cyan-500/25 text-cyan-200">
                            <Video className="h-5 w-5" />
                        </span>
                        <div>
                            <h3 className="text-lg font-semibold text-white">Animate All Scenes ({scenes.length})</h3>
                            <p className="mt-0.5 text-xs text-violet-300/80">
                                Animating up to {model.parallel_limit} scene{model.parallel_limit !== 1 ? 's' : ''} in parallel based on your plan.
                            </p>
                        </div>
                    </div>
                    <button
                        type="button"
                        onClick={onClose}
                        className="rounded-lg p-1.5 text-gray-400 transition hover:bg-white/[0.06] hover:text-white"
                        aria-label="Close"
                    >
                        <X className="h-5 w-5" />
                    </button>
                </div>

                {/* body */}
                <div className="grid max-h-[70vh] grid-cols-1 overflow-y-auto md:grid-cols-[1fr_360px]">
                    {/* left: scene thumbnails */}
                    <div className="space-y-3 border-b border-white/[0.06] p-5 md:border-b-0 md:border-r">
                        <p className="text-xs font-semibold uppercase tracking-wider text-gray-500">
                            Scenes to Animate ({scenes.length})
                        </p>
                        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
                            {scenes.map((scene) => (
                                <div
                                    key={scene.index}
                                    className="relative aspect-[9/16] overflow-hidden rounded-lg border border-white/[0.08] bg-black/40"
                                >
                                    {scene.imageData ? (
                                        <img
                                            src={scene.imageData}
                                            alt={`Scene ${scene.index + 1}`}
                                            className="h-full w-full object-cover"
                                        />
                                    ) : (
                                        <div className="flex h-full w-full items-center justify-center text-[11px] text-gray-600">
                                            Scene {scene.index + 1}
                                        </div>
                                    )}
                                    <div className="absolute right-1 top-1 rounded bg-black/70 px-1.5 py-0.5 text-[9px] font-bold text-white">
                                        Scene {scene.index + 1}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>

                    {/* right: animation config */}
                    <div className="space-y-5 p-5">
                        <div>
                            <p className="mb-2 text-xs font-semibold uppercase tracking-wider text-gray-500">Animation Model</p>
                            {availableModels && availableModels.length > 1 && onModelChange ? (
                                <select
                                    value={model.id}
                                    onChange={(e) => onModelChange(e.target.value)}
                                    className="w-full rounded-lg border border-violet-500/40 bg-violet-500/10 px-3 py-2.5 text-sm font-semibold text-white outline-none transition focus:border-violet-400"
                                    style={{ colorScheme: 'dark' }}
                                >
                                    {availableModels.map((m) => (
                                        <option key={m.id} value={m.id} style={{ backgroundColor: '#0b0b12', color: '#ffffff' }}>
                                            {m.label} — {m.subtitle}
                                        </option>
                                    ))}
                                </select>
                            ) : (
                                <div className="rounded-lg border border-violet-500/40 bg-violet-500/10 p-3">
                                    <div className="flex items-start gap-2">
                                        <Video className="mt-0.5 h-4 w-4 shrink-0 text-violet-300" />
                                        <div className="min-w-0">
                                            <div className="text-sm font-semibold text-white truncate">{model.label}</div>
                                            <div className="mt-0.5 text-[11px] text-violet-300/80">{model.subtitle}</div>
                                        </div>
                                    </div>
                                </div>
                            )}
                            <p className="mt-2 text-[11px] text-gray-500">
                                {availableModels && availableModels.length > 1
                                    ? 'Switching here overrides the main Scenes stage model for this Animate All batch.'
                                    : 'Change model in the main Scenes stage. Animate-All uses whatever\'s currently picked.'}
                            </p>
                        </div>

                        <div>
                            <p className="mb-1 text-xs font-semibold uppercase tracking-wider text-gray-500">Duration</p>
                            <div className="rounded-lg border border-white/[0.08] bg-white/[0.02] px-3 py-2 text-sm text-gray-200">
                                Fixed at {modelDurationSec} seconds
                            </div>
                        </div>

                        <div className="rounded-lg border border-violet-500/30 bg-violet-500/[0.06] p-3">
                            <p className="text-[11px] font-semibold text-violet-200">Scene duration: {sceneDurationSec.toFixed(2)}s</p>
                            <p className="mt-1 text-[11px] leading-snug text-violet-200/70">
                                Set scene duration to a lower value and Studio will automatically slow the clip down to match — saves credits on models with fixed length.
                            </p>
                        </div>

                        {model.supports_audio && (
                            <div className="flex items-start justify-between gap-3 rounded-lg border border-white/[0.08] bg-white/[0.02] px-3 py-2.5">
                                <div>
                                    <p className="text-sm font-semibold text-white">Mute Audio</p>
                                    <p className="mt-0.5 text-[11px] text-gray-500">Remove model-generated audio (free).</p>
                                </div>
                                <button
                                    type="button"
                                    onClick={() => onMuteAudioToggle(!muteAudio)}
                                    className={`relative h-6 w-11 shrink-0 rounded-full border transition ${
                                        muteAudio
                                            ? 'border-violet-500 bg-violet-500/60'
                                            : 'border-white/[0.15] bg-white/[0.05]'
                                    }`}
                                    aria-pressed={muteAudio}
                                >
                                    <span
                                        className={`absolute top-0.5 h-5 w-5 rounded-full bg-white transition-all ${
                                            muteAudio ? 'left-[22px]' : 'left-0.5'
                                        }`}
                                    />
                                </button>
                            </div>
                        )}
                    </div>
                </div>

                {/* footer */}
                <div className="flex items-center justify-between gap-3 border-t border-white/[0.06] p-4">
                    <div className="text-[11px] text-gray-500">
                        {shortCredits ? (
                            <span className="text-amber-300">
                                Need {requiredCredits} animation credits — you have {availableCredits}. Top up to continue.
                            </span>
                        ) : (
                            <span>
                                {requiredCredits} animation credits required · {availableCredits} available
                            </span>
                        )}
                    </div>
                    <div className="flex items-center gap-2">
                        <button
                            type="button"
                            onClick={onClose}
                            className="rounded-lg px-4 py-2 text-sm font-semibold text-gray-300 transition hover:text-white"
                        >
                            Cancel
                        </button>
                        <button
                            type="button"
                            onClick={onAnimate}
                            disabled={disabled || shortCredits}
                            className="inline-flex items-center gap-1.5 rounded-lg bg-gradient-to-r from-violet-600 via-fuchsia-600 to-cyan-600 px-4 py-2 text-sm font-semibold text-white shadow-md shadow-violet-900/30 transition hover:opacity-95 disabled:cursor-not-allowed disabled:opacity-50"
                        >
                            <Video className="h-4 w-4" />
                            Animate All
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}
