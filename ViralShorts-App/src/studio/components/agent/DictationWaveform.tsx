/** Live mic waveform bars for Studio Agent dictation. */
import { DICTATION_BAR_COUNT } from '../../hooks/useSpeechDictation';

export default function DictationWaveform({
    levels,
    active,
    className = '',
}: {
    levels?: number[];
    active: boolean;
    className?: string;
}) {
    const bars =
        levels && levels.length
            ? levels
            : Array.from({ length: DICTATION_BAR_COUNT }, () => (active ? 0.15 : 0.06));

    return (
        <div
            className={`flex h-7 items-end justify-center gap-[2px] px-1 ${className}`}
            aria-hidden
            title={active ? 'Listening' : undefined}
        >
            {bars.map((level, i) => {
                const h = active
                    ? Math.max(3, Math.round(4 + Math.min(1, level) * 20))
                    : 3;
                return (
                    <span
                        key={i}
                        className={`w-[2.5px] rounded-full transition-[height,background-color,opacity] duration-75 ${
                            active
                                ? 'bg-gradient-to-t from-rose-600 to-rose-300 opacity-95'
                                : 'bg-white/15 opacity-40'
                        }`}
                        style={{ height: `${h}px` }}
                    />
                );
            })}
        </div>
    );
}
