import type { LandingProofChannel } from '../../data/landingProofVideos';

export default function ChannelFilterTabs({
    channels,
    active,
    onChange,
}: {
    channels: Array<{ id: LandingProofChannel; label: string }>;
    active: LandingProofChannel;
    onChange: (channel: LandingProofChannel) => void;
}) {
    return (
        <div className="flex gap-2 overflow-x-auto pb-2">
            {channels.map((channel) => {
                const selected = channel.id === active;
                return (
                    <button
                        key={channel.id}
                        type="button"
                        onClick={() => onChange(channel.id)}
                        className={[
                            'shrink-0 rounded-lg border px-4 py-2 text-sm font-semibold transition',
                            selected
                                ? 'border-cyan-400/50 bg-cyan-400/12 text-cyan-100'
                                : 'border-white/[0.08] bg-white/[0.03] text-gray-300 hover:border-white/[0.16] hover:text-white',
                        ].join(' ')}
                    >
                        {channel.label}
                    </button>
                );
            })}
        </div>
    );
}
