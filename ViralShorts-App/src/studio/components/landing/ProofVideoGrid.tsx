import { useMemo, useState } from 'react';
import type { LandingProofChannel, LandingProofVideo } from '../../data/landingProofVideos';
import { landingProofChannels } from '../../data/landingProofVideos';
import ChannelFilterTabs from './ChannelFilterTabs';
import ProofVideoCard from './ProofVideoCard';
import ProofVideoModal from './ProofVideoModal';

const INITIAL_VISIBLE = 9;
const LOAD_MORE_COUNT = 9;

export default function ProofVideoGrid({
    videos,
}: {
    videos: LandingProofVideo[];
}) {
    const [activeChannel, setActiveChannel] = useState<LandingProofChannel>('all');
    const [visibleCount, setVisibleCount] = useState(INITIAL_VISIBLE);
    const [selectedVideo, setSelectedVideo] = useState<LandingProofVideo | null>(null);

    const filteredVideos = useMemo(() => {
        if (activeChannel === 'all') return videos;
        return videos.filter((video) => video.channel === activeChannel);
    }, [activeChannel, videos]);

    const visibleVideos = filteredVideos.slice(0, visibleCount);
    const activeLabel = landingProofChannels.find((channel) => channel.id === activeChannel)?.label || 'this channel';

    return (
        <div>
            <ChannelFilterTabs
                channels={landingProofChannels}
                active={activeChannel}
                onChange={(channel) => {
                    setActiveChannel(channel);
                    setVisibleCount(INITIAL_VISIBLE);
                }}
            />
            {visibleVideos.length > 0 ? (
                <div className="mt-6 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
                    {visibleVideos.map((video, index) => (
                        <ProofVideoCard key={video.youtubeVideoId} video={video} priority={index < 3} onOpen={setSelectedVideo} />
                    ))}
                </div>
            ) : (
                <div className="mt-6 rounded-lg border border-dashed border-white/[0.14] bg-white/[0.02] p-8 text-center">
                    <p className="text-sm font-semibold text-white">No public proof videos loaded for {activeLabel} yet.</p>
                    <p className="mx-auto mt-2 max-w-xl text-sm text-gray-400">
                        The landing system is ready for this channel. Add YouTube IDs to the proof manifest and they will appear without changing the page.
                    </p>
                </div>
            )}
            {visibleCount < filteredVideos.length && (
                <div className="mt-8 flex justify-center">
                    <button
                        type="button"
                        onClick={() => setVisibleCount((count) => count + LOAD_MORE_COUNT)}
                        className="rounded-lg border border-white/[0.1] bg-white/[0.04] px-5 py-3 text-sm font-semibold text-white transition hover:border-cyan-400/40 hover:bg-cyan-400/10"
                    >
                        Load more proof
                    </button>
                </div>
            )}
            <ProofVideoModal video={selectedVideo} onClose={() => setSelectedVideo(null)} />
        </div>
    );
}
