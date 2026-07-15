import { Play } from 'lucide-react';
import type { LandingProofVideo } from '../../data/landingProofVideos';

const formatViews = (views?: number) => {
    if (typeof views !== 'number') return '';
    if (views >= 1_000_000) return `${(views / 1_000_000).toFixed(1)}M views`;
    if (views >= 1_000) return `${(views / 1_000).toFixed(1)}K views`;
    return `${views} views`;
};

export default function ProofVideoCard({
    video,
    priority,
    onOpen,
}: {
    video: LandingProofVideo;
    priority?: boolean;
    onOpen: (video: LandingProofVideo) => void;
}) {
    const viewsCopy = video.viewsLabel || formatViews(video.views);

    return (
        <button
            type="button"
            onClick={() => onOpen(video)}
            className="group overflow-hidden rounded-lg border border-white/[0.08] bg-white/[0.03] text-left transition hover:-translate-y-0.5 hover:border-cyan-400/40 hover:bg-white/[0.05]"
        >
            <div className="relative aspect-video overflow-hidden bg-black">
                <img
                    src={video.thumbnailUrl}
                    alt=""
                    loading={priority ? 'eager' : 'lazy'}
                    decoding="async"
                    width="480"
                    height="360"
                    className="h-full w-full object-cover opacity-90 transition group-hover:scale-[1.03] group-hover:opacity-100"
                />
                <span className="absolute bottom-2 right-2 rounded bg-black/80 px-2 py-1 text-xs font-semibold text-white">
                    {video.duration}
                </span>
                <span className="absolute left-3 top-3 flex h-9 w-9 items-center justify-center rounded-full bg-black/75 text-white ring-1 ring-white/20 transition group-hover:bg-cyan-500">
                    <Play className="h-4 w-4 fill-current" />
                </span>
            </div>
            <div className="p-4">
                <div className="flex flex-wrap items-center gap-2 text-xs text-gray-400">
                    <span className="font-semibold text-cyan-200">{video.channelLabel}</span>
                    <span>{video.category}</span>
                    {viewsCopy && <span>{viewsCopy}</span>}
                </div>
                <h3 className="mt-2 line-clamp-2 min-h-[2.75rem] text-sm font-bold leading-snug text-white">
                    {video.title}
                </h3>
            </div>
        </button>
    );
}
