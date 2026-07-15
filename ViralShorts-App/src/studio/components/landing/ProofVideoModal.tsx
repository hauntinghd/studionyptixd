import { X } from 'lucide-react';
import { useEffect } from 'react';
import type { LandingProofVideo } from '../../data/landingProofVideos';

export default function ProofVideoModal({
    video,
    onClose,
}: {
    video: LandingProofVideo | null;
    onClose: () => void;
}) {
    useEffect(() => {
        if (!video) return;
        const onKeyDown = (event: KeyboardEvent) => {
            if (event.key === 'Escape') onClose();
        };
        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [onClose, video]);

    if (!video) return null;

    return (
        <div className="fixed inset-0 z-[80] flex items-center justify-center bg-black/82 p-4 backdrop-blur-sm" role="dialog" aria-modal="true">
            <div className="w-full max-w-4xl overflow-hidden rounded-lg border border-white/[0.12] bg-[#101014] shadow-2xl">
                <div className="flex items-start justify-between gap-4 border-b border-white/[0.08] px-4 py-3">
                    <div>
                        <p className="text-xs font-semibold uppercase tracking-[0.16em] text-cyan-300">{video.channelLabel}</p>
                        <h2 className="mt-1 text-base font-bold text-white sm:text-lg">{video.title}</h2>
                    </div>
                    <button
                        type="button"
                        onClick={onClose}
                        className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-white/[0.08] bg-white/[0.03] text-gray-300 hover:bg-white/[0.08] hover:text-white"
                        aria-label="Close video"
                    >
                        <X className="h-4 w-4" />
                    </button>
                </div>
                <div className="aspect-video bg-black">
                    <iframe
                        title={video.title}
                        src={`https://www.youtube-nocookie.com/embed/${video.youtubeVideoId}?autoplay=1&rel=0`}
                        className="h-full w-full"
                        allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                        allowFullScreen
                    />
                </div>
            </div>
        </div>
    );
}
