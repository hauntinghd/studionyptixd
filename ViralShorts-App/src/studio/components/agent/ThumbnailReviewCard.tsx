import { useContext, useState } from 'react';
import { Download, Image as ImageIcon } from 'lucide-react';
import { AuthContext } from '../../shared';
import { downloadStudioAsset } from '../../lib/agentProduction';
import { useAuthenticatedMediaUrls } from '../../hooks/useAuthenticatedMedia';

export type ThumbnailReview = {
    review_id?: string;
    job_id?: string;
    title?: string;
    candidate_urls?: string[];
    feedback?: string;
    updated_at?: number;
};

function thumbnailFilename(title: string, index: number) {
    const slug = String(title || 'thumbnail')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '')
        .slice(0, 48) || 'thumbnail';
    return `${slug}-candidate-${index + 1}.png`;
}

/** Plan-only packaging proof. This is intentionally not a production card. */
export default function ThumbnailReviewCard({ review }: { review: ThumbnailReview }) {
    const { session } = useContext(AuthContext);
    const [downloading, setDownloading] = useState<number | null>(null);
    const [downloadError, setDownloadError] = useState('');
    const urls = (review.candidate_urls || []).filter(Boolean);
    const token = session?.access_token || '';
    const thumbnailMedia = useAuthenticatedMediaUrls(urls, token, urls.length > 0);
    if (!urls.length) return null;

    const handleDownload = async (url: string, index: number) => {
        if (!token || downloading != null) return;
        setDownloading(index);
        setDownloadError('');
        try {
            await downloadStudioAsset(url, token, thumbnailFilename(review.title || 'thumbnail', index));
        } catch (e) {
            setDownloadError((e as Error).message);
        } finally {
            setDownloading(null);
        }
    };

    return (
        <section className="mt-3 overflow-hidden rounded-2xl border border-violet-400/25 bg-violet-500/[0.06]">
            <header className="flex items-center gap-2 border-b border-violet-300/15 px-4 py-3">
                <ImageIcon className="h-4 w-4 text-violet-300" />
                <div>
                    <p className="text-sm font-semibold text-violet-50">Thumbnail review</p>
                    <p className="text-[11px] text-violet-100/65">{review.title || 'Current plan'} · packaging only, no video render started</p>
                </div>
            </header>
            <div className="grid grid-cols-1 gap-3 p-3 sm:grid-cols-3">
                {urls.map((url, index) => (
                    <div
                        key={`${url}-${index}`}
                        className="overflow-hidden rounded-xl border border-white/10 bg-black/35"
                    >
                        {thumbnailMedia.urls[index] ? (
                            <img
                                src={thumbnailMedia.urls[index]}
                                alt={`Thumbnail candidate ${index + 1}`}
                                className="aspect-video w-full object-cover"
                                loading="lazy"
                            />
                        ) : (
                            <div className="aspect-video w-full animate-pulse bg-white/5" />
                        )}
                        <div className="flex items-center justify-between gap-2 px-2.5 py-2">
                            <p className="text-xs font-medium text-violet-50">Candidate {index + 1}</p>
                            <button
                                type="button"
                                disabled={!token || downloading === index}
                                onClick={() => void handleDownload(url, index)}
                                className="inline-flex items-center gap-1 rounded-md border border-violet-300/25 bg-violet-500/15 px-2 py-1 text-[10px] font-semibold text-violet-50 transition hover:bg-violet-500/25 disabled:opacity-50"
                            >
                                <Download className="h-3 w-3" />
                                {downloading === index ? 'Saving…' : 'Download'}
                            </button>
                        </div>
                    </div>
                ))}
            </div>
            {downloadError ? (
                <p className="px-4 pb-2 text-xs text-red-300">{downloadError}</p>
            ) : null}
            {thumbnailMedia.error ? (
                <p className="px-4 pb-2 text-xs text-red-300">{thumbnailMedia.error}</p>
            ) : null}
            <p className="px-4 pb-3 text-xs text-violet-100/70">Reply naturally to revise these — for example: “make candidate 2 darker and focus on the siege.”</p>
        </section>
    );
}
