/**
 * CatalystReferencesSection — paste viral YouTube URLs as inspiration refs.
 *
 * Per Casey 2026-05-06: 'guarantee virality' lever. User pastes any YT URL,
 * yt-dlp pulls metadata, the reference is tagged to a Studio channel
 * (Empire Magnates, Lacuna, ZeroTier, etc.) so when the user generates
 * scripts for that channel, the reference titles + descriptions thread
 * into the Grok system prompt as 'mimic these patterns' context.
 *
 * Phase 1 (this component): metadata-only. Add / list / delete / edit notes.
 * Phase 2 (deferred): one-click 'Analyze' → Whisper transcript + keyframe
 * vision decode → pattern_summary jsonb that auto-injects into prompts.
 */
import { useCallback, useContext, useEffect, useState } from 'react';
import {
    BookmarkPlus, Eye, Heart, Link2, Loader2, MessageSquare, Plus, Trash2,
} from 'lucide-react';
import { AuthContext } from '../shared';

interface ReferenceRow {
    id: string;
    user_id: string;
    channel_key: string;
    yt_video_id: string;
    yt_url: string;
    title: string;
    description?: string;
    tags?: string[];
    yt_channel_id?: string;
    channel_title?: string;
    duration_sec?: number;
    view_count?: number;
    like_count?: number;
    comment_count?: number;
    thumbnail_url?: string;
    upload_date?: string;
    user_notes?: string;
    created_at: string;
}

const CHANNEL_KEY_OPTIONS: { key: string; label: string }[] = [
    { key: '',                   label: 'All channels (any generation)' },
    { key: 'empire_magnates',    label: 'Empire Magnates' },
    { key: 'lacuna',             label: 'We Are Lacuna' },
    { key: 'history_rewind',     label: 'History Rewind' },
    { key: 'pb_live',            label: 'PB Live' },
    { key: 'hidden_cortex',      label: 'Hidden Cortex' },
    { key: 'lofi_radio',         label: 'Lo-Fi Radio' },
    { key: 'cryptic_science',    label: 'Cryptic Science (shorts)' },
    { key: 'zerotier',           label: 'ZeroTier (shorts)' },
    { key: 'lexi_manhwa',        label: 'Lexi Manhwa (shorts)' },
];

export default function CatalystReferencesSection() {
    const { session } = useContext(AuthContext);
    const accessToken = session?.access_token || '';

    const [refs, setRefs] = useState<ReferenceRow[]>([]);
    const [loading, setLoading] = useState(false);
    const [filter, setFilter] = useState<string>('');         // channel_key filter, '' = all
    const [url, setUrl] = useState('');
    const [channelKey, setChannelKey] = useState<string>('');
    const [notes, setNotes] = useState('');
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState('');

    const load = useCallback(async () => {
        if (!accessToken) return;
        setLoading(true);
        try {
            const qs = filter ? `?channel_key=${encodeURIComponent(filter)}` : '';
            const r = await fetch(`/api/catalyst/references${qs}`, {
                headers: { Authorization: `Bearer ${accessToken}` },
            });
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            const d = await r.json();
            setRefs(Array.isArray(d.references) ? d.references : []);
        } catch (e) {
            setError(`load failed: ${(e as Error).message}`);
        } finally {
            setLoading(false);
        }
    }, [accessToken, filter]);

    useEffect(() => { void load(); }, [load]);

    const addRef = useCallback(async () => {
        if (!url.trim()) { setError('paste a YouTube URL'); return; }
        if (!accessToken) { setError('not signed in'); return; }
        setBusy(true);
        setError('');
        try {
            const r = await fetch('/api/catalyst/references', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${accessToken}`,
                },
                body: JSON.stringify({
                    url: url.trim(),
                    channel_key: channelKey,
                    notes: notes.trim(),
                }),
            });
            if (!r.ok) {
                const txt = await r.text().catch(() => '');
                throw new Error(`${r.status} ${txt.slice(0, 200)}`);
            }
            setUrl('');
            setNotes('');
            await load();
        } catch (e) {
            setError(`add failed: ${(e as Error).message}`);
        } finally {
            setBusy(false);
        }
    }, [url, channelKey, notes, accessToken, load]);

    const removeRef = useCallback(async (id: string) => {
        if (!accessToken) return;
        if (!window.confirm('Remove this reference?')) return;
        try {
            const r = await fetch(`/api/catalyst/references/${id}`, {
                method: 'DELETE',
                headers: { Authorization: `Bearer ${accessToken}` },
            });
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            setRefs((prev) => prev.filter((rf) => rf.id !== id));
        } catch (e) {
            setError(`delete failed: ${(e as Error).message}`);
        }
    }, [accessToken]);

    const totalRefs = refs.length;
    const totalViews = refs.reduce((s, r) => s + (r.view_count || 0), 0);

    return (
        <section className="rounded-lg border border-zinc-800 bg-zinc-950 p-4 flex flex-col gap-4">
            <header className="flex items-center justify-between">
                <div>
                    <h3 className="text-base font-bold text-white flex items-center gap-2">
                        <BookmarkPlus className="h-5 w-5 text-violet-400" />
                        Reference Videos
                    </h3>
                    <p className="text-xs text-zinc-400 mt-0.5">
                        Paste viral YouTube URLs. Studio threads them into Grok prompts so
                        scripts mimic patterns that already work.
                    </p>
                </div>
                <div className="text-xs text-zinc-400 text-right">
                    <div>{totalRefs} reference{totalRefs === 1 ? '' : 's'}</div>
                    {totalViews > 0 && (
                        <div className="text-[10px] text-zinc-500">{totalViews.toLocaleString()} total views</div>
                    )}
                </div>
            </header>

            <div className="rounded-md border border-zinc-800 bg-zinc-900 p-3 flex flex-col gap-2">
                <label className="text-[11px] font-semibold text-zinc-300 uppercase tracking-wide">
                    Add reference
                </label>
                <div className="flex flex-col sm:flex-row gap-2">
                    <input
                        type="text"
                        value={url}
                        onChange={(e) => setUrl(e.target.value)}
                        placeholder="https://youtube.com/shorts/... or watch?v=..."
                        className="flex-1 rounded-md bg-zinc-950 border border-zinc-800 px-3 py-2 text-sm text-white placeholder-zinc-600 focus:border-violet-500 outline-none"
                        onKeyDown={(e) => { if (e.key === 'Enter' && !busy) void addRef(); }}
                    />
                    <select
                        value={channelKey}
                        onChange={(e) => setChannelKey(e.target.value)}
                        className="rounded-md bg-zinc-950 border border-zinc-800 px-3 py-2 text-sm text-white focus:border-violet-500 outline-none"
                    >
                        {CHANNEL_KEY_OPTIONS.map((o) => (
                            <option key={o.key || 'all'} value={o.key}>{o.label}</option>
                        ))}
                    </select>
                </div>
                <textarea
                    value={notes}
                    onChange={(e) => setNotes(e.target.value)}
                    placeholder="Optional notes — why this video matters (hook style, pacing, lighting…)"
                    rows={2}
                    className="rounded-md bg-zinc-950 border border-zinc-800 px-3 py-2 text-xs text-white placeholder-zinc-600 focus:border-violet-500 outline-none resize-none"
                />
                <button
                    onClick={addRef}
                    disabled={busy || !url.trim()}
                    className="self-end rounded-md bg-violet-500 hover:bg-violet-600 disabled:bg-zinc-800 disabled:text-zinc-500 px-4 py-2 text-sm font-semibold text-white flex items-center gap-2"
                >
                    {busy ? (
                        <><Loader2 className="h-4 w-4 animate-spin" /> Fetching metadata…</>
                    ) : (
                        <><Plus className="h-4 w-4" /> Add reference</>
                    )}
                </button>
                {error && (
                    <div className="text-xs text-rose-300 mt-1">{error}</div>
                )}
            </div>

            <div className="flex items-center gap-2 text-xs text-zinc-400">
                <span>Filter:</span>
                <select
                    value={filter}
                    onChange={(e) => setFilter(e.target.value)}
                    className="rounded-md bg-zinc-950 border border-zinc-800 px-2 py-1 text-xs text-white focus:border-violet-500 outline-none"
                >
                    {CHANNEL_KEY_OPTIONS.map((o) => (
                        <option key={o.key || 'all'} value={o.key}>{o.label}</option>
                    ))}
                </select>
                {loading && <Loader2 className="h-3 w-3 animate-spin text-zinc-500" />}
            </div>

            {refs.length === 0 && !loading && (
                <div className="rounded-md border border-zinc-800 bg-zinc-900 p-4 text-center text-xs text-zinc-500">
                    No references yet. Paste a YouTube URL above to start your inspiration library.
                </div>
            )}

            <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                {refs.map((r) => (
                    <ReferenceCard key={r.id} ref_={r} onRemove={() => removeRef(r.id)} />
                ))}
            </div>
        </section>
    );
}

function ReferenceCard({ ref_, onRemove }: { ref_: ReferenceRow; onRemove: () => void }) {
    const dur = ref_.duration_sec || 0;
    const durStr = dur >= 60 ? `${Math.floor(dur / 60)}m ${dur % 60}s` : `${dur}s`;
    const channelLabel = (CHANNEL_KEY_OPTIONS.find((o) => o.key === ref_.channel_key)?.label) || ref_.channel_key || 'all channels';
    return (
        <div className="rounded-md border border-zinc-800 bg-zinc-900 p-3 flex gap-3">
            {ref_.thumbnail_url ? (
                <a href={ref_.yt_url} target="_blank" rel="noopener noreferrer"
                   className="block w-28 flex-shrink-0">
                    <img
                        src={ref_.thumbnail_url}
                        alt={ref_.title}
                        className="w-full aspect-video object-cover rounded-sm bg-zinc-800"
                        loading="lazy"
                    />
                </a>
            ) : (
                <div className="w-28 flex-shrink-0 aspect-video bg-zinc-800 rounded-sm flex items-center justify-center">
                    <Link2 className="h-5 w-5 text-zinc-600" />
                </div>
            )}
            <div className="flex-1 min-w-0 flex flex-col gap-1">
                <div className="flex items-start justify-between gap-2">
                    <a href={ref_.yt_url} target="_blank" rel="noopener noreferrer"
                       className="text-sm font-semibold text-white hover:text-violet-300 line-clamp-2"
                       title={ref_.title}>
                        {ref_.title || ref_.yt_video_id}
                    </a>
                    <button
                        onClick={onRemove}
                        className="text-zinc-500 hover:text-rose-300 flex-shrink-0"
                        title="Remove"
                    >
                        <Trash2 className="h-3.5 w-3.5" />
                    </button>
                </div>
                <div className="text-[10px] text-zinc-400 truncate" title={ref_.channel_title || ''}>
                    {ref_.channel_title || '?'}
                </div>
                <div className="flex items-center gap-3 text-[10px] text-zinc-500">
                    {ref_.view_count !== undefined && (
                        <span className="flex items-center gap-1">
                            <Eye className="h-3 w-3" /> {ref_.view_count.toLocaleString()}
                        </span>
                    )}
                    {ref_.like_count !== undefined && ref_.like_count > 0 && (
                        <span className="flex items-center gap-1">
                            <Heart className="h-3 w-3" /> {ref_.like_count.toLocaleString()}
                        </span>
                    )}
                    {ref_.comment_count !== undefined && ref_.comment_count > 0 && (
                        <span className="flex items-center gap-1">
                            <MessageSquare className="h-3 w-3" /> {ref_.comment_count.toLocaleString()}
                        </span>
                    )}
                    <span>{durStr}</span>
                </div>
                <div className="flex items-center gap-1.5 mt-0.5">
                    <span className="rounded bg-violet-500/10 border border-violet-500/30 text-violet-300 px-1.5 py-0.5 text-[9px] font-mono">
                        {channelLabel}
                    </span>
                </div>
                {ref_.user_notes && (
                    <div className="text-[10px] text-zinc-400 italic mt-0.5 line-clamp-2" title={ref_.user_notes}>
                        {ref_.user_notes}
                    </div>
                )}
            </div>
        </div>
    );
}
