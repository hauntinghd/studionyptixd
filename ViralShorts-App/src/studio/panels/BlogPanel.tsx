import { useContext, useEffect, useMemo, useState } from 'react';
import { CheckCircle2, Loader2, Pencil, Trash2 } from 'lucide-react';
import { AuthContext, GENERATION_API } from '../shared';

type BlogPost = {
    slug: string;
    title: string;
    date: string;
    label: string;
    summary: string;
    bullets: string[];
    published: boolean;
};

const blankPost: BlogPost = {
    slug: '',
    title: '',
    date: new Date().toISOString().slice(0, 10),
    label: 'Product update',
    summary: '',
    bullets: [''],
    published: true,
};

export default function BlogPanel() {
    const { session } = useContext(AuthContext);
    const [posts, setPosts] = useState<BlogPost[]>([]);
    const [draft, setDraft] = useState<BlogPost>(blankPost);
    const [loading, setLoading] = useState(false);
    const [saving, setSaving] = useState(false);
    const [error, setError] = useState('');
    const [saved, setSaved] = useState('');

    const headers = useMemo(() => ({
        Authorization: `Bearer ${session?.access_token || ''}`,
        'Content-Type': 'application/json',
    }), [session?.access_token]);

    const load = async () => {
        if (!session) return;
        setLoading(true);
        setError('');
        try {
            const res = await fetch(`${GENERATION_API}/api/admin/blog/posts`, { headers });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(data?.detail || `HTTP ${res.status}`);
            setPosts(Array.isArray(data.posts) ? data.posts : []);
        } catch (err: any) {
            setError(err?.message || 'Could not load posts');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        void load();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [session?.access_token]);

    const save = async () => {
        if (!session) return;
        setSaving(true);
        setError('');
        setSaved('');
        try {
            const bullets = draft.bullets.map((b) => b.trim()).filter(Boolean);
            const res = await fetch(`${GENERATION_API}/api/admin/blog/posts`, {
                method: 'POST',
                headers,
                body: JSON.stringify({ ...draft, bullets }),
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(data?.detail || `HTTP ${res.status}`);
            setPosts(Array.isArray(data.posts) ? data.posts : []);
            setDraft(blankPost);
            setSaved('Post saved. It is now available to the landing page.');
        } catch (err: any) {
            setError(err?.message || 'Could not save post');
        } finally {
            setSaving(false);
        }
    };

    const remove = async (slug: string) => {
        if (!session || !slug) return;
        setError('');
        setSaved('');
        try {
            const res = await fetch(`${GENERATION_API}/api/admin/blog/posts/${encodeURIComponent(slug)}`, {
                method: 'DELETE',
                headers,
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(data?.detail || `HTTP ${res.status}`);
            setPosts(Array.isArray(data.posts) ? data.posts : []);
            setSaved('Post deleted.');
        } catch (err: any) {
            setError(err?.message || 'Could not delete post');
        }
    };

    return (
        <div className="space-y-6">
            <div>
                <p className="text-sm font-semibold uppercase tracking-[0.18em] text-cyan-300">Owner blog</p>
                <h1 className="mt-2 text-3xl font-bold text-white">Landing page update posts</h1>
                <p className="mt-2 max-w-3xl text-sm leading-6 text-gray-400">
                    Write public product notes manually. Published posts appear in the Studio updates section on the landing page.
                </p>
            </div>

            {error && <div className="rounded-xl border border-rose-500/25 bg-rose-500/10 px-4 py-3 text-sm text-rose-100">{error}</div>}
            {saved && (
                <div className="flex items-center gap-2 rounded-xl border border-emerald-500/25 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-100">
                    <CheckCircle2 className="h-4 w-4" />
                    {saved}
                </div>
            )}

            <section className="rounded-2xl border border-white/[0.08] bg-white/[0.025] p-5">
                <div className="grid gap-4 md:grid-cols-2">
                    <label className="block">
                        <span className="text-[11px] font-semibold uppercase tracking-wider text-gray-500">Title</span>
                        <input
                            value={draft.title}
                            onChange={(e) => setDraft((d) => ({ ...d, title: e.target.value }))}
                            className="mt-1 w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white outline-none focus:border-cyan-400"
                            placeholder="What shipped?"
                        />
                    </label>
                    <div className="grid grid-cols-2 gap-3">
                        <label className="block">
                            <span className="text-[11px] font-semibold uppercase tracking-wider text-gray-500">Date</span>
                            <input
                                value={draft.date}
                                onChange={(e) => setDraft((d) => ({ ...d, date: e.target.value }))}
                                className="mt-1 w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white outline-none focus:border-cyan-400"
                            />
                        </label>
                        <label className="block">
                            <span className="text-[11px] font-semibold uppercase tracking-wider text-gray-500">Label</span>
                            <input
                                value={draft.label}
                                onChange={(e) => setDraft((d) => ({ ...d, label: e.target.value }))}
                                className="mt-1 w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white outline-none focus:border-cyan-400"
                            />
                        </label>
                    </div>
                </div>
                <label className="mt-4 block">
                    <span className="text-[11px] font-semibold uppercase tracking-wider text-gray-500">Summary</span>
                    <textarea
                        value={draft.summary}
                        onChange={(e) => setDraft((d) => ({ ...d, summary: e.target.value }))}
                        rows={4}
                        className="mt-1 w-full resize-y rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white outline-none focus:border-cyan-400"
                        placeholder="Short public explanation for the landing page."
                    />
                </label>
                <div className="mt-4 space-y-2">
                    <span className="text-[11px] font-semibold uppercase tracking-wider text-gray-500">Bullets</span>
                    {draft.bullets.map((bullet, idx) => (
                        <input
                            key={idx}
                            value={bullet}
                            onChange={(e) => {
                                const next = [...draft.bullets];
                                next[idx] = e.target.value;
                                setDraft((d) => ({ ...d, bullets: next }));
                            }}
                            className="w-full rounded-lg border border-white/[0.08] bg-black/30 px-3 py-2 text-sm text-white outline-none focus:border-cyan-400"
                            placeholder={`Bullet ${idx + 1}`}
                        />
                    ))}
                    <button
                        type="button"
                        onClick={() => setDraft((d) => ({ ...d, bullets: [...d.bullets, ''] }))}
                        className="text-xs font-semibold text-cyan-300 hover:text-cyan-200"
                    >
                        Add bullet
                    </button>
                </div>
                <div className="mt-5 flex flex-wrap items-center justify-between gap-3">
                    <label className="flex items-center gap-2 text-sm text-gray-300">
                        <input
                            type="checkbox"
                            checked={draft.published}
                            onChange={(e) => setDraft((d) => ({ ...d, published: e.target.checked }))}
                        />
                        Published
                    </label>
                    <button
                        type="button"
                        disabled={saving || !draft.title.trim() || !draft.summary.trim()}
                        onClick={() => void save()}
                        className="rounded-xl bg-cyan-500 px-5 py-2.5 text-sm font-bold text-black transition hover:bg-cyan-300 disabled:opacity-50"
                    >
                        {saving ? 'Saving...' : 'Save post'}
                    </button>
                </div>
            </section>

            <section className="rounded-2xl border border-white/[0.08] bg-white/[0.025] p-5">
                <div className="mb-4 flex items-center justify-between">
                    <h2 className="text-lg font-bold text-white">Existing posts</h2>
                    {loading && <Loader2 className="h-4 w-4 animate-spin text-gray-500" />}
                </div>
                <div className="space-y-3">
                    {posts.map((post) => (
                        <article key={post.slug} className="rounded-xl border border-white/[0.08] bg-black/20 p-4">
                            <div className="flex flex-wrap items-start justify-between gap-3">
                                <div>
                                    <p className="text-xs font-bold uppercase tracking-[0.16em] text-cyan-300">{post.label} - {post.date}</p>
                                    <h3 className="mt-2 text-lg font-bold text-white">{post.title}</h3>
                                    <p className="mt-1 text-sm leading-6 text-gray-400">{post.summary}</p>
                                </div>
                                <div className="flex gap-2">
                                    <button
                                        type="button"
                                        onClick={() => setDraft({ ...post, bullets: post.bullets?.length ? post.bullets : [''] })}
                                        className="rounded-lg border border-white/[0.08] p-2 text-gray-300 hover:bg-white/[0.06]"
                                        title="Edit"
                                    >
                                        <Pencil className="h-4 w-4" />
                                    </button>
                                    <button
                                        type="button"
                                        onClick={() => void remove(post.slug)}
                                        className="rounded-lg border border-rose-500/20 p-2 text-rose-300 hover:bg-rose-500/10"
                                        title="Delete"
                                    >
                                        <Trash2 className="h-4 w-4" />
                                    </button>
                                </div>
                            </div>
                        </article>
                    ))}
                    {!posts.length && !loading && <p className="text-sm text-gray-500">No owner posts yet.</p>}
                </div>
            </section>
        </div>
    );
}
