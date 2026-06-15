import { Crown, Plus, Trophy, Upload, Users } from 'lucide-react';
import { useContext, useEffect, useState, type ReactNode } from 'react';
import { AuthContext } from '../shared';
import {
    addStudioHubMessage,
    addStudioHubWin,
    defaultStudioHubState,
    loadStudioHubState,
    patchStudioHubState,
    type StudioHubChecklistItem,
    type StudioHubMessage,
    type StudioHubPowerSignal,
    type StudioHubWin,
} from '../lib/studioHubState';

export function NetworkPanel() {
    const { session } = useContext(AuthContext);
    const accessToken = session?.access_token || '';
    const [powerSignals, setPowerSignals] = useState<StudioHubPowerSignal[]>(defaultStudioHubState.power_signals);
    const [messages, setMessages] = useState<StudioHubMessage[]>(defaultStudioHubState.network_messages);
    const [message, setMessage] = useState('');
    const [saving, setSaving] = useState(false);

    useEffect(() => {
        if (!accessToken) return;
        loadStudioHubState(accessToken).then((state) => {
            setPowerSignals(state.power_signals);
            setMessages(state.network_messages);
        }).catch(() => {});
    }, [accessToken]);

    const sendMessage = async () => {
        const clean = message.trim();
        if (!clean) return;
        const optimistic = { id: `local-${Date.now()}`, name: displayName(session?.user?.email), body: clean, created_at: new Date().toISOString() };
        setMessages((items) => [...items, optimistic]);
        setMessage('');
        if (!accessToken) return;
        try {
            setSaving(true);
            const state = await addStudioHubMessage(accessToken, clean, optimistic.name);
            setMessages(state.network_messages);
        } finally {
            setSaving(false);
        }
    };

    return (
        <HubPageShell eyebrow="Network" title="One room for operators, creators, clients, and teams.">
            <section className="grid gap-4 xl:grid-cols-[1fr_360px]">
                <div className="rounded-2xl border border-white/[0.08] bg-[#102033]">
                    <div className="border-b border-white/[0.08] px-5 py-4">
                        <h2 className="text-base font-bold text-white"># studio-network</h2>
                        <p className="mt-1 text-sm text-gray-400">Studios organize work. Network is where people talk, share wins, and collaborate.</p>
                    </div>
                    <div className="space-y-3 p-5">
                        {messages.map((item) => <NetworkPost key={item.id} name={item.name || 'Operator'} body={item.body} />)}
                    </div>
                    <div className="border-t border-white/[0.08] p-4">
                        <div className="flex items-center gap-2 rounded-xl bg-[#07111b] px-3 py-2">
                            <Plus className="h-4 w-4 text-cyan-200" />
                            <input
                                value={message}
                                onChange={(e) => setMessage(e.target.value)}
                                onKeyDown={(e) => {
                                    if (e.key === 'Enter') void sendMessage();
                                }}
                                className="h-9 min-w-0 flex-1 bg-transparent text-sm text-white outline-none placeholder:text-gray-500"
                                placeholder="Message # studio-network"
                            />
                            <button type="button" disabled={saving} onClick={() => void sendMessage()} className="rounded-lg bg-cyan-500 px-3 py-2 text-xs font-bold text-white disabled:opacity-50">
                                Send
                            </button>
                        </div>
                    </div>
                </div>
                <aside className="rounded-2xl border border-white/[0.08] bg-[#102033] p-5">
                    <div className="flex items-center gap-2">
                        <Users className="h-5 w-5 text-cyan-200" />
                        <h2 className="text-base font-bold text-white">Level up</h2>
                    </div>
                    <p className="mt-2 text-sm leading-6 text-gray-400">There are no manual titles here. Useful activity raises your power level.</p>
                    <div className="mt-4 space-y-3">
                        {powerSignals.map((signal) => (
                            <div key={`${signal.name}-${signal.xp}`} className="rounded-xl border border-white/[0.08] bg-[#07111b] p-3">
                                <div className="flex items-start justify-between gap-3">
                                    <p className="text-sm font-bold text-white">{signal.name}</p>
                                    <span className="shrink-0 rounded-full bg-cyan-500/10 px-2 py-0.5 text-[10px] font-bold text-cyan-100">+{signal.xp} XP</span>
                                </div>
                                <p className="mt-1 text-xs leading-5 text-gray-400">{signal.detail}</p>
                            </div>
                        ))}
                    </div>
                </aside>
            </section>
        </HubPageShell>
    );
}

export function WinsPanel() {
    const { session } = useContext(AuthContext);
    const accessToken = session?.access_token || '';
    const [wins, setWins] = useState<StudioHubWin[]>(defaultStudioHubState.wins);
    const [message, setMessage] = useState('');
    const [image, setImage] = useState('');
    const [imageName, setImageName] = useState('');
    const [error, setError] = useState('');

    useEffect(() => {
        if (!accessToken) return;
        loadStudioHubState(accessToken).then((state) => setWins(state.wins)).catch(() => {});
    }, [accessToken]);

    const saveWin = async () => {
        const clean = message.trim();
        setError('');
        if (!clean) {
            setError('Write the win before posting.');
            return;
        }
        if (!image) {
            setError('Attach a screenshot before posting a win.');
            return;
        }
        const title = clean.split('\n').find(Boolean)?.slice(0, 120) || 'Proof win';
        const optimistic = { id: `local-${Date.now()}`, title, body: clean, image_url: image, image_name: imageName, created_at: new Date().toISOString() };
        setWins((items) => [optimistic, ...items]);
        setMessage('');
        setImage('');
        setImageName('');
        if (accessToken) {
            const state = await addStudioHubWin(accessToken, optimistic.title, optimistic.body, optimistic.image_url, optimistic.image_name || '');
            setWins(state.wins);
        }
    };

    const selectImage = (file?: File | null) => {
        if (!file) return;
        if (!file.type.startsWith('image/')) {
            setError('Use a PNG, JPG, or other image file.');
            return;
        }
        const reader = new FileReader();
        reader.onload = () => {
            setImage(String(reader.result || ''));
            setImageName(file.name);
            setError('');
        };
        reader.readAsDataURL(file);
    };

    return (
        <HubPageShell eyebrow="Wins" title="Proof of work, growth, shipped outcomes.">
            <div className="grid gap-4 md:grid-cols-3">
                <Metric label="Videos shipped" value={String(wins.length)} />
                <Metric label="Winning tests" value={String(wins.filter((w) => /test|winner|won/i.test(`${w.title} ${w.body || ''}`)).length)} />
                <Metric label="Proof screenshots" value={String(wins.filter((w) => w.image_url).length)} />
            </div>
            <section className="rounded-2xl border border-white/[0.08] bg-[#102033] p-5">
                <div className="flex items-center gap-2">
                    <Trophy className="h-5 w-5 text-amber-300" />
                    <h2 className="text-base font-bold text-white">Wins feed</h2>
                </div>
                <div className="mt-4 rounded-xl border border-white/[0.08] bg-[#07111b] p-3">
                    <textarea value={message} onChange={(e) => setMessage(e.target.value)} placeholder="Post the win. Add the result, context, and what changed." className="min-h-24 w-full resize-y rounded-lg border border-white/[0.08] bg-black/20 px-3 py-3 text-sm text-white outline-none placeholder:text-gray-600" />
                    <div className="mt-3 flex flex-wrap items-center gap-3">
                        <label className="inline-flex cursor-pointer items-center gap-2 rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 py-2 text-xs font-bold text-cyan-100 hover:bg-white/[0.06]">
                            <Upload className="h-4 w-4" />
                            Attach screenshot
                            <input type="file" accept="image/png,image/jpeg,image/webp,image/gif" className="hidden" onChange={(e) => selectImage(e.target.files?.[0])} />
                        </label>
                        {imageName && <span className="text-xs text-gray-400">{imageName}</span>}
                        <button type="button" onClick={() => void saveWin()} className="ml-auto rounded-lg bg-cyan-600 px-4 py-2 text-xs font-bold text-white hover:bg-cyan-500">Send win</button>
                    </div>
                    {image && <img src={image} alt="Win proof preview" className="mt-3 max-h-72 rounded-xl border border-white/[0.08] object-contain" />}
                    {error && <p className="mt-3 text-sm text-amber-200">{error}</p>}
                </div>
                <div className="mt-4 space-y-2">
                    {wins.length === 0 && <p className="text-sm text-gray-400">Wins require proof: a short message plus a screenshot.</p>}
                    {wins.map((win) => (
                        <article key={win.id} className="rounded-xl border border-white/[0.06] bg-[#07111b] p-3">
                            <p className="text-sm font-bold text-white">{win.title}</p>
                            {win.body && <p className="mt-1 whitespace-pre-wrap text-sm leading-6 text-gray-300">{win.body}</p>}
                            {win.image_url && <img src={win.image_url} alt={win.image_name || 'Win proof'} className="mt-3 max-h-96 rounded-xl border border-white/[0.08] object-contain" />}
                        </article>
                    ))}
                </div>
            </section>
        </HubPageShell>
    );
}

export function ChecklistPanel() {
    const { session } = useContext(AuthContext);
    const accessToken = session?.access_token || '';
    const [items, setItems] = useState<StudioHubChecklistItem[]>(defaultStudioHubState.checklist);

    useEffect(() => {
        if (!accessToken) return;
        loadStudioHubState(accessToken).then((state) => setItems(state.checklist)).catch(() => {});
    }, [accessToken]);

    const toggle = (id: string) => {
        const next = items.map((item) => item.id === id ? { ...item, done: !item.done } : item);
        setItems(next);
        if (accessToken) patchStudioHubState(accessToken, { checklist: next }).catch(() => {});
    };

    return (
        <HubPageShell eyebrow="Checklist" title="Simple operating checklist for every creator.">
            <section className="rounded-2xl border border-white/[0.08] bg-[#102033] p-5">
                <div className="space-y-2">
                    {items.map((item) => (
                        <label key={item.id} className="flex items-center gap-3 rounded-xl border border-white/[0.06] bg-[#07111b] px-4 py-3 text-sm text-gray-200">
                            <input type="checkbox" checked={item.done} onChange={() => toggle(item.id)} className="h-4 w-4 accent-cyan-400" />
                            {item.label}
                        </label>
                    ))}
                </div>
            </section>
        </HubPageShell>
    );
}

function HubPageShell({ eyebrow, title, children }: { eyebrow: string; title: string; children: ReactNode }) {
    return (
        <div className="mx-auto max-w-7xl space-y-5">
            <section className="rounded-2xl border border-white/[0.08] bg-[radial-gradient(circle_at_14%_0%,rgba(6,182,212,0.16),transparent_34%),linear-gradient(135deg,rgba(8,47,73,0.28),rgba(9,9,11,0.96)_48%,rgba(46,16,101,0.28))] p-6 shadow-2xl shadow-black/30">
                <p className="text-xs font-bold uppercase tracking-[0.22em] text-cyan-300">{eyebrow}</p>
                <h1 className="mt-2 max-w-3xl text-3xl font-bold text-white">{title}</h1>
            </section>
            {children}
        </div>
    );
}

function NetworkPost({ name, body }: { name: string; body: string }) {
    return (
        <article className="rounded-xl bg-[#162a3d] p-4">
            <div className="flex items-start gap-3">
                <div className="flex h-9 w-9 items-center justify-center rounded-full bg-cyan-500/15 text-xs font-bold text-cyan-100">{name.slice(0, 1)}</div>
                <div>
                    <p className="text-sm font-bold text-white">{name}</p>
                    <p className="mt-1 text-sm leading-6 text-gray-300">{body}</p>
                </div>
            </div>
        </article>
    );
}

function Metric({ label, value }: { label: string; value: string }) {
    return (
        <div className="rounded-2xl border border-white/[0.08] bg-[#102033] p-5">
            <Crown className="h-5 w-5 text-cyan-200" />
            <p className="mt-4 text-xs font-bold uppercase tracking-[0.18em] text-gray-500">{label}</p>
            <p className="mt-2 text-3xl font-bold text-white">{value}</p>
        </div>
    );
}

function displayName(email?: string | null): string {
    const clean = String(email || '').trim();
    return clean ? clean.split('@', 1)[0] : 'Operator';
}
