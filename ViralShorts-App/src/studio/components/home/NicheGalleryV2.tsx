import { Film, Link2, Palette, Wand2, Zap } from 'lucide-react';
import { STUDIO_TOOLS, visibleNiches, type NicheId, type StudioNiche } from '../../lib/studioProduct';

const TOOL_ICONS: Record<string, typeof Zap> = {
    agent: Wand2,
    shorts: Zap,
    longform: Film,
    style: Palette,
    automate: Link2,
};

export default function NicheGalleryV2({
    isOwner,
    onPick,
}: {
    isOwner: boolean;
    onPick: (niche: StudioNiche) => void;
}) {
    const niches = visibleNiches(isOwner).filter((n) => n.id !== 'style_clone');
    return (
        <section>
            <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
                <div>
                    <h2 className="text-lg font-bold text-white">Trending niches</h2>
                    <p className="mt-1 text-sm text-gray-500">Each tile opens a distinct style pack and script lane.</p>
                </div>
            </div>
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
                {niches.map((n) => (
                    <button
                        key={n.id}
                        type="button"
                        onClick={() => onPick(n)}
                        className="group relative overflow-hidden rounded-2xl border border-white/[0.08] bg-zinc-900/80 text-left transition hover:border-violet-500/40 hover:shadow-xl hover:shadow-violet-950/30"
                    >
                        <div className="relative aspect-[16/10]">
                            <img
                                src={`/niche_thumbs/${n.id}.jpg`}
                                alt=""
                                loading="lazy"
                                onError={(e) => {
                                    (e.target as HTMLImageElement).style.display = 'none';
                                }}
                                className="absolute inset-0 h-full w-full object-cover transition duration-500 group-hover:scale-105"
                            />
                            <div className="absolute inset-0 bg-gradient-to-t from-black via-black/40 to-transparent" />
                            <div className="absolute inset-x-0 bottom-0 p-4">
                                <h3 className="text-base font-bold text-white">{n.title}</h3>
                                <p className="mt-1 text-xs text-gray-300">{n.desc}</p>
                            </div>
                            {n.badge && (
                                <span className="absolute right-3 top-3 rounded-full border border-white/15 bg-black/60 px-2.5 py-0.5 text-[9px] font-bold uppercase tracking-wider text-white backdrop-blur-sm">
                                    {n.badge}
                                </span>
                            )}
                        </div>
                    </button>
                ))}
            </div>
        </section>
    );
}

export function StudioToolsRow({
    onTool,
    isAdmin = false,
}: {
    onTool: (action: string) => void;
    isAdmin?: boolean;
}) {
    const tools = STUDIO_TOOLS.filter((t) => t.action !== 'agent' || isAdmin);
    return (
        <section>
            <h2 className="mb-3 text-lg font-bold text-white">Studio tools</h2>
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                {tools.map((tool) => {
                    const Icon = TOOL_ICONS[tool.id] || Zap;
                    return (
                    <button
                        key={tool.id}
                        type="button"
                        disabled={tool.comingSoon}
                        onClick={() => onTool(tool.action)}
                        className={`rounded-2xl border border-white/[0.08] bg-white/[0.02] p-4 text-left transition hover:border-violet-500/30 hover:bg-violet-500/5 ${
                            tool.comingSoon ? 'cursor-not-allowed opacity-70' : ''
                        }`}
                    >
                        <div className="flex items-start justify-between gap-2">
                            <Icon className="h-5 w-5 text-violet-300" />
                            {tool.badge && (
                                <span className="rounded border border-amber-500/30 bg-amber-500/10 px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-amber-200">
                                    {tool.badge}
                                </span>
                            )}
                        </div>
                        <h3 className="mt-3 text-sm font-bold text-white">{tool.title}</h3>
                        <p className="mt-1 text-xs leading-relaxed text-gray-400">{tool.desc}</p>
                    </button>
                    );
                })}
            </div>
        </section>
    );
}

export function StudioHomeHero({
    greeting,
    name,
    ownerPreview,
}: {
    greeting: string;
    name: string;
    ownerPreview?: boolean;
}) {
    return (
        <section className="relative overflow-hidden rounded-2xl border border-white/[0.06] bg-gradient-to-br from-violet-950/40 via-[#0c0c10] to-cyan-950/20 p-5 sm:p-6">
            <div className="relative z-10">
                <p className="text-xs font-semibold uppercase tracking-[0.2em] text-violet-300">Workspace</p>
                <h1 className="mt-2 text-2xl font-bold text-white sm:text-3xl">
                    {greeting}, {name}
                </h1>
                <p className="mt-2 max-w-2xl text-sm leading-relaxed text-gray-400">
                    {ownerPreview
                        ? 'Owner preview — all lanes open. Draft with fal, Ship with cinematic realism, Documentary for long-form.'
                        : 'Pick a niche, choose Draft or Ship, connect YouTube in Settings — we tell you what worked after upload.'}
                </p>
            </div>
            <div className="pointer-events-none absolute -right-8 -top-8 h-40 w-40 rounded-full bg-violet-600/20 blur-3xl" />
        </section>
    );
}

export type { NicheId };
