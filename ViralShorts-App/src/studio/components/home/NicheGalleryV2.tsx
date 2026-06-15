import { ArrowRight, BadgeDollarSign, BookOpen, Film, GraduationCap, LineChart, Link2, Palette, ShieldCheck, Trophy, Wand2, Zap } from 'lucide-react';
import { STUDIO_TOOLS, visibleNiches, type NicheId, type StudioNiche } from '../../lib/studioProduct';

const TOOL_ICONS: Record<string, typeof Zap> = {
    agent: Wand2,
    campus: GraduationCap,
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
                        className="group relative overflow-hidden rounded-2xl border border-white/[0.08] bg-zinc-900/80 text-left shadow-sm shadow-black/30 transition hover:border-violet-500/40 hover:shadow-xl hover:shadow-violet-950/30"
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
        <section className="space-y-4">
            <div>
                <h2 className="text-lg font-bold text-white">Studio tools</h2>
                <p className="mt-1 text-sm text-gray-500">Production lanes for packaging, rendering, clipping, and automation.</p>
            </div>
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                {tools.map((tool) => {
                    const Icon = TOOL_ICONS[tool.id] || Zap;
                    return (
                        <button
                            key={tool.id}
                            type="button"
                            disabled={tool.comingSoon}
                            onClick={() => onTool(tool.action)}
                            className={`group rounded-2xl border border-white/[0.08] bg-gradient-to-br from-white/[0.045] to-white/[0.015] p-4 text-left shadow-sm shadow-black/30 transition hover:border-violet-500/35 hover:bg-violet-500/5 hover:shadow-xl hover:shadow-violet-950/20 ${
                                tool.comingSoon ? 'cursor-not-allowed opacity-70' : ''
                            }`}
                        >
                            <div className="flex items-start justify-between gap-2">
                                <div className="flex h-9 w-9 items-center justify-center rounded-xl border border-white/[0.08] bg-black/30">
                                    <Icon className="h-4 w-4 text-violet-300" />
                                </div>
                                {tool.badge && (
                                    <span className="rounded border border-amber-500/30 bg-amber-500/10 px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-amber-200">
                                        {tool.badge}
                                    </span>
                                )}
                            </div>
                            <h3 className="mt-3 text-sm font-bold text-white">{tool.title}</h3>
                            <p className="mt-1 text-xs leading-relaxed text-gray-400">{tool.desc}</p>
                            {!tool.comingSoon && (
                                <div className="mt-4 flex items-center gap-1 text-[11px] font-semibold uppercase tracking-wider text-cyan-300 opacity-0 transition group-hover:opacity-100">
                                    Open <ArrowRight className="h-3 w-3" />
                                </div>
                            )}
                        </button>
                    );
                })}
            </div>
        </section>
    );
}

export function StudioCampusPreview({ onOpen }: { onOpen: () => void }) {
    const lanes = [
        { label: 'Creator growth', icon: Film, desc: 'Retention, watch time, subscribers, community love.' },
        { label: 'Business ads', icon: BadgeDollarSign, desc: 'UGC, product videos, platform ads, ROI-focused creative.' },
        { label: 'Playbooks', icon: BookOpen, desc: 'Niche engines, lessons, reusable style systems.' },
        { label: 'Ranks', icon: Trophy, desc: 'Wins, progress, operator levels, proof-of-work profiles.' },
    ];
    return (
        <section className="overflow-hidden rounded-2xl border border-cyan-400/15 bg-[radial-gradient(circle_at_12%_0%,rgba(6,182,212,0.18),transparent_34%),linear-gradient(135deg,rgba(8,47,73,0.18),rgba(9,9,11,0.94)_48%,rgba(46,16,101,0.2))] p-5 shadow-sm shadow-black/30">
            <div className="flex flex-wrap items-start justify-between gap-5">
                <div className="max-w-3xl">
                    <div className="inline-flex items-center gap-2 rounded-full border border-emerald-400/20 bg-emerald-500/10 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-emerald-200">
                        <ShieldCheck className="h-3.5 w-3.5" />
                        Growth OS
                    </div>
                    <h2 className="mt-4 text-2xl font-bold tracking-tight text-white">Studio Hub is the next layer.</h2>
                    <p className="mt-2 max-w-2xl text-sm leading-6 text-gray-400">
                        A TRW-style command center for creators and businesses: content, ads, automation, analytics, ranks, lessons, wins, and reusable production systems.
                    </p>
                </div>
                <button
                    type="button"
                    onClick={onOpen}
                    className="inline-flex items-center gap-2 rounded-xl bg-gradient-to-r from-cyan-500 to-violet-500 px-4 py-2.5 text-sm font-bold text-white shadow-lg shadow-cyan-950/30 transition hover:from-cyan-400 hover:to-violet-400"
                >
                    Open Studio Hub
                    <ArrowRight className="h-4 w-4" />
                </button>
            </div>
            <div className="mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                {lanes.map((lane) => {
                    const Icon = lane.icon;
                    return (
                        <div key={lane.label} className="rounded-xl border border-white/[0.07] bg-black/25 p-4">
                            <Icon className="h-4 w-4 text-cyan-200" />
                            <h3 className="mt-3 text-sm font-bold text-white">{lane.label}</h3>
                            <p className="mt-1 text-xs leading-5 text-gray-400">{lane.desc}</p>
                        </div>
                    );
                })}
            </div>
            <div className="mt-4 grid gap-3 sm:grid-cols-3">
                <CampusMetric icon={LineChart} label="Goal" value="Retention + ROI" />
                <CampusMetric icon={Palette} label="Engines" value="25 industries" />
                <CampusMetric icon={Zap} label="Output" value="Ship-ready QA" />
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
        <section className="relative overflow-hidden rounded-2xl border border-white/[0.08] bg-[radial-gradient(circle_at_20%_0%,rgba(124,58,237,0.25),transparent_34%),linear-gradient(135deg,rgba(8,47,73,0.28),rgba(9,9,11,0.96)_45%,rgba(46,16,101,0.28))] p-6 shadow-2xl shadow-black/30 sm:p-7">
            <div className="relative z-10 flex flex-wrap items-end justify-between gap-6">
                <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.22em] text-violet-300">Workspace</p>
                    <h1 className="mt-2 text-3xl font-bold text-white sm:text-4xl">
                        {greeting}, {name}
                    </h1>
                    <p className="mt-3 max-w-2xl text-sm leading-relaxed text-gray-400">
                        {ownerPreview
                            ? 'Owner preview - all lanes open. Draft fast, ship cinematic, build long-form, or grow with Studio Hub.'
                            : 'Studio Agent is the command center: research, style, scene control, animation, packaging, and upload strategy.'}
                    </p>
                </div>
                <div className="grid min-w-[260px] grid-cols-3 gap-2">
                    <HeroMetric label="Engines" value="25" />
                    <HeroMetric label="Styles" value="24" />
                    <HeroMetric label="Hub" value="Live" />
                </div>
            </div>
        </section>
    );
}

function CampusMetric({
    icon: Icon,
    label,
    value,
}: {
    icon: typeof LineChart;
    label: string;
    value: string;
}) {
    return (
        <div className="flex items-center gap-3 rounded-xl border border-white/[0.07] bg-black/20 px-4 py-3">
            <Icon className="h-4 w-4 text-violet-200" />
            <div>
                <p className="text-[10px] uppercase tracking-[0.16em] text-gray-500">{label}</p>
                <p className="text-sm font-bold text-white">{value}</p>
            </div>
        </div>
    );
}

function HeroMetric({ label, value }: { label: string; value: string }) {
    return (
        <div className="rounded-xl border border-white/[0.08] bg-black/25 px-3 py-3 text-center">
            <p className="text-[10px] uppercase tracking-wider text-gray-500">{label}</p>
            <p className="mt-1 text-lg font-bold text-white">{value}</p>
        </div>
    );
}

export type { NicheId };
