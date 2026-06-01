/**
 * Studio product model — single source of truth for niches, render tiers,
 * navigation, and output estimates (Korpi-shaped, NYPTID-owned).
 */

export type RenderTierId = 'draft' | 'ship' | 'documentary';

export type NicheId =
    | 'alt_battles'
    | 'dilemma'
    | 'scary'
    | 'history'
    | 'longform'
    | 'style_clone'
    | 'zerotier_private'
    | 'alt_history_private'
    | 'history_rewind_private';

export type DashboardTab =
    | 'home'
    | 'create'
    | 'agent'
    | 'longform'
    | 'automate'
    | 'analytics'
    | 'catalyst'
    | 'refunds'
    | 'waitlist';

export interface RenderTier {
    id: RenderTierId;
    label: string;
    tagline: string;
    provider: string;
    creditHint: string;
    eta: string;
    badge?: string;
}

export interface StudioNiche {
    id: NicheId;
    title: string;
    desc: string;
    badge?: string;
    ownerOnly?: boolean;
    /** skeleton-ai category key or pipeline route */
    categoryKey: string;
    format: 'short' | 'long';
}

export interface StudioTool {
    id: string;
    title: string;
    desc: string;
    badge?: string;
    action: 'create' | 'longform' | 'style_clone' | 'automate' | 'playground' | 'agent';
    comingSoon?: boolean;
}

export const RENDER_TIERS: RenderTier[] = [
    {
        id: 'draft',
        label: 'Draft',
        tagline: 'Iterate fast — fal LTX / Pixverse, hook and pacing first.',
        provider: 'fal.ai',
        creditHint: '~1–2 AC',
        eta: '~3 min',
    },
    {
        id: 'ship',
        label: 'Ship',
        tagline: 'Cinematic realism — hero scenes, locked identity, upload-ready.',
        provider: 'fal + Higgsfield (Pro)',
        creditHint: '~4–8 AC',
        eta: '~8 min',
        badge: 'Premium',
    },
    {
        id: 'documentary',
        label: 'Documentary',
        tagline: 'Long-form v5 — still approval gate, then animate + compose.',
        provider: 'fal LTX + MiniMax',
        creditHint: '~15–25 AC',
        eta: 'Stage 1 ~20 min',
        badge: 'Long Form',
    },
];

export const STUDIO_NICHES: StudioNiche[] = [
    {
        id: 'alt_battles',
        title: 'Alt-History Battles',
        desc: 'Counterfactual matchups — painterly cinematic battles',
        badge: 'Live',
        categoryKey: 'classical_clash',
        format: 'short',
    },
    {
        id: 'dilemma',
        title: 'Moral Dilemma',
        desc: 'Binary-choice hooks that drive comments',
        badge: 'Trending',
        categoryKey: 'wildcard_clash',
        format: 'short',
    },
    {
        id: 'scary',
        title: 'Scary Stories',
        desc: 'Horror atmosphere, true-crime pacing',
        badge: 'Trending',
        categoryKey: 'medieval_clash',
        format: 'short',
    },
    {
        id: 'history',
        title: 'Historical Epic',
        desc: 'Ridley-Scott scale — armies, siege, empire',
        badge: 'Trending',
        categoryKey: 'gunpowder_clash',
        format: 'short',
    },
    {
        id: 'longform',
        title: 'Documentary',
        desc: '15–60 min episodes — fraud, mystery, science',
        badge: 'Beta',
        categoryKey: 'v5_episode',
        format: 'long',
    },
    {
        id: 'style_clone',
        title: 'Clone a Style',
        desc: 'Paste a reference URL — save your visual + title pack',
        badge: 'New',
        categoryKey: 'reference',
        format: 'short',
    },
    {
        id: 'zerotier_private',
        title: 'ZeroTier (Private)',
        desc: 'DC comic shorts — mechanism-first canon',
        badge: 'Owner',
        ownerOnly: true,
        categoryKey: 'zerotier_private',
        format: 'short',
    },
    {
        id: 'alt_history_private',
        title: 'Alt-History (Private)',
        desc: 'Cryptic Science + Catalyst outcomes',
        badge: 'Owner',
        ownerOnly: true,
        categoryKey: 'alt_history_private',
        format: 'short',
    },
    {
        id: 'history_rewind_private',
        title: 'History Rewind (Private)',
        desc: '9-hour sleep doc topics',
        badge: 'Owner',
        ownerOnly: true,
        categoryKey: 'history_rewind_private',
        format: 'long',
    },
];

export const STUDIO_TOOLS: StudioTool[] = [
    {
        id: 'agent',
        title: 'Studio Agent',
        desc: 'Chat-first production — long-form, shorts, analytics',
        badge: 'Beta',
        action: 'agent',
    },
    {
        id: 'shorts',
        title: 'Short Builder',
        desc: 'Script → scenes → ship a vertical short',
        action: 'create',
    },
    {
        id: 'longform',
        title: 'Documentary',
        desc: 'Outline → still gallery → LTX + VO compose',
        badge: 'Beta',
        action: 'longform',
    },
    {
        id: 'style',
        title: 'Style Pack',
        desc: 'Lock look, title shape, and hook cadence',
        badge: 'New',
        action: 'style_clone',
    },
    {
        id: 'automate',
        title: 'Automate',
        desc: 'Schedule renders + outcome checks',
        badge: 'Soon',
        action: 'automate',
        comingSoon: true,
    },
];

export function nicheById(id: string | null | undefined): StudioNiche | undefined {
    const key = String(id || '').trim();
    return STUDIO_NICHES.find((n) => n.id === key);
}

export function renderTierById(id: string | null | undefined): RenderTier {
    const key = String(id || 'draft').trim() as RenderTierId;
    return RENDER_TIERS.find((t) => t.id === key) || RENDER_TIERS[0];
}

/** Rough shorts remaining from total AC (marketing-friendly, not a hard guarantee). */
export function estimateShortsRemaining(totalAc: number, tier: RenderTierId = 'draft'): number {
    const cost = tier === 'ship' ? 6 : tier === 'documentary' ? 20 : 2;
    return Math.max(0, Math.floor(totalAc / cost));
}

export function visibleNiches(isOwner: boolean): StudioNiche[] {
    return STUDIO_NICHES.filter((n) => !n.ownerOnly || isOwner);
}

/** Public short-form niches that use the Create builder (skeleton-ai lane). */
export function creatableShortNiches(_isOwner = false): StudioNiche[] {
    const ids: NicheId[] = ['alt_battles', 'dilemma', 'scary', 'history'];
    return ids.map((id) => nicheById(id)).filter(Boolean) as StudioNiche[];
}
