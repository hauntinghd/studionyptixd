export type LandingUpdatePost = {
    slug: string;
    title: string;
    date: string;
    label: string;
    summary: string;
    bullets: string[];
};

export const landingUpdatePosts: LandingUpdatePost[] = [
    {
        slug: 'studio-1-0-public-release',
        title: 'NYPTID Studio 1.0 is now available',
        date: '2026-07-18',
        label: 'Public release',
        summary: 'Studio 1.0 brings desktop-first short-form and long-form production, secure sign-in, billing, review, repair, animation, and export into one creator workflow.',
        bullets: [
            'Plan and produce short-form or long-form video inside Studio Agent.',
            'Review and repair individual scenes before animation and final export.',
            'Use secure Google sign-in, Stripe memberships, unified credits, and signed in-app updates.',
        ],
    },
    {
        slug: 'studio-all-in-one-proof-update',
        title: 'Studio landing page now leads with real proof',
        date: '2026-07-04',
        label: 'Product update',
        summary: 'The public page now shows real channel analytics, AI-made long-form proof, public pricing, and click-to-load video examples without loading YouTube embeds on first paint.',
        bullets: [
            'Added verified creator analytics screenshots with 823K+ channel views.',
            'Added Lume AI-made long-form examples to the proof wall.',
            'Added public pricing cards while keeping purchase gated behind sign-in and card setup.',
        ],
    },
    {
        slug: 'studio-agent-visibility-and-cost-control',
        title: 'Studio Agent progress and cost control are becoming first-class',
        date: '2026-07-03',
        label: 'Build log',
        summary: 'Studio Agent now puts more work in front of the user: production state, verification steps, render progress, cost gates, and refund-safe failure handling.',
        bullets: [
            'Render progress is visible instead of silent background work.',
            'Failed render flows are being tightened around automatic credit protection.',
            'Cost-aware production gates are being built around expensive image-to-video steps.',
        ],
    },
    {
        slug: 'cliplab-internal-opus-bridge',
        title: 'ClipLab is moving toward long-form to short-form workflows',
        date: '2026-07-02',
        label: 'Internal beta',
        summary: 'ClipLab is staying owner-only while it matures, with an internal Opus bridge available for testing long-form clip discovery before public release.',
        bullets: [
            'Long-form upload and clip candidate analysis are being tested privately.',
            'Studio Agent can route ClipLab work without changing the normal Studio Agent flow.',
            'Public release remains blocked until quality, previews, and upload packages are dependable.',
        ],
    },
];
