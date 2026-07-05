export type LandingProofChannel =
    | 'all'
    | 'lume'
    | 'mr_skelewelly'
    | 'empire_magnates'
    | 'history_rewind'
    | 'six_figure_god'
    | 'zero_two';

export type LandingProofVideo = {
    channel: Exclude<LandingProofChannel, 'all'>;
    channelLabel: string;
    title: string;
    youtubeVideoId: string;
    thumbnailUrl: string;
    duration: string;
    category: string;
    views?: number;
    viewsLabel?: string;
};

const youtubeThumb = (id: string) => `https://i.ytimg.com/vi/${id}/hqdefault.jpg`;

export const landingProofChannels: Array<{ id: LandingProofChannel; label: string }> = [
    { id: 'all', label: 'All' },
    { id: 'lume', label: 'Lume' },
    { id: 'mr_skelewelly', label: 'Mr. Skelewelly' },
    { id: 'empire_magnates', label: 'Empire Magnates' },
    { id: 'history_rewind', label: 'History Rewind' },
    { id: 'six_figure_god', label: 'Six Figure God' },
    { id: 'zero_two', label: 'Zero Two' },
];

export const landingProofVideos: LandingProofVideo[] = [
    {
        channel: 'lume',
        channelLabel: 'Lume',
        title: 'He Hired 35 Couriers to Buy Every Single Lottery Ticket and Won $27 Million',
        youtubeVideoId: '55vqCXateoA',
        thumbnailUrl: youtubeThumb('55vqCXateoA'),
        duration: '20:37',
        category: 'AI-made long-form proof',
        viewsLabel: 'High-view proof',
    },
    {
        channel: 'lume',
        channelLabel: 'Lume',
        title: 'The Engineer Who Cheated Casino 11 Times in One Night Using A Secret Method',
        youtubeVideoId: 'DAgKs6KF5kU',
        thumbnailUrl: youtubeThumb('DAgKs6KF5kU'),
        duration: '20:19',
        category: 'AI-made long-form proof',
        viewsLabel: 'High-view proof',
    },
    {
        channel: 'mr_skelewelly',
        channelLabel: 'Mr. Skelewelly',
        title: 'The Real Reason Men Build Emotional Walls',
        youtubeVideoId: 'IOZIRg_aJkA',
        thumbnailUrl: youtubeThumb('IOZIRg_aJkA'),
        duration: '0:32',
        category: 'Psychology short',
        views: 571,
    },
    {
        channel: 'mr_skelewelly',
        channelLabel: 'Mr. Skelewelly',
        title: 'The Reason You Never stay Consistant',
        youtubeVideoId: 'gGHZGAe3Cks',
        thumbnailUrl: youtubeThumb('gGHZGAe3Cks'),
        duration: '0:54',
        category: 'Self-improvement short',
        views: 306,
    },
    {
        channel: 'mr_skelewelly',
        channelLabel: 'Mr. Skelewelly',
        title: "The Real Reason You Can't Stop Scrolling Your Phone",
        youtubeVideoId: 'dBe8b8jXPa8',
        thumbnailUrl: youtubeThumb('dBe8b8jXPa8'),
        duration: '0:58',
        category: 'Behavior short',
        views: 36,
    },
    {
        channel: 'empire_magnates',
        channelLabel: 'Empire Magnates',
        title: 'He Found a Loophole That Legally Stole $1.9 Billion From Germany | Wirecard',
        youtubeVideoId: 'ljSM_fPAwMQ',
        thumbnailUrl: youtubeThumb('ljSM_fPAwMQ'),
        duration: '13:51',
        category: 'Financial crime',
        views: 93,
    },
    {
        channel: 'history_rewind',
        channelLabel: 'History Rewind',
        title: 'The Rise and Fall of the Ottoman Empire | Full Documentary | 9 Hours',
        youtubeVideoId: 'VHcfUnXPwZA',
        thumbnailUrl: youtubeThumb('VHcfUnXPwZA'),
        duration: '7:19:57',
        category: 'Long-form documentary',
        views: 12,
    },
    {
        channel: 'zero_two',
        channelLabel: 'Zero Two',
        title: 'The Arrow-verse Mystery that was never legally published',
        youtubeVideoId: '__DNmMQQ80E',
        thumbnailUrl: youtubeThumb('__DNmMQQ80E'),
        duration: '8:20',
        category: 'Entertainment documentary',
        views: 8,
    },
];
