// Curated idea presets for the "Spark Script" wizard.
//
// Every niche exposes EXACTLY 4 idea styles and each style exposes 5
// concrete topic ideas. Styles were chosen from short-form formats that
// are either currently viral on YouTube Shorts / TikTok or have a
// multi-year track record of working (POV reactions, tier lists, "they
// don't teach you this", comparison hooks, NoSleep, etc.).
//
// Each style carries:
//   - `emoji` — niche cue shown top-left on the style card
//   - `angle` — one-line hook theory note (why the format works)
// Plus per-day virality badges + heat scores from `heatScore.ts`.
//
// Future: swap this file for `/api/templates/{template}/ideas`
// once Catalyst trend signals rank live-topic candidates.

export type IdeaStyle = {
    id: string;
    label: string;
    description: string;
    emoji: string;
    angle: string;
    ideas: string[];
};

export type TemplateIdeaPreset = {
    template: string;
    styles: IdeaStyle[];
};

export const SHORTFORM_IDEA_PRESETS: TemplateIdeaPreset[] = [
    {
        template: "skeleton",
        styles: [
            {
                id: "skeleton_reacts",
                label: "Skeleton Reacts",
                description: "POV reaction format — skeleton reacts to X.",
                emoji: "😶",
                angle: "Reaction format owns the shorts algorithm. Cheap to make, high retention.",
                ideas: [
                    "Skeleton reacts to its first paycheck after 10 years of unemployment",
                    "Skeleton reacts to being told AI just replaced its job",
                    "Skeleton reacts to hearing Social Security is bankrupt",
                    "Skeleton reacts to the Gen Z way of answering the phone",
                    "Skeleton reacts to a $7 coffee on a Tuesday",
                ],
            },
            {
                id: "skeleton_tries",
                label: "Skeleton Tries",
                description: "First-time POV — skeleton attempts X.",
                emoji: "💀",
                angle: "'Tries X for the first time' is a proven retention hook on every platform.",
                ideas: [
                    "Skeleton tries to return a used PS5 to Walmart",
                    "Skeleton tries to negotiate a medical bill",
                    "Skeleton tries to cancel a gym membership",
                    "Skeleton tries to dispute a $40 late fee",
                    "Skeleton tries to ask for a raise in 2026",
                ],
            },
            {
                id: "skeleton_explains",
                label: "Skeleton Explains",
                description: "Deadpan explainer — skeleton breaks down X.",
                emoji: "🧠",
                angle: "Deadpan voice + absurd premise = high share-rate educational comedy.",
                ideas: [
                    "Skeleton explains why renting is actually smart in 2026",
                    "Skeleton explains the real reason eggs cost $9",
                    "Skeleton explains why nobody your age has friends anymore",
                    "Skeleton explains how dating apps are designed to keep you single",
                    "Skeleton explains why the 40-hour work week was a marketing scam",
                ],
            },
            {
                id: "skeleton_ranks",
                label: "Skeleton Ranks",
                description: "Tier-list format — skeleton sorts X from worst to best.",
                emoji: "📊",
                angle: "Tier lists drive comments. Every ranking triggers 'actually...' replies.",
                ideas: [
                    "Skeleton ranks every generation's financial mistakes",
                    "Skeleton ranks the most useless college majors of 2026",
                    "Skeleton ranks side hustles from dignity-destroying to elite",
                    "Skeleton ranks American cities by cost-of-living suffering",
                    "Skeleton ranks the jobs most likely to survive AI",
                ],
            },
        ],
    },
    {
        template: "daytrading",
        styles: [
            {
                id: "the_setup",
                label: "The Setup",
                description: "One chart pattern that prints.",
                emoji: "📈",
                angle: "Single-idea chart hook — saveable, re-shareable, algorithmic.",
                ideas: [
                    "The opening range breakout setup that prints daily",
                    "The one candle pattern institutions actually trade",
                    "The setup that caught every NVDA top in 2025",
                    "The 5-min setup that makes $500 before 10am",
                    "The order-flow tell nobody teaches on YouTube",
                ],
            },
            {
                id: "retail_traps",
                label: "Retail Traps",
                description: "Why retail always loses.",
                emoji: "🎣",
                angle: "Contrarian 'you're being scammed' hooks punch through retail-trader noise.",
                ideas: [
                    "Why 'buy the dip' is the most expensive phrase on Wall Street",
                    "How market makers front-run your stop-losses in real time",
                    "The PFOF loophole that decides what you pay",
                    "Why your broker wants you to trade options",
                    "The dark pool trick that moves price without you seeing it",
                ],
            },
            {
                id: "prop_firm_diary",
                label: "Prop Firm Diary",
                description: "Day-X of the challenge format.",
                emoji: "🗓️",
                angle: "Serialized-diary format builds return viewership — proven long-tail format.",
                ideas: [
                    "Day 1 of trying to pass a $100K prop firm challenge",
                    "Day 5 — I'm up $3K but one bad trade ends it all",
                    "The rule I broke that got me an instant account bust",
                    "How I scaled from $10K to $200K funded in 90 days",
                    "The exact risk plan that passes FTMO 8/10 times",
                ],
            },
            {
                id: "billion_dollar_lessons",
                label: "Billion-Dollar Lessons",
                description: "What the pros know that retail doesn't.",
                emoji: "🏛️",
                angle: "Forbidden-knowledge framing → highest-retention niche in fintok.",
                ideas: [
                    "How Jim Simons averaged 66% a year for 30 years",
                    "The Renaissance Technologies rule nobody copies",
                    "Why Stan Druckenmiller sizes his positions differently than you",
                    "The Soros trade that broke the Bank of England",
                    "How Citadel's market-making desk actually makes money",
                ],
            },
        ],
    },
    {
        template: "dilemma",
        styles: [
            {
                id: "would_you_rather",
                label: "Would You Rather",
                description: "Hard binary choices with no clean answer.",
                emoji: "⚖️",
                angle: "Binary-choice comment-bait by design — comment section does the work.",
                ideas: [
                    "Would you take $1M now or $10K/month for life?",
                    "Would you rather know when you die or how you die?",
                    "Would you delete 5 years of memories for $5M?",
                    "Would you rather be famous or anonymous forever?",
                    "Would you take your dream job at half pay?",
                ],
            },
            {
                id: "what_would_you_do",
                label: "What Would You Do",
                description: "Situational ethics in 30 seconds.",
                emoji: "🤔",
                angle: "Point-of-view framing forces viewer to answer in their head — high replay.",
                ideas: [
                    "You find $100K on the subway. Nobody saw. Now what?",
                    "Your best friend's partner hits on you. Do you tell?",
                    "You catch your coworker stealing. Report or stay quiet?",
                    "Your boss asks you to lie on a deposition. Yes or no?",
                    "You see someone shoplifting baby formula. Intervene?",
                ],
            },
            {
                id: "the_ethical_cost",
                label: "The Ethical Cost",
                description: "Real-world tradeoffs most people dodge.",
                emoji: "💸",
                angle: "Adult-stakes framing — parents and 25-40 audience over-indexes here.",
                ideas: [
                    "Accept a 3x salary job that requires leaving your kids?",
                    "Sell out your startup principles for $50M exit?",
                    "Fire the friend who got you your first job?",
                    "Snitch on your brother to get a plea deal?",
                    "Stop funding your parents to save your own retirement?",
                ],
            },
            {
                id: "whos_wrong",
                label: "Who's Wrong",
                description: "Reddit AITA / comment-splitter format.",
                emoji: "🗣️",
                angle: "AITA format is the single most-commented-on shorts format of 2025-2026.",
                ideas: [
                    "Wife quit her $200K job without telling me. Am I wrong to divorce?",
                    "I charged my brother rent after he moved in. Am I the villain?",
                    "My friend asked me to lie at his wedding. I refused.",
                    "I took my bonus instead of splitting with my team. AITA?",
                    "I didn't invite my sister's new boyfriend to Thanksgiving.",
                ],
            },
        ],
    },
    {
        template: "business",
        styles: [
            {
                id: "founder_pivot",
                label: "Founder Pivot",
                description: "The moment that made the company.",
                emoji: "🚀",
                angle: "Origin-story pivots are the universal biz-shorts hook. Always works.",
                ideas: [
                    "The 3am email that saved Stripe from bankruptcy",
                    "Why Airbnb almost shut down 4 times in its first year",
                    "The pivot that turned Instagram from Burbn to $1B",
                    "How Shopify's founder rebuilt the product 3 times",
                    "The rejection letter that convinced Bezos to quit his job",
                ],
            },
            {
                id: "scaling_secret",
                label: "Scaling Secret",
                description: "One move that 10x'd the company.",
                emoji: "📊",
                angle: "Specific-tactic hook — saveable, actionable, drives follower conversion.",
                ideas: [
                    "The $9 pricing tweak that doubled Stripe's conversion",
                    "The one hire that unlocked $10M ARR for Linear",
                    "The referral trick that got Dropbox to 4M users",
                    "How Notion killed meetings with one template",
                    "The onboarding email that cut Slack's churn in half",
                ],
            },
            {
                id: "quiet_collapse",
                label: "Quiet Collapse",
                description: "Billion-dollar companies that quietly died.",
                emoji: "💥",
                angle: "Schadenfreude + lesson = rewatchable combo. Evergreen retention.",
                ideas: [
                    "How WeWork lost $47B without firing anyone",
                    "Why Quibi died in 6 months with $1.75B raised",
                    "The social app that burned $400M before launch",
                    "Theranos in 60 seconds — the fraud that fooled everyone",
                    "How Peloton lost 90% of its valuation in 18 months",
                ],
            },
            {
                id: "built_from_nothing",
                label: "Built From Nothing",
                description: "Zero-to-empire stories.",
                emoji: "🛠️",
                angle: "Underdog origin framing — motivational traffic + high bookmark rate.",
                ideas: [
                    "How a laid-off engineer built a $100M SaaS in 3 years",
                    "The teenager who turned $400 into a million-dollar brand",
                    "How one immigrant built 3 businesses before age 30",
                    "The single mom who bootstrapped a $50M e-commerce empire",
                    "How a community-college dropout sold his company for $300M",
                ],
            },
        ],
    },
    {
        template: "finance",
        styles: [
            {
                id: "money_myth_busted",
                label: "Money Myth Busted",
                description: "Common advice that's actually wrong.",
                emoji: "🚫",
                angle: "Contrarian money takes beat consensus on every feed. Top of the niche.",
                ideas: [
                    "Why paying off your mortgage early is a financial mistake",
                    "Index funds aren't as safe as you've been told",
                    "Emergency funds are costing you more than they save",
                    "Why 'skip the $5 latte' is the dumbest advice ever given",
                    "The real reason renters end up wealthier than buyers",
                ],
            },
            {
                id: "wealth_rule",
                label: "Wealth Rule",
                description: "What rich people actually do.",
                emoji: "💰",
                angle: "'Rich-people secret' framing → aspirational CTR spike.",
                ideas: [
                    "The tax loophole only millionaires know about",
                    "Why the wealthy borrow instead of selling assets",
                    "How trust funds actually work (and why yours should too)",
                    "The Buffett compounding rule that built $100B",
                    "What rich families teach their kids at age 12",
                ],
            },
            {
                id: "the_math",
                label: "The Math",
                description: "Surprising numbers people dodge.",
                emoji: "🧮",
                angle: "Math-on-screen hook forces screenshot-saves and replays.",
                ideas: [
                    "The real cost of working until 65 vs retiring at 50",
                    "Why $100/month at 25 beats $1000/month at 45",
                    "How inflation ate 30% of your savings in 4 years",
                    "The hidden APR on buy-now-pay-later apps",
                    "What $1M at retirement actually gets you in 2060",
                ],
            },
            {
                id: "hidden_drain",
                label: "Hidden Drain",
                description: "Invisible lifestyle costs.",
                emoji: "🔎",
                angle: "Lifestyle-cost-audit format drives comment debate + shares to couples.",
                ideas: [
                    "The 7 subscriptions secretly draining $2K/year from you",
                    "How much your car actually costs you per year (hint: not the payment)",
                    "Why your Amazon habit is worth $80K in 10 years",
                    "The true cost of 'just one drink' every weekend",
                    "How delivery apps steal $3K/year without you noticing",
                ],
            },
        ],
    },
    {
        template: "tech",
        styles: [
            {
                id: "ai_reality_check",
                label: "AI Reality Check",
                description: "Hype vs what AI can actually do.",
                emoji: "🤖",
                angle: "Hype-check framing cuts through AI noise. Top retention in tech niche.",
                ideas: [
                    "Why ChatGPT still can't replace a junior developer in 2026",
                    "The AI capability nobody is talking about (yet)",
                    "AGI is further away than OpenAI wants you to believe",
                    "The AI job that pays $500K today — and why it won't exist in 2028",
                    "Why GPT-5 is going to disappoint you",
                ],
            },
            {
                id: "quiet_winner",
                label: "Quiet Winner",
                description: "Unknown tech quietly dominating.",
                emoji: "🕳️",
                angle: "Under-the-radar framing → 'I heard it first' share behavior.",
                ideas: [
                    "The company quietly replacing Shopify (you've never heard of it)",
                    "How a Belgian chip startup took $50B from Nvidia",
                    "The AI browser nobody's using that's crushing Chrome",
                    "The legal-tech tool making BigLaw obsolete",
                    "Why a Danish software company is beating Oracle on its home turf",
                ],
            },
            {
                id: "predictions_5yr",
                label: "5-Year Predictions",
                description: "What the next 5 years actually look like.",
                emoji: "🔮",
                angle: "Future-bet format builds anticipation + comment debate.",
                ideas: [
                    "The job AI will fully replace next — and it's not what you think",
                    "Why remote work is already dead in 2027",
                    "The platform that will replace TikTok",
                    "How humanoid robots will kill the gig economy",
                    "The tech that makes smartphones obsolete by 2029",
                ],
            },
            {
                id: "dead_tech_walking",
                label: "Dead Tech Walking",
                description: "What's quietly obsolete already.",
                emoji: "💀",
                angle: "Eulogy framing — 'you still use this?' triggers defensive engagement.",
                ideas: [
                    "Why email is already dead and nobody's admitting it",
                    "The programming language that won't exist in 5 years",
                    "Why your passwords are already cracked and you don't know it",
                    "The SaaS category AI is about to erase completely",
                    "Cloud computing in 2030 — the next Blockbuster",
                ],
            },
        ],
    },
    {
        template: "crypto",
        styles: [
            {
                id: "narrative_cycle",
                label: "Narrative Cycle",
                description: "The meta that's actually moving bags.",
                emoji: "🌊",
                angle: "Meta-cycle calls drive alpha-seeker shares + saves. Top of crypto niche.",
                ideas: [
                    "Why AI tokens are about to run another 5x this cycle",
                    "The RWA narrative nobody's priced in yet",
                    "Memecoin rotation — what's next after dog coins",
                    "DePIN is the sleeper sector of 2026 — here's why",
                    "Gaming tokens are in final accumulation — top 5 picks",
                ],
            },
            {
                id: "whale_watch",
                label: "Whale Watch",
                description: "What smart money is doing on-chain.",
                emoji: "🐋",
                angle: "On-chain detective angle → evergreen content, saveable leads.",
                ideas: [
                    "The wallet that bought the exact bottom in every asset",
                    "Why Jump Trading is accumulating this one token",
                    "The rug signal that saved 10,000 wallets this week",
                    "How insiders front-ran the last 3 Binance listings",
                    "The quiet accumulation under $0.50 nobody's watching",
                ],
            },
            {
                id: "memecoin_playbook",
                label: "Memecoin Playbook",
                description: "How to survive the casino.",
                emoji: "🎰",
                angle: "Tactical survival framing — highest share rate in crypto-Twitter crossover.",
                ideas: [
                    "The 3 rules that would've saved you from every 2024 rug",
                    "Why 'HODL' is the worst strategy for memecoins",
                    "The $100-to-$100K memecoin trade I'd make again",
                    "How to DCA out of a memecoin without regret",
                    "The chart signal that calls every memecoin top",
                ],
            },
            {
                id: "exit_strategy",
                label: "Exit Strategy",
                description: "How to not give it all back at the top.",
                emoji: "🚪",
                angle: "Practical utility → highest share rate in crypto niche.",
                ideas: [
                    "The exit signal that printed at every cycle top since 2017",
                    "Why CT influencers never sell — until it's already too late",
                    "The on-chain metric that calls every major top",
                    "How to set exit ladders before you get greedy",
                    "The tax trick that legally saves 30% on crypto gains",
                ],
            },
        ],
    },
    {
        template: "scary",
        styles: [
            {
                id: "nosleep_story",
                label: "NoSleep Story",
                description: "Reddit-style first-person horror.",
                emoji: "📱",
                angle: "NoSleep narration format is the single best-performing horror-shorts format.",
                ideas: [
                    "I work night security at a mall that closed in 1998",
                    "My smart doorbell caught someone who shouldn't exist",
                    "I'm a 911 dispatcher. Last night a dead person called back.",
                    "There's a tenant in 4B who's been there since before the building was built",
                    "I found a second basement in my house last week",
                ],
            },
            {
                id: "unsolved_mystery",
                label: "Unsolved Mystery",
                description: "Cold cases that still don't add up.",
                emoji: "🔍",
                angle: "Cold-case hooks land reliably — long comment threads, re-shares.",
                ideas: [
                    "The Springfield 3 — three women vanished from a house in 1992",
                    "The JonBenét evidence the public has never seen",
                    "The Delphi murders — what investigators still won't release",
                    "The disappearance that looks more staged every decade",
                    "The 40-year cold case solved by one handwritten note",
                ],
            },
            {
                id: "caught_on_camera",
                label: "Caught On Camera",
                description: "Real footage nobody can explain.",
                emoji: "📹",
                angle: "Visual-proof hook — 3-sec scrollstop is near-universal on this format.",
                ideas: [
                    "The Ring doorbell footage police refuse to comment on",
                    "The dashcam clip that became evidence in a murder trial",
                    "The hotel hallway video that makes no logical sense",
                    "The hiking GoPro footage the family released — then recalled",
                    "The convenience-store tape time-stamped from a different day",
                ],
            },
            {
                id: "paranormal_real",
                label: "Paranormal Real",
                description: "Documented locations with histories.",
                emoji: "🕯️",
                angle: "Real-location anchor grounds the eerie — higher retention than pure fiction.",
                ideas: [
                    "The Oregon forest where compasses stop working",
                    "The Pennsylvania house where clocks run backwards",
                    "The Maine island that appears once a decade",
                    "The lighthouse nobody can prove was ever built",
                    "The tunnel under Chicago that has no recorded end",
                ],
            },
        ],
    },
    {
        template: "history",
        styles: [
            {
                id: "they_dont_teach",
                label: "They Don't Teach You",
                description: "History that got erased from textbooks.",
                emoji: "📚",
                angle: "Suppressed-history framing is THE top-performing history-shorts format.",
                ideas: [
                    "The Black Wall Street massacre that was scrubbed from US textbooks",
                    "The 1956 Hungarian uprising the West stood by and watched",
                    "The real reason the Library of Alexandria burned",
                    "The women who actually broke Enigma (not the ones in the movie)",
                    "The US city that tried to secede from America in 1860",
                ],
            },
            {
                id: "empires_fall",
                label: "Empire's Fall",
                description: "How great civilizations actually ended.",
                emoji: "🏛️",
                angle: "Collapse narratives — strong climax arc fits the 60-90s shorts format.",
                ideas: [
                    "The 3 bad decisions that ended the Roman Empire",
                    "Why the Minoans vanished literally overnight",
                    "The single earthquake that killed the Bronze Age civilizations",
                    "How a drought in 1200 BC ended 3 empires at once",
                    "The empire that ruled Bronze-Age seas — and why you've never heard of it",
                ],
            },
            {
                id: "unknown_hero",
                label: "Unknown Hero",
                description: "Erased figures who changed everything.",
                emoji: "📜",
                angle: "Rediscovered-hero angle → high shares from educators + parents.",
                ideas: [
                    "The African queen who defeated Portugal and was erased from history",
                    "The scientist whose work made the atom bomb possible — and who was forgotten",
                    "The general who never lost a battle — and whose name you don't know",
                    "The spy who changed both World Wars from a single ballroom in Lisbon",
                    "The engineer who built Constantinople's walls — and saved Europe for 1000 years",
                ],
            },
            {
                id: "day_that_changed",
                label: "Day That Changed Everything",
                description: "The 24 hours that reshaped the world.",
                emoji: "⏳",
                angle: "'One day' framing gives built-in tight scope — perfect for short-form.",
                ideas: [
                    "July 4, 1776 — the meeting that almost didn't happen",
                    "November 9, 1989 — the press-conference error that toppled the Berlin Wall",
                    "The single afternoon in 1914 that started WW1",
                    "September 2, 1945 — the day the modern world really began",
                    "October 16, 1962 — the 13 days the world almost ended",
                ],
            },
        ],
    },
];

export function getIdeaStylesForTemplate(template: string): IdeaStyle[] {
    const preset = SHORTFORM_IDEA_PRESETS.find((p) => p.template === template);
    return preset?.styles ?? [];
}
