// Hardcoded idea presets for the "Generate w/ AI" script wizard.
// Each template exposes 3 idea styles. Picking a style populates a list of
// concrete topic ideas the user can one-click into the narration pipeline.
//
// Future: move to `/api/templates/{template}/ideas` endpoint so the list
// rotates with Catalyst trend signals. Hardcoding first to ship.

export type IdeaStyle = {
    id: string;
    label: string;
    description: string;
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
                id: "human_limits",
                label: "Human Limits",
                description: "Body and brain failure over time.",
                ideas: [
                    "How long can you stay awake?",
                    "How much pain can the human body take?",
                    "Why do you stop growing at 18?",
                    "What happens to your bones in space?",
                    "How much blood can you lose and survive?",
                ],
            },
            {
                id: "career_compare",
                label: "Comparing Career Paths",
                description: "Easy-entry job vs high-difficulty path.",
                ideas: [
                    "Who makes more: FBI agent or basketball player?",
                    "Plumber vs software engineer — who retires first?",
                    "Nurse vs truck driver — which pays better?",
                    "Electrician vs lawyer — who's richer at 40?",
                    "Welder vs accountant — who has more savings?",
                ],
            },
            {
                id: "socrates",
                label: "Socrates",
                description: "Selling products or opening businesses in ancient Greece.",
                ideas: [
                    "How would you sell sandals in ancient Athens?",
                    "What would a gym franchise look like in 400 BC?",
                    "Could you open a coffee shop in ancient Rome?",
                    "How did merchants actually get paid before coins?",
                    "What's the oldest business model still used today?",
                ],
            },
        ],
    },
    {
        template: "daytrading",
        styles: [
            {
                id: "patterns",
                label: "Pattern Recognition",
                description: "Chart setups that print money when you spot them.",
                ideas: [
                    "The only candle pattern that actually works",
                    "Why you keep buying tops and selling bottoms",
                    "The setup institutions use to trap retail",
                    "One pattern that signals a 3x move",
                    "The chart trick nobody teaches on YouTube",
                ],
            },
            {
                id: "misconceptions",
                label: "Market Misconceptions",
                description: "What retail believes vs what actually moves price.",
                ideas: [
                    "Why 'buy the dip' is a trap",
                    "Technical analysis is mostly lies",
                    "The real reason stocks gap up overnight",
                    "Your broker is not on your side",
                    "Volume doesn't mean what you think it means",
                ],
            },
            {
                id: "insider_secrets",
                label: "Institutional Secrets",
                description: "How market makers and funds actually operate.",
                ideas: [
                    "How market makers front-run your orders",
                    "The real reason hedge funds short meme stocks",
                    "What a dark pool trade actually looks like",
                    "Why PFOF decides what you pay",
                    "How props desks exit before the crash",
                ],
            },
        ],
    },
    {
        template: "dilemma",
        styles: [
            {
                id: "ethics",
                label: "Ethical Trolley Problems",
                description: "Hard binary choices with no clean answer.",
                ideas: [
                    "Would you steal a million dollars to save a stranger?",
                    "Snitch on your best friend or lose your job?",
                    "Save 5 strangers or 1 family member?",
                    "Accept a bribe to feed your kids?",
                    "Tell the truth and lose everything?",
                ],
            },
            {
                id: "career",
                label: "Career Ethics",
                description: "The workplace decisions that define you.",
                ideas: [
                    "Take your boss's job behind their back?",
                    "Quit the second you get a better offer?",
                    "Cover for a coworker stealing time?",
                    "Report a safety violation at your own cost?",
                    "Fire your friend to save the team?",
                ],
            },
            {
                id: "relationships",
                label: "Relationship Choices",
                description: "Love, loyalty, betrayal — pick one.",
                ideas: [
                    "Tell your friend their partner is cheating?",
                    "Marry for money or marry for love?",
                    "Forgive a decade-old betrayal?",
                    "Choose your sibling or your spouse?",
                    "Stay for the kids or leave for yourself?",
                ],
            },
        ],
    },
    {
        template: "business",
        styles: [
            {
                id: "founder",
                label: "Founder Stories",
                description: "The moment that made the company.",
                ideas: [
                    "How Airbnb survived three near-deaths",
                    "The email that saved Stripe",
                    "Why Jeff Bezos almost quit Amazon in 2001",
                    "How Shopify got its first 1000 merchants",
                    "The pivot that built Instagram",
                ],
            },
            {
                id: "scaling",
                label: "Scaling Secrets",
                description: "What actually 10x's a company.",
                ideas: [
                    "The hire that unlocks $10M ARR",
                    "Why 90% of startups die at $1M",
                    "The pricing tweak that doubled revenue",
                    "How to survive the dead zone at 20 employees",
                    "The one metric every CEO tracks weekly",
                ],
            },
            {
                id: "disruption",
                label: "Industry Disruption",
                description: "Who's eating whose lunch right now.",
                ideas: [
                    "Why Uber is losing to no-name competitors",
                    "How AI is killing the consulting industry",
                    "The company quietly replacing Shopify",
                    "Why big pharma fears semaglutide copycats",
                    "The legal tech killing big law",
                ],
            },
        ],
    },
    {
        template: "finance",
        styles: [
            {
                id: "myths",
                label: "Money Myths",
                description: "Personal finance advice that's actually wrong.",
                ideas: [
                    "Why paying off your mortgage early is a mistake",
                    "Index funds aren't as safe as you think",
                    "Renting beats buying in 2026 (the math)",
                    "Emergency funds are costing you money",
                    "Why 'skip the latte' advice is a scam",
                ],
            },
            {
                id: "wealth",
                label: "Wealth Rules",
                description: "How rich people actually stay rich.",
                ideas: [
                    "The tax loophole only millionaires use",
                    "Why the wealthy never sell assets",
                    "How trust funds actually work",
                    "The Buffett rule for compounding wealth",
                    "What rich families teach kids at 12",
                ],
            },
            {
                id: "macro",
                label: "Economic Shifts",
                description: "The macro moves reshaping your money.",
                ideas: [
                    "Why interest rates will stay high for a decade",
                    "The housing crash no one sees coming",
                    "How BRICS is replacing the dollar",
                    "The commodity about to 10x",
                    "Why private credit is the next 2008",
                ],
            },
        ],
    },
    {
        template: "tech",
        styles: [
            {
                id: "ai_reality",
                label: "AI Reality Check",
                description: "What AI can and can't actually do.",
                ideas: [
                    "Why ChatGPT still can't replace a junior dev",
                    "The AI capability nobody is talking about",
                    "AGI is further away than OpenAI admits",
                    "The AI job that pays $500K today",
                    "Why GPT-5 will disappoint you",
                ],
            },
            {
                id: "failures",
                label: "Startup Failures",
                description: "Billion-dollar companies that quietly died.",
                ideas: [
                    "How WeWork lost $47 billion",
                    "The unicorn that went to zero in 60 days",
                    "Why Quibi failed in 6 months",
                    "The social app that burned $400M",
                    "Theranos in 60 seconds",
                ],
            },
            {
                id: "predictions",
                label: "Future Predictions",
                description: "What the next 5 years actually look like.",
                ideas: [
                    "The job AI will replace next",
                    "Why remote work is already dead",
                    "The next platform after TikTok",
                    "How robotics kills the gig economy",
                    "The tech that makes phones obsolete",
                ],
            },
        ],
    },
    {
        template: "crypto",
        styles: [
            {
                id: "narratives",
                label: "Narrative Cycles",
                description: "The meta that actually moves bags.",
                ideas: [
                    "Why AI tokens are about to 5x",
                    "The RWA narrative nobody's priced in yet",
                    "Memecoin rotation — what's next after dogs",
                    "Why DePIN is the sleeper of this cycle",
                    "Gaming tokens in the final accumulation zone",
                ],
            },
            {
                id: "whales",
                label: "Whale Moves",
                description: "What smart money is actually doing on-chain.",
                ideas: [
                    "The wallet that bought the exact bottom",
                    "Why Jump Trading is accumulating this token",
                    "The rug signal that saved 10,000 wallets",
                    "How insiders front-ran the last Binance listing",
                    "The quiet accumulation no one is watching",
                ],
            },
            {
                id: "exits",
                label: "Exit Strategies",
                description: "How to not give it all back at the top.",
                ideas: [
                    "The exit signal that printed last cycle",
                    "Why HODL is the dumbest strategy ever",
                    "How to DCA out without regret",
                    "The on-chain metric that calls every top",
                    "Why CT influencers never sell (until it's too late)",
                ],
            },
        ],
    },
    {
        template: "scary",
        styles: [
            {
                id: "urban_legends",
                label: "Urban Legends",
                description: "The stories that won't stop spreading.",
                ideas: [
                    "The hotel room that shouldn't exist",
                    "The 911 call the police still won't release",
                    "The town that vanished in one night",
                    "The road sign that keeps appearing",
                    "The phone number you should never dial",
                ],
            },
            {
                id: "true_crime",
                label: "True Crime Mystery",
                description: "Cases that still don't add up.",
                ideas: [
                    "The Delphi murders — what investigators won't say",
                    "The disappearance that was staged",
                    "The note that solved a 40-year cold case",
                    "The suspect everyone ignored",
                    "The surveillance tape that changed everything",
                ],
            },
            {
                id: "paranormal",
                label: "Paranormal Events",
                description: "Real locations with unexplained history.",
                ideas: [
                    "The forest where compasses stop working",
                    "The lighthouse nobody built",
                    "The house where clocks run backwards",
                    "The island that appears once a decade",
                    "The tunnel with no end",
                ],
            },
        ],
    },
    {
        template: "history",
        styles: [
            {
                id: "civilizations",
                label: "Lost Civilizations",
                description: "Empires that vanished before we understood them.",
                ideas: [
                    "The city beneath the Sahara",
                    "Why the Minoans disappeared overnight",
                    "The civilization older than Egypt",
                    "The empire that ruled the Bronze Age seas",
                    "The forgotten kingdom in the Amazon",
                ],
            },
            {
                id: "battles",
                label: "Epic Battles",
                description: "The fights that reshaped the world.",
                ideas: [
                    "The siege that ended an empire",
                    "How 300 Spartans actually died",
                    "The night Constantinople fell",
                    "The charge that saved Europe",
                    "The naval battle nobody remembers",
                ],
            },
            {
                id: "untold",
                label: "Untold Stories",
                description: "Historical figures you were never taught.",
                ideas: [
                    "The emperor who faked his own death",
                    "The queen who led her army in person",
                    "The general who never lost a battle",
                    "The scientist erased from textbooks",
                    "The spy who changed two wars",
                ],
            },
        ],
    },
];

export function getIdeaStylesForTemplate(template: string): IdeaStyle[] {
    const preset = SHORTFORM_IDEA_PRESETS.find((p) => p.template === template);
    return preset?.styles ?? [];
}
