// Per-idea heat score for the "Spark Script" wizard.
//
// First iteration: deterministic per-day hash so each style card shows a
// stable 60-95 score that rotates overnight. Users coming back tomorrow
// see different styles trending — the modal feels alive without needing
// a live backend feed.
//
// Swap this out for a real Catalyst-backed endpoint
// (`/api/templates/{niche}/heat-scores`) once the Catalyst trend signal
// pipeline can rank our 9 niches × 3 styles daily. That upgrade is
// transparent to the modal — same function signature.

function hash32(seed: string): number {
    let h = 0;
    for (let i = 0; i < seed.length; i++) {
        h = ((h << 5) - h) + seed.charCodeAt(i);
        h = h | 0;
    }
    return Math.abs(h);
}

function today(): string {
    return new Date().toISOString().slice(0, 10);
}

// Returns an integer 60-95. Deterministic per (niche, style, date).
export function computeHeatScore(nicheId: string, styleId: string, isoDate: string = today()): number {
    const h = hash32(`${nicheId}:${styleId}:${isoDate}`);
    return 60 + (h % 36);
}

// Pick a Tailwind color class keyed on score bands. Keeps the badge
// visually meaningful: mid-60s amber (lukewarm), 75-84 orange, 85+ red.
export function heatScoreColorClass(score: number): string {
    if (score >= 85) return "text-red-300";
    if (score >= 75) return "text-orange-300";
    return "text-amber-300";
}
