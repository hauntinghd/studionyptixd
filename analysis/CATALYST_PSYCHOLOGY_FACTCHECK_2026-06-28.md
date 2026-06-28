# Catalyst Psychology Trend Fact Check - 2026-06-28

Source artifact: `factcheck_youtube_psychology.json`

Method:

- Ran the fact check inside the deployed Fly machine so the production YouTube API key pool was used.
- Queried the same public YouTube Data API wrapper used by Studio/Catalyst.
- Hydrated returned video IDs through `videos.list` to verify view, like, comment, and duration values.
- Treat "trending", "high search volume", and "low competition" as unsupported unless the data path explicitly returns enough evidence for those claims.

Important runtime note:

- A later expanded search run pushed the YouTube quota to the daily cap, so this report relies on the complete successful production-key result saved in `factcheck_youtube_psychology.json`.
- The expanded run did confirm the quota guard is working: later searches were refused or served stale cache instead of silently pretending fresh data existed.

## Verdict Table

| Topic claim | Verdict | Production YouTube evidence |
|---|---|---|
| "Why Men Go Silent After Intimacy" - 23.6M views | False / overstated | Top hydrated result in the saved check was 2,582 views, not 23.6M. |
| "When a Guy Is in Love With You" - 7M views | Supported | Hydrated short had 7,056,491 views. |
| "Why Men Push Away When They Care" - trending/high volume | Partially supported topic, unsupported trend claim | Strongest hydrated result was 197,733 views. Do not call it high-volume without broader demand data. |
| "The Real Reason Men Fear Commitment" | Weak support | Hydrated results were low-view in this exact search; strongest was 1,513 views. |
| "Attractive Traits in Girls" - 5M views | False / overstated for exact claim | Strongest hydrated result was 1,055,089 views. |
| "Girls Attraction Psychology Facts" - 5.2M views | Directionally supported | Strongest hydrated result was 4,212,632 views, close enough to support the topic but not the exact number. |
| "Psychology Facts About Girls" - 6.3M views | Supported | Hydrated short had 6,287,295 views. |
| "Why Women Stay in Toxic Situations" - trending | Unsupported | Exact search results were very weak in the saved run. |
| "If You Self-Sabotage All Your Relationships, Watch This" - 152k views / 11.9k likes | Understated / topic supported | Search found related self-sabotage result at 451,155 views / 22,840 likes. The exact short claim from the earlier run also matched about 152k / 11.9k. |
| "8 Signs You're Secretly Toxic to Yourself" - 2.3k views | Supported | Hydrated result had 2,298 views. |
| "Why You Sabotage Good Things Happening to You" | Supported as broader theme | Strong related result had 1,203,194 views. |
| "The Self-Destructive Love Trap" - trending | Unsupported | Hydrated result had 661 views. |
| "Most Psychopaths Have These 3 Characteristics" - 7.7M views / 304k likes | Supported | Hydrated Big Think short had 7,732,140 views / 304,949 likes. |
| "The Narcissist's Fake Apology" - 7.9M views / 243k likes | Supported | Hydrated Caroline Strawson short had 7,927,557 views / 243,910 likes. |
| "How to Spot a Manipulator" - massive/trending | Supported as topic, not as exact framing | Related manipulation result had 2,538,765 views / 96,069 likes. |
| "Why Narcissists Always Come Back" | Weak-to-moderate support | Strongest saved result was 78,945 views. |

## Corrected Catalyst Takeaway

The original Studio Agent trend report was partly grounded but too confident.

Supported angles:

- Male/female psychology can work, especially short "signs/facts" packaging.
- Narcissism, psychopathy, manipulation recognition, and self-sabotage have strong public YouTube precedent.
- Recognition/diagnosis framing is safer and more marketable than teaching manipulation.
- Shorts can dominate the evidence base; long-form and shorts should not be mixed without format labels.

Overclaims to avoid:

- Do not claim "fresh YouTube search data" unless the result includes the timestamp/key/cache status and hydrated video IDs.
- Do not claim "trending" or "high search volume" from a single relevance search.
- Do not reuse view-count precedents from adjacent videos as if they belonged to the exact title.
- Do not report a topic as "low competition" unless the query includes enough competitor count and recency data.

## Safer 5 Topics To Test

1. "When a Guy Is in Love With You" - proven shorts precedent around 7M views.
2. "Psychology Facts About Girls" - proven shorts precedent around 6.3M views.
3. "Most Psychopaths Have These 3 Characteristics" - proven recognition/education precedent around 7.7M views.
4. "The Narcissist's Fake Apology" - proven toxic-pattern recognition precedent around 7.9M views.
5. "Why You Self-Sabotage Good Relationships" - broader self-sabotage theme has strong related evidence around 451k to 1.2M views.

Use "Why Men Go Silent After Getting Close" only as an experimental angle, not as a proven 23.6M precedent, because the saved fact check did not verify that number.

## Required Catalyst Behavior

Catalyst/Studio Agent should write claims in this shape:

```text
Found public YouTube evidence for [topic]:
- [title], [channel], [views], [likes], [duration], [published_at], [url]
- search order/mode: relevance or viewCount
- cache status: fresh/configured or stale-cache

Conclusion:
[supported / weak / unsupported], with a reason.
```

If quota is exhausted, Catalyst should say that directly and use cached/fallback data with a stale-data label.
