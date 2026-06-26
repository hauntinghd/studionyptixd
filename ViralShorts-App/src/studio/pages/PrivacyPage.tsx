import { Logo } from '../shared';

function H2({ children }: { children: React.ReactNode }) {
    return <h2 className="mt-10 text-xl font-semibold text-white">{children}</h2>;
}

function H3({ children }: { children: React.ReactNode }) {
    return <h3 className="mt-6 text-base font-semibold text-gray-200">{children}</h3>;
}

function P({ children }: { children: React.ReactNode }) {
    return <p className="mt-3 text-sm leading-relaxed text-gray-300">{children}</p>;
}

function UL({ children }: { children: React.ReactNode }) {
    return <ul className="mt-3 list-disc space-y-1.5 pl-6 text-sm leading-relaxed text-gray-300">{children}</ul>;
}

function Code({ children }: { children: React.ReactNode }) {
    return (
        <code className="rounded border border-white/[0.08] bg-black/40 px-1.5 py-0.5 text-[0.85em] text-gray-100">
            {children}
        </code>
    );
}

function A({ href, children }: { href: string; children: React.ReactNode }) {
    const external = /^https?:/i.test(href);
    return (
        <a
            href={href}
            className="text-violet-300 underline underline-offset-4 transition hover:text-violet-200"
            {...(external ? { target: '_blank', rel: 'noopener noreferrer' } : {})}
        >
            {children}
        </a>
    );
}

export default function PrivacyPage() {
    return (
        <div className="min-h-screen bg-[#09090b] text-gray-100">
            <header className="border-b border-white/[0.06] bg-black/30">
                <div className="mx-auto flex max-w-3xl items-center justify-between px-6 py-5">
                    <a href="/" className="flex items-center gap-3 text-white">
                        <Logo />
                    </a>
                    <nav className="flex items-center gap-4 text-xs text-gray-400">
                        <a href="/" className="transition hover:text-gray-200">Home</a>
                        <a href="/terms" className="transition hover:text-gray-200">Terms</a>
                    </nav>
                </div>
            </header>

            <main className="mx-auto max-w-3xl px-6 py-14">
                <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-violet-300">NYPTID Studio</p>
                <h1 className="mt-2 text-3xl font-bold tracking-tight text-white sm:text-4xl">Privacy Policy</h1>
                <p className="mt-2 text-sm text-gray-500">Last updated: June 25, 2026</p>

                <P>
                    NYPTID Studio ("Studio," "we," "us") is a creator-tools platform operated by NYPTID Industries.
                    This policy explains what information Studio collects, how we use it, who we share it with, and how you can control it.
                </P>

                <H2>1. What We Collect</H2>
                <H3>Account information</H3>
                <UL>
                    <li>Email address (via Supabase authentication or Google Sign-In)</li>
                    <li>Plan level and subscription status (via PayPal / Stripe payment records)</li>
                    <li>Profile role (creator, admin)</li>
                </UL>

                <H3>YouTube channel data (only if you connect a channel)</H3>
                <P>
                    If you connect a YouTube channel, Studio retrieves data from your channel through Google's OAuth-authenticated APIs.
                    The scopes we request are:
                </P>
                <UL>
                    <li><Code>youtube.readonly</Code> — channel and video metadata (titles, descriptions, tags, thumbnails, durations, counts)</li>
                    <li><Code>yt-analytics.readonly</Code> — per-video retention, traffic sources, watch-time, impressions, and click-through rate</li>
                    <li><Code>youtube.force-ssl</Code> — HTTPS-only API operations required for token auth</li>
                    <li><Code>youtube.upload</Code> — only used if you click "Publish to YouTube" to upload a Studio-made video to your own channel</li>
                </UL>

                <H3>Generated content you create in Studio</H3>
                <UL>
                    <li>Scripts, scenes, thumbnails, and renders you produce</li>
                    <li>Your prompts, reference images, and voice selections</li>
                    <li>Edits, approvals, rejections, tool results, errors, and training feedback signals</li>
                </UL>

                <H3>Technical data</H3>
                <UL>
                    <li>IP address and browser user-agent (request logs)</li>
                    <li>Usage analytics (page views, feature events — aggregated and non-identifying)</li>
                </UL>

                <H2>2. How We Use Your Data</H2>
                <UL>
                    <li><strong className="text-white">To provide Studio's features.</strong> Your YouTube data powers the Catalyst research and learning engine that suggests scripts, titles, and thumbnails based on what has performed well on your own channel.</li>
                    <li><strong className="text-white">To publish on your behalf.</strong> The <Code>youtube.upload</Code> scope is only used when you explicitly click "Publish" — never automatically.</li>
                    <li><strong className="text-white">To operate the service.</strong> Authentication, plan enforcement, billing reconciliation, and anti-abuse.</li>
                    <li><strong className="text-white">To improve Studio.</strong> Operational metrics help us diagnose and improve the service.</li>
                    <li><strong className="text-white">Optional NYPTID model training.</strong> If you explicitly enable training contribution in Settings, Studio may retain your prompts, uploaded references, generated outputs, edits, tool results, approvals, rejections, and feedback as versioned training examples.</li>
                </UL>

                <H3>Training contribution controls</H3>
                <UL>
                    <li>General model-training collection is disabled until you explicitly opt in from Settings.</li>
                    <li>You can separately decide whether authorized NYPTID reviewers may inspect selected examples for quality control.</li>
                    <li>Secrets, authentication tokens, payment credentials, authorization headers, and detected contact information are redacted from training exports.</li>
                    <li>YouTube OAuth-authorized analytics are quarantined and excluded from general model-training exports.</li>
                    <li>You can disable future collection or delete previously collected training examples from Settings.</li>
                </UL>

                <H2>3. How YouTube Data Is Handled Specifically</H2>
                <div className="mt-4 rounded-2xl border border-violet-500/20 bg-violet-500/[0.06] px-5 py-4 text-sm leading-relaxed text-violet-100">
                    <strong className="text-white">Studio's use of information received from Google APIs will adhere to the{' '}
                        <A href="https://developers.google.com/terms/api-services-user-data-policy">Google API Services User Data Policy</A>, including the Limited Use requirements.</strong>
                </div>
                <UL>
                    <li><strong className="text-white">Scope of use.</strong> YouTube-derived data is used only to serve the creator who authorized access, inside that creator's own Studio dashboard.</li>
                    <li><strong className="text-white">Never sold.</strong> We do not sell, rent, or share YouTube data with third parties for advertising or any other purpose.</li>
                    <li><strong className="text-white">No human review.</strong> No NYPTID employee reads your YouTube data outside of what is strictly required to diagnose a support issue you've explicitly reported, or to comply with law.</li>
                    <li><strong className="text-white">Server-side tokens.</strong> Refresh tokens are stored server-side only, never exposed to the browser or any third party. Access tokens are refreshed server-to-server and are not persisted beyond the request lifecycle.</li>
                    <li><strong className="text-white">Purpose-limited storage.</strong> YouTube analytics and authorized channel statistics may be retained while authorization remains active to power creator-facing Catalyst and Studio Agent features. Authorization and data validity are periodically rechecked.</li>
                    <li><strong className="text-white">Not used for general model training.</strong> YouTube OAuth-authorized data is segregated from NYPTID model-training datasets.</li>
                    <li><strong className="text-white">No redistribution.</strong> Studio never re-exposes one creator's private data to another creator or to the public internet.</li>
                </UL>

                <H2>4. Data Retention &amp; Deletion</H2>
                <UL>
                    <li>YouTube-authorized data: retained only while needed for connected creator-facing features and refreshed or revalidated according to YouTube API requirements.</li>
                    <li>Generated content you create: retained while your account is active; deleted within 30 days of account deletion.</li>
                    <li>Opted-in model-training examples: retained while training consent remains active, subject to your deletion request and applicable legal requirements.</li>
                    <li>Account records: retained for as long as required by billing, tax, and fraud-prevention regulations.</li>
                    <li><strong className="text-white">Disconnect &amp; delete:</strong> You can disconnect your YouTube channel at any time from <em>Settings → YouTube → Disconnect</em>. This revokes the refresh token at Google and deletes the stored token on our side. You can also revoke access directly from your{' '}
                        <A href="https://myaccount.google.com/permissions">Google Account → Third-party apps with account access</A> page.</li>
                    <li><strong className="text-white">Account deletion:</strong> Email{' '}
                        <A href="mailto:atlassetter@nyptidindustries.com">atlassetter@nyptidindustries.com</A> to request full account deletion. YouTube-authorized data is deleted as soon as possible and within 7 calendar days; other generated account content is deleted within 30 days unless retention is legally required.</li>
                </UL>

                <H2>5. Who We Share Data With</H2>
                <P>Studio uses the following sub-processors to operate the service:</P>
                <UL>
                    <li><strong className="text-white">Supabase</strong> — authentication and authoritative data storage</li>
                    <li><strong className="text-white">RunPod</strong> — GPU inference workers</li>
                    <li><strong className="text-white">Fal.ai</strong> — managed AI model inference (image, video, audio, LLM)</li>
                    <li><strong className="text-white">PayPal</strong> — payment processing</li>
                    <li><strong className="text-white">Vercel</strong> — frontend hosting</li>
                    <li><strong className="text-white">Google (YouTube Data API, YouTube Analytics API)</strong> — only when you connect a YouTube channel</li>
                </UL>
                <P>We do not share your data with advertisers or data brokers. We do not sell your data.</P>

                <H2>6. Security</H2>
                <UL>
                    <li>All traffic to Studio is encrypted in transit (TLS).</li>
                    <li>Refresh tokens and API keys are stored in environment-isolated secret stores, not in application code.</li>
                    <li>Access to production systems is limited to Studio operators and logged.</li>
                </UL>

                <H2>7. Your Rights</H2>
                <P>
                    Depending on your region, you may have the right to access, correct, export, or delete your personal data. Email{' '}
                    <A href="mailto:atlassetter@nyptidindustries.com">atlassetter@nyptidindustries.com</A> to exercise any of these rights.
                </P>

                <H2>8. Children</H2>
                <P>Studio is not directed at users under 13. We do not knowingly collect data from children under 13.</P>

                <H2>9. Changes to This Policy</H2>
                <P>
                    Material changes to this policy will be announced in-app and via email to account holders at least 14 days before taking effect.
                    The "Last updated" date above is authoritative.
                </P>

                <H2>10. Contact</H2>
                <UL>
                    <li><strong className="text-white">Operator:</strong> NYPTID Industries</li>
                    <li><strong className="text-white">Email:</strong> <A href="mailto:atlassetter@nyptidindustries.com">atlassetter@nyptidindustries.com</A></li>
                    <li><strong className="text-white">Site:</strong> <A href="https://studio.nyptidindustries.com">https://studio.nyptidindustries.com</A></li>
                </UL>

                <footer className="mt-16 border-t border-white/[0.06] pt-6 text-xs text-gray-500">
                    © 2026 NYPTID Industries. All rights reserved. &nbsp;·&nbsp;
                    <a href="/terms" className="text-gray-400 transition hover:text-gray-200">Terms of Service</a> &nbsp;·&nbsp;
                    <a href="/" className="text-gray-400 transition hover:text-gray-200">Back to Studio</a>
                </footer>
            </main>
        </div>
    );
}
