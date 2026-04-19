import { Logo } from '../shared';

function H2({ children }: { children: React.ReactNode }) {
    return <h2 className="mt-10 text-xl font-semibold text-white">{children}</h2>;
}

function P({ children }: { children: React.ReactNode }) {
    return <p className="mt-3 text-sm leading-relaxed text-gray-300">{children}</p>;
}

function UL({ children }: { children: React.ReactNode }) {
    return <ul className="mt-3 list-disc space-y-1.5 pl-6 text-sm leading-relaxed text-gray-300">{children}</ul>;
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

export default function TermsPage() {
    return (
        <div className="min-h-screen bg-[#09090b] text-gray-100">
            <header className="border-b border-white/[0.06] bg-black/30">
                <div className="mx-auto flex max-w-3xl items-center justify-between px-6 py-5">
                    <a href="/" className="flex items-center gap-3 text-white">
                        <Logo />
                    </a>
                    <nav className="flex items-center gap-4 text-xs text-gray-400">
                        <a href="/" className="transition hover:text-gray-200">Home</a>
                        <a href="/privacy" className="transition hover:text-gray-200">Privacy</a>
                    </nav>
                </div>
            </header>

            <main className="mx-auto max-w-3xl px-6 py-14">
                <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-violet-300">NYPTID Studio</p>
                <h1 className="mt-2 text-3xl font-bold tracking-tight text-white sm:text-4xl">Terms of Service</h1>
                <p className="mt-2 text-sm text-gray-500">Last updated: April 19, 2026</p>

                <P>
                    These Terms of Service ("Terms") are a binding agreement between you and NYPTID Industries ("NYPTID," "we," "us") governing
                    your use of NYPTID Studio (the "Service"). By creating an account or using the Service, you agree to these Terms. If you
                    don't agree, don't use the Service.
                </P>

                <H2>1. Eligibility</H2>
                <P>
                    You must be at least 13 years old to use the Service. If you are between 13 and 18, you confirm that a parent or legal
                    guardian has reviewed these Terms on your behalf. You are responsible for complying with the laws of your jurisdiction.
                </P>

                <H2>2. Your Account</H2>
                <UL>
                    <li>You are responsible for maintaining the security of your login credentials.</li>
                    <li>You are responsible for all activity that occurs under your account.</li>
                    <li>You must provide accurate information when registering and keep it current.</li>
                    <li>One person or legal entity may maintain one free account. Additional accounts require a paid plan.</li>
                </UL>

                <H2>3. Subscription, Billing, and Refunds</H2>
                <UL>
                    <li>Paid plans are billed in advance on a recurring basis (monthly or annually) via PayPal or Stripe.</li>
                    <li>Plan fees are non-refundable except where required by law or at NYPTID's sole discretion. Refund requests can be submitted via <em>Settings → Billing → Request Refund</em>.</li>
                    <li>We may change plan pricing with at least 14 days' advance notice. Price changes apply at your next renewal.</li>
                    <li>Failure to pay may result in service suspension until the balance is resolved.</li>
                </UL>

                <H2>4. Credits &amp; Usage Limits</H2>
                <UL>
                    <li>Generation credits ("AC" / "Catalyst credits") are consumed by AI model calls. Credit costs are posted in-app before each generation.</li>
                    <li>Unused monthly credits do not roll over unless your plan explicitly includes rollover.</li>
                    <li>Top-up packs do not expire.</li>
                    <li>NYPTID reserves the right to rate-limit abusive usage patterns that would otherwise disrupt service for other creators.</li>
                </UL>

                <H2>5. Your Content</H2>
                <UL>
                    <li><strong className="text-white">You own your content.</strong> You retain all rights to scripts, scenes, images, videos, and other materials you create in Studio ("Your Content").</li>
                    <li>You grant NYPTID a non-exclusive, worldwide license to host, process, and display Your Content solely to provide the Service to you.</li>
                    <li>You are responsible for ensuring that Your Content does not infringe third-party rights and complies with these Terms.</li>
                </UL>

                <H2>6. Acceptable Use</H2>
                <P>You will not use the Service to:</P>
                <UL>
                    <li>Create content that is unlawful, defamatory, sexually explicit involving minors, or that promotes violence or hate toward protected classes.</li>
                    <li>Impersonate real individuals in ways that could mislead viewers (including deepfakes of identifiable people without their consent).</li>
                    <li>Infringe copyright, trademark, publicity, or other intellectual property rights.</li>
                    <li>Reverse-engineer, scrape, or exceed the rate limits of the Service.</li>
                    <li>Circumvent billing, credit caps, or plan limits.</li>
                    <li>Transmit malware, phishing content, or use the Service to attack other systems.</li>
                </UL>
                <P>Violation of this section may result in account suspension, content removal, and forfeiture of plan fees.</P>

                <H2>7. Third-Party Services</H2>
                <P>
                    Studio integrates with third-party services including (but not limited to) YouTube (Google), Supabase, RunPod, Fal.ai, PayPal, Vercel, and Stripe.
                    Your use of those services through Studio is also subject to their respective terms.
                </P>
                <P>
                    In particular, when you connect a YouTube channel you are also bound by the{' '}
                    <A href="https://www.youtube.com/t/terms">YouTube Terms of Service</A> and the{' '}
                    <A href="https://policies.google.com/privacy">Google Privacy Policy</A>.
                </P>

                <H2>8. Intellectual Property</H2>
                <UL>
                    <li>The Studio software, branding, UI, documentation, and model prompt engineering are owned by NYPTID.</li>
                    <li>You may not copy, modify, or redistribute the Service except as strictly permitted by these Terms.</li>
                </UL>

                <H2>9. Disclaimer</H2>
                <P>
                    The Service is provided "as is" and "as available" without warranties of any kind, express or implied. NYPTID disclaims all
                    warranties of merchantability, fitness for a particular purpose, and non-infringement to the maximum extent permitted by law.
                    AI-generated output can be incorrect, biased, or unsuitable — you are responsible for reviewing output before publishing.
                </P>

                <H2>10. Limitation of Liability</H2>
                <P>
                    To the maximum extent permitted by law, NYPTID's total liability arising out of or relating to the Service is limited to the
                    greater of (a) the amount you paid NYPTID in the 12 months preceding the claim, or (b) USD $100. NYPTID is not liable for
                    indirect, consequential, incidental, or punitive damages.
                </P>

                <H2>11. Termination</H2>
                <UL>
                    <li>You can cancel your subscription at any time from <em>Settings → Billing</em>.</li>
                    <li>We may suspend or terminate your account for violation of these Terms, fraud, chargeback abuse, or legal requirement.</li>
                    <li>On termination, your access to Studio ceases. Data deletion follows the <A href="/privacy">Privacy Policy</A>.</li>
                </UL>

                <H2>12. Changes to the Terms</H2>
                <P>
                    We may update these Terms from time to time. Material changes will be announced in-app and via email to account holders at
                    least 14 days before taking effect. Continued use of the Service after the effective date constitutes acceptance.
                </P>

                <H2>13. Governing Law</H2>
                <P>
                    These Terms are governed by the laws of the State of Florida, United States, without regard to its conflict-of-laws principles.
                    Disputes will be resolved in the state or federal courts located in Florida, and you consent to personal jurisdiction there.
                </P>

                <H2>14. Contact</H2>
                <UL>
                    <li><strong className="text-white">Operator:</strong> NYPTID Industries</li>
                    <li><strong className="text-white">Email:</strong> <A href="mailto:atlassetter@nyptidindustries.com">atlassetter@nyptidindustries.com</A></li>
                    <li><strong className="text-white">Site:</strong> <A href="https://studio.nyptidindustries.com">https://studio.nyptidindustries.com</A></li>
                </UL>

                <footer className="mt-16 border-t border-white/[0.06] pt-6 text-xs text-gray-500">
                    © 2026 NYPTID Industries. All rights reserved. &nbsp;·&nbsp;
                    <a href="/privacy" className="text-gray-400 transition hover:text-gray-200">Privacy Policy</a> &nbsp;·&nbsp;
                    <a href="/" className="text-gray-400 transition hover:text-gray-200">Back to Studio</a>
                </footer>
            </main>
        </div>
    );
}
