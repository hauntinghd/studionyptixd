import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
/** Injects Search Console HTML-tag verification when VITE_GOOGLE_SITE_VERIFICATION is set. */
function googleSiteVerificationPlugin() {
    return {
        name: 'google-site-verification',
        transformIndexHtml: function (html) {
            var token = String(process.env.VITE_GOOGLE_SITE_VERIFICATION || '').trim();
            if (!token)
                return html;
            var tag = "<meta name=\"google-site-verification\" content=\"".concat(token, "\" />");
            if (html.includes('google-site-verification'))
                return html;
            return html.replace('</head>', "    ".concat(tag, "\n</head>"));
        },
    };
}
export default defineConfig({
    plugins: [react(), googleSiteVerificationPlugin()],
    build: {
        target: 'es2019',
        // Split vendor deps into their own long-lived chunks so refreshes
        // don't redownload React + icon library + supabase on every app
        // change. Combined with React.lazy() on rarely-clicked panels
        // (DashboardPage.tsx) this drops cold-refresh payload materially.
        rollupOptions: {
            output: {
                manualChunks: function (id) {
                    if (!id.includes('node_modules'))
                        return;
                    if (id.includes('react-dom') || id.includes('scheduler'))
                        return 'vendor-react';
                    if (id.includes('/react/') || id.includes('react\\'))
                        return 'vendor-react';
                    if (id.includes('lucide-react'))
                        return 'vendor-icons';
                    if (id.includes('@supabase'))
                        return 'vendor-supabase';
                    return 'vendor';
                },
            },
        },
        // Don't warn about chunk sizes under 800 KB — we've already
        // code-split what we can; the long tail is intentional bundle
        // content, not a config miss.
        chunkSizeWarningLimit: 800,
    },
    server: {
        port: 8080,
        proxy: {
            '/api': {
                target: 'http://127.0.0.1:10000',
                changeOrigin: true,
                ws: true,
            },
        },
    },
});
