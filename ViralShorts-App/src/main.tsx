import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.tsx'
import { relayDesktopAuthToTauri } from './desktopAuthRelay.ts'
import './index.css'

// Embed the immutable web release identifier into the production bundle. The
// deploy gate fetches the exact asset served by the custom domain and refuses
// to publish unless it contains the candidate ID.
const studioBuildId = String(import.meta.env.VITE_STUDIO_BUILD_ID || '').trim()
if (studioBuildId) {
    document.documentElement.dataset.studioBuild = studioBuildId
}

if (!relayDesktopAuthToTauri()) {
    ReactDOM.createRoot(document.getElementById('root')!).render(
        <React.StrictMode>
            <App />
        </React.StrictMode>,
    )
}
