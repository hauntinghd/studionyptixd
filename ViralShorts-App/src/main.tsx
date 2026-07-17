import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.tsx'
import { relayDesktopAuthToTauri } from './desktopAuthRelay.ts'
import './index.css'

if (!relayDesktopAuthToTauri()) {
    ReactDOM.createRoot(document.getElementById('root')!).render(
        <React.StrictMode>
            <App />
        </React.StrictMode>,
    )
}
