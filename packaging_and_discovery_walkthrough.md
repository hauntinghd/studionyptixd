# Viral Shorts Studio - Packaging and Sniper Discovery Walkthrough

This document outlines the steps taken to successfully package the Viral Shorts Studio application and enhance the Polymarket 5m Sniper Bot's discovery mechanism.

## 1. Polymarket 5m Sniper Discovery

The goal was to reliably discover and track 5-minute interval markets on Polymarket.

### Improvements:
- **Search-V2 Integration**: Refactored the `discover_markets` function to use the `search-v2` API, which is more accurate for finding specifically timed markets like "5m" intervals.
- **Parallel Detail Fetching**: Since the search API doesn't always provide the `clobTokenIds` required for price tracking, the bot now fetches detailed information for each market ID in parallel using `asyncio.gather`.
- **Robustness**: Added fallback search queries and improved parsing logic to handle various API response formats.
- **Verification**: Added detailed logging to monitor price sums and signal detection. The bot is currently tracking 30 active 5m markets and successfully detecting nano-arbitrage opportunities and extreme odds (Reversal Hunter).

## 2. Viral Shorts Studio - Desktop Packaging

The goal was to package the React/FastAPI application into a single standalone executable.

### Improvements:
- **Spec File Correction**: Fixed the `launcher.spec` file by correcting the `datas` path list, ensuring PyInstaller can find the frontend `dist` directory and `client_secrets.json`.
- **Resource Path Handling**: Updated `backend.py` with a `get_resource_path` helper function. This function dynamically resolves asset paths, working correctly both in development and when bundled in a PyInstaller temp folder (`_MEIPASS`).
- **Build Success**: Successfully executed the PyInstaller build, resulting in a 313 MB standalone executable: `dist/ViralShorts-Pro.exe`.

## 3. How to Run

### Running the Sniper Bot:
- Use `run_watcher.bat` or run `python shadow_v8_hyper.py` directly.
- Monitor `shadow_hyper_v8.log` for real-time market discovery and signal detection.

### Running the Desktop App:
- Launch `dist/ViralShorts-Pro.exe` to run the bundled Viral Shorts Studio.
- The app will start the FastAPI backend on a background thread and open a native window using `pywebview`.
