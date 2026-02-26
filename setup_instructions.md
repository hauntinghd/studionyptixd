# Google Cloud Setup Instructions

To allow the AI to analyze your YouTube analytics, you need to set up a project in the Google Cloud Console.

### 1. Create a Project
- Go to the [Google Cloud Console](https://console.cloud.google.com/).
- Create a new project named `YouTube Vibe Optimizer`.

### 2. Enable APIs
Enable the following APIs for your project:
- **YouTube Data API v3** (for fetching video lists, metadata)
- **YouTube Analytics API** (for fetching retention and performance data)
- **YouTube Reporting API** (optional, but good for bulk data)

### 3. Configure OAuth Consent Screen
- Go to "APIs & Services" > "OAuth consent screen".
- Choose "External".
- Add your email and fill in the required app information.
- **Scopes**: Add the following scopes:
    - `https://www.googleapis.com/auth/youtube.readonly`
    - `https://www.googleapis.com/auth/yt-analytics.readonly`
- **Test Users**: Add your own YouTube account email as a test user.

### 4. Create Credentials
- Go to "APIs & Services" > "Credentials".
- Click "Create Credentials" > "OAuth client ID".
- Choose "Desktop app".
- Name it `Vibe Optimizer CLI`.
- Click "Create" and then **Download the JSON file**.
- Rename the downloaded file to `client_secrets.json` and place it in this project folder.

---

### Once you have `client_secrets.json` in the folder, let me know!
I will then provide the script to generate your `token.json` which will allow me to start the analysis.
