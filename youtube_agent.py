import os
import json
import datetime
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors

# Scopes for YouTube Data and Analytics
SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly"
]

class YouTubeVibeAgent:
    def __init__(self, secrets_file="client_secrets.json", token_file="token.json"):
        self.secrets_file = secrets_file
        self.token_file = token_file
        self.youtube = None
        self.analytics = None

    def authenticate(self):
        """Authenticates the user and saves/loads the token."""
        flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
            self.secrets_file, SCOPES
        )
        credentials = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open(self.token_file, 'w') as token:
            token.write(credentials.to_json())
        
        self.youtube = googleapiclient.discovery.build("youtube", "v3", credentials=credentials)
        self.analytics = googleapiclient.discovery.build("youtubeAnalytics", "v2", credentials=credentials)
        print("Successfully authenticated!")

    def load_credentials(self):
        """Loads credentials from token.json if available."""
        if os.path.exists(self.token_file):
            from google.oauth2.credentials import Credentials
            from google.auth.transport.requests import Request
            try:
                credentials = Credentials.from_authorized_user_file(self.token_file, SCOPES)
                if credentials.expired and credentials.refresh_token:
                    credentials.refresh(Request())
                    with open(self.token_file, 'w') as token:
                        token.write(credentials.to_json())
                self.youtube = googleapiclient.discovery.build("youtube", "v3", credentials=credentials)
                self.analytics = googleapiclient.discovery.build("youtubeAnalytics", "v2", credentials=credentials)
                return True
            except Exception as e:
                print(f"Error loading token: {e}")
                return False
        return False

    def get_top_performing_videos(self, max_results=5):
        """Fetches the top N videos based on views in the last 30 days."""
        # Note: In a real scenario, we'd query analytics, but for the 'video list' we use Data API
        request = self.youtube.channels().list(mine=True, part="contentDetails")
        response = request.execute()
        items = response.get('items', [])
        if not items:
            return {"items": []}
        uploads_playlist_id = items[0]['contentDetails']['relatedPlaylists']['uploads']
        
        request = self.youtube.playlistItems().list(
            playlistId=uploads_playlist_id,
            part="snippet,contentDetails",
            maxResults=max_results
        )
        return request.execute()

    def get_retention_data(self, video_id):
        """Fetches audience retention data for a specific video."""
        # Analytics query for retention
        # This requires the YouTube Analytics API
        now = datetime.datetime.now()
        start_date = (now - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        end_date = now.strftime('%Y-%m-%d')

        request = self.analytics.reports().query(
            ids='channel==MINE',
            startDate=start_date,
            endDate=end_date,
            metrics='audienceWatchRatio',
            dimensions='elapsedVideoTimeRatio',
            filters=f'video=={video_id}'
        )
        return request.execute()

if __name__ == "__main__":
    agent = YouTubeVibeAgent()
    if agent.load_credentials():
        print("Loaded existing credentials.")
        # Example: get info
        videos = agent.get_top_performing_videos()
        print(json.dumps(videos, indent=2))
    else:
        print("Credentials not found. Please run the authentication flow.")
        # agent.authenticate() # This requires a browser UI, so we provide it as a separate command for the user.
