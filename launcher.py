import webview
import threading
import uvicorn
import os
import sys
import time
from backend import app

def start_server():
    uvicorn.run(app, host="127.0.0.1", port=8081)

if __name__ == '__main__':
    # Build a thread for the FastAPI server
    t = threading.Thread(target=start_server)
    t.daemon = True
    t.start()

    # Wait for server to boot
    time.sleep(2)

    # Launch the native window
    webview.create_window('Viral Shorts Studio Pro', 'http://127.0.0.1:8081', width=1280, height=800)
    webview.start()
