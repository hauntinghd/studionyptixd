@echo off
echo Building Viral Shorts Studio Pro EXE...
pip install pyinstaller pywebview
cd ViralShorts-App && npm run build
cd ..
pyinstaller --noconfirm --onefile --windowed --add-data "ViralShorts-App/dist;ViralShorts-App/dist" --add-data "client_secrets.json;." launcher.py
echo Done! Check the 'dist' folder for launcher.exe
pause
