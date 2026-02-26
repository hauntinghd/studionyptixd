@echo off
taskkill /F /IM python.exe /T
python -u shadow_v8_hyper.py > sniper_out.log 2>&1
