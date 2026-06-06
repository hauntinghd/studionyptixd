import httpx, json
from pathlib import Path
env={}
for l in Path(r'D:/games/asd/runpod-serverless/.runpod.env').read_text().splitlines():
 if '=' in l and not l.strip().startswith('#'): k,v=l.split('=',1); env[k.strip()]=v.strip()
api=env['RUNPOD_API_KEY']; ep='30lt3s0grkw5le'
# get recent health
r=httpx.post(f'https://api.runpod.ai/v2/{ep}/run',headers={'Authorization':f'Bearer {api}'},json={'input':{'task':'health'}},timeout=60)
print('run', r.json())
jid=r.json().get('id')
import time; time.sleep(30)
s=httpx.get(f'https://api.runpod.ai/v2/{ep}/status/{jid}',headers={'Authorization':f'Bearer {api}'},timeout=60)
print('status', json.dumps(s.json(), indent=2))
