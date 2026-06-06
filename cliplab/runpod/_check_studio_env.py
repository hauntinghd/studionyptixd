import httpx, json
from pathlib import Path
env={}
for l in Path(r'D:/games/asd/runpod-serverless/.runpod.env').read_text().splitlines():
 if '=' in l and not l.strip().startswith('#'): env[l.split('=')[0].strip()]=l.split('=',1)[1].strip()
api=env['RUNPOD_API_KEY']
tid=next(e['templateId'] for e in httpx.post(f'https://api.runpod.io/graphql?api_key={api}',json={'query':'query { myself { endpoints { id templateId name } } }'}).json()['data']['myself']['endpoints'] if e['id']==env['RUNPOD_ENDPOINT_ID'])
tmpl=httpx.post(f'https://api.runpod.io/graphql?api_key={api}',json={'query':'query($id:String!){podTemplate(id:$id){env{key value}}}', 'variables':{'id':tid}}).json()
keys={e['key']:e['value'] for e in tmpl['data']['podTemplate']['env']}
for k in ['CLIPLAB_VIRALITY_BACKEND','CLIPLAB_REFRAME_BACKEND','RUNPOD_CLIPLAB_ENDPOINT_ID']:
 print(k, keys.get(k,'MISSING'))
