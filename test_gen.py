import httpx, json, time

resp = httpx.post("http://localhost:8081/api/generate", json={
    "template": "skeleton",
    "prompt": "Software Engineer vs Doctor salary",
    "resolution": "720p"
}, timeout=30)
print("Generate response:", resp.status_code, resp.text[:200])

if resp.status_code == 200:
    job_id = resp.json().get("job_id")
    if job_id:
        for i in range(30):
            time.sleep(3)
            s = httpx.get(f"http://localhost:8081/api/status/{job_id}", timeout=10)
            data = s.json()
            print(f"[{i}] {data.get('status')} {data.get('progress', 0)}% - {data.get('error', '')}")
            if data.get("status") in ("complete", "error"):
                break
