import asyncio
import logging

import backend
from backend_queue import (
    dequeue_generation_job,
    get_persisted_job_state,
    init_queue_runtime,
    persist_job_state,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nyptid-worker")


async def _run_worker_loop():
    init_queue_runtime(backend.jobs, backend.log)
    task_map = {
        "run_generation_pipeline": backend.run_generation_pipeline,
        "_run_creative_pipeline": backend._run_creative_pipeline,
        "run_clone_pipeline": backend.run_clone_pipeline,
    }
    log.info("Redis worker started")
    while True:
        try:
            payload = await dequeue_generation_job()
        except Exception as e:
            log.warning(f"Queue poll failed; retrying: {e}")
            await asyncio.sleep(1.0)
            continue
        if not payload:
            await asyncio.sleep(0.5)
            continue
        job_id = str(payload.get("job_id", "") or "").strip()
        task_name = str(payload.get("task_name", "") or "").strip()
        args = tuple(payload.get("args", []) or [])
        if not job_id or task_name not in task_map:
            log.warning(f"Skipping unknown payload: task={task_name} job_id={job_id}")
            continue
        try:
            seed = await get_persisted_job_state(job_id)
            if seed:
                backend.jobs[job_id] = seed
            else:
                backend.jobs.setdefault(job_id, {"status": "queued", "progress": 0})
            backend.jobs[job_id]["status"] = "processing"
            await persist_job_state(job_id, backend.jobs[job_id])
            await task_map[task_name](*args)
            await persist_job_state(job_id, backend.jobs.get(job_id, {}))
        except Exception as e:
            log.error(f"[{job_id}] Worker execution failed: {e}", exc_info=True)
            backend.jobs.setdefault(job_id, {})
            backend.jobs[job_id]["status"] = "error"
            backend.jobs[job_id]["error"] = str(e)
            await persist_job_state(job_id, backend.jobs[job_id])


if __name__ == "__main__":
    asyncio.run(_run_worker_loop())
