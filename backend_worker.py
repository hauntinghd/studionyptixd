import asyncio
import logging

import backend
from backend_queue import (
    embedded_worker_enabled,
    init_queue_runtime,
    run_generation_consumer,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nyptid-worker")


async def _run_worker_loop():
    if embedded_worker_enabled():
        raise RuntimeError(
            "Standalone worker refused: RUN_EMBEDDED_WORKER enables the API-owned consumer"
        )
    init_queue_runtime(backend.jobs, backend.log)
    task_map = {
        "run_generation_pipeline": backend.run_generation_pipeline,
        "_run_creative_pipeline": backend._run_creative_pipeline,
        "_run_longform_pipeline": backend._run_longform_pipeline,
        "run_clone_pipeline": backend.run_clone_pipeline,
    }
    log.info("Standalone Redis production consumer started")
    await run_generation_consumer(task_map, recover_inflight=True)


if __name__ == "__main__":
    asyncio.run(_run_worker_loop())
