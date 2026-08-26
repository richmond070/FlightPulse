import asyncio
from arq import create_pool
from worker.settings import get_redis_settings, QUEUE_NAME

async def main():
    pool = await create_pool(get_redis_settings())
    bad_batch = {
        "events": [
            {"source": "opensky", "ingested_at": "2026-08-26T10:00:00Z", "ingestion_id": "bad-1"}
        ]
    }
    job = await pool.enqueue_job("process_telemetry_batch", bad_batch, _queue_name=QUEUE_NAME)
    result = await job.result(timeout=10)
    print("job result:", result)
    await pool.aclose()

asyncio.run(main())
