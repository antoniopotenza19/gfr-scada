from fastapi import APIRouter

from ..services.energysaving_runtime import csv_db_ingestor_service

router = APIRouter(prefix="/api/ingest")


@router.get("/status")
def ingest_status(plant: str | None = None):
    runtime_rows = csv_db_ingestor_service.runtime_jobs(plant=plant)
    return {
        "running": csv_db_ingestor_service.is_running,
        "active_jobs_count": csv_db_ingestor_service.active_jobs_count,
        "count": len(runtime_rows),
        "items": runtime_rows,
    }


@router.get("/sources")
def ingest_sources():
    return csv_db_ingestor_service.sources()


@router.post("/start")
async def ingest_start():
    started = await csv_db_ingestor_service.start()
    return {"running": csv_db_ingestor_service.is_running, "started": started}


@router.post("/stop")
async def ingest_stop():
    stopped = await csv_db_ingestor_service.stop()
    return {"running": csv_db_ingestor_service.is_running, "stopped": stopped}
