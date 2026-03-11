import logging

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/ingest")
LOGGER = logging.getLogger(__name__)


@router.get("/csv")
def ingest_csv():
    LOGGER.warning("legacy_manual_ingest_disabled")
    raise HTTPException(
        status_code=410,
        detail="Legacy manual CSV ingest is disabled. Use the background CSV -> DB ingestor on gfr_energysaving.",
    )
