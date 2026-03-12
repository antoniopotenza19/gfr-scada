import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..schemas import SaleChartsResponse, SaleCompressorActivityResponse
from ..services.sale_charts import fetch_sale_chart_timeseries, fetch_sale_compressor_activity

router = APIRouter(prefix="/api/sale")
LOGGER = logging.getLogger(__name__)


@router.get("/{sale_id}/timeseries", response_model=SaleChartsResponse)
def sale_timeseries(
    sale_id: str,
    range_key: Optional[str] = Query(None, alias="range"),
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
    max_points: int = Query(360, ge=60, le=720),
):
    try:
        payload = fetch_sale_chart_timeseries(
            identifier=sale_id,
            range_key=range_key,
            from_ts=from_ts,
            to_ts=to_ts,
            max_points=max_points,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    LOGGER.debug(
        "sale_charts_timeseries sale=%s range=%s from=%s to=%s granularity=%s points=%s",
        sale_id,
        payload["requested_range"],
        payload["from_ts"],
        payload["to_ts"],
        payload["granularity"],
        len(payload["points"]),
    )
    return payload


@router.get("/{sale_id}/compressors/activity", response_model=SaleCompressorActivityResponse)
def sale_compressor_activity(
    sale_id: str,
    range_key: Optional[str] = Query(None, alias="range"),
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
):
    try:
        payload = fetch_sale_compressor_activity(
            identifier=sale_id,
            range_key=range_key,
            from_ts=from_ts,
            to_ts=to_ts,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    LOGGER.debug(
        "sale_charts_compressors sale=%s range=%s from=%s to=%s granularity=%s items=%s",
        sale_id,
        payload["requested_range"],
        payload["from_ts"],
        payload["to_ts"],
        payload["granularity"],
        len(payload["items"]),
    )
    return payload
