from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import func, select

from ..services.energysaving_runtime import (
    csv_db_ingestor_service,
    energysaving_session,
    get_energysaving_schema,
)

router = APIRouter(prefix='/api')


@router.get('/health')
def api_health():
    db_connected = True
    measurements_latest_ts: dict[str, str | None] = {}
    try:
        schema = get_energysaving_schema()
        with energysaving_session() as session:
            session.execute(select(1))
            latest_per_sala = (
                session.execute(
                    select(schema.registrazioni_sale.c.idSala, func.max(schema.registrazioni_sale.c.timestamp))
                    .group_by(schema.registrazioni_sale.c.idSala)
                )
                .all()
            )
            sala_codes = dict(
                session.execute(select(schema.sale.c.idSala, schema.sale.c.codice)).all()
            )
            measurements_latest_ts = {
                str(sala_codes.get(sala_id, sala_id)): ts.isoformat() if ts else None
                for sala_id, ts in latest_per_sala
            }
    except Exception:
        db_connected = False

    return {
        'db_connected': db_connected,
        'now_utc': datetime.now(timezone.utc).isoformat(),
        'measurements_latest_ts': measurements_latest_ts,
        'ingest_running': csv_db_ingestor_service.is_running,
        'active_jobs_count': csv_db_ingestor_service.active_jobs_count,
    }
