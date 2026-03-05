from datetime import datetime

from app.routers import ingest as ingest_router
from app.services.csv_watcher import IngestResult


def test_ingest_csv_mock(client_and_db, monkeypatch):
    client, _ = client_and_db

    monkeypatch.setattr(
        ingest_router,
        'list_remote_csv_files',
        lambda *_args, **_kwargs: ['01-01-DEMO_PLANT.csv'],
    )
    monkeypatch.setattr(
        ingest_router,
        'ingest_remote_csv',
        lambda **_kwargs: IngestResult(
            source_url='http://dummy/2026/01/01',
            filename='01-01-DEMO_PLANT.csv',
            rows_parsed=1,
            signals=2,
            inserted=2,
            skipped=0,
            last_ts=datetime(2026, 1, 1, 0, 0, 0),
            last_modified=datetime(2026, 1, 1, 0, 0, 1),
            bytes_read=128,
        ),
    )

    r = client.get(
        '/api/ingest/csv',
        params={
            'plant': 'DEMO_PLANT',
            'date': '2026-01-01',
            'base_url': 'http://dummy',
        },
    )

    assert r.status_code == 200
    body = r.json()
    assert body['inserted'] > 0
    assert body['signals'] >= 2
