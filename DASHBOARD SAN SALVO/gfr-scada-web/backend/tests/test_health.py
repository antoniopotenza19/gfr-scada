from datetime import datetime

from app.models import IngestState, Measurement


def test_api_health(client_and_db):
    client, Session = client_and_db

    db = Session()
    db.add(Measurement(plant='DEMO_PLANT', room=None, signal='PT-060', value=7.1, unit='bar', ts=datetime.utcnow()))
    db.commit()
    db.close()

    r = client.get('/api/health')
    assert r.status_code == 200
    body = r.json()

    assert body['db_connected'] is True
    assert 'now_utc' in body
    assert body['measurements_latest_ts'].get('DEMO_PLANT')
    assert body['ingest_running'] is False
    assert isinstance(body['active_jobs_count'], int)


def test_ingest_status_runtime_fields(client_and_db):
    client, Session = client_and_db

    db = Session()
    db.add(
        IngestState(
            source_url='http://dummy/2026/01/01',
            plant='DEMO_PLANT',
            filename='01-01-DEMO_PLANT.csv',
            last_modified=datetime.utcnow(),
            last_byte_offset=1024,
            last_ts=datetime.utcnow(),
        )
    )
    db.commit()
    db.close()

    r = client.get('/api/ingest/status')
    assert r.status_code == 200
    body = r.json()

    assert 'active_jobs_count' in body
    assert body['count'] == 1
    item = body['items'][0]
    assert 'last_success_ts' in item
    assert 'last_insert_count' in item
    assert 'last_error' in item
