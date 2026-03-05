from datetime import datetime

from app.models import Measurement
from app.schemas import PlantSummary


def test_summary(client_and_db):
    client, Session = client_and_db

    db = Session()
    db.add(Measurement(plant='DEMO_PLANT', room='R1', signal='PT-060', value=7.2, unit='bar', ts=datetime.utcnow()))
    db.add(Measurement(plant='DEMO_PLANT', room='R1', signal='AT-061', value=-20.1, unit='C', ts=datetime.utcnow()))
    db.commit()
    db.close()

    r = client.get('/api/plants/DEMO_PLANT/summary')
    assert r.status_code == 200
    body = r.json()
    PlantSummary(**body)
    assert body['plant'] == 'DEMO_PLANT'
    assert 'PT-060' in body['signals']
    assert isinstance(body['last_update'], str)
    assert body['last_update']
    assert isinstance(body['signals']['PT-060']['ts'], str)


def test_summary_empty_payload_when_no_measurements(client_and_db):
    client, _ = client_and_db

    r = client.get('/api/plants/DEMO_PLANT/summary')
    assert r.status_code == 200
    body = r.json()
    PlantSummary(**body)
    assert body['plant'] == 'DEMO_PLANT'
    assert body['last_update'] is None
    assert body['signals'] == {}
    assert body['active_alarms'] == []
