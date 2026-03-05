from datetime import date

from app.models import Plant
from app.services.csv_watcher import CsvWatcher, _extract_unit_from_header, resolve_plant_filename


def test_resolve_plant_filename_handles_spaces_and_underscores():
    files = [
        '03-03-SS2 Bassa Pressione.CSV',
        '03-03-SS1.CSV',
    ]
    day = date(2026, 3, 3)

    assert resolve_plant_filename('SS2 Bassa Pressione', day, files) == '03-03-SS2 Bassa Pressione.CSV'
    assert resolve_plant_filename('SS2_Bassa_Pressione', day, files) == '03-03-SS2 Bassa Pressione.CSV'
    assert resolve_plant_filename('SS1', day, files) == '03-03-SS1.CSV'


def test_sync_plants_from_files_adds_missing_plants(client_and_db):
    _, Session = client_and_db
    watcher = CsvWatcher(
        session_factory=Session,
        base_csv_url='http://dummy/shared',
        poll_seconds=5,
        plants=None,
    )

    db = Session()
    added = watcher._sync_plants_from_files(
        db,
        computed_day=date(2026, 3, 3),
        files=[
            '03-03-DEMO_PLANT.CSV',
            '03-03-SS1.CSV',
            '03-03-SS2 Bassa Pressione.CSV',
        ],
    )
    db.commit()

    names = [name for (name,) in db.query(Plant.name).order_by(Plant.name).all()]
    db.close()

    assert sorted(added) == ['SS1', 'SS2 Bassa Pressione']
    assert 'DEMO_PLANT' in names
    assert 'SS1' in names
    assert 'SS2 Bassa Pressione' in names


def test_extract_unit_from_header_supports_secondary_channel_suffix():
    signal, unit = _extract_unit_from_header('Pressione (bar) (2)')
    assert signal == 'Pressione (2)'
    assert unit == 'bar'

    signal, unit = _extract_unit_from_header('Temperatura (°C) (2)')
    assert signal == 'Temperatura (2)'
    assert unit == '°C'


def test_extract_unit_from_header_supports_pandas_duplicate_suffix():
    signal, unit = _extract_unit_from_header('Pressione (bar).1')
    assert signal == 'Pressione (2)'
    assert unit == 'bar'
