"""
Seed script: creates admin user, two plants and rooms.
Run: `python app/scripts/seed.py` inside container or virtualenv.
"""
import os
import sys
from dotenv import load_dotenv
load_dotenv()
# ensure project root is on path when running as script
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import User, Plant, Room
from app.auth import hash_password

DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise SystemExit('DATABASE_URL not set')

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def seed():
    Base.metadata.create_all(bind=engine)
    db = Session()
    if not db.query(User).filter(User.username == 'admin').first():
        admin = User(username='admin', hashed_password=hash_password('admin123!'), role='admin')
        db.add(admin)
    # plants
    p1 = db.query(Plant).filter(Plant.name == 'SAN SALVO').first()
    if not p1:
        p1 = Plant(name='SAN SALVO')
        db.add(p1)
        db.flush()
        db.add(Room(plant_id=p1.id, name='Room A'))
        db.add(Room(plant_id=p1.id, name='Room B'))

    p2 = db.query(Plant).filter(Plant.name == 'MARGHERA').first()
    if not p2:
        p2 = Plant(name='MARGHERA')
        db.add(p2)
        db.flush()
        db.add(Room(plant_id=p2.id, name='Room 1'))

    db.commit()
    print('Seed completed')

if __name__ == '__main__':
    seed()
