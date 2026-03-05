from sqlalchemy import BigInteger, Column, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.sql import func

from app.db import Base


class IngestState(Base):
    __tablename__ = 'ingest_state'
    __table_args__ = (
        UniqueConstraint('source_url', 'plant', 'filename', name='uq_ingest_state_source_plant_file'),
    )

    id = Column(Integer, primary_key=True)
    source_url = Column(String(500), nullable=False)
    plant = Column(String(100), nullable=False, index=True)
    filename = Column(String(255), nullable=False)
    last_modified = Column(DateTime(timezone=True), nullable=True)
    last_byte_offset = Column(BigInteger, nullable=True)
    last_ts = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
