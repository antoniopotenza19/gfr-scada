"""add plant+ts index for fast realtime lookups

Revision ID: 0004_measurements_plant_ts_idx
Revises: 0003_ingest_state
Create Date: 2026-03-03 16:36:00.000000
"""
from alembic import op


revision = '0004_measurements_plant_ts_idx'
down_revision = '0003_ingest_state'
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        'CREATE INDEX IF NOT EXISTS ix_measurements_plant_ts_desc '
        'ON measurements (plant, ts DESC);'
    )


def downgrade():
    op.execute('DROP INDEX IF EXISTS ix_measurements_plant_ts_desc;')
