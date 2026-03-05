"""add ingest_state and idempotent measurement index

Revision ID: 0003_ingest_state
Revises: 0002_create_core_tables
Create Date: 2026-03-02 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa


revision = '0003_ingest_state'
down_revision = '0002_create_core_tables'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'ingest_state',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('source_url', sa.String(length=500), nullable=False),
        sa.Column('plant', sa.String(length=100), nullable=False),
        sa.Column('filename', sa.String(length=255), nullable=False),
        sa.Column('last_modified', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_byte_offset', sa.BigInteger(), nullable=True),
        sa.Column('last_ts', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index('ix_ingest_state_plant', 'ingest_state', ['plant'], unique=False)
    op.create_index(
        'uq_ingest_state_source_plant_file',
        'ingest_state',
        ['source_url', 'plant', 'filename'],
        unique=True,
    )

    op.execute(
        """
        WITH ranked AS (
            SELECT id, ROW_NUMBER() OVER (PARTITION BY plant, signal, ts ORDER BY id) AS rn
            FROM measurements
        )
        DELETE FROM measurements
        WHERE id IN (SELECT id FROM ranked WHERE rn > 1);
        """
    )
    op.execute(
        'CREATE UNIQUE INDEX IF NOT EXISTS uq_measurements_plant_signal_ts '
        'ON measurements (plant, signal, ts);'
    )


def downgrade():
    op.execute('DROP INDEX IF EXISTS uq_measurements_plant_signal_ts;')
    op.drop_index('uq_ingest_state_source_plant_file', table_name='ingest_state')
    op.drop_index('ix_ingest_state_plant', table_name='ingest_state')
    op.drop_table('ingest_state')
