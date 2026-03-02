"""create measurements hypertable

Revision ID: 0001_create_measurements
Revises: 
Create Date: 2026-02-28 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0001_create_measurements'
down_revision = None
branch_labels = None
depends_on = None


from alembic import context


def upgrade():
    # enable extension if possible (idempotent)
    op.execute('CREATE EXTENSION IF NOT EXISTS timescaledb;')

    # create table if it doesn't exist using a single SQL statement so that
    # `alembic upgrade --sql` works (no runtime inspection required).
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS measurements (
            id serial PRIMARY KEY,
            plant varchar(100),
            room varchar(100),
            signal varchar(100),
            value double precision,
            unit varchar(50),
            ts timestamptz NOT NULL
        );
        """
    )

    # convert to hypertable; offline mode just emits the SELECT statement,
    # online mode executes it with autocommit to avoid transaction errors.
    create_stmt = "SELECT create_hypertable('measurements', 'ts', if_not_exists => TRUE);"
    if context.is_offline_mode():
        op.execute(create_stmt)
    else:
        try:
            conn = op.get_bind()
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(sa.text(create_stmt))
        except Exception:
            pass


def downgrade():
    try:
        op.drop_table('measurements')
    except Exception:
        pass
