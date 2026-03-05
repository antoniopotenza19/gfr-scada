"""create core tables

Revision ID: 0002_create_core_tables
Revises: 0001_create_measurements
Create Date: 2026-03-02 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa


revision = '0002_create_core_tables'
down_revision = '0001_create_measurements'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'users',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('username', sa.String(length=50), nullable=False),
        sa.Column('hashed_password', sa.String(length=255), nullable=False),
        sa.Column('role', sa.String(length=20), nullable=False, server_default='viewer'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index('ix_users_username', 'users', ['username'], unique=True)

    op.create_table(
        'plants',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index('ix_plants_name', 'plants', ['name'], unique=True)

    op.create_table(
        'rooms',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('plant_id', sa.Integer(), sa.ForeignKey('plants.id'), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
    )

    op.create_table(
        'commands',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('requested_by', sa.Integer(), sa.ForeignKey('users.id')),
        sa.Column('command', sa.String(length=50)),
        sa.Column('target', sa.String(length=100)),
        sa.Column('params', sa.Text()),
        sa.Column('status', sa.String(length=30), server_default='requested'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('approved_by', sa.Integer(), sa.ForeignKey('users.id'), nullable=True),
        sa.Column('executed_by', sa.Integer(), sa.ForeignKey('users.id'), nullable=True),
    )

    op.create_table(
        'alarms',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('plant', sa.String(length=100)),
        sa.Column('room', sa.String(length=100)),
        sa.Column('signal', sa.String(length=100)),
        sa.Column('severity', sa.String(length=20)),
        sa.Column('message', sa.Text()),
        sa.Column('active', sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column('ack_user', sa.String(length=50), nullable=True),
        sa.Column('ack_time', sa.DateTime(timezone=True), nullable=True),
        sa.Column('ts', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade():
    op.drop_table('alarms')
    op.drop_table('commands')
    op.drop_table('rooms')
    op.drop_index('ix_plants_name', table_name='plants')
    op.drop_table('plants')
    op.drop_index('ix_users_username', table_name='users')
    op.drop_table('users')
