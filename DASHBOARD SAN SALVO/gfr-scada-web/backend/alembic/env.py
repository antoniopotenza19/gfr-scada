from logging.config import fileConfig
import os
from sqlalchemy import engine_from_config, create_engine
from sqlalchemy import pool
from alembic import context
from dotenv import load_dotenv

load_dotenv()

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
fileConfig(config.config_file_name)

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Import the single Base and ensure models are imported once via app.models package
from app.db import Base
import app.models

target_metadata = Base.metadata

def run_migrations_offline():
    url = os.getenv('DATABASE_URL')
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    # Prefer DATABASE_URL env var if provided; otherwise fall back to ini config
    db_url = os.getenv('DATABASE_URL')
    if db_url:
        connectable = create_engine(db_url)
    else:
        connectable = engine_from_config(
            config.get_section(config.config_ini_section),
            prefix='sqlalchemy.',
            poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
