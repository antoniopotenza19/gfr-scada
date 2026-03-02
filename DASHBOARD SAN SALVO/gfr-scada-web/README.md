# gfr-scada-web

Fullstack SCADA web demo: FastAPI backend, TimescaleDB, Alembic, JWT auth, WebSocket realtime, static AdminLTE-like pages served from `/static`.

Quick run (dev):

1) Copy `.env.example` to `.env` and edit values.

2) Run with Docker Compose:

```bash
cd gfr-scada-web
docker compose up --build
```

This starts TimescaleDB (Postgres), and the backend (Uvicorn). Backend serves static pages at `http://localhost:8000/static/` and API at `http://localhost:8000/api/`.

Seed admin and example data:

```bash
docker compose exec backend python app/scripts/seed.py
```

Run Alembic migrations (inside backend container):

```bash
docker compose exec backend bash -c "./run_migrations.sh"
```

Example: ingest a single CSV for plant **BRAVO** on **2026‑01‑01** directly from the remote server:

```bash
# default URL is built from BASE_CSV_URL + YYYY/MM/DD/ + filename
curl "http://localhost:8000/api/ingest/csv?plant=BRAVO&date=2026-01-01"

# optionally override the base URL
curl "http://localhost:8000/api/ingest/csv?plant=BRAVO&date=2026-01-01&base_url=http://other.example.com/data"
```

The backend constructs the remote path using the pattern:  
`BASE_CSV_URL/YYYY/MM/DD/MM-DD-<PLANT_UPPER>.CSV`  

For example, with `BASE_CSV_URL=http://94.138.172.234:46812/shared` and plant **BRAVO** on **2026-01-01**, it will fetch:  
`http://94.138.172.234:46812/shared/2026/01/01/01-01-BRAVO.CSV`

No local CSV folder or synchronization is required.

After migration the `measurements` table will be created and converted to a TimescaleDB hypertable if supported by the DB.

**DEV: Reset database objects**

If a migration fails during development and leaves leftover objects (for example a partially-created `measurements` table or `alembic_version`), you can run the included dev-only cleanup script to remove them.

From the project root run (dev-only):

```bash
docker compose exec backend bash -lc "scripts/db_cleanup_dev.sh"
# non-interactive, also drop dev tables `alarms`, `users`, `commands`:
docker compose exec backend bash -lc "scripts/db_cleanup_dev.sh -y --drop-others"
```

The script will attempt to execute SQL via the DB container using:

```bash
docker compose exec db psql -U $POSTGRES_USER -d $POSTGRES_DB -c "DROP TABLE IF EXISTS measurements CASCADE; DROP TABLE IF EXISTS alembic_version CASCADE;"
```

After running the cleanup, re-run migrations:

```bash
docker compose exec backend alembic upgrade head
```

Warning: **DEV ONLY: this will delete data**. Do not run against production databases.


Notes:
- Change `JWT_SECRET` in `.env` to a secure value.
- Alembic config in `backend/alembic`.
