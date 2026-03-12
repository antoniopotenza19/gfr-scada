import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

load_dotenv()

from .config import settings
from .db.bootstrap import run_legacy_bootstrap
from .routers import auth, commands, debug, health, ingest, ingest_realtime, plants, realtime, sale, site_commands
from .services.energysaving_runtime import csv_db_ingestor_service, warm_energysaving_runtime_caches

app = FastAPI(title='gfr-scada-web')

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allow_origins,
    allow_credentials=True,
    allow_methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allow_headers=['Authorization', 'Content-Type'],
)

app.include_router(auth.router)
app.include_router(debug.router)
app.include_router(health.router)
app.include_router(ingest.router)
app.include_router(ingest_realtime.router)
app.include_router(commands.router)
app.include_router(site_commands.router)
app.include_router(realtime.router)
app.include_router(plants.router)
app.include_router(sale.router)

static_dir = os.path.join(os.path.dirname(__file__), 'static')
if os.path.isdir(static_dir):
    app.mount('/static', StaticFiles(directory=static_dir), name='static')


@app.on_event('startup')
async def startup_ingest_scheduler():
    if os.getenv("ENABLE_LEGACY_BOOTSTRAP", "false").strip().lower() == "true":
        run_legacy_bootstrap()
    warm_energysaving_runtime_caches()
    if settings.ingest_autostart:
        await csv_db_ingestor_service.start()


@app.on_event('shutdown')
async def shutdown_ingest_scheduler():
    await csv_db_ingestor_service.stop()


@app.get('/')
def index():
    return {'msg': 'gfr-scada-web backend running. Browse /static for UI.'}
