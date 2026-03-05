import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

load_dotenv()

from .config import settings
from .routers import auth, commands, debug, health, ingest, ingest_realtime, plants, realtime, site_commands
from .services import csv_watcher

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

static_dir = os.path.join(os.path.dirname(__file__), 'static')
if os.path.isdir(static_dir):
    app.mount('/static', StaticFiles(directory=static_dir), name='static')


@app.on_event('startup')
async def startup_ingest_scheduler():
    if settings.ingest_autostart:
        await csv_watcher.start()


@app.on_event('shutdown')
async def shutdown_ingest_scheduler():
    await csv_watcher.stop()


@app.get('/')
def index():
    return {'msg': 'gfr-scada-web backend running. Browse /static for UI.'}
