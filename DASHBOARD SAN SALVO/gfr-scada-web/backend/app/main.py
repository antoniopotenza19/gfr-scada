import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from .db import engine, Base

load_dotenv()

app = FastAPI(title='gfr-scada-web')

# CORS (allow frontend during development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import routers
from .routers import auth, ingest, realtime, commands, plants

app.include_router(auth.router)
app.include_router(ingest.router)
app.include_router(commands.router)
app.include_router(realtime.router)
app.include_router(plants.router)

# NOTE: migrations should be used to create DB schema (Alembic).
# Avoid calling Base.metadata.create_all() here to prevent duplicate
# table creation when running alembic migrations.

# serve static pages under /static
static_dir = os.path.join(os.path.dirname(__file__), 'static')
if os.path.isdir(static_dir):
    app.mount('/static', StaticFiles(directory=static_dir), name='static')


@app.get('/')
def index():
    return {'msg': 'gfr-scada-web backend running. Browse /static for UI.'}
