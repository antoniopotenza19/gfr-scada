# gfr-scada-web

Monorepo SCADA: FastAPI backend + Timescale/Postgres + React/Vite frontend.

## Ambiente Python

Il repository usa una sola virtual environment Python nella root:

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Per i test backend:

```powershell
pip install -r requirements-dev.txt
```

`backend/venv` non e piu usata.

## Avvio rapido

Da root repository, per rialzare tutto dopo aver chiuso:

```powershell
.\start-dev.cmd
```

Alternativa da PowerShell:

```powershell
npm.cmd start
```

Il comando:
- ferma eventuali container esistenti
- rialza `db` e `backend`
- aspetta che il DB sia sano
- esegue migrazioni e seed
- apre il frontend Vite in una nuova finestra PowerShell

Prerequisito: Docker Desktop deve essere aperto e con engine attivo.

Varianti utili:

```powershell
npm.cmd run start:no-seed
npm.cmd run start:no-frontend
```

## Avvio locale con DB via VPN

Quando vuoi usare backend e frontend locali contro un database raggiungibile via VPN:

1. Crea `.env.vpn` partendo da `.env.vpn.example`
2. Imposta `DATABASE_URL` con host/IP raggiungibile via VPN
3. Connetti la VPN
4. Avvia:

```powershell
.\start-vpn.cmd
```

Alternativa:

```powershell
npm.cmd run start:vpn
```

Questa modalita:
- non usa Docker
- avvia backend locale su `127.0.0.1:8000`
- avvia frontend locale su `localhost:5173`
- non esegue migrazioni o seed di default

Comandi opzionali:

```powershell
npm.cmd run start:vpn:migrate
npm.cmd run start:vpn:seed
```

Usa `start:vpn:seed` solo su DB di sviluppo locale o ambienti controllati.

## Env

1. Copia `.env.example` in `.env`.
2. Imposta almeno:
   - `DATABASE_URL`
   - `JWT_SECRET`
   - `CORS_ALLOW_ORIGINS`
   - `BASE_CSV_URL` (default: `http://94.138.172.234:46812/shared`)
   - `INGEST_POLL_SECONDS` (default: `5`)
   - `INGEST_AUTOSTART` (`true`/`false`)
   - `INGEST_PLANTS` (opzionale, lista separata da virgole)
   - `DEV_DEFAULT_ADMIN_USERNAME` (solo bootstrap locale, default: `admin`)
   - `DEV_DEFAULT_ADMIN_PASSWORD` (solo bootstrap locale, default: `admin123`)

### Credenziali default sviluppo locale

- Username: `admin`
- Password: `admin123`
- Ruolo: `admin`
- Valide solo quando `APP_ENV` e impostato a `development`/`dev`/`local` durante il seed.

## Avvio Locale Windows (senza Docker completo)

### Opzione A: DB in Docker, backend/frontend locali

```powershell
cd gfr-scada-web
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
docker compose up -d db
```

```powershell
cd backend
alembic upgrade head
python app/scripts/seed.py
uvicorn app.main:app --reload
```

```powershell
cd frontend
npm install
npm run dev
```

### Opzione B: tutto locale (Postgres già installato)

Stessi comandi backend/frontend, con `DATABASE_URL` puntato al Postgres locale.

## Ingest CSV remoto

La sorgente usa direttamente HTTP directory listing:
`BASE_CSV_URL/YYYY/MM/DD/`

Esempio:
`http://94.138.172.234:46812/shared/2026/03/02/`

Endpoint manuale:

```bash
GET /api/ingest/csv?plant=DEMO_PLANT&date=2026-01-01
```

## Realtime Ingest Scheduler

Polling near real-time configurabile (default 5s), ingest incrementale con cursor `last_ts` per file in tabella `ingest_state`.

Nuovi endpoint:
- `GET /api/ingest/status`
- `GET /api/ingest/sources`
- `POST /api/ingest/start`
- `POST /api/ingest/stop`

Le API frontend esistenti `/api/plants/...` restano invariate.

## Backfill aggregate sale

Le aggregate ora seguono un rollup piramidale:

- sale: `registrazioni_sale -> sale_agg_1min -> sale_agg_15min -> sale_agg_1h -> sale_agg_1d -> sale_agg_1month`
- compressori: `registrazioni_compressori -> compressori_agg_1min -> compressori_agg_1h`

Per il backfill storico esegui i livelli in ordine, senza saltare quello inferiore:

```powershell
python backend/scripts/backfill_aggregates.py --granularity 1min --from 2026-02-20 --to 2026-03-01 --truncate-target-range --chunk-unit day
python backend/scripts/backfill_aggregates.py --granularity 15min --from 2026-02-01 --to 2026-03-01 --truncate-target-range --chunk-unit day
python backend/scripts/backfill_aggregates.py --granularity 1h --from 2025-11-01 --to 2026-03-01 --truncate-target-range --chunk-unit week
python backend/scripts/backfill_aggregates.py --granularity 1d --from 2025-11-01 --to 2026-03-01 --truncate-target-range --chunk-unit month
python backend/scripts/backfill_aggregates.py --granularity 1month --from 2025-11-01 --to 2026-03-01 --truncate-target-range --chunk-unit month
python backend/scripts/backfill_aggregates.py --dataset compressori --granularity 1min --from 2026-02-20 --to 2026-03-01 --truncate-target-range --chunk-unit day
python backend/scripts/backfill_aggregates.py --dataset compressori --granularity 1h --from 2026-02-20 --to 2026-03-01 --truncate-target-range --chunk-unit day
```

Note operative:
- usa il resolver DB dell'ingestor se disponibile, altrimenti `DATABASE_URL` o `DB_*`
- salva lo stato in `backend/runtime/backfill_aggregates.status.json`
- `--to` e esclusivo
- per `1month`, `--from` e `--to` devono essere il primo giorno del mese
- i livelli superiori non leggono piu il raw: `15min` legge `1min`, `1h` legge `15min`, `1d` legge `1h`, `1month` legge `1d`
- le medie nei rollup superiori sono pesate con `samples_count`
- `cons_specifico_avg` viene ricalcolato come `energia_kwh_sum / volume_nm3_sum`
- l'ingestor aggiorna automaticamente la piramide aggregate dopo il commit dei dati raw e dello stato corrente

## Debugging data freeze

Quando i valori sembrano bloccati:

1. Verifica salute API e DB:

```bash
curl -s http://127.0.0.1:8000/api/health
```

Controlla:
- `db_connected: true`
- `ingest_running: true`
- `measurements_latest_ts` in avanzamento per ogni plant.

2. Verifica stato ingest realtime:

```bash
curl -s http://127.0.0.1:8000/api/ingest/status
```

Controlla per ogni job:
- `last_modified`
- `last_byte_offset`
- `last_success_ts`
- `last_insert_count`
- `last_error` (deve essere `null`)
- `no_progress_cycles` (se cresce, file aggiornato ma nessun nuovo dato utile inserito).

3. Verifica in DB che `max(ts)` cresca:

```sql
SELECT plant, MAX(ts) AS latest_ts
FROM measurements
GROUP BY plant
ORDER BY plant;
```

4. Verifica frontend polling in DevTools Network:
- summary: richiesta ogni ~5s (`/api/plants/{plant}/summary`)
- alarms: richiesta ogni ~15s (`/api/plants/{plant}/alarms`)
- timeseries: richiesta ogni ~15-30s (`/api/plants/{plant}/timeseries`)

5. In ambiente `vite dev`, usa il pannello `Debug data freeze` nella Dashboard:
- plant/room selezionato
- `summary.last_update`
- ultimo fetch per query principali
- `API base URL` effettivamente in uso.
