# GFR SCADA Frontend

React + Vite + TypeScript + Tailwind dashboard for SCADA.

## Quick Start

```bash
cd frontend
npm install
npm run dev
```

Open `http://localhost:5173` (default Vite port).

## Connect to Backend

Set backend URL via env var:

```bash
export VITE_API_BASE_URL=http://localhost:8000
npm run dev
```

Or hardcoded in `.env`:

```
VITE_API_BASE_URL=http://localhost:8000
```

## Build

```bash
npm run build
```

Outputs to `dist/`.

## Pages

- **Login** (`/login`): Auth screen
- **Dashboard** (`/dashboard`): Main SCADA dashboard with KPIs, charts, summary
- **SCADA** (`/scada/:plant`): Plant-specific mimic page
- **Alarms** (`/alarms`): Alarm list (stub with mock data)

## Architecture

- `/src/pages`: Page components
- `/src/components`: Reusable UI components
- `/src/api`: API client (axios)
- `/src/hooks`: React hooks
- `/src/styles`: CSS (Tailwind)

## Stack

- **React 18** + **Vite 5**
- **TypeScript** (strict mode)
- **Tailwind CSS** for styling
- **Recharts** for time-series charts
- **React Router** for navigation
- **React Query** (@tanstack/react-query) for data fetching
- **Axios** for HTTP + JWT auth headers

## Notes

- Token stored in `sessionStorage` (dev-friendly)
- Auth flow: on login, token is stored; subsequent requests auto-inject `Authorization: Bearer <token>`
- If backend auth unavailable, login accepts any non-empty credentials
- ChartComponent fetches `/api/measurements/timeseries` endpoint (adapt if needed)
- Alarms page uses mock data; update to use real WebSocket or polling if available
