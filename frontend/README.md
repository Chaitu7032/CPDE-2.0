# CPDE Frontend

Frontend scaffold using Vite + React + TypeScript. Map editor uses `react-leaflet` and `react-leaflet-draw`.

Setup

1. From `frontend` folder install dependencies:

```bash
cd frontend
npm install
```

2. Start dev server:

```bash
npm run dev
```

Notes
- The `MapEditor` component posts drawn polygons to `/lands/` (relative path). In development, configure `proxy` in `package.json` or start backend on the same host/port.
- Install Leaflet CSS and ensure assets load (Vite handles this).
