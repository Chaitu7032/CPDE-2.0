# CPDE — Crop Stress Detection & Early Warning Engine

Phase 0: Project scaffold and setup instructions.

Quick start

1. Create a Python 3.11+ virtual environment

```bash
python -m venv .venv
source .venv/Scripts/activate   # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r "requirements.txt"
```

2. Copy `.env.example` to `.env` and set `DATABASE_URL` (PostGIS)

3. Run the FastAPI server

```bash
uvicorn backend.api.main:app --reload --port 8000
```

Files created in Phase 0:
- `backend/api/main.py` — FastAPI entrypoint
- `backend/db/connection.py` — DB config (reads `.env`)
- `.env.example` — example env vars

After Phase 0 I will need PostGIS credentials (host, port, database, user, password) to validate DB connectivity and run migrations. Provide them after you want me to proceed.
