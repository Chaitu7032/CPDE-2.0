"""Backend-local Uvicorn entrypoint.

Allows running Uvicorn from within the `backend/` directory:

  uvicorn main:app --reload

The canonical app lives at `backend.api.main:app`.
"""

from backend.api.main import app  # noqa: F401
