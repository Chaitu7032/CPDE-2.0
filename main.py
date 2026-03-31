"""Uvicorn entrypoint shim.

This project’s FastAPI app lives at `backend.api.main:app`.
Keeping a top-level `main.py` lets you run:

  uvicorn main:app --reload

from the repository root.
"""

from backend.api.main import app  # noqa: F401
