import os
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv


load_dotenv()


def _connect_with_postgres_env() -> tuple[psycopg2.extensions.connection, str, str]:
    dbname = os.getenv("POSTGRES_DB", "cpde")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))

    if not password:
        raise ValueError("POSTGRES_PASSWORD is not set")

    return (
        psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port),
        dbname,
        user,
    )


def _connect_with_database_url() -> tuple[psycopg2.extensions.connection, str, str]:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not set")

    # psycopg2 expects a sync URL; our app uses asyncpg.
    sync_url = database_url.replace("postgresql+asyncpg://", "postgresql://")
    parsed = urlparse(sync_url)
    dbname = (parsed.path or "").lstrip("/")
    user = parsed.username or ""

    if not parsed.hostname or parsed.port is None:
        raise ValueError("DATABASE_URL is missing host/port")

    return (
        psycopg2.connect(
            dbname=dbname,
            user=user,
            password=parsed.password,
            host=parsed.hostname,
            port=int(parsed.port),
        ),
        dbname,
        user,
    )


try:
    conn, dbname, user = _connect_with_postgres_env()
except Exception:
    conn, dbname, user = _connect_with_database_url()

conn.close()
print(f"✅ Connected to {dbname} as {user}")
