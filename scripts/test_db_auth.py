import os
from urllib.parse import urlparse, urlunparse

import psycopg2
from dotenv import load_dotenv


def _safe_label(url: str) -> str:
    p = urlparse(url)
    user = p.username or "(no-user)"
    host = p.hostname or "(no-host)"
    port = p.port or 5432
    db = (p.path or "/").lstrip("/") or "postgres"
    return f"{user}@{host}:{port}/{db}"


def try_conn(url: str, label: str) -> None:
    p = urlparse(url)
    try:
        c = psycopg2.connect(
            dbname=p.path.lstrip("/") or "postgres",
            user=p.username,
            password=p.password,
            host=p.hostname,
            port=p.port,
        )
        c.close()
        print("OK:", label)
    except Exception as e:
        print("FAIL:", label, type(e).__name__, e)


def _swap_db(url: str, dbname: str) -> str:
    p = urlparse(url)
    return urlunparse(
        (
            p.scheme,
            p.netloc,
            f"/{dbname}",
            p.params,
            p.query,
            p.fragment,
        )
    )


def _force_ipv4_localhost(url: str) -> str:
    p = urlparse(url)
    host = p.hostname
    if host in ("localhost", "::1"):
        # keep credentials/port, only change hostname
        netloc = p.netloc.replace("localhost", "127.0.0.1").replace("::1", "127.0.0.1")
        return urlunparse((p.scheme, netloc, p.path, p.params, p.query, p.fragment))
    return url


def main() -> None:
    load_dotenv()
    # Prefer explicit test URLs if provided (semicolon-separated)
    urls_env = os.getenv("DB_TEST_URLS")
    if urls_env:
        urls = [u.strip() for u in urls_env.split(";") if u.strip()]
        for u in urls:
            try_conn(u, _safe_label(u))
        return

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is not set")

    # Always test both the target DB and the default 'postgres' DB
    candidates = []
    for base in (database_url, _force_ipv4_localhost(database_url)):
        candidates.append(base)
        candidates.append(_swap_db(base, "postgres"))

    seen = set()
    for u in candidates:
        if u in seen:
            continue
        seen.add(u)
        try_conn(u, _safe_label(u))


if __name__ == "__main__":
    main()
