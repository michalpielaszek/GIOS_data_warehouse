from getpass import getpass
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL
import os
from dotenv import load_dotenv

DB_USER = "pmichal"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "hurtownia_db"
DB_SCHEMA = "gios"

_ENGINE: Engine | None = None

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

def get_engine(password: str | None = None) -> Engine:
    global _ENGINE

    if _ENGINE is not None:
        return _ENGINE

    if password is None:
        password = os.getenv("POSTGRES_PASSWORD")

    if password is None:
        password = getpass("Hasło do PostgreSQL: ")

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER,
        password=password,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )

    _ENGINE = create_engine(url)

    return _ENGINE


def test_connection() -> None:
    engine = get_engine()

    with engine.connect() as conn:
        conn.execute(text(f"SET search_path TO {DB_SCHEMA}, public"))

        result = conn.execute(text("""
            SELECT
                current_user,
                current_database(),
                current_schema();
        """))

        print(result.fetchone())