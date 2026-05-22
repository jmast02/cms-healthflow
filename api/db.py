"""Database connection and session management."""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

load_dotenv()

DATABASE_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER', 'healthflow')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'healthflow_secret')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'cms_healthflow')}"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    """FastAPI dependency — yields a DB session and closes it after the request."""
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def check_connection() -> bool:
    """Return True if the database is reachable."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
