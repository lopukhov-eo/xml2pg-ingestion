from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.settings.settings import settings


def get_engine() -> Engine:
    """
    Создаёт SQLAlchemy Engine.

    :return: Engine для работы с PostgreSQL.
    """
    return create_engine(
        settings.env.db_url,
        pool_pre_ping=True,
        future=True,
    )


SessionLocal = sessionmaker(
    bind=get_engine(),
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)


@contextmanager
def session_scope() -> Iterator[Session]:
    """
    Контекст-менеджер для работы с ORM-сессией (transaction scope).

    :yield: Session (ORM).
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def raw_connection(engine: Engine) -> Iterator[object]:
    """
    Возвращает "сырой" DBAPI connection (psycopg3/2), необходимый для COPY.

    :param engine: SQLAlchemy Engine.
    :yield: DBAPI connection.
    """
    conn = engine.raw_connection()
    try:
        yield conn
    finally:
        conn.close()
