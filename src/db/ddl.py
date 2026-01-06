from sqlalchemy.engine import Engine

from src.db.models import Base


def init_db(engine: Engine) -> None:
    """
    Инициализирует структуру базы данных.

    :param engine: SQLAlchemy Engine.
    :return: None.
    """
    Base.metadata.create_all(engine)

    with engine.begin() as conn:
        conn.exec_driver_sql("ALTER TABLE IF EXISTS stg_group_event SET UNLOGGED;")
        conn.exec_driver_sql("ALTER TABLE IF EXISTS stg_event SET UNLOGGED;")


def truncate_staging(engine: Engine) -> None:
    """
    Очищает staging-таблицы.

    :param engine: SQLAlchemy Engine.
    :return: None.
    """
    with engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE stg_event;")
        conn.exec_driver_sql("TRUNCATE TABLE stg_group_event;")
