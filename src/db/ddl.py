from sqlalchemy.engine import Engine

from src.db.models import Base
from src.settings.settings import settings


def init_db(engine: Engine) -> None:
    """
    Инициализирует структуру базы данных.

    :param engine: SQLAlchemy Engine.
    :return: None.
    """
    Base.metadata.create_all(engine)

    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"ALTER TABLE IF EXISTS "
            f"stg_{settings.ini.groups_table_name} "
            f"SET UNLOGGED;"
        )
        conn.exec_driver_sql(
            f"ALTER TABLE IF EXISTS "
            f"stg_{settings.ini.events_table_name} "
            f"SET UNLOGGED;"
        )


def truncate_staging(engine: Engine) -> None:
    """
    Очищает staging-таблицы.

    :param engine: SQLAlchemy Engine.
    :return: None.
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"TRUNCATE TABLE " f"stg_{settings.ini.events_table_name};"
        )
        conn.exec_driver_sql(
            f"TRUNCATE TABLE " f"stg_{settings.ini.groups_table_name};"
        )
