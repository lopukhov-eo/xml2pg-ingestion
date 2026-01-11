from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from sqlalchemy.engine import Engine

from src.db.copy import CopySpec, copy_rows

STG_GROUP_EVENT_SPEC = CopySpec(
    table="stg_group_event",
    columns=("id", "name"),
)

STG_EVENT_SPEC = CopySpec(
    table="stg_event",
    columns=("id", "group_event_id", "name"),
)


@dataclass(frozen=True)
class StagingCopySpecs:
    """
    Спецификации COPY для staging-таблиц.

    :return: Набор CopySpec для stg_group_event и stg_event.
    """

    group_event: CopySpec = STG_GROUP_EVENT_SPEC
    event: CopySpec = STG_EVENT_SPEC


class StagingLoader:
    """
    Загружает данные в staging-таблицы через COPY.

    :param engine: SQLAlchemy Engine для подключения к БД.
    :param specs: Спецификации COPY (опционально).
    """

    def __init__(self, engine: Engine, specs: StagingCopySpecs | None = None) -> None:
        """
        Инициализирует загрузчик staging-таблиц.

        :param engine: SQLAlchemy Engine.
        :param specs: Набор CopySpec для staging-таблиц.
        :return: Ничего не возвращает.
        Создаёт экземпляр StagingLoader с настроенными engine/specs.
        """
        self.engine = engine
        self.specs = specs or StagingCopySpecs()

    def copy_group_events(self, rows: Iterable[Sequence[Any]]) -> int:
        """
        Загружает батч group_event в stg_group_event.

        :param rows: Итератор строк (id, name).
        :return: Количество строк (или -1 при psycopg2).
        """
        return copy_rows(self.engine, self.specs.group_event, rows)

    def copy_events(self, rows: Iterable[Sequence[Any]]) -> int:
        """
        Загружает батч event в stg_event.

        :param rows: Итератор строк (id, group_event_id, name).
        :return: Количество строк (или -1 при psycopg2).
        """
        return copy_rows(self.engine, self.specs.event, rows)
