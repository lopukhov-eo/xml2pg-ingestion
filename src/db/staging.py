from dataclasses import dataclass
from typing import Any, Iterable, Sequence, Type

from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase

from src.db.copy import CopySpec, copy_rows
from src.db.models import StgEvent, StgGroupEvent


def copy_spec_from_model(
    model: Type[DeclarativeBase],
    *,
    table: str | None = None,
    columns: Sequence[str] | None = None,
) -> CopySpec:
    """
    Формирует CopySpec на основе ORM-модели SQLAlchemy.

    :param model: ORM-модель SQLAlchemy.
    :param table: Явное имя таблицы для COPY.
    :param columns: Явный список колонок для COPY.
    Если не задано, используются все колонки таблицы в порядке их объявления.
    :return: Объект CopySpec (имя таблицы + колонки),
    готовый для использования в COPY FROM STDIN.
    """
    tbl = model.__table__
    cols = tuple(columns) if columns is not None else tuple(c.name for c in tbl.columns)
    return CopySpec(table=table or tbl.name, columns=cols)


@dataclass(frozen=True)
class StagingCopySpecs:
    """
    Спецификации COPY для staging-таблиц.

    :return: Набор CopySpec для stg_group_event и stg_event.
    """

    group_event: CopySpec = copy_spec_from_model(StgGroupEvent)
    event: CopySpec = copy_spec_from_model(StgEvent)


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
