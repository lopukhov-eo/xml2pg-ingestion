from typing import Optional

from sqlalchemy import BigInteger, ForeignKey, MetaData, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Configuring Constraint Naming Conventions (SQLAlchemy 2.0 Documentation)
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """
    Базовый класс для всех ORM-моделей проекта.

    Содержит общий объект MetaData с настроенными naming convention
    для первичных ключей, внешних ключей, индексов и ограничений.
    """

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class GroupEvent(Base):
    """
    ORM-модель таблицы group_event.

    Описывает группу событий, получаемую из XML.

    :param id: Уникальный идентификатор группы событий (берётся из XML).
    :param name: Название группы событий (может отсутствовать).
    """

    __tablename__ = "group_event"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class Event(Base):
    """
    ORM-модель таблицы event.

    Описывает отдельное событие, относящееся к группе событий.

    :param id: Уникальный идентификатор события (берётся из XML).
    :param group_event_id: Идентификатор родительской группы событий.
    :param name: Название события (текст внутри XML-тега event).
    """

    __tablename__ = "event"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    group_event_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("group_event.id"), nullable=False
    )
    name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class StgGroupEvent(Base):
    """
    ORM-модель staging-таблицы stg_group_event.

    Используется для быстрой загрузки данных из XML
    перед переносом в финальную таблицу group_event.

    В таблице отсутствуют первичные и внешние ключи
    для максимальной скорости bulk insert (COPY).

    :param id: Идентификатор группы событий из XML.
    :param name: Название группы событий.
    """

    __tablename__ = "stg_group_event"

    id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class StgEvent(Base):
    """
    ORM-модель staging-таблицы stg_event.

    Используется для быстрой загрузки событий из XML
    перед переносом в финальную таблицу event.

    Ограничения целостности отсутствуют и проверяются
    на этапе финализации данных.

    :param id: Идентификатор события из XML.
    :param group_event_id: Идентификатор родительской группы событий.
    :param name: Название события.
    """

    __tablename__ = "stg_event"

    id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    group_event_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
