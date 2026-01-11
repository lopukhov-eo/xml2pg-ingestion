from psycopg import sql
from sqlalchemy.engine import Engine

from src.settings.settings import settings


def finalize(engine: Engine) -> None:
    """
    Финализация данных после COPY в staging.

    Шаги:
    1) Удаляем FK/PK/индекс (если есть), чтобы не мешали массовой вставке
    2) TRUNCATE финальные таблицы
    3) INSERT DISTINCT ON из staging в финальные (устраняем дубли по id)
    4) Восстанавливаем PK, индекс и FK
    5) ANALYZE

    :param engine: SQLAlchemy Engine.
    :return: Ничего не возвращает.
    """
    events = sql.Identifier(settings.ini.events_table_name)
    groups = sql.Identifier(settings.ini.groups_table_name)

    pk_group = sql.Identifier(f"pk_{settings.ini.groups_table_name}")
    pk_event = sql.Identifier(f"pk_{settings.ini.events_table_name}")
    fk_event_group = sql.Identifier("fk_event_group_event_id_group_event")
    ix_event_group = sql.Identifier("ix_event_group_event_id")

    with engine.begin() as conn:
        raw = conn.connection  # psycopg connection

        raw.execute(
            sql.SQL(
                "ALTER TABLE IF EXISTS {events} DROP CONSTRAINT IF EXISTS {fk};"
            ).format(events=events, fk=fk_event_group)
        )

        raw.execute(
            sql.SQL(
                "ALTER TABLE IF EXISTS {events} DROP CONSTRAINT IF EXISTS {pk};"
            ).format(events=events, pk=pk_event)
        )

        raw.execute(
            sql.SQL(
                "ALTER TABLE IF EXISTS {groups} DROP CONSTRAINT IF EXISTS {pk};"
            ).format(groups=groups, pk=pk_group)
        )

        raw.execute(sql.SQL("DROP INDEX IF EXISTS {ix};").format(ix=ix_event_group))

        raw.execute(sql.SQL("TRUNCATE TABLE {events};").format(events=events))
        raw.execute(sql.SQL("TRUNCATE TABLE {groups};").format(groups=groups))

        raw.execute(
            sql.SQL(
                """
                INSERT INTO {groups} (id, name)
                SELECT DISTINCT ON (id) id, name
                FROM {stg_groups}
                ORDER BY id;
                """
            ).format(
                groups=groups,
                stg_groups=sql.Identifier(f"stg_{settings.ini.groups_table_name}"),
            )
        )

        raw.execute(
            sql.SQL(
                """
                INSERT INTO {events} (id, group_event_id, name)
                SELECT DISTINCT ON (se.id)
                    se.id, se.group_event_id, se.name
                FROM {stg_events} se
                JOIN {groups} ge ON ge.id = se.group_event_id
                ORDER BY se.id;
                """
            ).format(
                events=events,
                groups=groups,
                stg_events=sql.Identifier(f"stg_{settings.ini.events_table_name}"),
            )
        )

        raw.execute(
            sql.SQL(
                "ALTER TABLE {groups} ADD CONSTRAINT {pk} PRIMARY KEY (id);"
            ).format(groups=groups, pk=pk_group)
        )

        raw.execute(
            sql.SQL(
                "ALTER TABLE {events} ADD CONSTRAINT {pk} PRIMARY KEY (id);"
            ).format(events=events, pk=pk_event)
        )

        raw.execute(
            sql.SQL("CREATE INDEX {ix} ON {events} (group_event_id);").format(
                ix=ix_event_group, events=events
            )
        )

        raw.execute(
            sql.SQL(
                """
                ALTER TABLE {events}
                ADD CONSTRAINT {fk}
                FOREIGN KEY (group_event_id)
                REFERENCES {groups}(id);
                """
            ).format(events=events, fk=fk_event_group, groups=groups)
        )

        raw.execute(sql.SQL("ANALYZE {groups};").format(groups=groups))
        raw.execute(sql.SQL("ANALYZE {events};").format(events=events))
