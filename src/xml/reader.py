from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

from lxml import etree

from src.settings.config import XML_GROUP_TAG_NAME
from src.xml.parser import EventRecord, GroupEventRecord, parse_group_event


@dataclass
class ReaderStats:
    """
    Счётчики работы streaming-ридера XML.

    Используется для мониторинга прогресса и качества данных при
    потоковом парсинге очень больших XML-файлов.

    :ivar groups_seen: Количество обработанных элементов <group_event>.
    :ivar groups_emitted: Количество успешно распарсенных <group_event>.
    :ivar events_emitted: Количество успешно распарсенных <event>.
    :ivar skipped_records: Количество пропусков из-за
    некорректных/отсутствующих id и прочих ошибок парсинга.
    """

    groups_seen: int = 0
    groups_emitted: int = 0
    events_emitted: int = 0
    skipped_records: int = 0


@dataclass(frozen=True)
class GroupEventBundle:
    """
    Результат парсинга одного элемента <group_event>.

    Содержит запись группы и список связанных событий, готовых для
    дальнейшей обработки (батчирования/загрузки в БД).

    :ivar group: Распарсенная группа событий (GroupEventRecord).
    :ivar events: Список событий, принадлежащих группе (list[EventRecord]).
    """

    group: GroupEventRecord
    events: list[EventRecord]


def iter_group_events(
    xml_path: Path,
    recover: bool = True,
    huge_tree: bool = True,
    stats: Optional[ReaderStats] = None,
) -> Iterator[GroupEventBundle]:
    """
    Итерирует по XML-файлу и потоково возвращает данные по одному <group_event> за раз.

    Функция рассчитана на очень большие XML (вплоть до сотен ГБ/ТБ) и поэтому:
    - использует lxml.etree.iterparse по событию "end" для тега "group_event";
    - после обработки каждого <group_event> освобождает память через
      element.clear() и удаление уже обработанных siblings слева,
      чтобы дерево не разрасталось.

    Алгоритм:
    1) iterparse находит очередной закрывающийся <group_event>
    2) parse_group_event извлекает group + events (с валидацией id)
    3) обновляется ReaderStats (если передан)
    4) при наличии корректного group.id возвращается GroupEventBundle
    5) выполняется очистка памяти текущего элемента

    :param xml_path: Путь к XML-файлу.
    :param recover: Включить режим восстановления при ошибках XML.
    :param huge_tree: Разрешить обработку "больших" деревьев XML.
    :param stats: Опциональный объект ReaderStats для накопления статистики.
    :yield: GroupEventBundle — группа и связанные события для каждого
    корректного <group_event>.
    :return: Итератор (generator), выдающий GroupEventBundle.
    """
    if stats is None:
        stats = ReaderStats()

    context = etree.iterparse(
        str(xml_path),
        events=("end",),
        tag=(XML_GROUP_TAG_NAME,),
        recover=recover,
        huge_tree=huge_tree,
    )

    for _event, ge in context:
        stats.groups_seen += 1

        parsed = parse_group_event(ge)
        stats.skipped_records += parsed.skipped

        if parsed.group is not None:
            stats.groups_emitted += 1
            stats.events_emitted += len(parsed.events)
            yield GroupEventBundle(group=parsed.group, events=parsed.events)

        # Очистка памяти:
        ge.clear()
        parent = ge.getparent()
        if parent is not None:
            while ge.getprevious() is not None:
                del parent[0]

    # iterparse держит файл/парсер — чистим
    del context
