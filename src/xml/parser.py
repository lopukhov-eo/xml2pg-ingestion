from dataclasses import dataclass
from typing import List, Optional

from lxml import etree


@dataclass(frozen=True)
class GroupEventRecord:
    """
    Запись группы событий, извлечённая из XML.

    Соответствует элементу <group_event> и содержит минимально необходимое поле
    для дальнейшей загрузки/связывания с событиями.

    :ivar id: Идентификатор группы событий. Обязателен, int.
    :ivar name: Название группы (может быть None).
    """

    id: int
    name: Optional[str]


@dataclass(frozen=True)
class EventRecord:
    """
    Запись события, извлечённая из XML.

    Соответствует элементу <event> внутри <group_event>.

    :ivar id: Идентификатор события (<event id="...">). Обязателен, int.
    :ivar group_event_id: Идентификатор родительской группы. Обязателен, int.
    :ivar name: Название события (может быть None).
    """

    id: int
    group_event_id: int
    name: Optional[str]


@dataclass(frozen=True)
class ParseResult:
    """
    Результат парсинга одного элемента <group_event>.

    Используется, чтобы:
    - вернуть распарсенную группу и список её событий;
    - зафиксировать количество пропусков.

    :ivar group: Распарсенная группа событий или None.
    :ivar events: Список распарсенных событий (только с корректным id).
    :ivar skipped: Количество пропущенных записей внутри данного <group_event>
                   (например, event без id, либо сам group_event без id).
    """

    group: Optional[GroupEventRecord]
    events: List[EventRecord]
    skipped: int


def _safe_int(value: Optional[str]) -> Optional[int]:
    """
    Безопасно преобразует строковое значение в int.

    :param value: Значение, которое нужно преобразовать в int.
    :return: int при успехе, иначе None.
    """
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean_text(value: Optional[str]) -> Optional[str]:
    """
    Нормализует строковое значение.

    Удаляет пробелы по краям и заменяет пустую строку на None.

    :param value: Входное строковое значение (может быть None).
    :return: Очищенная строка без пробелов по краям или None,
    если входное значение None/пустое.
    """
    if value is None:
        return None
    s = value.strip()
    return s if s else None


def _extract_group_name(group_el: etree._Element) -> Optional[str]:
    """
    Извлекает имя группы событий из элемента <group_event>.

    Источник имени:
    - атрибут name у тега <group_event name="...">.

    :param group_el: XML-элемент <group_event>.
    :return: Имя группы событий или None, если атрибут name отсутствует/пустой.
    """
    name = _clean_text(group_el.get("name"))
    if name:
        return name
    return None


def _extract_event_name(event_el: etree._Element) -> Optional[str]:
    """
    Извлекает имя события из элемента <event>.

    Источник имени:
    - текст внутри тега <event>TEXT</event>.

    :param event_el: XML-элемент <event>.
    :return: Имя события или None, если текст отсутствует/пустой.
    """
    name = _clean_text(event_el.text)
    if name:
        return name
    return None


def parse_group_event(group_el: etree._Element) -> ParseResult:
    """
    Парсит один элемент <group_event> и все вложенные элементы <event>.

    Правила:
    - <group_event id="..."> обязателен и должен быть целым числом.
      Если id отсутствует/некорректен — вся группа пропускается.
    - <event id="..."> обязателен и должен быть целым числом.
      События без корректного id пропускаются.
    - name для события берётся из текста тега и может быть None.

    :param group_el: XML-элемент <group_event>, полученный из lxml.
    :return: ParseResult с распарсенной группой (или None), списком событий и
    числом пропусков.
    """
    skipped = 0

    group_id = _safe_int(group_el.get("id"))
    if group_id is None:
        # нельзя корректно связать events -> group_event
        return ParseResult(group=None, events=[], skipped=1)

    group = GroupEventRecord(id=group_id, name=_extract_group_name(group_el))

    events: List[EventRecord] = []
    for ev in group_el.iterfind("event"):
        ev_id = _safe_int(ev.get("id"))
        if ev_id is None:
            skipped += 1
            continue
        events.append(
            EventRecord(
                id=ev_id,
                group_event_id=group_id,
                name=_extract_event_name(ev),
            )
        )

    return ParseResult(group=group, events=events, skipped=skipped)
