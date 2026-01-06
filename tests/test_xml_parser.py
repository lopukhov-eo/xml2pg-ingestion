from pathlib import Path

from src.xml.reader import ReaderStats, iter_group_events


def test_iter_group_events_from_fixture():
    """
    Тесты для streaming-парсера XML (iterparse).

    Проверяет, что:
    - XML читается потоково через iter_group_events и возвращает bundles.
    - group_event без атрибута id пропускается.
    - event без атрибута id пропускается.
    - корректно заполняются поля id, group_event_id и name.
    - корректно обновляется статистика ReaderStats.

    :return: None.
    """
    fixtures_dir = Path(__file__).parent / "fixtures"
    xml_path = fixtures_dir / "test.xml"

    stats = ReaderStats()
    bundles = list(iter_group_events(xml_path, stats=stats))

    # group_event without id должен быть пропущен
    assert len(bundles) == 1

    g1 = bundles[0]
    assert g1.group.id == 1
    assert [e.id for e in g1.events] == [10, 11]
    assert [e.group_event_id for e in g1.events] == [1, 1]
    assert [e.name for e in g1.events] == ["Event Ten", "Event Eleven"]

    # Проверим статистику
    assert stats.groups_seen == 2
    assert stats.groups_emitted == 1
    assert stats.events_emitted == 2
    # skipped: 1 event без id + 1 group_event без id
    assert stats.skipped_records >= 2


def test_recover_mode_handles_partial_broken_xml(tmp_path: Path):
    """
    Проверяет, что streaming-парсер XML в режиме recover=True переживает частично повреждённый XML.

    Сценарий:
    - Создаём временный файл broken.xml с намеренно некорректной разметкой (не закрыт <event>).
    - Запускаем iter_group_events(..., recover=True).
    - Ожидаем, что парсер вернёт хотя бы один bundle для group_event id=1 и не упадёт.
    - Гарантируем, что корректно закрытый event (id=10) присутствует в результате
      (event id=11 может быть потерян — зависит от стратегии восстановления парсера).

    :param tmp_path: Временная директория pytest для создания тестового файла.
    :return: None.
    """

    # кусок битого xml: незакрытый тег event — recover=True
    # должен пережить и выдать хоть что-то
    xml = tmp_path / "broken.xml"
    xml.write_text(
        "<xml>"
        '<group_event id="1"><event id="10">Ok</event><event id="11">Broken'
        "</group_event>"
        "</xml>",
        encoding="utf-8",
    )

    bundles = list(iter_group_events(xml, recover=True))
    assert len(bundles) == 1
    assert bundles[0].group.id == 1
    # event 10 точно должен быть, event 11 может быть потерян
    assert any(e.id == 10 for e in bundles[0].events)
