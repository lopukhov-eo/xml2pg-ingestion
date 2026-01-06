from pathlib import Path

from src.settings.config import XML_GROUP_TAG_NAME, XML_TAG_NAME


def generate_sample_xml(
    out_path: Path,
    groups: int = 1000,
    events_per_group: int = 2,
) -> None:
    """
    Генерирует синтетический XML-файл для тестов/бенчмарков.

    Формат:
    <xml>
      <group_event id="1">
        <event id="1">Event 1</event>
        <event id="2">Event 2</event>
      </group_event>
    </xml>

    :param out_path: Путь, куда сохранить XML-файл.
    :param groups: Количество элементов <group_event>.
    :param events_per_group: Количество элементов <event>
    внутри каждого <group_event>.
    :return: None.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    eid = 1
    with out_path.open("w", encoding="utf-8") as f:
        f.write("<xml>\n")

        for gid in range(1, groups + 1):
            f.write(f'  <{XML_GROUP_TAG_NAME} id="{gid}">\n')

            for _ in range(events_per_group):
                f.write(
                    f'    <{XML_TAG_NAME} id="{eid}">'
                    f"Event {eid}"
                    f"</{XML_TAG_NAME}>\n"
                )
                eid += 1

            f.write(f"  </{XML_GROUP_TAG_NAME}>\n")
        f.write("</xml>\n")


project_dir = Path(__file__).parent.parent.parent
fixtures_dir = project_dir / "tests" / "fixtures"
xml_path = fixtures_dir / "sample.xml"

generate_sample_xml(out_path=xml_path, groups=1000000, events_per_group=2)
