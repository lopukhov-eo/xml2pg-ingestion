import configparser
from dataclasses import dataclass
from pathlib import Path

from src.utils.errors import SettingsError

CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config.ini"


@dataclass(frozen=True)
class IniSettings:
    """
    Класс для загрузки и валидации настроек из INI-файла.

    Отвечает за:
    - проверку существования конфигурационного файла;
    - чтение INI-файла;
    - валидацию обязательных секций и ключей;
    - предоставление настроек в виде неизменяемого объекта.

    Используется при старте приложения. При ошибках конфигурации
    выбрасывает исключение ConfigError и останавливает работу программы.
    """

    xml_path: str
    xml_tag_name: str
    xml_group_tag_name: str
    lxml_recover: bool
    lxml_huge_tree: bool
    events_table_name: str
    groups_table_name: str
    amount_workers: int
    queue_maxsize: int
    batch_max_rows: int
    batch_max_bytes: int
    log_interval_sec: float

    # имя_поля_в_классе -> (секция, ключ)
    _MAP = {
        "xml_path": ("XML", "path"),
        "xml_tag_name": ("XML", "tag_name"),
        "xml_group_tag_name": ("XML", "group_tag_name"),
        "lxml_recover": ("XML", "lxml_recover"),
        "lxml_huge_tree": ("XML", "lxml_huge_tree"),
        "events_table_name": ("DB", "events_table_name"),
        "groups_table_name": ("DB", "groups_table_name"),
        "amount_workers": ("PIPELINE", "amount_workers"),
        "queue_maxsize": ("PIPELINE", "queue_maxsize"),
        "batch_max_rows": ("PIPELINE", "batch_max_rows"),
        "batch_max_bytes": ("PIPELINE", "batch_max_bytes"),
        "log_interval_sec": ("LOG", "log_interval_sec"),
    }

    @classmethod
    def load(cls, path: Path = CONFIG_PATH) -> "IniSettings":
        """
        Загружает и валидирует настройки из INI-файла.

        :param path: Путь к INI-файлу конфигурации.
        :return: Экземпляр IniSettings с загруженными настройками.
        :raises ConfigError: Если файл не найден, не прочитан или с ошибками.
        """
        if not path.exists():
            raise SettingsError(f"INI файл не найден: {path}")

        parser = configparser.ConfigParser()
        if not parser.read(path):
            raise SettingsError(f"Не удалось прочитать INI файл: {path}")

        raw_data = {
            field: cls._required(parser, sec, key)
            for field, (sec, key) in cls._MAP.items()
        }
        data = cls._cast_types(raw_data)
        return cls(**data)

    @staticmethod
    def _required(parser: configparser.ConfigParser, section: str, key: str) -> str:
        """
        Возвращает обязательный параметр из указанной секции INI-файла.

        Метод выполняет строгую валидацию:
        - проверяет наличие секции;
        - проверяет наличие ключа в секции;
        - проверяет, что значение ключа не пустое.

        Используется для чтения параметров,
        без которых работа приложения невозможна.

        :param parser: Экземпляр ConfigParser с загруженным INI-файлом.
        :param section: Имя секции INI-файла.
        :param key: Имя параметра в секции.
        :return: Значение параметра в виде строки.
        :raises ConfigError: Если секция, ключ отсутствуют или значение пустое.
        """
        if not parser.has_section(section):
            raise SettingsError(
                f"Секция [{section}] " f"отсутствует в {CONFIG_PATH.name}"
            )

        if not parser.has_option(section, key):
            raise SettingsError(
                f"Ключ '{key}' отсутствует в секции [{section}] "
                f"({CONFIG_PATH.name})"
            )

        value = parser.get(section, key)
        if not value.strip():
            raise SettingsError(
                f"Ключ '{key}' в секции [{section}] " f"пустой ({CONFIG_PATH.name})"
            )

        return value

    @classmethod
    def _cast_types(cls, raw: dict[str, str]) -> dict[str, object]:
        """Приводит строковые значения из INI к типам, указанным в аннотациях IniSettings."""
        result: dict[str, object] = {}

        for field, value in raw.items():
            target_type = cls.__annotations__[field]

            try:
                if target_type is bool:
                    result[field] = value.lower() in {"1", "true", "yes", "on"}
                elif target_type is int:
                    result[field] = int(value)
                elif target_type is float:
                    result[field] = float(value)
                else:
                    result[field] = value
            except ValueError as e:
                raise SettingsError(
                    f"Некорректное значение для '{field}': {value}"
                ) from e

        return result
