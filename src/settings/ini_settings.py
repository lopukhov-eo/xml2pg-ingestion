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

    xml_tag_name: str
    xml_group_tag_name: str

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
        read_ok = parser.read(path)
        if not read_ok:
            # на всякий случай: read() иногда может вернуть пусто
            raise SettingsError(f"Не удалось прочитать INI файл: {path}")

        xml_tag_name = cls._required(parser, "XML", "name_tag")
        xml_group_tag_name = cls._required(parser, "XML", "group_tag_name")

        return cls(
            xml_tag_name=xml_tag_name,
            xml_group_tag_name=xml_group_tag_name,
        )

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
