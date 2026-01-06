from dataclasses import dataclass

from src.settings.env_settings import EnvSettings
from src.settings.ini_settings import IniSettings
from src.settings.logging import logger
from src.utils.errors import SettingsError


@dataclass(frozen=True)
class AppSettings:
    """
    Главный класс настроек приложения.

    Объединяет настройки из различных источников:
    - переменные окружения (EnvSettings);
    - INI-файлы конфигурации (IniSettings).
    """

    env: EnvSettings
    ini: IniSettings


def load_settings() -> AppSettings:
    """
    Загружает и валидирует все настройки приложения.

    Используется при инициализации приложения.
    При ошибках загрузки останавливает дальнейшую работу программы.

    :return: Экземпляр AppSettings с валидированными настройками.
    :raises SettingsError: Если произошла ошибка при загрузке
                           или валидации настроек.
    """
    env = EnvSettings.load()
    ini = IniSettings.load()
    return AppSettings(env=env, ini=ini)


try:
    settings = load_settings()
except SettingsError as e:
    logger.error(f"Ошибка при загрузке настроек программы: {e}")
    raise
