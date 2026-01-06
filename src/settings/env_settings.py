import os
from dataclasses import dataclass

from src.utils.errors import SettingsError


@dataclass(frozen=True)
class EnvSettings:
    """
    Класс для загрузки и валидации настроек из переменных окружения.

    Отвечает за:
    - чтение обязательных переменных окружения;
    - валидацию наличия и корректности значений;
    - формирование строки подключения к базе данных (DB_URL);
    - остановку приложения при ошибках конфигурации.

    Используется при старте приложения. При отсутствии обязательных
    переменных окружения выбрасывает ConfigError.
    """

    db_url: str

    @classmethod
    def load(cls) -> "EnvSettings":
        """
        Загружает и валидирует настройки из переменных окружения.

        :return: Экземпляр EnvSettings с корректно загруженными настройками.
        :raises ConfigError: Если обязательные переменные окружения отсутствуют
                             или имеют некорректный формат.
        """
        host = cls._required("POSTGRES_HOST")
        port = cls._int("POSTGRES_PORT", default=5432)
        db = cls._required("POSTGRES_DB")
        user = cls._required("POSTGRES_USER")
        password = cls._required("POSTGRES_PASSWORD")

        db_url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"
        return cls(db_url=db_url)

    @staticmethod
    def _required(name: str) -> str:
        """
        Возвращает обязательную переменную окружения.

        :param name: Имя переменной окружения.
        :return: Значение переменной окружения в виде строки.
        :raises ConfigError: Если переменная окружения отсутствует или пуста.
        """
        value = os.getenv(name)
        if value is None or value.strip() == "":
            raise SettingsError(
                f"ENV {name} не задан. Проверь .env / переменные окружения."
            )
        return value

    @staticmethod
    def _int(name: str, default: int | None = None) -> int:
        """
        Возвращает целочисленную переменную окружения.

        Метод:
        - читает значение переменной окружения;
        - при отсутствии значения возвращает default (если он задан);
        - приводит значение к типу int;
        - валидирует корректность формата.

        :param name: Имя переменной окружения.
        :param default: Значение по умолчанию, если переменная не задана.
        :return: Значение переменной окружения в виде целого числа.
        :raises ConfigError: Если значение отсутствует и default не задан,
                             либо если значение невозможно привести к int.
        """
        value = os.getenv(name)
        if value is None or value.strip() == "":
            if default is None:
                raise SettingsError(f"ENV {name} не задан и не имеет default.")
            return default
        try:
            return int(value)
        except ValueError:
            raise SettingsError(
                f"ENV {name} должен быть числом, " f"получено: {value!r}"
            )
