from src.db.connection import get_engine
from src.db.ddl import init_db, truncate_staging
from src.db.finalize import finalize
from src.pipeline.coordinator import PipelineConfig, run_pipeline
from src.settings.logging import logger
from src.settings.settings import settings


def init() -> None:
    """
    Инициализация базы данных перед запуском pipeline.

    Выполняет подготовительные шаги:
    1) Создаёт все необходимые таблицы (основные и staging), если их ещё нет
    2) Очищает staging-таблицы для чистого старта ingestion
    3) Логирует завершение инициализации

    Используется как обязательный шаг перед запуском основного pipeline.

    :return: None.
    """
    engine = get_engine()
    init_db(engine)
    logger.info("База данных иницилизирована.")

    truncate_staging(engine)
    logger.info("Временные таблицы очищены.")
    logger.info("Процесс инициализации завершён.")


def main() -> None:
    """
    Точка входа для запуска ingestion pipeline.

    Последовательность выполнения:
    1) Читает конфигурацию pipeline из settings
    2) Запускает streaming XML → PostgreSQL pipeline (producer/consumer + COPY)
    3) Дожидается завершения загрузки всех данных в staging
    4) Выполняет финализацию:
       - дедупликацию данных
       - перенос из staging в основные таблицы
       - восстановление индексов и ограничений

    :return: None.
    """
    engine = get_engine()

    cfg = PipelineConfig(
        xml_path=settings.ini.xml_path,
        workers=settings.ini.amount_workers,
        queue_maxsize=settings.ini.queue_maxsize,
        batch_max_rows=settings.ini.batch_max_rows,
        batch_max_bytes=settings.ini.batch_max_bytes,
        recover=settings.ini.lxml_recover,
        huge_tree=settings.ini.lxml_huge_tree,
        log_interval_sec=settings.ini.log_interval_sec,
    )

    logger.info("Запуск pipeline: %s", cfg)
    snap = run_pipeline(cfg)
    logger.info("Pipeline завершил работу: %s", snap.as_dict())

    finalize(engine)
    logger.info("Данные перенесены из временных в основные таблицы.")


if __name__ == "__main__":
    init()
    main()
