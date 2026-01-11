import time
from dataclasses import dataclass
from multiprocessing import Event
from multiprocessing.queues import Queue
from typing import Optional

from sqlalchemy.engine import Engine

from src.db.connection import get_engine
from src.db.staging import StagingLoader
from src.pipeline.batching import Batch
from src.pipeline.metrics import SharedMetrics
from src.settings.logging import logger


@dataclass(frozen=True)
class ConsumerConfig:
    """
    Конфигурация consumer-процесса.

    :param worker_id: Идентификатор воркера (для логов).
    :param copy_retries: Кол-во ретраев при ошибке COPY.
    :param retry_base_sleep_sec: Базовая задержка ретрая.
    :param queue_get_timeout_sec: Таймаут ожидания сообщений в очереди.
    """

    worker_id: int
    copy_retries: int = 5
    retry_base_sleep_sec: float = 0.5
    queue_get_timeout_sec: float = 1.0


def consumer_main(
    in_queue: Queue,
    stop_event: Event,
    metrics: SharedMetrics,
    cfg: ConsumerConfig,
) -> None:
    """
    Consumer: читает Batch из очереди и делает COPY в staging.

    Sentinel для остановки: None.

    :param in_queue: multiprocessing.Queue с Batch/None.
    :param stop_event: Event для остановки.
    :param metrics: SharedMetrics.
    :param cfg: ConsumerConfig.
    :return: None.
    """
    engine: Engine = get_engine()
    loader = StagingLoader(engine)

    logger.info("Consumer#%s запущен", cfg.worker_id)

    while True:
        if stop_event.is_set():
            break

        try:
            msg = in_queue.get(timeout=cfg.queue_get_timeout_sec)
        except Exception as e:
            logger.warning(str(e))
            # timeout или другое — просто проверим stop_event и продолжим
            continue

        if msg is None:
            # sentinel
            break

        if not isinstance(msg, Batch):
            logger.warning(
                "Consumer#%s получил необрабатываемое сообщение: %r", cfg.worker_id, msg
            )
            continue

        ok = _process_batch(loader, msg, metrics, cfg)
        if not ok:
            # фатальная ошибка после ретраев
            stop_event.set()
            break

    logger.info("Consumer#%s закончил работу", cfg.worker_id)


def _process_batch(
    loader: StagingLoader,
    batch: Batch,
    metrics: SharedMetrics,
    cfg: ConsumerConfig,
) -> bool:
    """
    Обрабатывает один Batch с ретраями.

    :param loader: StagingLoader.
    :param batch: Batch.
    :param metrics: SharedMetrics.
    :param cfg: ConsumerConfig.
    :return: True если успех, иначе False.
    """
    last_err: Optional[BaseException] = None

    for attempt in range(cfg.copy_retries + 1):
        try:
            if batch.kind == "group":
                n = loader.copy_group_events(batch.rows)
                # psycopg2 ветка вернёт -1, тогда используем len(rows)
                metrics.inc(metrics.groups_copied, len(batch.rows) if n == -1 else n)
            elif batch.kind == "event":
                n = loader.copy_events(batch.rows)
                metrics.inc(metrics.events_copied, len(batch.rows) if n == -1 else n)
            else:
                logger.warning(
                    "Consumer#%s неизвестный тип батча=%s", cfg.worker_id, batch.kind
                )
                return True

            metrics.inc(metrics.batches_copied, 1)
            return True

        except Exception as e:
            last_err = e
            metrics.inc(metrics.copy_errors, 1)

            if attempt >= cfg.copy_retries:
                logger.exception(
                    "Consumer#%s COPY failed окончательно. kind=%s rows=%s",
                    cfg.worker_id,
                    batch.kind,
                    len(batch.rows),
                )
                return False

            sleep_s = cfg.retry_base_sleep_sec * (2**attempt)
            logger.warning(
                "Consumer#%s COPY error (attempt %s/%s), sleep %.2fs: %r",
                cfg.worker_id,
                attempt + 1,
                cfg.copy_retries,
                sleep_s,
                e,
            )
            time.sleep(sleep_s)

    # формально сюда не дойдём
    if last_err is not None:
        logger.error(
            "Consumer#%s неожиданный выход с ошибкой: %r", cfg.worker_id, last_err
        )
    return False
