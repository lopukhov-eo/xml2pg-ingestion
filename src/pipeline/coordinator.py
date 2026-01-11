import multiprocessing as mp
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List

from src.pipeline.consumer import ConsumerConfig, consumer_main
from src.pipeline.metrics import MetricsSnapshot, SharedMetrics
from src.pipeline.producer import ProducerConfig, producer_main
from src.settings.logging import logger


@dataclass(frozen=True)
class PipelineConfig:
    """
    Конфиг координатора пайплайна.

    :param xml_path: Путь к XML.
    :param workers: Кол-во writer процессов.
    :param queue_maxsize: Размер очереди батчей (backpressure).
    :param batch_max_rows: Лимит строк в батче.
    :param batch_max_bytes: Лимит байт (оценочный) в батче.
    :param recover: lxml recover.
    :param huge_tree: lxml huge_tree.
    :param log_interval_sec: Интервал логирования метрик координатором.
    """

    xml_path: Path
    workers: int = 4
    queue_maxsize: int = 32
    batch_max_rows: int = 50_000
    batch_max_bytes: int = 8 * 1024 * 1024
    recover: bool = True
    huge_tree: bool = True
    log_interval_sec: float = 5.0


def run_pipeline(cfg: PipelineConfig) -> MetricsSnapshot:
    """
    Запускает producer/consumer пайплайн и ждёт завершения.

    Схема:
    - producer процесс: читает XML -> кладёт Batch в очередь
    - N consumer процессов: читают Batch -> COPY в staging
    - по завершению producer coordinator отправляет N sentinel (None)

    :param cfg: PipelineConfig.
    :return: Финальный MetricsSnapshot.
    """
    ctx = mp.get_context("spawn")

    metrics = SharedMetrics()
    stop_event = ctx.Event()
    queue = ctx.Queue(maxsize=cfg.queue_maxsize)

    consumers: List[mp.Process] = []
    for i in range(cfg.workers):
        p = ctx.Process(
            target=consumer_main,
            name=f"consumer-{i}",
            args=(
                queue,
                stop_event,
                metrics,
                ConsumerConfig(worker_id=i),
            ),
            daemon=True,
        )
        p.start()
        consumers.append(p)

    producer_cfg = ProducerConfig(
        xml_path=cfg.xml_path,
        recover=cfg.recover,
        huge_tree=cfg.huge_tree,
        batch_max_rows=cfg.batch_max_rows,
        batch_max_bytes=cfg.batch_max_bytes,
    )
    producer = ctx.Process(
        target=producer_main,
        name="producer",
        args=(queue, stop_event, metrics, producer_cfg),
        daemon=True,
    )
    producer.start()

    last = metrics.snapshot()
    last_log_t = time.monotonic()

    try:
        while True:
            if stop_event.is_set():
                break

            if not producer.is_alive():
                break

            now = time.monotonic()
            if now - last_log_t >= cfg.log_interval_sec:
                snap = metrics.snapshot()
                _log_progress(snap, last)
                last = snap
                last_log_t = now

            time.sleep(0.2)

    finally:
        # дождаться producer
        producer.join(timeout=10)

        # если producer умер с ошибкой — стопаем всё
        if producer.exitcode not in (0, None):
            stop_event.set()
            logger.error("Producer exitcode=%s", producer.exitcode)

        # отправляем sentinel каждому consumer
        for _ in consumers:
            try:
                queue.put(None)
            except Exception as e:
                logger.warning("Producer exception: %s", e)
                # если очередь/пайп сломан — просто продолжим
                pass

        # ждём consumers
        for p in consumers:
            p.join(timeout=30)

        # если кто-то упал — это важно
        bad = [p for p in consumers if p.exitcode not in (0, None)]
        if bad:
            stop_event.set()
            logger.error(
                "Some consumers failed: %s", [(p.name, p.exitcode) for p in bad]
            )

    final = metrics.snapshot()
    _log_progress(final, last)
    return final


def _log_progress(cur: MetricsSnapshot, prev: MetricsSnapshot) -> None:
    """
    Логирует прогресс и throughput между двумя снимками.

    :param cur: Текущий снимок.
    :param prev: Предыдущий снимок.
    :return: None.
    """
    dt = max(1e-6, cur.ts - prev.ts)

    dg = cur.groups_copied - prev.groups_copied
    de = cur.events_copied - prev.events_copied

    logger.info(
        "progress: copied groups=%s events=%s (%.0f g/s, %.0f e/s) "
        "enqueued_batches=%s copied_batches=%s skipped=%s copy_errors=%s",
        cur.groups_copied,
        cur.events_copied,
        dg / dt,
        de / dt,
        cur.batches_enqueued,
        cur.batches_copied,
        cur.skipped_records,
        cur.copy_errors,
    )
