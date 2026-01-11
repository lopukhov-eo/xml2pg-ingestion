from dataclasses import dataclass
from multiprocessing import Event
from multiprocessing.queues import Queue
from pathlib import Path

from src.pipeline.batching import Batch, BatchBuilder
from src.pipeline.metrics import SharedMetrics
from src.settings.logging import logger
from src.xml.reader import ReaderStats, iter_group_events


@dataclass(frozen=True)
class ProducerConfig:
    """
    Конфигурация producer-процесса.

    :param xml_path: Путь к XML файлу.
    :param recover: lxml recover (терпим частично битый XML).
    :param huge_tree: lxml huge_tree.
    :param batch_max_rows: Максимум строк в батче.
    :param batch_max_bytes: Максимум "оценочных" байт в батче.
    """

    xml_path: Path
    recover: bool = True
    huge_tree: bool = True
    batch_max_rows: int = 50_000
    batch_max_bytes: int = 8 * 1024 * 1024


def producer_main(
    out_queue: Queue,
    stop_event: Event,
    metrics: SharedMetrics,
    cfg: ProducerConfig,
) -> None:
    """
    Потоково читает XML, батчит и кладёт батчи в общую очередь.

    Сообщения в очереди:
    - Batch(kind="group", rows=[(id, name), ...])
    - Batch(kind="event", rows=[(id, group_event_id, name), ...])
    - None как sentinel (producer не отправляет None; это делает coordinator)

    :param out_queue: Общая multiprocessing очередь для батчей.
    :param stop_event: Событие остановки (graceful shutdown).
    :param metrics: SharedMetrics.
    :param cfg: ProducerConfig.
    :return: None.
    """
    stats = ReaderStats()

    group_batcher = BatchBuilder(
        kind="group",
        max_rows=cfg.batch_max_rows,
        max_bytes=cfg.batch_max_bytes,
    )
    event_batcher = BatchBuilder(
        kind="event",
        max_rows=cfg.batch_max_rows,
        max_bytes=cfg.batch_max_bytes,
    )

    logger.info(
        "Producer started. xml=%s batch_rows=%s batch_bytes=%s",
        str(cfg.xml_path),
        cfg.batch_max_rows,
        cfg.batch_max_bytes,
    )

    try:
        for bundle in iter_group_events(
            cfg.xml_path,
            recover=cfg.recover,
            huge_tree=cfg.huge_tree,
            stats=stats,
        ):
            if stop_event.is_set():
                break

            # group row
            g = bundle.group
            metrics.inc(metrics.groups_parsed, 1)

            maybe = group_batcher.add((g.id, g.name))
            if maybe is not None:
                _put_batch(out_queue, stop_event, metrics, maybe)

            # event rows
            if bundle.events:
                metrics.inc(metrics.events_parsed, len(bundle.events))
                for ev in bundle.events:
                    maybe_ev = event_batcher.add((ev.id, ev.group_event_id, ev.name))
                    if maybe_ev is not None:
                        _put_batch(out_queue, stop_event, metrics, maybe_ev)

        # flush tails
        tail_g = group_batcher.flush()
        if tail_g is not None and not stop_event.is_set():
            _put_batch(out_queue, stop_event, metrics, tail_g)

        tail_e = event_batcher.flush()
        if tail_e is not None and not stop_event.is_set():
            _put_batch(out_queue, stop_event, metrics, tail_e)

    finally:
        # переносим skipped из ReaderStats
        # (там копится внутри iter_group_events)
        metrics.inc(metrics.skipped_records, int(stats.skipped_records))

        logger.info(
            "Producer finished. groups_seen=%s "
            "groups_emitted=%s events_emitted=%s skipped=%s",
            stats.groups_seen,
            stats.groups_emitted,
            stats.events_emitted,
            stats.skipped_records,
        )


def _put_batch(
    out_queue: Queue,
    stop_event: Event,
    metrics: SharedMetrics,
    batch: Batch,
) -> None:
    """
    Кладёт батч в очередь с учётом stop_event.

    :param out_queue: multiprocessing.Queue.
    :param stop_event: Event.
    :param metrics: SharedMetrics.
    :param batch: Batch.
    :return: None.
    """
    if stop_event.is_set():
        return

    out_queue.put(batch)  # backpressure тут
    metrics.inc(metrics.batches_enqueued, 1)

    if batch.kind == "group":
        metrics.inc(metrics.groups_enqueued, len(batch.rows))
    else:
        metrics.inc(metrics.events_enqueued, len(batch.rows))
