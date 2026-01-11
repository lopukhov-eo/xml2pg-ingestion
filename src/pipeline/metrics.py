from dataclasses import dataclass
from multiprocessing import Lock, Value
from time import monotonic
from typing import Dict


@dataclass(frozen=True)
class MetricsSnapshot:
    """
    Снимок метрик пайплайна.

    :param ts: Timestamp monotonic (секунды) на момент снимка.
    :param groups_parsed: Сколько group_event распарсено (валидных, отправленных дальше).
    :param events_parsed: Сколько event распарсено (валидных, отправленных дальше).
    :param groups_enqueued: Сколько group_event строк поставлено в очередь (в батчах).
    :param events_enqueued: Сколько event строк поставлено в очередь (в батчах).
    :param groups_copied: Сколько group_event строк загружено COPY (по отчётам writer-ов).
    :param events_copied: Сколько event строк загружено COPY (по отчётам writer-ов).
    :param batches_enqueued: Сколько батчей поставлено в очередь.
    :param batches_copied: Сколько батчей реально обработано writer-ами.
    :param skipped_records: Сколько записей пропущено на стадии парсинга.
    :param copy_errors: Сколько ошибок COPY (с учётом ретраев).
    """

    ts: float
    groups_parsed: int
    events_parsed: int
    groups_enqueued: int
    events_enqueued: int
    groups_copied: int
    events_copied: int
    batches_enqueued: int
    batches_copied: int
    skipped_records: int
    copy_errors: int

    def as_dict(self) -> Dict[str, int | float]:
        """
        Преобразует снимок метрик в словарь.

        :return: Словарь со значениями метрик и timestamp.
        """
        return {
            "ts": self.ts,
            "groups_parsed": self.groups_parsed,
            "events_parsed": self.events_parsed,
            "groups_enqueued": self.groups_enqueued,
            "events_enqueued": self.events_enqueued,
            "groups_copied": self.groups_copied,
            "events_copied": self.events_copied,
            "batches_enqueued": self.batches_enqueued,
            "batches_copied": self.batches_copied,
            "skipped_records": self.skipped_records,
            "copy_errors": self.copy_errors,
        }


class SharedMetrics:
    """
    Shared-счётчики между процессами (multiprocessing.Value).

    Используем 64-bit integer Value('q') + общий Lock,
    чтобы инкременты были атомарны.

    Важно: это очень лёгкая синхронизация
    (нам не нужны строгие транзакционные метрики),
    но при этом счётчики корректно суммируются между процессами.

    :return: Объект SharedMetrics.
    """

    def __init__(self) -> None:
        """
        Инициализирует все shared-счётчики и общий lock.

        Все значения инициализируются нулём. Экземпляр безопасен для
        использования из нескольких процессов.

        :return: None.
        """
        self._lock = Lock()

        self.groups_parsed = Value("q", 0)
        self.events_parsed = Value("q", 0)

        self.groups_enqueued = Value("q", 0)
        self.events_enqueued = Value("q", 0)

        self.groups_copied = Value("q", 0)
        self.events_copied = Value("q", 0)

        self.batches_enqueued = Value("q", 0)
        self.batches_copied = Value("q", 0)

        self.skipped_records = Value("q", 0)
        self.copy_errors = Value("q", 0)

    def inc(self, field: Value, delta: int = 1) -> None:
        """
        Потокобезопасно увеличивает указанный счётчик.

        :param field: multiprocessing.Value('q'),
        который нужно инкрементировать.
        :param delta: На сколько увеличить.
        :return: None.
        """
        if delta == 0:
            return
        with self._lock:
            field.value += int(delta)

    def snapshot(self) -> MetricsSnapshot:
        """
        Делает консистентный снимок всех счётчиков.

        :return: MetricsSnapshot.
        """
        with self._lock:
            return MetricsSnapshot(
                ts=monotonic(),
                groups_parsed=int(self.groups_parsed.value),
                events_parsed=int(self.events_parsed.value),
                groups_enqueued=int(self.groups_enqueued.value),
                events_enqueued=int(self.events_enqueued.value),
                groups_copied=int(self.groups_copied.value),
                events_copied=int(self.events_copied.value),
                batches_enqueued=int(self.batches_enqueued.value),
                batches_copied=int(self.batches_copied.value),
                skipped_records=int(self.skipped_records.value),
                copy_errors=int(self.copy_errors.value),
            )
