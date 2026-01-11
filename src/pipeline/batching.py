from dataclasses import dataclass
from typing import Any, Iterable, Iterator, List, Sequence, Tuple

Row = Tuple[Any, ...]


def _estimate_copy_text_row_bytes(row: Sequence[Any]) -> int:
    r"""
    Приблизительная оценка размера строки COPY TEXT (в байтах).

    Нам не нужна идеальная точность — цель контролировать память батча.
    Оцениваем как сумму длины str(value) (или 2 для '\\N') + табы + '\\n'.

    :param row: Строка значений.
    :return: Оценка размера в байтах (int).
    """
    size = 1  # '\n'
    if not row:
        return size

    # табы между полями: (n-1)
    size += max(0, len(row) - 1)

    for v in row:
        if v is None:
            size += 2  # \N
        else:
            # worst-ish: utf-8 может быть > len(str), но это ок для контроля
            size += len(str(v))
    return size


@dataclass(frozen=True)
class Batch:
    """
    Батч строк для загрузки.

    :param kind: Тип данных ("group" или "event").
    :param rows: Список строк (tuples), готовых для COPY.
    """

    kind: str
    rows: List[Row]


class BatchBuilder:
    """
    Накопитель строк в батч по двум лимитам: max_rows и max_bytes.

    :param kind: "group" или "event".
    :param max_rows: Максимум строк в батче.
    :param max_bytes: Максимум "оценочных" байт в батче.
    """

    def __init__(self, *, kind: str, max_rows: int, max_bytes: int) -> None:
        """
        Создаёт новый BatchBuilder для указанного типа данных.

        :param kind: Тип данных батча ("group" или "event").
        :param max_rows: Максимально допустимое количество строк в батче.
        :param max_bytes: Максимально допустимый оценочный размер батча в байтах.
        :return: None.
        """
        self.kind = kind
        self.max_rows = int(max_rows)
        self.max_bytes = int(max_bytes)

        self._rows: List[Row] = []
        self._bytes: int = 0

    def __len__(self) -> int:
        """
        Возвращает текущее количество строк в накапливаемом батче.

        :return: Количество строк в буфере.
        """
        return len(self._rows)

    @property
    def bytes_estimate(self) -> int:
        """
        Возвращает текущую оценку размера батча в байтах.

        :return: Оценочный размер батча в байтах.
        """
        return self._bytes

    def add(self, row: Row) -> Batch | None:
        """
        Добавляет строку.

        Если после добавления превышены лимиты — возвращает готовый батч
        и начинает новый (с текущей строкой уже внутри).

        :param row: Строка данных.
        :return: Batch, если батч "сброшен", иначе None.
        """
        row_bytes = _estimate_copy_text_row_bytes(row)

        # если один row сам по себе огромный
        # — всё равно грузим отдельным батчем
        if self._rows and (
            (len(self._rows) + 1 > self.max_rows)
            or (self._bytes + row_bytes > self.max_bytes)
        ):
            out = Batch(kind=self.kind, rows=self._rows)
            self._rows = [row]
            self._bytes = row_bytes
            return out

        self._rows.append(row)
        self._bytes += row_bytes

        if len(self._rows) >= self.max_rows or self._bytes >= self.max_bytes:
            return self.flush()

        return None

    def flush(self) -> Batch | None:
        """
        Возвращает текущий батч и очищает буфер.

        :return: Batch или None, если буфер пуст.
        """
        if not self._rows:
            return None
        out = Batch(kind=self.kind, rows=self._rows)
        self._rows = []
        self._bytes = 0
        return out


def iter_batches(
    rows: Iterable[Row],
    *,
    kind: str,
    max_rows: int,
    max_bytes: int,
) -> Iterator[Batch]:
    """
    Утилита: преобразует поток строк в поток батчей.

    :param rows: Итератор строк.
    :param kind: Тип ("group"/"event").
    :param max_rows: Лимит строк.
    :param max_bytes: Лимит байт (оценочный).
    :return: Итератор Batch.
    """
    b = BatchBuilder(kind=kind, max_rows=max_rows, max_bytes=max_bytes)
    for r in rows:
        maybe = b.add(r)
        if maybe is not None:
            yield maybe
    tail = b.flush()
    if tail is not None:
        yield tail
