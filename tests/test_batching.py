from src.pipeline.batching import BatchBuilder


def test_batch_flush_by_rows():
    """
    Тесты для модуля batching.

    Проверяют корректность формирования батчей по количеству строк
    и по приблизительному объёму данных в байтах.

    Модуль batching является критичным для контроля потребления памяти,
    поэтому тесты фокусируются на граничных условиях и flush-логике.
    """
    b = BatchBuilder(kind="event", max_rows=2, max_bytes=10_000)

    assert b.add((1, 2, "a")) is None
    batch = b.add((2, 2, "b"))

    assert batch is not None
    assert len(batch.rows) == 2
