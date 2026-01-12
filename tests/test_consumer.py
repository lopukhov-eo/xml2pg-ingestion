class FakeLoader:
    def copy_events(self, rows):
        return len(rows)


def test_consumer_processes_batch(monkeypatch):
    """
    Тесты для consumer-процесса пайплайна.

    Consumer отвечает за чтение батчей из очереди и загрузку данных
    в staging-таблицы PostgreSQL через COPY.

    Для тестов используется подмена StagingLoader, чтобы исключить
    реальные обращения к базе данных.
    """
    monkeypatch.setattr("pipeline.consumer.StagingLoader", FakeLoader)
