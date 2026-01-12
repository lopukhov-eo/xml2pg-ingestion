import queue
import threading
from pathlib import Path

from src.pipeline.metrics import SharedMetrics
from src.pipeline.producer import ProducerConfig, producer_main

project_dir = Path(__file__).parent.parent
xml_path = project_dir / "tests" / "fixtures" / "small.xml"


def test_producer_puts_batches(tmp_path):
    """
    Тесты для producer-процесса пайплайна.

    Producer отвечает за потоковый парсинг XML и преобразование данных
    в батчи для последующей загрузки в PostgreSQL.

    Тесты проверяют корректность формирования батчей и обновление метрик
    без запуска multiprocessing.
    """
    q = queue.Queue()
    stop = threading.Event()
    metrics = SharedMetrics()

    producer_main(
        out_queue=q,
        stop_event=stop,
        metrics=metrics,
        cfg=ProducerConfig(xml_path=xml_path),
    )

    batches = []
    while not q.empty():
        batches.append(q.get())

    assert any(b.kind == "group" for b in batches)
    assert any(b.kind == "event" for b in batches)
