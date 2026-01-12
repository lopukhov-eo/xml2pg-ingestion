from pathlib import Path

from src.pipeline import PipelineConfig, run_pipeline

project_dir = Path(__file__).parent.parent
xml_path = project_dir / "tests" / "fixtures" / "small.xml"


def test_pipeline_smoke(tmp_path):
    """
    Smoke-тест для coordinator пайплайна.

    Проверяет, что полный ingestion-пайплайн
    (producer → consumer → COPY → метрики)
    успешно выполняется на небольшом XML-файле
    без исключений и зависаний.
    """
    snap = run_pipeline(
        PipelineConfig(
            xml_path=xml_path,
            workers=1,
            queue_maxsize=2,
            batch_max_rows=2,
        )
    )
    assert snap.groups_copied > 0
