import io
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Protocol, Sequence, runtime_checkable

from sqlalchemy.engine import Engine

from src.db.connection import raw_connection


@dataclass(frozen=True)
class CopySpec:
    """
    Спецификация COPY-операции.

    Содержит имя таблицы и порядок колонок, в которые будет выполняться COPY.

    :param table: Имя таблицы назначения (например, "stg_event").
    :param columns: Последовательность имён колонок в том порядке,
    в котором идут значения в строках.
    """

    table: str
    columns: Sequence[str]


# Таблица трансляции для быстрого экранирования спецсимволов для COPY TEXT.
# PostgreSQL COPY ... FORMAT text использует backslash-экранирование.
_TRANSLATE = {
    ord("\\"): "\\\\",  # обратный слэш
    ord("\t"): "\\t",  # таб
    ord("\n"): "\\n",  # перевод строки
    ord("\r"): "\\r",  # возврат каретки
    ord("\b"): "\\b",  # backspace
    ord("\f"): "\\f",  # formfeed
    ord("\v"): "\\v",  # vertical tab
}


def _escape_copy_text(value: Any) -> str:
    r"""
    Преобразует значение поля в строку для PostgreSQL COPY ... FORMAT text.

    Правила:
    - None кодируем как \\N (маркер NULL в COPY).
    - Спецсимволы (tab/newline/backslash и т.п.) экранируем по COPY TEXT.

    :param value: Значение поля (любой тип, приводимый к str).
    :return: Строковое представление, безопасное для COPY TEXT.
    """
    # NULL в COPY TEXT передаётся специальным маркером \N
    if value is None:
        return r"\N"

    s = str(value)

    # Быстрый путь: если строка не содержит спецсимволов, возвращаем как есть
    if ("\t" not in s) and ("\n" not in s) and ("\r" not in s) and ("\\" not in s):
        return s

    # Экранирование спецсимволов через translate
    return s.translate(_TRANSLATE)


def _text_lines(rows: Iterable[Sequence[Any]]) -> Iterator[str]:
    r"""
    Генерирует строки для COPY в текстовом формате (text).

    Каждая строка:
    - поля разделены табом '\\t'
    - строка заканчивается '\\n'
    - None кодируется как '\\N'

    :param rows: Итератор строк (строка — последовательность значений полей).
    :return: Итератор строк (готовых к записи в COPY).
    """
    esc = _escape_copy_text

    for row in rows:
        # Собираем одну строку COPY: TAB между полями + LF в конце
        yield "\t".join(esc(v) for v in row) + "\n"


def _bytes_chunks(
    rows: Iterable[Sequence[Any]],
    *,
    max_chunk_bytes: int,
) -> Iterator[tuple[bytes, int]]:
    """
    Генерирует чанки байтов для COPY TEXT (оптимизация под psycopg3).

    Вместо записи каждой строки отдельно делаем буферизацию:
    - собираем несколько строк в bytearray
    - как только буфер достигает max_chunk_bytes — "сбрасываем" chunk

    Это снижает overhead на большое число вызовов copy.write().

    :param rows: Итератор строк (строка — последовательность значений полей).
    :param max_chunk_bytes: Максимальный размер одного чанка в байтах.
    :return: Итератор кортежей (chunk_bytes, rows_in_chunk).
    """
    esc = _escape_copy_text
    encode = str.encode

    buf = bytearray()
    rows_in_buf = 0

    for row in rows:
        # Формируем одну строку COPY TEXT как str, затем кодируем в UTF-8.
        # Важно: поля разделены табом, строка заканчивается \n.
        line_str = "\t".join(esc(v) for v in row) + "\n"
        line_bytes = encode(line_str, "utf-8")

        # Добавляем в буфер
        buf.extend(line_bytes)
        rows_in_buf += 1

        # Если буфер достиг лимита — отдаём chunk наружу и очищаем буфер
        if len(buf) >= max_chunk_bytes:
            yield bytes(buf), rows_in_buf
            buf.clear()
            rows_in_buf = 0

    # Отдаём хвост буфера
    if buf:
        yield bytes(buf), rows_in_buf


class _IterTextIO(io.TextIOBase):
    """
    Адаптер iterator[str] -> file-like объект для psycopg2 copy_expert().

    psycopg2 ожидает файловый объект с методом read().
    Мы подсовываем поток строк, который "читается" кусками.

    :param it: Итератор строк (готовых к COPY).
    """

    def __init__(self, it: Iterator[str]) -> None:
        super().__init__()
        self._it = it
        self._buf = ""

    def readable(self) -> bool:
        """
        Сообщаем, что поток поддерживает чтение.

        :return: True.
        """
        return True

    def read(self, size: int = -1) -> str:
        """
        Читает из итератора строк и возвращает строку указанного размера.

        - size = -1: прочитать всё до конца.
        - иначе: наполняем внутренний буфер, пока не наберём нужный размер
        или не кончится итератор

        :param size: Количество символов для чтения.
        :return: Строка прочитанных данных.
        """
        if size == -1:
            return self._buf + "".join(self._it)

        while len(self._buf) < size:
            try:
                self._buf += next(self._it)
            except StopIteration:
                break

        out, self._buf = self._buf[:size], self._buf[size:]
        return out


@runtime_checkable
class _Psycopg3CopyCursor(Protocol):
    """
    Протокол курсора psycopg3, который поддерживает метод copy().

    Нужен, чтобы аккуратно определить "psycopg3 путь" через isinstance().
    """

    def copy(self, sql: str): ...


def copy_rows(
    engine: Engine,
    spec: CopySpec,
    rows: Iterable[Sequence[Any]],
    *,
    max_chunk_bytes: int = 8 * 1024 * 1024,
) -> int:
    r"""
    Выполняет быструю загрузку данных в PostgreSQL через COPY FROM STDIN.

    Поддерживаются два режима:
    - psycopg3: используем cursor.copy() и пишем чанки bytes
    - psycopg2: используем cursor.copy_expert() и file-like поток строк

    COPY идёт в формате text:
    - DELIMITER = '\\t'
    - NULL = '\\N'

    :param engine: SQLAlchemy Engine.
    :param spec: Спецификация таблицы и колонок для COPY.
    :param rows: Итератор данных (строки значений).
    :param max_chunk_bytes: Максимальный размер чанка для
                            psycopg3-записи (в байтах).
    :return: Количество загруженных строк (для psycopg3) или
            -1 (для psycopg2 fallback).
    """
    cols = ", ".join(spec.columns)

    # Формируем SQL для COPY.
    # Важно: явно задаём формат/разделитель/маркер NULL.
    sql = (
        f"COPY {spec.table} ({cols}) "
        "FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
    )

    # raw_connection даёт доступ к DBAPI соединению (psycopg3/psycopg2),
    # что нужно для COPY FROM STDIN.
    with raw_connection(engine) as dbapi_conn:
        cur = dbapi_conn.cursor()
        try:
            # Ветка psycopg3: умеет cur.copy(sql)
            if isinstance(cur, _Psycopg3CopyCursor):
                total = 0
                with cur.copy(sql) as copy:
                    # Пишем крупными чанками bytes,
                    # чтобы уменьшить overhead на write()
                    for chunk, nrows in _bytes_chunks(
                        rows, max_chunk_bytes=max_chunk_bytes
                    ):
                        copy.write(chunk)
                        total += nrows

                dbapi_conn.commit()
                return total

            # Ветка psycopg2: используем copy_expert и файловый интерфейс
            stream = _IterTextIO(_text_lines(rows))
            cur.copy_expert(sql, stream)
            dbapi_conn.commit()

            # Для psycopg2 точный подсчёт строк не делаем
            # (чтобы не проходить rows второй раз)
            return -1

        except Exception:
            # В случае ошибки обязательно откатываем транзакцию
            dbapi_conn.rollback()
            raise
        finally:
            cur.close()
