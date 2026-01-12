# XML2PostgreSQL Ingestion

Проект для парсинга XML-файлов и загрузки данных в PostgreSQL через staging-таблицы и COPY FROM STDIN.

В данном проекте выбран **потоковый (streaming) подход к обработке XML** в сочетании с **bulk-загрузкой данных в PostgreSQL через COPY**, поскольку размер входных данных (до ~1 TB) существенно превышает объём доступной оперативной памяти. Использование `lxml.iterparse` позволяет читать и обрабатывать XML инкрементально, освобождая память сразу после обработки элементов, а архитектура producer/consumer с ограниченной очередью обеспечивает естественный backpressure и стабильное потребление памяти независимо от размера файла.

Для загрузки в базу данных применяются **staging-таблицы и COPY FROM STDIN**, что является наиболее производительным и надёжным способом массовой вставки данных в PostgreSQL. Индексы и ограничения создаются только после завершения загрузки, чтобы не снижать throughput. Параллельная запись несколькими worker-процессами позволяет эффективно использовать ресурсы БД, а ретраи и graceful shutdown делают пайплайн устойчивым к временным сбоям. Такой подход соответствует best practices data engineering и хорошо масштабируется при росте объёма данных без усложнения архитектуры.


## 1. Архитектура

Пайплайн загрузки данных:

XML
 ↓
Parser
 ↓
stg_group_event / stg_event   (COPY)
 ↓
group_event / event           (finalize)

## 2. Установка

##### 2.1. Требования
- **Python**: 3.10+
- **PostgreSQL**: 13+ (рекомендуется 14+)
- **Операционная система**: Linux / macOS  
  (Windows — без гарантий производительности)

##### 2.2. Основные библиотеки
- **lxml** — потоковый парсинг XML (`iterparse`)
- **SQLAlchemy** — управление соединениями и DDL
- **psycopg2** или **psycopg3** — COPY FROM STDIN
- **python-dotenv** — конфигурация через `.env`

##### 2.3. Настройка переменных окружения

Необходимо создать .env со следующими параметрами:
```
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_USER=xml2pg_user
POSTGRES_PASSWORD=xml2pg_password
POSTGRES_DB=xml2pg
```

##### 2.4. Запуск контейнера с PostgreSQL
```
docker compose up -d
```

##### 2.5. Установка зависимостей
```
uv sync
```

##### 2.6. Генерация тестового xml файла
```
python -m src.xml.sample_generator --out data/sample.xml --groups 1000000 --events-per-group 2
```
- out - путь сгенерированного файла
- groups - количество групп в файле
- events-per-group - количество событий в одной группе

## 3. Запуск
```
python src/main.py
```

## 4. Провекра работы программы

##### Подключение к БД:
```
docker exec -it xml2pg-postgres psql -U xml2pg_user -d xml2pg
```

##### Посмотреть таблицы:
```
\d
```

##### Проверка staging таблиц:
```
SELECT count(*) FROM stg_group_event;
SELECT count(*) FROM stg_event;
```
##### Проверка финальныех таблиц:
```
SELECT * FROM group_event LIMIT 10;
SELECT * FROM event LIMIT 10;
```

## 5. Описание работы программы
Что происходит при запуске:

##### 5.1. Создаются таблицы
##### 5.2. XML парсится в Python-структуры
##### 5.3. Данные загружаются в stg_* через COPY
##### 5.4. Данные переносятся в финальные таблицы

##### Идемпотентность
- staging-таблицы можно очищать и перезагружать
- финальные таблицы используют ON CONFLICT DO NOTHING
- пайплайн можно запускать повторно без побочных эффектов

## 6. Тесты

## 7. Структура проект
```
xml2pg-ingestion/
├── src/
│   ├── main.py                # Точка входа
│   ├── db/                    
│   │   ├── connection.py      # Подключение к PostgreSQL
│   │   ├── copy.py            # COPY FROM STDIN помощник
│   │   ├── ddl.py             # Инициализация схемы
│   │   ├── finalize.py        # Финализация данных
│   │   ├── models.py          # ORM и Table модели таблиц БД
│   │   └── staging.py         # COPY в staging-таблицы
│   ├── pipeline/              
│   │   ├── batching.py        # Батчирование по rows / bytes
│   │   ├── consumer.py        # COPY в PostgreSQL
│   │   ├── coordinator.py     # Оркестрация процессов
│   │   ├── metrics.py         # Метрики пайплайна 
│   │   └── producer.py        # Потоковый парсинг XML
│   ├── settings/              
│   │   ├── env_settings.py    # Загрузка параметров из .env
│   │   ├── ini_settings.py    # Загрузка параметров из .ini
│   │   ├── logging.py         # Настройки логгера
│   │   └── settings.py        # Загрузка настроек программы
│   ├── utils/
│   │   └── errors.py          # Кастомные ошибки
│   └── xml/                   
│       ├── parser.py          # Извлечение сущностей
│       ├── reader.py          # Streaming iterparse + cleanup
│       └── sample_generator.py # Генератор тестового XML
└── tests/                     
    └── test_xml_parser/       # Тесты парсера XML
```
## 8. Лицензия

#### MIT