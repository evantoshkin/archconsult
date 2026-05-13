# Техническое задание: REST API для поиска пути между системами в PostgreSQL + Apache AGE

## 1. Назначение сервиса

Необходимо разработать backend-сервис на FastAPI, который предоставляет REST API для поиска пути между двумя системами.

Клиент передает идентификаторы систем:

- `from_system_id` - идентификатор исходной системы;
- `to_system_id` - идентификатор целевой системы.

Сервис выполняет запрос в существующую базу данных PostgreSQL с установленным Apache AGE и возвращает результат поиска пути в JSON-формате.

Под капотом должен использоваться существующий рабочий Cypher/SQL-запрос к графу `rsm_eotar_interface`.

## 2. Общая схема работы

1. Клиент вызывает REST endpoint сервиса.
2. FastAPI валидирует входные параметры.
3. Сервис открывает соединение с PostgreSQL.
4. Сервис выполняет SQL-запрос, внутри которого вызывается `cypher(...)` Apache AGE.
5. В запрос подставляются `from_system_id` и `to_system_id`.
6. База данных возвращает найденный путь.
7. Сервис преобразует результат БД в JSON.
8. Клиент получает JSON-ответ.

## 3. Технологический стек

Обязательные технологии:

- Python 3.11+;
- FastAPI;
- Uvicorn;
- PostgreSQL;
- Apache AGE;
- Pydantic v2;
- SQLAlchemy 2.x async или `asyncpg`.

Рекомендуемый вариант доступа к БД:

- `asyncpg`, если нужен максимально простой и прямой доступ к PostgreSQL;
- либо SQLAlchemy async, если в проекте уже используется SQLAlchemy.

## 4. Конфигурация сервиса

Сервис должен получать настройки из переменных окружения.

Обязательные переменные:

| Переменная | Описание | Пример |
|---|---|---|
| `APP_HOST` | host для запуска приложения | `0.0.0.0` |
| `APP_PORT` | port для запуска приложения | `8000` |
| `POSTGRES_HOST` | host PostgreSQL | `localhost` |
| `POSTGRES_PORT` | port PostgreSQL | `5432` |
| `POSTGRES_DB` | имя БД | `rsm` |
| `POSTGRES_USER` | пользователь БД | `rsm_user` |
| `POSTGRES_PASSWORD` | пароль БД | `password` |
| `POSTGRES_POOL_MIN_SIZE` | минимальный размер пула соединений | `1` |
| `POSTGRES_POOL_MAX_SIZE` | максимальный размер пула соединений | `10` |
| `AGE_GRAPH_NAME` | имя графа Apache AGE | `rsm_eotar_interface` |
| `PATH_SEARCH_LIMIT` | лимит путей внутри Cypher-запроса | `1000` |

Дополнительно:

| Переменная | Описание | Пример |
|---|---|---|
| `LOG_LEVEL` | уровень логирования | `INFO` |
| `DB_STATEMENT_TIMEOUT_MS` | timeout SQL-запроса | `30000` |

## 5. REST API

### 5.1 Получение наиболее частого кратчайшего пути

Endpoint:

```http
GET /api/v1/paths
```

Описание:

Возвращает один путь между исходной и целевой системой. Логика выбора результата должна соответствовать текущему SQL-запросу:

```sql
ORDER BY frequency DESC, path_length ASC
LIMIT 1
```

То есть сервис возвращает путь с максимальной частотой, а при равной частоте - с минимальной длиной.

Query parameters:

| Параметр | Тип | Обязательный | Описание |
|---|---|---:|---|
| `from_system_id` | string | да | RSM ID исходной системы |
| `to_system_id` | string | да | RSM ID целевой системы |

Пример запроса:

```http
GET /api/v1/paths?from_system_id=61fd350d49dcba598b5475f8&to_system_id=619e0d20cab6bb05226e3e4b
```

Пример успешного ответа:

```json
{
  "from_system_id": "61fd350d49dcba598b5475f8",
  "from_system_name": "Исходная система",
  "to_system_id": "619e0d20cab6bb05226e3e4b",
  "to_system_name": "Целевая система",
  "path_length": 3,
  "path": "[{\"id\": 844424930131969, \"label\": \"SYSTEM\", \"properties\": {\"system_rsm_id\": \"61fd350d49dcba598b5475f8\"}}]",
  "frequency": 12,
  "example_eotar_rsm_id": "eotar_rsm_id_example"
}
```

Важно:

- поле `path` возвращается из БД как `agtype`;
- на первом этапе допустимо возвращать `path` как строку;
- если будет реализован парсинг `agtype`, поле `path` можно возвращать как JSON-массив объектов, но это должно быть отдельно согласовано и покрыто тестами.

### 5.2 Healthcheck

Endpoint:

```http
GET /health
```

Назначение:

Проверка, что приложение запущено.

Пример ответа:

```json
{
  "status": "ok"
}
```

### 5.3 Readiness check

Endpoint:

```http
GET /ready
```

Назначение:

Проверка доступности PostgreSQL и возможности выполнить простой запрос.

Пример ответа при успешной проверке:

```json
{
  "status": "ready",
  "database": "ok"
}
```

Пример ответа при недоступной БД:

```json
{
  "status": "not_ready",
  "database": "error"
}
```

HTTP status при недоступной БД: `503 Service Unavailable`.

## 6. Формат ответа API

### 6.1 Успешный ответ, путь найден

HTTP status: `200 OK`

Тело ответа:

```json
{
  "from_system_id": "string",
  "from_system_name": "string",
  "to_system_id": "string",
  "to_system_name": "string",
  "path_length": 0,
  "path": "string или object/array после парсинга agtype",
  "frequency": 0,
  "example_eotar_rsm_id": "string"
}
```

Описание полей:

| Поле | Тип | Описание |
|---|---|---|
| `from_system_id` | string | ID исходной системы |
| `from_system_name` | string | имя исходной системы |
| `to_system_id` | string | ID целевой системы |
| `to_system_name` | string | имя целевой системы |
| `path_length` | integer | длина найденного пути |
| `path` | string / array / object | путь из поля `nodes(p)` Apache AGE |
| `frequency` | integer | количество одинаковых путей в выборке |
| `example_eotar_rsm_id` | string | пример ID EOTAR из первого ребра пути |

### 6.2 Успешный ответ, путь не найден

Если запрос выполнен успешно, но БД не вернула строку, API должно вернуть:

HTTP status: `404 Not Found`

```json
{
  "detail": {
    "code": "PATH_NOT_FOUND",
    "message": "Path between systems was not found",
    "from_system_id": "61fd350d49dcba598b5475f8",
    "to_system_id": "619e0d20cab6bb05226e3e4b"
  }
}
```

## 7. Валидация входных данных

Для `from_system_id` и `to_system_id`:

- параметр обязателен;
- тип - строка;
- пустая строка запрещена;
- пробельная строка запрещена;
- рекомендуемая длина: от `1` до `128` символов;
- допустимые символы: латинские буквы, цифры, `_`, `-`;
- если текущие RSM ID всегда являются hex-строками Mongo ObjectId, можно ужесточить формат до regex `^[a-fA-F0-9]{24}$`.

Рекомендуемый базовый regex:

```regex
^[a-zA-Z0-9_-]{1,128}$
```

При ошибке валидации FastAPI возвращает стандартный ответ `422 Unprocessable Entity`.

## 8. SQL-запрос

В основе должен использоваться следующий запрос.

Вместо захардкоженных значений:

```text
61fd350d49dcba598b5475f8
619e0d20cab6bb05226e3e4b
```

должны использоваться значения, полученные из параметров API.

Базовая логика запроса:

```sql
WITH paths AS (
SELECT *
FROM cypher('rsm_eotar_interface', $$
MATCH p = (a:SYSTEM)-[r1:EOTAR_INTERFACE]->(:SYSTEM)-[:EOTAR_INTERFACE*0..]->(b:SYSTEM)
WHERE a.system_rsm_id = "61fd350d49dcba598b5475f8"
AND b.system_rsm_id = "619e0d20cab6bb05226e3e4b"
RETURN
a.system_rsm_id,
a.system_rsm_name,
b.system_rsm_id,
b.system_rsm_name,
length(p),
nodes(p),
r1.eotar_rsm_id
LIMIT 1000
$$) AS (
from_system_id text,
from_system_name text,
to_system_id text,
to_system_name text,
path_length int,
path_nodes agtype,
eotar_rsm_id text
)
)
SELECT
from_system_id,
from_system_name,
to_system_id,
to_system_name,
path_length,
path_nodes AS path,
COUNT(*) AS frequency,
MIN(eotar_rsm_id) AS example_eotar_rsm_id
FROM paths
GROUP BY
from_system_id,
from_system_name,
to_system_id,
to_system_name,
path_length,
path_nodes
ORDER BY frequency DESC, path_length ASC
LIMIT 1;
```

## 9. Требования к безопасной подстановке параметров

Нельзя напрямую конкатенировать пользовательские значения в SQL/Cypher без валидации.

Apache AGE `cypher(...)` принимает Cypher-запрос как строку, поэтому обычные bind-параметры PostgreSQL не всегда можно напрямую использовать внутри тела Cypher. Нужно выбрать один из безопасных подходов.

### Вариант 1. Строгая валидация ID и формирование Cypher-строки

Допустимо, если ID строго валидируются regex-ом до формирования запроса.

Требования:

- `from_system_id` и `to_system_id` проходят regex-валидацию;
- кавычки, пробелы, спецсимволы и управляющие символы запрещены;
- имя графа не принимается от пользователя, а берется только из конфигурации;
- `PATH_SEARCH_LIMIT` берется только из конфигурации и приводится к integer;
- SQL-шаблон хранится в коде сервиса;
- пользователь не может передать произвольный Cypher.

### Вариант 2. Использование параметров Apache AGE

Если установленная версия Apache AGE и драйвер позволяют использовать параметры в `cypher(...)`, нужно использовать этот вариант.

Идея:

```sql
SELECT *
FROM cypher(
  'rsm_eotar_interface',
  $$
  MATCH p = (a:SYSTEM)-[r1:EOTAR_INTERFACE]->(:SYSTEM)-[:EOTAR_INTERFACE*0..]->(b:SYSTEM)
  WHERE a.system_rsm_id = $from_system_id
  AND b.system_rsm_id = $to_system_id
  RETURN ...
  $$,
  '{"from_system_id": "...", "to_system_id": "..."}'::agtype
) AS (...)
```

Перед реализацией нужно проверить поддержку параметров на целевой версии Apache AGE.

## 10. Требования к работе с Apache AGE

При открытии соединения с PostgreSQL сервис должен обеспечить корректную инициализацию Apache AGE.

Обычно для сессии PostgreSQL требуется выполнить:

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;
```

Требования:

- эти команды должны выполняться для каждого нового соединения в пуле;
- если используется `asyncpg`, инициализация должна быть задана через callback инициализации соединения;
- если используется SQLAlchemy, инициализация должна быть выполнена через event listener или явный initialization hook;
- ошибки инициализации должны логироваться.

## 11. Ошибки и HTTP-коды

| Ситуация | HTTP status | Код ошибки |
|---|---:|---|
| Некорректные параметры запроса | `422` | стандартный ответ FastAPI |
| Путь не найден | `404` | `PATH_NOT_FOUND` |
| БД недоступна | `503` | `DATABASE_UNAVAILABLE` |
| Timeout запроса к БД | `504` | `DATABASE_TIMEOUT` |
| Внутренняя ошибка сервиса | `500` | `INTERNAL_SERVER_ERROR` |

Формат ошибок, кроме стандартной валидации FastAPI:

```json
{
  "detail": {
    "code": "DATABASE_TIMEOUT",
    "message": "Database query timeout"
  }
}
```

## 12. Логирование

Сервис должен логировать:

- старт приложения;
- остановку приложения;
- создание пула соединений;
- ошибки подключения к БД;
- ошибки выполнения SQL-запроса;
- timeout запросов;
- `from_system_id` и `to_system_id` для диагностических логов.

Требования:

- не логировать пароль БД;
- не логировать полный DSN с паролем;
- не логировать произвольный пользовательский ввод без валидации;
- для каждого входящего HTTP-запроса желательно иметь request id.

## 13. Производительность

Требования:

- API должно использовать пул соединений;
- запрос к БД должен иметь timeout;
- endpoint должен быть асинхронным;
- лимит путей внутри Cypher-запроса должен быть конфигурируемым, значение по умолчанию - `1000`;
- сервис должен корректно обрабатывать параллельные запросы.

Рекомендуемые стартовые настройки:

- pool min size: `1`;
- pool max size: `10`;
- statement timeout: `30 секунд`;
- application timeout: `35 секунд`.

## 14. OpenAPI / Swagger

FastAPI должен автоматически публиковать документацию:

- Swagger UI: `/docs`;
- OpenAPI JSON: `/openapi.json`.

В OpenAPI должны быть описаны:

- endpoint `/api/v1/paths`;
- query parameters;
- успешный ответ;
- ошибки `404`, `503`, `504`, `500`.

## 15. Рекомендуемая структура проекта

```text
app/
  __init__.py
  main.py
  api/
    __init__.py
    v1/
      __init__.py
      paths.py
      health.py
  core/
    __init__.py
    config.py
    logging.py
  db/
    __init__.py
    pool.py
    queries.py
  schemas/
    __init__.py
    paths.py
tests/
  test_paths_api.py
  test_health.py
.env.example
pyproject.toml
README.md
```

## 16. Модели данных

### 16.1 Request model

Для `GET` endpoint отдельное тело запроса не нужно. Используются query parameters.

Внутри приложения можно использовать модель:

```python
class PathSearchParams(BaseModel):
    from_system_id: str
    to_system_id: str
```

### 16.2 Response model

```python
class PathSearchResponse(BaseModel):
    from_system_id: str
    from_system_name: str | None = None
    to_system_id: str
    to_system_name: str | None = None
    path_length: int
    path: str
    frequency: int
    example_eotar_rsm_id: str | None = None
```

Если будет реализован парсинг `agtype`, тип поля `path` нужно изменить на:

```python
path: list[dict[str, Any]] | dict[str, Any] | str
```

## 17. Тестирование

### 17.1 Unit-тесты

Покрыть:

- валидацию `from_system_id`;
- валидацию `to_system_id`;
- сборку SQL/Cypher-запроса;
- обработку пустого результата БД;
- обработку ошибки БД;
- mapping строки БД в response model.

### 17.2 API-тесты

Покрыть:

- `GET /health` возвращает `200`;
- `GET /ready` возвращает `200`, если БД доступна;
- `GET /api/v1/paths` возвращает `200`, если путь найден;
- `GET /api/v1/paths` возвращает `404`, если путь не найден;
- `GET /api/v1/paths` возвращает `422`, если параметры отсутствуют;
- `GET /api/v1/paths` возвращает `422`, если ID содержит запрещенные символы.

### 17.3 Интеграционные тесты

При наличии тестовой PostgreSQL + Apache AGE БД проверить:

- выполнение `LOAD 'age'`;
- выполнение `SET search_path = ag_catalog, "$user", public`;
- выполнение реального `cypher(...)`;
- корректность ответа на известных тестовых данных.

## 18. Docker

Желательно предусмотреть Dockerfile для API.

Минимальные требования:

- запуск приложения через `uvicorn`;
- настройки передаются через переменные окружения;
- контейнер не хранит секреты внутри образа;
- healthcheck может использовать `/health`.

Пример команды запуска:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 19. Нефункциональные требования

Сервис должен быть:

- stateless;
- готовым к запуску в контейнере;
- устойчивым к ошибкам БД;
- безопасным относительно SQL/Cypher injection;
- документированным через OpenAPI;
- покрытым базовыми тестами.

## 20. Критерии приемки

Работа считается выполненной, если:

1. Сервис запускается локально через `uvicorn`.
2. Endpoint `/health` возвращает `200 OK`.
3. Endpoint `/ready` проверяет доступность PostgreSQL.
4. Endpoint `/api/v1/paths` принимает `from_system_id` и `to_system_id`.
5. Endpoint `/api/v1/paths` выполняет запрос к Apache AGE.
6. В запросе используется граф `rsm_eotar_interface`.
7. В запросе используется логика исходного SQL/Cypher-запроса.
8. При найденном пути API возвращает JSON с полями:
   - `from_system_id`;
   - `from_system_name`;
   - `to_system_id`;
   - `to_system_name`;
   - `path_length`;
   - `path`;
   - `frequency`;
   - `example_eotar_rsm_id`.
9. При отсутствии пути API возвращает `404`.
10. При некорректных входных данных API возвращает `422`.
11. При недоступной БД API возвращает `503`.
12. Секреты БД не захардкожены в коде.
13. Есть README с инструкцией запуска.
14. Есть `.env.example`.
15. Есть базовые тесты.

## 21. Открытые вопросы перед разработкой

Перед началом реализации нужно уточнить:

1. Точный формат `system_rsm_id`: всегда ли это Mongo ObjectId длиной 24 hex-символа?
2. Нужно ли возвращать `path` строкой или распарсить `agtype` в полноценный JSON?
3. Нужна ли авторизация API?
4. Нужен ли POST endpoint для поиска пути или достаточно GET?
5. Нужно ли возвращать несколько путей или только один лучший путь по текущей логике `LIMIT 1`?
6. Какая целевая версия PostgreSQL и Apache AGE используется?
7. Можно ли использовать параметры Apache AGE в установленной версии или нужно оставить вариант со строгой валидацией и сборкой Cypher-строки?
8. Какие требования к максимальному времени ответа API?
9. Нужны ли метрики Prometheus?
10. Где сервис будет запускаться: bare metal, Docker, Kubernetes или другой runtime?

