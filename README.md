# MCP Path Service

REST API сервис для поиска путей между системами в PostgreSQL + Apache AGE.

## Описание

Сервис предоставляет API для поиска наиболее частого кратчайшего пути между двумя системами в графе, хранящемся в PostgreSQL с расширением Apache AGE.

**Ключевые особенности:**
- Асинхронная работа с БД через `asyncpg`
- Пул соединений с автоматической инициализацией Apache AGE
- Валидация входных параметров
- Request ID для трассировки запросов
- OpenAPI документация из коробки

## Требования

- Python 3.9+
- PostgreSQL с установленным Apache AGE
- Граф `rsm_eotar_interface` (или другой, указанный в конфигурации)

## Быстрый старт

### 1. Установка

```bash
# Клонирование проекта
cd /path/to/mcp-path-service

# Создание виртуального окружения
python3 -m venv venv
source venv/bin/activate

# Установка зависимостей
pip install -e ".[dev]"
```

### 2. Конфигурация

Создай файл `.env` на основе примера:

```bash
cp .env.example .env
```

Отредактируй `.env` под твоё окружение:

```env
# Приложение
APP_HOST=0.0.0.0
APP_PORT=8000

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=graphrag
POSTGRES_USER=graphrag
POSTGRES_PASSWORD=your_password
POSTGRES_POOL_MIN_SIZE=1
POSTGRES_POOL_MAX_SIZE=10

# Apache AGE
AGE_GRAPH_NAME=rsm_eotar_interface
PATH_SEARCH_LIMIT=1000

# Логирование и таймауты
LOG_LEVEL=INFO
DB_STATEMENT_TIMEOUT_MS=30000
```

### 3. Запуск

```bash
uvicorn app.main:app --reload
```

Сервис будет доступен по адресу: http://localhost:8000

### 4. Проверка

```bash
# Health check
curl http://localhost:8000/health

# Readiness check (проверка БД)
curl http://localhost:8000/ready

# OpenAPI документация
open http://localhost:8000/docs
```

## API Endpoints

### GET /health

Базовая проверка работоспособности сервиса.

**Response 200:**
```json
{
  "status": "ok"
}
```

---

### GET /ready

Проверка готовности сервиса (включая подключение к БД).

**Response 200:**
```json
{
  "status": "ready",
  "database": "ok"
}
```

**Response 503** (БД недоступна):
```json
{
  "detail": {
    "status": "not_ready",
    "database": "error"
  }
}
```

---

### GET /api/v1/paths

Поиск пути между двумя системами.

**Query Parameters:**

| Параметр | Тип | Обязательный | Описание |
|----------|-----|--------------|----------|
| `from_system_id` | string | да | RSM ID исходной системы |
| `to_system_id` | string | да | RSM ID целевой системы |

**Формат ID:** regex `^[a-zA-Z0-9_-]{1,128}$` (латинские буквы, цифры, подчёркивание, дефис; длина 1-128 символов)

**Пример запроса:**
```bash
curl "http://localhost:8000/api/v1/paths?from_system_id=61fd350d49dcba598b5475f8&to_system_id=619e0d20cab6bb05226e3e4b"
```

**Response 200** (путь найден):
```json
{
  "from_system_id": "61fd350d49dcba598b5475f8",
  "from_system_name": "Исходная система",
  "to_system_id": "619e0d20cab6bb05226e3e4b",
  "to_system_name": "Целевая система",
  "path_length": 3,
  "path": "[{\"id\": 844424930131969, \"label\": \"SYSTEM\", \"properties\": {\"system_rsm_id\": \"...\"}}]",
  "frequency": 12,
  "example_eotar_rsm_id": "eotar_rsm_id_example"
}
```

**Поля ответа:**

| Поле | Тип | Описание |
|------|-----|----------|
| `from_system_id` | string | ID исходной системы |
| `from_system_name` | string \| null | Имя исходной системы |
| `to_system_id` | string | ID целевой системы |
| `to_system_name` | string \| null | Имя целевой системы |
| `path_length` | integer | Длина пути (количество узлов) |
| `path` | string | Путь в формате agtype (строка) |
| `frequency` | integer | Частота данного пути |
| `example_eotar_rsm_id` | string \| null | Пример EOTAR ID из первого ребра |

**Response 404** (путь не найден):
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

**Response 422** (невалидные параметры):
```json
{
  "detail": [
    {
      "loc": ["query", "from_system_id"],
      "msg": "String should match pattern '^[a-zA-Z0-9_-]{1,128}$'",
      "type": "string_pattern_mismatch"
    }
  ]
}
```

**Response 503** (БД недоступна):
```json
{
  "detail": {
    "code": "DATABASE_UNAVAILABLE",
    "message": "Database connection error"
  }
}
```

**Response 504** (timeout БД):
```json
{
  "detail": {
    "code": "DATABASE_TIMEOUT",
    "message": "Database query timeout"
  }
}
```

**Response 500** (внутренняя ошибка):
```json
{
  "detail": {
    "code": "INTERNAL_SERVER_ERROR",
    "message": "Internal server error"
  }
}
```

## Конфигурация

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `APP_HOST` | Host для запуска приложения | `0.0.0.0` |
| `APP_PORT` | Port для запуска приложения | `8000` |
| `POSTGRES_HOST` | PostgreSQL host | `localhost` |
| `POSTGRES_PORT` | PostgreSQL port | `5432` |
| `POSTGRES_DB` | Имя базы данных | `rsm` |
| `POSTGRES_USER` | Пользователь БД | `rsm_user` |
| `POSTGRES_PASSWORD` | Пароль БД | - |
| `POSTGRES_POOL_MIN_SIZE` | Минимальный размер пула соединений | `1` |
| `POSTGRES_POOL_MAX_SIZE` | Максимальный размер пула соединений | `10` |
| `AGE_GRAPH_NAME` | Имя графа Apache AGE | `rsm_eotar_interface` |
| `PATH_SEARCH_LIMIT` | Лимит путей внутри Cypher-запроса | `1000` |
| `LOG_LEVEL` | Уровень логирования (DEBUG, INFO, WARNING, ERROR) | `INFO` |
| `DB_STATEMENT_TIMEOUT_MS` | Timeout SQL-запроса в миллисекундах | `30000` |

## Структура проекта

```
app/
├── __init__.py
├── main.py              # FastAPI приложение, lifespan, middleware
├── api/
│   └── v1/
│       ├── __init__.py
│       ├── paths.py     # Endpoint поиска пути
│       └── health.py    # Health/Ready endpoints
├── core/
│   ├── __init__.py
│   ├── config.py        # Pydantic Settings
│   └── logging.py       # Логирование, Request ID middleware
├── db/
│   ├── __init__.py
│   ├── pool.py          # asyncpg pool, AGE инициализация
│   └── queries.py       # SQL/Cypher запросы
└── schemas/
    ├── __init__.py
    └── paths.py         # Pydantic модели

tests/
├── __init__.py
├── test_health.py       # Тесты health endpoints
└── test_paths_api.py    # Тесты paths endpoint
```

## Docker

### Сборка образа

```bash
docker build -t mcp-path-service .
```

### Запуск контейнера

```bash
docker run -d \
  --name mcp-path-service \
  -p 8000:8000 \
  -e POSTGRES_HOST=your-db-host \
  -e POSTGRES_PORT=5432 \
  -e POSTGRES_DB=rsm \
  -e POSTGRES_USER=rsm_user \
  -e POSTGRES_PASSWORD=your_password \
  mcp-path-service
```

### Docker Compose

Для продакшн-деплоя используйте Docker Compose:

```bash
# Создай файл .env с переменными окружения
cp .env.example .env

# Отредактируй .env под твоё окружение
# Обязательно укажи:
# - POSTGRES_HOST (адрес внешней БД)
# - POSTGRES_PASSWORD (пароль БД)

# Запуск
docker compose up -d --build
```

**Проверка статуса:**

```bash
# Проверить, что контейнер работает
docker compose ps

# Проверить логи
docker compose logs -f

# Health check
curl http://localhost:8000/health
curl http://localhost:8000/ready
```

**Остановка:**

```bash
docker compose down
```

**Переменные окружения для продакшена:**

| Переменная | Обязательно | Описание |
|------------|-------------|----------|
| `POSTGRES_HOST` | да | Адрес PostgreSQL сервера |
| `POSTGRES_PORT` | нет | Порт PostgreSQL (по умолчанию 5432) |
| `POSTGRES_DB` | нет | Имя базы данных (по умолчанию rsm) |
| `POSTGRES_USER` | нет | Пользователь БД (по умолчанию rsm_user) |
| `POSTGRES_PASSWORD` | да | Пароль БД |
| `LOG_LEVEL` | нет | Уровень логирования (по умолчанию INFO) |

**Особенности docker-compose.yml:**
- Multi-stage build для минимального размера образа
- Непривилегированный пользователь внутри контейнера
- Healthcheck с curl
- Restart policy: `unless-stopped`
- Ограничения ресурсов: 1 CPU, 512MB RAM
- Volume для логов: `./logs:/app/logs`

## Разработка

### Запуск тестов

```bash
pytest -v
```

### Форматирование кода (опционально)

```bash
pip install ruff
ruff format app tests
```

### Проверка типов (опционально)

```bash
pip install mypy
mypy app
```

## Подключение через SSH-туннель

Если PostgreSQL доступен только через SSH:

```bash
# Терминал 1: SSH-туннель
ssh -L 5434:localhost:5433 user@remote-host

# Терминал 2: Запуск сервиса
uvicorn app.main:app --reload
```

В `.env` укажи:
```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5434
```

## Логирование

Сервис логирует:
- Старт и остановку приложения
- Создание/закрытие пула соединений
- Ошибки подключения к БД
- Ошибки выполнения SQL-запросов
- Request ID для каждого запроса

Формат логов:
```
2024-05-04 10:00:00 | INFO     | app.main | request_id=abc123 | Starting MCP Path Service...
```

## Безопасность

- Валидация ID через regex перед подстановкой в Cypher (защита от injection)
- Пароль БД не логируется
- Имя графа берётся из конфигурации, не от пользователя
- PATH_SEARCH_LIMIT валидируется как integer

## Лицензия

MIT
