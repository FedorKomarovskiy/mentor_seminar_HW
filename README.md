# 🚀 Аналитическая платформа на Trino: Итоговый проект

## 📋 О проекте

Данный проект демонстрирует создание аналитической платформы на основе **Trino** для работы с разнородными источниками данных. Решение позволяет выполнять федеративные запросы, агрегировать данные и сохранять результаты в современных форматах хранения.

## 🏗️ Архитектура

Платформа включает следующие компоненты:

| Компонент | Назначение |
|-----------|------------|
| **Trino Coordinator** | Координация выполнения распределенных запросов |
| **PostgreSQL** | Реляционная БД для хранения заказов и клиентов |
| **MySQL** | Реляционная БД для хранения платежей |
| **MinIO** | Объектное хранилище S3-совместимое |
| **Hive Metastore** | Каталог метаданных для таблиц |
| **Apache Iceberg** | Формат таблиц для аналитических нагрузок |

## 🚀 Быстрый старт

### Предварительные требования

- **Docker** и **Docker Compose**
- **Python 3.8+** с пакетным менеджером pip
- **4+ ГБ ОЗУ** для работы контейнеров
- Свободные порты: `3306`, `5432`, `8080`, `9000`, `9001`, `9083`

### Установка и запуск

1. **Клонируйте репозиторий:**
```bash
git clone <your-repo-url>
cd <project-directory>
```

2. **Настройте переменные окружения:**
```bash
cat > .env << 'EOF'
# Database configurations
POSTGRES_DB=demo_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

MYSQL_ROOT_PASSWORD=mysql
MYSQL_DATABASE=demo_db
MYSQL_USER=mysql
MYSQL_PASSWORD=mysql

# Trino configuration
TRINO_HOST=trino
TRINO_PORT=8080
TRINO_USER=trino

# MinIO credentials
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Hive Metastore settings
METASTORE_DB=metastore
METASTORE_USER=hive
METASTORE_PASSWORD=hive
EOF
```

3. **Запустите инфраструктуру:**
```bash
# Запуск всех сервисов
docker-compose up -d

# Проверка статуса
docker-compose ps

# Просмотр логов
docker-compose logs -f
```

4. **Установите Python-зависимости:**
```bash
pip install -r requirements.txt
```

### 🌐 Доступ к веб-интерфейсам

| Сервис | URL | Учетные данные |
|--------|-----|----------------|
| **Trino UI** | http://localhost:8080 | - |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |

## 📊 Структура данных

### Источники данных в Trino

```sql
-- Просмотр доступных каталогов
SHOW CATALOGS;

-- Таблицы в PostgreSQL
SELECT * FROM postgresql.demo_schema.trn_customers;
SELECT * FROM postgresql.demo_schema.trn_orders;

-- Таблицы в MySQL
SELECT * FROM mysql.demo_schema.trn_payments;

-- Таблицы в Iceberg
SELECT * FROM iceberg.analytics.daily_reports;
```

### Создание таблиц (уже настроено в контейнерах)

**PostgreSQL:**
```sql
CREATE TABLE trn_customers (
    customer_id BIGINT PRIMARY KEY,
    customer_name VARCHAR(255),
    email VARCHAR(255),
    created_at TIMESTAMP
);

CREATE TABLE trn_orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    order_ts TIMESTAMP,
    total_amount DECIMAL(10,2)
);
```

**MySQL:**
```sql
CREATE TABLE trn_payments (
    payment_id BIGINT PRIMARY KEY,
    order_id BIGINT,
    amount DECIMAL(10,2),
    paid_at TIMESTAMP(3),
    payment_method VARCHAR(50)
);
```

## 🐍 Python модули

Проект содержит следующие основные модули:

### `trino_connection.py`
```python
from trino_connection import create_trino_connection, execute_sql_query

# Подключение к Trino
conn = create_trino_connection()

# Выполнение запроса
df = execute_sql_query("SELECT * FROM postgresql.demo_schema.trn_orders")
```

### `data_aggregation.py`
```python
from data_aggregation import (
    aggregate_daily_orders,
    aggregate_daily_payments,
    create_final_analytics_dataframe
)

# Агрегация данных
daily_orders = aggregate_daily_orders()
daily_payments = aggregate_daily_payments()
analytics_df = create_final_analytics_dataframe()
```

### `visualization.py`
```python
from visualization import (
    create_time_series_revenue_chart,
    create_combined_analytics_dashboard
)

# Создание графиков
create_time_series_revenue_chart(analytics_df)
create_combined_analytics_dashboard(analytics_df)
```

### `iceberg_storage.py`
```python
from iceberg_storage import save_analytics_to_iceberg

# Сохранение в Iceberg
save_analytics_to_iceberg(analytics_df, 'analytics.daily_reports')
```

## 📓 Jupyter Notebook

Основной анализ выполняется в ноутбуке `trino_analytics_homework.ipynb`, который включает три этапа:

### Этап 1: Инициализация
- Подключение к Trino
- Тестирование всех источников данных
- Исследование доступных схем и таблиц

### Этап 2: Аналитика
- Агрегация данных из PostgreSQL и MySQL
- Объединение данных из разных источников
- Расчет бизнес-метрик

### Этап 3: Визуализация и сохранение
- Создание графиков и дашбордов
- Экспорт результатов в Apache Iceberg
- Проверка целостности данных

## 🔧 Конфигурация

### Файлы конфигурации Trino

**Основные настройки** (`trino-config/config.properties`):
```properties
coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
query.max-memory=2GB
```

**Подключение к PostgreSQL** (`trino-config/catalog/postgresql.properties`):
```properties
connector.name=postgresql
connection-url=jdbc:postgresql://postgres:5432/demo_db
```

**Подключение к Iceberg** (`trino-config/catalog/iceberg.properties`):
```properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://hive-metastore:9083
```

## 📁 Структура проекта

```
project/
├── docker-compose.yml          # Конфигурация контейнеров
├── .env                       # Переменные окружения
├── requirements.txt           # Python зависимости
├── trino_connection.py       # Модуль работы с Trino
├── data_aggregation.py       # Модуль агрегации данных
├── visualization.py          # Модуль визуализации
├── iceberg_storage.py        # Модуль работы с Iceberg
├── trino_analytics_homework.ipynb  # Основной ноутбук
├── trino-config/             # Конфигурация Trino
│   ├── config.properties
│   ├── node.properties
│   ├── jvm.config
│   └── catalog/
│       ├── postgresql.properties
│       ├── mysql.properties
│       └── iceberg.properties
└── data/                     (опционально) Тестовые данные
```

## 🧪 Тестирование

Проверьте работоспособность системы:

```bash
# Тест подключения к Trino
python -c "from trino_connection import test_catalog_connectivity; test_catalog_connectivity()"

# Тест доступа к данным
python -c "from trino_connection import test_data_access; test_data_access()"
```

## 🗑️ Остановка и очистка

```bash
# Остановка контейнеров
docker-compose down

# Остановка с удалением томов
docker-compose down -v

# Очистка Docker
docker system prune -a --volumes
```

## 📚 Полезные команды

```bash
# Просмотр логов Trino
docker-compose logs trino

# Проверка здоровья сервисов
docker-compose ps

# Доступ к оболочке Trino
docker exec -it trino trino

# Перезапуск конкретного сервиса
docker-compose restart trino
```

---

⭐ Если проект был полезен, поставьте звезду на GitHub!
