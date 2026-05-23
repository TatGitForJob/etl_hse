# Отчет по практической работе 13-14

## Задача 1. Перенос PostgreSQL в Object Storage

```text
scripts/export_postgres_to_s3.py
```

Скрипт читает таблицу `public.orders` из PostgreSQL и выгружает снимок данных в:

```text
s3://etl-bucket/raw/orders.csv
```

Повторная активация копирования выполняется повторным запуском скрипта. Файл в Object Storage перезаписывается актуальным состоянием таблицы.

## Задача 2. Обработка Object Storage через Airflow

Yandex Managed Service for Apache Airflow заменен локальными контейнерами `airflow-webserver` и `airflow-scheduler`.

Yandex Data Processing заменен Python-обработкой внутри Airflow DAG:

```text
dags/process_s3_orders.py
```

DAG `process_s3_orders` читает файл `raw/orders.csv`, группирует заказы по статусу и записывает витрину:

```text
s3://etl-bucket/mart/orders_by_status.csv
```

## Результаты для проверки

Команды запуска и проверки приведены в `README.md`.

Ожидаемый результат витрины:

```csv
status,orders_count,total_amount
cancelled,1,300.0
new,2,1361.25
paid,2,3699.99
```
