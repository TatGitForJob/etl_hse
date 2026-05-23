# Практическая работа 13-14

## Запуск

```bash
docker-compose up -d postgres-source minio minio-init airflow-webserver airflow-scheduler
```

Интерфейсы:

- Airflow: <http://localhost:8080>, логин `admin`, пароль `admin`
- MinIO Console: <http://localhost:9001>, логин `minioadmin`, пароль `minioadmin`

## Подготовка источника PostgreSQL

```bash
docker-compose exec postgres-source psql -U etl_user -d source_db -f /sql/seed.sql
```

Проверка таблицы:

```bash
docker-compose exec postgres-source psql -U etl_user -d source_db -c "select * from public.orders order by order_id;"
```

## Перенос PostgreSQL в Object Storage

```bash
docker-compose exec postgres-source sh -c "mkdir -p /transfer/raw && psql -U etl_user -d source_db -c \"\\copy (select order_id, customer_name, status, amount, created_at from public.orders order by order_id) to '/transfer/raw/orders.csv' with csv header\""
docker-compose run --rm mc -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc cp /transfer/raw/orders.csv local/etl-bucket/raw/orders.csv"
```

Результат появится в MinIO:

```text
s3://etl-bucket/raw/orders.csv
```

Проверка через MinIO:

```bash
docker run --rm --network task_13_14_default minio/mc:RELEASE.2024-06-12T14-34-03Z \
  sh -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc cat local/etl-bucket/raw/orders.csv"
```

## Повторная активация копирования

Добавьте или измените данные в PostgreSQL:

```bash
docker-compose exec postgres-source psql -U etl_user -d source_db -c \
  "insert into public.orders values (6, 'Repeat Load', 'paid', 777.77, now()) on conflict (order_id) do update set amount = excluded.amount;"
```

Повторно запустите перенос:

```bash
docker-compose exec postgres-source sh -c "psql -U etl_user -d source_db -c \"\\copy (select order_id, customer_name, status, amount, created_at from public.orders order by order_id) to '/transfer/raw/orders.csv' with csv header\""
docker-compose run --rm mc -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc cp /transfer/raw/orders.csv local/etl-bucket/raw/orders.csv"
```

Файл `raw/orders.csv` будет перезаписан актуальным снимком таблицы.

## Обработка данных из Object Storage в Airflow

В Airflow откройте DAG `process_s3_orders` и нажмите Trigger DAG.

То же самое из консоли:

```bash
docker-compose exec airflow-scheduler airflow dags trigger process_s3_orders
```

DAG читает:

```text
s3://etl-bucket/raw/orders.csv
```

И пишет витрину:

```text
s3://etl-bucket/mart/orders_by_status.csv
```

Проверка результата:

```bash
docker run --rm --network task_13_14_default minio/mc:RELEASE.2024-06-12T14-34-03Z \
  sh -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc cat local/etl-bucket/mart/orders_by_status.csv"
```

## Что приложить в LMS

Скриншоты:

1. Контейнеры запущены: `docker-compose ps`.
2. Таблица `public.orders` в PostgreSQL.
3. Файл `raw/orders.csv` в MinIO.
4. Повторный перенос после добавления строки `order_id = 6`.
5. Успешный запуск DAG `process_s3_orders` в Airflow.
6. Файл `mart/orders_by_status.csv` в MinIO.

## Остановка

```bash
docker-compose down
```

Полная очистка данных:

```bash
docker-compose down -v
```
