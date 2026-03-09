# Отчет по итоговому ДЗ модуля 3

## Что сделано

### 1. Развернуты MongoDB и PostgreSQL

Стенд поднят в Docker через `docker-compose`.

Скрины:

- [MongoDB и PostgreSQL запущен](./screenshots/01_mongo.png)

### 2. Сгенерированы данные в MongoDB

В MongoDB создана коллекция `user_sessions` по примеру `UserSessions` из задания.

Скрин:

- [Данные в MongoDB](./screenshots/02_mongo.png)

### 3. Настроена репликация в PostgreSQL через Airflow

Создан DAG `mongo_to_postgres_replication`, который переносит данные из MongoDB в PostgreSQL.

Скрин:

- [Успешная репликация в Airflow](./screenshots/03_airflow.png)

### 4. Добавлена трансформация данных

При загрузке рассчитывается `session_duration_minutes`, данные очищаются и дедуплицируются.

Скрин:

- [Полученная табица](./screenshots/04_transformation.png)

### 5. Данные в PostgreSQL очищены и пригодны для аналитики

В `raw.user_sessions` нет дублей. Таблица секционирована.

Скрины:

- [Проверка на дубли](./screenshots/05_no_duplicates.png)
- [Секционированная таблица](./screenshots/06_partitioned.png)

### 6. Построены аналитические витрины в Airflow

Создан DAG `build_analytics_marts`, который формирует 2 витрины:

- `marts.user_activity_daily`
- `marts.page_action_popularity_daily`

Скрины:

- [Успешный DAG витрин](./screenshots/07_airflows.png)
- [Витрина user_activity_daily](./screenshots/08_mart_user_activity.png)
- [Витрина page_action_popularity_daily](./screenshots/09_mart_page_action.png)