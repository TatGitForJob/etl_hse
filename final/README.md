# Итоговое ДЗ по модулю 3

Проект реализует ETL-стенд по первому примеру из задания: `UserSessions`.

- `MongoDB` используется как нереляционный источник с одной коллекцией `user_sessions`.
- `PostgreSQL` хранит очищенные и реплицированные сессии.
- `Apache Airflow` оркестрирует загрузку и построение витрин.
- Весь стенд запускается через `docker-compose`.

## Архитектура

Поток данных:

`mongo-seed -> MongoDB.user_sessions -> Airflow DAG mongo_to_postgres_replication -> PostgreSQL.raw.user_sessions -> Airflow DAG build_analytics_marts -> PostgreSQL.marts`

Сервисы:

- `mongo` - исходная нереляционная база.
- `mongo-seed` - одноразовый контейнер, который генерирует и загружает данные в MongoDB.
- `postgres` - реляционная база для Airflow metadata и аналитического слоя.
- `airflow-init` - инициализация метабазы Airflow и создание администратора.
- `airflow-webserver` - UI Airflow.
- `airflow-scheduler` - выполнение DAG'ов.

## Структура проекта

```text
.
├── airflow
│   ├── dags
│   │   ├── _etl_common.py
│   │   ├── build_analytics_marts_dag.py
│   │   └── mongo_to_postgres_dag.py
│   ├── Dockerfile
│   └── requirements.txt
├── generator
│   ├── Dockerfile
│   ├── requirements.txt
│   └── seed_mongo.py
├── postgres
│   └── init
│       └── 00-bootstrap.sql
├── screenshots
│   └── images...
├── docker-compose.yml
└── README.md
└── REPORT.md
```

## Быстрый запуск

Из корня проекта:

```bash
docker-compose up --build -d
```

После запуска:

- MongoDB поднимется на `localhost:27017`
- PostgreSQL поднимется на `localhost:5432`
- Airflow будет доступен на `http://localhost:8080`

Учетные данные Airflow:

- логин: `airflow`
- пароль: `airflow`

## Порядок старта

1. `postgres` создает БД `airflow` и `warehouse`, а также схемы и таблицы.
2. `mongo` поднимает MongoDB.
3. `mongo-seed` заполняет MongoDB исходными данными.
4. `airflow-init` инициализирует Airflow metadata database и пользователя.
5. `airflow-scheduler` подхватывает DAG `mongo_to_postgres_replication`.
6. После репликации автоматически запускается DAG `build_analytics_marts`.
