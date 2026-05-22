# Практическая работа 15: Kafka + PySpark

Задание выполнено без Yandex Cloud: Apache Kafka запускается локально в Docker, сообщения пишутся в topic, затем PySpark читает этот topic и печатает результат в консоль.

## Что нужно установить

- Docker
- Docker Compose

Проверка:

```bash
docker-compose version
```

## 1. Запустить Kafka

```bash
docker-compose up -d kafka
```

Проверить, что Kafka работает:

```bash
docker-compose ps
```

В колонке `STATUS` у сервиса `kafka` должен быть статус `healthy`.

## 2. Записать сообщения в Kafka topic

```bash
docker-compose run --rm producer
```

Что смотреть в выводе:

```text
Finished sending 10 messages to topic 'etl_practice_events'.
```

Сообщения берутся из файла `data/messages.jsonl`.

## 3. Прочитать сообщения PySpark-заданием

```bash
docker-compose run --rm spark-reader
```

## Что скринить для LMS

Сделайте скриншот терминала после команды:

```bash
docker-compose run --rm spark-reader
```

## Остановить и удалить контейнеры

```bash
docker-compose down -v
```

## Локальная проверка Python-кода

```bash
python3 -m unittest tests/test_producer.py
```
