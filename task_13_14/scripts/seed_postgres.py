import os

import psycopg2


def getenv(name, default):
    return os.environ.get(name, default)


def main():
    conn = psycopg2.connect(
        host=getenv("POSTGRES_HOST", "localhost"),
        port=int(getenv("POSTGRES_PORT", "5432")),
        dbname=getenv("POSTGRES_DB", "source_db"),
        user=getenv("POSTGRES_USER", "etl_user"),
        password=getenv("POSTGRES_PASSWORD", "etl_password"),
    )
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    create table if not exists public.orders (
                        order_id integer primary key,
                        customer_name text not null,
                        status text not null,
                        amount numeric(12, 2) not null,
                        created_at timestamp not null
                    )
                    """
                )
                cursor.executemany(
                    """
                    insert into public.orders
                        (order_id, customer_name, status, amount, created_at)
                    values (%s, %s, %s, %s, %s)
                    on conflict (order_id) do update set
                        customer_name = excluded.customer_name,
                        status = excluded.status,
                        amount = excluded.amount,
                        created_at = excluded.created_at
                    """,
                    [
                        (1, "Ivan Petrov", "paid", 1200.00, "2026-04-01 10:00:00"),
                        (2, "Anna Sidorova", "new", 540.50, "2026-04-01 11:30:00"),
                        (3, "Petr Ivanov", "paid", 2499.99, "2026-04-02 09:15:00"),
                        (4, "Olga Smirnova", "cancelled", 300.00, "2026-04-02 13:45:00"),
                        (5, "Maria Volkova", "new", 820.75, "2026-04-03 16:20:00"),
                    ],
                )
    finally:
        conn.close()

    print("Seeded public.orders with 5 rows")


if __name__ == "__main__":
    main()
