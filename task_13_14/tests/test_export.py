from io import StringIO

from scripts.export_postgres_to_s3 import rows_to_csv


def test_rows_to_csv_writes_header_and_rows():
    rows = [
        {"order_id": 1, "customer": "Ann", "amount": 10.5},
        {"order_id": 2, "customer": "Bob", "amount": 20.0},
    ]

    result = rows_to_csv(rows)

    assert StringIO(result).read().splitlines() == [
        "order_id,customer,amount",
        "1,Ann,10.5",
        "2,Bob,20.0",
    ]


def test_rows_to_csv_returns_empty_string_for_no_rows():
    assert rows_to_csv([]) == ""
