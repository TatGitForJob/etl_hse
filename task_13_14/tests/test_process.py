from scripts.process_orders import aggregate_orders


def test_aggregate_orders_groups_by_status():
    rows = [
        {"order_id": "1", "status": "paid", "amount": "100.50"},
        {"order_id": "2", "status": "paid", "amount": "20"},
        {"order_id": "3", "status": "new", "amount": "10"},
    ]

    result = aggregate_orders(rows)

    assert result == [
        {"status": "new", "orders_count": 1, "total_amount": 10.0},
        {"status": "paid", "orders_count": 2, "total_amount": 120.5},
    ]
