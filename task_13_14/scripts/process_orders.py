import csv
from collections import defaultdict
from io import StringIO


def aggregate_orders(rows):
    totals = defaultdict(lambda: {"orders_count": 0, "total_amount": 0.0})
    for row in rows:
        status = row["status"]
        totals[status]["orders_count"] += 1
        totals[status]["total_amount"] += float(row["amount"])

    return [
        {
            "status": status,
            "orders_count": values["orders_count"],
            "total_amount": round(values["total_amount"], 2),
        }
        for status, values in sorted(totals.items())
    ]


def csv_to_rows(csv_text):
    return list(csv.DictReader(StringIO(csv_text)))


def rows_to_csv(rows):
    if not rows:
        return ""

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()
