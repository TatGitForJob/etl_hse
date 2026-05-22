import unittest

from app.producer import build_messages


class ProducerMessageTests(unittest.TestCase):
    def test_build_messages_returns_ordered_json_ready_payloads(self):
        messages = build_messages(count=3)

        self.assertEqual(
            messages,
            [
                {
                    "event_id": 1,
                    "student": "student_1",
                    "course": "etl-processes",
                    "score": 71,
                    "event_time": "2026-05-22T09:00:01Z",
                },
                {
                    "event_id": 2,
                    "student": "student_2",
                    "course": "etl-processes",
                    "score": 72,
                    "event_time": "2026-05-22T09:00:02Z",
                },
                {
                    "event_id": 3,
                    "student": "student_3",
                    "course": "etl-processes",
                    "score": 73,
                    "event_time": "2026-05-22T09:00:03Z",
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
