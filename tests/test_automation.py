from __future__ import annotations

import unittest
from datetime import UTC, datetime

from blog_agent.automation import (
    AutomationSettings,
    build_post_run_updates,
    evaluate_automation_schedule,
)


class AutomationScheduleTests(unittest.TestCase):
    def test_run_now_takes_priority(self) -> None:
        settings = AutomationSettings(
            enabled=False,
            daily_time="09:00",
            timezone="Asia/Kolkata",
            run_now=True,
        )
        decision = evaluate_automation_schedule(
            settings,
            now_utc=datetime(2026, 4, 9, 4, 0, tzinfo=UTC),
        )
        self.assertTrue(decision.should_run)
        self.assertEqual(decision.reason, "run_now")
        self.assertTrue(decision.next_run_at)

    def test_before_daily_window_does_not_run(self) -> None:
        settings = AutomationSettings(
            enabled=True,
            daily_time="09:00",
            timezone="Asia/Kolkata",
            run_now=False,
            last_run_at="",
        )
        decision = evaluate_automation_schedule(
            settings,
            now_utc=datetime(2026, 4, 9, 1, 0, tzinfo=UTC),  # 06:30 IST
        )
        self.assertFalse(decision.should_run)
        self.assertEqual(decision.reason, "before_daily_window")

    def test_after_daily_window_runs_once(self) -> None:
        settings = AutomationSettings(
            enabled=True,
            daily_time="09:00",
            timezone="Asia/Kolkata",
            run_now=False,
            last_run_at="",
        )
        decision = evaluate_automation_schedule(
            settings,
            now_utc=datetime(2026, 4, 9, 5, 0, tzinfo=UTC),  # 10:30 IST
        )
        self.assertTrue(decision.should_run)
        self.assertEqual(decision.reason, "daily_window_due")

        updates = build_post_run_updates(
            settings,
            now_utc=datetime(2026, 4, 9, 5, 0, tzinfo=UTC),
        )
        self.assertFalse(updates["runNow"])
        self.assertTrue(str(updates["lastRunAt"]))
        self.assertTrue(str(updates["nextRunAt"]))


if __name__ == "__main__":
    unittest.main()
