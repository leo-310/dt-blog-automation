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

    def test_after_daily_window_runs(self) -> None:
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

    def test_same_day_second_run_allowed_when_time_moves_forward(self) -> None:
        settings = AutomationSettings(
            enabled=True,
            daily_time="10:31",
            timezone="Asia/Kolkata",
            run_now=False,
            # 09:03 IST same day; should not block a 10:31 IST schedule.
            last_run_at="2026-04-14T03:33:09+00:00",
        )
        decision = evaluate_automation_schedule(
            settings,
            now_utc=datetime(2026, 4, 14, 5, 2, tzinfo=UTC),  # 10:32 IST
        )
        self.assertTrue(decision.should_run)
        self.assertEqual(decision.reason, "daily_window_due")

    def test_same_day_second_run_blocked_if_window_already_executed(self) -> None:
        settings = AutomationSettings(
            enabled=True,
            daily_time="10:31",
            timezone="Asia/Kolkata",
            run_now=False,
            # 10:40 IST same day; already executed for this configured window.
            last_run_at="2026-04-14T05:10:00+00:00",
        )
        decision = evaluate_automation_schedule(
            settings,
            now_utc=datetime(2026, 4, 14, 5, 12, tzinfo=UTC),  # 10:42 IST
        )
        self.assertFalse(decision.should_run)
        self.assertEqual(decision.reason, "already_ran_for_current_window")


if __name__ == "__main__":
    unittest.main()
