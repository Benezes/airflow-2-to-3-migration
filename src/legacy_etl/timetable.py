from datetime import timedelta

from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from pendulum import UTC, DateTime


class DualSessionTimetable(Timetable):
    description = "weekdays at 08:00 and 18:00 America/Sao_Paulo"

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        start = run_after.subtract(hours=12)
        return DataInterval(start=start, end=run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval,
        restriction: TimeRestriction,
    ):
        if restriction.max_end is not None and restriction.max_end <= restriction.earliest:
            return None
        if last_automated_data_interval is None:
            start = restriction.earliest or DateTime(2024, 1, 1, tzinfo=UTC)
        else:
            start = last_automated_data_interval.end
        end = start + timedelta(hours=12)
        return DagRunInfo.interval(start=start, end=end)
