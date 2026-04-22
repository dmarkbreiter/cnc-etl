from prefect import flow

from flows.additional_stats_common import run_additional_stat_update
from settings import resolve_year, settings


@flow(name="update_quality_grades")
def update_quality_grades(
    *,
    year: int | None = None,
    api_call_delay: float = 2.0,
) -> dict:
    return run_additional_stat_update(
        stat_name="quality_grades",
        year=year,
        api_call_delay=api_call_delay,
    )


if __name__ == "__main__":
    update_quality_grades(year=resolve_year(settings.year))
