from prefect import flow

from flows.additional_stats_common import run_additional_stat_update
from settings import resolve_year, settings


@flow(name="update_most_observed_species")
def update_most_observed_species(
    *,
    year: int | None = None,
    api_call_delay: float = 3.0,
) -> dict:
    return run_additional_stat_update(
        stat_name="most_observed_species",
        year=year,
        api_call_delay=api_call_delay,
    )


if __name__ == "__main__":
    update_most_observed_species(year=resolve_year(settings.year))
