from results.fetch import get_umbrella_project_stats
import pandas as pd
from clients.spaces import SpacesObject
from datetime import datetime, timezone

from prefect import flow, get_run_logger, task

from settings import resolve_year, settings


@task(retries=3, retry_delay_seconds=10)
def fetch_umbrella_stats(project_id: str) -> list[dict]:
    return get_umbrella_project_stats(project_id)


@task
def count_totals(stats: list[dict]) -> dict:
    totals = {"observation_count": 0, "species_count": 0, "observer_count": 0}

    for stat in stats:
        totals["observation_count"] += stat.get("observation_count", 0)
        totals["species_count"] += stat.get("species_count", 0)
        totals["observer_count"] += stat.get("observers_count", 0)

    return totals


@task
def process_umbrella_stats(stats: list[dict], year: int) -> dict:
    projects = pd.read_csv(
        f"data/{year}/inaturalist-projects_{year}.csv",
        keep_default_na=False,
    )

    # Remove duplicate ID entries, keeping the first occurrence
    projects = projects.drop_duplicates(subset="id", keep="first")

    projects = projects.set_index("id")
    projects = projects[["city", "project"]].to_dict(orient="index")
    # projects = {int(k): v for k, v in projects.items()}

    totals = count_totals(stats)

    results = []
    for stat in stats:
        project_id = stat.get("project").get("id")
        if project_id in projects:
            results.append(
                {
                    "id": project_id,
                    "city": projects.get(project_id).get("city"),
                    "name": projects.get(project_id).get("project"),
                    "observation_count": stat.get("observation_count", 0),
                    "species_count": stat.get("species_count", 0),
                    "observers_count": stat.get("observers_count", 0),
                }
            )

    return {
        "timestamp": int(datetime.now(timezone.utc).timestamp()),
        "totals": totals,
        "results": results,
    }


@task(retries=3, retry_delay_seconds=10)
def upload_umbrella_stats(stats: dict) -> None:
    logger = get_run_logger()
    spaces = SpacesObject(key="umbrella-stats")
    response = spaces.upload(stats)

    response_success = response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
    if response_success:
        logger.info("Upload successful.")
        return

    raise RuntimeError(f"Upload failed (response={response!r})")


@flow(name="update_umbrella_stats")
def update_umbrella_stats(
    project_id: str | None = None, *, year: int | None = None
) -> dict:
    logger = get_run_logger()
    resolved_year = resolve_year(year)
    resolved_project_id = project_id or f"city-nature-challenge-{resolved_year}"

    results = fetch_umbrella_stats(resolved_project_id)
    processed_results = process_umbrella_stats(results, year=resolved_year)
    logger.info("Fetched umbrella stats for project %s.", resolved_project_id)
    upload_umbrella_stats(processed_results)
    return processed_results


if __name__ == "__main__":
    update_umbrella_stats(year=resolve_year(settings.year))
