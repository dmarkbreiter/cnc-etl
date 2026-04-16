from results.fetch import (
    get_project_identifiers_count,
    get_project_quality_grades,
    get_project_most_observed_species,
)

import pandas as pd
from clients.spaces import SpacesObject
from datetime import datetime, timezone
import time
import tqdm

from prefect import flow, get_run_logger, task

from settings import resolve_year, settings


@task(retries=3, retry_delay_seconds=10)
def fetch_additional_stats(project_id: int, api_call_delay: float = 5) -> dict:
    logger = get_run_logger()

    most_observed_species = get_project_most_observed_species(project_id)

    identifiers_count = get_project_identifiers_count(project_id)
    quality_grades = get_project_quality_grades(project_id)

    # Sleep briefly between API calls to avoid hitting rate limits.
    time.sleep(api_call_delay)

    logger.info("Fetched additional stats for project_id=%s.", project_id)
    return {
        "id": project_id,
        "most_observed_species": most_observed_species,
        "identifiers_count": identifiers_count,
        "quality_grades": quality_grades,
    }


@task
def get_project_ids(year: int) -> list[int]:

    projects = pd.read_csv(
        f"data/{year}/inaturalist-projects_{year}.csv"
    )
    return [*{*projects["id"].tolist()}]


@task(retries=3, retry_delay_seconds=10)
def upload_additional_stats(stats: dict) -> None:
    logger = get_run_logger()
    spaces = SpacesObject(key="additional-stats")
    response = spaces.upload(stats)

    response_success = response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
    if response_success:
        logger.info("Upload successful.")
        return

    raise RuntimeError(f"Upload failed (response={response!r})")


@task
def process_additional_stats(stats: list[dict]) -> dict:
    return {
        "timestamp": int(datetime.now(timezone.utc).timestamp()),
        "datetime": datetime.now(timezone.utc).isoformat(),
        "results": stats,
    }


@flow(name="update_additional_stats")
def update_additional_stats(*, year: int | None = None, api_call_delay: float = 5.0) -> dict:
    logger = get_run_logger()
    resolved_year = resolve_year(year)
    ids = get_project_ids(year=resolved_year)
    results = []
    now = datetime.now(timezone.utc)

    for project_id in tqdm.tqdm(ids, desc="Fetching additional stats"):
        additional_stats = fetch_additional_stats(
            project_id, api_call_delay=api_call_delay
        )
        results.append(additional_stats)

    time_elapsed = (datetime.now(timezone.utc) - now).total_seconds()

    logger.info(
        f"Fetched additional stats for {len(results)} projects in {time_elapsed:.2f} seconds."
    )

    processed_stats = process_additional_stats(results)
    upload_additional_stats(processed_stats)
    return processed_stats


if __name__ == "__main__":
    update_additional_stats(year=resolve_year(settings.year))
