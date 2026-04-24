from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any, Literal

import pandas as pd
import requests
import tqdm
from prefect import get_run_logger, task

from clients.spaces import SpacesObject
from results.fetch import (
    get_project_identifiers_count,
    get_project_most_observed_species,
    get_project_quality_grades,
)
from settings import resolve_year

AdditionalStatName = Literal[
    "identifiers_count",
    "quality_grades",
    "most_observed_species",
]

ADDITIONAL_STATS_OBJECT_KEYS: dict[AdditionalStatName, str] = {
    "identifiers_count": "additional-stats-identifiers-count",
    "quality_grades": "additional-stats-quality-grades",
    "most_observed_species": "additional-stats-most-observed-species",
}


def _fetch_stat_value(project_id: int, stat_name: AdditionalStatName) -> Any:
    if stat_name == "identifiers_count":
        return get_project_identifiers_count(project_id)
    if stat_name == "quality_grades":
        return get_project_quality_grades(project_id)
    if stat_name == "most_observed_species":
        return get_project_most_observed_species(project_id)
    raise ValueError(f"Unsupported additional stat: {stat_name}")


def _default_stat_value(stat_name: AdditionalStatName) -> Any:
    if stat_name == "identifiers_count":
        return 0
    if stat_name == "quality_grades":
        return {"research": 0, "needs_id": 0, "casual": 0}
    if stat_name == "most_observed_species":
        return {
            "media": {"url": "", "attribution": "", "original_dimensions": {}},
            "scientific_name": "",
            "common_name": "",
            "count": 0,
        }
    raise ValueError(f"Unsupported additional stat: {stat_name}")


@task
def get_project_ids(year: int) -> list[int]:
    projects = pd.read_csv(f"data/{year}/inaturalist-projects_{year}.csv")
    return list(dict.fromkeys(projects["id"].tolist()))


@task(retries=3, retry_delay_seconds=10)
def fetch_additional_stat(
    project_id: int,
    stat_name: AdditionalStatName,
    api_call_delay: float = 5.0,
) -> dict:
    logger = get_run_logger()
    try:
        value = _fetch_stat_value(project_id, stat_name)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code != 422:
            raise

        logger.warning(
            "Project %s returned 422 for %s; using default value because the project may have been deleted.",
            project_id,
            stat_name,
        )
        value = _default_stat_value(stat_name)

    # Sleep briefly between project requests to avoid hitting rate limits.
    time.sleep(api_call_delay)

    logger.info("Fetched %s for project_id=%s.", stat_name, project_id)
    return {
        "id": project_id,
        stat_name: value,
    }


@task
def process_additional_stat_results(
    stat_name: AdditionalStatName,
    stats: list[dict],
    year: int,
) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "timestamp": int(now.timestamp()),
        "datetime": now.isoformat(),
        "year": year,
        "metric": stat_name,
        "results": stats,
    }


@task(retries=3, retry_delay_seconds=10)
def upload_additional_stat_results(
    stat_name: AdditionalStatName,
    stats: dict,
) -> None:
    logger = get_run_logger()
    spaces = SpacesObject(key=ADDITIONAL_STATS_OBJECT_KEYS[stat_name])
    response = spaces.upload(stats)

    response_success = response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
    if response_success:
        logger.info("Upload successful.")
        return

    raise RuntimeError(f"Upload failed (response={response!r})")


def merge_additional_stat_results(*stats_payloads: list[dict]) -> list[dict]:
    merged_by_id: dict[int, dict] = {}

    for payload in stats_payloads:
        for stat in payload:
            project_id = stat.get("id")
            if project_id is None:
                continue
            merged_by_id.setdefault(project_id, {"id": project_id}).update(
                {
                    key: value
                    for key, value in stat.items()
                    if key != "id"
                }
            )

    return list(merged_by_id.values())


def run_additional_stat_update(
    *,
    stat_name: AdditionalStatName,
    year: int | None = None,
    api_call_delay: float = 5.0,
) -> dict:
    logger = get_run_logger()
    resolved_year = resolve_year(year)
    ids = get_project_ids(year=resolved_year)
    results = []
    started_at = datetime.now(timezone.utc)

    for project_id in tqdm.tqdm(ids, desc=f"Fetching {stat_name}"):
        results.append(
            fetch_additional_stat(
                project_id,
                stat_name=stat_name,
                api_call_delay=api_call_delay,
            )
        )

    elapsed_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
    logger.info(
        "Fetched %s for %s projects in %.2f seconds.",
        stat_name,
        len(results),
        elapsed_seconds,
    )

    processed_results = process_additional_stat_results(
        stat_name,
        results,
        year=resolved_year,
    )
    upload_additional_stat_results(stat_name, processed_results)
    return processed_results
