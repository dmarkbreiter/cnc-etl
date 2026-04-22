from datetime import datetime, timezone

import pandas as pd
from prefect import flow, get_run_logger, task

from clients.spaces import SpacesObject
from results.fetch import get_non_inaturalist_project_stats
from settings import resolve_year, settings


def _normalize_int(value: object) -> int:
    try:
        if value in ("", None):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _normalize_media(media: dict | None) -> dict:
    media = media or {}
    return {
        "url": media.get("url", ""),
        "attribution": media.get("attribution", ""),
        "original_dimensions": media.get("original_dimensions") or {},
    }


def _normalize_most_observed_species(value: object) -> dict:
    if isinstance(value, list):
        species = value[0] if value else {}
    elif isinstance(value, dict):
        species = value
    else:
        species = {}

    if not isinstance(species, dict):
        species = {}

    return {
        "media": _normalize_media(species.get("media")),
        "scientific_name": species.get("scientific_name") or "",
        "common_name": species.get("common_name") or "",
        "count": _normalize_int(species.get("count")),
    }


def _normalize_quality_grades(stats: dict) -> dict:
    observation_count = _normalize_int(stats.get("observation_count"))
    research_count = _normalize_int(stats.get("research_grade_observations_count"))
    casual_count = max(0, observation_count - research_count)

    return {
        "research": research_count,
        "needs_id": 0,
        "casual": casual_count,
    }


def _normalize_non_inat_result(project: dict, stats: dict) -> dict:
    return {
        "id": _normalize_int(project.get("id")),
        "city": project.get("city"),
        "most_observed_species": _normalize_most_observed_species(
            stats.get("most_observed_species")
        ),
        "identifiers_count": _normalize_int(stats.get("identifiers_count")),
        "quality_grades": _normalize_quality_grades(stats),
        "observation_count": _normalize_int(stats.get("observation_count")),
        "species_count": _normalize_int(stats.get("species_count")),
        "observers_count": _normalize_int(stats.get("observers_count")),
    }


def _count_totals(stats: list[dict]) -> dict:
    totals = {
        "observation_count": 0,
        "species_count": 0,
        "observers_count": 0,
        "identifiers_count": 0,
        "research_grade_observations_count": 0,
    }

    for stat in stats:
        totals["observation_count"] += _normalize_int(stat.get("observation_count"))
        totals["species_count"] += _normalize_int(stat.get("species_count"))
        totals["observers_count"] += _normalize_int(stat.get("observers_count"))
        totals["identifiers_count"] += _normalize_int(stat.get("identifiers_count"))
        totals["research_grade_observations_count"] += _normalize_int(
            (stat.get("quality_grades") or {}).get("research")
        )

    return totals


@task
def get_non_inat_projects(year: int) -> list[dict]:
    projects = pd.read_csv(
        f"data/{year}/non-inaturalist-projects_{year}.csv",
        keep_default_na=False,
    )
    return projects.to_dict(orient="records")


@task(retries=3, retry_delay_seconds=10)
def fetch_non_inat_stats(project: dict) -> dict:
    logger = get_run_logger()
    stats = get_non_inaturalist_project_stats(project["endpoint"])
    normalized = _normalize_non_inat_result(project, stats)
    logger.info(
        "Fetched non-iNaturalist stats for city=%s endpoint=%s.",
        normalized["city"],
        project["endpoint"],
    )
    return normalized


@task
def process_non_inat_stats(stats: list[dict], year: int) -> dict:
    now = datetime.now(timezone.utc)

    return {
        "timestamp": int(now.timestamp()),
        "datetime": now.isoformat(),
        "year": year,
        "totals": _count_totals(stats),
        "results": stats,
    }


@task(retries=3, retry_delay_seconds=10)
def upload_non_inat_stats(stats: dict) -> None:
    logger = get_run_logger()
    spaces = SpacesObject(key="non-inat-stats")
    response = spaces.upload(stats)

    response_success = response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
    if response_success:
        logger.info("Upload successful.")
        return

    raise RuntimeError(f"Upload failed (response={response!r})")


@flow(name="update_non_inat_stats")
def update_non_inat_stats(*, year: int | None = None) -> dict:
    logger = get_run_logger()
    resolved_year = resolve_year(year)
    projects = get_non_inat_projects(resolved_year)

    results = [fetch_non_inat_stats(project) for project in projects]
    processed_results = process_non_inat_stats(results, year=resolved_year)

    logger.info(
        "Fetched non-iNaturalist stats for %s projects.", len(processed_results["results"])
    )
    upload_non_inat_stats(processed_results)
    return processed_results


if __name__ == "__main__":
    update_non_inat_stats(year=resolve_year(settings.year))
