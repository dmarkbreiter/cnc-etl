from datetime import time
import random
from typing import Any, TypedDict, Optional

import requests


class MostObservedSpecies(TypedDict):
    media: dict[str, str]
    scientific_name: str
    common_name: str | None
    count: int


class QualityGradeCounts(TypedDict):
    research: int
    needs_id: int
    casual: int


class UmbrellaProjectStats(TypedDict):
    observation_count: int
    species_count: int
    observers_count: int
    project: dict


def _normalize_strapi_value(value):
    """
    Recursively unwrap Strapi `data` / `attributes` containers into plain values.
    """

    if isinstance(value, list):
        return [_normalize_strapi_value(item) for item in value]

    if isinstance(value, dict):
        if "data" in value and len(value) == 1:
            return _normalize_strapi_value(value["data"])

        if "attributes" in value and isinstance(value["attributes"], dict):
            normalized = _normalize_strapi_value(value["attributes"])
            if isinstance(normalized, dict) and "id" in value:
                normalized["id"] = value.get("id")
            return normalized

        return {key: _normalize_strapi_value(item) for key, item in value.items()}

    return value


def get_project_most_observed_species(project_id: int) -> MostObservedSpecies:
    """
    Get the most observed species for a given iNaturalist project.
     - project_id: iNaturalist project ID to query
    """
    response = requests.get(
        "https://api.inaturalist.org/v2/observations/species_counts",
        params={
            "per_page": 1,
            "fields": "all",
            "project_id": project_id,
        },
    )

    response.raise_for_status()
    if response.status_code > 299:
        raise ValueError(
            f"Error fetching most observed species for project_id {project_id}: {response.status_code} {response.text}"
        )

    data = response.json()
    results = data.get("results", [])

    if not results:
        results.append(
            {
                "count": 0,
                "taxon": {"name": "", "preferred_common_name": ""},
                "media": {"url": "", "attribution": "", "original_dimensions": {}},
            }
        )

    taxon = results[0].get("taxon", {})

    return MostObservedSpecies(
        media={
            "url": taxon.get("default_photo", {}).get("medium_url", ""),
            "attribution": taxon.get("default_photo", {}).get("attribution", ""),
            "original_dimensions": taxon.get("default_photo", {}).get(
                "original_dimensions", {}
            ),
        },
        scientific_name=taxon.get("name", ""),
        common_name=taxon.get("preferred_common_name", "").capitalize(),
        count=results[0].get("count", 0),
    )


def get_project_identifiers_count(
    project_id: int,
) -> int:
    """
    Count of identifiers for a given iNaturalist project.
    """

    url = "https://api.inaturalist.org/v2/observations/identifiers"
    params = {"fields": "all", "project_id": project_id, "per_page": 1}

    # Request aggregated response from paginate (returns response-like dict)
    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    results_count = data.get("total_results", 0)
    return results_count


def get_project_quality_grades(project_id: int) -> QualityGradeCounts:
    """
    Get quality grade counts for a given iNaturalist project.
    """

    url = "https://api.inaturalist.org/v2/observations/quality_grades"
    params = {"fields": "all", "project_id": project_id, "per_page": 1}

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    items = data.get("results", [])

    # Build a deterministic mapping with defaults for missing grades
    counts = {"research": 0, "needs_id": 0, "casual": 0}
    for item in items:
        grade = item.get("quality_grade")
        cnt = item.get("count", 0)
        if grade in counts:
            counts[grade] = cnt

    return QualityGradeCounts(**counts)


def get_umbrella_project_stats(project_id: str) -> list[UmbrellaProjectStats]:
    """
    Get aggregated stats for a given iNaturalist project.
    """

    response = requests.get(
        "https://api.inaturalist.org/v2/observations/umbrella_project_stats",
        params={
            "project_id": project_id,
        },
    )

    response.raise_for_status()
    data = response.json()
    results = data.get("results", [])

    if not results:
        raise ValueError(f"No results found for project_id {project_id}")

    return results


def get_strapi_results(year: int) -> list[dict[str, Any]]:
    """
    Fetch raw project results for a City Nature Challenge year from Strapi.
    """

    response = requests.get(
        "https://cnc.nhmlac.org/api/event-dates",
        params={
            "populate[0]": "results",
            "populate[results][populate][most_observed_species][populate]": "*",
            "filters[year][$eq]": year,
        },
    )

    response.raise_for_status()
    data = response.json()

    event_dates = _normalize_strapi_value(data.get("data", []))
    raw_results = []
    if event_dates:
        raw_results = event_dates[0].get("results", [])

    return [_normalize_strapi_value(result) for result in raw_results]
