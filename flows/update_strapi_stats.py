from datetime import datetime, timezone

from clients.spaces import SpacesObject
import pandas as pd
from prefect import flow, get_run_logger, task

from results.fetch import get_strapi_results
from settings import resolve_year, settings


@task(retries=3, retry_delay_seconds=10)
def fetch_strapi_results(year: int) -> list[dict]:
    logger = get_run_logger()
    results = get_strapi_results(year)
    logger.info("Fetched %s Strapi results for year=%s.", len(results), year)
    return results


def _normalize_media(media: dict | None) -> dict:
    media = media or {}
    image = media.get("image") or {}
    if "data" in image:
        image = image.get("data") or {}

    image_attributes = image.get("attributes") or image
    medium_format = (image_attributes.get("formats") or {}).get("medium") or {}

    url = medium_format.get("url") or media.get("url", "")

    if image_attributes.get("height") and image_attributes.get("width"):
        original_dimensions = {
            "height": image_attributes.get("height"),
            "width": image_attributes.get("width"),
        }
    else:
        original_dimensions = media.get("original_dimensions") or {}

    return {
        "url": url,
        "attribution": media.get("attribution", ""),
        "original_dimensions": original_dimensions,
    }


def _normalize_most_observed_species(species: dict | None) -> dict:
    species = species or {}
    return {
        "media": _normalize_media(species.get("media")),
        "scientific_name": species.get("scientific_name"),
        "common_name": species.get("common_name"),
        "count": species.get("count") or 0,
    }


def _transform_strapi_result(result: dict) -> dict:
    project_id = result.get("project_id")

    return {
        "id": project_id,
        "city": result.get("city"),
        "most_observed_species": _normalize_most_observed_species(
            result.get("most_observed_species")
        ),
        "identifiers_count": int(result.get("identifiers_count") or 0),
        "quality_grades": result.get("quality_grades")
        or {"research": 0, "needs_id": 0, "casual": 0},
        "observation_count": result.get("observation_count", 0),
        "species_count": result.get("species_count", 0),
        "observers_count": result.get("observers_count", 0),
    }


@task
def process_strapi_results(results: list[dict], year: int) -> dict:
    now = datetime.now(timezone.utc)
    normalized_results = [_transform_strapi_result(result) for result in results]

    return {
        "timestamp": int(now.timestamp()),
        "datetime": now.isoformat(),
        "year": year,
        "results": normalized_results,
    }


@task(retries=3, retry_delay_seconds=10)
def upload_strapi_results(stats: dict) -> None:
    logger = get_run_logger()
    spaces = SpacesObject(key="strapi-results")
    response = spaces.upload(stats)

    response_success = response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
    if response_success:
        logger.info("Upload successful.")
        return

    raise RuntimeError(f"Upload failed (response={response!r})")


@flow(name="update_strapi_results")
def update_strapi_results(*, year: int | None = None) -> dict:
    resolved_year = resolve_year(year)

    results = fetch_strapi_results(resolved_year)
    processed_results = process_strapi_results(results, year=resolved_year)
    upload_strapi_results(processed_results)
    return processed_results


if __name__ == "__main__":
    update_strapi_results(year=resolve_year(settings.year))
