from clients.spaces import SpacesObject
import pandas as pd

from prefect import flow, get_run_logger, task


@task(retries=3, retry_delay_seconds=5)
def fetch_additional_stats():
    logger = get_run_logger()
    spaces = SpacesObject(key="additional-stats")
    content = spaces.get_content()

    logger.info("Fetched additional stats payload (has_results=%s).", bool(content))
    return content.get("results", [])


@task(retries=3, retry_delay_seconds=5)
def fetch_umbrella_stats():
    logger = get_run_logger()
    spaces = SpacesObject(key="umbrella-stats")
    content = spaces.get_content()

    logger.info("Fetched umbrella stats payload (has_results=%s).", bool(content))
    return content


@task(retries=3, retry_delay_seconds=5)
def fetch_strapi_stats():
    logger = get_run_logger()
    spaces = SpacesObject(key="strapi-results")
    content = spaces.get_content()

    logger.info("Fetched Strapi stats payload (has_results=%s).", bool(content))
    return content.get("results", [])


@task(retries=3, retry_delay_seconds=5)
def fetch_non_inat_stats():
    logger = get_run_logger()
    spaces = SpacesObject(key="non-inat-stats")
    content = spaces.get_content()

    logger.info("Fetched non-iNat stats payload (has_results=%s).", bool(content))
    return content.get("results", [])


def _count_totals(results: list[dict]) -> dict:
    return {
        "observation_count": sum(result.get("observation_count", 0) for result in results),
        "species_count": sum(result.get("species_count", 0) for result in results),
        "observer_count": sum(result.get("observers_count", 0) for result in results),
    }


@task
def merge_stats(
    additional_stats: list[dict],
    umbrella_stats: dict,
    strapi_stats: list[dict],
    non_inat_stats: list[dict],
) -> dict:
    # Create a mapping of project_id to umbrella stats for quick lookup
    additional_mapping = {stat.get("id"): stat for stat in additional_stats}

    merged_results = []
    for umbrella in umbrella_stats.get("results", []):
        project_id = umbrella.get("id")
        additional = additional_mapping.get(project_id, {})

        merged_results.append(
            {
                "id": project_id,
                "city": umbrella.get("city"),
                "most_observed_species": additional.get("most_observed_species"),
                "identifiers_count": additional.get("identifiers_count"),
                "quality_grades": additional.get("quality_grades"),
                "observation_count": umbrella.get("observation_count", 0),
                "species_count": umbrella.get("species_count", 0),
                "observers_count": umbrella.get("observers_count", 0),
            }
        )

    merged_results.extend(strapi_stats)
    merged_results.extend(non_inat_stats)

    return {
        "timestamp": int(pd.Timestamp.now(tz="UTC").timestamp()),
        "datetime": pd.Timestamp.now(tz="UTC").isoformat(),
        "totals": _count_totals(merged_results),
        "results": merged_results,
    }


@task(retries=3, retry_delay_seconds=10)
def upload_merged_stats(merged_stats: dict) -> None:
    logger = get_run_logger()
    spaces = SpacesObject(key="city-results")
    response = spaces.upload(merged_stats)

    response_success = response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
    if response_success:
        logger.info("Upload successful.")
        return

    raise RuntimeError(f"Upload failed (response={response!r})")


@flow(name="compose_city_results")
def compose_city_results() -> dict:
    additional_stats = fetch_additional_stats()
    umbrella_stats = fetch_umbrella_stats()
    strapi_stats = fetch_strapi_stats()
    non_inat_stats = fetch_non_inat_stats()
    merged = merge_stats(
        additional_stats, umbrella_stats, strapi_stats, non_inat_stats
    )
    upload_merged_stats(merged)
    return merged


if __name__ == "__main__":
    compose_city_results()
