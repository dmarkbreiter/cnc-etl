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

from settings import settings


# future task
def fetch_additional_stats(project_id: int, api_call_delay: float = 5) -> dict:

    most_observed_species = get_project_most_observed_species(project_id)

    identifiers_count = get_project_identifiers_count(project_id)
    quality_grades = get_project_quality_grades(project_id)

    # Sleep briefly between API calls to avoid hitting rate limits.
    time.sleep(api_call_delay)

    return {
        "id": project_id,
        "most_observed_species": most_observed_species,
        "identifiers_count": identifiers_count,
        "quality_grades": quality_grades,
    }


# future task
def get_project_ids() -> list[int]:

    projects = pd.read_csv(
        f"data/{settings.year}/inaturalist-projects_{settings.year}.csv"
    )
    return [*{*projects["id"].tolist()}]


# future task
def upload_additional_stats(stats: dict) -> None:
    spaces = SpacesObject(key="additional-stats")
    response = spaces.upload(stats)

    response_success = response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
    if response_success:
        print("Upload successful!")
    else:
        print("Upload failed.")


# future task
def process_additional_stats(stats: list[dict]) -> dict:
    return {
        "timestamp": int(datetime.now(timezone.utc).timestamp()),
        "datetime": datetime.now(timezone.utc).isoformat(),
        "results": stats,
    }


# future flow
def update_additional_stats() -> None:
    ids = get_project_ids()
    results = []
    now = datetime.now(timezone.utc)

    for id in tqdm.tqdm(ids, desc="Fetching additional stats"):
        additional_stats = fetch_additional_stats(id)
        results.append(additional_stats)

    time_elapsed = (datetime.now(timezone.utc) - now).total_seconds()

    print(
        f"Fetched additional stats for {len(results)} projects in {time_elapsed:.2f} seconds."
    )

    processed_stats = process_additional_stats(results)
    upload_additional_stats(processed_stats)


if __name__ == "__main__":
    update_additional_stats()
