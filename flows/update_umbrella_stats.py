from results.fetch import get_umbrella_project_stats
import pandas as pd
from clients.spaces import SpacesObject
from datetime import datetime, timezone

from settings import settings


# future task
def fetch_umbrella_stats(project_id: str) -> list[dict]:

    return get_umbrella_project_stats(project_id)


# future task
def count_totals(stats: list[dict]) -> dict:
    totals = {"observation_count": 0, "species_count": 0, "observer_count": 0}

    for stat in stats:
        totals["observation_count"] += stat.get("observation_count", 0)
        totals["species_count"] += stat.get("species_count", 0)
        totals["observer_count"] += stat.get("observers_count", 0)

    return totals


# future task
def process_umbrella_stats(stats: list[dict]) -> list[dict]:
    # Placeholder for any processing logic; currently just returns the input.
    projects = pd.read_csv(
        f"data/{settings.year}/inaturalist-projects_{settings.year}.csv",
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


# future task
def upload_umbrella_stats(stats: list[dict]) -> None:
    # Placeholder for upload logic; currently just prints the stats.
    spaces = SpacesObject(key="umbrella-stats")
    response = spaces.upload(stats)

    response_success = response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
    if response_success:
        print("Upload successful!")
    else:
        print("Upload failed!")


# future flow
def update_umbrella_stats(project_id: str) -> list[dict]:
    results = fetch_umbrella_stats(project_id)
    processed_results = process_umbrella_stats(results)
    print(f"Fetched umbrella stats for project {project_id}: {processed_results}")
    upload_umbrella_stats(processed_results)
    return processed_results


if __name__ == "__main__":
    update_umbrella_stats(f"city-nature-challenge-{settings.year}")
