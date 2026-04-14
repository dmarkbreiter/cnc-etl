from clients.spaces import SpacesObject
import pandas as pd


# future task
def fetch_additional_stats():
    spaces = SpacesObject(key="additional-stats")
    content = spaces.get_content()

    return content.get("results", [])


# future task
def fetch_umbrella_stats():
    spaces = SpacesObject(key="umbrella-stats")
    content = spaces.get_content()

    return content


# future task
def merge_stats(additional_stats: list[dict], umbrella_stats: list[dict]) -> list[dict]:
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

    return {
        "timestamp": int(pd.Timestamp.now(tz="UTC").timestamp()),
        "datetime": pd.Timestamp.now(tz="UTC").isoformat(),
        "totals": umbrella_stats.get("totals", {}),
        "results": merged_results,
    }


# future task
def upload_merged_stats(merged_stats: list[dict]) -> None:
    spaces = SpacesObject(key="city-results")
    response = spaces.upload(merged_stats)

    response_success = response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
    if response_success:
        print("Upload successful!")
    else:
        print("Upload failed.")


if __name__ == "__main__":
    additional_stats = fetch_additional_stats()
    umbrella_stats = fetch_umbrella_stats()
    merged = merge_stats(additional_stats, umbrella_stats)
    upload_merged_stats(merged)
