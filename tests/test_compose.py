from flows.compose import merge_stats


def test_merge_stats_appends_strapi_and_non_inat_results():
    additional_stats = [
        {
            "id": 1,
            "most_observed_species": {"scientific_name": "A"},
            "identifiers_count": 10,
            "quality_grades": {"research": 1, "needs_id": 2, "casual": 3},
        }
    ]
    umbrella_stats = {
        "results": [
            {
                "id": 1,
                "city": "Los Angeles",
                "observation_count": 11,
                "species_count": 12,
                "observers_count": 13,
            }
        ],
    }
    strapi_stats = [
        {
            "id": 8,
            "city": "Copenhagen",
            "most_observed_species": {"scientific_name": "Gray Squirrel"},
            "identifiers_count": 0,
            "quality_grades": {"research": 0, "needs_id": 0, "casual": 0},
            "observation_count": 0,
            "species_count": 0,
            "observers_count": 0,
        }
    ]
    non_inat_stats = [
        {
            "id": 522,
            "city": "Barcelona Metropolitan Area",
            "most_observed_species": {"scientific_name": "Corvus corax"},
            "identifiers_count": 2,
            "quality_grades": {"research": 1, "needs_id": 0, "casual": 3},
            "observation_count": 4,
            "species_count": 2,
            "observers_count": 1,
        }
    ]

    actual = merge_stats.fn(
        additional_stats, umbrella_stats, strapi_stats, non_inat_stats
    )

    assert actual["totals"] == {
        "observation_count": 15,
        "species_count": 14,
        "observer_count": 14,
    }
    assert actual["results"] == [
        {
            "id": 1,
            "city": "Los Angeles",
            "most_observed_species": {"scientific_name": "A"},
            "identifiers_count": 10,
            "quality_grades": {"research": 1, "needs_id": 2, "casual": 3},
            "observation_count": 11,
            "species_count": 12,
            "observers_count": 13,
        },
        {
            "id": 8,
            "city": "Copenhagen",
            "most_observed_species": {"scientific_name": "Gray Squirrel"},
            "identifiers_count": 0,
            "quality_grades": {"research": 0, "needs_id": 0, "casual": 0},
            "observation_count": 0,
            "species_count": 0,
            "observers_count": 0,
        },
        {
            "id": 522,
            "city": "Barcelona Metropolitan Area",
            "most_observed_species": {"scientific_name": "Corvus corax"},
            "identifiers_count": 2,
            "quality_grades": {"research": 1, "needs_id": 0, "casual": 3},
            "observation_count": 4,
            "species_count": 2,
            "observers_count": 1,
        },
    ]
