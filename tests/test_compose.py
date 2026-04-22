from flows.compose import merge_additional_stats, merge_stats


def test_merge_additional_stats_combines_partial_results_by_project_id():
    identifiers_count_stats = [
        {"id": 1, "identifiers_count": 10},
        {"id": 2, "identifiers_count": 5},
    ]
    quality_grades_stats = [
        {"id": 1, "quality_grades": {"research": 1, "needs_id": 2, "casual": 3}},
        {"id": 2, "quality_grades": {"research": 4, "needs_id": 0, "casual": 1}},
    ]
    most_observed_species_stats = [
        {"id": 1, "most_observed_species": {"scientific_name": "A"}},
        {"id": 2, "most_observed_species": {"scientific_name": "B"}},
    ]

    actual = merge_additional_stats.fn(
        identifiers_count_stats,
        quality_grades_stats,
        most_observed_species_stats,
    )

    assert actual == [
        {
            "id": 1,
            "identifiers_count": 10,
            "quality_grades": {"research": 1, "needs_id": 2, "casual": 3},
            "most_observed_species": {"scientific_name": "A"},
        },
        {
            "id": 2,
            "identifiers_count": 5,
            "quality_grades": {"research": 4, "needs_id": 0, "casual": 1},
            "most_observed_species": {"scientific_name": "B"},
        },
    ]


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
