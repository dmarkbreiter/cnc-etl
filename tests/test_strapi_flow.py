from flows.update_strapi_stats import _transform_strapi_result


def test_transform_strapi_result_maps_nested_species_media():
    project_lookup = {
        8: {
            "city": "Copenhagen",
            "project": "Copenhagen",
        }
    }

    raw_result = {
        "id": 5,
        "title": "Copenhagen",
        "display": "Copenhagen",
        "project_id": 8,
        "observation_count": 0,
        "species_count": 0,
        "observers_count": 0,
        "identifiers_count": 0,
        "most_observed_species": {
            "id": 1,
            "scientific_name": None,
            "common_name": "Gray Squirrel ",
            "count": None,
            "media": {
                "id": 1,
                "url": "www.google.com",
                "attribution": None,
                "image": {
                    "id": 167,
                    "formats": {
                        "medium": {
                            "url": "https://cnc-assets.sfo3.digitaloceanspaces.com/NHMLA/medium_Amador_L_Headshot_sm_1861379b34.jpg",
                        }
                    },
                    "height": 2049,
                    "width": 1929,
                },
                "original_dimensions": None,
            },
        },
    }

    actual = _transform_strapi_result(raw_result, project_lookup)

    assert actual == {
        "id": 8,
        "city": "Copenhagen",
        "most_observed_species": {
            "media": {
                "url": "https://cnc-assets.sfo3.digitaloceanspaces.com/NHMLA/medium_Amador_L_Headshot_sm_1861379b34.jpg",
                "attribution": None,
                "original_dimensions": {
                    "height": 2049,
                    "width": 1929,
                },
            },
            "scientific_name": None,
            "common_name": "Gray Squirrel ",
            "count": 0,
        },
        "identifiers_count": 0,
        "quality_grades": {
            "research": 0,
            "needs_id": 0,
            "casual": 0,
        },
        "observation_count": 0,
        "species_count": 0,
        "observers_count": 0,
    }
