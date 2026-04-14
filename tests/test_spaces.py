import clients.spaces as spaces


def test_get_content_city_results_json():

    obj = spaces.SpacesObject(key="city-results")
    actual = obj.get_content()

    results = actual.get("results", [])
    totals = actual.get("totals", {})

    assert len(results)
    assert totals.get("observations")
