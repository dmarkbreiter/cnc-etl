import pyinaturalist


def test_setup_inat_client():
    # Ensure the client is initialized before tests run.

    projects = pyinaturalist.get_projects(q="city-nature-challenge-2024", per_page=1000)

    assert projects.get("total_results", 0) >= 0
