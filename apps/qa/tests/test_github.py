from dcpy.connectors.github import get_default_branch, get_branches

TEST_REPO = "data-engineering-qaqc"


## TODO - these tests don't belong here since this functionality has been moved to dcpy
def test_get_default_branch():
    branch = get_default_branch(repo=TEST_REPO)
    assert branch == "main"


def test_get_branches():
    branches = get_branches(repo=TEST_REPO, branches_blacklist=[])
    assert "main" in branches
