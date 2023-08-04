from src.github import get_default_branch, get_branches

TEST_REPO = "data-engineering-qaqc"

def test_get_default_branch():
    branch = get_default_branch(repo=TEST_REPO)
    assert branch == "main"


def test_get_branches():
    branches = get_branches(repo=TEST_REPO, branches_blacklist=[])
    assert "main" in branches
