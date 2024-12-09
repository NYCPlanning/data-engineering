from dcpy.connectors.github import download_repo, get_default_branch, get_branches

TEST_REPO = "data-engineering"


def test_get_default_branch():
    branch = get_default_branch(repo=TEST_REPO)
    assert branch == "main"


def test_download_repo(create_temp_filesystem):
    repo_path = download_repo(
        repo=TEST_REPO,
        output_directory=create_temp_filesystem,
        branch="main",
    )
    assert repo_path.exists()


def test_get_branches():
    branches = get_branches(repo=TEST_REPO, branches_blacklist=[])
    assert "main" in branches
