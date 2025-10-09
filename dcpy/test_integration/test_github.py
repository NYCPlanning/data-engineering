from dcpy.utils.git import github

TEST_REPO = "data-engineering"


def test_get_default_branch():
    branch = github.get_default_branch(repo=TEST_REPO)
    assert branch == "main"


def test_download_repo(tmp_path):
    repo_path = github.clone_repo(
        repo=TEST_REPO,
        output_directory=tmp_path,
        branch="main",
    )
    assert repo_path.exists()


def test_get_branches():
    branches = github.get_branches(repo=TEST_REPO, branches_blacklist=[])
    assert "main" in branches
