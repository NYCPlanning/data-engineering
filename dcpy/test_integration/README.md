# Integration tests

Tests in this directory require network resources like databases and servers.

We'd like to run these tests in CI/CD, but haven't resolved all issues related to spinning up services during the relevant github action.

## Running tests

### SFTP

The SFTP tests depend on the dev container configured at the root of this repositiory.

1. Start the repo's dev container either using using VS Code or terminal with `docker exec -it de sh`
2. From a terminal in the dev container, confirm the SFTP service is acessible using:

    ```bash
    sftp -i .devcontainer/sftp/id_rsa_key_integration_test \
        -o StrictHostKeyChecking=yes \
        -o UserKnownHostsFile=.devcontainer/sftp/known_hosts_integration_test \
        dedev@sftp-server
    ```

3. Run tests using pytest, for example

    ```bash
    python3 -m pytest ./dcpy/test_integration/test_sftp.py -v -s
    ```

    ```bash
    python3 -m pytest ./dcpy/test_integration/test_sftp.py::test_list_directory -v -s
    ```
