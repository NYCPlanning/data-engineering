# Integration tests

Tests in this directory require network resources like databases and servers.

We'd like to run these tests in CI/CD, but haven't resolved all issues related to spinning up services during the relevant github action.

## Running tests

### SFTP

The SFTP tests depend on the dev container configured at the root of this repositiory.

1. Start the repo's dev container either using using VS Code or [INSERT COMMAND HERE]
2. From a terminal in the dev container, confirm the SFTP service is acessible using:

    ```bash
    sftp -i .devcontainer/sftp/ssh_host_rsa_key -o StrictHostKeyChecking=no dedev@sftp-server
    ```

3. Run tests using pytest, for example

    ```bash
    python3 -m pytest ./dcpy/test_integration/test_sftp.py -v -s
    ```

    ```bash
    python3 -m pytest ./dcpy/test_integration/test_sftp.py::test_list_directory -v -s
    ```
