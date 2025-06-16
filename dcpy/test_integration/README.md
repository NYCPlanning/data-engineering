# Integration tests

Tests in this directory require network resources like databases and servers.

We'd like to run these tests in CI/CD, but haven't resolved all issues related to spinning up services during the relevant github action.

## Running tests

### Dependency setup - docker

The SFTP and postgres tests depend on a docker compose setup.

1. Regardless of whether you work in a dev container, the integration test containers need to be started. From a terminal (NOT in the container, if you're working from within one), run

    ```bash
    docker network create de
    docker compose up -d
    ```

    The first command only needed the first time you do this (or first time since cleaning up your docker setup)

2. Ensure correct permissions on the (not-so) private key: `chmod 600 dcpy/test_integration/docker/sftp/id_rsa_key_integration_test`    

3. From a terminal, confirm the SFTP service is acessible using. Slight difference depending on environment. Dev Container:

    ```bash
    sftp -i dcpy/test_integration/docker/sftp/id_rsa_key_integration_test \
        -o StrictHostKeyChecking=yes \
        -o UserKnownHostsFile=dcpy/test_integration/docker/sftp/known_hosts_integration_test \
        dedev@sftp-server
    ```

    No container:

    ```bash
    sftp -i dcpy/test_integration/docker/sftp/id_rsa_key_integration_test \
        -o StrictHostKeyChecking=yes \
        -o UserKnownHostsFile=dcpy/test_integration/docker/sftp/known_hosts_integration_test \
        -o Port=2222 \
        dedev@localhost
    ```

3. Run tests using pytest, for example

    ```bash
    python3 -m pytest ./dcpy/test_integration/test_sftp.py -v -s
    ```

    ```bash
    python3 -m pytest ./dcpy/test_integration/test_sftp.py::test_list_directory -v -s
    ```
