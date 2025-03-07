# #!/bin/bash
set -e

function run {
    bash $(pwd)/recipes/$1/runner.sh
}
register 'run' 'recipe' '{ recipe name }' run
