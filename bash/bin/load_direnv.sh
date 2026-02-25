#!/bin/bash
# Load direnv environment variables for the current directory. Mostly for processes
# that, ahem, make use of non-interactive terminals
eval "$(direnv export bash)"
