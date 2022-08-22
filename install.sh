#!/usr/bin/env bash

set -e

PYTHON=${1:-python3}
which ${PYTHON} > /dev/null
if [ $? -ne 0 ]; then
    PYTHON=python
fi

MAJOR=$(${PYTHON} -c 'import sys; print(sys.version_info.major)')
MINOR=$(${PYTHON} -c 'import sys; print(sys.version_info.minor)')
echo "${PYTHON} v${MAJOR}.${MINOR}"
if [ ${MAJOR} -eq 3 ] && [ ${MINOR} -lt 8 ] || [ ${MAJOR} -lt 3 ]; then
    echo "${PYTHON} version must at least be 3.8" >&2
    exit 1
fi

${PYTHON} -m pip install --upgrade pip
${PYTHON} -m pip install --progress-bar off --upgrade -r requirements.lint.txt
