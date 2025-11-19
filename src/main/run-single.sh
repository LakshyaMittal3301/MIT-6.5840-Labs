#!/usr/bin/env bash

set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <app-name-or-go-file>   (e.g. wc or wc.go)"
    exit 1
fi

APP_ARG="$1"
APP_BASE="${APP_ARG%.go}"       # wc.go -> wc, wc -> wc
APP_GO="${APP_BASE}.go"         # wc -> wc.go
APP_SO="${APP_BASE}.so"         # wc -> wc.so

# We assume this script is run from src/main
# Create a fresh work dir under main
WORKDIR="mr-single-tmp"
rm -rf "$WORKDIR"
mkdir "$WORKDIR"
cd "$WORKDIR"

echo "*** Building plugin for ${APP_GO}"
( cd ../../mrapps && go build -buildmode=plugin "$APP_GO" )

echo "*** Building coordinator/worker/sequential"
( cd .. && go build mrcoordinator.go )
( cd .. && go build mrworker.go )
( cd .. && go build mrsequential.go )

echo "*** Generating expected output with mrsequential"
../mrsequential "../../mrapps/${APP_SO}" ../pg*txt

# if mrsequential writes mr-expected directly:
if [ -f mr-expected ]; then
    sort mr-expected > mr-expected.sorted
    mv mr-expected.sorted mr-expected
# otherwise, fall back to lab's default mr-out-0
elif [ -f mr-out-0 ]; then
    sort mr-out-0 > mr-expected
    rm -f mr-out*
else
    echo "ERROR: mrsequential did not produce mr-expected or mr-out-0"
    exit 1
fi

echo "*** Running distributed MapReduce for ${APP_BASE}"

# start coordinator
../mrcoordinator ../pg*txt &
CID=$!

# give coordinator time to create socket
sleep 1

# start a few workers
../mrworker "../../mrapps/${APP_SO}" &
../mrworker "../../mrapps/${APP_SO}" &
../mrworker "../../mrapps/${APP_SO}" &

# wait for coordinator to finish
wait "$CID" || true

# wait for workers to exit
wait || true

echo "*** Collecting and comparing output"
if ls mr-out-* >/dev/null 2>&1; then
    sort mr-out-* > mr-all
    if diff -u mr-expected mr-all >/dev/null; then
        echo "RESULT: PASS (${APP_BASE})"
        exit 0
    else
        echo "RESULT: FAIL (${APP_BASE})"
        echo "--- diff between expected and actual:"
        diff -u mr-expected mr-all || true
        exit 1
    fi
else
    echo "RESULT: FAIL (${APP_BASE}) - no mr-out-* files produced"
    exit 1
fi