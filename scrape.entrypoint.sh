#!/usr/bin/env bash

set -euo pipefail

SCRIPT=${1:-}

if [[ -z $SCRIPT ]]; then
    echo "Usage: $0 <script>"
    exit 1
fi

# run every day at 4am Europe/Athens time
while true; do
    now=$(TZ="Europe/Athens" date +%H%M)
    if [[ "$now" == "0400" ]]; then
        echo "Running $SCRIPT"
        python "$SCRIPT"

        for table in $(clickhouse client --host clickhouse -q 'show tables'); do
            clickhouse client --host clickhouse -q "optimize table $table final"
        done
    fi
    echo "Now: $now, sleeping for 30 seconds"
    sleep 30
done
