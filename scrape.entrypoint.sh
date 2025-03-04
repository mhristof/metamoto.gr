#!/usr/bin/env bash

set -euo pipefail

SCRIPT=${1:-}

if [[ -z $SCRIPT ]]; then
    echo "Usage: $0 <script>"
    exit 1
fi

while true; do

    # Calculate how many seconds to sleep until next 04:00 in Europe/Athens time.
    now_sec=$(TZ="Europe/Athens" date +%s)
    today4=$(TZ="Europe/Athens" date -d "04:00" +%s)
    if [ "$now_sec" -lt "$today4" ]; then
        next_run=$today4
    else
        next_run=$(TZ="Europe/Athens" date -d "tomorrow 04:00" +%s)
    fi
    sleep_seconds=$((next_run - now_sec))
    echo "Now: $(TZ='Europe/Athens' date +%H%M), sleeping for $sleep_seconds seconds until next run at 04:00"
    sleep "$sleep_seconds"

    echo "Running $SCRIPT"
    python "$SCRIPT"

    for table in $(clickhouse client --host clickhouse -q 'show tables'); do
        clickhouse client --host clickhouse -q "optimize table $table final"
    done
done
