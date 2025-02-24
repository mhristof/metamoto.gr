#!/usr/bin/env bash

set -euo pipefail

echo "Starting scrape.entrypoint.sh"

# run every day at 4am Europe/Athens time
while true; do
    now=$(TZ="Europe/Athens" date +%H%M)
    if [[ "$now" == "0400" ]]; then
        echo "Running scrape.sh"
        find /app/scrape -name '*.py' -exec python {} \;
        for table in $(clickhouse client --host clickhouse -q 'show tables'); do
            clickhouse client --host clickhouse -q "optimize table $table final"
        done
    fi
    echo "Now: $now, sleeping for 30 seconds"
    sleep 30
done
