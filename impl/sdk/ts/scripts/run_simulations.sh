#!/bin/bash

LOGFILE="failed_seeds.log"
> "$LOGFILE"  # clear file at start

for i in $(seq 1 100); do
    SEED=$RANDOM
    bun sim/main.ts --seed "$SEED"
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 1 ]; then
        echo "$SEED" >> "$LOGFILE"
        echo "Seed $SEED failed (exit code 1)"
    fi
done

echo "Done. Failed seeds saved to $LOGFILE"
