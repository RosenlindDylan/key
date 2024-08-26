#!/bin/bash

# get pids and kill processes related to mr_worker and mr_coordinator
ps aux | grep './mr_' | grep -v grep | awk '{print $2}' | xargs -r kill -9

if [ $? -eq 0 ]; then
    echo "Processes related to ./mr_* scripts have been terminated."
else
    echo "No processes related to ./mr_* scripts were found or failed to terminate."
fi
