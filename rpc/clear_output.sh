#!/bin/bash

# Find and delete all files that start with "mr"
for file in mr-*.txt; do
  if [ -f "$file" ]; then
    rm "$file"
    echo "Deleted: $file"
  fi
done

echo "Output files cleared"
