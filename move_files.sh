#!/bin/bash

# Define the top-level directory
TOP_DIR=$(pwd)

# Recursively find all .xlsx files in subdirectories
find "$TOP_DIR" -mindepth 2 -type f -name "*.xls" | while read -r file; do
  # Get the base name of the file
  base_name=$(basename "$file")
  
  # Define the target path in the top-level directory
  target_path="$TOP_DIR/$base_name"
  
  # Check if a file with the same name already exists
  if [[ -e "$target_path" ]]; then
    # Append a unique identifier (e.g., timestamp + random number) to avoid overwriting
    unique_suffix=$(date +%s%N) # Combines timestamp with nanoseconds for uniqueness
    target_path="$TOP_DIR/${unique_suffix}_$base_name"
  fi
  
  # Move the file to the top-level directory
  mv "$file" "$target_path"
done

echo "All .xlsx files have been moved to the top-level directory without overwriting."
