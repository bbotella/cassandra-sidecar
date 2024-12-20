#!/bin/bash

search_dir="."

target_folder="${1}"

mkdir -p $target_folder

find "$search_dir" -type d -path "*/build/jacoco" -print0 | while IFS= read -r -d '' dir; do
  destination_path="$target_folder/${dir:2}"
  mkdir -p "$destination_path"
  cp -r $dir "$destination_path"
done

