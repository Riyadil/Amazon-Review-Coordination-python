#!/usr/bin/env bash
# Download the four Amazon Reviews'23 categories for the CSC 7740 project
cd "$(dirname "$0")"
B=https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories
CATS="Video_Games CDs_and_Vinyl Arts_Crafts_and_Sewing Baby_Products"
echo "=== started $(date) ==="
for c in $CATS; do
  echo
  echo "==> $c"
  curl -L --retry 3 --retry-delay 3 -C - -o "$c.jsonl.gz" "$B/$c.jsonl.gz" \
       --progress-bar --write-out "    done: %{size_download} bytes at %{speed_download} B/s\n"
done
echo
echo "=== finished $(date) ==="
ls -lh ./*.jsonl.gz
echo
echo "total: $(du -ch ./*.jsonl.gz | tail -1 | cut -f1)"
echo
echo "DOWNLOAD_COMPLETE - press q or Ctrl-b then d to detach"
