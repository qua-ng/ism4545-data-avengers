# Data Download Instructions

The MediaWave Streaming datasets are hosted in the course data repository. They are **not committed to this repo** because they total ~150 MB.

## Download

Run from the project root:

```
# TODO: Juan confirms exact download commands once verified.
# Datasets live at:
# https://github.com/prof-tcsmith/ism6562s26-class/tree/main/final-projects/data/10-mediawave-streaming/

# Example (one file):
# curl -L -o data/user-profiles.csv.gz \\
#   https://raw.githubusercontent.com/prof-tcsmith/ism6562s26-class/main/final-projects/data/10-mediawave-streaming/user-profiles.csv.gz
```

## Files

After download, this folder should contain:

- user-profiles.csv.gz
- viewing-history.csv.gz
- content-catalog.json.gz
- user-interactions.json.gz
- streaming-quality.csv.gz
- generate_data.py (reference only - do not execute)

These files are excluded from git via .gitignore.
