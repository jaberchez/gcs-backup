# gcs-backup
This project contains an example to backup files to Google Cloud Storage in Go

## Configuration
The configuration is a Yaml file with the following structure:
```
# List of directories to copy
directories:
  - "/dir"
  - "/path/to/another/dir"

googleCloud:
  nameBucket: gcp-bucket-backups     # Name bucket where store the files
  pathJsonKey: "sa-buckups-key.json" # Path to Google service account key
```
