#!/bin/sh
BUCKET="${1:}" # s3://bucket_name/...

aws s3 ls --summarize --human-readable --recursive s3://"$BUCKET"/
