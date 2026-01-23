UNLOAD ('SELECT * FROM table_name')
TO 's3://<bucket_name>/'
IAM_ROLE 'arn:aws:iam::.....'
CSV
HEADER
PARALLEL OFF;
