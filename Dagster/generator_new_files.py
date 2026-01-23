AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''


MAX_KEYS = ...


def get_files(bucket, prefix="", since_key=None, s3_session=None):
    if not s3_session:
        s3_session = boto3.resource("s3", use_ssl=True, verify=True, aws_access_key_id=AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY).meta.client
    contents = []

    response = s3_session.list_objects_v2(
        Bucket=bucket,
        Delimiter="",
        MaxKeys=MAX_KEYS,
        Prefix=prefix,
        StartAfter=since_key,
    )
    contents.extend(response.get("Contents", []))

    for obj in contents:
        yield obj["Key"]

