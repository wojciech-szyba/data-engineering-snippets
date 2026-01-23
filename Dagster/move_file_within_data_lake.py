def move_file_within_s3(bucket, source_key, destination_key):
    logger = logging.getLogger()
    logger.setLevel("INFO")
    try:
        # Copy object to new location
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': source_key},
            Key=destination_key
        )

        # Delete original object
        s3_client.delete_object(
            Bucket=bucket,
            Key=source_key
        )
    except Exception as e:
        logger.error(f"Error moving file within S3: {e}")

