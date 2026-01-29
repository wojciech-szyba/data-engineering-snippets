import boto3

s3_client = boto3.client('s3')
s3_paginator = boto3.client('s3').get_paginator('list_objects_v2')

BUCKET_NAME = '...'
PREFIX = '...'


def keys(bucket_name, prefix='/', delimiter='/', start_after=''):
    cnt_glacier = 0
    cnt_non_glacier = 0
    prefix = prefix.lstrip(delimiter)
    start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
    for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
        for content in page.get('Contents', ()):
            if content['StorageClass'] == 'INTELLIGENT_TIERING':
                cnt_glacier += 1
                try:
                    response = s3_client.restore_object(
                        Bucket=bucket_name,
                        Key=content['Key'],
                        RestoreRequest={
                            'GlacierJobParameters': {
                                'Tier': 'Expedited',
                            },
                        },
                    )
                    print(response)
                except Exception as err:                    
                    print(str(err))

            else:
                cnt_non_glacier += 1

    print(prefix)
    print(f'Files archived in Glacier: {cnt_glacier}')
    print(f'Files standard: {cnt_non_glacier}')


keys(BUCKET_NAME, PREFIX)

