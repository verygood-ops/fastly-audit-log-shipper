import io

from botocore.exceptions import ClientError

from fastly_audit_log_shipper import s3, logger


def get_offset(customer_id, bucket, prefix):
    """Retrieve offset for audit log from S3.

    :param customer_id: A customer ID to use for storing log file
    :type customer_id: str
    :param bucket: S3 bucket to use for storing offsets.
    :type bucket: str
    :param prefix: S3 prefix to use for storing offset.
    :type bucket: str
    """
    sio = io.BytesIO()
    try:
        s3.download_fileobj(bucket, '/'.join([prefix, customer_id, 'offsets/offset.txt']), sio)
    except ClientError:
        logger.exception('No offsets file was created, initializing empty offsets')
        ret = 1
    else:
        sio.seek(0)
        ret = int(sio.read().strip())
    return ret


def update_offset(offset, customer_id, bucket, prefix):
    """Retrieve offset for audit log to S3 file.

    :param offset: An offset value to write
    :type offset: str
    :param customer_id: A customer ID to use for storing log file
    :type customer_id: str
    :param bucket: S3 bucket to use for storing offsets.
    :type bucket: str
    :param prefix: S3 prefix to use for storing offset.
    :type bucket: str
    """
    tio = io.BytesIO()
    tio.write(str(offset).encode('utf-8'))
    tio.seek(0)
    try:
        s3.upload_fileobj(tio, bucket, '/'.join([prefix, customer_id, 'offsets/offset.txt']))
    except ClientError:
        logger.exception('Can not update offsets file')
        ret = False
    else:
        logger.warning(f'Wrote offset == {offset}')
        ret = True
    return ret
