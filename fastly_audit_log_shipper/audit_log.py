import datetime
import requests

from botocore.exceptions import ClientError

from fastly_audit_log_shipper import s3, logger, fastly_api_url


def retrieve_fastly_data(page, token, customer_id, page_size=10):
    """Retrieve audit log data from Fastly.

    :param page: A page of audit log to retrieve.
    :type page: int
    :param token: A Fastly CDN token to use
    :type token: str
    :param customer_id: A customer ID to fetch audit log for
    :type customer_id: str
    :param page_size: Page size for each request
    :type page_size: int

    :return: A list of Fastly audit log contents.
    """
    return requests.get(
        fastly_api_url.format(
            CUSTOMER_ID=customer_id,
            SIZE=page_size,
            OFFSET=page,
        ),
        headers={
            'User-Agent': 'fastly-audit-log-shipper',
            'Fastly-Key': token,
        }
    ).json().get('data')


def write_s3_data(fio, bucket, prefix, customer_id):
    """Write audit log data to remote S3 storage

    :param fio: File object which contains audit log data
    :param bucket: A bucket to use for uploading audit log
    :param prefix: S3 prefix to use for uploading audit log
    :param customer_id: A customer ID to use for prefixing log object

    :returns: True if data written successfully, False otherwise.
    """
    o_name = '/'.join(
        [
            prefix,
            customer_id,
            f'{datetime.datetime.isoformat(datetime.datetime.utcnow())}-fastly-audit-log-${customer_id}.log',
        ]
    )
    try:
        s3.upload_fileobj(fio, bucket, o_name)
    except ClientError:
        logger.exception('Failed to upload file')
        return False
    return True
