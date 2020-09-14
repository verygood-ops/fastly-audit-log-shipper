import logging
import boto3


logger = logging.getLogger(__name__)
s3 = boto3.client('s3')
fastly_api_url = 'https://api.fastly.com/events' \
                 '?filter[customer_id]={CUSTOMER_ID}&page[number]={OFFSET}&page[size]={SIZE}'
