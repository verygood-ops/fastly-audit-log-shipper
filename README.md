# fastly-audit-log-shipper

### How it works ?

Fastly Audit Log Shipper is an utility to replicate Fastly audit logs into S3-compatible storage.
It achieves full log replication by keeping offset of last retrieved Fastly audit log page in S3.
Designed to use as a Kubernetes CronJob.

### How to use

- find the S3 bucket `<s3_bucket_name>` suitable for storing audit logs
- create Fastly API token value `<fastly_token>` as described at https://docs.fastly.com/en/guides/using-api-tokens
- retrieve `<customer_id>` value from https://manage.fastly.com/account/company
- invoke log shipper module

```
python3 -m fastly_audit_log_shipper --fastly-token='<fastly_token>' \
--log-bucket=<s3_bucket_name> --customer-id=<customer_id>
```

