import argparse
import io
import json
import os
import time

import fastly_audit_log_shipper
import fastly_audit_log_shipper.audit_log
import fastly_audit_log_shipper.s3_offsets


arg_parser = argparse.ArgumentParser('Fastly Audit Log Shipper')
arg_parser.add_argument('--log-bucket', help='Log bucket to use', required=True)
arg_parser.add_argument('--log-prefix', default='fastly-cdn-audit')
arg_parser.add_argument('--fastly-token', default=os.environ.get('FASTLY_TOKEN'))
arg_parser.add_argument('--customer-id', default=os.environ.get('FASTLY_CUSTOMER'))
arg_parser.add_argument('--entries-per-scrape', type=int, default=10)
arg_parser.add_argument('--entries-per-file', type=int, default=100)


def main():
    """Fastly Audit Log retrieval.

    Until there is new audit log data,
        0. Retrieve current offset for customer id
        1. Retrieve Fastly audit log data for customer id and offset
        2. Write Fastly audit log data into S3
        3. Write updated offset for customer id into S3
    """
    args = arg_parser.parse_args()

    while True:
        written_entries = 0
        contents = []
        dio = io.BytesIO()

        offset = fastly_audit_log_shipper.s3_offsets.get_offset(
            args.customer_id,
            args.log_bucket,
            args.log_prefix,
        )
        contents += fastly_audit_log_shipper.audit_log.retrieve_fastly_data(
            offset,
            args.fastly_token,
            args.customer_id,
            args.entries_per_scrape,
        )

        if contents:
            for audit_log_entry in contents:
                dio.write(json.dumps(audit_log_entry).encode('utf-8'))
                fastly_audit_log_shipper.logger.warning(f'Written audit log evt {audit_log_entry["id"]}')
                dio.write(os.linesep.encode('utf-8'))
                written_entries += 1

        else:
            break

        if written_entries:

            dio.seek(0)
            while not fastly_audit_log_shipper.audit_log.write_s3_data(
                dio,
                args.log_bucket,
                args.log_prefix,
                args.customer_id,
            ):

                fastly_audit_log_shipper.logger.error('Can not write audit log data to destination, will retry')
                time.sleep(5)

        else:
            break

        full_page = int(written_entries == args.entries_per_scrape)

        while not fastly_audit_log_shipper.s3_offsets.update_offset(
            offset + full_page,
            args.customer_id,
            args.log_bucket,
            args.log_prefix,
        ):

            fastly_audit_log_shipper.logger.error('Can not update offsets, will retry')
            time.sleep(5)

        if not full_page:
            break


if __name__ == '__main__':
    main()
