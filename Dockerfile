FROM python:3.8.5-slim-buster

COPY requirements.txt /opt/requirements.txt
RUN python -m pip install -r /opt/requirements.txt
ENV PYTHONPATH="/opt/"
COPY fastly_audit_log_shipper /opt/fastly_audit_log_shipper

ENTRYPOINT ["python3", "-m", "fastly_audit_log_shipper"]
