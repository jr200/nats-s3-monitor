# nats-s3-monitor

This service:
  - receives a timestamp `epoch_ms` published from nats-time-server.
  - finds the latest file in the s3 bucket as-of `epoch_ms`.
  - publishes the details of any new file it encounters.

_Example use-case..._

Some other service:
  - ingests periodic payloads from a data source (e.g., a web-scrape)
  - saves payloads to an s3 bucket such that:
    - the filename is timestamped according to a convention
    - e.g.: `scrape-id/%Y%m%d/%Y%m%d_%H%M%S.jsonl.gz`

# Notes

- DR/what happens when the data source has an outage
