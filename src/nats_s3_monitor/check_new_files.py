from datetime import datetime
import logging

from nats.js.client import JetStreamContext
from pyapi_service_kit.nats import NatsPayload

from .config import Config

LOGGER = logging.getLogger(__name__)


async def try_report_new_files(t: datetime, js: JetStreamContext):
    config = Config()
    s3_client = config.s3_config.client()

    for monitor in config.monitors_config.monitors:
        cache = monitor._cache
        did_update = await cache._update_cache_if_stale(t, monitor, s3_client)
        if not did_update:
            continue

        is_new, latest_file = cache.try_get_latest_file(t)
        if not is_new:
            continue

        msg = NatsPayload(
            type="json",
            data={
                "epoch_ms": int(t.timestamp() * 1000),
                "latest_file": latest_file,
                "bucket": monitor.bucket,
            },
        )

        LOGGER.info(f"Publish[{monitor.output_subject}] => {msg}")
        await js.publish(monitor.output_subject, msg.as_bytes())
