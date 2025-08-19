import asyncio
import json
from datetime import datetime
from typing import List

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.js.client import JetStreamContext
import pytz

from pyapi_service_kit.nats import subscribe_task

from .config import Config
from .check_new_files import try_report_new_files


async def process_msg(msg: Msg, js: JetStreamContext):
    payload = json.loads(msg.data)
    epoch_ms: int = payload["data"]
    epoch_s = epoch_ms / 1000
    datetime_utc = datetime.fromtimestamp(epoch_s, tz=pytz.UTC)

    await try_report_new_files(datetime_utc, js)


async def register_tasks(nc: NATS, tasks: List[asyncio.Task]):
    config = Config()
    js: JetStreamContext = nc.jetstream()

    async def cb(msg: Msg):
        await process_msg(msg, js)

    tasks.append(
        asyncio.create_task(
            subscribe_task(
                nc,
                config.service_config.time_service_subject,
                cb=cb,
            )
        )
    )
