import asyncio
import logging
from importlib.resources import files
from typing import List

from pyapi_service_kit.nats import make_nats_client, create_jetstream_streams
from pyapi_service_kit.service import mark_service_ready, unmark_service_ready
from pyapi_service_kit.utils import (
    create_stop_event,
    initialise_logging,
    parse_args,
)

from .config import Config
from .register_tasks import register_tasks

LOGGER = logging.getLogger(__name__)


async def _start_server() -> None:
    config = Config()
    tasks: List[asyncio.Task] = []

    nc = None

    try:
        nc, js = await make_nats_client(
            config.nats_config.servers,
            config.nats_config.options,
        )
        await create_jetstream_streams(js, config.nats_config.streams)

        await register_tasks(nc, tasks)

        # Loop until the stop event is set
        stop = create_stop_event()
        LOGGER.info("Server started...")
        mark_service_ready()
        await stop

        # Clean up all tasks
        for i, task in enumerate(tasks):
            if task:
                LOGGER.info(
                    f"  -- Cancelling task {i + 1}/{len(tasks)}: {task.get_name()}"
                )
                task.cancel()

        # Wait for all tasks to finish
        await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as e:
        LOGGER.error(f"Error occurred: {e}")
    finally:
        LOGGER.info("\nShutting down gracefully...")
        if nc:
            await nc.close()
        unmark_service_ready()


def main():
    default_config_file = str(files(__package__).joinpath("./config.yaml"))
    default_logging_file = str(files(__package__).joinpath("./logging.yaml"))

    args = parse_args(default_config_file, default_logging_file)
    initialise_logging(args.CONFIG_LOG)
    Config.from_yaml(args.CONFIG_FILE)

    try:
        asyncio.run(_start_server())
    except KeyboardInterrupt:
        LOGGER.info("\nReceived shutdown signal...")


if __name__ == "__main__":
    main()
