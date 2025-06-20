"""Print all the event messages from an ewms workflow out-queue."""

import argparse
import asyncio
import json
import logging
from pathlib import Path

from mqclient.queue import Queue
from rest_tools.client import RestClient, SavedDeviceGrantAuth

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def get_output_queue(rc: RestClient, workflow_id: str) -> Queue:
    """Retrieve the output queue object."""
    LOGGER.info("getting queues...")
    mqprofiles: list[dict] = []
    while not (mqprofiles and all(m["is_activated"] for m in mqprofiles)):
        # (re)try until queues are activated
        await asyncio.sleep(10)
        mqprofiles = (
            await rc.request(
                "GET",
                f"/v1/mqs/workflows/{workflow_id}/mq-profiles/public",
            )
        )["mqprofiles"]
    LOGGER.info(json.dumps(mqprofiles, indent=4))

    out_mqprofile = next(p for p in mqprofiles if p["alias"] == "output-queue")
    out_queue = Queue(
        out_mqprofile["broker_type"],
        address=out_mqprofile["broker_address"],
        name=out_mqprofile["mqid"],
        auth_token=out_mqprofile["auth_token"],
    )

    return out_queue


async def sub_events(queue: Queue) -> int:
    """Retrieve 'n_tasks' number of events (tasks)."""
    n = -1

    async with queue.open_sub() as sub:
        async for msg in sub:
            n += 1
            print(f"#{n}: {msg}")

    return n + 1


async def main():
    """Main."""
    parser = argparse.ArgumentParser(
        description="Print all the event messages from an ewms workflow out-queue."
    )
    parser.add_argument(
        "workflow_id",
        required=True,
        help="the ewms workflow id",
    )
    args = parser.parse_args()
    LOGGER.info(args)

    rc = SavedDeviceGrantAuth(
        "https://ewms-dev.icecube.aq",
        token_url="https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
        filename=str(Path("~/ewms-dev-device-refresh-token").expanduser().resolve()),
        client_id="ewms-dev-public",
        retries=0,
    )

    queue = await get_output_queue(rc, args.workflow_id)
    n_recvd = await sub_events(queue)
    LOGGER.info(f"got {n_recvd} event messages: {queue}")


if __name__ == "__main__":
    asyncio.run(main())
    LOGGER.info("Done.")
