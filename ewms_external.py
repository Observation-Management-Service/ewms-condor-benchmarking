#!/usr/bin/env python3
"""The external task-requesting & event-serving script for an ewms workflow."""

import argparse
import asyncio
import json
import logging
import time
from pathlib import Path

from mqclient.queue import Queue
from rest_tools.client import RestClient, SavedDeviceGrantAuth

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


async def request_ewms(rc: RestClient, ewms_request_json: Path):
    """Request an ewms workflow from the json file."""
    LOGGER.info(f"Requesting single-task workflow to EWMS ({ewms_request_json})...")

    with open(ewms_request_json) as f:
        post_body = json.load(f)
    LOGGER.debug(json.dumps(post_body, indent=4))

    resp = await rc.request("POST", "/v1/workflows", post_body)
    LOGGER.debug(json.dumps(resp, indent=4))

    return (
        resp["workflow"]["workflow_id"],
        resp["task_directives"][0]["input_queues"][0],
        resp["task_directives"][0]["output_queues"][0],
    )


async def get_queues(
    rc: RestClient,
    workflow_id: str,
    in_mqid: str,
    out_mqid: str,
) -> tuple[Queue, Queue]:
    """Retrieve the queue objects."""
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

    in_mqprofile = next(p for p in mqprofiles if p["mqid"] == in_mqid)
    in_queue = Queue(
        in_mqprofile["broker_type"],
        address=in_mqprofile["broker_address"],
        name=in_mqprofile["mqid"],
        auth_token=in_mqprofile["auth_token"],
    )

    out_mqprofile = next(p for p in mqprofiles if p["mqid"] == out_mqid)
    out_queue = Queue(
        out_mqprofile["broker_type"],
        address=out_mqprofile["broker_address"],
        name=out_mqprofile["mqid"],
        auth_token=out_mqprofile["auth_token"],
        timeout=4 * 60 * 60,  # 4 hours
    )

    return in_queue, out_queue


async def serve_events(n_tasks: int, in_queue: Queue, out_queue: Queue):
    """Serve 'n_tasks' number of events (tasks) and wait to receive all return events."""
    start = time.time()

    inflight: list[int] = []

    # 1st: pub -- include the "n" integer so later we can track it
    async with in_queue.open_pub() as pub:
        for n in range(n_tasks):
            await pub.send({"n": n})
            inflight.append(n)
            LOGGER.info(f"Sent: #{n}")

    # 2nd: sub
    async def sub_time():
        # receive all the we can -- there should be 1+ return messages (same "n")
        async with out_queue.open_sub() as sub:
            ct = -1
            async for msg in sub:
                ct += 1
                LOGGER.info(f"Received: #{ct} ({msg})")
                try:
                    inflight.remove(msg["n"])
                except ValueError:
                    LOGGER.info(
                        "ok: received duplicate (ewms does not guarantee deliver-once)"
                    )
                if not inflight:
                    LOGGER.info(f"RECEIVED ALL EXPECTED MESSAGES")
                    return

    await sub_time()

    # done
    end = time.time()
    LOGGER.info(f"done: {start=} {end=} ({end - start})")


async def main():
    """Main."""
    parser = argparse.ArgumentParser(
        description="Submit EWMS task workflow and collect results."
    )
    parser.add_argument(
        "--request-json",
        required=True,
        type=Path,
        help="JSON file containing the ewms workflow request",
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

    workflow_id, in_mqid, out_mqid = await request_ewms(rc, args.request_json)
    in_queue, out_queue = await get_queues(rc, workflow_id, in_mqid, out_mqid)
    await serve_events(args.n_tasks, in_queue, out_queue)


if __name__ == "__main__":
    asyncio.run(main())
    LOGGER.info("Done.")
