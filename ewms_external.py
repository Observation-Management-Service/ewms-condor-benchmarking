"""The external task-requesting & event-serving script for an ewms workflow."""

import argparse
import asyncio
import json
import logging
from pathlib import Path

from mqclient.queue import Queue
from rest_tools.client import RestClient, SavedDeviceGrantAuth

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def request_ewms(rc: RestClient, ewms_request_json: Path):
    """Request an ewms workflow from the json file."""
    LOGGER.info(f"Requesting single-task workflow to EWMS ({ewms_request_json})...")

    with open(ewms_request_json) as f:
        post_body = json.load(f)
    LOGGER.info(json.dumps(post_body, indent=4))

    resp = await rc.request("POST", "/v1/workflows", post_body)
    LOGGER.info(json.dumps(resp, indent=4))

    return (
        resp["workflow"]["workflow_id"],
        resp["task_directives"][0]["input_queues"][0],
        resp["task_directives"][0]["output_queues"][0],
    )


async def get_input_queue(
    rc: RestClient,
    workflow_id: str,
    in_mqid: str,
) -> Queue:
    """Retrieve the input queue object."""
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

    return in_queue


async def serve_events(n_tasks: int, in_queue: Queue) -> int:
    """Serve 'n_tasks' number of events (tasks)."""
    n = -1

    async with in_queue.open_pub() as pub:
        for n in range(n_tasks):
            await pub.send(str(n))  # str & bytes message don't incur json cost on pilot
            # inflight.append(n)
            LOGGER.info(f"Sent: #{n}")

    return n + 1


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
    parser.add_argument(
        "--n-tasks",
        required=True,
        type=int,
        help="Total number of tasks to generate",
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

    # request
    workflow_id, in_mqid, out_mqid = await request_ewms(rc, args.request_json)

    # load queue
    in_queue = await get_input_queue(rc, workflow_id, in_mqid)
    nsent = await serve_events(args.n_tasks, in_queue)
    LOGGER.info(f"done sending {nsent} event messages: {in_queue}")
    LOGGER.info(f"OPTIONAL: to receive output event messages use queue: {out_mqid}")


if __name__ == "__main__":
    asyncio.run(main())
    LOGGER.info("Done.")
