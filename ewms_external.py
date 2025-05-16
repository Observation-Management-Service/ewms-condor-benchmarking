import asyncio
import json
import logging
import os
import time
from pathlib import Path

from mqclient.queue import Queue
from rest_tools.client import SavedDeviceGrantAuth

LOGGER = logging.getLogger(__name__)


async def request_ewms(rc, work_duration_min, work_duration_max, fail_prob):
    LOGGER.info("Requesting single-task workflow to EWMS...")
    post_body = {
        "public_queue_aliases": ["input-queue", "output-queue"],
        "tasks": [
            {
                "cluster_locations": ["sub-2"],
                "in_queue_aliases": ["input-queue"],
                "out_queue_aliases": ["output-queue"],
                "task_image": os.environ["TASK_IMAGE"],
                "task_args": "",
                "n_workers": int(os.environ["EWMS_MAX_WORKERS"]),
                "worker_config": {
                    "condor_requirements": "",
                    "do_transfer_worker_stdouterr": True,
                    "max_worker_runtime": 60 * 60,
                    "n_cores": 1,
                    "priority": 100,
                    "worker_disk": "1GB",
                    "worker_memory": "512M",
                },
            }
        ],
    }
    resp = await rc.request("POST", "/v1/workflows", post_body)
    LOGGER.debug(json.dumps(resp))

    return (
        resp["workflow"]["workflow_id"],
        resp["task_directives"][0]["input_queues"][0],
        resp["task_directives"][0]["output_queues"][0],
    )


async def get_queues(rc, workflow_id, in_mqid, out_mqid):
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
    LOGGER.info(f"{mqprofiles=}")

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


async def serve_events(n_tasks, in_queue, out_queue):
    start = time.time()

    # pub
    async with in_queue.open_pub() as pub:
        for i in range(n_tasks):
            await pub.send({"n": i})
            print(f"Sent: #{i}")

    # sub
    async with out_queue.open_sub() as sub:
        i = 0
        async for msg in sub:
            i += 1
            print(f"Received: {i}")

    # done
    end = time.time()
    print(f"done: {start=} {end=} ({end - start})")


async def main(n_tasks, work_duration_min, work_duration_max, fail_prob):
    rc = SavedDeviceGrantAuth(
        args.ewms_url,
        token_url="https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
        filename=str(Path(f"~/ewms-device-refresh-token").expanduser().resolve()),
        client_id=f"{prefix}-public",  # ex: ewms-prod-public
        retries=0,
    )

    workflow_id, in_mqid, out_mqid = await request_ewms(rc, work_duration_min, work_duration_max, fail_prob)
    in_queue, out_queue = await get_queues(rc, workflow_id, in_mqid, out_mqid)
    await serve_events(n_tasks, in_queue, out_queue)


if __name__ == "__main__":
    asyncio.run(
        main(
            int(os.environ["N_TASKS_IN_BUNDLE"]),
            float(os.environ["WORK_DURATION_MIN"]),
            float(os.environ["WORK_DURATION_MAX"]),
            float(os.environ["FAIL_PROB"]),
        )
    )
