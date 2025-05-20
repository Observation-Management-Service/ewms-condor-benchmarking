#!/usr/bin/env python3

import argparse
import asyncio
import json
import logging
import time
from pathlib import Path

from mqclient.queue import Queue
from rest_tools.client import SavedDeviceGrantAuth

LOGGER = logging.getLogger(__name__)


async def request_ewms(
    rc,
    task_image,
    task_runtime,
    fail_prob,
    do_task_runtime_poisson,
    do_worker_speed_factor,
    ewms_workers,
):
    LOGGER.info("Requesting single-task workflow to EWMS...")
    post_body = {
        "public_queue_aliases": ["input-queue", "output-queue"],
        "tasks": [
            {
                "cluster_locations": ["sub-2"],
                "in_queue_aliases": ["input-queue"],
                "out_queue_aliases": ["output-queue"],
                "task_image": task_image,
                "task_args": "",
                "task_env": {
                    "TASK_RUNTIME": str(task_runtime),
                    "FAIL_PROB": str(fail_prob),
                    "DO_TASK_RUNTIME_POISSON": str(do_task_runtime_poisson).lower(),
                    "DO_WORKER_SPEED_FACTOR": str(do_worker_speed_factor).lower(),
                },
                "n_workers": ewms_workers,
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
    """Serve 'n_tasks' number of events (tasks) and wait to receive all return events."""
    start = time.time()

    inflight: list[int] = []

    # 1st: pub -- include the "n" integer so later we can track it
    async with in_queue.open_pub() as pub:
        for n in range(n_tasks):
            await pub.send({"n": n})
            inflight.append(n)
            print(f"Sent: #{n}")

    # 2nd: sub
    async def sub_time():
        # receive all the we can -- there should be 1+ return messages (same "n")
        async with out_queue.open_sub() as sub:
            ct = -1
            async for msg in sub:
                ct += 1
                print(f"Received: #{ct} ({msg})")
                try:
                    inflight.remove(msg["n"])
                except ValueError:
                    print(
                        "ok: received duplicate (ewms does not guarantee deliver-once)"
                    )
                if not inflight:
                    print(f"RECEIVED ALL EXPECTED MESSAGES")
                    return

    await sub_time()

    # done
    end = time.time()
    print(f"done: {start=} {end=} ({end - start})")


async def main():
    """Main."""
    parser = argparse.ArgumentParser(
        description="Submit EWMS task workflow and collect results."
    )
    parser.add_argument(
        "--task-image",
        required=True,
        help="Container image to run",
    )
    parser.add_argument(
        "--task-runtime",
        type=int,
        required=True,
        help="Average task runtime (seconds)",
    )
    parser.add_argument(
        "--fail-prob",
        type=float,
        required=True,
        help="Probability of simulated failure (0.0–1.0)",
    )
    parser.add_argument(
        "--do-task-runtime-poisson",
        action="store_true",
        help="Use Poisson-distributed task duration",
    )
    parser.add_argument(
        "--do-worker-speed-factor",
        action="store_true",
        help="Use per-worker Gaussian speed factor",
    )
    parser.add_argument(
        "--ewms-workers",
        type=int,
        required=True,
        help="Number of EWMS workers to deploy",
    )
    parser.add_argument(
        "--n-tasks",
        type=int,
        required=True,
        help="Number of events to publish",
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

    workflow_id, in_mqid, out_mqid = await request_ewms(
        rc,
        args.task_image,
        args.task_runtime,
        args.fail_prob,
        args.do_task_runtime_poisson,
        args.do_worker_speed_factor,
        args.ewms_workers,
    )
    in_queue, out_queue = await get_queues(rc, workflow_id, in_mqid, out_mqid)
    await serve_events(args.n_tasks, in_queue, out_queue)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
