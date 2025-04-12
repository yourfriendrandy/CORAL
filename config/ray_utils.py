# config/ray_utils.py

import os
import ray
from time import time
from config.logger import (  
    log_step_progress,
    log_success,
)


def init_ray():
    ray.init(ignore_reinit_error=True, num_cpus=os.cpu_count() - 1)


def stream_ray_batches(
    chunked_data,
    remote_function,
    remote_args=(),
    store_callback=None,
    max_in_flight=1,
    log_prefix="Batch",
    port_every=None,
    port_callback=None
):
    """
    Streams Ray tasks with backpressure.

    - `chunked_data`: iterable of batchable data chunks
    - `remote_function`: a Ray remote function (do NOT wrap in lambda)
    - `remote_args`: shared args passed to every batch task
    - `store_callback`: function to consume results (e.g. DB insert)
    - `max_in_flight`: max Ray tasks simultaneously
    - `log_prefix`: prefix for logging progress
    - `port_every`: trigger port_callback every N batches
    - `port_callback`: function to call when porting is due
    """
    chunk_iter = iter(chunked_data)
    in_flight = []
    total_chunks = len(chunked_data)
    completed = 0
    t0 = time()

    # Initial task dispatch
    for _ in range(min(max_in_flight, total_chunks)):
        try:
            chunk = next(chunk_iter)
            obj_ref = remote_function.remote(chunk, *remote_args)
            in_flight.append(obj_ref)
        except StopIteration:
            break

    # Stream results
    while in_flight:
        done, in_flight = ray.wait(in_flight, num_returns=1)
        result = ray.get(done[0])

        if store_callback:
            store_callback(result)
        completed += 1

        if port_every and port_callback and (completed % port_every == 0):
            port_callback()

        log_step_progress(log_prefix, completed, total_chunks)

        try:
            chunk = next(chunk_iter)
            obj_ref = remote_function.remote(chunk, *remote_args)
            in_flight.append(obj_ref)
        except StopIteration:
            continue

    log_success(f"All {total_chunks} {log_prefix.lower()}s processed in {round(time() - t0, 2)}s.")


def run_ray_futures(
    chunked_data,
    remote_function,
    store_callback=None,
    log_prefix="Future Task"
):
    """
    Dispatches all tasks using Ray futures and optionally stores results after gathering.

    - `chunked_data`: list of data chunks
    - `remote_function`: Ray remote function to execute
    - `store_callback`: optional callback to store each result (e.g. DB insert)
    - `log_prefix`: status print prefix
    """
    total_chunks = len(chunked_data)
    t0 = time()

    # Launch all tasks in parallel
    futures = [remote_function.remote(chunk) for chunk in chunked_data]
    results = ray.get(futures)

    if store_callback:
        for i, result in enumerate(results, 1):
            store_callback(result)
            log_step_progress(log_prefix, i, total_chunks)

    log_success(f"All {total_chunks} {log_prefix.lower()}s complete in {round(time() - t0, 2)}s.")
    return results
