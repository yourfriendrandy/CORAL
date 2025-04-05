# config/ray_utils.py

import ray
import os
from time import time

def init_ray():
    ray.init(ignore_reinit_error=True, num_cpus=os.cpu_count() - 1)

def stream_ray_batches(
    chunked_data,
    remote_function,
    store_callback,
    max_in_flight=4,
    log_prefix="Batch",
    port_every=None,
    port_callback=None
):
    """
    Streams Ray tasks with backpressure.

    - `chunked_data`: iterable of batchable data chunks
    - `remote_function`: a Ray remote function to execute on each chunk
    - `store_callback`: a function that consumes each result (e.g. stores to DB)
    - `max_in_flight`: number of Ray tasks allowed simultaneously
    - `log_prefix`: optional log prefix per chunk
    - `port_every`: how often to trigger port_callback (in completed batches)
    - `port_callback`: optional function to call when port_every is hit
    """
    chunk_iter = iter(chunked_data)
    in_flight = []
    total_chunks = len(chunked_data)
    completed = 0
    t0 = time()

   
    # Initial tasks
    for _ in range(min(max_in_flight, total_chunks)):
        try:
            chunk = next(chunk_iter)
            in_flight.append(remote_function(chunk))  # ✅ this
        except StopIteration:
            break

    # Stream results
    while in_flight:
        done, in_flight = ray.wait(in_flight, num_returns=1)
        result = ray.get(done[0])
        store_callback(result)
        completed += 1

        if port_every and port_callback:
            if completed % port_every == 0:
                port_callback()

        print(f"✅ {log_prefix} {completed}/{total_chunks}")

        try:
            chunk = next(chunk_iter)
            in_flight.append(remote_function(chunk))
        except StopIteration:
            pass

    print(f"🎉 All {total_chunks} {log_prefix.lower()}s processed in {round(time() - t0, 2)}s.")
