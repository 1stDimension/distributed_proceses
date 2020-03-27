#!/usr/bin/env python3
from multiprocessing import Queue, JoinableQueue
from multiprocessing.managers import BaseManager

import argparse as ap

DEFAULT_PORT = 9900

parser = ap.ArgumentParser(description="Server for managing distributed processes.")
parser.add_argument(
    "-p",
    "--port",
    type=int,
    help=f"Port number server will bind to. Default is {DEFAULT_PORT}.",
    default=DEFAULT_PORT
)
parser.add_argument(
    "password",
    type=lambda x: x.encode(),
    help="Password used by clients to authenticate themselves.",
)
arguments = parser.parse_args()

job_queue = JoinableQueue()
result_queue = Queue()
vector = []

PORT = arguments.port
PASS = arguments.password


BaseManager.register("get_job_queue", callable=lambda: job_queue)
BaseManager.register("get_vector", callable=lambda: vector)
BaseManager.register("get_result_queue", callable=lambda: result_queue)

manager = BaseManager(address=("", PORT), authkey=PASS)
server = manager.get_server()

print(f"Server will be started on port {PORT} in order to connect use password {PASS}")

server.serve_forever()
