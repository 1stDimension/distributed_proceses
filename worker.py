#!/usr/bin/env python3
from multiprocessing import Queue, JoinableQueue, Pool
from multiprocessing.managers import BaseManager
from typing import List, Tuple
import math as m
import argparse as ap

DEFAULT_PORT = 9900

parser = ap.ArgumentParser(description="Worker for doing the actual computation")
parser.add_argument(
    "-p",
    "--port",
    type=int,
    help=f"Port number server will bind to. Default is {DEFAULT_PORT}.",
    default=DEFAULT_PORT,
)
parser.add_argument("ip", type=str, help="IP client is going to connect to")
parser.add_argument(
    "password",
    type=lambda x: x.encode(),
    help="Password to authenticate myself to server.",
)
parser.add_argument(
    "--process",
    type=int,
    help="How many sub processes will be used. Default is 4",
    default=4,
    dest="process",
)
parser.add_argument('-d', action='store_true', dest="if_daemon", help="If added worker runs as daemon ")
arguments = parser.parse_args()

PORT = arguments.port
PASS = arguments.password
IP = arguments.ip
PROCESS_COUNT = arguments.process
IF_DEMON = arguments.if_daemon

BaseManager.register("get_job_queue")
BaseManager.register("get_vector")
BaseManager.register("get_result_queue")

manager = BaseManager(address=(IP, PORT), authkey=PASS)
manager.connect()

JOB_QUEUE = manager.get_job_queue()
VECTOR = manager.get_vector().copy()
RESULT_QUEUE = manager.get_result_queue()


def vector_x_vector(a: list, b: list) -> float:
    sum: float = 0.0
    for i, j in zip(a, b):
        sum += i * j
    # print(f"sum = {sum}")
    return sum


def do_job(task) -> list:
    print(f"Type of job = {task[0]}")
    # print(f"Type of VECTOR = {type(VECTOR)}")
    index = task[0]
    row = task[1]
    part = vector_x_vector(row, VECTOR)
    answer = (index, part)
    return answer


with Pool(PROCESS_COUNT) as pool:
    while not JOB_QUEUE.empty() or IF_DEMON:
        # print(f"Size before get = {JOB_QUEUE.qsize()}")
        job = JOB_QUEUE.get()
        begin = job[0]
        end = job[1]
        done = pool.map(do_job, zip(range(begin, end), job[2] ))
        for each in done:
            print(f"Done = {each[0]}")
            RESULT_QUEUE.put(each)
        JOB_QUEUE.task_done()

size = JOB_QUEUE.qsize()
# print(f"Queue size before exit = {size}")
# print(f"Is Queue empty = {JOB_QUEUE.empty()}")
print("I've played my part")
