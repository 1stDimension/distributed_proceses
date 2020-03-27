#!/usr/bin/env python3

from multiprocessing import Queue, JoinableQueue
from multiprocessing.managers import BaseManager
from typing import List, Tuple
import math as m
import argparse as ap


def load_matrix(filename: str) -> List[List[float]]:
    with open(filename, "r") as f:
        number_rows = int(f.readline())
        number_columns = int(f.readline())
        matrix = []
        for _ in range(number_rows):
            this = []
            for _ in range(number_columns):
                this.append(float(f.readline()))
            matrix.append(this)
    return matrix

def load_vector(filename: str) -> List[float]:
    with open(filename, "r") as f:
        number_rows = int(f.readline())
        number_columns = int(f.readline())
        matrix = []
        for _ in range(number_rows):
            this = float(f.readline())
            matrix.append(this)
    return matrix

def matrix_to_baches(
    matrix: List[List[float]], batch_count: int
) -> List[Tuple[int, int, List[List[float]]]]:

    batch_size = m.ceil(len(matrix) / batch_count)
    batches = []
    last_index: int = 0
    for i in range(batch_count):
        begin = last_index
        if last_index + batch_size > len(matrix):
            end = len(matrix)
        else:
            end = last_index + batch_size
        entry = (begin, end, matrix[begin:end])
        batches.append(entry)
        last_index = end
    return batches


DEFAULT_PORT = 9900

parser = ap.ArgumentParser(description="Client for repporting jobs to server and gather results")
parser.add_argument(
    "-p",
    "--port",
    type=int,
    help=f"Port number to connect to. Default is {DEFAULT_PORT}.",
    default=DEFAULT_PORT,
)
parser.add_argument("ip", type=str, help="IP client is going to connect to")
parser.add_argument(
    "password",
    type=lambda x: x.encode(),
    help="Password to authenticate myself to server.",
)
parser.add_argument("batch_count", type=int, help="Into how many batches matrix will be divided")
parser.add_argument("matrix_file", type=str, help="File from where to load matrix")
parser.add_argument("vector_file", type=str, help="File from where to load vector")
arguments = parser.parse_args()

PORT = arguments.port
PASS = arguments.password
IP = arguments.ip
BACH_COUNT = arguments.batch_count
MATRIX_FILENAME = arguments.matrix_file
VECTOR_FILENAME = arguments.vector_file

local_matrix = load_matrix(MATRIX_FILENAME)
local_vector = load_vector(VECTOR_FILENAME)

# print("MATRIX:")
# print(matrix)
# print("VECTOR:")
# print(vector)

BaseManager.register("get_job_queue")
BaseManager.register("get_vector")
BaseManager.register("get_result_queue")

manager = BaseManager(address=(IP, PORT), authkey=PASS)
manager.connect()

job_queue = manager.get_job_queue()
vector = manager.get_vector()
result_queue = manager.get_result_queue()

vector.clear()
vector.extend(local_vector)
batches = matrix_to_baches(local_matrix, BACH_COUNT)
print("Baches = :")
for i in batches:
  print(f"Begin = {i[0]}, end = {i[1]}")
print(f"Length of matrix {len(local_matrix)}")
for job in batches:
  job_queue.put(job)
print("Waiting for workers to do their work")
job_queue.join()
print("Job is done")
answer = [0] * len(local_matrix)
while not result_queue.empty():
  partial_result = result_queue.get()
  index = partial_result[0]
  entry = partial_result[1]
  answer[index] = entry

print("Resulting vector is:")
print(answer)