import os
import random
import sqlite3
import string
import sys
import time
from datetime import datetime

from ulid import ULID
import roundrobin

BUFFER_SIZE: int = 5000
MAX_EPS: int = 500

DATABASE: str = "queue.db"  # New

# We use weighted round-robin algorithm between two queues.
# Current implementation has equal weights.
IN_MEMORY_Q_WEIGHT: int = 1  # New
PERSISTENT_Q_WEIGHT: int = 1  # New

NETWORK_LOSS_PROBABILITY_PERCENT: int = 1  # Simulation only


class Queue():
    def push(self, item: str) -> None:
        pass

    def peek(self) -> str:
        return ""

    def pop(self) -> str:
        return ""

    def size(self) -> int:
        return 0


class InMemoryQueue(Queue):
    __queue: list[str]
    __maxSize: int

    def __init__(self, maxSize: int) -> None:
        self.__maxSize = maxSize
        self.__queue = []

    def push(self, item: str) -> None:
        if (len(self.__queue) < self.__maxSize):
            self.__queue.append(item)

    def peek(self) -> str:
        return self.__queue[0]

    def pop(self) -> str:
        return self.__queue.pop()

    def size(self) -> int:
        return len(self.__queue)


class PersistentQueue(Queue):
    __con: sqlite3.Connection
    __cur: sqlite3.Cursor

    def __init__(self) -> None:
        self.__con = sqlite3.connect(DATABASE)

        # Performance optimizations
        self.__con.execute('PRAGMA journal_mode = OFF;')
        self.__con.execute('PRAGMA synchronous = 0;')
        self.__con.execute('PRAGMA cache_size = 1000000;')  # give it a GB
        self.__con.execute('PRAGMA locking_mode = EXCLUSIVE;')
        self.__con.execute('PRAGMA temp_store = MEMORY;')

        self.__cur = self.__con.cursor()
        self.__cur.execute(
            "CREATE TABLE  IF NOT EXISTS logs(timestamp TEXT, log TEXT);")
        self.__con.commit()

    def push(self, item: str) -> None:
        id: str = str(ULID.from_timestamp(time.time()))
        self.__cur.execute("INSERT INTO logs VALUES (?, ?);", (id, item))
        self.__con.commit()

    def peek(self) -> str:
        self.__cur.execute("SELECT timestamp, log FROM logs LIMIT 1;")
        _, log = self.__cur.fetchone()
        return log

    def pop(self) -> str:
        self.__cur.execute("SELECT timestamp, log FROM logs LIMIT 1;")
        timestamp, log = self.__cur.fetchone()
        self.__cur.execute(
            "DELETE FROM logs WHERE timestamp = ?;", (timestamp,))
        self.__con.commit()
        return log

    def size(self) -> int:
        self.__cur.execute("SELECT COUNT(log) FROM logs;")
        count: int = self.__cur.fetchone()[0]
        return count


def is_time_up(expectedDelay: float, lastSent: datetime) -> bool:
    diff = datetime.now() - lastSent
    return diff.microseconds * 1000 >= expectedDelay


def try_send(log: str) -> bool:
    # Here, the function needs if there is a connectivity between the agent and the server.
    # It can use last_keepalive or last_ack
    random.seed(time.process_time())
    luckyNumber: int = random.randrange(100)
    # Simulate loss. If loss probability is higher than zero, we check the number is in a predefined range.
    if (NETWORK_LOSS_PROBABILITY_PERCENT > 0 and luckyNumber in range(0, NETWORK_LOSS_PROBABILITY_PERCENT)):
        print("ERROR: Failed to send the log.")
        return False
    else:
        print(f"INFO: Succesfully sent log: {log}")
        return True


def get_random_string(length) -> str:
    # choose from all lowercase letter
    letters = string.ascii_lowercase

    random.seed(time.process_time())
    result_str: str = ''.join(random.choice(letters) for i in range(length))
    return result_str

# This is an approximation of dispatch_buffer function in buffer.c (https://github.com/wazuh/wazuh/blob/8b613b4ff11873a9a189acfcb19db6688858cafc/src/client-agent/buffer.c#L138)


def main() -> None:
    q: InMemoryQueue = InMemoryQueue(BUFFER_SIZE)
    pq: PersistentQueue = PersistentQueue()
    lastSent: datetime = datetime.now()
    delayMs: float = 1000 / MAX_EPS

    # WRR implementation
    get_weighted = roundrobin.weighted(
        [(q, IN_MEMORY_Q_WEIGHT), (pq, PERSISTENT_Q_WEIGHT)])

    # TODO: Add Leaky Bucket controls into the loop
    while (True):  # TODO: Add sliding window statistic collection with Prometheus: average EPS generated, EPS sent, EPS failed, q size, pq size

        # We generate a 20 char random string on every cycle.
        # The assumption is a new log on every cycle: a high volume of logs
        log: str = get_random_string(20)

        # TODO: New event generated. Add to stats.
        # If there is a log parsed by the agent, it is read on every cycle. Not waiting for MAX_EPS delay.
        q.push(log)
        print(f"Q Size: {q.size()}\nPQ Size: {pq.size()}")

        # Even though we read logs in an non-deterministic manner, we should send them under MAX_EPS
        if (is_time_up(delayMs, lastSent)):
            buffer: Queue

            # If pq is empty, send from buffer. If pq has at least 1 value, start round robin.
            if (pq.size() == 0):
                buffer = q
            else:
                buffer = get_weighted()  # type: ignore

            next: str = buffer.peek()

            if (try_send(next)):
                # TODO: Succesfully sent. Add to stats.
                # Pop the sent log out ot the queue
                _: str = buffer.pop()

            else:
                # TODO: Failed to send. Add to stats.
                # No network connection, send to PQ
                pq.push(next)

            print(f"Q Size: {q.size()}\nPQ Size: {pq.size()}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Cancelled by user.')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
    except Exception as ex:
        print('ERROR: ' + str(ex))
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)
