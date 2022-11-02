import os
import random
import sqlite3
import string
import sys
import time
from datetime import datetime

from ulid import ULID

DATABASE: str = "queue.db"
BUFFER_SIZE: int = 5000
MAX_EPS: int = 500
NETWORK_LOSS_PROBABILITY_PERCENT: int = 1


class InMemoryQueue():
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


class PersistentQueue():
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


def main() -> None:
    q: InMemoryQueue = InMemoryQueue(BUFFER_SIZE)
    pq: PersistentQueue = PersistentQueue()
    lastSent: datetime = datetime.now()
    delayMs: float = 1000 / MAX_EPS
    # a simple value fo round robin switchinhg between q and pq.
    isLastFromPQ: bool = False

    while (True):  # TODO: Add sliding window statistic collection with Prometheus: average EPS generated, EPS sent, EPS failed

        # We generate a 20 char random string on every cycle
        # The assumption is a new log on every cycle, a high volume of logs
        log: str = get_random_string(20)

        # If there is a log parsed by the agent, it is read on every cycle. Not waiting for MAX_EPS delay.
        q.push(log)
        print(f"Q Size: {q.size()}")

        # Even though we read logs in an non-deterministic manner, we should send them under MAX_EPS
        if (is_time_up(delayMs, lastSent)):
            # If pq is empty, send from buffer. If pq has at least 1 value, start round robin.
            if (pq.size() == 0):
                next: str = q.peek()
                isLastFromPQ = False
            else:
                if (isLastFromPQ):
                    next: str = q.peek()
                    isLastFromPQ = False
                else:
                    next: str = pq.peek()
                    isLastFromPQ = True

            if (try_send(next)):
                if isLastFromPQ:
                    _: str = pq.pop()  # Get it out of the queue
                    print(f"PQ Size: {pq.size()}")
                else:
                    _: str = q.pop()
                    print(f"Q Size: {q.size()}")
            else:
                pq.push(next)  # No network connection, send to PQ
                print(f"PQ Size: {pq.size()}")


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
