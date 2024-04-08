#
# A simple load test
#
# We use python process and threads to open up several consecutive connections
# on the SMB server and perform either open/write, open/read and delete
# operations with an interval of 0.5 seconds between each operation.
# The tests are run for  fixed duration of time before we stop and
# print out the stats for the number of operations performed
#
# 10 processes each with 100 thread to simulate a total of 1000 consecutive
# connections are created


import testhelper
import random
import time
import threading
import typing
import pytest
import os
from multiprocessing import Process, Queue

test_info_file = os.getenv("TEST_INFO_FILE")
test_info: dict = testhelper.read_yaml(test_info_file)

# total number of processes
total_processes: int = 10
# each with this number of threads
per_process_threads: int = 50
# running the connection test for this many seconds
test_runtime: int = 30
# size of test files
test_file_size = 4 * 1024  # 4k size
# number of files each thread creates
test_file_number = 10


class SimpleLoadTest:
    """A helper class to generate a simple load on a SMB server"""

    instance_num: int = 0
    max_files: int = test_file_number
    test_string: str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

    def __init__(
        self,
        hostname: str,
        share: str,
        username: str,
        passwd: str,
        testdir: str,
        testfile: str = "",
    ):
        self.idnum: int = type(self).instance_num
        type(self).instance_num += 1

        self.testfile = testfile
        self.rootpath: str = f"{testdir}/test{self.idnum}"
        self.files: typing.List[str] = []
        self.thread = None
        self.test_running: bool = False
        self.stats: dict["str", int] = {
            "read": 0,
            "write": 0,
            "delete": 0,
            "error": 0,
            "client_error": 0,
        }

        # Operations in the frequency of which they are called
        self.actions: dict["str", int] = {"write": 1, "read": 3, "delete": 1}
        # Use the dict above to generate weights for random.choice()
        self.ops: list[str] = list(self.actions.keys())
        self.ops_count: list[int] = [self.actions[str] for str in self.ops]

        try:
            self.smbclient: testhelper.SMBClient = testhelper.SMBClient(
                hostname, share, username, passwd
            )
        except (IOError, TimeoutError, ConnectionError) as error:
            self.stats["error"] += 1
            raise RuntimeError(f"failed to setup connection: {error}")

    def disconnect(self) -> None:
        self.smbclient.disconnect()

    def _new_file(self) -> str:
        """return a new filename which doesn't exist"""
        # Don't go above max_files
        if len(self.files) >= self.max_files:
            return ""
        file: str = "file" + str(random.randint(0, 1000))
        # if we don't already have this filename open, return filename
        if file not in self.files:
            self.files.append(file)
            return f"{self.rootpath}/{file}"
        # else recursive call until we have a filename to return
        return self._new_file()

    def _get_file(self) -> str:
        """Get a filename which has already been created"""
        if not self.files:
            return ""
        file = random.choice(self.files)
        return f"{self.rootpath}/{file}"

    def _del_file(self) -> str:
        """Delete filename which has been created"""
        if not self.files:
            return ""
        file = random.choice(self.files)
        self.files.remove(file)
        return f"{self.rootpath}/{file}"

    def _simple_run(self, op=""):
        """Run random operations on the share
        This is based on the ops and weight set in self.actions
        in the intialiser.
        """
        # if op hasn't been set, randomly select an op
        if not op:
            op = random.sample(self.ops, k=1, counts=self.ops_count)[0]
        try:
            if op == "read":
                file = self._get_file()
                if not file:
                    # If no files exist, then run an write op first
                    self._simple_run(op="write")
                    return
                self.stats["read"] += 1
                if self.testfile:
                    tfile = testhelper.get_tmp_file()
                    with open(tfile, "wb") as fd:
                        self.smbclient.read(file, fd)
                    os.unlink(tfile)
                else:
                    self.smbclient.read_text(file)
            elif op == "write":
                file = self._new_file()
                if not file:
                    return
                self.stats["write"] += 1
                if self.testfile:
                    with open(self.testfile, "rb") as fd:
                        self.smbclient.write(file, fd)
                else:
                    self.smbclient.write_text(file, type(self).test_string)
            elif op == "delete":
                file = self._del_file()
                if not file:
                    return
                self.stats["delete"] += 1
                self.smbclient.unlink(file)
        # Catch operational errors
        except (IOError, TimeoutError, ConnectionError) as error:
            print(error)
            self.stats["error"] += 1

    def _clean_up(self):
        # Go through open file list and delete any existing files
        for file in self.files:
            self.smbclient.unlink(f"{self.rootpath}/{file}")
        self.files = []

    def simple_load(self, test_start: float, test_stop: float) -> None:
        """Run a simple load tests between test_start and test_stop times"""
        # Do not proceed if we hit an error here
        try:
            self.smbclient.mkdir(self.rootpath)
        except Exception:
            print("Error creating test subdirectory")
            self.stats["error"] += 1
            return
        while time.time() < test_start:
            time.sleep(test_start - time.time())
        self.test_running = True
        while time.time() < test_stop:
            self._simple_run()
            # Sleep for half a second between each operation
            time.sleep(0.5)
        # Record these errors but proceed with other operations
        try:
            self._clean_up()
            self.smbclient.rmdir(self.rootpath)
        except (IOError, TimeoutError, ConnectionError) as error:
            print(error)
            self.stats["error"] += 1
        self.test_running = False

    def start(self, test_start, test_stop):
        self.thread = threading.Thread(
            target=self.simple_load, args=(test_start, test_stop)
        )
        try:
            self.thread.start()
        except RuntimeError:
            print("Could not start thread")
            self.thread = None
            self.stats["client_error"] += 1

    def cleanup(self):
        while self.test_running:
            time.sleep(1)
        if self.thread:
            self.thread.join()
        # Just report errors during cleanup
        try:
            self.disconnect()
        except Exception as error:
            print(error)


class LoadTest:
    def __init__(
        self,
        hostname: str,
        share: str,
        username: str,
        passwd: str,
        testdir: str,
        testfile: str = "",
    ):
        self.server: str = hostname
        self.share: str = share
        self.username: str = username
        self.password: str = passwd
        self.testdir: str = testdir
        self.testfile = testfile

        self.connections: typing.List[SimpleLoadTest] = []
        self.start_time: float = 0
        self.stop_time: float = 0

    def get_connection_num(self) -> int:
        return len(self.connections)

    def set_connection_num(self, num: int) -> None:
        cnum: int = self.get_connection_num()
        if cnum < num:
            for _ in range(0, num - cnum):
                smbclient = SimpleLoadTest(
                    self.server,
                    self.share,
                    self.username,
                    self.password,
                    self.testdir,
                    self.testfile,
                )
                self.connections.append(smbclient)
        elif cnum > num:
            for testclient in self.connections[num:]:
                testclient.cleanup()
            del self.connections[num:]

    def total_stats(self) -> typing.Dict[str, int]:
        total_stats: dict[str, int] = {
            "write": 0,
            "read": 0,
            "delete": 0,
            "error": 0,
            "client_error": 0,
        }
        for smbcon in self.connections:
            stats = smbcon.stats
            total_stats["read"] += stats.get("read", 0)
            total_stats["write"] += stats.get("write", 0)
            total_stats["delete"] += stats.get("delete", 0)
            total_stats["error"] += stats.get("error", 0)
            total_stats["client_error"] += stats.get("client_error", 0)
        return total_stats

    def start_tests(self, runtime: int) -> None:
        # delay start by 10 seconds to give sufficient time to
        # setup threads/processes.
        self.start_time = time.time() + 10
        self.stop_time = self.start_time + runtime
        for testclient in self.connections:
            testclient.start(self.start_time, self.stop_time)

    def stop_tests(self):
        while time.time() < self.stop_time:
            time.sleep(self.stop_time - time.time())
        for testclient in self.connections:
            testclient.cleanup()


def print_stats(header: str, stats: typing.Dict[str, int]) -> None:
    """Helper function to print out process stats"""
    ret = header + " "
    ret += f'read: {stats.get("read", 0)} '
    ret += f'write: {stats.get("write", 0)} '
    ret += f'delete: {stats.get("delete", 0)} '
    ret += f'error: {stats.get("error", 0)} '
    ret += f'client_error: {stats.get("client_error", 0)} '
    print(ret)


def start_process(
    process_number: int,
    numcons: int,
    ret_queue: Queue,
    mount_params: typing.Dict[str, str],
    testdir: str,
    testfile: str = "",
) -> None:
    """Start function for test processes"""
    loadtest: LoadTest = LoadTest(
        mount_params["host"],
        mount_params["share"],
        mount_params["username"],
        mount_params["password"],
        testdir,
        testfile,
    )
    loadtest.set_connection_num(numcons)
    loadtest.start_tests(test_runtime)
    loadtest.stop_tests()
    total_stats: dict[str, int] = loadtest.total_stats()
    total_stats["process_number"] = process_number
    total_stats["number_connections"] = numcons
    # Push process stats to the main process
    ret_queue.put(total_stats)


def generate_loading_check() -> typing.List[tuple[str, str]]:
    """return a list of tuples containig hostname and sharename to test"""
    arr = []
    for sharename in testhelper.get_exported_shares(test_info):
        share = testhelper.get_share(test_info, sharename)
        arr.append((share["server"], share["name"]))
    return arr


@pytest.mark.parametrize("hostname,sharename", generate_loading_check())
def test_loading(hostname: str, sharename: str) -> None:
    # Get a tmp file of size 4K
    tmpfile = testhelper.get_tmp_file(size=test_file_size)
    mount_params: dict[str, str] = testhelper.get_mount_parameters(
        test_info, sharename
    )
    testdir: str = "/loadtest"
    # Open a connection to create and finally remove the testdir
    smbclient: testhelper.SMBClient = testhelper.SMBClient(
        hostname,
        mount_params["share"],
        mount_params["username"],
        mount_params["password"],
    )
    smbclient.mkdir(testdir)

    # Start load test

    # return queue for stats
    ret_queue: Queue = Queue()
    processes: list[Process] = []
    for process_number in range(total_processes):
        process_testdir: str = f"{testdir}/p{process_number}"
        smbclient.mkdir(process_testdir)
        process = Process(
            target=start_process,
            args=(
                process_number,
                per_process_threads,
                ret_queue,
                mount_params,
                process_testdir,
                tmpfile,
            ),
        )
        processes.append(process)

    for process in processes:
        process.start()

    for process in processes:
        process.join()

    total_stats: dict[str, int] = {
        "write": 0,
        "read": 0,
        "delete": 0,
        "error": 0,
        "client_error": 0,
    }
    while not ret_queue.empty():
        stats = ret_queue.get()
        print_stats(
            f'Process #{stats["process_number"]} '
            + f'{stats.get("number_connections", 0)} Connections:',
            stats,
        )
        total_stats["read"] += stats.get("read", 0)
        total_stats["write"] += stats.get("write", 0)
        total_stats["delete"] += stats.get("delete", 0)
        total_stats["error"] += stats.get("error", 0)
        total_stats["client_error"] += stats.get("client_error", 0)

    for process_number in range(total_processes):
        process_testdir = f"{testdir}/p{process_number}"
        smbclient.rmdir(process_testdir)
    # End load test

    smbclient.rmdir(testdir)
    smbclient.disconnect()
    os.unlink(tmpfile)

    print_stats("Total:", total_stats)
    assert (
        total_stats["error"] == 0
    ), "Server side errors seen when running load tests"
