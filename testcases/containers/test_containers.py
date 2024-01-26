#!/usr/bin/env python3

# This test first mounts a cifs share, creates a new file on it,
# writes to it, and unmounts the share, and then tests that it
# can get exactly the data written into the file through various
# possible ways of mounting the share (combinations of users and
# ip addresses).

import testhelper
import os
import pytest
import typing
import yaml
from pathlib import Path
import shutil

script_root = Path(__file__).resolve().parent
container_tests_file = script_root / "test_containers.yml"

# Use a global test_info to get a better output when running pytest
test_info: typing.Dict[str, typing.Any] = {}
# global containing tests read from yaml
container_tests: typing.Dict[str, str] = {}


def load_container_tests() -> int:
    global container_tests
    with open(container_tests_file) as f:
        ct = yaml.safe_load(f)
    if ct is None:
        return 0
    for t in ct:
        container_tests[t["name"]] = t["url"]
    return len(container_tests)


# Load globals
test_info = testhelper.read_yaml(os.getenv("TEST_INFO_FILE"))
assert load_container_tests() != 0, "No tests loaded"


def containers_check_mounted(mount_point: Path, test: str) -> None:
    test_dir = mount_point / test
    test_dir.mkdir()
    try:
        ret, output = testhelper.podman_run(container_tests[test], test_dir)
        print(output)
        assert ret == 0, "Error running test"
    finally:
        # Cannot use Path.rmdir() here since test_dir isn't empty
        shutil.rmtree(test_dir, ignore_errors=True)


def containers_check(ipaddr: str, share_name: str, test: str) -> None:
    mount_params = testhelper.get_mount_parameters(test_info, share_name)
    mount_params["host"] = ipaddr
    tmp_root = testhelper.get_tmp_root()
    mount_point = testhelper.get_tmp_mount_point(tmp_root)
    testhelper.cifs_mount(mount_params, mount_point)
    try:
        containers_check_mounted(mount_point, test)
    finally:
        testhelper.cifs_umount(mount_point)
        mount_point.rmdir()
        tmp_root.rmdir()


def generate_containers_test() -> typing.List[typing.Tuple[str, str, str]]:
    # Use the first given public_interface for our tests
    ipaddr = test_info["public_interfaces"][0]
    arr = []
    for share_name in test_info["exported_sharenames"]:
        for test in container_tests.keys():
            arr.append((ipaddr, share_name, test))
    return arr


@pytest.mark.privileged
@pytest.mark.parametrize(
    "ipaddr,share_name,test",
    generate_containers_test(),
)
def test_containers(ipaddr: str, share_name: str, test: str) -> None:
    containers_check(ipaddr, share_name, test)
