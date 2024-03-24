#!/usr/bin/env python3

# This test first mounts a cifs share, creates a new file on it,
# writes to it, and unmounts the share, and then tests that it
# can get exactly the data written into the file through various
# possible ways of mounting the share (combinations of users and
# ip addresses).

import testhelper
from testhelper import SMBClient
import os
import pytest
import typing

test_string = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
test_info_file = os.getenv("TEST_INFO_FILE")
test_info = testhelper.read_yaml(test_info_file)


def consistency_check(hostname: str, share_name: str) -> None:
    mount_params = testhelper.get_mount_parameters(test_info, share_name)
    test_filename = "/test_consistency"

    # file write cycle
    scon = SMBClient(
        hostname,
        share_name,
        mount_params["username"],
        mount_params["password"],
    )
    scon.simple_write(test_filename, test_string)
    scon.disconnect()

    # file read cycle
    scon = SMBClient(
        hostname,
        share_name,
        mount_params["username"],
        mount_params["password"],
    )
    retstr = scon.simple_read(test_filename)
    scon.unlink(test_filename)
    scon.disconnect()

    assert retstr == test_string, "File content does not match"


def generate_consistency_check() -> typing.List[typing.Tuple[str, str]]:
    arr = []
    for sharename in testhelper.get_exported_shares(test_info):
        share = testhelper.get_share(test_info, sharename)
        arr.append((share["server"], share["name"]))
    return arr


@pytest.mark.parametrize("hostname,share_name", generate_consistency_check())
def test_consistency(hostname: str, share_name: str) -> None:
    consistency_check(hostname, share_name)
