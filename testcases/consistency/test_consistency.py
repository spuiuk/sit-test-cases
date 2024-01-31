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
from pathlib import Path


test_string = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
test_info_file = os.getenv("TEST_INFO_FILE")
test_info = testhelper.read_yaml(test_info_file)


def file_content_check(f: typing.IO, comp_str: str) -> bool:
    read_data = f.read()
    return read_data == comp_str


def consistency_check(mount_point: Path, ipaddr: str, share_name: str) -> None:
    mount_params = testhelper.get_mount_parameters(test_info, share_name)
    mount_params["host"] = ipaddr
    try:
        test_file = testhelper.get_tmp_file(mount_point)
        test_file_resp = test_file.with_suffix(".resp")
        test_file_remote = "test-" + ipaddr + "." + share_name
        with open(test_file, "w") as f:
            f.write(test_string)
        put_cmds = "put  %s %s" % (test_file, test_file_remote)
        (ret, output) = testhelper.smbclient(mount_params, put_cmds)
        assert ret == 0, "Failed to copy file to server"

        # The file read cycle
        get_cmds = "get %s %s; rm %s" % (
            test_file_remote,
            test_file_resp,
            test_file_remote,
        )
        (ret, output) = testhelper.smbclient(mount_params, get_cmds)
        assert ret == 0, "Failed to copy file from server"
        with open(test_file_resp, "r") as f:
            assert file_content_check(
                f, test_string
            ), "File content does not match"
    finally:
        if test_file.exists():
            test_file.unlink()
        if test_file_resp.exists():
            test_file_resp.unlink()


def generate_consistency_check(
    test_info_file: dict,
) -> typing.List[typing.Tuple[str, str]]:
    if not test_info_file:
        return []
    arr = []
    for share in testhelper.get_exported_shares(test_info):
        s = testhelper.get_share(test_info, share)
        arr.append((s["server"], s["name"]))
    return arr


@pytest.mark.parametrize(
    "ipaddr,share_name", generate_consistency_check(test_info)
)
def test_consistency(ipaddr: str, share_name: str) -> None:
    tmp_root = testhelper.get_tmp_root()
    mount_point = testhelper.get_tmp_mount_point(tmp_root)
    consistency_check(mount_point, ipaddr, share_name)
    os.rmdir(mount_point)
    os.rmdir(tmp_root)
