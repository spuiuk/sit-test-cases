# This test is to check writes to a folder owned by a supplementary group.
# expected username in share["users"] and
# share["extra"]["supplementary_group"] with supplementary group
# also needs direct access to underlying filesystem
#
# Requirements:
# 1. username in share["user"] exists
# 2. username is part of supplementary group provided in
#    user["extra"]["supplementary_group"]
# 3. share["backend"]["path"] should be set
#
# Steps:
# 1. Create folder test_subdir/ on direct path with
#    group set to sgroup and mode 0770
# 2. Upload file to test_subdir/test-cp
#
# Expected:
# Copy passes

import testhelper
import pytest
import shutil
import pwd
import grp
import os
from pathlib import Path

test_info_file = os.getenv("TEST_INFO_FILE")
test_info = testhelper.read_yaml(test_info_file)
test_string = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
test_subdir = Path("supplementary_group")


def check_reqs(username: str, groupname: str) -> bool:
    try:
        pwd.getpwnam(username)
        if username not in grp.getgrnam(groupname).gr_mem:
            return False
    except KeyError:
        return False
    return True


def setup_local_testdir(root: Path, group: str) -> Path:
    testdir = root / test_subdir
    testdir.mkdir(exist_ok=True)
    shutil.chown(testdir, group=group)
    testdir.chmod(0o770)
    return testdir


def write_file_remote(
    mount_point: Path,
    ipaddr: str,
    share_name: str,
) -> None:
    mount_params = testhelper.get_mount_parameters(test_info, share_name)
    mount_params["host"] = ipaddr
    try:
        test_file = testhelper.get_tmp_file(mount_point)
        test_file_remote = test_subdir / Path("test-cp")
        with open(test_file, "w") as f:
            f.write(test_string)
        put_cmds = "put  %s %s" % (test_file, test_file_remote)
        (ret, output) = testhelper.smbclient(mount_params, put_cmds)
        assert ret == 0, "Failed to copy file to server: " + output
    finally:
        if test_file.exists():
            test_file.unlink()


def gen_supplementary_group_param(test_info: dict) -> list:
    if not test_info:
        return []
    sgroup = testhelper.get_conf_extra(test_info, "supplementary_group")
    if sgroup is None:
        return []
    arr = []
    for share in testhelper.get_shares_with_directmnt(test_info):
        s = testhelper.get_share(test_info, share)
        username = list(s["users"].keys())[0]
        if check_reqs(username, sgroup):
            arr.append((s["server"], share))
    return arr


@pytest.mark.parametrize(
    "ipaddr,share_name", gen_supplementary_group_param(test_info)
)
def test_supplementary_group(ipaddr: str, share_name: str) -> None:
    share = testhelper.get_share(test_info, share_name)
    fs_path = Path(share["backend"]["path"])
    sgroup = testhelper.get_conf_extra(test_info, "supplementary_group")
    testdir = setup_local_testdir(fs_path, sgroup)
    try:
        tmp_root = testhelper.get_tmp_root()
        mount_point = testhelper.get_tmp_mount_point(tmp_root)
        write_file_remote(mount_point, ipaddr, share_name)
    finally:
        shutil.rmtree(testdir)
