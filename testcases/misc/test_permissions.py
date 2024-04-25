import testhelper
import os
import pytest
import pwd
import grp
import shutil
from pathlib import Path

test_info_file = os.getenv("TEST_INFO_FILE")
test_info = testhelper.read_yaml(test_info_file)
test_string = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

# test_supplementary_group:
# This test is to check writes to a folder owned by a supplementary group.
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


# function to check if requirements are met
def check_reqs_supplementary_group(share: dict, sgroup: str) -> bool:
    if share["backend"]["path"] is None:
        return False

    username = list(share["users"].keys())[0]
    try:
        pwd.getpwnam(username)
        if username not in grp.getgrnam(sgroup).gr_mem:
            return False
    except KeyError:
        return False
    return True


def gen_supplementary_group_param(test_info: dict) -> list:
    if not test_info:
        return []
    sgroup = testhelper.get_extra_configuration(
        test_info, "supplementary_group"
    )
    if sgroup is None:
        return []

    arr = []
    for s in testhelper.get_shares(test_info).values():
        if check_reqs_supplementary_group(s, sgroup):
            arr.append((s["server"], s["name"]))
    return arr


@pytest.mark.privileged
@pytest.mark.parametrize(
    "ipaddr,share_name", gen_supplementary_group_param(test_info)
)
def test_supplementary_group(ipaddr: str, share_name: str) -> None:
    share = testhelper.get_share(test_info, share_name)
    fs_path = Path(share["backend"]["path"])
    sgroup = testhelper.get_extra_configuration(
        test_info, "supplementary_group"
    )
    test_subdir = Path("supplementary_group")
    mount_params = testhelper.get_mount_parameters(test_info, share_name)

    # setup local testdir
    testdir = fs_path / test_subdir
    testdir.mkdir(exist_ok=True)
    shutil.chown(testdir, group=sgroup)
    testdir.chmod(0o770)

    smbclient = testhelper.SMBClient(
        ipaddr,
        mount_params["share"],
        mount_params["username"],
        mount_params["password"],
    )

    try:
        remote_test_file = str(Path("/") / test_subdir / Path("test-cp"))
        smbclient.write_text(remote_test_file, test_string)
    finally:
        smbclient.disconnect()
        shutil.rmtree(testdir)
