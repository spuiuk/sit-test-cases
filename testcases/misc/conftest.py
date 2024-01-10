#!/usr/bin/env python3

import pytest
import os
import shutil
import testhelper
import typing
from pathlib import Path

test_info_file = os.getenv("TEST_INFO_FILE")
test_info = testhelper.read_yaml(test_info_file)


@pytest.fixture
def setup_mount(
    request: pytest.FixtureRequest,
) -> typing.Generator[Path, None, None]:
    ipaddr, share_name = request.param
    flag_mounted: bool = False
    tmp_root = testhelper.get_tmp_root()
    mount_point = testhelper.get_tmp_mount_point(tmp_root)
    try:
        mount_params = testhelper.get_mount_parameters(test_info, share_name)
        mount_params["host"] = ipaddr

        # mount cifs share
        testhelper.cifs_mount(mount_params, mount_point)
        flag_mounted = True
        test_dir = mount_point / "mount_test"
        test_dir.mkdir()
    except Exception as e:
        raise Exception(f"Setup failed: {str(e)}")

    # Yield the setup result
    yield test_dir

    # Perform teardown after the test has run
    try:
        if flag_mounted and test_dir:
            shutil.rmtree(test_dir, ignore_errors=True)
            testhelper.cifs_umount(mount_point)
        mount_point.rmdir()
        tmp_root.rmdir()
    except Exception as e:
        raise Exception(f"Teardown failed: {str(e)}")


def generate_mount_check() -> typing.List[typing.Any]:
    ipaddr = test_info["public_interfaces"][0]
    exported_sharenames = test_info.get("exported_sharenames", [])
    arr = []
    for share_name in exported_sharenames:
        arr.append(
            pytest.param((ipaddr, share_name), id=f"{ipaddr}-{share_name}")
        )
    return arr


def generate_mount_check_premounted() -> typing.List[Path]:
    return testhelper.get_premounted_shares(test_info)
