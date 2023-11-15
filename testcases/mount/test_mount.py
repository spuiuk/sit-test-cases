#!/usr/bin/env python3

# Test mounts a cifs share, creates a new file on it, writes to it,
# deletes the file and unmounts

import testhelper
import os
import pytest
import typing
import shutil
from pathlib import Path

from .mount_io import check_io_consistency
from .mount_dbm import check_dbm_consistency
from .mount_stress import check_mnt_stress

test_info_file = os.getenv("TEST_INFO_FILE")
test_info = testhelper.read_yaml(test_info_file)


def mount_check_mounted(
    mount_point: Path, test_function: typing.Callable
) -> None:
    try:
        test_dir = mount_point / "mount_test"
        test_dir.mkdir()
        test_function(test_dir)
    finally:
        shutil.rmtree(test_dir, ignore_errors=True)


def mount_check(
    ipaddr: str, share_name: str, test_function: typing.Callable
) -> None:
    mount_params = testhelper.get_mount_parameters(test_info, share_name)
    mount_params["host"] = ipaddr
    tmp_root = testhelper.get_tmp_root()
    mount_point = testhelper.get_tmp_mount_point(tmp_root)
    flag_mounted = False
    try:
        testhelper.cifs_mount(mount_params, mount_point)
        flag_mounted = True
        mount_check_mounted(Path(mount_point), test_function)
    finally:
        if flag_mounted:
            testhelper.cifs_umount(mount_point)
        os.rmdir(mount_point)
        os.rmdir(tmp_root)


def generate_test_parameters() -> typing.List[typing.Tuple[str, str]]:
    public_interfaces = test_info.get("public_interfaces", [])
    if len(public_interfaces) < 1:
        return []
    exported_sharenames = test_info.get("exported_sharenames", [])
    if len(exported_sharenames) < 1:
        return []
    arr = []
    ipaddr = public_interfaces[0]
    for share_name in exported_sharenames:
        arr.append((ipaddr, share_name))
    return arr


@pytest.mark.parametrize("ipaddr,share_name", generate_test_parameters())
def test_io_consistency(ipaddr: str, share_name: str) -> None:
    mount_check(ipaddr, share_name, check_io_consistency)


@pytest.mark.parametrize("ipaddr,share_name", generate_test_parameters())
def test_dbm_consistency(ipaddr: str, share_name: str) -> None:
    mount_check(ipaddr, share_name, check_dbm_consistency)


@pytest.mark.parametrize("ipaddr,share_name", generate_test_parameters())
def test_mnt_stress(ipaddr: str, share_name: str) -> None:
    mount_check(ipaddr, share_name, check_mnt_stress)


@pytest.mark.parametrize(
    "test_dir", testhelper.get_premounted_shares(test_info)
)
def test_io_consistency_premounted(test_dir: Path) -> None:
    mount_check_mounted(test_dir, check_io_consistency)


@pytest.mark.parametrize(
    "test_dir", testhelper.get_premounted_shares(test_info)
)
def test_dbm_consistency_premounted(test_dir: Path) -> None:
    mount_check_mounted(test_dir, check_dbm_consistency)


@pytest.mark.parametrize(
    "test_dir", testhelper.get_premounted_shares(test_info)
)
def test_mnt_stress_premounted(test_dir: Path) -> None:
    mount_check_mounted(test_dir, check_mnt_stress)
