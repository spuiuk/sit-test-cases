import os
import subprocess
import typing
import shutil
from pathlib import Path


def cifs_mount(
    mount_params: typing.Dict[str, str], mount_point: Path, opts: str = ""
) -> int:
    """Use the cifs module to mount a share.

    Parameters:
    mount_params: Dict containing mount parameters
    mount_point: Directory location to mount the share.
    opts: Additional options to pass to the mount command

    Returns:
    int: return value of the mount command.
    """

    if not opts:
        mount_options = (
            "username="
            + mount_params["username"]
            + ",password="
            + mount_params["password"]
        )
    else:
        mount_options = (
            opts
            + ",username="
            + mount_params["username"]
            + ",password="
            + mount_params["password"]
        )
    share = "//" + mount_params["host"] + "/" + mount_params["share"]
    cmd = (
        "mount -t cifs -o "
        + mount_options
        + " "
        + share
        + " "
        + str(mount_point)
    )
    ret = os.system(cmd)
    assert ret == 0, "Error mounting: ret %d cmd: %s\n" % (ret, cmd)
    return ret


def cifs_umount(mount_point: Path) -> int:
    """Unmount a mounted filesystem.

    Parameters:
    mount_point: Directory of the mount point.

    Returns:
    int: return value of the umount command.
    """
    cmd = "umount -fl %s" % (mount_point)
    ret = os.system(cmd)
    assert ret == 0, "Error mounting: ret %d cmd: %s\n" % (ret, cmd)
    return ret


def check_cmds(cmds: typing.List[str]) -> Path:
    """Return first file path which exists.

    Parameters:
    cmds: list of commands to test

    Returns:
    pathlib.Path: first available file path from the list
    """
    for c in cmds:
        cmd = shutil.which(c)
        if cmd is not None:
            return Path(cmd)
    assert False, "Could not find command"


def podman_run(test_image: str, test_root: Path) -> typing.Tuple[int, str]:
    """Run podman command

    Parameters:
    test_image: The image to be used for the podman run
    test_root: The root of the folder which will be used to perform the tests

    Returns:
    int: Return value from the execution
    string: stdout
    """
    cmds = ["podman", "docker"]
    cmd = str(check_cmds(cmds))
    mount_path = str(test_root)
    podman_cmd = [
        cmd,
        "run",
        f"--volume={mount_path}:/testdir",
        "--privileged",
        test_image,
    ]
    ret = subprocess.run(
        podman_cmd,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return (ret.returncode, ret.stdout)
