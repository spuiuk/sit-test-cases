import os
import tempfile
from pathlib import Path


def get_tmp_root() -> Path:
    """
    Returns a temporary directory for use

    Parameters:
    tmp_dir: Temporary location to use. Set to "/tmp" by default

    Returns:
    tmp_root: Location of the temporary directory.
    """
    return Path(tempfile.mkdtemp())


def get_tmp_mount_point(tmp_root: Path = Path(tempfile.gettempdir())) -> Path:
    """
    Return a mount point within the temporary directory

    Parameters:
    tmp_root: Directory in which to create mount point.

    Returns:
    mnt_point: Directory location in which you can mount a share.
    """
    return Path(tempfile.mkdtemp(prefix="mnt_", dir=tmp_root))


def get_tmp_file(
    tmp_root: Path = Path(tempfile.gettempdir()), size: int = 0
) -> Path:
    """
    Return a temporary file within the temporary directory

    Parameters:
    tmp_root: Directory in which to create temporary file.
    size: If provided, create a file of size bytes

    Returns:
    tmp_file: Location of temporary file.
    """
    fd, file_name = tempfile.mkstemp(dir=tmp_root)
    if size != 0:
        with os.fdopen(fd, "wb") as f:
            f.write(os.urandom(size))
    return Path(file_name)


def get_tmp_dir(tmp_root: Path = Path(tempfile.gettempdir())) -> Path:
    """
    Return a temporary directory within the temporary directory

    Parameters:
    tmp_root: Directory in which to create temporary directory.

    Returns:
    tmp_dir: Location of temporary directory.
    """
    return Path(tempfile.mkdtemp(dir=tmp_root))
