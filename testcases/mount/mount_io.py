#!/usr/bin/env python3

# Test various file-system I/O operations via local SMB mount-point.

import datetime
import pathlib
import random
import shutil
import typing


class DataPath:
    """A pair of random data-buffer and path-name to its regular file"""

    def __init__(self, path: pathlib.Path, size: int) -> None:
        self.path = path
        self.size = size
        self.data = _generate_random_bytes(size)

    def renew(self) -> None:
        self.data = _generate_random_bytes(self.size)

    def write(self) -> None:
        self.path.write_bytes(self.data)

    def overwrite(self) -> None:
        self.renew()
        self.write()

    def read(self) -> bytes:
        return self.path.read_bytes()

    def mkdirs(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def unlink(self) -> None:
        self.path.unlink()

    def stat_size(self) -> int:
        return self.path.stat().st_size

    def verify(self) -> None:
        self.verify_size()
        self.verify_data()

    def verify_size(self) -> None:
        st_size = self.stat_size()
        if st_size != self.size:
            raise IOError(
                f"stat size mismatch: {st_size} != {self.size} {self.path}"
            )

    def verify_data(self) -> None:
        data = self.read()
        dlen = len(data)
        if dlen != self.size:
            raise IOError(f"data length mismatch: {dlen} != {self.size}")
        if data != self.data:
            raise IOError(f"data mismatch at {self.path}")

    def verify_noent(self) -> None:
        has_stat = False
        try:
            self.path.stat()
            has_stat = True
        except FileNotFoundError:
            pass
        if has_stat:
            raise IOError(f"still exists: {self.path}")


def _make_pathname(base: pathlib.Path, idx: int) -> pathlib.Path:
    return base / str(idx)


def _make_datapath(base: pathlib.Path, idx: int, size: int) -> DataPath:
    return DataPath(_make_pathname(base, idx), size)


def _make_datasets(
    base: pathlib.Path, size: int, count: int
) -> typing.List[DataPath]:
    return [_make_datapath(base, idx, size) for idx in range(0, count)]


def _run_checks(dsets: typing.List[DataPath]) -> None:
    for dset in dsets:
        dset.mkdirs()
    for dset in dsets:
        dset.write()
    for dset in dsets:
        dset.verify()
    for dset in dsets:
        dset.overwrite()
    for dset in dsets:
        dset.verify()
    for dset in dsets:
        dset.unlink()
    for dset in dsets:
        dset.verify_noent()


def _check_io_consistency(rootdir: str) -> None:
    base = None
    try:
        print("\n")
        base = pathlib.Path(rootdir) / "test_io_consistency"
        base.mkdir(parents=True)
        # Case-1: single 4K file
        _run_checks(_make_datasets(base, 4096, 1))
        # Case-2: single 16M file
        _run_checks(_make_datasets(base, 2**24, 1))
        # Case-3: few 1M files
        _run_checks(_make_datasets(base, 2**20, 10))
        # Case-4: many 1K files
        _run_checks(_make_datasets(base, 1024, 100))
    except Exception as ex:
        print("Error while executing test_io_consistency: %s", ex)
        raise
    finally:
        if base:
            shutil.rmtree(base, ignore_errors=True)


def _generate_random_bytes(size: int) -> bytes:
    """
    Creates bytes-sequence filled with random values.

    In order to avoid exhausting random pool (which causes high CPU usage)
    re-use the first page of current random buffer to re-construct larger
    one, thus creating only "pseudo" random bytes instead of true random.

    Needed because random.randbytes() is available only in Python>=3.9.
    """
    rbytes = bytearray(0)
    while len(rbytes) < size:
        rnd = random.randint(0, 0xFFFFFFFFFFFFFFFF)
        rba = bytearray(rnd.to_bytes(8, "big"))
        if len(rbytes) < 4096:
            rbytes = rbytes + rba
        else:
            rbytes = rba + rbytes + rba + rbytes
    return rbytes[:size]


def _reset_random_seed() -> None:
    now = datetime.datetime.now()
    seed = int(now.year * now.day * now.hour * now.minute / (now.second + 1))
    random.seed(seed)


def check_io_consistency(rootdir: str) -> None:
    _reset_random_seed()
    _check_io_consistency(rootdir)