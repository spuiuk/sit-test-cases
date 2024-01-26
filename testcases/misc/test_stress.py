import pytest
import threading
import testhelper
import shutil
from pathlib import Path
from .conftest import gen_params, gen_params_premounted


def _perform_file_operations(
    client_id: int, root_dir: Path, num_operations: int, file_size: int
) -> None:
    try:
        for i in range(num_operations):
            file_content = testhelper.generate_random_bytes(file_size)
            path = root_dir / f"testfile_{client_id}_{i}.txt"
            path.write_bytes(file_content)
            file_content_out = path.read_bytes()

            if file_content_out != file_content:
                raise IOError("content mismatch")

            path.unlink()
    except Exception as ex:
        print(f"Error while stress testing with Client {client_id}: %s", ex)
        raise


def _stress_test(
    root_dir: Path, num_clients: int, num_operations: int, file_size: int
) -> None:
    threads = []

    for i in range(num_clients):
        thread = threading.Thread(
            target=_perform_file_operations,
            args=(i, root_dir, num_operations, file_size),
        )
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    print("Stress test complete.")


def _run_stress_tests(directory: Path) -> None:
    directory.mkdir(exist_ok=True)
    try:
        _stress_test(
            directory, num_clients=20, num_operations=40, file_size=2**25
        )
    finally:
        shutil.rmtree(directory, ignore_errors=True)


@pytest.mark.privileged
@pytest.mark.parametrize("setup_mount", gen_params(), indirect=True)
def test_check_mnt_stress(setup_mount: Path) -> None:
    base = setup_mount / "stress-test"
    _run_stress_tests(base)


@pytest.mark.parametrize("test_dir", gen_params_premounted())
def test_check_mnt_stress_premounted(test_dir: Path) -> None:
    base = test_dir / "stress-test"
    _run_stress_tests(base)
