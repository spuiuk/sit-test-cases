import threading
import testhelper
from pathlib import Path


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


def check_mnt_stress(root_dir: Path) -> None:
    _stress_test(root_dir, num_clients=5, num_operations=20, file_size=2**22)
    _stress_test(
        root_dir, num_clients=10, num_operations=30, file_size=2**23
    )
    _stress_test(
        root_dir, num_clients=20, num_operations=40, file_size=2**24
    )
    _stress_test(
        root_dir, num_clients=15, num_operations=25, file_size=2**25
    )
