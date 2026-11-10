import multiprocessing, os, signal, time
import pytest
from openalex_api.ingest import lock, local_lock_path


def hold(path, pipe):
    with lock(path):
        pipe.send(True)
        time.sleep(30)


def test_lock_exclusion_and_sigkill_recovery(tmp_path):
    path = tmp_path / "operation.lock"
    parent, child = multiprocessing.Pipe()
    p = multiprocessing.Process(target=hold, args=(path, child))
    p.start()
    try:
        assert parent.poll(5) and parent.recv()
        with pytest.raises(RuntimeError):
            with lock(path):
                pass
        os.kill(p.pid, signal.SIGKILL)
        p.join(timeout=5)
        with lock(path):
            assert local_lock_path(path).is_file()
    finally:
        if p.is_alive():
            p.kill()
            p.join()
