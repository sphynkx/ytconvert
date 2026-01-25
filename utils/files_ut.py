import os
import shutil


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def atomic_write_bytes(path, data):
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def safe_unlink(path):
    try:
        os.unlink(path)
    except FileNotFoundError:
        return
    except Exception:
        return


def rm_tree(path):
    try:
        shutil.rmtree(path, ignore_errors=True)
    except Exception:
        return