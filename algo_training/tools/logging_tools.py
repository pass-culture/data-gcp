import psutil


def log_memory_info():
    return psutil.virtual_memory()._asdict()
