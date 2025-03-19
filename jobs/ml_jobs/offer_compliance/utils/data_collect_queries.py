import subprocess
from multiprocessing import Pool, cpu_count

import pandas as pd
from loguru import logger


def read_parquet(file):
    logger.info(f"Read.. {file}")
    return pd.read_parquet(file)


def read_from_gcs(storage_path, table_name, parallel=True, max_process=None):
    if max_process is None:
        max_process = cpu_count() - 1
    bucket_name = f"{storage_path}/{table_name}/*.parquet"
    result = subprocess.run(["gsutil", "ls", bucket_name], stdout=subprocess.PIPE)
    files = [file.strip().decode("utf-8") for file in result.stdout.splitlines()]

    if parallel and len(files) > max_process // 2:
        logger.info(f"Will load {len(files)} with {max_process} processes...")
        with Pool(processes=max_process) as pool:
            return (
                pd.concat(pool.map(read_parquet, files), ignore_index=True)
                .sample(frac=1)
                .reset_index(drop=True)
            )
    else:
        return (
            pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
            .sample(frac=1)
            .reset_index(drop=True)
        )
