# Is `:memory:` mode compatible with the Hybrid AsyncIO Architecture?

**Yes, absolutely.**

The Hybrid AsyncIO architecture (using `ProcessPoolExecutor` for reports + `ThreadPoolExecutor` for queries) is fully compatible with DuckDB's in-memory mode (`:memory:`).

## Why it works

1.  **Independent Connections**: Even in `:memory:` mode, DuckDB allows multiple connections.
2.  **Thread Safety**: With the recent fix using `conn.cursor()`, each thread gets its own isolated cursor, which works perfectly whether the underlying database is a file (`database.duckdb`) or in RAM (`:memory:`).
3.  **Process Isolation**: Since your architecture spawns a **new process** for each report (`process_report_worker`), each process gets its own **completely independent** in-memory DuckDB instance. They do not share memory.

## Performance Comparison

| Feature | `database.duckdb` (File) | `:memory:` (RAM) |
| :--- | :--- | :--- |
| **Speed** | Slower (Disk I/O for reads/writes) | **Faster** (Pure RAM speed) |
| **Concurrency** | Shared file lock (potential contention) | **Zero contention** (Isolated instances per process) |
| **Persistence** | Data saved to disk | Data lost when process ends |
| **Startup Cost** | Fast (attach to file) | Slower (Must reload all data from BigQuery every time) |

## Trade-off: The "Startup Cost"

The main catch with switching to `:memory:` in your specific architecture is **Data Loading**:

*   **File Mode (`database.duckdb`):** The parent process loads data from BigQuery *once* into the file. The worker processes just connect to that file and read.
*   **Memory Mode (`:memory:`):** Since processes don't share RAM, the parent process **cannot** load data into a `:memory:` DB and expect workers to see it.
    *   **Implication:** Each worker process would have to re-download/re-load the data from BigQuery into its own private RAM. This would be **much slower** overall and incredibly expensive (BigQuery costs).

## Conclusion

While `:memory:` mode is *technically* compatible and faster for queries, **it is likely NOT suitable for your specific architecture** because it would force every single report worker to re-download the dataset.

**Best Practice:** Stick with `database.duckdb` (file mode) so you download once and share with workers, but use the `ThreadPoolExecutor` optimization you just implemented to mask the disk I/O latency.

---

# Evaluation: Strategy "N Persistent Workers with In-Memory DB"

You asked about a specific variation: **Having N persistent database workers, each with a `:memory:` dataset, consuming a queue of reports.**

## Architecture
1.  **Initialization:** Start $N$ worker processes (e.g., 4 workers).
2.  **Hydration:** Each of the $N$ workers independently downloads the data from BigQuery into its own RAM (`:memory:`).
3.  **Execution:** The Main Process sends report tasks to a Queue. Workers pick a task, generate the report, and repeat.

## Pros & Cons

### ✅ Pros
1.  **Blazing Fast Queries:** Once loaded, queries run in microseconds because data is in pure RAM with no disk I/O.
2.  **Amortized Loading Cost:** If you have 100 reports and 4 workers, each worker processes 25 reports. The cost of downloading data is paid only once per worker, not once per report.
3.  **Isolation:** If one worker crashes, others survive.

### ❌ Cons
1.  **Massive RAM Usage:** This is the dealbreaker. If your dataset is 2GB:
    *   4 Workers = $4 \times 2\text{GB}$ RAM (+ overhead).
    *   If you scale to 10 workers to go faster, you need 20GB+ RAM.
2.  **High Startup Latency:** The system hangs at the start while *every* worker downloads the data. You don't see the first report until the slowest download finishes.
3.  **Complexity:** You need to implement a persistent worker pool (e.g., `celery` or a custom `multiprocessing.Queue` loop) rather than the simple `ProcessPoolExecutor`.

## Verdict
**Viable ONLY IF:**
1.  Your dataset is **small** (< 500MB).
2.  You have **plenty of RAM**.
3.  You have a **large number of reports** (e.g., > 50) to justify the startup time.

**For your current setup (files + DuckDB):**
The current **File-Based approach** is superior because DuckDB is designed to be an "OLAP SQLite". It efficiently caches the file in OS memory (page cache). So, after the first few reads, your `database.duckdb` effectively resides in RAM anyway, shared by all processes via the OS, without the memory penalty of duplicating it $N$ times.