import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# ==========================================
# 1. CONFIGURATION
# ==========================================
OUTPUT_FILENAME = "mock_snapshot.csv"
START_DATE = datetime(2024, 1, 1)
DAYS_TO_SIMULATE = 60  # ~2 months

# Initial State
INITIAL_ID_COUNT = 1000

# Daily Variance Config
# Changes = Updates + Deletes
AVG_DAILY_CHANGES = 50
CHANGES_JITTER = 6  # +/- 6

# Creates = Brand new IDs
AVG_DAILY_CREATES = 30
CREATES_JITTER = 3  # +/- 3

UPDATE_RATIO = (
    0.8  # Ratio of updates to total changes (0.8 means 80% updates, 20% deletes)
)

ANOMALY_EVENTS = {
    # Example: Massive Update Spike on Jan 10
    datetime(2024, 1, 10): {"type": "update", "volume": 700},
    # Example: Massive Create Spike on Jan 15
    datetime(2024, 1, 15): {"type": "create", "volume": 700},
    # Example: Massive Delete Spike on Jan 20
    datetime(2024, 1, 20): {"type": "delete", "volume": 700},
    # Example: moderate Update Spike on Feb 1
    datetime(2024, 2, 1): {"type": "update", "volume": 200},
    # Example: moderate Create Spike on Feb 6
    datetime(2024, 2, 6): {"type": "create", "volume": 200},
    # Example: moderate Delete Spike on Feb 11
    datetime(2024, 2, 11): {"type": "delete", "volume": 200},
}

# Random Seed for reproducibility
random.seed(42)
np.random.seed(42)

# ==========================================
# 2. STATE MANAGEMENT
# ==========================================
history_log = []
active_ids_map = {}
global_id_counter = 1

# ==========================================
# 3. INITIALIZATION (Day 0)
# ==========================================
print(f"Initializing with {INITIAL_ID_COUNT} base IDs...")

for _ in range(INITIAL_ID_COUNT):
    row = {
        "id": global_id_counter,
        "dbt_valid_from": START_DATE,
        "dbt_valid_to": pd.NaT,
        "type": "initial_load",
    }
    history_log.append(row)
    active_ids_map[global_id_counter] = len(history_log) - 1
    global_id_counter += 1

# ==========================================
# 4. DAILY SIMULATION LOOP
# ==========================================
current_date = START_DATE

for day in range(1, DAYS_TO_SIMULATE + 1):
    current_date += timedelta(days=1)

    # -------------------------------------------------
    # PART A: BASELINE PROCESSING (Normal Noise)
    # -------------------------------------------------
    n_changes = AVG_DAILY_CHANGES + random.randint(-CHANGES_JITTER, CHANGES_JITTER)
    n_creates = AVG_DAILY_CREATES + random.randint(-CREATES_JITTER, CREATES_JITTER)

    available_ids = list(active_ids_map.keys())
    if n_changes > len(available_ids):
        n_changes = len(available_ids)

    # --- HANDLE BASELINE CHANGES (UPDATES & DELETES) ---
    if n_changes > 0:
        ids_to_change = random.sample(available_ids, n_changes)

        # Split between Update and Delete based on UPDATE_RATIO
        split_index = int(n_changes * UPDATE_RATIO)
        ids_to_update = ids_to_change[:split_index]
        ids_to_delete = ids_to_change[split_index:]

        # 1. Process UPDATES
        for uid in ids_to_update:
            row_idx = active_ids_map[uid]
            history_log[row_idx]["dbt_valid_to"] = current_date
            new_row = {
                "id": uid,
                "dbt_valid_from": current_date,
                "dbt_valid_to": pd.NaT,
                "type": "updated_row",
            }
            history_log.append(new_row)
            active_ids_map[uid] = len(history_log) - 1

        # 2. Process DELETES
        for uid in ids_to_delete:
            row_idx = active_ids_map[uid]
            history_log[row_idx]["dbt_valid_to"] = current_date
            history_log[row_idx]["type"] = "deleted_row"
            del active_ids_map[uid]

    # --- HANDLE BASELINE CREATES ---
    for _ in range(n_creates):
        new_row = {
            "id": global_id_counter,
            "dbt_valid_from": current_date,
            "dbt_valid_to": pd.NaT,
            "type": "new_create",
        }
        history_log.append(new_row)
        active_ids_map[global_id_counter] = len(history_log) - 1
        global_id_counter += 1

    # -------------------------------------------------
    # PART B: ANOMALY INJECTION (Additive)
    # -------------------------------------------------
    if current_date in ANOMALY_EVENTS:
        anomaly = ANOMALY_EVENTS[current_date]
        atype = anomaly["type"]
        volume = anomaly["volume"]

        print(
            f"Injecting Anomaly on {current_date.date()}: Type={atype}, Volume={volume}"
        )

        # Refetch available IDs because baseline might have modified them
        available_ids = list(active_ids_map.keys())

        if atype == "create":
            for _ in range(volume):
                new_row = {
                    "id": global_id_counter,
                    "dbt_valid_from": current_date,
                    "dbt_valid_to": pd.NaT,
                    "type": "anomaly_create",
                }
                history_log.append(new_row)
                active_ids_map[global_id_counter] = len(history_log) - 1
                global_id_counter += 1

        elif atype == "delete":
            target_vol = min(volume, len(available_ids))
            ids_to_kill = random.sample(available_ids, target_vol)

            for uid in ids_to_kill:
                row_idx = active_ids_map[uid]
                history_log[row_idx]["dbt_valid_to"] = current_date
                history_log[row_idx]["type"] = "anomaly_deleted_row"
                del active_ids_map[uid]

        elif atype == "update":
            target_vol = min(volume, len(available_ids))
            ids_to_upd = random.sample(available_ids, target_vol)

            for uid in ids_to_upd:
                row_idx = active_ids_map[uid]
                # Close old
                history_log[row_idx]["dbt_valid_to"] = current_date

                # Open new
                new_row = {
                    "id": uid,
                    "dbt_valid_from": current_date,
                    "dbt_valid_to": pd.NaT,
                    "type": "anomaly_updated_row",
                }
                history_log.append(new_row)
                active_ids_map[uid] = len(history_log) - 1

# ==========================================
# 5. EXPORT
# ==========================================
df = pd.DataFrame(history_log)

# Keep datetimes without microseconds and convert to ISO 8601 for TIMESTAMP
df["dbt_valid_from"] = df["dbt_valid_from"].apply(
    lambda x: x.replace(microsecond=0).isoformat()
)
df["dbt_valid_to"] = df["dbt_valid_to"].apply(
    lambda x: x.replace(microsecond=0).isoformat() if pd.notna(x) else None
)

# Sort by ID and Valid From
df = df.sort_values(by=["id", "dbt_valid_from"])

# Reorder columns
df = df[["id", "dbt_valid_from", "dbt_valid_to", "type"]]

print("Simulation Complete.")
print(f"  - Total Rows Generated: {len(df)}")
print(f"  - Final Active ID count: {len(active_ids_map)}")
print(f"  - Final Max ID: {global_id_counter - 1}")

# Export to CSV
df.to_csv(OUTPUT_FILENAME, index=False, na_rep="")  # empty strings for NULLs
print(f"Exported to {OUTPUT_FILENAME}")
