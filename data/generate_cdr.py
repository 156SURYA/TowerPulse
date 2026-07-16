import csv, random, time
from datetime import datetime

CALL_TYPES = ["VOICE", "SMS", "DATA"]
TOWERS = ["T1", "T2", "T3", "T4"]

for batch in range(1, 21):  # 20 batches
    filename = f"cdr_batch_{batch}.csv"
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["call_id","caller","receiver","call_type","duration","call_date","tower_id"])

        for i in range(500):  # 500 records per batch
            writer.writerow([
                f"C{batch}_{i}",
                random.randint(9000000000, 9999999999),
                random.randint(9000000000, 9999999999),
                random.choice(CALL_TYPES),
                random.randint(10, 600),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                random.choice(TOWERS)
            ])

    print(f"Generated {filename}")
    time.sleep(10)
