"""
Chip Test Data Generator
Simulates semiconductor chip test events for the NVIDIA pipeline demo.
"""
import json
import random
import uuid
import os
from datetime import datetime, timedelta

FABS = ["TSMC_N4", "TSMC_N5", "Samsung_3nm"]
CHIP_TYPES = ["A100", "H100", "H200", "B100"]
TEST_TYPES = ["voltage_swing", "thermal", "memory_bandwidth", "signal_integrity"]

FAB_PASS_RATES = {
    "TSMC_N4": 0.96,
    "TSMC_N5": 0.95,
    "Samsung_3nm": 0.89,
}

MEASUREMENT_RANGES = {
    "voltage_swing": (0.8, 1.4),
    "thermal": (45, 95),
    "memory_bandwidth": (1500, 3200),
    "signal_integrity": (0.85, 1.0),
}

def generate_event(timestamp):
    chip_type = random.choice(CHIP_TYPES)
    fab = random.choice(FABS)
    test = random.choice(TEST_TYPES)
    pass_rate = FAB_PASS_RATES[fab]
    passed = random.random() < pass_rate
    low, high = MEASUREMENT_RANGES[test]
    measurement = round(random.uniform(low, high), 3)
    
    return {
        "event_id": str(uuid.uuid4()),
        "chip_id": f"{chip_type}-{random.randint(10000, 99999)}",
        "chip_type": chip_type,
        "fab": fab,
        "test_type": test,
        "measurement": measurement,
        "passed": passed,
        "timestamp": timestamp.isoformat(),
        "operator_id": f"OP_{random.randint(100, 999)}"
    }

if __name__ == "__main__":
    base_date = datetime.utcnow() - timedelta(days=7)
    
    for day_offset in range(7):
        day = base_date + timedelta(days=day_offset)
        events = []
        for _ in range(10000):
            seconds_offset = random.randint(0, 86399)
            ts = day.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=seconds_offset)
            events.append(generate_event(ts))
        
        partition_path = f"chip_tests/year={day.year}/month={day.month:02d}/day={day.day:02d}"
        filename = f"{partition_path}/events.json"
        os.makedirs(partition_path, exist_ok=True)
        
        with open(filename, "w") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
        
        print(f"Generated {len(events)} events -> {filename}")
    
    print("\nDone. Total: 70,000 events across 7 days.")
