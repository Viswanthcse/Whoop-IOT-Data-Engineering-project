import os
import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Configuration
NUM_USERS = 50
DAYS_OF_DATA = 30
OUTPUT_DIR = "../unstructured_data"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

ACTIVITY_TYPES = ["Running", "Cycling", "Weight Training", "Yoga", "Swimming", "CrossFit", "HIIT"]
FIRMWARE_VERSIONS = ["v1.2.0", "v1.2.1", "v1.3.0", "v2.0.0-beta"]

def generate_user_profile():
    return {
        "user_id": str(uuid.uuid4()),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "dob": fake.date_of_birth(minimum_age=18, maximum_age=65).isoformat(),
        "gender": random.choice(["M", "F", "Other"]),
        "height_cm": round(random.uniform(150.0, 200.0), 1),
        "weight_kg": round(random.uniform(50.0, 110.0), 1),
        "device_firmware": random.choice(FIRMWARE_VERSIONS)
    }

def generate_daily_metrics(date_str):
    # Introduce some "messiness" - sometimes sensors fail
    sensor_failure = random.random() < 0.05
    
    if sensor_failure:
        return {
            "date": date_str,
            "error": "SENSOR_READ_ERROR"
        }
    
    total_sleep_seconds = random.randint(14400, 36000) # 4 to 10 hours
    deep_sleep = int(total_sleep_seconds * random.uniform(0.15, 0.25))
    rem_sleep = int(total_sleep_seconds * random.uniform(0.20, 0.25))
    light_sleep = total_sleep_seconds - deep_sleep - rem_sleep
    awake_time = random.randint(0, 3600)
    
    return {
        "date": date_str,
        "sleep": {
            "total_sleep_seconds": total_sleep_seconds,
            "deep_sleep_seconds": deep_sleep,
            "rem_sleep_seconds": rem_sleep,
            "light_sleep_seconds": light_sleep,
            "awake_seconds": awake_time
        },
        "vitals": {
            "rhr": random.randint(45, 85),
            "hrv_avg": round(random.uniform(25.0, 120.0), 2),
            "respiratory_rate": round(random.uniform(12.0, 20.0), 1),
            "spO2": round(random.uniform(94.0, 100.0), 1)
        },
        "recovery_score": random.randint(1, 100),
        "strain_score": round(random.uniform(0.0, 21.0), 1)
    }

def generate_workouts(date_str):
    # Some days have 0 workouts, some have 1, rarely 2
    num_workouts = random.choices([0, 1, 2], weights=[0.4, 0.5, 0.1])[0]
    workouts = []
    
    for _ in range(num_workouts):
        start_hour = random.randint(5, 20)
        start_time = f"{date_str}T{start_hour:02d}:00:00Z"
        end_time = f"{date_str}T{start_hour+1:02d}:{random.randint(10,59):02d}:00Z"
        
        workouts.append({
            "workout_id": str(uuid.uuid4()),
            "start_time": start_time,
            "end_time": end_time,
            "activity_type": random.choice(ACTIVITY_TYPES),
            "max_hr": random.randint(140, 190),
            "avg_hr": random.randint(110, 160),
            "calories_burned": random.randint(150, 800)
        })
    return workouts

def main():
    print(f"Generating Simulation Data for {NUM_USERS} users over {DAYS_OF_DATA} days...")
    users = [generate_user_profile() for _ in range(NUM_USERS)]
    
    base_date = datetime.now() - timedelta(days=DAYS_OF_DATA)
    
    for user in users:
        # Simulate a device sending a large JSON payload containing days of synced data
        payload = {
            "sync_id": str(uuid.uuid4()),
            "sync_timestamp": datetime.now().isoformat(),
            "user_profile": user,
            "daily_summaries": [],
            "workouts": []
        }
        
        for i in range(DAYS_OF_DATA):
            current_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            
            # Simulate skipping days (device battery died, forgot to wear)
            if random.random() < 0.10:
                continue 
            
            daily_metrics = generate_daily_metrics(current_date)
            payload["daily_summaries"].append(daily_metrics)
            
            workouts = generate_workouts(current_date)
            payload["workouts"].extend(workouts)
            
        # Write payload to a JSON file (Simulating a file dropped in GCS or sent via API)
        file_name = f"sync_payload_{user['user_id']}_{payload['sync_timestamp'].replace(':', '-')}.json"
        file_path = os.path.join(OUTPUT_DIR, file_name)
        
        with open(file_path, "w") as f:
            json.dump(payload, f, indent=4)
            
    print(f"Generated {NUM_USERS} payload files in '{OUTPUT_DIR}'")

if __name__ == "__main__":
    main()
