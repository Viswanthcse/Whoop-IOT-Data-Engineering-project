import os
import json
import random
import uuid
import time
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Configuration
NUM_USERS = 20
DAYS_OF_DATA = 7
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "local_gcs_bucket"))

os.makedirs(OUTPUT_DIR, exist_ok=True)

ACTIVITY_TYPES = ["Running", "Cycling", "Weight Training", "Yoga", "Swimming", "CrossFit", "HIIT"]

def generate_user_profile():
    return {
        "user_id": str(uuid.uuid4()),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "dob": fake.date_of_birth(minimum_age=18, maximum_age=65).isoformat(),
        "gender": random.choice(["M", "F", "Other"]),
        "height_cm": round(random.uniform(150.0, 200.0), 1),
        "weight_kg": round(random.uniform(50.0, 110.0), 1),
        "device_firmware": "v2.1.0"
    }

def main():
    print(f"Generating unstructured payload drops into simulated GCS bucket...")
    users = [generate_user_profile() for _ in range(NUM_USERS)]
    
    base_date = datetime.now() - timedelta(days=DAYS_OF_DATA)
    
    for user in users:
        payload = {
            "sync_id": str(uuid.uuid4()),
            "sync_timestamp": datetime.now().isoformat(),
            "user_profile": user,
            "daily_summaries": [],
            "workouts": []
        }
        
        for i in range(DAYS_OF_DATA):
            current_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            
            # Daily Metric
            payload["daily_summaries"].append({
                "date": current_date,
                "sleep": {"total_sleep_seconds": random.randint(14400, 36000)},
                "vitals": {"rhr": random.randint(45, 85), "hrv_avg": round(random.uniform(25.0, 120.0), 2)},
                "recovery_score": random.randint(1, 100),
                "strain_score": round(random.uniform(0.0, 21.0), 1)
            })
            
            # Workout
            if random.random() > 0.3:
                payload["workouts"].append({
                    "workout_id": str(uuid.uuid4()),
                    "start_time": f"{current_date}T10:00:00Z",
                    "end_time": f"{current_date}T11:00:00Z",
                    "activity_type": random.choice(ACTIVITY_TYPES),
                    "calories_burned": random.randint(150, 800)
                })
                
        file_name = f"sync_payload_{user['user_id']}_{payload['sync_timestamp'].replace(':', '-')}.json"
        
        # Write to our local "GCS Bucket" folder
        with open(os.path.join(OUTPUT_DIR, file_name), "w") as f:
            json.dump(payload, f, indent=4)
            
    print(f"Done. Dropped {NUM_USERS} unstructured JSON objects into {OUTPUT_DIR}.")

if __name__ == "__main__":
    main()
