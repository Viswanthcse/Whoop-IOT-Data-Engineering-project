import time
import requests
import random
import uuid
from datetime import datetime

# URL of our locally running streaming API
STREAMING_API_URL = "http://localhost:8001/stream"

def start_streaming():
    print("Starting continuous sensor streaming...")
    user_ids = [str(uuid.uuid4()) for _ in range(5)]  # Simulate 5 active users exercising right now
    
    try:
        while True:
            payload = []
            now = datetime.utcnow().isoformat() + "Z"
            for uid in user_ids:
                payload.append({
                    "user_id": uid,
                    "timestamp": now,
                    "heart_rate_bpm": random.randint(120, 180), # Simulating mid-workout
                    "skin_temp_celsius": round(random.uniform(36.5, 38.0), 2)
                })
            
            try:
                response = requests.post(STREAMING_API_URL, json=payload)
                if response.status_code == 200:
                    print(f"Successfully streamed {len(payload)} biometric packets.")
            except requests.exceptions.ConnectionError:
                print("Connection failed. Is the API running on port 8001?")
                
            time.sleep(1) # Send data every 1 second
            
    except KeyboardInterrupt:
        print("Streaming stopped.")

if __name__ == "__main__":
    start_streaming()
