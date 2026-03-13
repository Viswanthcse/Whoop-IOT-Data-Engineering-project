from fastapi import FastAPI, Request
from pydantic import BaseModel
from typing import List
import time

app = FastAPI(title="Sensor Streaming API to BigTable (Cloud Run)")

class HeartRatePoint(BaseModel):
    user_id: str
    timestamp: str  # ISO8601
    heart_rate_bpm: int
    skin_temp_celsius: float

@app.post("/stream")
async def stream_sensor_data(points: List[HeartRatePoint]):
    """
    Accepts real-time streaming data from fitness devices.
    In reality, this API writes directly into Cloud Bigtable rows.
    """
    for point in points:
        # Construct Bigtable row key: user_id#timestamp
        row_key = f"{point.user_id}#{point.timestamp}"
        
        # Mocking Bigtable Write Operation:
        # table.mutate_rows([ RowMutation(row_key, set_cell('metrics', 'hr', point.heart_rate_bpm)) ])
        print(f"[BIGTABLE WRITE] RowKey: {row_key} -> HR: {point.heart_rate_bpm}, Temp: {point.skin_temp_celsius}")
        
    return {"status": "Data ingested into Bigtable successfully", "points_received": len(points)}
