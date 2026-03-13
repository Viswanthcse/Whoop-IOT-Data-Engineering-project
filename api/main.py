from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
import os
import json
from datetime import datetime

from database import engine, SessionLocal, init_db, get_db
import models

app = FastAPI(title="Fitness Device Sync API")

# Initialize database schemas
init_db()

# Simulated GCS bucket upload logic
def upload_to_datalake(payload: dict):
    # In reality, this uses the google-cloud-storage SDK.
    # We will simulate dumping the raw JSON into the "unstructured_data" directory.
    print(f"Background Task: Uploading payload {payload.get('sync_id')} to Data Lake GCS Bucket...")
    # This is handled already by the generation script simulating the drop, but this represents the API's role
    pass


@app.post("/sync")
def sync_device_data(payload: dict, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Endpoint that the mobile phone/device hits to sync data.
    1. Saves the raw unstructured payload to the Data Lake (GCS).
    2. Parses the structured elements into the normalized OLTP SQL Database.
    """
    user_data = payload.get("user_profile")
    if not user_data:
        raise HTTPException(status_code=400, detail="Missing user profile in payload")

    user_id = user_data["user_id"]

    # 1. Update or Create User
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        user = models.User(
            id=user_id,
            first_name=user_data.get("first_name"),
            last_name=user_data.get("last_name"),
            dob=datetime.strptime(user_data.get("dob"), "%Y-%m-%d").date(),
            gender=user_data.get("gender"),
            height_cm=user_data.get("height_cm"),
            weight_kg=user_data.get("weight_kg")
        )
        db.add(user)
    
    # 2. Update Device Info
    device = db.query(models.Device).filter(models.Device.user_id == user_id).first()
    sync_time_str = payload.get("sync_timestamp")
    sync_time = datetime.fromisoformat(sync_time_str) if sync_time_str else datetime.utcnow()
    
    if not device:
        device = models.Device(
            user_id=user_id,
            firmware_version=user_data.get("device_firmware"),
            last_sync=sync_time
        )
        db.add(device)
    else:
        device.firmware_version = user_data.get("device_firmware")
        device.last_sync = sync_time

    # 3. Parse Daily Summaries
    daily_summaries = payload.get("daily_summaries", [])
    for day in daily_summaries:
        date_obj = datetime.strptime(day["date"], "%Y-%m-%d").date()
        
        # Check if record already exists for this user/day combination
        existing_metric = db.query(models.DailyMetric).filter(
            models.DailyMetric.user_id == user_id, 
            models.DailyMetric.date == date_obj
        ).first()

        if not existing_metric:
            metric = models.DailyMetric(user_id=user_id, date=date_obj)
            
            if "error" in day:
                metric.error_flag = day["error"]
            else:
                sleep = day.get("sleep", {})
                vitals = day.get("vitals", {})
                metric.total_sleep_seconds = sleep.get("total_sleep_seconds")
                metric.deep_sleep_seconds = sleep.get("deep_sleep_seconds")
                metric.rem_sleep_seconds = sleep.get("rem_sleep_seconds")
                metric.light_sleep_seconds = sleep.get("light_sleep_seconds")
                metric.awake_seconds = sleep.get("awake_seconds")
                metric.rhr = vitals.get("rhr")
                metric.hrv_avg = vitals.get("hrv_avg")
                metric.respiratory_rate = vitals.get("respiratory_rate")
                metric.recovery_score = day.get("recovery_score")
                metric.strain_score = day.get("strain_score")

            db.add(metric)

    # 4. Parse Workouts
    workouts = payload.get("workouts", [])
    for w in workouts:
        wid = w["workout_id"]
        existing_workout = db.query(models.Workout).filter(models.Workout.id == wid).first()
        if not existing_workout:
            new_workout = models.Workout(
                id=wid,
                user_id=user_id,
                start_time=datetime.strptime(w["start_time"], "%Y-%m-%dT%H:%M:%SZ"),
                end_time=datetime.strptime(w["end_time"], "%Y-%m-%dT%H:%M:%SZ"),
                activity_type=w.get("activity_type"),
                max_hr=w.get("max_hr"),
                avg_hr=w.get("avg_hr"),
                calories_burned=w.get("calories_burned")
            )
            db.add(new_workout)

    db.commit()

    # 5. Background Task: Datalake Upload
    background_tasks.add_task(upload_to_datalake, payload)

    return {"status": "success", "message": "Data synced and persisted to OLTP", "sync_id": payload.get("sync_id")}
