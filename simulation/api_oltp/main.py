import os
import json
import asyncio
from fastapi import FastAPI, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from datetime import datetime
from database import engine, SessionLocal, init_db, get_db
import models

app = FastAPI(title="OLTP Sync API (Cloud Run)")
init_db()

LOCAL_GCS_BUCKET = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "local_gcs_bucket"))

@app.post("/process_gcs_bucket")
def process_gcs_bucket(db: Session = Depends(get_db)):
    """
    Simulates a Cloud Run service that gets triggered whenever a file is dropped in GCS.
    For this simulation, we'll manually call this endpoint to sweep the directory and parse into DB.
    """
    if not os.path.exists(LOCAL_GCS_BUCKET):
        return {"status": "No files found."}

    processed_count = 0
    for filename in os.listdir(LOCAL_GCS_BUCKET):
        if not filename.endswith(".json"):
            continue
            
        filepath = os.path.join(LOCAL_GCS_BUCKET, filename)
        with open(filepath, "r") as f:
            payload = json.load(f)
            
        user_data = payload.get("user_profile", {})
        user_id = user_data.get("user_id")
        
        if not user_id:
            continue
            
        # 1. Update User
        user = db.query(models.User).filter(models.User.id == user_id).first()
        if not user:
            dob = datetime.strptime(user_data.get("dob", "1990-01-01")[:10], "%Y-%m-%d").date()
            user = models.User(
                id=user_id, 
                first_name=user_data.get("first_name"),
                last_name=user_data.get("last_name"),
                dob=dob,
                gender=user_data.get("gender"),
                height_cm=user_data.get("height_cm"),
                weight_kg=user_data.get("weight_kg")
            )
            db.add(user)
            
        # 2. Daily Metrics
        for day in payload.get("daily_summaries", []):
            date_obj = datetime.strptime(day["date"], "%Y-%m-%d").date()
            metric = db.query(models.DailyMetric).filter(models.DailyMetric.user_id == user_id, models.DailyMetric.date == date_obj).first()
            if not metric:
                sleep = day.get("sleep", {})
                vitals = day.get("vitals", {})
                metric = models.DailyMetric(
                    user_id=user_id, 
                    date=date_obj, 
                    recovery_score=day.get("recovery_score"),
                    strain_score=day.get("strain_score"),
                    total_sleep_seconds=sleep.get("total_sleep_seconds", 0),
                    deep_sleep_seconds=sleep.get("deep_sleep_seconds", 0),
                    rem_sleep_seconds=sleep.get("rem_sleep_seconds", 0),
                    light_sleep_seconds=sleep.get("light_sleep_seconds", 0),
                    awake_seconds=sleep.get("awake_seconds", 0),
                    rhr=vitals.get("rhr", 0),
                    hrv_avg=vitals.get("hrv_avg", 0.0),
                    respiratory_rate=vitals.get("respiratory_rate", 0.0),
                    error_flag=day.get("error", None)
                )
                db.add(metric)
                
        # 3. Workouts
        for w in payload.get("workouts", []):
            wid = w.get("workout_id")
            workout = db.query(models.Workout).filter(models.Workout.id == wid).first()
            if not workout:
                workout_start = datetime.strptime(w.get("start_time"), "%Y-%m-%dT%H:%M:%SZ") if "start_time" in w else datetime.utcnow()
                workout_end = datetime.strptime(w.get("end_time"), "%Y-%m-%dT%H:%M:%SZ") if "end_time" in w else datetime.utcnow()
                workout = models.Workout(
                    id=wid, 
                    user_id=user_id, 
                    activity_type=w.get("activity_type"),
                    start_time=workout_start,
                    end_time=workout_end
                )
                db.add(workout)
                
        db.commit()
        
        # After processing, archive or delete the file. We'll rename it to simulate processed.
        os.rename(filepath, filepath + ".processed")
        processed_count += 1
        
    return {"status": "success", "files_processed": processed_count}

