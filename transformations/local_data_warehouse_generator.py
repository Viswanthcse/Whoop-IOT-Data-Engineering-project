import os
import pandas as pd
import sqlite3
import numpy as np

# Set up paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "simulation", "api_oltp", "cloud_sql_sim.db")
OUT_DIR = os.path.join(BASE_DIR, "..", "local_data_warehouse")

os.makedirs(OUT_DIR, exist_ok=True)

def run_local_pipeline():
    print(f"Connecting to local OLTP Database: {DB_PATH}")
    
    if not os.path.exists(DB_PATH):
        print("Error: cloud_sql_sim.db not found. Have you run the simulation API and generated data yet?")
        return

    conn = sqlite3.connect(DB_PATH)

    # 1. EXTRACT
    print("Extracting tables...")
    users_df = pd.read_sql("SELECT * FROM users", conn)
    daily_df = pd.read_sql("SELECT * FROM daily_metrics", conn)
    workouts_df = pd.read_sql("SELECT * FROM workouts", conn)
    conn.close()

    # 2. TRANSFORM
    print("Transforming dim_users...")
    # Setup basic dim_users
    dim_users = users_df.copy()
    dim_users.rename(columns={'id': 'user_id'}, inplace=True)
    dim_users['current_firmware'] = 'v2.1.0'

    # Calculate Age
    current_year = pd.Timestamp.now().year
    dim_users['dob'] = pd.to_datetime(dim_users['dob'])
    dim_users['age'] = current_year - dim_users['dob'].dt.year

    # Calculate BMR
    def calc_bmr(row):
        if pd.isna(row['weight_kg']) or pd.isna(row['height_cm']):
            return None
        if row['gender'] == 'M':
            return 88.362 + (13.397 * row['weight_kg']) + (4.799 * row['height_cm']) - (5.677 * row['age'])
        else:
            return 447.593 + (9.247 * row['weight_kg']) + (3.098 * row['height_cm']) - (4.330 * row['age'])

    dim_users['bmr'] = dim_users.apply(calc_bmr, axis=1)
    
    # Calculate Performance Age
    dim_users['performance_age'] = np.where(dim_users['bmr'].notna(), dim_users['age'] - 2, dim_users['age'])

    # Clean dim_users columns
    dim_users = dim_users[['user_id', 'first_name', 'last_name', 'age', 'gender', 'height_cm', 'weight_kg', 'bmr', 'performance_age', 'current_firmware']]

    print("Transforming fact_daily_health...")
    fact_daily = daily_df.copy()
    fact_daily['date'] = pd.to_datetime(fact_daily['date'])
    fact_daily['total_sleep_hours'] = fact_daily['total_sleep_seconds'] / 3600
    fact_daily['deep_sleep_hours'] = fact_daily['deep_sleep_seconds'] / 3600
    fact_daily['rem_sleep_hours'] = fact_daily['rem_sleep_seconds'] / 3600
    fact_daily['light_sleep_hours'] = fact_daily['light_sleep_seconds'] / 3600
    fact_daily['awake_hours'] = fact_daily['awake_seconds'] / 3600

    # Calculate 7-day rolling fatigue
    fact_daily = fact_daily.sort_values(by=['user_id', 'date'])
    
    # Rolling averages over the past 7 days per user
    fact_daily['rolling_strain'] = fact_daily.groupby('user_id')['strain_score'].transform(lambda x: x.rolling(7, min_periods=1).sum())
    fact_daily['rolling_recovery'] = fact_daily.groupby('user_id')['recovery_score'].transform(lambda x: x.rolling(7, min_periods=1).sum())
    
    # Fatigue formula: Strain / Recovery
    fact_daily['accumulated_fatigue'] = (fact_daily['rolling_strain'] / fact_daily['rolling_recovery'].replace(0, 1)) * 10
    
    fact_daily = fact_daily[['user_id', 'date', 'total_sleep_hours', 'deep_sleep_hours', 'rem_sleep_hours', 'light_sleep_hours', 'awake_hours', 'rhr', 'hrv_avg', 'respiratory_rate', 'recovery_score', 'strain_score', 'accumulated_fatigue', 'error_flag']]

    print("Transforming fact_workouts...")
    fact_workouts = workouts_df.copy()
    fact_workouts.rename(columns={'id': 'workout_id'}, inplace=True)
    fact_workouts['start_time'] = pd.to_datetime(fact_workouts['start_time'])
    fact_workouts['end_time'] = pd.to_datetime(fact_workouts['end_time'])
    fact_workouts['workout_date'] = fact_workouts['start_time'].dt.date
    fact_workouts['duration_minutes'] = (fact_workouts['end_time'] - fact_workouts['start_time']).dt.total_seconds() / 60
    
    def get_time_of_day(hour):
        if hour < 12: return "Morning"
        elif hour < 17: return "Afternoon"
        else: return "Evening"
        
    fact_workouts['time_of_day'] = fact_workouts['start_time'].dt.hour.apply(get_time_of_day)

    # 3. LOAD
    print("Loading into local Data Warehouse (CSV Format for PowerBI/Tableau)...")
    dim_users.to_csv(os.path.join(OUT_DIR, "dim_users.csv"), index=False)
    fact_daily.to_csv(os.path.join(OUT_DIR, "fact_daily_health.csv"), index=False)
    fact_workouts.to_csv(os.path.join(OUT_DIR, "fact_workouts.csv"), index=False)
    
    print(f"Success! Your Star Schema is ready in the '{OUT_DIR}' folder.")
    print("You can now connect PowerBI directly to these CSVs!")

if __name__ == "__main__":
    run_local_pipeline()
