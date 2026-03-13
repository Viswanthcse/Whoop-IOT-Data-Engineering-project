import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, monotonically_increasing_id

def perform_etl(raw_gcs_path, bq_dataset, temp_gcs_bucket):
    """
    Reads raw Whoop data from GCS, transforms into a Star Schema, 
    and writes to BigQuery.
    """
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("WhoopFitnessETL") \
        .getOrCreate()
        
    # Set GCP temporary bucket for BigQuery writes
    spark.conf.set("temporaryGcsBucket", temp_gcs_bucket)
    
    print(f"Reading raw data from: {raw_gcs_path}")
    df = spark.read.csv(raw_gcs_path, header=True, inferSchema=True)
    
    print("Transforming: Creating Dimension & Fact tables...")

    # ---------------------------------------------------------
    # 1. DIMENSION: Users
    # ---------------------------------------------------------
    dim_users = df.select(
        "user_id", "age", "gender", "weight_kg", "height_cm", 
        "fitness_level", "primary_sport"
    ).dropDuplicates(["user_id"])
    
    # ---------------------------------------------------------
    # 2. DIMENSION: Date
    # ---------------------------------------------------------
    dim_date_raw = df.select("date", "day_of_week").dropDuplicates(["date"])
    # Add a surrogate key for dates
    dim_date = dim_date_raw.orderBy("date").withColumn("date_id", monotonically_increasing_id())

    # ---------------------------------------------------------
    # 3. FACT: Daily Health & Recovery
    # ---------------------------------------------------------
    # We join with dim_date to get the date_id
    df_with_date_id = df.join(dim_date, on="date", how="left")
    
    fact_daily_health = df_with_date_id.select(
        "user_id", 
        "date_id",
        col("date").alias("health_date"),
        "recovery_score", "day_strain", "sleep_hours", "sleep_efficiency",
        "sleep_performance", "light_sleep_hours", "rem_sleep_hours", 
        "deep_sleep_hours", "wake_ups", "time_to_fall_asleep_min", 
        "hrv", "resting_heart_rate", "hrv_baseline", "rhr_baseline",
        "respiratory_rate", "skin_temp_deviation", "calories_burned"
    )
    
    # ---------------------------------------------------------
    # 4. FACT: Workouts
    # ---------------------------------------------------------
    # Filter only days where a workout was completed
    fact_workouts = df_with_date_id.filter(col("workout_completed") == 1).select(
        "user_id",
        "date_id",
        col("date").alias("workout_date"),
        "activity_type", "activity_duration_min", "activity_strain", 
        "avg_heart_rate", "max_heart_rate", "activity_calories", 
        "hr_zone_1_min", "hr_zone_2_min", "hr_zone_3_min", 
        "hr_zone_4_min", "hr_zone_5_min", "workout_time_of_day"
    )

    print("Loading: Writing DataFrames to BigQuery...")
    # Write to BigQuery Using WriteTruncate (Overwrite existing data)
    
    tables = {
        "dim_users": dim_users,
        "dim_date": dim_date,
        "fact_daily_health": fact_daily_health,
        "fact_workouts": fact_workouts
    }
    
    for table_name, dataframe in tables.items():
        bq_table_path = f"{bq_dataset}.{table_name}"
        print(f"Writing {table_name} to BigQuery table {bq_table_path}...")
        dataframe.write \
            .format("bigquery") \
            .option("table", bq_table_path) \
            .mode("overwrite") \
            .save()
            
    print("ETL Job Completed Successfully!")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw_gcs_path", required=True, help="GCS URI for raw data CSV")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery Dataset name (e.g., project.dataset)")
    parser.add_argument("--temp_gcs_bucket", required=True, help="Temp GCS bucket for Spark BQ connector")
    
    args = parser.parse_args()
    perform_etl(args.raw_gcs_path, args.bq_dataset, args.temp_gcs_bucket)
