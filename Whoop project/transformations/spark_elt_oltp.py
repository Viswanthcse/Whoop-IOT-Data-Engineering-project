import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Setup detailed logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_pipeline(jdbc_url, jdbc_user, jdbc_password, bq_dataset, temp_gcs_bucket):
    logger.info("Initializing Spark Session for OLTP to BigQuery ETL...")
    spark = SparkSession.builder \
        .appName("Whoop_OLTP_to_BQ") \
        .getOrCreate()

    spark.conf.set("temporaryGcsBucket", temp_gcs_bucket)
    
    # Connection logic
    def read_pg_table(table_name):
        logger.info(f"Extracting table '{table_name}' from Cloud SQL...")
        return spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", jdbc_user) \
            .option("password", jdbc_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

    # 1. EXTRACT
    df_users = read_pg_table("users")
    df_devices = read_pg_table("devices")
    df_daily = read_pg_table("daily_metrics")
    df_workouts = read_pg_table("workouts")

    logger.info("Extraction complete. Commencing transformations...")

    # 2. TRANSFORM: Dimensions
    logger.info("Transforming dim_users. Calculating Age, BMR, and Performance Age...")
    
    # Join users with latest device to get firmware
    window_device = Window.partitionBy("user_id").orderBy(F.col("last_sync").desc())
    latest_devices = df_devices.withColumn("rn", F.row_number().over(window_device)) \
                               .filter(F.col("rn") == 1) \
                               .select("user_id", F.col("firmware_version").alias("current_firmware"))

    dim_users = df_users.join(latest_devices, df_users.id == latest_devices.user_id, "left") \
                        .drop(latest_devices.user_id) \
                        .withColumnRenamed("id", "user_id")

    # Calculate Age
    dim_users = dim_users.withColumn("age", F.floor(F.datediff(F.current_date(), F.col("dob")) / 365.25))

    # Calculate BMR (Harris-Benedict Equation)
    # Men: BMR = 88.362 + (13.397 x weight) + (4.799 x height) - (5.677 x age)
    # Women: BMR = 447.593 + (9.247 x weight) + (3.098 x height) - (4.330 x age)
    dim_users = dim_users.withColumn(
        "bmr",
        F.when(F.col("gender") == "M", 88.362 + (13.397 * F.col("weight_kg")) + (4.799 * F.col("height_cm")) - (5.677 * F.col("age")))
         .when(F.col("gender") == "F", 447.593 + (9.247 * F.col("weight_kg")) + (3.098 * F.col("height_cm")) - (4.330 * F.col("age")))
         .otherwise(None)
    )

    # Calculate Performance Age (Business logic: Age tweaked by RHR. Normally complex, here is a simple approximation)
    dim_users = dim_users.withColumn("performance_age", 
                                     F.when(F.col("bmr").isNotNull(), F.col("age") - 2).otherwise(F.col("age")))
    
    # 3. TRANSFORM: Daily Health Fact
    logger.info("Transforming fact_daily_health. Aggregating sleep and calculating accumulated fatigue...")
    
    fact_daily = df_daily.withColumn("total_sleep_hours", F.col("total_sleep_seconds") / 3600) \
                         .withColumn("deep_sleep_hours", F.col("deep_sleep_seconds") / 3600) \
                         .withColumn("rem_sleep_hours", F.col("rem_sleep_seconds") / 3600) \
                         .withColumn("light_sleep_hours", F.col("light_sleep_seconds") / 3600) \
                         .withColumn("awake_hours", F.col("awake_seconds") / 3600)
    
    # 7-Day Rolling Fatigue logic
    # Fatigue = Sum of strain over 7 days / Sum of recovery score over 7 days * 10
    window_7_days = Window.partitionBy("user_id").orderBy(F.col("date").cast("timestamp").cast("long")).rangeBetween(-7*86400, 0)
    
    fact_daily = fact_daily.withColumn("rolling_strain", F.sum("strain_score").over(window_7_days)) \
                           .withColumn("rolling_recovery", F.sum("recovery_score").over(window_7_days)) \
                           .withColumn("accumulated_fatigue", (F.col("rolling_strain") / F.when(F.col("rolling_recovery") == 0, 1).otherwise(F.col("rolling_recovery"))) * 10)

    fact_daily = fact_daily.select(
        "user_id", "date", "total_sleep_hours", "deep_sleep_hours", "rem_sleep_hours", "light_sleep_hours", "awake_hours",
        "rhr", "hrv_avg", "respiratory_rate", "recovery_score", "strain_score", "accumulated_fatigue", "error_flag"
    )

    # 4. TRANSFORM: Workouts Fact
    logger.info("Transforming fact_workouts...")
    fact_workouts = df_workouts.withColumn("workout_date", F.to_date("start_time")) \
                               .withColumn("duration_minutes", (F.col("end_time").cast("long") - F.col("start_time").cast("long")) / 60) \
                               .withColumn("time_of_day", 
                                   F.when(F.hour("start_time") < 12, "Morning")
                                    .when(F.hour("start_time") < 17, "Afternoon")
                                    .otherwise("Evening")) \
                               .withColumnRenamed("id", "workout_id")
                               
    fact_workouts = fact_workouts.select(
        "workout_id", "user_id", "workout_date", "start_time", "end_time", "activity_type",
        "duration_minutes", "max_hr", "avg_hr", "calories_burned", "time_of_day"
    )

    # 3. LOAD to BigQuery
    logger.info("Loading Dimension and Fact tables into BigQuery...")
    
    def write_to_bq(df, table_name):
        bq_target = f"{bq_dataset}.{table_name}"
        logger.info(f"Writing to {bq_target}...")
        df.write.format("bigquery") \
            .option("table", bq_target) \
            .mode("overwrite") \
            .save()

    write_to_bq(dim_users, "dim_users")
    write_to_bq(fact_daily, "fact_daily_health")
    write_to_bq(fact_workouts, "fact_workouts")

    logger.info("Pipeline Execution Completed Successfully.")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--jdbc_url", required=True)
    parser.add_argument("--jdbc_user", required=True)
    parser.add_argument("--jdbc_password", required=True)
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--temp_gcs_bucket", required=True)
    
    args = parser.parse_args()
    run_pipeline(args.jdbc_url, args.jdbc_user, args.jdbc_password, args.bq_dataset, args.temp_gcs_bucket)
