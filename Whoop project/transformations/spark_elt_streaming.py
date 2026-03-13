import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_streaming_aggregation(project_id, instance_id, table_name, bq_dataset, temp_gcs_bucket):
    """
    Reads from Cloud Bigtable using the spark-hbase connector.
    Aggregates massive volumes of second-by-second telemetry down to Hourly Vitals
    and writes to BigQuery.
    """
    logger.info("Initializing Spark Session for Bigtable to BigQuery Pipeline...")
    spark = SparkSession.builder \
        .appName("Whoop_Streaming_to_BQ") \
        .getOrCreate()
        
    spark.conf.set("temporaryGcsBucket", temp_gcs_bucket)

    # Note: Reading from BigTable in PySpark requires a catalog definition matching the HBase structure
    catalog = ''.join("""{
        "table":{"namespace":"default", "name":"%s"},
        "rowkey":"key",
        "columns":{
            "rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
            "hr":{"cf":"metrics", "col":"hr", "type":"int"},
            "temp":{"cf":"metrics", "col":"temp", "type":"float"}
        }
    }""".format(table_name).split())

    logger.info("Extracting data from Cloud Bigtable...")
    
    try:
        # In a real GCP environment this connects via HBase API
        df_sensor = spark.read \
            .options(catalog=catalog) \
            .format("org.apache.hadoop.hbase.spark") \
            .load()
            
        # RowKey format: user_id#timestamp (e.g. uuid#2023-10-01T12:35:01Z)
        logger.info("Transforming: Extracting user_id and timestamp from rowkey...")
        df_parsed = df_sensor.withColumn("user_id", F.split(F.col("rowkey"), "#").getItem(0)) \
                             .withColumn("timestamp", F.split(F.col("rowkey"), "#").getItem(1).cast("timestamp"))
                             
        # Aggregate to HOURLY metrics to reduce BigQuery costs and facilitate dashboarding
        logger.info("Aggregating: Downsampling second-level events into Hourly Windows...")
        df_hourly = df_parsed.withColumn("date_hour", F.date_trunc("hour", "timestamp")) \
            .groupBy("user_id", "date_hour") \
            .agg(
                F.avg("hr").alias("avg_heart_rate_bpm"),
                F.max("hr").alias("max_heart_rate_bpm"),
                F.avg("temp").alias("avg_skin_temp_celsius")
            )
            
        bq_target = f"{bq_dataset}.fact_hourly_vitals"
        logger.info(f"Loading: Writing hourly aggregates to BigQuery table {bq_target}")
        
        df_hourly.write.format("bigquery") \
            .option("table", bq_target) \
            .mode("append") \
            .save()
            
        logger.info("Streaming Aggregation Pipeline Complete.")
        
    except Exception as e:
        logger.error(f"Failed to read from BigTable or Write to BQ. Ensure correct JARs are passed. Error: {e}")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gcp_project", required=True)
    parser.add_argument("--bt_instance", required=True)
    parser.add_argument("--bt_table", required=True)
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--temp_gcs_bucket", required=True)
    
    args = parser.parse_args()
    run_streaming_aggregation(args.gcp_project, args.bt_instance, args.bt_table, args.bq_dataset, args.temp_gcs_bucket)
