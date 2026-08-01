import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TowerPulse {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("TowerPulse - Real-Time Telecom Congestion Analytics")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val cdrSchema = StructType(Seq(
      StructField("call_id",   StringType,    true),
      StructField("caller",    LongType,      true),
      StructField("receiver",  LongType,      true),
      StructField("call_type", StringType,    true),
      StructField("duration",  IntegerType,   true),
      StructField("call_date", TimestampType, true),
      StructField("tower_id",  StringType,    true)
    ))

    val cdrStreamDF = {
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "telecom_cdr")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), cdrSchema).alias("data"))
        .select("data.*")
    }

    val towerCongestionStream = cdrStreamDF
      .withWatermark("call_date", "10 minutes")
      .groupBy(
        window(col("call_date"), "5 minutes"),
        col("tower_id")
      )
      .agg(count(lit(1)).alias("total_calls"))
      .withColumn(
        "status",
        when(col("total_calls") >= 5, lit("CRITICAL"))
          .when(col("total_calls") >= 3, lit("HIGH"))
          .otherwise(lit("NORMAL"))
      )
      .select(
        col("tower_id"),
        col("total_calls"),
        col("status"),
        col("window").getField("start").alias("window_start"),
        col("window").getField("end").alias("window_end")
      )

    val hiveQuery = towerCongestionStream.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", "/user/telecom/hive/tower_congestion_rt")
      .option("checkpointLocation", "/user/telecom/stream/checkpoints/tower_congestion_rt")
      .start()

    val alertDF = towerCongestionStream
      .filter(col("status") === "CRITICAL")
      .select(
        to_json(struct(
          col("tower_id"),
          col("total_calls"),
          col("status"),
          col("window_start"),
          col("window_end")
        )).alias("value")
      )

    val alertQuery = alertDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "telecom_alerts")
      .option("checkpointLocation", "/tmp/telecom_ckpt_clean")
      .outputMode("append")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
