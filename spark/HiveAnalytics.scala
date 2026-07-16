import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HiveAnalytics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("TowerPulse - Hive Analytics")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("CREATE DATABASE IF NOT EXISTS telecom")

    spark.sql("""
      CREATE TABLE IF NOT EXISTS telecom.congested_towers (
        tower_id      STRING,
        total_calls   BIGINT,
        status        STRING,
        window_start  TIMESTAMP,
        window_end    TIMESTAMP
      )
      USING PARQUET
      LOCATION '/user/telecom/curated/congested_towers'
    """)

    spark.sql("""
      CREATE OR REPLACE VIEW telecom.daily_congestion_summary AS
      SELECT
        date(window_start) AS date,
        tower_id,
        COUNT(*) AS congestion_windows
      FROM telecom.congested_towers
      WHERE status = 'CONGESTED'
      GROUP BY date(window_start), tower_id
    """)

    spark.sql("""
      CREATE OR REPLACE VIEW telecom.sla_breaches AS
      SELECT tower_id, window_start, window_end, total_calls, status
      FROM telecom.congested_towers
      WHERE status IN ('HIGH', 'CRITICAL')
    """)

    println("=== Congested Towers ===")
    spark.sql("SELECT * FROM telecom.congested_towers ORDER BY window_start DESC").show(false)

    println("=== Daily Congestion Summary ===")
    spark.sql("SELECT * FROM telecom.daily_congestion_summary").show(false)

    println("=== SLA Breaches ===")
    spark.sql("SELECT * FROM telecom.sla_breaches").show(false)

    spark.read
      .parquet("/user/telecom/hive/tower_congestion_rt")
      .filter("status IN ('HIGH','CRITICAL')")
      .write
      .mode("overwrite")
      .parquet("/user/telecom/curated/congested_towers")

    spark.stop()
  }
}
