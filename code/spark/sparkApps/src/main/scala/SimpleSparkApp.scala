import org.apache.spark.sql.SparkSession

object SimpleSparkApp extends App {
  val spark = SparkSession
    .builder()
    .appName("Simple Spark App")
    .master("local[*]")
    .getOrCreate()
}
