import org.apache.spark.sql.SparkSession

object SimpleSparkApp extends App {
  val spark = SparkSession
    .builder()
    .appName("Simple Spark App")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Create First DataFrame
  val rangeDF = spark.range(1000).toDF("number")
  rangeDF.show(15)

<<<<<<< HEAD
  val fichierDF = spark.read.csv("/user/myname/file.csv")
  // trasformation sur Dataframe
  fichierDF.write.parquet("/path/tohdfs")

=======
>>>>>>> 6c3c767df6ffd8a1a670aafe26c5a5748a98f056
}
