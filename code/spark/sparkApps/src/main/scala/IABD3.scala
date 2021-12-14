import org.apache.spark.sql.SparkSession

object IABD3 extends App {
  val ss = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  ss.sparkContext.setLogLevel("ERROR")
  val txtrdd = ss.sparkContext.textFile("src/main/resources")

  //txtrdd.take(5).foreach(println(_))

  val wordsrdd = txtrdd.flatMap(_.split(" "))
  //wordsrdd.take(5).foreach(println(_))
  val cnt1 = wordsrdd.count()
  println("number of elements : " + cnt1)

}
