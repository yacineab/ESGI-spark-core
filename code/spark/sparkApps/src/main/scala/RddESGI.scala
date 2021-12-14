import org.apache.spark.sql.SparkSession

object RddESGI extends App {

  val ss = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  ss.sparkContext.setLogLevel("ERROR")
  val rdd1 = ss
    .sparkContext
    .textFile("src/main/resources")

  //rdd1.take(20).foreach(println(_))

  val words = rdd1.flatMap(_.split(" "))
  //words.take(2000).foreach(println(_))
  println("words number of element: " + words.count())

  val wordsRDD = words
    .map(_.replace(",","")
      .replace("?","")
      .replace(".","")
      .replace(";","")
      .replace("!","")
      .replace(":","")
    ).filter(_.length > 0)

  println("wordsrdd number of element: " + wordsRDD.count())


  val rddStartswothS = wordsRDD.filter(_.startsWith("s"))
  //rddStartswothS.take(30).foreach(println(_))

  val sorted = wordsRDD.sortBy(_.length, false)
  sorted.take(5).foreach(println(_))

  def bigger (s1: String, s2: String) = {
    if (s1.length > s2.length) s1
    else s2
  }

  val biggestRDD = wordsRDD.reduce(bigger)
  println("reduce: " + biggestRDD)

  // wordcount
  val wordslower = wordsRDD.map(_.toLowerCase())

  // algo MapReduce
  val wordcunt = wordslower.map((_,1)).reduceByKey(_+_)

  // vérifier le nombre de partitions dans le RDD
  val nbpartitions=  wordcunt.getNumPartitions
  println(s"Mapredecue RDD has: $nbpartitions partitions")

  // On peut changer le nombre de partitions avec .repartition
  val repartitionRDD = wordcunt.repartition(5)

  // Pour diminuer le nombre de partitions avec colaesce
  // avec colesce le nombre de partition doit être inférieur à nbpartitions, sinon il sera ignoré
  val coalesceRDD = wordcunt.coalesce(1)

  // Saving our result as textFile
  wordcunt.saveAsTextFile("src/main/resources/mapreduce")
}
