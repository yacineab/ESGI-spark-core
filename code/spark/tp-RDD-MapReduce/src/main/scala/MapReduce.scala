import org.apache.spark.sql.SparkSession

object MapReduce extends App {

  /**
   * Create Spark Session
   */
  val spark = SparkSession
    .builder()
    .appName("MapReduce with Spark RDD")
    .master("local[*]").getOrCreate()

  // Ceating rdd from a collection
  val myCollection = "Spark engin by IBAD2 class from ESGI".split(" ")
  val wordsCollectionRDD = spark.sparkContext.parallelize(myCollection)

  /**
   * RDD From source file
   */
  val humanRights = spark.sparkContext.textFile("src/main/resources/humanRights.txt")

  /**
   * RDD OF WORDS: flatMap and split by space , we use flatmap instead of Map beacause "split" retrun an Array
   */

  val wordsRDD = humanRights.flatMap(_.split(" "))
  //counting element of rdd
  val wordsElements = wordsRDD.count()

  println("counting 1 : " + wordsElements)
  // RDD transforlation remove ponctuation from words
  val rights = wordsRDD
    .map(_
      .replace(",","")
      .replace(".","")
      .replace("!","")
      .replace("?","")
      .replace(";",""))
    .filter(_.length > 0)

  val coutingRights = rights.count()

  println("counting 2 : " + coutingRights)

  // filtring rdd starting with a letter, example A
  val startingwithA = rights.filter(_.startsWith("A"))

  // Sort by word's length, seconde parametere = false to sort by desc
  val sortedRightRDD = rights.sortBy(_.length, false)

  /**
   * REDUCE
   */

  // range RDD
  val rangeRDD = spark.range(1000).rdd
  val sumRDD = rangeRDD.reduce(_+_)

  /**
   * REDUCE 2
   */

  /**
   *
   * @param r : right word
   * @param l: left word
   * @return the biggest number in size between r et l and return l if equal size
   */
  def biggerWord(r: String, l: String): String = {
    if (r.length > l.length)
      r
    else
      l
  }

  // Using the previous function with Reduce on RDD to calculate the biggest word on our RDD

  val biggestWordonRDD  = rights.reduce(biggerWord)
  println(s"bigest word in RDD is $biggestWordonRDD")

  /**
   * Map Reduce: Using the MapReduce Algorithme calculating the number of occurence of each word on our RDD, then sorting by the largest appearance
   */

  val mapreduceRDD = rights.map((_,1)).reduceByKey(_+_).sortBy(_._2, false)

  // Saving our result as textFile
  mapreduceRDD.saveAsTextFile("src/main/resources/mapreduce")
}
