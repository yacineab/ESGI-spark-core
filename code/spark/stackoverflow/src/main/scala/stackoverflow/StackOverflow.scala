package stackoverflow

import org.apache.spark.sql.{Dataset, SparkSession}

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Int, parentId: QID, score: Int, tags: String) extends Serializable

/** The main class */
object StackOverflow extends StackOverflowUtils {

  /** Main function */
  def main(args: Array[String]): Unit = {

    // Defining the spark Session
    val spark = SparkSession
      .builder()
      .appName("Stackover Flow TP")
      .master("local[*]")
      .getOrCreate()

    // Setting log level to ERROR to Avoid multiple INFO logs
    spark.sparkContext.setLogLevel("ERROR")

    // Reading the DataFrame from source File, the columns are not named
    val path = getClass.getResource("stackoverflow.csv").getFile
    println(s"the is $path")
    val stackDF_unamed = spark.read.option("inferSchema","true").csv(path)
    println("----- Unmamed StackoverFlow DataFrame ----- ")
    stackDF_unamed.printSchema()
    stackDF_unamed.show(false)
    // Defining a seq to name the columns of our DataFrame
    val dfColumnsName = Seq("postTypeId","id","acceptedAnswer","parentId","score","tag")

    // Naming the columns of Our DataFrame
    val stackDF = stackDF_unamed.toDF(dfColumnsName: _*)
    println("------ Named DF ---------")
    stackDF.printSchema()
    stackDF.show(false)

    // Defining the DataFrame with the Question and the highest Score of its Answers
    val questionWithMaxscore = groupedPostings(stackDF)

    // import spark.implicits._ in order to define our encoders to transform our DataFrame to DataSet

    /**
      * Defining a vector of DataSet[(Question, Int)] containing a questions and the max answer score
      */
    import spark.implicits._
    val vectorsDS: Dataset[(Question, Int)] = questionWithMaxscore.
      map(row =>
        (Posting(
          row(0).asInstanceOf[Int],
          row(1).asInstanceOf[Int],
          row(2).asInstanceOf[Int],
          row(3).asInstanceOf[Int],
          row(4).asInstanceOf[Int],
          row(5).asInstanceOf[String]
        ),
        row(6).asInstanceOf[Int]
        ))

    println("--- Printing Vectors DataSet ...")
    vectorsDS.printSchema()
    vectorsDS.show(false)
   // vectorePostings for the kmeans, pensez à persister votre DataSet car kmeans est un algorithme itératif
    val vectors = vectorPostings(vectorsDS).cache()
   val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
   val results = clusterResults(means, vectors)
   printResults(results)
  }
}

