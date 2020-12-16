package stackoverflow

import org.apache.spark.sql.{Dataset, SparkSession}

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Int, parentId: QID, score: Int, tags: String) extends Serializable

/** The main class */
object StackOverflow extends StackOverflowUtils {

  /** Main function */
  def main(args: Array[String]): Unit = {

    // Defining the spark Session
    val spark = ???

    // Setting log level to ERROR to Avoid multiple INFO logs
    spark.sparkContext.setLogLevel("ERROR")

    // Reading the DataFrame from source File, the columns are not named
    val stackDF_unamed = ???

    // Defining a seq to name the columns of our DataFrame
    val dfColumnsName = Seq(???)

    // Naming the columns of Our DataFrame
    val stackDF = ???

    // Defining the DataFrame with the Question and the highest Score of its Answers
    val questionWithMaxscore = groupedPostings(stackDF)

    // import spark.implicits._ in order to define our encoders to transform our DataFrame to DataSet
    import spark.implicits._

    /**
      * Defining a vector of DataSet[(Question, Int)] containing a questions and the max answer score
      */
    val vectorsDS: Dataset[(Question, Int)] = ???

   // vectorePostings for the kmeans, pensez à persister votre DataSet car kmeans est un algorithme itératif
    val vectors = vectorPostings(vectorsDS).???
    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
   printResults(results)
  }
}

