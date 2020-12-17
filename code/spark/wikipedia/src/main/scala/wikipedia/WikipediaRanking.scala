package wikipedia

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  // Define your Spark Session - le point d'entrée dans Spark
 val spark = SparkSession
   .builder()
   .appName("Ranking Wikipedia Artciles")
   .master("local[*]")
   .getOrCreate()


  // Hint: use a combination of `textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  /**
    * Reading Wikipedia articles form text file
    * as it is a textfile, the data is unstructured, we use RDD to do that, hence we have to call the sparkContext
    * Read the wikipedia File from the resource (we use, WikipediaData.filePath function)
    * La fonction "textFile" renvoi un RDD[String], une ligne du fichier sera une ligne du RDD au format String
    * using WikipediaData.parse pour parser chaque ligne (String) en Objet WikipediaArticle
    * la fonction parse est ecrite pour vous dans WikipediaData.scala
    *
    *  @return RDD[WikipediaArticle]
    */

  val wikiRdd: RDD[WikipediaArticle] = spark
    .sparkContext
    .textFile(WikipediaData.filePath).map(WikipediaData.parse)

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.filter(t => t.mentionsLanguage(lang)).count().toInt
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */

  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    langs.map(l => (l, occurrencesOfLang(l, rdd))).sortBy(- _._2)
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */

  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    val arPairs = rdd.flatMap(article => {
      val lgMentionned = langs.filter(lang => article.text.split(" ").contains(lang))
      lgMentionned.map(t => (t, article))
    })
    arPairs.groupByKey()
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index.mapValues(a => a.size).sortBy(_._2, false).collect().toList
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    rdd.flatMap( article => {
      langs.filter( lang => article.mentionsLanguage(lang)).map((_, 1))
    }).reduceByKey(_+_).sortBy(_._2, false).collect().toList
  }

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
