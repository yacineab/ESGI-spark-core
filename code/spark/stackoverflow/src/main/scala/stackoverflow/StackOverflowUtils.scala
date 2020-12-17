package stackoverflow

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.max

import scala.annotation.tailrec

class StackOverflowUtils extends Serializable {
  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //


  /** Group the questions and answers together */
    //: RDD[(QID, Iterable[(Question, Answer)])]
  def groupedPostings(postings: DataFrame): DataFrame = {
      // Question DF
      val qDF = postings.filter("postTypeId=1")
      println("------- GroupedPosting Function ---------")
      println("------- Question DF ---------")
      qDF.show(false)

      // asnwers DF
      val aDFColumns = Seq("a_postTypeId","a_id","a_acceptedAnswer","a_parentId","a_score","a_tag")

      val aDF = postings.filter("postTypeId=2 AND parentId is NOT NULL ").toDF(aDFColumns:_*)
      println("------- Answer DF ---------")
      aDF.show(false)

      // Jointure entre aDF et qDF
      val selected = qDF.columns
      // resultat  : (questsionsDF.columns, sonMAxScore)
      val qdf_answerMAXScore = qDF
        .join(aDF,
          qDF.col("id").equalTo(aDF.col("a_parentId")),
        "inner").
        groupBy(selected.head, selected.tail:_*)
        .agg(max("a_score") as "answer_maxscore")

      println("---- qdf_answerMAXScore DF -----")
      qdf_answerMAXScore.show()

      // checking the abnswers for a given ParentID
      println("---- checking the answers for a given parentId 32414, and check the max score among them -----")

      aDF.where("a_parentId=32414").show()
      // return dF questsionsDF.columns, sonMAxScore
     qdf_answerMAXScore

  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: Dataset[(Posting, HighScore)]): Dataset[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    //  import scored.sparkSession.implicits._  => décommenter cette ligne
    import scored.sparkSession.implicits._
    def firstLangInTag(tag: String, ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    // pour chaque vecteur scoring, on multiplie l'index (dans la liste langs) du langage du programation * langSpread (50000)
    // langSpread est un paramètre de notre algorithme Kmeans pour définir la distance euclidienne entre chaque mean
    scored.map( scors => (firstLangInTag(scors._1.tags, langs).get * langSpread, scors._2))
  }


  /** Sample the vectors */
  def sampleVectors(vectorsDS: Dataset[(LangIndex, HighScore)]): Array[(Int, Int)] = {
    val vectors = vectorsDS.rdd
    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
      // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
      // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: Dataset[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    val newMeans = means.clone() // you need to compute newMeans

    // TODO: Fill in the newMeans array
    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
        println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
          f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: Dataset[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    import vectors.sparkSession.implicits._
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.rdd.groupByKey()

    val median = closestGrouped.mapValues { vs =>
      val clustred = vs.map(_._1 / langSpread)
        .groupBy(identity)
        .mapValues(_.size)
      val maxindex = clustred.maxBy(_._2)._1
      val langLabel: String   = langs(maxindex)  // most common language in the cluster
      val langPercent: Double = clustred(maxindex) * 100 / vs.size // percent of the questions in the most common language
      val clusterSize: Int    = vs.size
      val sortedScores = vs.map(_._2).toList.sorted
      val medianScore: Int    = {
        if( clusterSize % 2 == 0  ) {
          (sortedScores(clusterSize/2 -1) + sortedScores(clusterSize/2)) / 2
        }
        else
          sortedScores(clusterSize/2)
      }

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }

}
