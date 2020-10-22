import org.apache.spark.sql.functions.{desc, _}
import org.apache.spark.sql.{Row, SparkSession}

object FlightData extends App {


  // Instanciation du SparkSession
  val spark = SparkSession
    .builder()
    .appName("Flight Data TP")
    .master("local[*]")
    .getOrCreate()


  // setting log level to Warn
  spark.sparkContext.setLogLevel("WARN")
  // Création du DataFrame flighData à partir des données CSV

  val flightData2015DF = spark.read.option("inferSchema", "true").option("header", "true").csv("../../../data/flight-data/2015-summary.csv")

  //print le schema de données
  //flightData2015DF.printSchema()
  //affichage des 20 premiere lignes
  // flightData2015DF.show()

  // la methode explain renvoie le plan d'execution des transformations DataFrame
  flightData2015DF.explain()

  // création dataFrame de vols locaux

  val localDF = flightData2015DF.where("DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME")

  // Tri du DF selon le nombre de vols
  val sortedDF = flightData2015DF.sort("count")

  //si on souhaite trier les données par ordre descedant on appelle la fonction desc sur la colonne count
  // Pour ce faire il faut importer le pacakge import org.apache.spark.sql.functions.desc
  val sortedDescDF = flightData2015DF.sort(desc("count"))

  // Lecture du DataFrame à partir d'un fichier JSON

  val flight2010Json = spark.read.json("../../../data/flight-data/2010-summary.json")
  //flight2010Json.show()

  // Reading Parquet file
  val flightParquet = spark.read.parquet("../../../data/flight-data/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")
  //flightParquet.show()

  // Part 2


  // on recupérer le schéma du DataFrame dans une variable
  val dfSchema = flight2010Json.schema
  println("---- schema ---- ")
  dfSchema.foreach(println(_))

  // Creation d'un DataFrame à partir en parallélisant une collection de données
  // On crée dans un premier temps le RDD ensuite on créé le DataFrame en inférant le schema manuellement
  val myRows = Seq(Row("Hello", null, 1L))
  val myRDD = spark.sparkContext.parallelize(myRows)
  val myDF = spark.createDataFrame(myRDD, dfSchema)
  // myDF.show()

  // Faire une selection de différente manière
  val selectDF = flightData2015DF.select(
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"),
    expr("DEST_COUNTRY_NAME")
  )
  //selectDF.show()


  //SelectExrp
  // On ajoute une colone "local" au DF, la valeur de "local" est à true si le vol est local et false sinon
  val withinCountry = flightData2015DF
    .selectExpr("*", // take all column
      "DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME as local" // add news column to DF where DEST=ORIGIN
    )

   withinCountry.show()

  // On filtre uniquement sur les vols locaux
  val localFlightsDF = withinCountry.where("local")
  localFlightsDF.show()

  /**
   *  On selectionne deux colones :
   *  1. on calcule la moyenne du nombre de vol
   *  2. On compte le nombre de destination distinct
   */
  val average = flightData2015DF
    .selectExpr("avg(count)", // moyenne des vols
      "count(distinct(DEST_COUNTRY_NAME))") // compter le nombre de pays dest distinct

  average.show()

  // On ajoute une colonne "local" de vol locaux, en appelant la fonction "withColumn"
  val local2 = flightData2015DF
    .withColumn("local",
      expr("DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME"))
  //local2.show()

  // Ajouter une nouvelle colonne où la DEST est en upper case
  val upperDF = flightData2015DF
    .withColumn("upper", upper(col("DEST_COUNTRY_NAME")))

  // Rename column
  val dfRenamed = flightData2015DF
    .withColumnRenamed("ORIGIN_COUNTRY_NAME", "ORIGINE")
    .withColumnRenamed("DEST_COUNTRY_NAME", "DEST")

  // dfRenamed.show()

  // REMOVING COLUMN

  val dfdroped = flightData2015DF.drop("count")
  //dfdroped.printSchema()

  // Filtring
  val count15 = flightData2015DF.filter("count < 15")
  val count50 = flightData2015DF.where("count > 15")

  // Sorting

  val dfOrder = flightData2015DF.orderBy(desc("count"))
  //dfOrder.show()

  // compter le nombre d'élement du DataFrame
  val numberElem = flightData2015DF.count()
  println("number elem DF = " + numberElem)

   val somme = flightData2015DF.agg(sum("count"))
  //somme.show()

  /**
   * LES DATASETS
   */

  // on import spark.implicits pour convertir le DataFrame en Dataset
  import spark.implicits._

  /**
   * Créer la case Class Flight qui contient le schema du DataSet
   */

  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Int)

  // Transformer le DataFrame en DataSet[Flight]
  val flightDS = flightData2015DF.as[Flight]
  // flightDS.show()

  /**
   *  Défénir une fonction qui prend un vol en entrée en renvoi true si le vol est local, false sinon
   * @param vol : correspond à chaque ligne du DataSet
   * @return : true si vol local, false sinon
   */
  def localFlight(vol: Flight): Boolean = {
    vol.DEST_COUNTRY_NAME == vol.ORIGIN_COUNTRY_NAME
  }


  /**
   * On fait une transformation pour créer un nouveau DataSet de vol locaux
   */
  val localDataSet = flightDS.filter(vol => localFlight(vol))

  localDataSet.show()

  // On fait une autre transformation pour mettre en Upper Case les deux première colonnes.
  val upperDS = flightDS
   .map(vol => Flight(vol.DEST_COUNTRY_NAME.toUpperCase,
                      vol.ORIGIN_COUNTRY_NAME.toUpperCase(),
                      vol.count))
  upperDS.printSchema()
  upperDS.show()

  /**
   * Depuis Spark 2, les APIs DataSet et DataFrame étant unifiées, on peut utiliser des select sur les DataSet de la même maniere que sur les DataFrame
   */
  upperDS.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME","count").printSchema()
  upperDS.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME","count").show()


  // On peut créer une table temporaire et la requeter en SQL avec SPARK
  flightData2015DF.createOrReplaceTempView("flight_data_2015_table")
  flightDS.createOrReplaceTempView("ds_table")

  val dfreq = spark.sql("select * from flight_data_2015_table limit 10")
  val dsreq = spark.sql("select * from ds_table limit 10")
  println("--------- DF SQL ------------")
  dfreq.show()
  println("--------- DS SQL ------------")
  dsreq.show()


  val sqlWay = spark.sql(" SELECT " +
    " DEST_COUNTRY_NAME, " +
    " count(1) " +
    " FROM flight_data_2015_table " +
    "GROUP BY DEST_COUNTRY_NAME ")

  println("---- Show SQL WAY -----")
  sqlWay.show()

  val dataFrameWay = flightData2015DF
    .groupBy('DEST_COUNTRY_NAME)
    .count()

  println("---- Show DF Way -----")
  dataFrameWay.show()

  // On vérifie le plan d'exeuction pour comparer l'execution DataFrame et SQL
  /**
   * On remarque que les deux s'excute exactement de la même maniere
   */
  println("---- explain physical plan SQLWAY -----")
  sqlWay.explain

  println("---- explain physical plan DF Way -----")
  dataFrameWay.explain

}