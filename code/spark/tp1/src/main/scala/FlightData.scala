import org.apache.spark.sql.functions.{desc, _}
import org.apache.spark.sql.{Row, SparkSession}

object FlightData extends App {

  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Int)

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


  val dfSchema = flight2010Json.schema
  //dfSchema.foreach(println(_))

  val myRows = Seq(Row("Hello", null, 1L))
  val myRDD = spark.sparkContext.parallelize(myRows)
  val myDF = spark.createDataFrame(myRDD, dfSchema)
  // myDF.show()

  val selectDF = flightData2015DF.select(
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"),
    expr("DEST_COUNTRY_NAME")
  )
  //selectDF.show()


  //SelectExrp
  val withinCountry = flightData2015DF
    .selectExpr("*", // take all column
      "DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME as local" // add news column to DF where DEST=ORIGIN
    )
  // withinCountry.show()

  val localFlightsDF = withinCountry.where("local")
  //localFlightsDF.show()


  val average = flightData2015DF
    .selectExpr("avg(count)", // moyenne des vols
      "count(distinct(DEST_COUNTRY_NAME))") // compter le nombre de pays dest distinct

  //average.show()

  val local2 = flightData2015DF
    .withColumn("local",
      expr("DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME"))

  // local2.show()
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

  val numberElem = flightData2015DF.count()
  println("number elem DF = " + numberElem)

  //val somme = flightData2015DF.agg(sum("count"))
  //somme.show()

  import spark.implicits._

  val flightDS = flightData2015DF.as[Flight]
  // flightDS.show()

  def localFlight(vol: Flight): Boolean = {
    vol.DEST_COUNTRY_NAME == vol.ORIGIN_COUNTRY_NAME
  }


  val localDataSet = flightDS.filter(vol => localFlight(vol))

  localDataSet.show()

  val upperDS = flightDS
   .map(vol => Flight(vol.DEST_COUNTRY_NAME.toUpperCase,
                      vol.ORIGIN_COUNTRY_NAME.toUpperCase(),
                      vol.count))
  upperDS.printSchema()
  upperDS.show()
println("------------ select on DataSet ---------------")
  upperDS.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME","count").printSchema()
  upperDS.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME","count").show()



}