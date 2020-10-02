import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object FlightData extends App {

  // Instantation du SparkSession
  val spark = SparkSession
    .builder()
    .appName("Flight Data TP")
    .master("local[*]")
    .getOrCreate()

  // Craetion du DataFrame flighData à partir des données CSV

  val flightData2015DF = spark.read.option("inferSchema","true").option("header","true").csv("../../../data/flight-data/2015-summary.csv")

  //print le schema de données
  flightData2015DF.printSchema()
  //affichage des 20 premiere lignes
  // flightData2015DF.show()

  // la methode explain renvoie le plan d'execution du DataFrame
  flightData2015DF.explain()

  // création dataFrame de vols locaux

  val localDF = flightData2015DF.where("DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME")

  // Sort du DataFrame
  val sortedDF = flightData2015DF.sort("count")

  //si on souhaite trier les données par ordre descedant on appelle la fonction desc sur la colonne count
  // Pour ce faire il faut importer le pacakge import org.apache.spark.sql.functions.desc
  val sortedDescDF = flightData2015DF.sort(desc("count"))

  // Lecture du DataFrame à partir d'un fichier JSON

  val flight2010Json = spark.read.json("../../../data/flight-data/2010-summary.json")
  flight2010Json.show()

  // Reading Parquet file
  val flightParquet = spark.read.parquet("../../../data/flight-data/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")
  flightParquet.show()

}
