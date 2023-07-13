package prac_dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import part2dataframes.DataFramesBasics.{cars, spark}

object basics extends App {
    //SparkSession has become an entry point to Spark to work with RDD, DataFrame, and Datasets.
  // creating a SparkSession
  val spark= SparkSession.builder()
                        .appName("DataFrames Basics")
                        .config("spark.master","local")
                        .getOrCreate()
  println(spark.version)

  //reading json file
  val firstDF= spark.read
    .format("json")
    .option("inferSchema",true) // meaning that all the columns of DF are going to be figured in the json file
    .load("src/main/resources/data/cars.json")
  // showing df
  firstDF.show()

  firstDF.printSchema()

  //get rows
  firstDF.take(10).foreach(println)

  // manual schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  //obtain a schema from existing
  val carDfSchema= firstDF.schema

  //read a df with your own schema
  val carDfWithSchema=spark.read
    .format("json")
    .schema(carDfSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA")

  // create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15, 8, 350, 165, 3693, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18, 8, 318, 150, 3436, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16, 8, 304, 150, 3433, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17, 8, 302, 140, 3449, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15, 8, 429, 198, 4341, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14, 8, 454, 220, 4354, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14, 8, 440, 215, 4312, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14, 8, 455, 225, 4425, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15, 8, 390, 190, 3850, 8.5, "1970-01-01", "USA")
  )
  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred

  //create dfs with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")
  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()
}
