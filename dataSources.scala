package prac_dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import part2dataframes.DataSources.{carsSchema, spark}

object dataSources extends App {

  val spark= SparkSession.builder()
    .appName("Data sources and formats")
    .config("spark.master","local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF:
    - format
    - schema or inferSchema = true
    - path
    - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
   Writing DFs
   - format
   - save mode = overwrite, append, ignore, errorIfExists
   - path
   - zero or more options
  */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  // JSON flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV
  val stockschema= StructType(Array(
    StructField("Symbol",StringType),
    StructField("Date",DateType),
    StructField("Price",DoubleType)
  ))

  spark.read
    .schema(stockschema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header","true")
    .option("sep",",")
    .option("nullvalue","")
    .csv("src/main/resources/data/stocks.csv")

  //PARQUET
  // - binary data storage format, optimized for fast reading of columns
  // default storage format for dataframes...
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")  //give .parquet or .save bcoz default storage format is parquet

  //TEXT FILE
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  //reading from a remote DB
  val employeesDF= spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.employees")
    .load()

  employeesDF.show()

}
