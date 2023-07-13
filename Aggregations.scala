package prac_dataframes

import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, min, stddev, sum}
import org.apache.spark.sql.SparkSession
import part2dataframes.Aggregations.moviesDF


object Aggregations extends App {
   val spark= SparkSession.builder()
      .appName("Aggregations and Grouping")
      .config("spark.master","local")
      .getOrCreate()
   val moviesDF=spark.read
     .option("inferSchema","true")
     .json("src/main/resources/data/movies.json")

  //counting
  val genreCountDf=moviesDF.select(count(col("Major_Genre")))  //all the values except null
  moviesDF.selectExpr("count(Major_Genre)")
  //counting all
  moviesDF.select(count("*")) //count all the rows nd will include nulls
  //counting distinct
  moviesDF.select(countDistinct(col("Major_Genre")))
  //approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))) //it will not scan full rows, scans only approximately

  //min and max
  val minRatingDf=moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )
  //grouping
  // includes null and is equal to select count(*) from moviesDF group by Major_Genre
  val countDf= moviesDF.groupBy(col("Major_Genre")).count()
  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg()

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating")).show()
}
