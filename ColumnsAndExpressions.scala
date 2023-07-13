import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)

  // various select methods
  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  //DF processing
  //adding a column
  val carsWithKgs3DF=carsDF.withColumn("weight_in_kg_3",col("weight_in_lbs") / 2.2).show
  //renaming a column
  val carsWithColumnRenamed=carsDF.withColumnRenamed("weight_in_lbs","weight in pounds")
  //careful with column names
  carsWithColumnRenamed.selectExpr("'weight in pounds'")
  //remove a column
  carsWithColumnRenamed.drop("Cyclinders","Displacement")

  //filtering
  val europeanCarsDf=carsDF.filter(col("origin")=!= "USA")
  val europeanCarsDf2=carsDF.where(col("origin")==="USA")
  //filtering with expressions
  val americanCarsDf=carsDF.where("origin = 'USA'")
  //chain filters
  val americanPowerfulCarsDf=carsDF.filter(col("origin")==="USA").filter(col("Horsepower")>150)
  val americanPowerfulCarsDf2=carsDF.filter(col("origin")==="USA" and col("Horsepower")>150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  //unioning - adding more rows
  val moreCarsDf= spark.read
    .option("inferschema","true")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDf= carsDF.union(moreCarsDf) //only works if both dfs have same schema
  //distinct values
  val allCountriesDf=carsDF.select("Origin").distinct()
  allCountriesDf.show()
  val allCountry= carsDF
}