import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.execution.streaming.state.StateStore.stop
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, date_format, dayofweek, explode, max, min, sum, to_date, when, split}


object Assignment_Set4 {
  def main(args: Array[String]): Unit = {

//    @todo creating sparkContext
//    val sc = new SparkContext("local[*]","rdd-Program")
//
//    @todo first way to create spark session
//    val spark = new SparkSession.Builder()
//      .appName("Assignment-Set2")
//      .master("local[*]")
//      .getOrCreate()
//
//    @todo second way to create spark session
    val conf = new SparkConf()
    conf.set("spark.app.name","Assignment-Set2")
    conf.set("spark.master","local[*]")

    val spark = new SparkSession.Builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

//    @todo Finding the count of orders placed by each customer and the total order amount for each customer
//
//    val orderData = List(
//      ("Order1", "John", 100),
//      ("Order2", "Alice", 200),
//      ("Order3", "Bob", 150),
//      ("Order4", "Alice", 300),
//      ("Order5", "Bob", 250),
//      ("Order6", "John", 400)
//    ).toDF("OrderID", "Customer", "Amount")
//    val df = orderData.groupBy(col("Customer")).agg(
//      count("orderID").alias("count_order"),
//      sum(col("Amount")).alias("order_amount"))
//    df.show()
//
//    val orderData_tbl = orderData.createOrReplaceTempView("order")
//    val orderData_sql = spark.sql(
//      """
//       SELECT Customer,
//       COUNT(orderID) AS count_order,
//       SUM(Amount) AS order_amount
//       FROM order
//       GROUP BY Customer
//       """)
//    orderData_sql.show()
//
//    @todo Finding the average score for each subject and the maximum score for each student.
//
//    val scoreData = List(
//      ("Alice", "Math", 80),
//      ("Bob", "Math", 90),
//      ("Alice", "Science", 70),
//      ("Bob", "Science", 85),
//      ("Alice", "English", 75),
//      ("Bob", "English", 95)
//    ).toDF("Student", "Subject", "Score")
//    val df = scoreData.groupBy(col("Subject")).agg(avg(col("Score")).alias("avg_score"))
//    df.show()
//    val df1 = scoreData.groupBy(col("Student")).agg(max(col("Score")).alias("max_score"))
//    df1.show()
//
//    val scoreData_tbl = scoreData.createOrReplaceTempView("score")
//    val scoreData_sql = spark.sql(
//      """
//        SELECT Student,
//        AVG(Score) AS avg_score,
//        MAX(Score) as max_score1
//        FROM score
//        GROUP BY Student
//        """)
//    scoreData_sql.show()

//    @todo Finding the average rating for each movie and the total number of ratings for each movie.
//
//    val ratingsData = List(
//      ("User1", "Movie1", 4.5),
//      ("User2", "Movie1", 3.5),
//      ("User3", "Movie2", 2.5),
//      ("User4", "Movie2", 3.0),
//      ("User1", "Movie3", 5.0),
//      ("User2", "Movie3", 4.0)
//    ).toDF("User", "Movie", "Rating")
//    val df = ratingsData.groupBy(col("Movie")).agg(
//      avg(col("Rating")).alias("avg_rating"),
//      count(col("Rating")).alias("count_rating")
//    )
//    df.show()
//
//    val ratingsData_tbl = ratingsData.createOrReplaceTempView("rating")
//    val ratingsData_sql = spark.sql(
//      """
//       SELECT Movie,
//       AVG(Rating) AS avg_rating,
//       COUNT(Rating) AS count_rating
//       FROM rating
//       GROUP BY Movie
//       """)
//    ratingsData_sql.show()
//
//    todo Finding the count of occurrences for each word in a text document.
//
    val textData = List(
      "Hello, how are you?",
      "I am fine, thank you!",
      "How about you?"
    ).toDF("Text")
    val df =  textData.select(explode(split(col("Text"), "\\s+")).alias("Word"))
    val df1 = df.groupBy("Word").agg(count("Word").alias("Count"))
    df1.show()

//    @todo Finding the minimum, maximum, and average temperature for each city in a weather dataset.
//
//    val weatherData = List(
//      ("City1", "2022-01-01", 10.0),
//      ("City1", "2022-01-02", 8.5),
//      ("City1", "2022-01-03", 12.3),
//      ("City2", "2022-01-01", 15.2),
//      ("City2", "2022-01-02", 14.1),
//      ("City2", "2022-01-03", 16.8)
//    ).toDF("City", "Date", "Temperature")
//    val df = weatherData.groupBy(col("City")).agg(
//      min(col("Temperature")).alias("min_temp"),
//      max(col("Temperature")).alias("min_temp"),
//      avg(col("Temperature")).alias("avg_temp")
//    )
//    df.show()
//
//    val weatherData_tbl = weatherData.createOrReplaceTempView("weather")
//    val weatherData_sql = spark.sql(
//      """
//       SELECT City,
//       MIN(Temperature) AS min_temp,
//       MAX(Temperature) AS max_temp,
//       AVG(Temperature) AS avg_temp
//       FROM weather
//       GROUP BY City
//       """)
//    weatherData_sql.show()
//
//    @todo Finding the count of distinct products purchased by each customer and the total purchase amount for each customer.
//
//    val purchaseData = List(
//      ("Customer1", "Product1", 100),
//      ("Customer1", "Product2", 150),
//      ("Customer1", "Product3", 200),
//      ("Customer2", "Product2", 120),
//      ("Customer2", "Product3", 180),
//      ("Customer3", "Product1", 80),
//      ("Customer3", "Product3", 250)
//    ).toDF("Customer", "Product", "Amount")
//    val df = purchaseData.groupBy(col("Customer")).agg(
//      countDistinct(col("Product")).alias("count_product"),
//      sum(col("Amount")).alias("total_amount")
//    )
//    df.show()
//
//    val purchaseData_tbl = purchaseData.createOrReplaceTempView("purchase")
//    val purchaseData_sql = spark.sql(
//      """
//       SELECT Customer,
//       COUNT(DISTINCT Product) AS count_product,
//       SUM(Amount) AS total_amount
//       FROM purchase
//       GROUP BY Customer
//       """)
//    purchaseData_sql.show
//
//    @todo Finding the top N products with the highest sales amount
//
//    val salesData = List(
//      ("Product1", 100),
//      ("Product2", 200),
//      ("Product3", 150),
//      ("Product4", 300),
//      ("Product5", 250),
//      ("Product6", 180)
//    ).toDF("Product", "SalesAmount")
//    val df = salesData.orderBy(col("SalesAmount").desc).limit(5)
//    df.show()
//
//    val salesData_tbl = salesData.createOrReplaceTempView("sales")
//    val salesData_sql = spark.sql(
//      """
//       SELECT *
//       FROM sales
//       ORDER BY SalesAmount DESC
//       LIMIT 5
//       """)
//    salesData_sql.show()
//
//    @todo Finding the cumulative sum of sales amount for each product.
//
//    val salesData = List(
//      ("Product1", 100),
//      ("Product2", 200),
//      ("Product3", 150),
//      ("Product4", 300),
//      ("Product5", 250),
//      ("Product6", 180)
//    ).toDF("Product", "SalesAmount")
//    val window = Window.orderBy(col("Product"))
//    val df = salesData.withColumn("sales_amount",sum(col("SalesAmount")).over(window))
//    df.show()
//
//    val salesData_tbl = salesData.createOrReplaceTempView("sales")
//    val salesData_sql = spark.sql(
//      """
//       SELECT Product,
//       SUM(SalesAmount) OVER(ORDER BY Product) AS sales_amount
//       FROM sales
//       """)
//    salesData_sql.show()
//
//    @todo Finding the average rating given by each user for each genre in a movie rating dataset.
//
//    val ratingData = List(
//      ("User1", "Movie1", "Action", 4.5),
//      ("User1", "Movie2", "Drama", 3.5),
//      ("User1", "Movie3", "Comedy", 2.5),
//      ("User2", "Movie1", "Action", 3.0),
//      ("User2", "Movie2", "Drama", 4.0),
//      ("User2", "Movie3", "Comedy", 5.0),
//      ("User3", "Movie1", "Action", 5.0),
//      ("User3", "Movie2", "Drama", 4.5),
//      ("User3", "Movie3", "Comedy", 3.0)
//    ).toDF("User", "Movie", "Genre", "Rating")
//    val df = ratingData.groupBy(col("User"),col("Genre")).agg(avg(col("Rating")).alias("avg_rating")).orderBy(col("User"))
//    df.show()
//
//    val ratingData_tbl = ratingData.createOrReplaceTempView("rating")
//    val ratingData_sql = spark.sql(
//      """
//       SELECT User,Genre,
//       AVG(Rating) as avg_rating
//       FROM rating
//       GROUP BY User,Genre
//       ORDER BY User
//       """)
//    ratingData_sql.show()








    spark.stop()
  }

}
