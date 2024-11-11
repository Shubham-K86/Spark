import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.execution.streaming.state.StateStore.stop
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, date_format, dayofweek, max, min, sum, to_date, when}

object Assignment_Set3 {
  def main(args: Array[String]): Unit = {

//    @todo creating sparkContext
//       val sc = new SparkContext("local[*]","rdd-Program")
//
//    @todo first way to create spark session
//       val spark = new SparkSession.Builder()
//          .appName("Assignment-Set2")
//          .master("local[*]")
//         .getOrCreate()
//
//    @todo second way to create spark session
    val conf = new SparkConf()
    conf.set("spark.app.name","Assignment-Set2")
    conf.set("spark.master","local[*]")

    val spark = new SparkSession.Builder()
      .config(conf)
      .getOrCreate()

      import spark.implicits._

//    @todo 1.Count the number of products in each category

//    val product = List(
//      ("Electronics","Laptop"),
//      ("Electronics","Phone"),
//      ("Clothing","T-Shirt"),
//      ("Clothing","Jeans"),
//      ("Furniture","Chair")
//    ).toDF("category","product")
//
//    val count_product = product.select(
//        col("category"),
//      col("product"))
//      .groupBy(col("category")).agg(count(col("product")).alias("product_count"))
//    count_product.show()

//    @todo 2. Find Minimum, Maximum, and Average Price per Product

//    val product = List(
//      ("Laptop",1000),
//      ("Phone",500),
//      ("T-Shirt",20),
//      ("Jeans",50),
//      ("Chair",150)
//    ).toDF("product","price")
//    val df = product.select(
//      col("product"),
//      col("price")).groupBy(col("product"))
//      .agg(
//        max(col("price")).alias("max_price"),
//        min(col("price")).alias("min_price"),
//        avg(col("price")).alias("avg_price")
//    )
//    df.show()

//    @todo 3.Group sales by month and year, and calculate the total amount for each month-year combination

//    val order = List(
//      ("2023-01-01" , "New York" , 100),
//      ("2023-02-15" , "London" , 200),
//      ("2023-03-10" , "Paris ", 300) ,
//      ("2023-04-20" , "Berlin" , 400) ,
//      ("2023-05-05" , "Tokyo ", 500)
//      ).toDF("order_date","city","amount")
//
//    val df = order.withColumn("order_date",to_date(col("order_date"), "yyyy-MM-dd"))
//    val df1 = df.withColumn("month-year",date_format(col("order_date"),"yyyy-MM"))
//    val df2 = df1.groupBy(col("month-year")).agg(sum(col("amount")).alias("total_amount"))
//    df2.show()

//    @todo 4.Find the top 5 products (by total quantity sold) across all orders.

//    val product = List(
//      ("Laptop","order_1",2),
//      ("Phone","order_2",1),
//      ("T-Shirt","order_1",3),
//      ("Jeans","order_3",4),
//      ("Chair","order_2",2)
//    ).toDF("product","order_id","quantity")
//
//    val df = product.groupBy(col("product")).agg(sum(col("quantity")).alias("total_quantity"))
//    val df1 = df.orderBy(col("total_quantity").desc).limit(5)
//    df1.show()

//    @todo 5.Calculate the average rating given by each user.

//    val rating = List(
//      (1,1,4),
//      (1,2,5),
//      (2,1,3),
//      (2,3,4),
//      (3,2,5)
//    ).toDF("user_id","product_id","rating")
//    val df = rating.groupBy(col("user_id")).agg(avg(col("rating")).alias("avg_rating"))
//    df.show()

//    @todo Group customers by country and calculate the total amount spent by customers in each country.

//    val country = List(
//      (1,"USA","order_1",100),
//      (1,"USA","order_2",200),
//      (2,"UK","order_3",150),
//      (3,"France","order_4",250),
//      (3,"France","order_5",300)
//    ).toDF("customer_id","country","order_id","amount")
//    val df = country.groupBy(col("country")).agg(sum(col("amount")).alias("total_spent"))
//    df.show()

//    @todo Identify products that had no sales between 2023-02-01 and 2023-03-31.

//    val product = List(
//      ("Laptop","2023-01-01"),
//      ("Phone","2023-02-15"),
//      ("T-Shirt","2023-03-10"),
//      ("Jeans","2023-04-20")
//    ).toDF("product","order_date")
//    val df = product.filter(col("order_date").between("2023-02-01","2023-03-31")

//      @todo Calculate Order Count per Customer and City

//    val order = List(
//      (1,"New York","order_1"),
//      (1,"New York","order")
//    ).toDF("customer_id","city","order_id")
//    val df  = order.groupBy(col("customer_id"),col("city")).agg(count(col("order_id")).alias("order_count"))
//    df.show()

//    @todo Group orders by weekday (use dayofweek) and calculate the average order value for weekdays and weekends using when and otherwise

//    val order = List(
//      ("2023-04-10",1,100),
//      ("2023-04-11",2,200),
//      ("2023-04-12",3,300),
//      ("2023-04-13",1,400),
//      ("2023-04-14",2,500)
//    ).toDF("order_date","customer_id","amount")
//
//    val df = order.withColumn("order_date",to_date(col("order_date"),"yyyy-MM-dd"))
//    val df2 = df.withColumn("daysofweek",dayofweek(col("order_date")))
//    val df3 = df2.withColumn("day_type",
//      when (col("daysofweek").between(2,6), "weekdays")
//        .otherwise("weekends")
//    )
//    val df4 = df3.groupBy(col("day_type")).agg(avg(col("amount")).alias("avg_amount"))
//    df4.show()






    spark.stop()
  }

}
