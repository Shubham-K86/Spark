import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Assignments_Set1 {
  def main(args: Array[String]): Unit = {

//    @todo creating sparkContext
    val sc = new SparkContext("local[*]","rdd-Program")

//    @todo first way to create spark session
//    val spark = new SparkSession.Builder()
//      .appName("SparkSession-Program")
//      .master("local[*]")
//      .getOrCreate()
//   @todo second way to create spark session
    val conf = new SparkConf()
    conf.set("spark.app.name","SparkSession-Program")
    conf.set("spark.master","local[*]")
    conf.set("spark.executor.memory","2g")

    val spark = new SparkSession.Builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

//  @todo first way to creating dataframe using toDF()
//    val orderData = List(
//            ("Order1", "John", 100),
//            ("Order2", "Alice", 200),
//            ("Order3", "Bob", 150),
//            ("Order4", "Alice", 300),
//            ("Order5", "Bob", 250),
//            ("Order6", "John", 400)
//          ).toDF("OrderID", "Customer", "Amount")
//    orderData.show()

//    @todo Second way to create dataframe using converting each tuple element into list using map(),rows()
//    @todo convert the list of row into rdd ,using createDataFrame() convert it into DF.
//    val orderData1 = List(
//      ("Order1", "John", 100),
//      ("Order2", "Alice", 200),
//      ("Order3", "Bob", 150),
//      ("Order4", "Alice", 300),
//      ("Order5", "Bob", 250),
//      ("Order6", "John", 400)
//    )
//    val schema = ("OrderID string, Customer string, Amount int")
//      val schema = StructType(List(
//          StructField("OrderID", StringType, true),
//          StructField("Customer", StringType, true),
//          StructField("Amount", IntegerType, true)
//        ))
//      val rows = orderData1.map(order => Row(order._1, order._2, order._3))
//      val df = spark.createDataFrame(
//              spark.sparkContext.parallelize(rows), schema)
//      df.show()

//    @todo Question 1: Employee Status Check
//           Create a DataFrame that lists employees with names and their work status. For each employee,
//           determine if they are “Active” or “Inactive” based on the last check-in date. If the check-in date is
//          within the last 7 days, mark them as "Active"; otherwise, mark them as "Inactive." Ensure the first letter of each name is capitalized.
//    val employees = List(
//      ("karthik", "2024-11-01"),
//      ("neha", "2024-10-20"),
//      ("priya", "2024-10-28"),
//      ("mohan", "2024-11-02"),
//      ("ajay", "2024-09-15"),
//      ("vijay", "2024-10-30"),
//      ("veer", "2024-10-25"),
//      ("aatish", "2024-10-10"),
//      ("animesh", "2024-10-15"),
//      ("nishad", "2024-11-01"),
//      ("varun", "2024-10-05"),
//      ("aadil", "2024-09-30")
//    ).toDF("name", "last_checkin")
//
//    val days_interval = employees.select(col("name"),
//      col("last_checkin"),
//      datediff(current_date(),col("last_checkin")).alias("days_interval")
//    )
//    val status_ = days_interval.select(initcap(col("name")).alias("name"),
//      col("last_checkin"),
//      when (col("days_interval") <= 7 ,"Active").otherwise("Inactive").alias("checkin_status")
//    )
//    status_.show()


//    @todo Question 2: Sales Performance by Agent
//      Given a DataFrame of sales agents with their total sales amounts, calculate the performance status
//      based on sales thresholds: “Excellent” if sales are above 50,000, “Good” if between 25,000 and
//      50,000, and “Needs Improvement” if below 25,000. Capitalize each agent's name, and show total
//      sales aggregated by performance status.

    val sales = List(
      ("karthik", 60000),
      ("neha", 48000),
      ("priya", 30000),
      ("mohan", 24000),
      ("ajay", 52000),
      ("vijay", 45000),
      ("veer", 70000),
      ("aatish", 23000),
      ("animesh", 15000),
      ("nishad", 8000),
      ("varun", 29000),
      ("aadil", 32000)
    ).toDF("name", "total_sales")

//    val sales_performance = sales.select(initcap(col("name")).alias("name"),col("total_sales"),
//      when (col("total_sales") > 50000, "Excellent")
//        .when(col("total_sales") > 25000 && (col("total_sales") <=50000),"Good")
//        .otherwise("Needs Improvement").alias("sales_statues")
//    )
//    val sum_sales = sales_performance.groupBy("sales_statues").agg(sum(col("total_sales"))).alias("total_sum")
//    sum_sales.show()

//    @todo Question 3: Project Allocation and Workload Analysis
//        Given a DataFrame with project allocation data for multiple employees, determine each employee's
//        workload level based on their hours worked in a month across various projects. Categorize
//        employees as “Overloaded” if they work more than 200 hours, “Balanced” if between 100-200 hours,
//        and “Underutilized” if below 100 hours. Capitalize each employee’s name, and show the aggregated
//        workload status count by category.

//    val workload = List(
//      ("karthik", "ProjectA", 120),
//      ("karthik", "ProjectB", 100),
//      ("neha", "ProjectC", 80),
//      ("neha", "ProjectD", 30),
//      ("priya", "ProjectE", 110),
//      ("mohan", "ProjectF", 40),
//      ("ajay", "ProjectG", 70),
//      ("vijay", "ProjectH", 150),
//      ("veer", "ProjectI", 190),
//      ("aatish", "ProjectJ", 60),
//      ("animesh", "ProjectK", 95),
//      ("nishad", "ProjectL", 210),
//      ("varun", "ProjectM", 50),
//      ("aadil", "ProjectN", 90)
//      ).toDF("name", "project", "hours")

//    val df = workload.groupBy(col("name")).agg(sum(col("hours")).alias("total_hours"))
//    val df1 = workload.select(initcap(col("name")).alias("name"),
//      col("project"),
//      col("hours"),
//      when(col("hours") > 200, "Overloaded")
//      .when(col("hours") >= 100 && col("hours") <= 200, "Balanced")
//      .otherwise("Underutilized").alias("workload")
//    )
//    val df2 = df1.groupBy(col("name")).agg(count(col("workload")).alias("workload_count"))
//
//    df2.show()

//    @todo 5. Overtime Calculation for Employees
//        Determine whether an employee has "Excessive Overtime" if their weekly hours exceed 60,
//        "Standard Overtime" if between 45-60 hours, and "No Overtime" if below 45 hours. Capitalize each
//        name and group by overtime status.

//    val employees = List(
//      ("karthik", 62),
//      ("neha", 50),
//      ("priya", 30),
//      ("mohan", 65),
//      ("ajay", 40),
//      ("vijay", 47),
//      ("veer", 55),
//      ("aatish", 30),
//      ("animesh", 75),
//      ("nishad", 60)
//    ).toDF("name", "hours_worked")
//
//    val df  = employees.select(initcap(col("name")).alias("name"),
//      col("hours_worked"),
//      when(col("hours_worked") > 60 ,"Excessive Overtime")
//        .when(col("hours_worked") > 45 && col("hours_worked") <= 60,"Standard Overtime")
//        .otherwise("No Overtime").alias("overtime_status")
//    )
//    val df1 = df.groupBy(col("overtime_status")).agg(count(col("name")).alias("total_overtime_status"))
//    df1.show()

//    @todo 6. Customer Age Grouping
//      Group customers as "Youth" if under 25, "Adult" if between 25-45, and "Senior" if over 45. Capitalize
//      names and show total customers in each group.

    val customers = List(
      ("karthik", 22),
      ("neha", 28),
      ("priya", 40),
      ("mohan", 55),
      ("ajay", 32),
      ("vijay", 18),
      ("veer", 47),
      ("aatish", 38),
      ("animesh", 60),
      ("nishad", 25)
    ).toDF("name", "age")
    val df = customers.withColumn("customer_age",
        when (col("age") < 25 , "Youth")
        .when(col("age").between(25, 45),"Adult")
        .otherwise("Senior")
      )
        .withColumn("name" , initcap(col("name")))
    val df1 = df.groupBy(col("customer_age")).agg(count(col("name")).alias("customer_count"))
    df1.show()


    scala.io.StdIn.readLine()
  }

}
