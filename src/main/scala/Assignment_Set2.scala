import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, dense_rank, lag, lead, max, min, round, sum, when}

object Assignment_Set2 {
  def main(args: Array[String]): Unit = {

//    @todo creating sparkContext
    val sc = new SparkContext("local[*]","rdd-Program")

//    @todo first way to create spark session
//    val spark = new SparkSession.Builder()
//      .appName("Assignment-Set2")
//      .master("local[*]")
//      .getOrCreate()

//    @todo second way to create spark session
    val conf = new SparkConf()
    conf.set("spark.app.name","Assignment-Set2")
    conf.set("spark.master","local[*]")

    val spark = new SparkSession.Builder()
    .config(conf)
    .getOrCreate()

    import spark.implicits._
//    @todo 1. we want to find the difference between the price on each day with it’s previous day.

//    val data = List (
//      (1, "KitKat",1000.0,"2021-01-01"),
//      (1, "KitKat",2000.0,"2021-01-02"),
//      (1, "KitKat",1000.0,"2021-01-03"),
//      (1, "KitKat",2000.0,"2021-01-04"),
//      (1, "KitKat",3000.0,"2021-01-05"),
//      (1, "KitKat",1000.0,"2021-01-06")
//    ).toDF("IT_ID","IT_Name","Price","PriceDate")
//
//    val window  = Window.orderBy(col("PriceDate"))
//    val previousDay_sal = data.withColumn("previous_day_sal",lag(col("Price"),1,0).over(window))
//    val salary_diff = previousDay_sal.withColumn("salary_diff",(col("price") - col("previous_day_sal")))
//    salary_diff.show()
//
//    val data_tbl = data.createOrReplaceTempView("data")
//    val data_Sql = spark.sql(
//      """
//          SELECT IT_ID,IT_Name,Price,PriceDate,
//          price - LAG(price,1,0) OVER(ORDER BY PriceDate) AS salary_diff
//          FROM data
//        """)
//    data_Sql.show()

//    @todo 2. If salary is less than previous month we will mark it as DOWN ;,
//             if salary has increased then UP;
//val salary = List(
//  (1,"John",1000,"2016-01-01"),
//  (1,"John",2000,"2016-01-02"),
//  (1,"John",1000,"2016-01-03"),
//  (1,"John",2000,"2016-01-04"),
//  (1,"John",3000,"2016-01-05"),
//  (1,"John",1000,"2016-01-06")
//).toDF("ID","NAME","SALARY","DATE")
//
//    val window = Window.orderBy(col("DATE"))
//    val previousDay_sal = salary.withColumn("previous_day_sal",lag(col("SALARY"),1,0).over(window))
//    val category = previousDay_sal.withColumn("salary_cate",
//      when (col("SALARY") < col("previous_day_sal"), "DOWN")
//        .otherwise("UP")
//    )
//    category.show()
//
//    val salary_tbl = salary.createOrReplaceTempView("salary_data")
//    val salary_sql = spark.sql(
//      """
//        SELECT ID,NAME,SALARY,DATE,
//        CASE
//        WHEN SALARY < LAG(SALARY,1) OVER(ORDER BY DATE) THEN 'DOWN'
//        ELSE 'UP'
//        END AS SALARY_CATEGORY
//        FROM salary_data
//        """)
//    salary_sql.show()

    val empData = List(
      (1,"karthik",1000),
      (2,"mohan",2000),
      (3,"vinay",1500),
      (4,"Deva",3000)
      ).toDF("id","name","salary")
    val empData_tbl = empData.createOrReplaceTempView("employee")

//    @todo 3. Calculate the lead and lag of the salary column ordered by id
//    val window = Window.orderBy(col("salary").desc)
//    val diff = empData.withColumn("lead_col",lead(col("salary"),1).over(window))
//      .withColumn("lag_col",lag(col("salary"),1).over(window))
//    diff.show()
//
//    val empData_tbl = empData.createOrReplaceTempView("employee")
//    val empData_sql = spark.sql(
//      """
//        SELECT id,name,salary,
//        LEAD(salary,1) OVER(ORDER BY id) as lead_tbl,
//        LAG(salary,1) OVER(ORDER BY id) as lag_tbl
//        FROM employee
//        """)
//    empData_sql.show()

//    @todo 4. Calculate the percentage change in salary from the previous row to the current row
//          , ordered by id (using the same sample data)

//    val window = Window.orderBy(col("id"))
//    val lag_cal = empData.withColumn("lag_col",lag(col("salary"),1,0).over(window))
//    val salaryDiff_percentage = lag_cal.withColumn("percentage_diff",(col("salary") - col("lag_col")) / col("salary") * 100)
//    salaryDiff_percentage.show()
//
//    val empData_sql = spark.sql(
//      """
//         WITH cte AS(
//        SELECT id,name,salary,
//        LAG(SALARY,1) OVER(ORDER BY id) AS  lag_col
//        FROM employee
//        )
//        select *,
//        ((salary - lag_col) / salary) * 100 as salary_diff
//        FROM cte
//        """)
//    empData_sql.show()

//    @todo 5. Calculate the rolling sum of salary for the current row and the previous two rows,
//       ordered by id.
//    val window = Window.orderBy(col("id"))
//    val rolling_sum = empData.withColumn("total_sum",sum(col("salary")).over(window))
//    rolling_sum.show()
//
//    val empData_sql = spark.sql(
//      """
//        SELECT id,name,salary,
//        SUM(salary) OVER(ORDER BY id) as rolling_sum
//        FROM employee
//        """)
//    empData_sql.show()

//    @todo 6. Calculate the difference between the current salary and the minimum salary within the last three rows, ordered by id.
//    val window = Window.orderBy(col("id"))
//    val salary_diff = empData.withColumn("salary_difference",min(lag(col("salary"),-3)).over(window))
//    val diff = salary_diff.withColumn("diff",col("salary") - col("salary_difference"))
//    diff.show()
//
//    val empData_sql = spark.sql(
//      """
//        SELECT id,name,salary,
//        salary - LAG(salary,-3) OVER(ORDER BY id) as diff
//        FROM employee
//        """)
//    empData_sql.show()

//    @todo 7. Calculate the lead and lag of salary within each group of employees (grouped by name) ordered by id.
//    val window = Window.partitionBy(col("name")).orderBy(col("id"))
//    val salary_grp = empData.withColumn("lead_col",
//      lead(col("salary"),1).over(window))
//      .withColumn("lag_col",lag(col("salary"),1).over(window))
//    salary_grp.show()

//    val window = Window.partitionBy(col("name")).orderBy(col("id"))
//    val salary_grp = empData.select(col("id"),
//      col("name"),
//      col("salary"),
//      lead(col("salary"),1).over(window).alias("lead_col"),
//      lag(col("salary"),1).over(window).alias("lag_col")
//    )
//    salary_grp.show()

//    val empData_sql = spark.sql(
//      """
//         SELECT id,name,salary,
//         LEAD(salary,1) OVER(PARTITION BY name ORDER BY id) as lead_col,
//         LAG(salary,1) OVER(PARTITION BY name ORDER BY id) as lag_col
//         FROM employee
//        """)
//    empData_sql.show()
//
//    @todo 8. Calculate the lead and lag of the salary column for each employee ordered by id, but only for the employees
//       who have a salary greater than 1500.
//    val filter_data = empData.filter(col("salary")  > 1500)
//    val window = Window.orderBy(col("id"))
//    val salary_cal = filter_data.withColumn("lead_Col",lead(col("salary"),1).over(window))
//      .withColumn("lag_col",lag(col("salary"),1).over(window))
//    salary_cal.show()

//    val empData_sql = spark.sql(
//      """
//        SELECT id,name,salary,
//        LEAD(salary,1) OVER(PARTITION BY name ORDER BY id) as lead_col,
//        LAG(salary,1) OVER(PARTITION BY name ORDER BY id) as lag_col
//        FROM employee
//        WHERE salary > 1500
//        """)
//    empData_sql.show()

//    @todo 9. Calculate the lead and lag of the salary column for each employee, ordered by id, but only for the employees
//         who have a change in salary greater than 500 from the previous row.
//    val window = Window.partitionBy(col("name")).orderBy(col("id"))
//    val salary_cal = empData.withColumn("lead_col",lead(col("salary"),1).over(window))
//      .withColumn("lag_col",lag(col("salary"),1).over(window))
//    val salary_comp = salary_cal.withColumn("salary_diff",(col("salary") - col("lag_col")))
//    val salary_500 = salary_comp.filter(col("salary_diff") > 500)
//    salary_500.show()
//
//    val empData_sql = spark.sql(
//      """
//         WITH cte AS(
//        SELECT id,name,salary,
//        LEAD(salary,1) OVER(PARTITION BY name ORDER BY id) as lead_col,
//        LAG(salary,1) OVER(PARTITION BY name ORDER BY id) as lag_col
//        FROM employee
//        )
//        SELECT *,
//        (salary - lag_col) as diff
//        FROM cte
//        WHERE (salary - lag_col) > 500
//        """)
//    empData_sql.show()

//    @todo 10. Calculate the cumulative count of employees, ordered by id, and reset the count when the name changes.
//    val window = Window.partitionBy(col("name")).orderBy(col("id"))
//    val cumlative_count = empData.withColumn("emp_count"       )

//    @todo 11. Calculate the running total of salary for each employee ordered by id.
//    val window = Window.partitionBy(col("name")).orderBy(col("id"))
//    val total = empData.withColumn("running_total",sum(col("salary")).over(window))
//    total.show()
//
//    val empData_sql = spark.sql(
//      """
//        SELECT id,name,salary,
//        SUM(salary) OVER(PARTITION BY name ORDER BY id) as running_total
//        FROM employee
//        """)
//    empData_sql.show()

//    @todo 12. Find the maximum salary for each employee’s group (partitioned by name) and display it for each row.
//    val window = Window.partitionBy(col("name")).orderBy(col("id"))
//    val max_sal = empData.withColumn("max_sal",max(col("salary")).over(window))
//    max_sal.show()

//    val empData_sql = spark.sql(
//      """
//        SELECT id,name,salary,
//        MAX(salary) OVER(PARTITION BY name ORDER BY id) as max_sal
//        FROM employee
//        """)
//    empData_sql.show()

//    @todo 13. Calculate the difference between the current salary and the average salary for each employee’s group
//      (partitioned by name) ordered by id.
//    val window = Window.partitionBy(col("name")).orderBy(col("id"))
//    val avg_salary = empData.withColumn("avg_salary",avg(col("salary")).over(window))
//    val salary_diff = avg_salary.withColumn("salary_diff",(col("salary") - col("avg_salary")))
//    salary_diff.show()
//
//    val empData_sql = spark.sql(
//      """
//        SELECT id,name,salary,
//        (salary - AVG(salary) OVER(PARTITION BY name ORDER BY id)) AS  salary_diff
//        FROM employee
//        """)
//    empData_sql.show()

//    @todo 14. Calculate the rank of each employee based on their salary, ordered by salary in descending order.
//    val window = Window.partitionBy(col("name")).orderBy(col("salary").desc)
//    val rank_emp = empData.withColumn("ranking",dense_rank().over(window))
//    rank_emp.show()
//
//    val empData_sql = spark.sql(
//            """
//              SELECT id,name,salary,
//              DENSE_RANK() OVER(PARTITION BY name ORDER BY salary DESC) AS  ranking
//              FROM employee
//              """)
//          empData_sql.show()

//    @todo 15. Calculate the lead and lag of the salary column, ordered by id, but only for the employees whose salaries
//         are strictly increasing (i.e., each employee’s salary is greater than the previous employee’s salary).
//     val window = Window.orderBy(col("salary"))
//    val salary_find = empData.withColumn("lead_col", lead(col("salary"),1).over(window))
//      .withColumn("lag_col",lag(col("salary"),1).over(window))
//    val salary_increment = salary_find.withColumn("increment_salary",(col("salary") > col("lag_col")))
//    salary_increment.show()
//
//    val empData_sql = spark.sql(
//            """
//              SELECT id,name,salary,
//              LEAD(salary,1) OVER(ORDER BY id) as lead_col,
//              (salary - LAG(salary,1) OVER(ORDER BY id)) as lag_col
//              FROM employee
//      """)
//    empData_sql.show()

//   @todo 16. Calculate the lead and lag of the salary column ordered by id,
//        but reset the lead and lag values when the employee’s name changes.
//    val window = Window.partitionBy(col("name")).orderBy(col("id"))
//    val salary_find = empData.withColumn("lead_col", lead(col("salary"),1).over(window))
//          .withColumn("lag_col",lag(col("salary"),1).over(window))
//    salary_find.show()
//
//    val empData_sql = spark.sql(
//            """
//              SELECT id,name,salary,
//              LEAD(salary,1) OVER(PARTITION BY name ORDER BY id) as lead_col,
//              LAG(salary,1) OVER(PARTITION BY name ORDER BY id) as lag_col
//              FROM employee
//              """)
//          empData_sql.show()

//   @todo 17. Calculate the percentage change in salary from the previous row to the current row,
//        ordered by id, but group the percentage changes by name.
    val window = Window.orderBy(col("id"))
    val salary_change = empData.withColumn("lag_col",lag(col("salary"),1).over(window))
    val percent_diff = salary_change.withColumn("percentage_change",(col("salary") - col("lag_col")) / col("salary") * 100)
    val diff = percent_diff.groupBy(col("name"))
    percent_diff.show()

    val empData_sql = spark.sql(
            """
              WITH cte AS(
              SELECT id,name,salary,
              LAG(salary,1) OVER(ORDER BY id) as lag_col
              FROM employee
              )
              SELECT * ,
              ((salary -lag_col) / salary * 100) as percentage_change
              FROM cte

              """)
          empData_sql.show()















  spark.stop()
  }

}
