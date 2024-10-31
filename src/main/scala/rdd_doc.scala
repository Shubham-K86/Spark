import org.apache.spark.SparkContext

import scala.Console.println

object rdd_doc {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]","rdd-program")

//    parallelize method : covert normal array/normal list into rdd
      val arr = Array(1,2,3,4,5)
      val rdd_arr = sc.parallelize(arr)
      rdd_arr.collect().foreach(println)
//    rdd_arr.saveAsTextFile("/Downloads/oct31.txt")

//    find average using mean() method
      val rdd_mean = rdd_arr.mean()
      println(f"mean() of rdd_mean : $rdd_mean")

//    find average without using function
      val rdd_reduce = rdd_arr.reduce((x,y)=>x+y)
      val rdd_count = rdd_arr.count()
      val rdd_average = rdd_reduce/rdd_count.toDouble
      println(f"average of rdd_arr : $rdd_average")

//    filter() method :
      val rdd_even = rdd_arr.filter(x => x % 2 == 0)
      val rdd_mul = rdd_even.map(x => (x * 100))
      rdd_mul.collect().foreach(println)

//    join operation :
      val rdd1 = sc.parallelize(Array((1, "apple"), (2, "banana"), (3, "orange")))
      val rdd2 = sc.parallelize(Array((1, "red"), (2, "yellow"), (4, "green")))
//      val rdd_join = rdd1.join(rdd2)
      val rdd_leftjoin = rdd1.leftOuterJoin(rdd2)
      rdd_leftjoin.collect().foreach(println)

//    remove duplicate using distinct()
      val rdd = sc.parallelize(Array(1, 2, 3, 2, 1, 4, 5))
      val rdd_distinct = rdd.distinct()
      rdd_distinct.collect().foreach(println)

//    subtract() method return unique element present in first rdd
      val rdd_arr1 = sc.parallelize(Array(1, 2, 3, 4, 5))
      val rdd_arr2 = sc.parallelize(Array(3, 4, 5))
      val rdd_subtract = rdd_arr1.subtract(rdd_arr2)
      rdd_subtract.collect().foreach(println)

//    union() method return all the elements present in both rdd including duplicates
      val rdd_un1 = sc.parallelize(Array(1, 2, 3))
      val rdd_un2 = sc.parallelize(Array(3, 4, 5))
      val rdd_union = rdd_un1.union(rdd_un2)
      rdd_union.collect().foreach(println)

//    cartesion() method return all possible combination created between two rdd.
      val rdd_cr1 = sc.parallelize(Array(1, 2, 3))
      val rdd_cr2 = sc.parallelize(Array("A", "B"))
      val rdd_cartesian = rdd_cr1.cartesian(rdd_cr2)
      rdd_cartesian.collect().foreach(println)

//    intersection() return common element present in both rdds.
      val rdd_in1 = sc.parallelize(Array(1, 2, 3, 4, 5))
      val rdd_in2 = sc.parallelize(Array(4, 5, 6, 7, 8))
      val rdd_intersection = rdd_in1.intersection(rdd_in2)
      rdd_intersection.collect().foreach(println)

//    filter() method return the exact match string from rdd.
      val rdd_sc = sc.parallelize(Array("apple", "banana","orange", "pear", "grape"))
      val searchItem = "pear"
      val rdd_filter = rdd_sc.filter(x => x == searchItem)
      rdd_filter.collect().foreach(println)

//    contains() method return search for all elements in the RDD that contain the substring "an"
      val rdd_con = sc.parallelize(Array("apple", "banana","orange", "pear", "grape"))
      val searchItems = "an"
      val rdd_contains = rdd_sc.filter(x => x.contains(searchItems))
      rdd_contains.collect().foreach(println)

//    Given an RDD containing words, find the most frequent 10 words
      val rdd_str = sc.parallelize(Seq("hello", "world", "hello", "world", "world", "hello", "hi"))
      val rdd_map = rdd_str.map(x => (x,1))
      val rdd_red = rdd_map.reduceByKey((x,y) => (x+y))
      val rdd_sort = rdd_red.sortBy(x => x._2,false)
      val rdd_m = rdd_sort.map(x => x._1)
      rdd_m.take(10).foreach(println)

//        rdd_str = sc.parallelize(["hello", "world", "hello", "world", "world", "hello", "hi"])
//        rdd_map = rdd_str.map(lambda x : (x,1))
//        rdd_red = rdd_map.reduceByKey(lambda x,y : x+y)
//        rdd_sort = rdd_red.sortBy(lambda x : x._2,false)
//        rdd_m = rdd_sort.map(lambda x : x._1)
//        rdd_m.take(10).foreach(println)




    scala.io.StdIn.readLine()
  }
}