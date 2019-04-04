package com.bigdata.apache.sparksql


import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DateTimeApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[*]").appName("DateTimeApp").getOrCreate()
    val sc = spark.sparkContext
    //val conf = new SparkConf().setAppName("SparkDateFuncs").setMaster("local[*]")
    //val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val filterCriteria = Array(30, 60, 90, 0, -1)

    filterCriteria.foreach { tmp =>

      val criteria = tmp.toInt

      val current = date_sub(to_date(current_timestamp), 0)

      val presentmonth = date_add(current, 0) //oct 31 day

      val startdate306090 = date_add(current, -criteria)

      val tabname = "voot_adsales_views_daywise306090l_this_prev_month_from_oct_31"

      println(s"processing $criteria days info")

      val lastDayOfPrevMonth = last_day(add_months(current, -1))

      val nextMonthfirstDay = date_add(last_day(current), 1) //nov 1

      val firstDayOfMonth = add_months(nextMonthfirstDay, -1) // oct 1

      val firstDayOfPrevMonth = add_months(nextMonthfirstDay, -2) // oct 1



      val startdate = if (criteria==0) s"$firstDayOfMonth" else if (criteria == -1) s"$firstDayOfPrevMonth" else s"$startdate306090"

      val enddate = if (criteria == -1) s"$lastDayOfPrevMonth" else s"$current"

      val data = "file:///C:\\nowli\\datasets\\DateFunc_Data.csv"

      val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data).toDF("dt","name","city")

      df.createOrReplaceTempView("tab")

      val res = spark.sql(s"select $current current, $firstDayOfMonth firstDayOfMonth, $firstDayOfPrevMonth firstDayOfPrevMonth, $lastDayOfPrevMonth lastDayOfPrevMonth, $startdate startdate, $enddate enddate, to_date(dt) dt  from tab")

      println("original data")

      res.show()

      spark.sql(s"select $current current, $firstDayOfMonth firstDayOfMonth, $firstDayOfPrevMonth firstDayOfPrevMonth, $lastDayOfPrevMonth lastDayOfPrevMonth, $startdate startdate, $enddate enddate, to_date(dt) dt  from tab where dt between $startdate and $enddate").show()

    }


    spark.stop()
  }

}