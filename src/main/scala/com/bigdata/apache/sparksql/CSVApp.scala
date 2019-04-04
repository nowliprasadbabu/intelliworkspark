

package com.bigdata.apache.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark._

object CSVApp {
  def main(args: Array[String]) {
    println("hi")
    val spark = SparkSession.builder.master("local[*]").appName("CSVApp").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql

    val cdata ="file:///C:/nowli/Book1.csv"
    val data = spark.read.option("header","true").option("inferSchema","true").csv(cdata)

   // data.write.format("parquet").partitionBy("Date").saveAsTable("cricketinfo")

    data.groupBy("batsman").avg("total_runs").withColumn("runRate", lit((2)) ).show()







/*
    data.createOrReplaceTempView("crick")
    val odata = spark.sql("select distinct(*) from crick")
    odata.createOrReplaceTempView("ocrick")
    spark.sql("select batsman, avg(total_runs) as AvgRuns from ocrick group by batsman").show()
*/

    spark.stop()
  }
}