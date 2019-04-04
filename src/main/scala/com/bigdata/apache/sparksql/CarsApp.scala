package com.bigdata.apache.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark._

object CarsApp {
  def main(args: Array[String]) {
   // println("hi")
    val spark = SparkSession.builder.master("local[*]").appName("CarsApp").getOrCreate()
    val sc = spark.sparkContext

    val input = "file:///C:/nowli/datasets/cars.csv"

  //  val input = args(0)
   // val output = args(1)

    val data = spark.read.format("csv").option("delimiter",";").option("header","true").option("inferSchema","true").load(input)
    data.show(false)
    data.printSchema()

  // data.write.format("csv").save(output)
   data.write.format("csv").save("hdfs://localhost:9000/prasad/cars7")

    spark.stop()
  }
}