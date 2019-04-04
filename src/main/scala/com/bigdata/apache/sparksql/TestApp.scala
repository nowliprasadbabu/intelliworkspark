package com.bigdata.apache.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark._

object TestApp {
  def main(args: Array[String]) {
   // println("hi")
    val spark = SparkSession.builder.master("local[*]").appName("TestApp").getOrCreate()
    val sc = spark.sparkContext

    val input = "file:///C:/nowli/datasets/cars.csv"

    val data = spark.read.format("csv").option("delimiter",";").option("header","true").option("inferSchema","true").load(input)
    data.show(4,false)
    data.printSchema()

    spark.stop()
  }
}