

package com.bigdata.apache.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrateg8wies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sparkKafkaConsumer {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkKafkaConsumer").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[2]").appName("kafka_wordcount").getOrCreate()
    import spark.implicits._
    import spark.sql
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    import scala.util.Try
    val topics = "personal"
    val brokers = "localhost:9092"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "bootstrap.servers"->"localhost:4444",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kaf",
      "key.serializer"->"org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer"->"org.apache.kafka.common.serialization.StringSerializer",
      "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer")

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,         // this is to run the
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    val lines = messages  // lines is a dStream  map(x=>x.value) or map(x=>x.key)
    // val data = args(0)

    lines.foreachRDD { x =>

      println(s"processing first line : $x")
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      //val df1=spark.read.format("csv").option("delimiter"," ").load(data)
      //val dfschema=df1.schema()

      val df = x.map(x=>x.value).map(x => x.split(",")).map(x => (x(0), x(1), x(2))).toDF("name", "age", "city")
      // val df = spark.read.format("csv").schema(dfschema).load(data)
      df.show()
      val mdf=df.where($"city"==="mas")
      val hdf=df.where($"city"==="hyd")

      val oUrl = "jdbc:oracle:thin://@oracledb.czzw4mblymse.ap-south-1.rds.amazonaws.com:1521/ORCL"
      val oProp = new java.util.Properties()
      oProp.setProperty("user","ousername")
      oProp.setProperty("password","opassword")
      oProp.setProperty("driver","oracle.jdbc.OracleDriver")
      //df.createOrReplaceTempView("personal")
      df.write.mode("append").jdbc(oUrl,"kafkalogssunil",oProp)
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

    spark.stop()
  }
}
