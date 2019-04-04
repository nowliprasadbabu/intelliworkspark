package com.bigdata.apache.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._

import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafka_wordcount {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("kafka_wordcount").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[2]").appName("kafka_wordcount").getOrCreate()
    import spark.implicits._
    import spark.sql
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    import scala.util.Try
    val topics =  Try(args(0).toString).getOrElse("aabb")
    val brokers = Try(args(1).toString).getOrElse("localhost:4444") //9092
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
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    //import kafka.serializer.StringDecoder //kafka client jars

    lines.foreachRDD{ x=>
      println(s"processing first line : $x")
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = x.map(x=>x.split(" ")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.show()

    }

    ssc.start() // Start the computation
    ssc.awaitTermination()
    //spark.stop()

  }
}
