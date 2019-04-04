

package com.bigdata.apache.sparkstreaming

import org.apache.spark.sql.SparkSession

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession

import org.apache.spark.streaming._
import org.apache.spark.streaming._
/*
Please add all kafka dependencies and spark_kafka_streaming 10 version jar
com.bigdata.spark.sparkstreaming.sparkKafkaProducer
 */
// sqoop vs kafka
// sqoop : data injection tools: auto it create jar/code to import or export data
//kafka : ur responsibility to import/get data create a code
//kafka: messaging system ..
//
object sparkKafkaProducer {
  def main(args: Array[String]) {
    val topic: String = if (args.length > 0) args(0) else "personal"

    val spark = SparkSession.builder.master("local[2]").appName("kafka_wordcount").getOrCreate()
    import spark.implicits._
    import spark.sql
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    import scala.util.Try
    //val path = args(0)
    val path="C:\\work\\datasets\\asl\\data"
    val data = spark.sparkContext.textFile(path)
    data.foreachPartition(rdd => {
      import java.util._

      val props = new java.util.Properties()
      //  props.put("metadata.broker.list", "localhost:9092")
      //      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("bootstrap.servers", "localhost:4444")

      // import kafka.producer._
      // val config = new ProducerConfig(props)
      val producer = new KafkaProducer[String, String](props)

      rdd.foreach(x => {
        println(x)
        producer.send(new ProducerRecord[String, String](topic.toString(), x.toString)) //sending to kafka broker
        //(topic, "venu,32,hyd")
        //(indpak,"anu,56,mas")
        Thread.sleep(5000)

      })

    })
    ssc.start() // Start the computation
    ssc.awaitTermination()
  }
}
