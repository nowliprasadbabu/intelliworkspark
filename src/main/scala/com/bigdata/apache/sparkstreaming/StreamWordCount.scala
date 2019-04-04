

package com.bigdata.apache.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamWordCount {
  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: StreamWordCount <hostname> <port>")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamWordCount").set("spark.driver.allowMultipleContexts", "true")
    val ssc = new StreamingContext(conf, Seconds(60))

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()
    wordCounts.saveAsTextFiles("file:///C:/nowli/datasets/abc")

    ssc.start()
    ssc.awaitTermination()


    // in windows, nc -l -p 4444
    // in ubtuntu, nc -lk 4444
    // nc64.exe -nlvp 4444
    // nc -nv 192.168.1.132 4444

    // pass 192.168.1.103 4444 as args to StreamWordCount.scala and run
  }
}