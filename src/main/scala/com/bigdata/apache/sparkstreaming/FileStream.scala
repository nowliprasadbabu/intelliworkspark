package com.bigdata.apache.sparkstreaming

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object FileStream {
  def main(args: Array[String]) {
    val ssc = new StreamingContext("local[2]", "FileStream", Seconds(20))
    val lines = ssc.textFileStream("file:///C:/nowli/datasets/StreamFileData")
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
