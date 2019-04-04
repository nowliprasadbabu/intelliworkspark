

package com.bigdata.apache.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.util.Try

object Patient {

  //System.setProperty("hadoop.home.dir", "C:\\winutils");

  case class Patient(DRGDefinition:String, ProviderId:Option[Int], ProviderName:String, providerStreetAddress: String,
                     ProviderCity:String,ProviderState:String,ProviderZipCode:Option[Int],HospitalReferralRegionDescription:String,
                     TotalDischarges:Option[Int],AverageCoveredCharges:Option[Double],AverageTotalPayments:Option[Double],
                     AverageMedicarePayments:Option[Double])

  def main(args: Array[String]) {

    val config = new SparkConf().setAppName("Patient").setMaster("local")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    val rowsRDD = sc.textFile("file:///C:\\nowli\\SampleInputData.txt")
    val RDD = rowsRDD.map{row => row.split("\t")}
      .map{x => Patient(x(0),Try(x(1).toInt).toOption,x(2),x(3),x(4),x(5),Try(x(6).toInt).toOption,x(7),
        Try(x(8).toInt).toOption,Try(x(9).toDouble).toOption,Try(x(10).toDouble).toOption,
        Try(x(11).toDouble).toOption)}

    val df = RDD.toDF()

    df.show()
    df.printSchema()

    println(df.groupBy("ProviderState").avg("AverageTotalPayments").orderBy("ProviderState").count())

    //Problem Statement 1: Find the amount of Average Covered Charges per state.

    df.groupBy("ProviderState").avg("AverageCoveredCharges").show

    //Problem Statement 2: Find the amount of Average Total Payments charges per state.

    df.groupBy("ProviderState").avg("AverageTotalPayments").show

    //Problem Statement 3: Find the amount of Average Medicare Payments charges per state.

    df.groupBy("ProviderState").avg("AverageMedicarePayments").show

    //Problem Statement 4: Find out the total number of Discharges per state and for each disease.

    df.groupBy("ProviderState","DRGDefinition").sum("TotalDischarges").show
    df.groupBy("ProviderState","DRGDefinition").sum("TotalDischarges").orderBy(desc(sum("TotalDischarges").toString)).show
  }
}