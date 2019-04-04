package com.bigdata.apache.sparksql
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DateFuncs {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[*]").appName("DateFuncs").getOrCreate()
    val sc = spark.sparkContext
    //val conf = new SparkConf().setAppName("SparkDateFuncs").setMaster("local[*]")
    //val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val date_data = "file:///C:\\nowli\\datasets\\DateFunc_Data.csv"

    val datedf = spark.read.format("csv").option("inferSchema", "true").load(date_data).toDF("dob", "name", "loc")
   // datedf.show()
    datedf.printSchema()


    //datedf.withColumn("newdate",to_date($"dob")).show()
    //datedf.withColumn("ndate",date_format($"dob","dd-MMM-yyyy")).show()
   // datedf.withColumn("datetime",date_format($"dob","dd-MM-yyyy HH:mm:ss:SSS")).show(false)
   // datedf.withColumn("daymonth", dayofmonth($"dob")).show()
    // datedf.withColumn("dayyear",dayofyear('dob)).show()
   // datedf.withColumn("dayweek",dayofweek($"dob")).show()
   // datedf.withColumn("lastday",last_day($"dob")).show()
   // datedf.withColumn("nextday", next_day($"dob", "TU")).show()
   // datedf.select(to_date($"dob").alias("Formatted_Date"),$"name",$"loc").show()
    //datedf.withColumnRenamed("dob","Date").show()
   // datedf.withColumn("currdate", current_date()).show()
    //datedf.withColumn("Year", year('dob)).withColumn("WeekYear",weekofyear('dob)).show()
   // datedf.withColumn("month", month('dob)).show()
   // datedf.withColumn("addmonth",add_months('dob,2)).show()
   // datedf.withColumn("dateadd",date_add('dob,-30)).show()
   // datedf.withColumn("datesub", date_sub('dob,5)).show()

   // datedf.select(to_date($"dob").alias("Formatted_dob"),$"name",$"loc").withColumn("today",current_date()).withColumn("Diff",datediff($"today",$"Formatted_dob")).show()

   // datedf.select(to_date($"dob").alias("Formatted_Date"),$"name",$"loc").orderBy($"Formatted_Date".desc).show()

    // datedf.withColumn("currTimeStamp", current_timestamp()).show(false)

      datedf.withColumn("UNIXTSP",unix_timestamp())
      .withColumn("UTSTOCURR",from_unixtime(lit("1552530621")))
      .withColumn("ASPST",from_utc_timestamp($"UTSTOCURR","PST")).show()



    spark.stop()

  }
}