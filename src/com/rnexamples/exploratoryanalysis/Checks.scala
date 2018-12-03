package com.rnexamples.exploratoryanalysis

import org.apache.spark.sql.{ SQLContext, SparkSession }

import com.databricks.spark.avro._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import java.util.{ Calendar, Date }

object Checks {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("StreamingToHDFSSink").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate();
    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val airports = spark.read.format("com.databricks.spark.avro").option("header", "true").load("/user/kafkawrite/avro/airports")
    val carriers = spark.read.format("com.databricks.spark.avro").option("header", "true").load("/user/kafkawrite/avro/carriers")
    val planes = spark.read.format("com.databricks.spark.avro").option("header", "true").load("/user/kafkawrite/avro/planes")
    val flights = spark.read.format("com.databricks.spark.avro").option("header", "true").load("/user/kafkawrite/avro/flights")

    airports.printSchema()
    carriers.printSchema()
    planes.printSchema()
    flights.printSchema()

    //airports.registerTempTable("airports")
    airports.createOrReplaceTempView("airports")
    carriers.createOrReplaceTempView("carriers")
    planes.createOrReplaceTempView("planes")
    flights.createOrReplaceTempView("flights")

    val cal = Calendar.getInstance()
    val date = cal.get(Calendar.DATE)
    val Year = cal.get(Calendar.YEAR)

    //When is the best time of day/day of week/time of year to fly to minimise delays?
    
    spark.sqlContext.sql("select DepTime,DayOfWeek,DayofMonth,Month,Year,origin,dest,avg(case when ArrDelay is null then 0 else arrdelay end),avg(case when DepDelay is null then 0 else depdelay end) from flights group by DepTime,DayOfWeek,DayofMonth,Month,Year,origin,dest order by 8,9 desc").show
    spark.sqlContext.sql("select * from (select DepTime,origin,dest,avg(case when ArrDelay is null then 0 else arrdelay end) avgarvdelay,avg(case when DepDelay is null then 0 else depdelay end) avgdepdelay from flights group by DepTime,origin,dest ) a join (select origin,dest,min(case when ArrDelay is null then 0 else arrdelay end) minArrDelay,min(case when DepDelay is null then 0 else depdelay end) minDepDelay from flights group by origin,dest ) b on (a.origin=b.origin and a.dest=b.dest) where avg").show
        

    //Do older planes suffer more delays?
    // finding the average the delay between set of origin and destinations and comparing it with avg delay with respoect to age of planes travelling between the same origin and destinations
    spark.sqlContext.sql("select * from (select Origin,Dest,avg(ArrDelay),avg(DepDelay) from flights group by Origin,Dest) avgDelays join (select Origin,Dest,datediff(current_Date(),to_date(issue_date,'mm/dd/yyyy')) daysOld,avg(ArrDelay),avg(DepDelay) from planes p join flights f on (p.TailNum=f.TailNum) where issue_date<>'null' group by Origin,Dest,datediff(current_Date(),to_date(issue_date,'mm/dd/yyyy'))) delaysbyage on (avgDelays.origin=delaysbyage.origin and avgDelays.dest=delaysbyage.dest) order by daysOld desc").show

    spark.sqlContext.sql("select count(*) from airports");

    //Come up with interesting insights you find through simple exploratory analysis
    //When is the best time of day/day of week/time of year to fly to minimise delays?
    //Do older planes suffer more delays?
    //How does the number of people flying between different locations change over time?
    //How well does weather predict plane delays?
    //Come up with data model that can predict below
    //Can you detect cascading failures as delays in one airport create delays in others? Are there critical links in the system?

  }

}