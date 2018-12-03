package com.rnexamples.sparkstreaming

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SQLContext, SparkSession }

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import com.databricks.spark.avro._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object DataStreamingHDFSSink {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("StreamingToHDFSSink").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate();
    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    val streamingSparkContext = new StreamingContext(spark.sparkContext, Seconds(60))
    val topic = args(0)

    val brokers = args(1)
    //val topic = "airports"
    //val brokers = "sandbox-hdp:6667"

    val convertToAvro = (topic: String, brokers: String) => {
      val topicsSet = topic.split(",").toSet
      val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers, "key.deserializer" -> classOf[StringDeserializer], "value.deserializer" -> classOf[StringDeserializer], "group.id" -> ("KafkaConsumer-" + topic), "auto.offset.reset" -> "earliest", "enable.auto.commit" -> (false: java.lang.Boolean))
      val messages = KafkaUtils.createDirectStream[String, String](streamingSparkContext, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
      messages.foreachRDD(rddRaw => {
        val rdd = rddRaw.map(_.value.toString)
        val df = spark.read.json(rdd)
        df.printSchema()
        df.write.mode("append").option("avroSchema", StreamingSchemas.schemaMatch(topic)).avro(s"/user/kafkawrite/avro/" + topic)
        df.write.format("parquet").mode("append").save(s"/user/kafkawrite/parquet/" + topic)
      })
    }
    convertToAvro(topic, brokers)
    streamingSparkContext.start();
    streamingSparkContext.awaitTermination()
  }
}

