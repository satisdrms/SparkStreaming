package com.rnexamples.utils

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SQLContext, SparkSession }

import java.util.{ Date, Properties }
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

object convertCSVToJSON {
  val pushToKafka = (topic: String, jsonRecord: String) => {
    val brokers = "sandbox-hdp:6667"
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "KafkaProducer-" + topic)
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()
    val data = new ProducerRecord[String, String](topic, jsonRecord)
    producer.send(data)
    producer.close()
  }

  val spark = SparkSession.builder.master("local").appName("StreamingToHDFSSink").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate();
  val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
  import sqlContext.implicits._

  val convertToJsonAndPushToKafka = (topic: String, fileName: String) => {
    val csvMessage = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/user/data/" + fileName)
    csvMessage.write.json("/user/data/json/" + topic)
    spark.sparkContext.textFile("/user/data/json/" + topic + "/*").foreach(jsonRecord => pushToKafka(topic, jsonRecord))
  }

  convertToJsonAndPushToKafka("airports", "airports.csv")
  convertToJsonAndPushToKafka("carriers", "carriers.csv")
  convertToJsonAndPushToKafka("planes", "plane-data.csv")
  convertToJsonAndPushToKafka("flights", "2008_100.csv")

}