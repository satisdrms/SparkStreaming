package com.rnexamples.sparkstreaming

import com.rnexamples.utils.HdfsUtils
import java.util.{ Date, Properties }
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util
import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.StringDeserializer 
import scala.util.parsing.json._


object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val msg: String = args(0)
    //val topic = args(1)
    //val brokers = args(2)
    //val msg: String = """{"iata":"00R","airport":"Livingston Municipal","city":"Livingston","state":"TX","country":"USA","lat":30.68586111,"long":-95.01792778}"""
    val topics = "airports"
    val brokers = "sandbox-hdp:6667"
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("group.id", "KafkaConsumer-" + topics)
    props.put("key.deserializer",classOf[StringDeserializer])
    props.put("value.deserializer",classOf[StringDeserializer])
    val kafkaConsumer = new KafkaConsumer[String, String](props)
    kafkaConsumer.subscribe(util.Arrays.asList(topics))

    while (true) {
      val results = kafkaConsumer.poll(2000).asScala
      //for ((topic, data) <- results) {
       // println(data)
     // }
      for(k <-results){
        println("------------------ ------ ------ ------ ")
        val parsed = JSON.parseFull(k.value())
        //print(parsed.map(_.asInstanceOf[Map[String, Any]]("city")))
        HdfsUtils.putKeyValToHDFS("/tmp/airports",parsed.map(_.asInstanceOf[Map[String, Any]]("iata")).fold("")(_.toString),k.value())
      }
    }
  }
}