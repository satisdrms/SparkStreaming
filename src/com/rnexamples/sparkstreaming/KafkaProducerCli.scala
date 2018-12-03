package com.rnexamples.sparkstreaming

import java.util.{ Date, Properties }
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

object KafkaProducerCli {
  def main(args: Array[String]): Unit = {
    val msg: String = args(0)
    val topic = args(1)
    val brokers = args(2)
    //val msg: String = """{"iata":"00R","airport":"Livingston Municipal","city":"Livingston","state":"TX","country":"USA","lat":30.68586111,"long":-95.01792778}"""
    //val topic = "airports"
    //val brokers = "sandbox-hdp:6667"
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
    val data = new ProducerRecord[String, String](topic, msg)
    producer.send(data)
    producer.close()
  }
}