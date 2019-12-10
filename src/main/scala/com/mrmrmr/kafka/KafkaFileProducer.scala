package com.mrmrmr.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

object KafkaFileProducer {
  def main(args: Array[String]): Unit = {
    val filePath = "D:\\course\\spark\\untitled\\src\\main\\resources\\data_mapped_optimized.txt"
    val bootstrapServer = "sandbox-hdp.hortonworks.com:6667"
    val topic = "test"
    val source = Source.fromFile(filePath)

    var counter = 0
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.RETRIES_CONFIG, 5.toString)
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384.toString)
    properties.put(ProducerConfig.LINGER_MS_CONFIG, 1.toString)
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Integer.MAX_VALUE.toString)

    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val producer = new KafkaProducer[String, String](properties)

//    producer.send(record)
    for (data <- source.getLines()) {
      counter += 1
      if (counter % 10000 == 0){
        println(counter)
      }
      val record = new ProducerRecord[String, String](topic, "0", data)
      producer.send(record)
      //MyCustomKafkaProducer.produce(bootstrapServer, topic, "0", data)
    }
    producer.close()

    source.close
  }
}
