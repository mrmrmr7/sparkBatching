package com.mrmrmr.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object MyCustomKafkaProducer {
  def produce(server: String, topic: String, key: String, value: String): Unit = {

    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.RETRIES_CONFIG, 5.toString)
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384.toString)
    properties.put(ProducerConfig.LINGER_MS_CONFIG, 1.toString)
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Integer.MAX_VALUE.toString)

    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val producer = new KafkaProducer[String, String](properties)
    val record = new ProducerRecord[String, String](topic, key, value)

    producer.send(record)
    producer.close()
  }
}
