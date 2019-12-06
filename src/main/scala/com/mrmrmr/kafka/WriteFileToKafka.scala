package com.mrmrmr.kafka

import scala.io.Source

object WriteFileToKafka {
  def main(args: Array[String]): Unit = {
    val filePath = "D:\\course\\spark\\untitled\\src\\main\\resources\\data_mapped_optimized.txt"
    val bootstrapServer = "sandbox-hdp.hortonworks.com:6667"
    val topic = "test"
    val source = Source.fromFile(filePath)

    for (data <- source.getLines()) {
      MyCustomKafkaProducer.produce(bootstrapServer, topic, "0", data)
    }

    source.close
  }
}
