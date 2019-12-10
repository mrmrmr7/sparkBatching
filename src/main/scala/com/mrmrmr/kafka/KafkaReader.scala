package com.mrmrmr.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object KafkaReader {
  def getHotelWithWeatherDF(session: SparkSession): DataFrame = {
    val hotelWithWeatherSchema = new StructType()
      .add("Id", StringType)
      .add("Name", StringType)
      .add("Country", StringType)
      .add("City", StringType)
      .add("Address", StringType)
      .add("Latitude", StringType)
      .add("Longitude", StringType)
      .add("GeoHash", StringType)
      .add("year", IntegerType)
      .add("month", IntegerType)
      .add("day", IntegerType)
      .add("avg_tmpr_c", DoubleType)
      .add("lng", DoubleType)
      .add("lat", DoubleType)
      .add("Precision", IntegerType)

    session
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value as string) as value")
      .drop(col("key"))
      .select(from_json(col("value"), hotelWithWeatherSchema).as("data"))
      .select("data.*")
  }
}