package com.mrmrmr

import com.mrmrmr.hdfs.{ExpediaReader, ExpediaWriter}
import com.mrmrmr.kafka.KafkaReader
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StartHere {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder
      .master("local[*]")
      .appName("KafkaReader")
      .getOrCreate()

    val hdfsMaster = session.sparkContext.getConf.get("spark.hdfs.master")
    val hdfsPath = session.sparkContext.getConf.get("spark.hdfs.expedia.path")
    val valid_store_path = session.sparkContext.getConf.get("spark.hdfs.store.valid.path")


    val hadoopConfig: Configuration = session.sparkContext.hadoopConfiguration
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    val (valid_df, invalid_df) = ExpediaReader.readFromHdfs(session, hdfsMaster, hdfsPath)
    val hotelWithWeather_df = KafkaReader.getHotelWithWeatherDF(session)

    val cols = Array("Id", "Name", "Country", "City", "Address",
      "Longitude", "Latitude", "GeoHash", "date_diff_col")

    invalid_df
      .join(hotelWithWeather_df, col("hotel_id") === col("id"))
      .select(cols.head, cols.tail: _*)
      .distinct()
      .show(numRows = invalid_df.count().toInt, truncate = false)

    val group_by_country = valid_df
      .join(hotelWithWeather_df, col("hotel_id") === col("id"))
      .select("expedia_id", "hotel_id", "Country")
      .groupBy("Country")
      .agg(count("*"))
    group_by_country.show(numRows = group_by_country.count().toInt, truncate = false)

    val group_by_city = valid_df
      .join(hotelWithWeather_df, col("hotel_id") === col("id"))
      .select("expedia_id", "hotel_id", "City")
      .groupBy("City")
      .agg(count("*"))
    group_by_city.show(numRows = group_by_city.count().toInt, truncate = false)

    ExpediaWriter.writeToHdfs(valid_df, hdfsMaster + valid_store_path)
  }
}
