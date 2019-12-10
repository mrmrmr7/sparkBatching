package com.mrmrmr

import com.mrmrmr.hdfs.ExpediaReader
import com.mrmrmr.kafka.CollectFromKafkaToObjects
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object StartHere {
  def main(args: Array[String]): Unit = {
    val hdfsMaster = "hdfs://sandbox-hdp.hortonworks.com:8020"
    val hdfsPath = "/user/spark/expedia/"
    val valid_store_path = "/user/spark/expedia_valid/"

    val session = SparkSession
      .builder
      .master("local[*]")
      .appName("KafkaReader")
      .getOrCreate()

    val hadoopConfig: Configuration = session.sparkContext.hadoopConfiguration
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    val (valid_df, invalid_df) = ExpediaReader.read(session, hdfsMaster, hdfsPath)
    val hotelWithWeather_df = CollectFromKafkaToObjects.collectToHotelWithWeather(session)

    val cols = Array("Id", "Name", "Country", "City", "Address",
      "Longitude", "Latitude", "GeoHash", "date_diff_col")


    val invalid_df_joined = invalid_df
      .join(hotelWithWeather_df, col("hotel_id") === col("id"))
      .select(cols.head, cols.tail: _*)
      .distinct()

    invalid_df_joined.show(false)


    val group_by_country = valid_df
      .join(hotelWithWeather_df, col("hotel_id") === col("id"))
      .select("expedia_id", "hotel_id", "Country")
      .groupBy("Country")
      .agg(count("*"))

    group_by_country.show(false)



    val group_by_city = valid_df
      .join(hotelWithWeather_df, col("hotel_id") === col("id"))
      .select("expedia_id", "hotel_id", "City")
      .groupBy("City")
      .agg(count("*"))

    group_by_city.show(numRows = group_by_city.count().toInt, truncate = false)


    valid_df
      .withColumn("srch_ci_year", year(col("srch_ci")))
      .withColumn("srch_ci_month", month(col("srch_ci")))
      .withColumn("srch_ci_day", dayofmonth(col("srch_ci")))
      .drop("srch_ci")
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("srch_ci_year")
      .format("com.databricks.spark.avro")
      .save(hdfsMaster + valid_store_path)
  }

  def getListOfHDFSFiles(session: SparkSession ,hdfsPath: String): RemoteIterator[LocatedFileStatus] ={
    val path = new Path(hdfsPath)
    path
      .getFileSystem(session.sparkContext.hadoopConfiguration)
      .listFiles(new Path(hdfsPath), true)
  }
}
