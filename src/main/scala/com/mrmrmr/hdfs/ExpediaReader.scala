package com.mrmrmr.hdfs

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import com.mrmrmr.StartHere.getListOfHDFSFiles
import com.mrmrmr.kafka.HotelWithWeatherOption
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StructField

object ExpediaReader {
  def read(session: SparkSession, master: String, path: String) : (DataFrame, DataFrame) = {
    session.sqlContext
      .sparkContext
      .hadoopConfiguration
      .set("avro.mapred.ignore.inputs.without.extension", "true")

    val window_srch_ci = Window
      .partitionBy("hotel_id")
      .orderBy("srch_ci", "srch_co")

    val df =session.read
      .format("com.databricks.spark.avro")
      .load(master + path)
      .withColumnRenamed("id", "expedia_id")
      .select("hotel_id", "expedia_id", "srch_ci", "srch_co")
      .withColumn("date_diff_col",
        datediff(col("srch_ci"), lag("srch_ci", 1).over(window_srch_ci)))
    val invalid_df = df.where((col("date_diff_col") > 1) && (col("date_diff_col") < 31))
    val valid_df   = df.where((col("date_diff_col") < 2) || (col("date_diff_col") > 30))

    (valid_df, invalid_df)
  }
}

case class Expedia(expedia_id: Option[String],
                   date_time: Option[String],
                   site_name: Option[String],
                   posa_continent: Option[Int],
                   user_location_country: Option[Int],
                   user_location_region: Option[Int],
                   user_location_city: Option[Int],
                   user_id: Option[Int],
                   orig_destination_distance: Option[Double],
                   is_mobile: Option[Int],
                   is_package: Option[Int],
                   channel: Option[Int],
                   srch_ci: Option[String],
                   srch_co: Option[String],
                   srch_adults_cnt: Option[Int],
                   srch_children_cnt: Option[Int],
                   srch_rm_cnt: Option[Int],
                   srch_destination_id: Option[Int],
                   srch_destination_type_id: Option[Int],
                   hotel_id: Option[String])
