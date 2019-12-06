package com.mrmrmr

import com.mrmrmr.hdfs.ExpediaReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object StartHere {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder
      .master("local[*]")
      .appName("KafkaReader")
      .getOrCreate()

    val hadoopConfig: Configuration = session.sparkContext.hadoopConfiguration

    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)

    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    val hdfsMaster = "hdfs://sandbox-hdp.hortonworks.com:8020"
    val hdfsPath = "/user/spark/expedia/"

    val list = getListOfHDFSFiles(session, hdfsMaster + hdfsPath)

    while (list.hasNext){
      val next = list.next()
      println(next.getPath)
    }

    ExpediaReader.read(session, hdfsMaster, hdfsPath)
  }

  def getListOfHDFSFiles(session: SparkSession ,hdfsPath: String): RemoteIterator[LocatedFileStatus] ={
    val path = new Path(hdfsPath)
    path
      .getFileSystem(session.sparkContext.hadoopConfiguration)
      .listFiles(new Path(hdfsPath), true)
  }
}
