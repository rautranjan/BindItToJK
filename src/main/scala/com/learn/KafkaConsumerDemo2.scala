package com.learn

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object KafkaConsumerDemo2 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val spark = SparkSession.builder.master("local[2]").getOrCreate()

    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/home/ranjan/Desktop/tmp")
      .option("subscribe", "ranjan-test")
      .load()
    df.selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")

    val df1 = df.selectExpr("replace(value, ' ', ',') as value")
    val ds = df1
      .selectExpr("CAST(value AS STRING) as value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/home/ranjan/Desktop/tmp")
      .option("topic", "test")
      .start()
      .awaitTermination()
  }

}
