package com.learn.Chapter1

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val file = sc.textFile(args(0))
    val words = file.flatMap(line => line.split(" "))
    val count = words.map(word => (word, 1)).reduceByKey { case (x, y) => (x + y) }
    count.saveAsTextFile("/home/ranjan/Desktop/output")

  }

}
