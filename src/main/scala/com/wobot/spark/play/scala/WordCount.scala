package com.wobot.spark.play.scala

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
      val conf = new SparkConf().setMaster("local[4]").setAppName("app")
      val sc = new SparkContext(conf)
      val lines = sc.textFile("./data/negatives.txt")
      val words = lines.flatMap(line => line.split(" "))
      val counts = words.map(word => (word, 1)).reduceByKey((x,y) => x + y).collect()

      for (count <- counts){
        println(count._1 + ": " + count._2)
      }
  }
}
