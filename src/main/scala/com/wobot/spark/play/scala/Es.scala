package com.wobot.spark.play.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

object Es {
    def main(args: Array[String]) {
        val conf = new SparkConf().setMaster("local[4]").setAppName("app")
        conf.set("es.nodes", "localhost:9200")

        val sc = new SparkContext(conf)

        val profileRDD = sc.esRDD("wobot1/profile")
        val postRDD = sc.esRDD("wobot1/post")
        val transformedPostRDD = postRDD.map(x=>(x._2.get("profile_id").get.toString, x._2))

        val joined = profileRDD.join(transformedPostRDD).take(1)

        for (item <- joined){
            println("profileId: " + item._1)
            println("profile: " + item._2._1)
            println("post: " + item._2._2)
        }

    }
}
