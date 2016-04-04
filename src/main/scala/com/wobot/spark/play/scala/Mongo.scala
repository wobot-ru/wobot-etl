package com.wobot.spark.play.scala

import com.mongodb.hadoop.MongoInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject

object Mongo {
    def main(args: Array[String]) {
        val conf = new SparkConf().setMaster("local[4]").setAppName("app")
        val sc = new SparkContext(conf)

        val tasksCfg = new Configuration()
        tasksCfg.set("mongo.input.uri", "mongodb://localhost:27017/scheduler1.tasks")

        val jobsCfg = new Configuration()
        jobsCfg.set("mongo.input.uri", "mongodb://localhost:27017/scheduler1.jobs")

        val tasksRDD = sc.newAPIHadoopRDD(tasksCfg, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])
        val jobsRDD = sc.newAPIHadoopRDD(jobsCfg, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])

        //to tuple (taskId, job)
        val transformedJobsRDD = jobsRDD.map(w => (w._2.get("task"), w._2))

        val joined = tasksRDD.join(transformedJobsRDD).take(1)

        for (item <- joined) {
            println("taskId: " + item._1)
            println("task: " + item._2._1)
            println("job: " + item._2._2)
        }

    }
}
