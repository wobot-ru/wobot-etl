package com.wobot.spark.play.java;


import com.mongodb.hadoop.MongoInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BSONObject;
import scala.Tuple2;

import java.util.List;


public class Mongo {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("app");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration tasksCfg = new Configuration();
        tasksCfg.set("mongo.job.input.format","com.mongodb.hadoop.MongoInputFormat");
        tasksCfg.set("mongo.input.uri","mongodb://localhost:27017/scheduler1.tasks");

        Configuration jobsCfg = new Configuration();
        jobsCfg.set("mongo.job.input.format","com.mongodb.hadoop.MongoInputFormat");
        jobsCfg.set("mongo.input.uri","mongodb://localhost:27017/scheduler1.jobs");

        //(taskId, task) tuples
        JavaPairRDD<Object, BSONObject> tasksRDD = sc.newAPIHadoopRDD(tasksCfg, MongoInputFormat.class, Object.class, BSONObject.class);

        //(jobId, job) tuples
        JavaPairRDD<Object, BSONObject> jobsRDD = sc.newAPIHadoopRDD(jobsCfg, MongoInputFormat.class,Object.class,BSONObject.class);

        //tuple of (taskId, job)
        JavaPairRDD<Object, BSONObject> transformedJobsRDD = jobsRDD.mapToPair(w -> new Tuple2<>(w._2.get("task"), w._2));

        //join by taskId, tuple of (taskId, (task, job))
        JavaPairRDD<Object, Tuple2<BSONObject, BSONObject>> joined = tasksRDD.join(transformedJobsRDD);
        List<Tuple2<Object, Tuple2<BSONObject, BSONObject>>> joinedList = joined.take(10);

        for (Tuple2<Object, Tuple2<BSONObject, BSONObject>> item: joinedList){
            System.out.println("taskId: " + item._1);
            System.out.println("task: " + item._2._1);
            System.out.println("job: " + item._2._2);
        }

    }
}
