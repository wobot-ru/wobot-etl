package com.wobot.spark.play.java;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.util.List;
import java.util.Map;


public class Es {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("app");
        conf.set("es.nodes", "localhost:9200");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Map<String, Object>> profileRDD = JavaEsSpark.esRDD(sc, "wobot/profile");
        JavaPairRDD<String, Map<String, Object>> postRDD = JavaEsSpark.esRDD(sc, "wobot/post");

        JavaPairRDD<String, Map<String, Object>> transformedPostRDD = postRDD.mapToPair(w -> new Tuple2<>(w._2.get("profile_id").toString(),w._2));

        JavaPairRDD<String, Tuple2<Map<String, Object>,Map<String, Object>>> joinedRDD = profileRDD.join(transformedPostRDD);

        List<Tuple2<String, Tuple2<Map<String, Object>,Map<String, Object>>>> joined = joinedRDD.take(1);

        for (Tuple2<String, Tuple2<Map<String, Object>,Map<String, Object>>> item: joined){
            System.out.println("profileId: " + item._1);
            System.out.println("profile: " + item._2._1);
            System.out.println("post: " + item._2._2);
        }

    }
}
